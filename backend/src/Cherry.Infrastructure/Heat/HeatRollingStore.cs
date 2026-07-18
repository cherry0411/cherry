using Microsoft.Data.Sqlite;

namespace Cherry.Infrastructure.Heat;

public sealed record RollingHeatChange(
    byte[] InfoHash,
    long CurrentCount,
    long ProjectedCount,
    long Revision);
public sealed record RollingPrivacyStatus(
    bool SecureDelete,
    long PageCount,
    long FreePages,
    long? LastCompactedHour);
public sealed record RollingCapacityStatus(
    long UsedBytes,
    long MaxBytes,
    long AvailableFreeBytes,
    long MinFreeBytes,
    bool Exhausted,
    string? Reason);

public sealed class HeatRollingCapacityException : IOException
{
    public HeatRollingCapacityException(RollingCapacityStatus status)
        : base(status.Reason ?? "Rolling heat storage capacity exhausted") => Status = status;

    public RollingCapacityStatus Status { get; }
}

/// <summary>
/// Disposable, host-local exact authority for rolling 24-hour uniqueness.
/// Stable actor fingerprints exist only here and are removed as soon as they
/// fall outside the 24 hourly buckets. This database must not enter backups.
/// </summary>
public sealed class HeatRollingStore
{
    private readonly string _path;
    private readonly long _maxBytes;
    private readonly long _minFreeBytes;
    private readonly SemaphoreSlim _initializeGate = new(1, 1);
    private readonly SemaphoreSlim _capacityGate = new(1, 1);
    private int _initialized;
    private long _preparedHour = long.MinValue;

    public HeatRollingStore(HeatOptions options)
    {
        _path = System.IO.Path.Combine(options.DataDirectory, "heat-rolling-24h.sqlite3");
        _maxBytes = options.RollingMaxBytes;
        _minFreeBytes = options.RollingMinFreeBytes;
    }

    public string Path => _path;

    /// <summary>
    /// Once per closed UTC hour, expire first and physically close the WAL
    /// privacy window before evaluating hard capacity limits. Subsequent
    /// batches in the same hour only perform the cheap status check.
    /// </summary>
    public async Task<RollingCapacityStatus> PrepareForIngestAsync(
        CancellationToken cancellationToken)
    {
        var targetHour = UnixHour(DateTime.UtcNow) - 1;
        if (Volatile.Read(ref _preparedHour) < targetHour)
        {
            await _capacityGate.WaitAsync(cancellationToken);
            try
            {
                if (_preparedHour < targetHour)
                {
                    await using var connection = await OpenAsync(cancellationToken);
                    await using (var transaction =
                        (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken))
                    {
                        await using var expire = connection.CreateCommand();
                        expire.Transaction = transaction;
                        expire.CommandText = "DELETE FROM active WHERE last_seen_hour < $cutoff";
                        expire.Parameters.AddWithValue("$cutoff", targetHour - 23);
                        await expire.ExecuteNonQueryAsync(cancellationToken);
                        await transaction.CommitAsync(cancellationToken);
                    }
                    await RunPrivacyMaintenanceAsync(
                        targetHour, force: false, connection, cancellationToken);
                    Volatile.Write(ref _preparedHour, targetHour);
                }
            }
            finally
            {
                _capacityGate.Release();
            }
        }

        var status = await GetCapacityStatusAsync(cancellationToken);
        ThrowIfCapacityExceeded(status);
        return status;
    }

    public async Task<RollingCapacityStatus> GetCapacityStatusAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        return await ReadCapacityStatusAsync(connection, cancellationToken);
    }

    public async Task<long?> GetProjectedHourAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT projected_hour FROM projection_state WHERE singleton=1";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull ? null : Convert.ToInt64(value);
    }

    public async Task<(long? ProjectedHour, int CoverageHours)> GetStatusAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT projection.projected_hour,coverage.complete_from_hour
              FROM projection_state projection
              LEFT JOIN coverage_state coverage ON coverage.singleton=projection.singleton
             WHERE projection.singleton=1
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return (null, 0);
        var projected = reader.GetInt64(0);
        // Storage uptime is not evidence that every expected crawler observed
        // and durably delivered a complete hour. Until authenticated per-crawler
        // hourly closure receipts are implemented, coverage is deliberately
        // unknown/zero rather than inferred from this host's runtime.
        return (projected, 0);
    }

    public async Task MarkRuntimeStartAsync(long firstCompleteHour, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            "INSERT INTO coverage_state(singleton,complete_from_hour) VALUES(1,$hour) " +
            "ON CONFLICT(singleton) DO UPDATE SET complete_from_hour=" +
            "MAX(coverage_state.complete_from_hour,excluded.complete_from_hour)";
        command.Parameters.AddWithValue("$hour", firstCompleteHour);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ResetProjectionAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            "DELETE FROM projected_counts; DELETE FROM projection_state; " +
            "DELETE FROM deferred_hashes; " +
            "INSERT INTO dirty_hashes(hash_id,revision) " +
            "SELECT DISTINCT hash_id,1 FROM active " +
            "ON CONFLICT(hash_id) DO UPDATE SET revision=dirty_hashes.revision+1;";
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ApplyAsync(
        IReadOnlyList<ChhtBatch> batches,
        CancellationToken cancellationToken)
    {
        if (batches.Count == 0) return;
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await using var insertHash = connection.CreateCommand();
        insertHash.Transaction = transaction;
        insertHash.CommandText = "INSERT OR IGNORE INTO hashes(info_hash) VALUES($hash)";
        var insertHashValue = insertHash.Parameters.Add("$hash", SqliteType.Blob);
        await using var findHash = connection.CreateCommand();
        findHash.Transaction = transaction;
        findHash.CommandText = "SELECT hash_id FROM hashes WHERE info_hash=$hash";
        var findHashValue = findHash.Parameters.Add("$hash", SqliteType.Blob);
        await using var upsert = connection.CreateCommand();
        upsert.Transaction = transaction;
        upsert.CommandText =
            "INSERT INTO active(hash_id,actor,last_seen_hour,previous_seen_hour) " +
            "VALUES($hash_id,$actor,$hour,NULL) " +
            "ON CONFLICT(hash_id,actor) DO UPDATE SET " +
            "previous_seen_hour=CASE " +
            "WHEN excluded.last_seen_hour>active.last_seen_hour THEN active.last_seen_hour " +
            "WHEN active.previous_seen_hour IS NULL OR " +
            "excluded.last_seen_hour>active.previous_seen_hour THEN excluded.last_seen_hour " +
            "ELSE active.previous_seen_hour END," +
            "last_seen_hour=MAX(active.last_seen_hour,excluded.last_seen_hour) " +
            "WHERE excluded.last_seen_hour>active.last_seen_hour OR " +
            "(excluded.last_seen_hour<active.last_seen_hour AND " +
            "(active.previous_seen_hour IS NULL OR " +
            "excluded.last_seen_hour>active.previous_seen_hour))";
        var activeHash = upsert.Parameters.Add("$hash_id", SqliteType.Integer);
        var activeActor = upsert.Parameters.Add("$actor", SqliteType.Integer);
        var activeHour = upsert.Parameters.Add("$hour", SqliteType.Integer);
        var currentHour = UnixHour(DateTime.UtcNow);
        var oldestAccepted = currentHour - 24;

        foreach (var batch in batches)
        {
            var bucket = UnixHour(batch.Day, batch.Hour);
            if (bucket > currentHour)
                throw new InvalidDataException("Rolling heat observation is from a future UTC hour");
            if (bucket < oldestAccepted) continue;
            activeHour.Value = bucket;
            foreach (var group in batch.Groups)
            {
                insertHashValue.Value = group.InfoHash;
                await insertHash.ExecuteNonQueryAsync(cancellationToken);
                findHashValue.Value = group.InfoHash;
                activeHash.Value = (long)(await findHash.ExecuteScalarAsync(cancellationToken)
                    ?? throw new InvalidDataException("Rolling heat hash lookup failed"));
                foreach (var actor in group.ActorFingerprints)
                {
                    activeActor.Value = actor;
                    await upsert.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }
        ThrowIfCapacityExceeded(await ReadCapacityStatusAsync(connection, cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<(long? ProjectedHour, IReadOnlyList<RollingHeatChange> Changes)> ReadChangesAsync(
        long targetHour,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        // Expiration is indexed and dirties only affected hashes. Counting is
        // restricted to dirty hash PK ranges, never a full active-table scan.
        await using (var expire = connection.CreateCommand())
        {
            expire.Transaction = transaction;
            expire.CommandText = "DELETE FROM active WHERE last_seen_hour < $cutoff";
            expire.Parameters.AddWithValue("$cutoff", targetHour - 23);
            await expire.ExecuteNonQueryAsync(cancellationToken);
        }
        long? projected = null;
        await using (var state = connection.CreateCommand())
        {
            state.Transaction = transaction;
            state.CommandText = "SELECT projected_hour FROM projection_state WHERE singleton=1";
            var value = await state.ExecuteScalarAsync(cancellationToken);
            if (value is not null and not DBNull) projected = Convert.ToInt64(value);
        }
        var result = new List<RollingHeatChange>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT h.info_hash,
                   COALESCE(SUM(CASE
                       WHEN active.last_seen_hour BETWEEN $cutoff AND $target THEN 1
                       WHEN active.last_seen_hour > $target AND
                            active.previous_seen_hour BETWEEN $cutoff AND $target THEN 1
                       ELSE 0 END),0),
                   COALESCE(projected.actor_count,0),
                   dirty.revision
              FROM dirty_hashes dirty
              JOIN hashes h USING(hash_id)
              LEFT JOIN active USING(hash_id)
              LEFT JOIN projected_counts projected USING(hash_id)
              LEFT JOIN deferred_hashes deferred USING(hash_id)
             WHERE deferred.retry_after_hour IS NULL OR deferred.retry_after_hour <= $target
             GROUP BY dirty.hash_id,h.info_hash,projected.actor_count,dirty.revision
             ORDER BY dirty.hash_id
            """;
        command.Parameters.AddWithValue("$cutoff", targetHour - 23);
        command.Parameters.AddWithValue("$target", targetHour);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                result.Add(new RollingHeatChange(
                    (byte[])reader[0], reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3)));
        await transaction.CommitAsync(cancellationToken);
        await RunPrivacyMaintenanceAsync(targetHour, force: false, connection, cancellationToken);
        return (projected, result);
    }

    public async Task CommitProjectionAsync(
        long targetHour,
        IReadOnlyList<(byte[] InfoHash, long Count, long Revision)> mapped,
        IReadOnlyList<(byte[] InfoHash, long Revision)> unmapped,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await using var findHash = connection.CreateCommand();
        findHash.Transaction = transaction;
        findHash.CommandText = "SELECT hash_id FROM hashes WHERE info_hash=$hash";
        var hashValue = findHash.Parameters.Add("$hash", SqliteType.Blob);
        await using var upsert = connection.CreateCommand();
        upsert.Transaction = transaction;
        upsert.CommandText =
            "INSERT INTO projected_counts(hash_id,actor_count) VALUES($hash_id,$count) " +
            "ON CONFLICT(hash_id) DO UPDATE SET actor_count=excluded.actor_count";
        var projectedHash = upsert.Parameters.Add("$hash_id", SqliteType.Integer);
        var projectedCount = upsert.Parameters.Add("$count", SqliteType.Integer);
        await using var delete = connection.CreateCommand();
        delete.Transaction = transaction;
        delete.CommandText = "DELETE FROM projected_counts WHERE hash_id=$hash_id";
        var deletedHash = delete.Parameters.Add("$hash_id", SqliteType.Integer);
        await using var clean = connection.CreateCommand();
        clean.Transaction = transaction;
        // A current (incomplete) hour is excluded from this projection but
        // must remain dirty so it is promoted automatically next hour.
        clean.CommandText =
            "DELETE FROM dirty_hashes WHERE hash_id=$hash_id AND revision=$revision " +
            "AND NOT EXISTS(" +
            "SELECT 1 FROM active WHERE active.hash_id=$hash_id " +
            "AND active.last_seen_hour>$target)";
        var cleanHash = clean.Parameters.Add("$hash_id", SqliteType.Integer);
        var cleanRevision = clean.Parameters.Add("$revision", SqliteType.Integer);
        clean.Parameters.AddWithValue("$target", targetHour);
        await using var clearDeferred = connection.CreateCommand();
        clearDeferred.Transaction = transaction;
        clearDeferred.CommandText = "DELETE FROM deferred_hashes WHERE hash_id=$hash_id";
        var clearDeferredHash = clearDeferred.Parameters.Add("$hash_id", SqliteType.Integer);
        await using var deferMapped = connection.CreateCommand();
        deferMapped.Transaction = transaction;
        deferMapped.CommandText =
            "INSERT INTO deferred_hashes(hash_id,retry_after_hour) " +
            "SELECT $hash_id,$retry WHERE EXISTS(" +
            "SELECT 1 FROM dirty_hashes WHERE hash_id=$hash_id AND revision=$revision) " +
            "ON CONFLICT(hash_id) DO UPDATE SET retry_after_hour=excluded.retry_after_hour";
        var deferMappedHash = deferMapped.Parameters.Add("$hash_id", SqliteType.Integer);
        var deferMappedRevision = deferMapped.Parameters.Add("$revision", SqliteType.Integer);
        deferMapped.Parameters.AddWithValue("$retry", checked(targetHour + 1));
        foreach (var row in mapped)
        {
            hashValue.Value = row.InfoHash;
            var hashId = await findHash.ExecuteScalarAsync(cancellationToken);
            if (hashId is null) continue;
            if (row.Count == 0)
            {
                deletedHash.Value = (long)hashId;
                await delete.ExecuteNonQueryAsync(cancellationToken);
            }
            else
            {
                projectedHash.Value = (long)hashId;
                projectedCount.Value = row.Count;
                await upsert.ExecuteNonQueryAsync(cancellationToken);
            }
            clearDeferredHash.Value = (long)hashId;
            await clearDeferred.ExecuteNonQueryAsync(cancellationToken);
            cleanHash.Value = (long)hashId;
            cleanRevision.Value = row.Revision;
            await clean.ExecuteNonQueryAsync(cancellationToken);

            // If the same revision remains dirty solely because it contains
            // current-hour observations, avoid regrouping it every 30 seconds.
            // A late closed-hour mutation increments revision and therefore
            // fails this CAS; its trigger also clears any prior deferral.
            deferMappedHash.Value = (long)hashId;
            deferMappedRevision.Value = row.Revision;
            await deferMapped.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var defer = connection.CreateCommand();
        defer.Transaction = transaction;
        defer.CommandText =
            "INSERT INTO deferred_hashes(hash_id,retry_after_hour) " +
            "SELECT $hash_id,$retry WHERE EXISTS(" +
            "SELECT 1 FROM dirty_hashes WHERE hash_id=$hash_id AND revision=$revision) " +
            "ON CONFLICT(hash_id) DO UPDATE SET retry_after_hour=excluded.retry_after_hour";
        var deferHash = defer.Parameters.Add("$hash_id", SqliteType.Integer);
        var deferRevision = defer.Parameters.Add("$revision", SqliteType.Integer);
        defer.Parameters.AddWithValue("$retry", checked(targetHour + 1));
        foreach (var row in unmapped)
        {
            hashValue.Value = row.InfoHash;
            var hashId = await findHash.ExecuteScalarAsync(cancellationToken);
            if (hashId is null) continue;
            deferHash.Value = (long)hashId;
            deferRevision.Value = row.Revision;
            await defer.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var state = connection.CreateCommand())
        {
            state.Transaction = transaction;
            state.CommandText =
                "INSERT INTO projection_state(singleton,projected_hour) VALUES(1,$hour) " +
                "ON CONFLICT(singleton) DO UPDATE SET projected_hour=excluded.projected_hour";
            state.Parameters.AddWithValue("$hour", targetHour);
            await state.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var prune = connection.CreateCommand())
        {
            prune.Transaction = transaction;
            prune.CommandText =
                "DELETE FROM deferred_hashes WHERE NOT EXISTS(" +
                "SELECT 1 FROM active WHERE active.hash_id=deferred_hashes.hash_id) " +
                "AND NOT EXISTS(SELECT 1 FROM projected_counts " +
                "WHERE projected_counts.hash_id=deferred_hashes.hash_id);" +
                "DELETE FROM dirty_hashes WHERE NOT EXISTS(" +
                "SELECT 1 FROM active WHERE active.hash_id=dirty_hashes.hash_id) " +
                "AND NOT EXISTS(SELECT 1 FROM projected_counts " +
                "WHERE projected_counts.hash_id=dirty_hashes.hash_id);" +
                "DELETE FROM hashes WHERE NOT EXISTS(" +
                "SELECT 1 FROM active WHERE active.hash_id=hashes.hash_id) AND NOT EXISTS(" +
                "SELECT 1 FROM projected_counts WHERE projected_counts.hash_id=hashes.hash_id) " +
                "AND NOT EXISTS(SELECT 1 FROM dirty_hashes " +
                "WHERE dirty_hashes.hash_id=hashes.hash_id)";
            await prune.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        // Projection rows contain no raw address, but they share a WAL with the
        // disposable actor table. Do not leave a successful projection commit
        // as an excuse for expired actor frames to remain for another cycle.
        await CheckpointAndTruncateWalAsync(connection, cancellationToken);
    }

    /// <summary>
    /// Physically bounds the disposable actor store. Every expiry pass performs
    /// a checked WAL truncation; secure_delete then covers the main database.
    /// The daily, freelist-aware VACUUM additionally releases peak pages. Force
    /// is used by privacy verification and controlled maintenance only.
    /// </summary>
    public async Task RunPrivacyMaintenanceAsync(
        long targetHour,
        bool force,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await RunPrivacyMaintenanceAsync(targetHour, force, connection, cancellationToken);
    }

    public async Task<RollingPrivacyStatus> GetPrivacyStatusAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        long Scalar(string pragma)
        {
            using var command = connection.CreateCommand();
            command.CommandText = pragma;
            return Convert.ToInt64(command.ExecuteScalar());
        }
        long? last = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText =
                "SELECT last_compacted_hour FROM maintenance_state WHERE singleton=1";
            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is not null and not DBNull) last = Convert.ToInt64(value);
        }
        return new RollingPrivacyStatus(
            Scalar("PRAGMA secure_delete") == 1,
            Scalar("PRAGMA page_count"),
            Scalar("PRAGMA freelist_count"),
            last);
    }

    private static async Task RunPrivacyMaintenanceAsync(
        long targetHour,
        bool force,
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        // This runs immediately after the expiration transaction. A busy
        // checkpoint is a retryable hard failure: returning success would leave
        // expired actor cells in forensic WAL history for up to another day.
        await CheckpointAndTruncateWalAsync(connection, cancellationToken);

        long? lastCompacted = null;
        await using (var state = connection.CreateCommand())
        {
            state.CommandText =
                "SELECT last_compacted_hour FROM maintenance_state WHERE singleton=1";
            var value = await state.ExecuteScalarAsync(cancellationToken);
            if (value is not null and not DBNull) lastCompacted = Convert.ToInt64(value);
        }
        if (!force && lastCompacted is not null && targetHour - lastCompacted < 24)
            return;

        long pageCount;
        long freePages;
        await using (var pages = connection.CreateCommand())
        {
            pages.CommandText = "PRAGMA page_count";
            pageCount = Convert.ToInt64(await pages.ExecuteScalarAsync(cancellationToken));
            pages.CommandText = "PRAGMA freelist_count";
            freePages = Convert.ToInt64(await pages.ExecuteScalarAsync(cancellationToken));
        }

        // Avoid a daily full-file rewrite for a few reusable pages on 2C/4G.
        // Deleted cells are already zeroed by secure_delete; VACUUM is for the
        // peak-size bound and runs at most once per 24 projected hours.
        if (force || freePages >= 32 && freePages * 10 >= Math.Max(pageCount, 1))
        {
            await using var compact = connection.CreateCommand();
            compact.CommandText = "VACUUM";
            await compact.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var record = connection.CreateCommand())
        {
            record.CommandText =
                "INSERT INTO maintenance_state(singleton,last_compacted_hour) VALUES(1,$hour) " +
                "ON CONFLICT(singleton) DO UPDATE SET last_compacted_hour=excluded.last_compacted_hour";
            record.Parameters.AddWithValue("$hour", targetHour);
            await record.ExecuteNonQueryAsync(cancellationToken);
        }
        // VACUUM and the maintenance marker are writes in WAL mode too.
        await CheckpointAndTruncateWalAsync(connection, cancellationToken);
    }

    private static async Task CheckpointAndTruncateWalAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        await using var checkpoint = connection.CreateCommand();
        checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
        await using var reader = await checkpoint.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            throw new IOException("SQLite WAL checkpoint returned no status row");
        var busy = reader.GetInt64(0);
        var logFrames = reader.GetInt64(1);
        var checkpointedFrames = reader.GetInt64(2);
        if (busy != 0)
            throw new IOException(
                $"SQLite WAL truncate checkpoint remained busy " +
                $"(log={logFrames}, checkpointed={checkpointedFrames}); retry required");
    }

    private async Task<RollingCapacityStatus> ReadCapacityStatusAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        long pageCount;
        long pageSize;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "PRAGMA page_count";
            pageCount = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
            command.CommandText = "PRAGMA page_size";
            pageSize = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
        }
        var logicalBytes = checked(pageCount * pageSize);
        var physicalBytes = ExistingLength(_path) + ExistingLength(_path + "-wal") +
                            ExistingLength(_path + "-shm");
        var usedBytes = Math.Max(logicalBytes, physicalBytes);
        var fullPath = System.IO.Path.GetFullPath(_path);
        var drive = DriveInfo.GetDrives()
            .Where(candidate => fullPath.StartsWith(
                candidate.RootDirectory.FullName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(candidate => candidate.RootDirectory.FullName.Length)
            .FirstOrDefault();
        var available = drive?.AvailableFreeSpace ?? 0;
        string? reason = null;
        if (_maxBytes > 0 && usedBytes >= _maxBytes)
            reason = $"Rolling heat store reached its {_maxBytes}-byte hard limit";
        else if (_minFreeBytes > 0 && available < _minFreeBytes)
            reason = $"Rolling heat filesystem has only {available} free bytes; {_minFreeBytes} required";
        return new RollingCapacityStatus(
            usedBytes, _maxBytes, available, _minFreeBytes, reason is not null, reason);
    }

    private static long ExistingLength(string path) =>
        File.Exists(path) ? new FileInfo(path).Length : 0;

    private static void ThrowIfCapacityExceeded(RollingCapacityStatus status)
    {
        if (status.Exhausted) throw new HeatRollingCapacityException(status);
    }

    public static long UnixHour(DateTime value) =>
        new DateTimeOffset(value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime())
            .ToUnixTimeSeconds() / 3600;

    public static long UnixHour(DateOnly day, byte hour)
    {
        if (hour > 23) throw new ArgumentOutOfRangeException(nameof(hour));
        return new DateTimeOffset(day.ToDateTime(new TimeOnly(hour, 0), DateTimeKind.Utc))
            .ToUnixTimeSeconds() / 3600;
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(_path)!);
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = _path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false
        }.ToString());
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA synchronous=FULL;
            PRAGMA foreign_keys=ON;
            PRAGMA busy_timeout=5000;
            PRAGMA secure_delete=ON;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);
        return connection;
    }

    private async Task EnsureSchemaAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _initialized) != 0) return;
        await _initializeGate.WaitAsync(cancellationToken);
        try
        {
            if (_initialized != 0) return;
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS hashes (
                hash_id INTEGER PRIMARY KEY,
                info_hash BLOB NOT NULL UNIQUE CHECK(length(info_hash)=20)
            );
            CREATE TABLE IF NOT EXISTS active (
                hash_id INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                last_seen_hour INTEGER NOT NULL,
                previous_seen_hour INTEGER NULL,
                PRIMARY KEY(hash_id,actor),
                FOREIGN KEY(hash_id) REFERENCES hashes(hash_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_heat_rolling_expiry ON active(last_seen_hour);
            CREATE TABLE IF NOT EXISTS dirty_hashes (
                hash_id INTEGER PRIMARY KEY,
                revision INTEGER NOT NULL CHECK(revision > 0),
                FOREIGN KEY(hash_id) REFERENCES hashes(hash_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS projected_counts (
                hash_id INTEGER PRIMARY KEY,
                actor_count INTEGER NOT NULL CHECK(actor_count > 0),
                FOREIGN KEY(hash_id) REFERENCES hashes(hash_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS deferred_hashes (
                hash_id INTEGER PRIMARY KEY,
                retry_after_hour INTEGER NOT NULL,
                FOREIGN KEY(hash_id) REFERENCES hashes(hash_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS projection_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                projected_hour INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS coverage_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                complete_from_hour INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS maintenance_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                last_compacted_hour INTEGER NOT NULL
            ) WITHOUT ROWID;
            """;
            await command.ExecuteNonQueryAsync(cancellationToken);

            // Upgrade disposable pre-release rolling stores without preserving
            // the obsolete aggregate counter. This DB is never restored.
            await using (var columns = connection.CreateCommand())
            {
                columns.CommandText =
                    "SELECT 1 FROM pragma_table_info('active') WHERE name='previous_seen_hour'";
                if (await columns.ExecuteScalarAsync(cancellationToken) is null)
                {
                    await using var upgrade = connection.CreateCommand();
                    upgrade.CommandText =
                        "ALTER TABLE active ADD COLUMN previous_seen_hour INTEGER NULL";
                    await upgrade.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            await using (var columns = connection.CreateCommand())
            {
                columns.CommandText =
                    "SELECT 1 FROM pragma_table_info('dirty_hashes') WHERE name='revision'";
                if (await columns.ExecuteScalarAsync(cancellationToken) is null)
                {
                    await using var upgrade = connection.CreateCommand();
                    upgrade.CommandText =
                        "ALTER TABLE dirty_hashes ADD COLUMN revision INTEGER NOT NULL DEFAULT 1";
                    await upgrade.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            await using (var triggers = connection.CreateCommand())
            {
                triggers.CommandText =
                    """
                DROP TRIGGER IF EXISTS trg_heat_rolling_active_insert;
                DROP TRIGGER IF EXISTS trg_heat_rolling_active_update;
                DROP TRIGGER IF EXISTS trg_heat_rolling_active_delete;
                DROP TABLE IF EXISTS current_counts;
                CREATE TRIGGER trg_heat_rolling_active_insert
                AFTER INSERT ON active BEGIN
                    INSERT INTO dirty_hashes(hash_id,revision) VALUES(new.hash_id,1)
                    ON CONFLICT(hash_id) DO UPDATE SET revision=dirty_hashes.revision+1;
                    DELETE FROM deferred_hashes
                     WHERE hash_id=new.hash_id AND EXISTS(
                        SELECT 1 FROM projection_state state
                         WHERE new.last_seen_hour BETWEEN state.projected_hour-23
                                                      AND state.projected_hour);
                END;
                CREATE TRIGGER trg_heat_rolling_active_update
                AFTER UPDATE OF last_seen_hour,previous_seen_hour ON active BEGIN
                    INSERT INTO dirty_hashes(hash_id,revision) VALUES(new.hash_id,1)
                    ON CONFLICT(hash_id) DO UPDATE SET revision=dirty_hashes.revision+1;
                    DELETE FROM deferred_hashes
                     WHERE hash_id=new.hash_id AND EXISTS(
                        SELECT 1 FROM projection_state state
                         WHERE (new.last_seen_hour BETWEEN state.projected_hour-23
                                                        AND state.projected_hour)
                            OR (new.previous_seen_hour IS NOT old.previous_seen_hour AND
                                new.previous_seen_hour BETWEEN state.projected_hour-23
                                                            AND state.projected_hour));
                END;
                CREATE TRIGGER trg_heat_rolling_active_delete
                AFTER DELETE ON active BEGIN
                    INSERT INTO dirty_hashes(hash_id,revision) VALUES(old.hash_id,1)
                    ON CONFLICT(hash_id) DO UPDATE SET revision=dirty_hashes.revision+1;
                END;
                """;
                await triggers.ExecuteNonQueryAsync(cancellationToken);
            }
            Volatile.Write(ref _initialized, 1);
        }
        finally
        {
            _initializeGate.Release();
        }
    }
}
