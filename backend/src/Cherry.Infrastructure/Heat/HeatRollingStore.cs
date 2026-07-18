using Microsoft.Data.Sqlite;
using System.Globalization;
using System.Text;

namespace Cherry.Infrastructure.Heat;

public sealed record RollingHeatChange(
    byte[] InfoHash,
    long CurrentCount,
    long ProjectedCount,
    long Revision);
public sealed record RollingHeatPage(
    long? ProjectedHour,
    long NextHashId,
    bool HasMore,
    IReadOnlyList<RollingHeatChange> Changes);
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
    // At the maximum textual width of slot + signed actor, 1,024 rows stay
    // safely below the ~85 KiB LOH threshold for both StringBuilder's char
    // storage and the bound JSON string.
    private const int StagingRowsPerCommand = 1_024;
    private readonly string _path;
    private readonly long _maxBytes;
    private readonly long _minFreeBytes;
    private readonly SemaphoreSlim _initializeGate = new(1, 1);
    private readonly SemaphoreSlim _capacityGate = new(1, 1);
    // SQLite WAL still permits only one writer. Keep the process-local writers
    // out of busy_timeout races, but never hold this gate while PostgreSQL or
    // Meilisearch is being awaited.
    private readonly SemaphoreSlim _writeGate = new(1, 1);
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
        await PrepareHourAsync(targetHour, cancellationToken);

        var status = await GetCapacityStatusAsync(cancellationToken);
        ThrowIfCapacityExceeded(status);
        return status;
    }

    /// <summary>
    /// Establishes the same expired-actor boundary used by ingest before a
    /// rolling projection starts. The work is idempotent per closed UTC hour.
    /// </summary>
    public Task PrepareForProjectionAsync(long targetHour, CancellationToken cancellationToken) =>
        PrepareHourAsync(targetHour, cancellationToken);

    private async Task PrepareHourAsync(long targetHour, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _preparedHour) < targetHour)
        {
            await _capacityGate.WaitAsync(cancellationToken);
            try
            {
                if (_preparedHour < targetHour)
                {
                    await _writeGate.WaitAsync(cancellationToken);
                    try
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
                    finally
                    {
                        _writeGate.Release();
                    }
                }
            }
            finally
            {
                _capacityGate.Release();
            }
        }
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
        await _writeGate.WaitAsync(cancellationToken);
        try
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
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task ResetProjectionAsync(CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
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
        finally
        {
            _writeGate.Release();
        }
    }

    public async Task ApplyAsync(
        IReadOnlyList<ChhtBatch> batches,
        CancellationToken cancellationToken)
    {
        if (batches.Count == 0) return;
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await ApplyCoreAsync(batches, cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private async Task ApplyCoreAsync(
        IReadOnlyList<ChhtBatch> batches,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using (var createStaging = connection.CreateCommand())
        {
            // One protocol batch is bounded to 250k observations. Process and
            // clear it before staging the next batch so group commit does not
            // multiply that memory bound by CommitBatchRequests.
            createStaging.CommandText =
                """
                PRAGMA temp_store=MEMORY;
                CREATE TEMP TABLE ingest_hashes (
                    slot INTEGER PRIMARY KEY,
                    info_hash BLOB NOT NULL CHECK(length(info_hash)=20),
                    hash_id INTEGER NULL
                );
                CREATE TEMP TABLE ingest_observations (
                    ordinal INTEGER PRIMARY KEY,
                    slot INTEGER NOT NULL,
                    actor INTEGER NOT NULL
                );
                CREATE TEMP TABLE ingest_mutations (
                    hash_id INTEGER NOT NULL,
                    actor INTEGER NOT NULL,
                    PRIMARY KEY(hash_id,actor)
                ) WITHOUT ROWID;
                """;
            await createStaging.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        var currentHour = UnixHour(DateTime.UtcNow);
        var oldestAccepted = currentHour - 24;

        foreach (var batch in batches)
        {
            var bucket = UnixHour(batch.Day, batch.Hour);
            if (bucket > currentHour)
                throw new InvalidDataException("Rolling heat observation is from a future UTC hour");
            if (bucket < oldestAccepted) continue;
            await ClearStagingAsync(connection, transaction, cancellationToken);
            await StageHashesAsync(connection, transaction, batch.Groups, cancellationToken);
            await StageObservationsAsync(connection, transaction, batch.Groups, cancellationToken);

            await using (var resolveHashes = connection.CreateCommand())
            {
                resolveHashes.Transaction = transaction;
                // Resolve every distinct hash once inside SQLite. The old path
                // crossed the provider boundary for both INSERT and SELECT on
                // every group before doing so again for every actor UPSERT.
                resolveHashes.CommandText =
                    """
                    INSERT OR IGNORE INTO hashes(info_hash)
                    SELECT info_hash FROM ingest_hashes ORDER BY slot;
                    UPDATE ingest_hashes
                       SET hash_id=(SELECT hashes.hash_id FROM hashes
                                     WHERE hashes.info_hash=ingest_hashes.info_hash);
                    """;
                await resolveHashes.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var identifyMutations = connection.CreateCommand())
            {
                identifyMutations.Transaction = transaction;
                identifyMutations.CommandText =
                    """
                    INSERT OR IGNORE INTO ingest_mutations(hash_id,actor)
                    SELECT incoming.hash_id,observation.actor
                      FROM ingest_observations observation
                      JOIN ingest_hashes incoming USING(slot)
                      LEFT JOIN active ON active.hash_id=incoming.hash_id
                                      AND active.actor=observation.actor
                     WHERE active.hash_id IS NULL
                        OR $hour>active.last_seen_hour
                        OR ($hour<active.last_seen_hour AND
                            (active.previous_seen_hour IS NULL OR
                             $hour>active.previous_seen_hour));
                    INSERT INTO rolling_ingest_control(singleton) VALUES(1);
                    """;
                identifyMutations.Parameters.AddWithValue("$hour", bucket);
                await identifyMutations.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var upsert = connection.CreateCommand();
            upsert.Transaction = transaction;
            // CHHT groups are unique within a protocol batch and every row in
            // the batch has the same UTC hour. Ordering by the wire ordinal
            // also preserves the legacy behavior for directly constructed
            // duplicate test inputs. The unchanged conflict predicate means a
            // replay fires no trigger, while every real insert/update advances
            // dirty_hashes exactly as before.
            upsert.CommandText =
                """
                INSERT INTO active(hash_id,actor,last_seen_hour,previous_seen_hour)
                SELECT incoming.hash_id,observation.actor,$hour,NULL
                  FROM ingest_observations observation
                  JOIN ingest_hashes incoming USING(slot)
                 WHERE true
                 ORDER BY observation.ordinal
                ON CONFLICT(hash_id,actor) DO UPDATE SET
                    previous_seen_hour=CASE
                    WHEN excluded.last_seen_hour>active.last_seen_hour THEN active.last_seen_hour
                    WHEN active.previous_seen_hour IS NULL OR
                    excluded.last_seen_hour>active.previous_seen_hour THEN excluded.last_seen_hour
                    ELSE active.previous_seen_hour END,
                    last_seen_hour=MAX(active.last_seen_hour,excluded.last_seen_hour)
                WHERE excluded.last_seen_hour>active.last_seen_hour OR
                (excluded.last_seen_hour<active.last_seen_hour AND
                (active.previous_seen_hour IS NULL OR
                excluded.last_seen_hour>active.previous_seen_hour));
                """;
            upsert.Parameters.AddWithValue("$hour", bucket);
            await upsert.ExecuteNonQueryAsync(cancellationToken);

            await using (var recordMutations = connection.CreateCommand())
            {
                recordMutations.Transaction = transaction;
                // Suppressing the row triggers avoids repeating the same dirty
                // hash and deferred lookups for every actor. The aggregate adds
                // the exact number of active-row mutations, preserving revision
                // CAS values as well as monotonicity. Deferred invalidation is
                // derived from the touched rows' final top-two hours and is
                // equivalent to the INSERT/UPDATE trigger predicates.
                recordMutations.CommandText =
                    """
                    INSERT INTO dirty_hashes(hash_id,revision)
                    SELECT hash_id,COUNT(*) FROM ingest_mutations GROUP BY hash_id
                    ON CONFLICT(hash_id) DO UPDATE SET
                        revision=dirty_hashes.revision+excluded.revision;
                    DELETE FROM deferred_hashes
                     WHERE hash_id IN (
                        SELECT DISTINCT mutation.hash_id
                          FROM ingest_mutations mutation
                          JOIN active ON active.hash_id=mutation.hash_id
                                     AND active.actor=mutation.actor
                          JOIN projection_state state ON state.singleton=1
                         WHERE active.last_seen_hour BETWEEN state.projected_hour-23
                                                         AND state.projected_hour
                            OR active.previous_seen_hour BETWEEN state.projected_hour-23
                                                             AND state.projected_hour
                     );
                    DELETE FROM rolling_ingest_control WHERE singleton=1;
                    """;
                await recordMutations.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        ThrowIfCapacityExceeded(await ReadCapacityStatusAsync(connection, cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task ClearStagingAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            "DELETE FROM ingest_mutations; DELETE FROM ingest_observations; DELETE FROM ingest_hashes";
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task StageHashesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyList<ChhtHashGroup> groups,
        CancellationToken cancellationToken)
    {
        if (groups.Count == 0) return;
        var packed = GC.AllocateUninitializedArray<byte>(checked(groups.Count * 20));
        for (var slot = 0; slot < groups.Count; slot++)
        {
            var hash = groups[slot].InfoHash;
            if (hash.Length != 20)
                throw new InvalidDataException("Rolling heat info hash must be exactly 20 bytes");
            hash.CopyTo(packed, slot * 20);
        }
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        // All protocol hashes are fixed width, so a single packed BLOB removes
        // 2*N provider parameters. The recursive counter is wholly inside
        // SQLite and creates one bounded temp row per hash.
        command.CommandText =
            """
            WITH RECURSIVE slots(slot) AS (
                SELECT 0
                UNION ALL
                SELECT slot+1 FROM slots WHERE slot+1 < $count
            )
            INSERT INTO ingest_hashes(slot,info_hash)
            SELECT slot,substr($packed,slot*20+1,20) FROM slots;
            """;
        command.Parameters.AddWithValue("$count", groups.Count);
        command.Parameters.Add("$packed", SqliteType.Blob).Value = packed;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task StageObservationsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyList<ChhtHashGroup> groups,
        CancellationToken cancellationToken)
    {
        var totalRecords = 0;
        foreach (var group in groups)
            totalRecords = checked(totalRecords + group.ActorFingerprints.Count);
        if (totalRecords == 0) return;

        var json = CreateObservationJson(
            Math.Min(StagingRowsPerCommand, totalRecords));
        var staged = 0;
        var ordinal = 0;
        for (var slot = 0; slot < groups.Count; slot++)
            foreach (var actor in groups[slot].ActorFingerprints)
            {
                if (staged != 0) json.Append(',');
                json.Append('[');
                AppendInvariant(json, slot);
                json.Append(',');
                AppendInvariant(json, actor);
                json.Append(']');
                staged++;
                if (staged == StagingRowsPerCommand)
                {
                    json.Append(']');
                    await InsertObservationRowsAsync(
                        connection, transaction, json.ToString(), ordinal, cancellationToken);
                    ordinal += staged;
                    staged = 0;
                    json = CreateObservationJson(
                        Math.Min(StagingRowsPerCommand, totalRecords - ordinal));
                }
            }
        if (staged != 0)
        {
            json.Append(']');
            await InsertObservationRowsAsync(
                connection, transaction, json.ToString(), ordinal, cancellationToken);
        }
    }

    private static StringBuilder CreateObservationJson(int recordCapacity) =>
        new StringBuilder(Math.Max(2, checked(recordCapacity * 32 + 2))).Append('[');

    private static void AppendInvariant(StringBuilder builder, long value)
    {
        Span<char> buffer = stackalloc char[20];
        if (!value.TryFormat(buffer, out var written, provider: CultureInfo.InvariantCulture))
            throw new InvalidOperationException("Failed to format rolling heat integer");
        builder.Append(buffer[..written]);
    }

    private static async Task InsertObservationRowsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string rowsJson,
        int ordinal,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO ingest_observations(ordinal,slot,actor)
            SELECT $ordinal+CAST(key AS INTEGER),
                   CAST(json_extract(value,'$[0]') AS INTEGER),
                   CAST(json_extract(value,'$[1]') AS INTEGER)
              FROM json_each($rows);
            """;
        command.Parameters.AddWithValue("$ordinal", ordinal);
        command.Parameters.AddWithValue("$rows", rowsJson);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<(long? ProjectedHour, IReadOnlyList<RollingHeatChange> Changes)> ReadChangesAsync(
        long targetHour,
        CancellationToken cancellationToken)
    {
        // Compatibility helper for tests and diagnostics. Production uses the
        // bounded page API below and therefore never materializes this list.
        await PrepareForProjectionAsync(targetHour, cancellationToken);
        const int pageSize = 4_096;
        var result = new List<RollingHeatChange>();
        long afterHashId = 0;
        long? projected = null;
        while (true)
        {
            var page = await ReadChangesPageAsync(
                targetHour, afterHashId, pageSize, cancellationToken);
            projected ??= page.ProjectedHour;
            result.AddRange(page.Changes);
            if (!page.HasMore) return (projected, result);
            afterHashId = page.NextHashId;
        }
    }

    /// <summary>
    /// Reads one stable, read-only dirty-hash page. New mutations below the
    /// cursor remain dirty and are picked up by the next pass; revision CAS in
    /// the commit path prevents an older page from acknowledging them.
    /// </summary>
    public async Task<RollingHeatPage> ReadChangesPageAsync(
        long targetHour,
        long afterHashId,
        int pageSize,
        CancellationToken cancellationToken)
    {
        if (afterHashId < 0) throw new ArgumentOutOfRangeException(nameof(afterHashId));
        if (pageSize is < 1 or > 50_000) throw new ArgumentOutOfRangeException(nameof(pageSize));

        await using var connection = await OpenAsync(cancellationToken);
        long? projected = null;
        await using (var state = connection.CreateCommand())
        {
            state.CommandText = "SELECT projected_hour FROM projection_state WHERE singleton=1";
            var value = await state.ExecuteScalarAsync(cancellationToken);
            if (value is not null and not DBNull) projected = Convert.ToInt64(value);
        }

        var rows = new List<(long HashId, RollingHeatChange Change)>(checked(pageSize + 1));
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            WITH candidates AS MATERIALIZED (
                SELECT dirty.hash_id,dirty.revision
                  FROM dirty_hashes dirty
                  LEFT JOIN deferred_hashes deferred USING(hash_id)
                 WHERE dirty.hash_id>$after
                   AND (deferred.retry_after_hour IS NULL OR
                        deferred.retry_after_hour <= $target)
                 ORDER BY dirty.hash_id
                 LIMIT $limit
            )
            SELECT candidate.hash_id,h.info_hash,
                   (SELECT COUNT(*) FROM active
                     WHERE active.hash_id=candidate.hash_id
                       AND (active.last_seen_hour BETWEEN $cutoff AND $target
                            OR (active.last_seen_hour>$target AND
                                active.previous_seen_hour BETWEEN $cutoff AND $target))),
                   COALESCE(projected.actor_count,0),
                   candidate.revision
              FROM candidates candidate
              JOIN hashes h USING(hash_id)
              LEFT JOIN projected_counts projected USING(hash_id)
            """;
        command.Parameters.AddWithValue("$cutoff", targetHour - 23);
        command.Parameters.AddWithValue("$target", targetHour);
        command.Parameters.AddWithValue("$after", afterHashId);
        command.Parameters.AddWithValue("$limit", checked(pageSize + 1));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            rows.Add((
                reader.GetInt64(0),
                new RollingHeatChange(
                    (byte[])reader[1], reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4))));
        rows.Sort(static (left, right) => left.HashId.CompareTo(right.HashId));
        var hasMore = rows.Count > pageSize;
        if (hasMore) rows.RemoveAt(pageSize);
        var nextHashId = rows.Count == 0 ? afterHashId : rows[^1].HashId;
        var result = rows.Select(static row => row.Change).ToArray();
        return new RollingHeatPage(projected, nextHashId, hasMore, result);
    }

    public async Task CommitProjectionAsync(
        long targetHour,
        IReadOnlyList<(byte[] InfoHash, long Count, long Revision)> mapped,
        IReadOnlyList<(byte[] InfoHash, long Revision)> unmapped,
        CancellationToken cancellationToken)
    {
        await CommitProjectionPageAsync(targetHour, mapped, unmapped, cancellationToken);
        await FinalizeProjectionAsync(targetHour, cancellationToken);
    }

    public async Task CommitProjectionPageAsync(
        long targetHour,
        IReadOnlyList<(byte[] InfoHash, long Count, long Revision)> mapped,
        IReadOnlyList<(byte[] InfoHash, long Revision)> unmapped,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await CommitProjectionCoreAsync(
                targetHour, mapped, unmapped, cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private async Task CommitProjectionCoreAsync(
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
        await using var cleanupAcknowledged = connection.CreateCommand();
        cleanupAcknowledged.Transaction = transaction;
        cleanupAcknowledged.CommandText =
            "DELETE FROM deferred_hashes WHERE hash_id=$hash_id AND NOT EXISTS(" +
            "SELECT 1 FROM dirty_hashes WHERE dirty_hashes.hash_id=$hash_id) " +
            "AND NOT EXISTS(SELECT 1 FROM active WHERE active.hash_id=$hash_id) " +
            "AND NOT EXISTS(SELECT 1 FROM projected_counts " +
            "WHERE projected_counts.hash_id=$hash_id);" +
            "DELETE FROM hashes WHERE hash_id=$hash_id AND NOT EXISTS(" +
            "SELECT 1 FROM active WHERE active.hash_id=$hash_id) AND NOT EXISTS(" +
            "SELECT 1 FROM projected_counts WHERE projected_counts.hash_id=$hash_id) " +
            "AND NOT EXISTS(SELECT 1 FROM dirty_hashes " +
            "WHERE dirty_hashes.hash_id=$hash_id) AND NOT EXISTS(" +
            "SELECT 1 FROM deferred_hashes WHERE deferred_hashes.hash_id=$hash_id)";
        var cleanupAcknowledgedHash =
            cleanupAcknowledged.Parameters.Add("$hash_id", SqliteType.Integer);
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
            cleanupAcknowledgedHash.Value = (long)hashId;
            await cleanupAcknowledged.ExecuteNonQueryAsync(cancellationToken);
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
        await using var cleanUnmappedOrphan = connection.CreateCommand();
        cleanUnmappedOrphan.Transaction = transaction;
        // An unmapped hash can be forgotten only when this exact revision has
        // no remaining rolling data. This replaces the old global prune,
        // which could erase a lower-id mutation created after the cursor.
        cleanUnmappedOrphan.CommandText =
            "DELETE FROM dirty_hashes WHERE hash_id=$hash_id AND revision=$revision " +
            "AND NOT EXISTS(SELECT 1 FROM active WHERE active.hash_id=$hash_id) " +
            "AND NOT EXISTS(SELECT 1 FROM projected_counts " +
            "WHERE projected_counts.hash_id=$hash_id)";
        var cleanUnmappedHash =
            cleanUnmappedOrphan.Parameters.Add("$hash_id", SqliteType.Integer);
        var cleanUnmappedRevision =
            cleanUnmappedOrphan.Parameters.Add("$revision", SqliteType.Integer);
        foreach (var row in unmapped)
        {
            hashValue.Value = row.InfoHash;
            var hashId = await findHash.ExecuteScalarAsync(cancellationToken);
            if (hashId is null) continue;
            cleanUnmappedHash.Value = (long)hashId;
            cleanUnmappedRevision.Value = row.Revision;
            await cleanUnmappedOrphan.ExecuteNonQueryAsync(cancellationToken);
            deferHash.Value = (long)hashId;
            deferRevision.Value = row.Revision;
            await defer.ExecuteNonQueryAsync(cancellationToken);
            cleanupAcknowledgedHash.Value = (long)hashId;
            await cleanupAcknowledged.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Records completion only after the caller has traversed every page for
    /// this target. Page commits never perform global work or advance state.
    /// </summary>
    public async Task FinalizeProjectionAsync(
        long targetHour,
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            // Checkpoint before advancing state. If truncation is busy/fails,
            // projected_hour stays old and the next pass retries finalization.
            // The following state write may leave only its non-actor WAL frame.
            await CheckpointAndTruncateWalAsync(connection, cancellationToken);
            await using (var state = connection.CreateCommand())
            {
                state.CommandText =
                    "INSERT INTO projection_state(singleton,projected_hour) VALUES(1,$hour) " +
                    "ON CONFLICT(singleton) DO UPDATE SET projected_hour=" +
                    "MAX(projection_state.projected_hour,excluded.projected_hour)";
                state.Parameters.AddWithValue("$hour", targetHour);
                await state.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            _writeGate.Release();
        }
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
        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenAsync(cancellationToken);
            await RunPrivacyMaintenanceAsync(targetHour, force, connection, cancellationToken);
        }
        finally
        {
            _writeGate.Release();
        }
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
            CREATE TABLE IF NOT EXISTS rolling_ingest_control (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1)
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
                AFTER INSERT ON active
                WHEN NOT EXISTS(SELECT 1 FROM rolling_ingest_control WHERE singleton=1)
                BEGIN
                    INSERT INTO dirty_hashes(hash_id,revision) VALUES(new.hash_id,1)
                    ON CONFLICT(hash_id) DO UPDATE SET revision=dirty_hashes.revision+1;
                    DELETE FROM deferred_hashes
                     WHERE hash_id=new.hash_id AND EXISTS(
                        SELECT 1 FROM projection_state state
                         WHERE new.last_seen_hour BETWEEN state.projected_hour-23
                                                      AND state.projected_hour);
                END;
                CREATE TRIGGER trg_heat_rolling_active_update
                AFTER UPDATE OF last_seen_hour,previous_seen_hour ON active
                WHEN NOT EXISTS(SELECT 1 FROM rolling_ingest_control WHERE singleton=1)
                BEGIN
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
