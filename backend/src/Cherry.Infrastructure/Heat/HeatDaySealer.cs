using Cherry.Infrastructure.Data;
using System.Buffers.Binary;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Heat;

public sealed class HeatDaySealer
{
    private const int JoinBatchSize = 5000;
    private readonly AppDbContext _db;
    private readonly HeatOptions _options;
    private readonly HeatRuntimeMetrics _metrics;

    public HeatDaySealer(AppDbContext db, HeatOptions options, HeatRuntimeMetrics metrics)
    {
        _db = db;
        _options = options;
        _metrics = metrics;
    }

    public async Task<IReadOnlyList<DateOnly>> MissingDaysAsync(
        DateOnly start,
        DateOnly end,
        int limit,
        CancellationToken cancellationToken)
    {
        if (end < start) return [];
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            SELECT candidate.day::date
              FROM generate_series(@start::date, @end::date, interval '1 day') AS candidate(day)
              LEFT JOIN heat_day_manifests manifest ON manifest.day = candidate.day::date
             WHERE manifest.day IS NULL
             ORDER BY candidate.day
             LIMIT @limit
            """, connection);
        command.Parameters.AddWithValue("start", start);
        command.Parameters.AddWithValue("end", end);
        command.Parameters.AddWithValue("limit", limit);
        var result = new List<DateOnly>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(reader.GetFieldValue<DateOnly>(0));
        return result;
    }

    public async Task SealAsync(DateOnly day, string sqlitePath, CancellationToken cancellationToken)
    {
        if (await VerifyPersistedAsync(day, null, cancellationToken)) return;
        await using var sqlite = await HeatAccumulatorService.OpenAsync(sqlitePath, cancellationToken);
        await using (var checkpoint = sqlite.CreateCommand())
        {
            checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
            await checkpoint.ExecuteNonQueryAsync(cancellationToken);
        }
        var coverageStatus = await DetermineCoverageStatusAsync(sqlite, cancellationToken);
        await PrepareCatalogCountsAsync(sqlite, cancellationToken);

        var pg = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (pg.State != System.Data.ConnectionState.Open)
            await pg.OpenAsync(cancellationToken);
        await MapCatalogIdsAsync(sqlite, pg, cancellationToken);

        var ordered = new List<HeatFrameEntry>();
        await using (var command = sqlite.CreateCommand())
        {
            command.CommandText = "SELECT torrent_id, actor_days FROM catalog_counts ORDER BY torrent_id";
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                ordered.Add(new HeatFrameEntry(reader.GetInt64(0), reader.GetInt64(1)));
        }
        var frames = HeatFrameCodec.Encode(ordered);
        var manifestDigest = HeatFrameCodec.ManifestDigest(day, frames);
        await PersistFramesAsync(pg, day, coverageStatus, frames, manifestDigest, cancellationToken);
        if (!await VerifyPersistedAsync(day, frames, cancellationToken))
            throw new InvalidDataException($"PostgreSQL heat frame verification failed for {day}");
        _metrics.Sealed();
    }

    public static void DeleteAccumulator(string sqlitePath)
    {
        foreach (var path in new[] { sqlitePath, $"{sqlitePath}-wal", $"{sqlitePath}-shm" })
            if (File.Exists(path)) File.Delete(path);
    }

    private static async Task PrepareCatalogCountsAsync(SqliteConnection sqlite, CancellationToken cancellationToken)
    {
        await using var command = sqlite.CreateCommand();
        command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS catalog_counts (
                torrent_id INTEGER PRIMARY KEY,
                actor_days INTEGER NOT NULL CHECK(actor_days > 0)
            ) WITHOUT ROWID;
            DELETE FROM catalog_counts;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task MapCatalogIdsAsync(
        SqliteConnection sqlite,
        NpgsqlConnection pg,
        CancellationToken cancellationToken)
    {
        long afterHashId = 0;
        while (true)
        {
            var hashes = new List<byte[]>(JoinBatchSize);
            var counts = new List<long>(JoinBatchSize);
            long lastHashId = afterHashId;
            await using (var source = sqlite.CreateCommand())
            {
                source.CommandText =
                    "SELECT s.hash_id,h.info_hash,count(*) " +
                    "FROM seen s JOIN hashes h ON h.hash_id=s.hash_id " +
                    "WHERE s.hash_id>$after GROUP BY s.hash_id ORDER BY s.hash_id LIMIT $limit";
                source.Parameters.AddWithValue("$after", afterHashId);
                source.Parameters.AddWithValue("$limit", JoinBatchSize);
                await using var reader = await source.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    lastHashId = reader.GetInt64(0);
                    hashes.Add((byte[])reader[1]);
                    counts.Add(reader.GetInt64(2));
                }
            }
            if (hashes.Count == 0) break;
            await MapBatchAsync(sqlite, pg, hashes, counts, cancellationToken);
            afterHashId = lastHashId;
        }
    }

    private async Task<short> DetermineCoverageStatusAsync(
        SqliteConnection sqlite,
        CancellationToken cancellationToken)
    {
        if (_options.ExpectedCrawlerIds.Length == 0) return 2;
        foreach (var crawlerId in _options.ExpectedCrawlerIds)
            if (!await HasVerifiedCompletionAsync(sqlite, crawlerId, cancellationToken)) return 2;
        return 1;
    }

    public static async Task<bool> HasVerifiedCompletionAsync(
        SqliteConnection sqlite,
        string crawlerId,
        CancellationToken cancellationToken)
    {
        byte[] epoch;
        ulong start;
        ulong next;
        await using (var completion = sqlite.CreateCommand())
        {
            completion.CommandText =
                "SELECT epoch,start_sequence,next_sequence,clean FROM completions WHERE crawler_id=$crawler";
            completion.Parameters.AddWithValue("$crawler", crawlerId);
            await using var reader = await completion.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.GetInt64(3) != 1) return false;
            epoch = (byte[])reader[0];
            start = ReadUInt64((byte[])reader[1]);
            next = ReadUInt64((byte[])reader[2]);
            if (epoch.Length != 8 || start == 0 || next < start || await reader.ReadAsync(cancellationToken))
                return false;
        }

        var receiptCount = 0;
        var expected = start;
        await using (var receipts = sqlite.CreateCommand())
        {
            receipts.CommandText =
                "SELECT epoch,start_sequence,end_sequence FROM receipts " +
                "WHERE crawler_id=$crawler ORDER BY epoch,start_sequence";
            receipts.Parameters.AddWithValue("$crawler", crawlerId);
            await using var reader = await receipts.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var receiptEpoch = (byte[])reader[0];
                var receiptStart = ReadUInt64((byte[])reader[1]);
                var receiptEnd = ReadUInt64((byte[])reader[2]);
                if (!receiptEpoch.AsSpan().SequenceEqual(epoch) || receiptStart != expected ||
                    receiptEnd < receiptStart || receiptEnd == ulong.MaxValue)
                    return false;
                expected = receiptEnd + 1;
                receiptCount++;
            }
        }
        if ((receiptCount == 0 && start != next) || (receiptCount > 0 && expected != next)) return false;

        await using var heads = sqlite.CreateCommand();
        heads.CommandText = "SELECT epoch,next_sequence FROM receipt_heads WHERE crawler_id=$crawler";
        heads.Parameters.AddWithValue("$crawler", crawlerId);
        await using var headReader = await heads.ExecuteReaderAsync(cancellationToken);
        var headCount = 0;
        while (await headReader.ReadAsync(cancellationToken))
        {
            headCount++;
            if (!((byte[])headReader[0]).AsSpan().SequenceEqual(epoch) ||
                ReadUInt64((byte[])headReader[1]) != next) return false;
        }
        return headCount == (receiptCount == 0 ? 0 : 1);
    }

    private static ulong ReadUInt64(byte[] value) =>
        value.Length == 8 ? BinaryPrimitives.ReadUInt64BigEndian(value) : 0;

    private static async Task MapBatchAsync(
        SqliteConnection sqlite,
        NpgsqlConnection pg,
        List<byte[]> hashes,
        List<long> counts,
        CancellationToken cancellationToken)
    {
        await using var query = new NpgsqlCommand(
            """
            SELECT torrent.id, incoming.actor_days
              FROM unnest(@hashes::bytea[], @counts::bigint[]) AS incoming(info_hash, actor_days)
              JOIN torrents torrent ON torrent.info_hash = incoming.info_hash
             ORDER BY torrent.id
            """, pg);
        query.Parameters.AddWithValue("hashes", NpgsqlDbType.Array | NpgsqlDbType.Bytea, hashes.ToArray());
        query.Parameters.AddWithValue("counts", NpgsqlDbType.Array | NpgsqlDbType.Bigint, counts.ToArray());
        var mapped = new List<(long Id, long Count)>();
        await using (var mappedReader = await query.ExecuteReaderAsync(cancellationToken))
            while (await mappedReader.ReadAsync(cancellationToken))
                mapped.Add((mappedReader.GetInt64(0), mappedReader.GetInt64(1)));

        await using var transaction = (SqliteTransaction)await sqlite.BeginTransactionAsync(cancellationToken);
        await using var insert = sqlite.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = "INSERT INTO catalog_counts(torrent_id,actor_days) VALUES($id,$count)";
        var idParameter = insert.Parameters.Add("$id", SqliteType.Integer);
        var countParameter = insert.Parameters.Add("$count", SqliteType.Integer);
        foreach (var row in mapped)
        {
            idParameter.Value = row.Id;
            countParameter.Value = row.Count;
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task PersistFramesAsync(
        NpgsqlConnection connection,
        DateOnly day,
        short coverageStatus,
        IReadOnlyList<EncodedHeatFrame> frames,
        byte[] manifestDigest,
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var manifest = new NpgsqlCommand(
            """
            INSERT INTO heat_day_manifests(
                day,status,coverage_status,codec_version,shard_count,entry_count,manifest_sha256)
            VALUES(@day,1,@coverage,1,64,@entries,@digest)
            ON CONFLICT(day) DO NOTHING
            """, connection, transaction))
        {
            manifest.Parameters.AddWithValue("day", day);
            manifest.Parameters.AddWithValue("coverage", coverageStatus);
            manifest.Parameters.AddWithValue("entries", frames.Sum(frame => (long)frame.EntryCount));
            manifest.Parameters.AddWithValue("digest", manifestDigest);
            await manifest.ExecuteNonQueryAsync(cancellationToken);
        }
        foreach (var frame in frames)
        {
            await using var command = new NpgsqlCommand(
                """
                INSERT INTO heat_day_frames(day,shard,codec_version,entry_count,payload_sha256,payload)
                VALUES(@day,@shard,1,@entries,@digest,@payload)
                ON CONFLICT(day,shard) DO NOTHING
                """, connection, transaction);
            command.Parameters.AddWithValue("day", day);
            command.Parameters.AddWithValue("shard", frame.Shard);
            command.Parameters.AddWithValue("entries", frame.EntryCount);
            command.Parameters.AddWithValue("digest", frame.Sha256);
            command.Parameters.AddWithValue("payload", frame.Payload);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<bool> VerifyPersistedAsync(
        DateOnly day,
        IReadOnlyList<EncodedHeatFrame>? expected,
        CancellationToken cancellationToken)
    {
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            SELECT manifest.status, manifest.coverage_status, manifest.manifest_sha256,
                   manifest.entry_count,
                   frame.shard, frame.entry_count, frame.payload_sha256, frame.payload
              FROM heat_day_manifests manifest
              JOIN heat_day_frames frame ON frame.day=manifest.day
             WHERE manifest.day=@day
             ORDER BY frame.shard
            """, connection);
        command.Parameters.AddWithValue("day", day);
        var rows = new List<EncodedHeatFrame>();
        byte[]? storedManifest = null;
        long? storedEntryCount = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt16(0) != 1 || reader.GetInt16(1) is not (1 or 2)) return false;
            storedManifest ??= (byte[])reader[2];
            storedEntryCount ??= reader.GetInt64(3);
            var payload = (byte[])reader[7];
            var digest = (byte[])reader[6];
            if (!System.Security.Cryptography.SHA256.HashData(payload).AsSpan().SequenceEqual(digest)) return false;
            var shard = reader.GetInt16(4);
            var entryCount = reader.GetInt32(5);
            try
            {
                _ = HeatFrameCodec.Decode(shard, entryCount, payload);
            }
            catch (InvalidDataException)
            {
                return false;
            }
            rows.Add(new EncodedHeatFrame(shard, entryCount, payload, digest));
        }
        if (rows.Count != HeatFrameCodec.ShardCount || storedManifest is null) return false;
        if (storedEntryCount is null || rows.Sum(row => (long)row.EntryCount) != storedEntryCount) return false;
        if (!HeatFrameCodec.ManifestDigest(day, rows).AsSpan().SequenceEqual(storedManifest)) return false;
        if (expected is null) return true;
        return rows.Zip(expected.OrderBy(frame => frame.Shard)).All(pair =>
            pair.First.Shard == pair.Second.Shard &&
            pair.First.EntryCount == pair.Second.EntryCount &&
            pair.First.Sha256.AsSpan().SequenceEqual(pair.Second.Sha256));
    }

}
