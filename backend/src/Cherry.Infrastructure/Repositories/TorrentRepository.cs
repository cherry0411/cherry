using System.Runtime.CompilerServices;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Heat;
using Cherry.Infrastructure.Search;
using Cherry.Infrastructure.Storage;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Repositories;

public class TorrentRepository : ITorrentRepository
{
    private readonly AppDbContext _db;
    private readonly MeiliSearchClient? _meiliClient;
    private readonly IProcessedHashFilter? _processedHashFilter;
    private readonly HeatOptions _heatOptions;
    private readonly HeatProjectionStatusCache _heatStatusCache;

    public TorrentRepository(
        AppDbContext db,
        MeiliSearchClient? meiliClient = null,
        IProcessedHashFilter? processedHashFilter = null,
        HeatOptions? heatOptions = null,
        HeatProjectionStatusCache? heatStatusCache = null)
    {
        _db = db;
        _meiliClient = meiliClient;
        _processedHashFilter = processedHashFilter;
        _heatOptions = heatOptions ?? new HeatOptions();
        _heatStatusCache = heatStatusCache ?? new HeatProjectionStatusCache();
    }

    public async Task<IReadOnlySet<string>> BulkInsertTorrentsAsync(
        List<Torrent> torrents,
        CancellationToken ct = default)
    {
        if (torrents.Count == 0) return new HashSet<string>();

        // In-memory dedup within the batch
        var seen = new HashSet<string>();
        var unique = new List<Torrent>();
        foreach (var t in torrents)
        {
            if (seen.Add(t.InfoHash))
                unique.Add(t);
        }
        if (unique.Count == 0) return new HashSet<string>();

        // Warm the probabilistic superset before the exact commit. A rollback
        // can only create harmless false positives; warming after commit would
        // create a race where a DB row exists while the filter still says no.
        _processedHashFilter?.RecordCandidates(unique.Select(t => t.InfoHash));

        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        // Use a transaction so catalog rows and compact detail are inserted atomically.
        await using var tx = await conn.BeginTransactionAsync(ct);
        Dictionary<string, long> insertedTorrents;
        try
        {
            await ExactHashTransactionLock.AcquireAsync(
                unique.Select(torrent => torrent.InfoHash),
                conn,
                tx,
                ct);

            // Step 1: INSERT torrents via unnest arrays �� no temp table needed.
            insertedTorrents = await InsertTorrentsAsync(unique, conn, tx, ct);
            await ExactHashTransactionLock.DeleteDecisionsForTorrentsAsync(
                unique.Select(torrent => torrent.InfoHash).ToArray(),
                conn,
                tx,
                ct);

            // Step 2: one versioned detail payload for each inserted torrent.
            var details = new List<TorrentDetail>(insertedTorrents.Count);
            foreach (var t in unique)
            {
                if (!insertedTorrents.TryGetValue(t.InfoHash, out var torrentId)) continue;
                details.Add(new TorrentDetail
                {
                    TorrentId = torrentId,
                    Payload = TorrentDetailCodec.Encode(t.Files, t.ExtensionSummaries)
                });
            }

            if (details.Count > 0)
                await CopyDetailsAsync(details, conn, ct);

            // The marker commits atomically with authoritative metadata. The
            // crawler/API response never waits for Meilisearch itself.
            await SearchOutboxWriter.EnqueueAsync(insertedTorrents.Values, conn, tx, ct);

            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }

        return insertedTorrents.Keys.ToHashSet(StringComparer.Ordinal);
    }

    public async Task<IReadOnlySet<string>> AddRejectedHashesAsync(
        IReadOnlyCollection<string> infoHashes,
        CancellationToken ct = default)
    {
        if (infoHashes.Count == 0)
            return new HashSet<string>();

        var unique = infoHashes.Distinct(StringComparer.Ordinal).ToArray();
        _processedHashFilter?.RecordCandidates(unique);

        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await ExactHashTransactionLock.AcquireAsync(unique, conn, tx, ct);
            await using var cmd = new NpgsqlCommand(
                """
                INSERT INTO metadata_decisions (info_hash, decision_code)
                SELECT decode(hash, 'hex'), @decision_code
                  FROM unnest(@hashes::text[]) AS incoming(hash)
                 WHERE NOT EXISTS (
                           SELECT 1 FROM torrents
                            WHERE info_hash = decode(incoming.hash, 'hex'))
                ON CONFLICT (info_hash) DO NOTHING
                RETURNING encode(info_hash, 'hex')
                """, conn, tx);
            cmd.Parameters.AddWithValue("hashes", unique);
            cmd.Parameters.AddWithValue(
                "decision_code",
                NpgsqlDbType.Smallint,
                (short)MetadataDecisionCode.Reject);

            var inserted = new HashSet<string>(StringComparer.Ordinal);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                inserted.Add(reader.GetString(0));
            await reader.DisposeAsync();
            await tx.CommitAsync(ct);
            return inserted;
        }
        catch
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task<Dictionary<string, long>> InsertTorrentsAsync(
        List<Torrent> torrents, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO torrents (info_hash, name, total_length, file_count, created_at)
            SELECT decode(hash, 'hex'), name, total_length, file_count, created_at
              FROM unnest(
                @hashes::text[],
                @names::text[],
                @totalLengths::bigint[],
                @fileCounts::int[],
                @createdAt::timestamptz[]
            ) AS t(hash, name, total_length, file_count, created_at)
            ON CONFLICT (info_hash) DO NOTHING
            RETURNING id, encode(info_hash, 'hex')
            """, conn, tx);

        cmd.Parameters.AddWithValue("hashes", torrents.Select(t => t.InfoHash).ToArray());
        cmd.Parameters.AddWithValue("names", torrents.Select(t => t.Name).ToArray());
        cmd.Parameters.AddWithValue("totalLengths", torrents.Select(t => t.TotalLength).ToArray());
        cmd.Parameters.AddWithValue("fileCounts", torrents.Select(t => t.FileCount).ToArray());
        cmd.Parameters.AddWithValue(
            "createdAt",
            NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
            torrents.Select(t => t.CreatedAt).ToArray());

        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(reader.GetString(1), reader.GetInt64(0));
        return result;
    }

    private static async Task CopyDetailsAsync(
        List<TorrentDetail> details,
        NpgsqlConnection conn,
        CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY torrent_details (torrent_id, payload) FROM STDIN (FORMAT BINARY)",
            ct);

        foreach (var detail in details)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(detail.TorrentId, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync(detail.Payload, NpgsqlDbType.Bytea, ct);
        }

        await writer.CompleteAsync(ct);
    }

    public async Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default)
    {
        var torrent = await _db.Torrents
            .AsNoTracking()
            .FirstOrDefaultAsync(t => t.InfoHash == infoHash, ct);

        if (torrent != null)
        {
            var payload = await _db.TorrentDetails
                .AsNoTracking()
                .Where(detail => detail.TorrentId == torrent.Id)
                .Select(detail => detail.Payload)
                .SingleOrDefaultAsync(ct);
            if (payload is not null)
            {
                var detail = TorrentDetailCodec.Decode(payload);
                foreach (var file in detail.Files)
                {
                    file.TorrentId = torrent.Id;
                    file.InfoHash = torrent.InfoHash;
                }
                foreach (var extension in detail.ExtensionSummaries)
                {
                    extension.TorrentId = torrent.Id;
                    extension.InfoHash = torrent.InfoHash;
                }
                torrent.Files = detail.Files;
                torrent.ExtensionSummaries = detail.ExtensionSummaries;
            }
        }

        return torrent;
    }

    public async Task<(List<Torrent> Items, long Total, DateOnly? HeatAsOfDay, int HeatCoverageDays)> SearchAsync(
        string query, string heatWindow, int page, int pageSize, CancellationToken ct = default)
    {
        if (_meiliClient == null)
            return ([], 0, null, 0);

        var result = await _meiliClient.SearchAsync(query, heatWindow, page, pageSize, ct);
        if (result.Hits.Count == 0)
        {
            var emptyStatus = await GetHeatStatusAsync(heatWindow, ct);
            return ([], result.EstimatedTotalHits, emptyStatus.Day, emptyStatus.Coverage);
        }

        var ids = result.Hits.Select(h => h.Id).ToList();
        var dbItems = await _db.Torrents
            .AsNoTracking()
            .Where(t => ids.Contains(t.Id))
            .ToListAsync(ct);

        // O(n) dict-based reorder to preserve Meilisearch ranking.
        var byId = dbItems.ToDictionary(t => t.Id);
        var ordered = new List<Torrent>(ids.Count);
        foreach (var hit in result.Hits)
        {
            if (!byId.TryGetValue(hit.Id, out var torrent)) continue;
            torrent.Heat1d = hit.Heat1d;
            torrent.Heat7d = hit.Heat7d;
            torrent.Heat15d = hit.Heat15d;
            torrent.Heat30d = hit.Heat30d;
            ordered.Add(torrent);
        }

        var status = await GetHeatStatusAsync(heatWindow, ct);
        return (ordered, result.EstimatedTotalHits, status.Day, status.Coverage);
    }

    private async Task<(DateOnly? Day, int Coverage)> GetHeatStatusAsync(string heatWindow, CancellationToken ct)
    {
        var windowDays = int.Parse(heatWindow[..^1], System.Globalization.CultureInfo.InvariantCulture);
        var status = await _heatStatusCache.GetAsync(async cancellationToken =>
        {
            var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
            if (connection.State != System.Data.ConnectionState.Open)
                await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(
                """
                SELECT projected_through,coverage_mask
                  FROM heat_projection_watermarks
                 WHERE index_generation=@generation
                """,
                connection);
            command.Parameters.AddWithValue("generation", _heatOptions.IndexGeneration);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await reader.ReadAsync(cancellationToken)
                ? (reader.IsDBNull(0) ? null : reader.GetFieldValue<DateOnly>(0), reader.GetInt32(1))
                : (null, 0);
        }, ct);
        return (status.Day, HeatCoverage.Count(status.CoverageMask, windowDays));
    }

    public async Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default)
    {
        if (hashes.Count == 0) return [];
        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT encode(t.info_hash, 'hex')
              FROM torrents AS t
              JOIN unnest(@hashes::text[]) AS incoming(hash)
                ON t.info_hash = decode(incoming.hash, 'hex')
            """, conn);
        cmd.Parameters.AddWithValue("hashes", hashes.ToArray());
        var existing = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            existing.Add(reader.GetString(0));
        return existing;
    }

    public async Task<List<string>> CheckProcessedAsync(List<string> hashes, CancellationToken ct = default)
    {
        if (hashes.Count == 0)
            return [];

        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            """
            WITH incoming(hash) AS (SELECT unnest(@hashes::text[]))
            SELECT incoming.hash
              FROM incoming
              JOIN torrents AS t
                ON t.info_hash = decode(incoming.hash, 'hex')
            UNION
            SELECT incoming.hash
              FROM incoming
              JOIN metadata_decisions AS d
                ON d.info_hash = decode(incoming.hash, 'hex')
            """, conn);
        cmd.Parameters.AddWithValue("hashes", hashes.ToArray());

        var processed = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            processed.Add(reader.GetString(0));

        return processed;
    }

    public async IAsyncEnumerable<string> StreamProcessedHashesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            """
            SELECT encode(info_hash, 'hex') FROM torrents
            UNION ALL
            SELECT encode(info_hash, 'hex') FROM metadata_decisions
            """, conn);
        await using var reader = await cmd.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess,
            ct);

        while (await reader.ReadAsync(ct))
            yield return reader.GetString(0);
    }

    public async Task<List<Torrent>> GetRecentAsync(int count, CancellationToken ct = default)
    {
        return await _db.Torrents
            .AsNoTracking()
            .OrderByDescending(t => t.CreatedAt)
            .Take(count)
            .ToListAsync(ct);
    }

    public async Task<long> GetTotalCountAsync(CancellationToken ct = default)
    {
        // Use pg_class.reltuples for a fast O(1) estimate instead of a full COUNT(*) seq-scan.
        // Falls back to exact count if the table is brand-new (reltuples = -1 before first ANALYZE).
        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT GREATEST(0, reltuples::bigint) FROM pg_class WHERE relname = 'torrents'", conn);
        var raw = await cmd.ExecuteScalarAsync(ct);
        var estimate = raw is long l ? l : Convert.ToInt64(raw ?? 0L);

        // If pg_class hasn't been analyzed yet, fall back to exact count.
        return estimate > 0 ? estimate : await _db.Torrents.LongCountAsync(ct);
    }

    public async Task<long> GetTodayCountAsync(CancellationToken ct = default)
    {
        var today = DateTime.UtcNow.Date;
        return await _db.Torrents.LongCountAsync(t => t.CreatedAt >= today, ct);
    }

    public async Task MarkRequestsDoneAsync(IEnumerable<string> infoHashes, CancellationToken ct = default)
    {
        var arr = infoHashes.ToArray();
        if (arr.Length == 0) return;

        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "UPDATE torrent_requests SET status = 'done' WHERE status = 'pending' AND info_hash = ANY(@hashes)",
            conn);
        cmd.Parameters.AddWithValue("hashes", arr);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}


