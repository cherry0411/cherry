using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Cherry.Infrastructure.Repositories;

public class TorrentRepository : ITorrentRepository
{
    private readonly AppDbContext _db;
    private readonly MeiliSearchClient? _meili;

    public TorrentRepository(AppDbContext db, MeiliSearchClient? meili = null)
    {
        _db = db;
        _meili = meili;
    }

    public async Task<long> BulkInsertTorrentsAsync(List<Torrent> torrents, CancellationToken ct = default)
    {
        if (torrents.Count == 0) return 0;

        // In-memory dedup within the batch
        var seen = new HashSet<string>();
        var unique = new List<Torrent>();
        foreach (var t in torrents)
        {
            if (seen.Add(t.InfoHash))
                unique.Add(t);
        }
        if (unique.Count == 0) return 0;

        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        var tableName = "_ingest_" + Guid.NewGuid().ToString("N");
        await EnsureTempTableAsync(conn, tableName, ct);

        // Step 1: COPY all candidates into temp table
        await CopyToTempAsync(unique, conn, tableName, ct);

        // Step 2: INSERT ... ON CONFLICT DO NOTHING RETURNING id, info_hash
        var hashToId = await InsertFromTempAsync(conn, tableName, ct);

        // Step 3: Build file list for successfully inserted torrents
        var files = new List<TorrentFile>();
        foreach (var t in unique)
        {
            if (hashToId.TryGetValue(t.InfoHash, out var id))
            {
                t.Id = id;
                foreach (var f in t.Files)
                {
                    f.TorrentId = id;
                    files.Add(f);
                }
            }
        }

        if (files.Count > 0)
            await CopyFilesAsync(files, conn, ct);

        await DropTempTableAsync(conn, tableName, ct);

        if (_meili != null && hashToId.Count > 0)
        {
            var indexDocs = unique.Where(t => hashToId.ContainsKey(t.InfoHash)).ToList();
            // Fire-and-forget: don't block the DB batch loop on meili latency
            _ = _meili.IndexDocumentsAsync(indexDocs, CancellationToken.None);
        }

        return hashToId.Count;
    }

    private static async Task DropTempTableAsync(NpgsqlConnection conn, string tableName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {tableName}", conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task EnsureTempTableAsync(NpgsqlConnection conn, string tableName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
            CREATE TEMP TABLE {tableName} (
                info_hash VARCHAR(40) NOT NULL,
                name TEXT NOT NULL,
                piece_length INTEGER NOT NULL,
                total_length BIGINT NOT NULL,
                file_count INTEGER NOT NULL,
                is_private BOOLEAN NOT NULL,
                source VARCHAR(32)
            )
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task CopyToTempAsync(List<Torrent> torrents, NpgsqlConnection conn, string tableName, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            $"COPY {tableName} (info_hash, name, piece_length, total_length, file_count, is_private, source) FROM STDIN (FORMAT BINARY)",
            ct);

        foreach (var t in torrents)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(t.InfoHash, ct);
            await writer.WriteAsync(t.Name, ct);
            await writer.WriteAsync(t.PieceLength, ct);
            await writer.WriteAsync(t.TotalLength, ct);
            await writer.WriteAsync(t.FileCount, ct);
            await writer.WriteAsync(t.IsPrivate, ct);
            await writer.WriteAsync(t.Source ?? (object)DBNull.Value, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task<Dictionary<string, long>> InsertFromTempAsync(NpgsqlConnection conn, string tableName, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
            INSERT INTO torrents (info_hash, name, piece_length, total_length, file_count, is_private, source)
            SELECT info_hash, name, piece_length, total_length, file_count, is_private, source
            FROM {tableName}
            ON CONFLICT (info_hash) DO NOTHING
            RETURNING id, info_hash
            """, conn);

        var result = new Dictionary<string, long>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(1)] = reader.GetInt64(0);
        }
        return result;
    }

    private static async Task CopyFilesAsync(List<TorrentFile> files, NpgsqlConnection conn, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY torrent_files (torrent_id, path_text, length) FROM STDIN (FORMAT BINARY)",
            ct);

        foreach (var f in files)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(f.TorrentId, ct);
            await writer.WriteAsync(f.PathText, ct);
            await writer.WriteAsync(f.Length, ct);
        }

        await writer.CompleteAsync(ct);
    }

    public async Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default)
    {
        return await _db.Torrents
            .Include(t => t.Files)
            .AsNoTracking()
            .FirstOrDefaultAsync(t => t.InfoHash == infoHash, ct);
    }

    public async Task<(List<Torrent> Items, long Total)> SearchAsync(
        string query, int page, int pageSize, string? fileType = null, CancellationToken ct = default)
    {
        // Try Meilisearch first
        if (_meili != null)
        {
            var result = await _meili.SearchAsync(query, page, pageSize, fileType, ct);
            if (result is { Hits.Count: > 0 })
            {
                var hashes = result.Hits.Select(h => h.InfoHash).ToList();
                var dbItems = await _db.Torrents
                    .AsNoTracking()
                    .Where(t => hashes.Contains(t.InfoHash))
                    .ToListAsync(ct);
                // Preserve Meilisearch order
                var ordered = hashes
                    .Select(h => dbItems.FirstOrDefault(t => t.InfoHash == h))
                    .Where(t => t != null)
                    .Cast<Torrent>()
                    .ToList();
                return (ordered, result.EstimatedTotalHits);
            }
        }

        // Fallback: PG trigram
        var baseQuery = _db.Torrents
            .AsNoTracking()
            .Where(t => EF.Functions.TrigramsSimilarityDistance(t.Name, query) < 0.95);

        if (!string.IsNullOrWhiteSpace(fileType))
        {
            var pattern = $".{fileType}";
            baseQuery = baseQuery.Where(t => t.Files.Any(f => f.PathText.EndsWith(pattern)));
        }

        var total = await baseQuery.LongCountAsync(ct);
        var items = await baseQuery
            .OrderByDescending(t => t.PeerCount)
            .ThenByDescending(t => t.CreatedAt)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync(ct);

        return (items, total);
    }

    public async Task DecayPeerCountsAsync(CancellationToken ct = default)
    {
        await _db.Torrents
            .Where(t => t.PeerCount > 0 && t.PeerUpdatedAt < DateTime.UtcNow.AddDays(-7))
            .ExecuteUpdateAsync(s => s.SetProperty(t => t.PeerCount, t => t.PeerCount / 2), ct);
    }

    public async Task BatchUpdatePeerCountsAsync(Dictionary<string, int> counts, CancellationToken ct = default)
    {
        if (counts.Count == 0) return;
        var conn = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync(ct);

        var tableName = "_peer_temp_" + Guid.NewGuid().ToString("N");
        await using (var cmd = new NpgsqlCommand($"CREATE TEMP TABLE {tableName} (info_hash VARCHAR(40), cnt INT)", conn))
            await cmd.ExecuteNonQueryAsync(ct);

        await using (var writer = await conn.BeginBinaryImportAsync($"COPY {tableName} FROM STDIN (FORMAT BINARY)", ct))
        {
            foreach (var (hash, count) in counts)
            {
                await writer.StartRowAsync(ct);
                await writer.WriteAsync(hash, ct);
                await writer.WriteAsync(count, ct);
            }
            await writer.CompleteAsync(ct);
        }

        await using (var cmd = new NpgsqlCommand(
            $"UPDATE torrents SET peer_count = torrents.peer_count + t.cnt, peer_updated_at = NOW() FROM {tableName} t WHERE torrents.info_hash = t.info_hash", conn))
            await cmd.ExecuteNonQueryAsync(ct);

        await using (var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {tableName}", conn))
            await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default)
    {
        if (hashes.Count == 0) return new List<string>();
        return await _db.Torrents
            .AsNoTracking()
            .Where(t => hashes.Contains(t.InfoHash))
            .Select(t => t.InfoHash)
            .ToListAsync(ct);
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
        return await _db.Torrents.LongCountAsync(ct);
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
