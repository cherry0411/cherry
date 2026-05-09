using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Repositories;

public class TorrentRepository : ITorrentRepository
{
    private readonly AppDbContext _db;
    private readonly MeiliIndexQueue? _meiliQueue;
    private readonly MeiliSearchClient? _meiliClient;

    public TorrentRepository(AppDbContext db, MeiliIndexQueue? meiliQueue = null, MeiliSearchClient? meiliClient = null)
    {
        _db = db;
        _meiliQueue = meiliQueue;
        _meiliClient = meiliClient;
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

        // Use a transaction so torrents and their files are inserted atomically.
        await using var tx = await conn.BeginTransactionAsync(ct);
        HashSet<string> insertedHashes;
        try
        {
            // Step 1: INSERT torrents via unnest arrays ¡ª no temp table needed.
            insertedHashes = await InsertTorrentsAsync(unique, conn, tx, ct);

            // Step 2: INSERT files for successfully inserted torrents.
            var files = new List<TorrentFile>();
            foreach (var t in unique)
            {
                if (!insertedHashes.Contains(t.InfoHash)) continue;
                foreach (var f in t.Files)
                {
                    f.InfoHash = t.InfoHash;
                    files.Add(f);
                }
            }

            if (files.Count > 0)
                await CopyFilesAsync(files, conn, tx, ct);

            await tx.CommitAsync(ct);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }

        if (_meiliQueue != null && insertedHashes.Count > 0)
        {
            var indexDocs = unique.Where(t => insertedHashes.Contains(t.InfoHash)).ToList();
            _meiliQueue.Enqueue(indexDocs);
        }

        return insertedHashes.Count;
    }

    private static async Task<HashSet<string>> InsertTorrentsAsync(
        List<Torrent> torrents, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO torrents (info_hash, name, piece_length, total_length, file_count, is_private, source)
            SELECT * FROM unnest(
                @hashes::varchar[],
                @names::text[],
                @pieceLengths::int[],
                @totalLengths::bigint[],
                @fileCounts::int[],
                @isPrivates::bool[],
                @sources::varchar[]
            ) AS t(info_hash, name, piece_length, total_length, file_count, is_private, source)
            ON CONFLICT (info_hash) DO NOTHING
            RETURNING info_hash
            """, conn, tx);

        cmd.Parameters.AddWithValue("hashes",      torrents.Select(t => t.InfoHash).ToArray());
        cmd.Parameters.AddWithValue("names",        torrents.Select(t => t.Name).ToArray());
        cmd.Parameters.AddWithValue("pieceLengths", torrents.Select(t => t.PieceLength).ToArray());
        cmd.Parameters.AddWithValue("totalLengths", torrents.Select(t => t.TotalLength).ToArray());
        cmd.Parameters.AddWithValue("fileCounts",   torrents.Select(t => t.FileCount).ToArray());
        cmd.Parameters.AddWithValue("isPrivates",   torrents.Select(t => t.IsPrivate).ToArray());
        // Use explicit NpgsqlDbType so Npgsql correctly maps nullable varchar[] without type ambiguity.
        cmd.Parameters.Add(new NpgsqlParameter<string?[]>("sources", NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = torrents.Select(t => t.Source).ToArray()
        });

        var result = new HashSet<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(reader.GetString(0));
        return result;
    }

    private static async Task CopyFilesAsync(
        List<TorrentFile> files, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY torrent_files (info_hash, path_text, length) FROM STDIN (FORMAT BINARY)",
            ct);
        // Npgsql binary COPY is implicitly part of the current transaction on this connection.

        foreach (var f in files)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(f.InfoHash, ct);
            await writer.WriteAsync(f.PathText, ct);
            await writer.WriteAsync(f.Length, ct);
        }

        await writer.CompleteAsync(ct);
    }

    public async Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default)
    {
        var torrent = await _db.Torrents
            .AsNoTracking()
            .FirstOrDefaultAsync(t => t.InfoHash == infoHash, ct);

        if (torrent != null)
            torrent.Files = await _db.TorrentFiles
                .AsNoTracking()
                .Where(f => f.InfoHash == infoHash)
                .ToListAsync(ct);

        return torrent;
    }

    public async Task<(List<Torrent> Items, long Total)> SearchAsync(
        string query, int page, int pageSize, CancellationToken ct = default)
    {
        if (_meiliClient == null)
            return ([], 0);

        var result = await _meiliClient.SearchAsync(query, page, pageSize, ct);
        if (result is not { Hits.Count: > 0 })
            return ([], result?.EstimatedTotalHits ?? 0);

        var hashes = result.Hits.Select(h => h.InfoHash).ToList();
        var dbItems = await _db.Torrents
            .AsNoTracking()
            .Where(t => hashes.Contains(t.InfoHash))
            .ToListAsync(ct);

        // O(n) dict-based reorder to preserve Meilisearch ranking.
        var byHash = dbItems.ToDictionary(t => t.InfoHash);
        var ordered = hashes
            .Select(h => byHash.GetValueOrDefault(h))
            .Where(t => t != null)
            .Cast<Torrent>()
            .ToList();

        return (ordered, result.EstimatedTotalHits);
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

        // unnest-based UPDATE: no temp table, no extra round-trips, single statement.
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE torrents
               SET peer_count      = torrents.peer_count + t.cnt,
                   peer_updated_at = NOW()
              FROM unnest(@hashes::varchar[], @counts::int[]) AS t(hash, cnt)
             WHERE torrents.info_hash = t.hash
            """, conn);

        cmd.Parameters.AddWithValue("hashes", counts.Keys.ToArray());
        cmd.Parameters.AddWithValue("counts", counts.Values.ToArray());
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default)
    {
        if (hashes.Count == 0) return [];
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


