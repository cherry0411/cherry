using System.Text;
using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Cherry.Api.Endpoints;

public static class TorrentEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/v1/torrents")
            .WithTags("Torrents");

        group.MapPost("/batch", IngestBatchAsync)
            .WithName("IngestBatch")
            .WithSummary("批量接收爬虫上报的种子元数据")
            .WithDescription("Receive torrent metadata batch from crawlers. Each event contains info_hash and metadata including file list.")
            .Produces<BatchIngestResponse>(200)
            .Produces(400);

        group.MapPost("/upload", UploadTorrentAsync)
            .WithName("UploadTorrent")
            .WithSummary("上传.torrent文件手动入库")
            .WithDescription("Upload a .torrent file, parse metadata, and save to the database.")
            .Produces<TorrentDto>(200)
            .Produces(400)
            .DisableAntiforgery();

        group.MapPost("/request", RequestTorrentAsync)
            .WithName("RequestTorrent")
            .WithSummary("提交infohash到DHT抓取队列")
            .WithDescription("Post {info_hash} to queue for DHT metadata fetch.");

        group.MapGet("/pending", GetPendingRequestsAsync)
            .WithName("GetPendingRequests")
            .WithSummary("获取待抓取的infohash列表")
            .WithDescription("Returns up to 10 pending infohashes for crawlers to fetch.");

        group.MapPost("/decay-peers", DecayPeerCountsAsync)
            .WithName("DecayPeerCounts")
            .WithSummary("衰减过期peer计数")
            .WithDescription("Halves peer_count for torrents not updated in 7 days.")
            .Produces(200);

        group.MapPost("/peers", UpdatePeerCountsAsync)
            .WithName("UpdatePeerCounts")
            .WithSummary("批量更新peer计数")
            .WithDescription("POST {hashes: {info_hash: count, ...}} — batch update peer counts for ranking.")
            .Produces(200);

        group.MapGet("/check", CheckExistsAsync)
            .WithName("CheckExists")
            .WithSummary("批量检查info_hash是否存在")
            .WithDescription("Given ?hashes=a1,b2,c3, returns which hashes already exist in the database.")
            .Produces<List<string>>(200)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(5)));

        group.MapGet("/recent", GetRecentAsync)
            .WithName("GetRecentTorrents")
            .WithSummary("获取最新种子")
            .WithDescription("Get the most recently discovered torrents.")
            .Produces<List<TorrentDto>>(200)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(30)));

        group.MapGet("/search", SearchAsync)
            .WithName("SearchTorrents")
            .WithSummary("搜索种子")
            .WithDescription("Search torrents by name using trigram fuzzy matching. Supports pagination and file type filter.")
            .Produces<SearchResponse>(200)
            .Produces(400)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(15)));

        group.MapGet("/{infoHash:regex(^[a-f0-9]{{40}}$)}", GetDetailAsync)
            .WithName("GetTorrentDetail")
            .WithSummary("获取种子详情（含文件列表）")
            .WithDescription("Get torrent detail by info_hash (40-char hex string). Returns full metadata including file list and magnet link.")
            .Produces<TorrentDto>(200)
            .Produces(404)
            .Produces(400)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(60)));
    }

    private static async Task<IResult> UploadTorrentAsync(
        HttpRequest request,
        ITorrentRepository repo,
        IDedupFilter dedup,
        CancellationToken ct)
    {
        if (!request.HasFormContentType || request.Form.Files.Count == 0)
            return Results.BadRequest("Upload a .torrent file");

        var file = request.Form.Files[0];
        using var ms = new MemoryStream();
        await file.CopyToAsync(ms, ct);
        var data = ms.ToArray();
        var info = ParseTorrentInfo(data);
        if (info is null)
            return Results.BadRequest("Invalid torrent file");

        var infoHash = ComputeInfoHash(data);
        if (!dedup.Add(infoHash))
            return Results.Ok(new { info_hash = infoHash, status = "duplicate" });

        var name = info.GetValueOrDefault("name.utf-8") as byte[] ?? info.GetValueOrDefault("name") as byte[];
        if (name is null) return Results.BadRequest("No name in torrent");
        var nameStr = Encoding.UTF8.GetString(name).Trim();
        var length = Convert.ToInt64(info.GetValueOrDefault("length", 0L));
        var pieceLength = Convert.ToInt64(info.GetValueOrDefault("piece length", 0L));
        var files = new List<TorrentFile>();
        var totalLen = length;

        if (info.TryGetValue("files", out var fileList) && fileList is List<object> fl)
        {
            totalLen = 0;
            foreach (var fobj in fl)
            {
                if (fobj is not Dictionary<string, object> fd) continue;
                var fbytes = fd.GetValueOrDefault("path.utf-8") as List<object>
                    ?? fd.GetValueOrDefault("path") as List<object>;
                if (fbytes is null) continue;
                var fpath = string.Join("/", fbytes.Select(p => Encoding.UTF8.GetString((byte[])p)));
                var flen = Convert.ToInt64(fd.GetValueOrDefault("length", 0L));
                if (string.IsNullOrEmpty(fpath) || fpath.Contains("_____padding_file_")) continue;
                files.Add(new TorrentFile { PathText = fpath, Length = flen });
                totalLen += flen;
            }
        }
        else if (length > 0)
        {
            files.Add(new TorrentFile { PathText = nameStr, Length = length });
        }

        if (files.Count == 0 || string.IsNullOrEmpty(nameStr))
            return Results.BadRequest("No valid files in torrent");

        var torrents = new List<Torrent>
        {
            new()
            {
                InfoHash = infoHash,
                Name = nameStr,
                PieceLength = (int)pieceLength,
                TotalLength = totalLen,
                FileCount = files.Count,
                Source = "upload",
                Files = files
            }
        };
        await repo.BulkInsertTorrentsAsync(torrents, ct);
        return Results.Ok(new { info_hash = infoHash, name = nameStr, files = files.Count, status = "added" });
    }

    private static string ComputeInfoHash(byte[] torrentData)
    {
        var infoStart = FindInfoDict(torrentData);
        var infoBytes = torrentData[infoStart..];
        var raw = infoBytes[1..^1]; // strip 'd' and 'e'
        return Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(raw)).ToLowerInvariant();
    }

    private static int FindInfoDict(byte[] data)
    {
        for (var i = 0; i < data.Length - 4; i++)
            if (data[i] == '4' && data[i + 1] == ':' && data[i + 2] == 'i' && data[i + 3] == 'n' && data[i + 4] == 'f' && data[i + 5] == 'o')
                return i + 6;
        return -1;
    }

    private static Dictionary<string, object>? ParseTorrentInfo(byte[] data)
    {
        var idx = FindInfoDict(data);
        if (idx < 0) return null;
        var result = BDecode(data, idx, out _);
        return result as Dictionary<string, object>;
    }

    private static object? BDecode(byte[] data, int pos, out int end)
    {
        end = pos;
        if (pos >= data.Length) return null;
        return data[pos] switch
        {
            (byte)'d' => ParseDict(data, pos, out end),
            (byte)'l' => ParseList(data, pos, out end),
            (byte)'i' => ParseInt(data, pos, out end),
            >= (byte)'0' and <= (byte)'9' => ParseString(data, pos, out end),
            _ => null
        };
    }

    private static Dictionary<string, object>? ParseDict(byte[] data, int pos, out int end)
    {
        var dict = new Dictionary<string, object>();
        pos++; // skip 'd'
        while (pos < data.Length && data[pos] != 'e')
        {
            var key = ParseString(data, pos, out pos);
            if (key is null) break;
            var val = BDecode(data, pos, out pos);
            if (val is not null) dict[Encoding.UTF8.GetString(key)] = val;
        }
        end = pos < data.Length ? pos + 1 : pos; // skip 'e'
        return dict;
    }

    private static List<object>? ParseList(byte[] data, int pos, out int end)
    {
        var list = new List<object>();
        pos++;
        while (pos < data.Length && data[pos] != 'e')
        {
            var val = BDecode(data, pos, out pos);
            if (val is not null) list.Add(val);
        }
        end = pos < data.Length ? pos + 1 : pos;
        return list;
    }

    private static long? ParseInt(byte[] data, int pos, out int end)
    {
        pos++; // skip 'i'
        end = Array.IndexOf(data, (byte)'e', pos);
        if (end < 0) return null;
        var str = Encoding.ASCII.GetString(data, pos, end - pos);
        end++;
        return long.TryParse(str, out var v) ? v : null;
    }

    private static byte[]? ParseString(byte[] data, int pos, out int end)
    {
        end = pos;
        var colon = Array.IndexOf(data, (byte)':', pos);
        if (colon < 0) return null;
        if (!int.TryParse(Encoding.ASCII.GetString(data, pos, colon - pos), out var len)) return null;
        end = colon + 1 + len;
        if (end > data.Length) return null;
        return data[(colon + 1)..end];
    }

    private static async Task<IResult> RequestTorrentAsync(
        TorrentRequestDto dto,
        AppDbContext db,
        CancellationToken ct)
    {
        var hash = dto.InfoHash.ToLowerInvariant().Trim();
        if (hash.Length != 40 || !hash.All(c => c is >= 'a' and <= 'f' or >= '0' and <= '9'))
            return Results.BadRequest("Invalid info_hash");

        if (db.TorrentRequests.Any(r => r.InfoHash == hash && r.Status == "pending"))
            return Results.Ok(new { status = "already_pending" });

        db.TorrentRequests.Add(new TorrentRequest { InfoHash = hash });
        await db.SaveChangesAsync(ct);
        return Results.Ok(new { status = "queued" });
    }

    private static async Task<IResult> GetPendingRequestsAsync(
        AppDbContext db,
        CancellationToken ct)
    {
        var pending = await db.TorrentRequests
            .Where(r => r.Status == "pending")
            .OrderBy(r => r.CreatedAt)
            .Take(10)
            .ToListAsync(ct);
        return Results.Ok(pending.Select(r => r.InfoHash).ToList());
    }

    private static async Task<IResult> DecayPeerCountsAsync(
        ITorrentRepository repo,
        CancellationToken ct)
    {
        await repo.DecayPeerCountsAsync(ct);
        return Results.Ok(new { decayed = true });
    }

    private static async Task<IResult> UpdatePeerCountsAsync(
        Dictionary<string, int> hashes,
        ITorrentRepository repo,
        CancellationToken ct)
    {
        if (hashes.Count == 0) return Results.Ok();
        await repo.BatchUpdatePeerCountsAsync(hashes, ct);
        return Results.Ok();
    }

    private static IResult CheckExistsAsync(
        HttpContext http,
        IDedupFilter dedup)
    {
        var hashesParam = http.Request.Query["hashes"].ToString();
        if (string.IsNullOrWhiteSpace(hashesParam))
            return Results.BadRequest("?hashes=a1,b2,c3 required");

        var existing = hashesParam.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(h => h.ToLowerInvariant())
            .Where(h => h.Length == 40 && dedup.MightContain(h))
            .ToList();

        return Results.Ok(existing);
    }

    private static async Task<IResult> GetRecentAsync(
        SearchService searchService,
        CancellationToken ct)
    {
        var items = await searchService.GetRecentAsync(ct);
        return Results.Ok(items);
    }

    private static async Task<IResult> IngestBatchAsync(
        [FromBody] BatchIngestRequest request,
        IngestService ingestService,
        CancellationToken ct)
    {
        if (request.Events.Count == 0)
            return Results.BadRequest(new { error = "No events provided" });

        var result = await ingestService.SubmitBatchAsync(request, ct);
        return Results.Ok(result);
    }

    private static async Task<IResult> SearchAsync(
        [FromQuery] string q,
        [FromQuery] int page,
        [FromQuery] int size,
        [FromQuery] string? fileType,
        SearchService searchService,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(q))
            return Results.BadRequest(new { error = "Query parameter 'q' is required" });

        page = Math.Max(1, page);
        size = Math.Clamp(size == 0 ? 20 : size, 1, 100);

        var result = await searchService.SearchAsync(
            new SearchRequest(q, page, size, fileType), ct);
        return Results.Ok(result);
    }

    private static async Task<IResult> GetDetailAsync(
        string infoHash,
        SearchService searchService,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(infoHash) || infoHash.Length != 40)
            return Results.BadRequest(new { error = "Invalid info_hash. Must be 40-char hex string." });

        var result = await searchService.GetDetailAsync(infoHash, ct);
        return result is null ? Results.NotFound() : Results.Ok(result);
    }
}
