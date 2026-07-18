using System.Text;
using System.Text.Json;
using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Repositories;
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

        group.MapPost("/batch/durable", IngestDurableBatchAsync)
            .WithName("IngestDurableBatch")
            .WithSummary("原子接收可重放的爬虫元数据批次")
            .WithDescription("Hashes the raw events JSON bytes and atomically commits normalized/summary metadata, hash/reject decisions, and a per-crawler epoch receipt.")
            .Produces<DurableBatchResponse>(StatusCodes.Status200OK)
            .Produces<DurableBatchResponse>(StatusCodes.Status409Conflict)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status401Unauthorized)
            .Produces(StatusCodes.Status413PayloadTooLarge)
            .Produces(StatusCodes.Status503ServiceUnavailable);

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
            .WithSummary("旧版热度接口兼容占位")
            .WithDescription("Deprecated compatibility endpoint. Legacy peer_count is no longer persisted.")
            .Produces(200);

        group.MapPost("/peers", UpdatePeerCountsAsync)
            .WithName("UpdatePeerCounts")
            .WithSummary("旧版热度接口兼容占位")
            .WithDescription("Deprecated compatibility endpoint. Payload is accepted while identity-aware heat replaces peer_count.")
            .Produces(200);

        group.MapGet("/check", CheckExistsGetAsync)
            .WithName("CheckExistsGet")
            .WithSummary("批量检查info_hash是否存在（GET）")
            .WithDescription("Given ?hashes=a1,b2,c3, returns which hashes already exist in the database.")
            .Produces<List<string>>(200);

        group.MapPost("/check", CheckExistsPostAsync)
            .WithName("CheckExistsPost")
            .WithSummary("批量检查info_hash是否存在（POST）")
            .WithDescription("POST [\"a1\",\"b2\",...], returns which hashes already exist. Preferred over GET for large batches.")
            .Produces<List<string>>(200)
            .Produces(400);

        group.MapPost("/reject", RejectHashesAsync)
            .WithName("RejectHashes")
            .WithSummary("标记infohash为已过滤")
            .WithDescription("POST [\"a1\",\"b2\",...] — marks hashes as rejected by crawler filter rules. " +
                             "Committed to the compact exact rejected-hash store so future /check calls return them as already processed, " +
                             "preventing unnecessary re-crawling.")
            .Produces(200)
            .Produces(400);

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
        var name = info.GetValueOrDefault("name.utf-8") as byte[] ?? info.GetValueOrDefault("name") as byte[];
        if (name is null) return Results.BadRequest("No name in torrent");
        var nameStr = Encoding.UTF8.GetString(name).Trim();
        var length = Convert.ToInt64(info.GetValueOrDefault("length", 0L));
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
                TotalLength = totalLen,
                FileCount = files.Count,
                Files = files
            }
        };
        var inserted = await repo.BulkInsertTorrentsAsync(torrents, ct);
        var status = inserted.Contains(infoHash) ? "added" : "duplicate";
        return Results.Ok(new { info_hash = infoHash, name = nameStr, files = files.Count, status });
    }

    private static string ComputeInfoHash(byte[] torrentData)
    {
        var root = BDecode(torrentData, 0, out _) as Dictionary<string, object>;
        if (root is null || !root.TryGetValue("info", out var info)) return string.Empty;
        var infoBytes = BEncode(info);
        return Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(infoBytes)).ToLowerInvariant();
    }

    private static byte[] BEncode(object value) => value switch
    {
        string s => Encoding.UTF8.GetBytes(s),
        byte[] b => b,
        long l => Encoding.ASCII.GetBytes($"i{l}e"),
        int i => Encoding.ASCII.GetBytes($"i{i}e"),
        List<object> list => BEncodeList(list),
        Dictionary<string, object> dict => BEncodeDict(dict),
        _ => Array.Empty<byte>()
    };

    private static byte[] BEncodeList(List<object> list)
    {
        var parts = new List<byte[]> { new byte[] { (byte)'l' } };
        foreach (var item in list) parts.Add(BEncode(item));
        parts.Add(new byte[] { (byte)'e' });
        return parts.SelectMany(p => p).ToArray();
    }

    private static byte[] BEncodeDict(Dictionary<string, object> dict)
    {
        var parts = new List<byte[]> { new byte[] { (byte)'d' } };
        foreach (var kv in dict.OrderBy(k => Encoding.UTF8.GetBytes(k.Key), ByteArrayComparer.Instance))
        {
            var key = Encoding.UTF8.GetBytes(kv.Key);
            parts.Add(Encoding.ASCII.GetBytes($"{key.Length}:"));
            parts.Add(key);
            parts.Add(BEncode(kv.Value));
        }
        parts.Add(new byte[] { (byte)'e' });
        return parts.SelectMany(p => p).ToArray();
    }

    private class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();
        public int Compare(byte[]? x, byte[]? y)
        {
            if (x is null || y is null) return 0;
            var len = Math.Min(x.Length, y.Length);
            for (var i = 0; i < len; i++) { var c = x[i].CompareTo(y[i]); if (c != 0) return c; }
            return x.Length.CompareTo(y.Length);
        }
    }

    private static Dictionary<string, object>? ParseTorrentInfo(byte[] data)
    {
        var root = BDecode(data, 0, out _) as Dictionary<string, object>;
        if (root is not null && root.TryGetValue("info", out var info) && info is Dictionary<string, object> infoDict)
            return infoDict;
        return null;
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
            var prev = pos;
            var val = BDecode(data, pos, out pos);
            if (val is not null) list.Add(val);
            else if (pos == prev) break;
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
        PendingRequestTracker tracker,
        CancellationToken ct)
    {
        var hash = dto.InfoHash.ToLowerInvariant().Trim();
        if (hash.Length != 40 || !hash.All(c => c is >= 'a' and <= 'f' or >= '0' and <= '9'))
            return Results.BadRequest("Invalid info_hash");

        if (await db.TorrentRequests.AnyAsync(r => r.InfoHash == hash && r.Status == "pending", ct))
            return Results.Ok(new { status = "already_pending" });

        if (await db.Torrents.AnyAsync(t => t.InfoHash == hash, ct))
            return Results.Ok(new { status = "already_exists" });

        db.TorrentRequests.Add(new TorrentRequest { InfoHash = hash });
        await db.SaveChangesAsync(ct);
        tracker.Track(hash);
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

    private static IResult DecayPeerCountsAsync() =>
        Results.Ok(new { decayed = false, deprecated = true });

    private static IResult UpdatePeerCountsAsync(
        [FromBody] PeerCountsRequest? request)
    {
        var hashes = request?.Hashes
            .Where(kv =>
                kv.Value > 0 &&
                kv.Key.Length == 40 &&
                kv.Key.All(c => c is >= 'a' and <= 'f' or >= '0' and <= '9'))
            .ToDictionary(kv => kv.Key.ToLowerInvariant(), kv => kv.Value) ?? [];

        return Results.Ok(new { accepted = hashes.Count, persisted = false, deprecated = true });
    }

    private static async Task<IResult> CheckExistsGetAsync(
        HttpContext http,
        SearchService searchService,
        CancellationToken ct)
    {
        var hashesParam = http.Request.Query["hashes"].ToString();
        if (string.IsNullOrWhiteSpace(hashesParam))
            return Results.BadRequest("?hashes=a1,b2,c3 required");

        var candidates = hashesParam
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(h => h.ToLowerInvariant())
            .Where(h => h.Length == 40 && h.All(c => c is >= 'a' and <= 'f' or >= '0' and <= '9'))
            .Distinct()
            .ToList();

        if (candidates.Count == 0)
            return Results.Ok(Array.Empty<string>());

        var existing = await searchService.CheckExistsAsync(candidates, ct);
        return Results.Ok(existing);
    }

    private static async Task<IResult> CheckExistsPostAsync(
        [FromBody] List<string>? hashes,
        SearchService searchService,
        CancellationToken ct)
    {
        if (hashes is null || hashes.Count == 0)
            return Results.Ok(Array.Empty<string>());

        var candidates = hashes
            .Where(hash => hash is not null)
            .Select(hash => hash.ToLowerInvariant())
            .Where(h => h.Length == 40 && h.All(c => c is >= 'a' and <= 'f' or >= '0' and <= '9'))
            .Distinct()
            .ToList();

        if (candidates.Count == 0)
            return Results.Ok(Array.Empty<string>());

        var existing = await searchService.CheckExistsAsync(candidates, ct);
        return Results.Ok(existing);
    }

    private static async Task<IResult> RejectHashesAsync(
        [FromBody] List<string>? hashes,
        ITorrentRepository repository,
        CancellationToken ct)
    {
        if (hashes is null || hashes.Count == 0)
            return Results.Ok();

        var valid = hashes
            .Where(hash => hash is not null)
            .Select(hash => hash.ToLowerInvariant())
            .Where(hash => hash.Length == 40 &&
                           hash.All(character => character is >= 'a' and <= 'f' or >= '0' and <= '9'))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var inserted = await repository.AddRejectedHashesAsync(valid, ct);
        return Results.Ok(new
        {
            accepted = inserted.Count,
            duplicates = valid.Length - inserted.Count
        });
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
        if (request.Events is null || request.Events.Count == 0)
            return Results.BadRequest(new { error = "No events provided" });

        var result = await ingestService.SubmitBatchAsync(request, ct);
        if (result.Backpressure)
            return Results.StatusCode(429);
        return Results.Ok(result);
    }

    private static async Task<IResult> IngestDurableBatchAsync(
        HttpRequest request,
        DurableIngestService ingestService,
        CancellationToken cancellationToken)
    {
        const int maxRequestBytes = 64 * 1024 * 1024;
        if (request.ContentLength > maxRequestBytes)
        {
            return Results.Json(
                new { error = $"Request body exceeds {maxRequestBytes} bytes" },
                statusCode: StatusCodes.Status413PayloadTooLarge);
        }

        try
        {
            var body = await ReadBodyAsync(request, maxRequestBytes, cancellationToken);
            var parsed = DurableBatchPayloadParser.Parse(body);
            var result = await ingestService.IngestAsync(parsed, cancellationToken);
            return result.IsConflict
                ? Results.Json(result.Response, statusCode: StatusCodes.Status409Conflict)
                : Results.Ok(result.Response);
        }
        catch (DurableRequestTooLargeException exception)
        {
            return Results.Json(
                new { error = exception.Message },
                statusCode: StatusCodes.Status413PayloadTooLarge);
        }
        catch (Exception exception) when (
            exception is JsonException or DurableBatchValidationException)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
    }

    private static async Task<byte[]> ReadBodyAsync(
        HttpRequest request,
        int maxBytes,
        CancellationToken cancellationToken)
    {
        var initialCapacity = request.ContentLength is > 0 && request.ContentLength <= maxBytes
            ? checked((int)request.ContentLength.Value)
            : 0;
        using var body = new MemoryStream(initialCapacity);
        var buffer = new byte[64 * 1024];
        while (true)
        {
            var read = await request.Body.ReadAsync(buffer, cancellationToken);
            if (read == 0)
                return body.ToArray();
            if (body.Length + read > maxBytes)
                throw new DurableRequestTooLargeException($"Request body exceeds {maxBytes} bytes");
            await body.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }
    }

    private sealed class DurableRequestTooLargeException(string message) : Exception(message);

    private static async Task<IResult> SearchAsync(
        [FromQuery] string q,
        [FromQuery] int page,
        [FromQuery] int size,
        SearchService searchService,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(q))
            return Results.BadRequest(new { error = "Query parameter 'q' is required" });

        page = Math.Max(1, page);
        size = Math.Clamp(size == 0 ? 20 : size, 1, 100);

        var result = await searchService.SearchAsync(
            new SearchRequest(q, page, size), ct);
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
