using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Microsoft.AspNetCore.Mvc;

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
