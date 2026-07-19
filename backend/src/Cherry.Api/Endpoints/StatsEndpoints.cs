using Cherry.Application.Dtos;
using Cherry.Application.Services;

namespace Cherry.Api.Endpoints;

public static class StatsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/v1")
            .WithTags("Stats");

        group.MapGet("/stats", GetStatsAsync)
            .WithName("GetStats")
            .WithSummary("获取系统统计信息")
            .WithDescription(
                "Get the PostgreSQL catalog estimate, exact durable-ingest counters " +
                "per crawler epoch, today's indexed catalog count, dedup filter size, and server time. " +
                "Delivered means records in committed wire batches; accepted means a first-written " +
                "torrent or policy decision; metadataCommitted counts only new torrent rows.")
            .Produces<StatsResponse>(200)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(10)));
    }

    private static async Task<IResult> GetStatsAsync(StatsService statsService, CancellationToken ct)
    {
        var result = await statsService.GetStatsAsync(ct);
        return Results.Ok(result);
    }
}
