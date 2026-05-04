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
            .WithDescription("Get system statistics: total torrents, today's new count, dedup filter size, server time.")
            .Produces<StatsResponse>(200)
            .CacheOutput(p => p.Expire(TimeSpan.FromSeconds(10)));
    }

    private static async Task<IResult> GetStatsAsync(StatsService statsService, CancellationToken ct)
    {
        var result = await statsService.GetStatsAsync(ct);
        return Results.Ok(result);
    }
}
