using Cherry.Infrastructure.Search;

namespace Cherry.Api.Endpoints;

public static class SearchOutboxEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/v1/search/outbox")
            .WithTags("Search operations");

        group.MapGet("/stats", GetStatsAsync)
            .WithName("GetSearchOutboxStats")
            .WithSummary("Get durable search projection backlog and worker metrics");

        group.MapPost("/rebuild", RebuildAsync)
            .WithName("RebuildSearchOutbox")
            .WithSummary("Re-enqueue every authoritative torrent for search projection");
    }

    private static async Task<IResult> GetStatsAsync(
        SearchOutboxStore store,
        SearchOutboxMetrics metrics,
        CancellationToken cancellationToken)
    {
        var backlog = await store.GetBacklogAsync(cancellationToken);
        return Results.Ok(new
        {
            backlog,
            worker = metrics.Snapshot(),
            measured_at = DateTime.UtcNow
        });
    }

    private static async Task<IResult> RebuildAsync(
        SearchOutboxStore store,
        CancellationToken cancellationToken)
    {
        var enqueued = await store.RebuildAsync(cancellationToken);
        return Results.Accepted(value: new { enqueued });
    }
}
