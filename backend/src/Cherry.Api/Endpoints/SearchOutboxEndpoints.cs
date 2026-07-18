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

        group.MapPost("/recover-empty-index", RecoverEmptyIndexAsync)
            .WithName("RecoverEmptySearchIndex")
            .WithSummary("Delete and recreate Meili, then atomically queue metadata and heat recovery")
            .Produces(StatusCodes.Status202Accepted)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status503ServiceUnavailable);
    }

    private static async Task<IResult> GetStatsAsync(
        SearchOutboxStore store,
        SearchOutboxMetrics metrics,
        IServiceProvider services,
        HttpRequest request,
        CancellationToken cancellationToken)
    {
        var backlog = await store.GetBacklogAsync(cancellationToken);
        var recovery = services.GetService<SearchRecoveryService>();
        var verifyDocuments = bool.TryParse(request.Query["verifyDocuments"], out var requested) && requested;
        var recoveryStatus = recovery is null
            ? null
            : await recovery.GetStatusAsync(verifyDocuments, cancellationToken);
        return Results.Ok(new
        {
            backlog,
            worker = metrics.Snapshot(),
            recovery = recoveryStatus,
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

    private static async Task<IResult> RecoverEmptyIndexAsync(
        SearchRecoveryRequest? request,
        IServiceProvider services,
        ILogger<SearchRecoveryService> logger,
        CancellationToken cancellationToken)
    {
        var recovery = services.GetService<SearchRecoveryService>();
        if (recovery is null)
            return Results.Json(new { error = "Meilisearch is not configured" }, statusCode: 503);
        try
        {
            var result = await recovery.RecoverAsync(request?.Confirmation ?? string.Empty, cancellationToken);
            return Results.Accepted(value: result);
        }
        catch (SearchRecoveryConfirmationException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
        catch (Exception exception) when (
            exception is HttpRequestException or InvalidDataException or TimeoutException or InvalidOperationException)
        {
            logger.LogWarning(exception, "Meilisearch clean recovery failed");
            return Results.Json(
                new { error = "Search index recovery is temporarily unavailable" },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "Meilisearch clean recovery timed out");
            return Results.Json(
                new { error = "Search index recovery is temporarily unavailable" },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    }
}

public sealed record SearchRecoveryRequest(string Confirmation);
