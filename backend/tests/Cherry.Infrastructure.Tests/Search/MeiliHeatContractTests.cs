using System.Net;
using System.Text;
using System.Text.Json;
using Cherry.Domain.Entities;
using Cherry.Infrastructure.Heat;
using Cherry.Infrastructure.Search;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Search;

public sealed class MeiliHeatContractTests
{
    [Fact]
    public async Task MetadataAndHeatUsePartialPutAndSearchReturnsThinHeatFields()
    {
        var handler = new RecordingHandler();
        var client = new MeiliSearchClient(new HttpClient(handler) { BaseAddress = new Uri("http://meili") });

        await client.EnsureIndexAsync(CancellationToken.None);
        await client.SubmitDocumentsAsync([new Torrent { Id = 7, Name = "name", CreatedAt = DateTime.UtcNow }],
            CancellationToken.None);
        await client.SubmitDailyHeatDocumentsAsync([new DailyHeatProjectionDocument(7, 2, 3, 4)], "torrents",
            CancellationToken.None);
        await client.SubmitHourlyHeatDocumentsAsync([new HourlyHeatProjectionDocument(7, 1)], "torrents",
            CancellationToken.None);
        await client.SearchAsync("中文", "15d", 1, 20, CancellationToken.None);
        await client.SearchAsync("", "24h", 1, 20, CancellationToken.None);

        Assert.Equal(HttpMethod.Put, handler.Requests[3].Method);
        Assert.DoesNotContain("heat24h", handler.Requests[3].Body);
        Assert.Equal(HttpMethod.Put, handler.Requests[4].Method);
        Assert.DoesNotContain("\"name\"", handler.Requests[4].Body);
        Assert.Contains("\"heat15d\":4", handler.Requests[4].Body);
        Assert.Equal(HttpMethod.Put, handler.Requests[5].Method);
        Assert.Contains("\"heat24h\":1", handler.Requests[5].Body);
        Assert.Contains("\"heat15d:desc\"", handler.Requests[6].Body);
        Assert.Contains("\"heat24h\"", handler.Requests[6].Body);
        using var fullTextRequest = JsonDocument.Parse(handler.Requests[6].Body);
        using var leaderboardRequest = JsonDocument.Parse(handler.Requests[7].Body);
        Assert.Equal("中文", fullTextRequest.RootElement.GetProperty("q").GetString());
        Assert.Equal("", leaderboardRequest.RootElement.GetProperty("q").GetString());
        Assert.Contains("\"heat24h:desc\"", handler.Requests[7].Body);
        // Meilisearch applies explicit sort only after the relevance rules. Thus
        // a non-empty query keeps relevance primary, while an empty query (all
        // relevance scores tied) becomes a heat-first leaderboard.
        Assert.Contains("\"exactness\",\"sort\"", handler.Requests[1].Body);
    }

    [Fact]
    public async Task SearchFailure_IsNotConvertedToAnEmptyResult()
    {
        using var client = new HttpClient(new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = new StringContent("{\"message\":\"index unavailable\"}")
            })))
        {
            BaseAddress = new Uri("http://meili")
        };
        var meili = new MeiliSearchClient(client);

        var exception = await Assert.ThrowsAsync<MeiliSearchUnavailableException>(() =>
            meili.SearchAsync("query", "7d", 1, 20, CancellationToken.None));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, exception.StatusCode);
        Assert.Contains("index unavailable", exception.Message);
    }

    [Fact]
    public async Task ResetIndex_WaitsForDeleteCreateAndSettings_ThenVerifiesEmptyStats()
    {
        var requests = new List<string>();
        long nextTask = 10;
        using var client = new HttpClient(new DelegateHandler(request =>
        {
            requests.Add($"{request.Method} {request.RequestUri!.AbsolutePath}");
            if (request.RequestUri.AbsolutePath.StartsWith("/tasks/", StringComparison.Ordinal))
                return Task.FromResult(Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}"));
            if (request.RequestUri.AbsolutePath == "/indexes/torrents" && request.Method == HttpMethod.Get)
                return Task.FromResult(Json(HttpStatusCode.NotFound, "{}"));
            if (request.RequestUri.AbsolutePath == "/indexes/torrents/stats")
                return Task.FromResult(Json(HttpStatusCode.OK, "{\"numberOfDocuments\":0}"));
            return Task.FromResult(Json(
                HttpStatusCode.Accepted,
                $"{{\"taskUid\":{Interlocked.Increment(ref nextTask)}}}"));
        }))
        {
            BaseAddress = new Uri("http://meili")
        };
        var meili = new MeiliSearchClient(client);

        await meili.ResetIndexAsync(CancellationToken.None);
        Assert.Equal(0, await meili.GetDocumentCountAsync(CancellationToken.None));

        Assert.Equal(
            [
                "DELETE /indexes/torrents",
                "GET /tasks/11",
                "GET /indexes/torrents",
                "POST /indexes",
                "GET /tasks/12",
                "PATCH /indexes/torrents/settings",
                "GET /tasks/13",
                "GET /indexes/torrents/stats"
            ],
            requests);
    }

    [Fact]
    public async Task RecoveryGate_WaitsForActiveProjection_AndBlocksNewProjection()
    {
        var coordinator = new SearchRecoveryCoordinator();
        Assert.Equal(0, coordinator.RecoveryGeneration);
        var activeProjection = await coordinator.EnterProjectionAsync(CancellationToken.None);

        var recoveryTask = coordinator.EnterRecoveryAsync(CancellationToken.None).AsTask();
        Assert.False(recoveryTask.IsCompleted);
        var queuedProjectionTask = coordinator.EnterProjectionAsync(CancellationToken.None).AsTask();
        Assert.False(queuedProjectionTask.IsCompleted);

        await activeProjection.DisposeAsync();
        var recovery = await recoveryTask.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(queuedProjectionTask.IsCompleted);

        await recovery.DisposeAsync();
        Assert.Equal(1, coordinator.RecoveryGeneration);
        var queuedProjection = await queuedProjectionTask.WaitAsync(TimeSpan.FromSeconds(1));
        await queuedProjection.DisposeAsync();
    }

    [Fact]
    public async Task RollingWorkerAcquiresRecoveryGateBeforeOpeningSnapshotStore()
    {
        var coordinator = new SearchRecoveryCoordinator();
        await using var recovery = await coordinator.EnterRecoveryAsync(CancellationToken.None);
        var directory = Path.Combine(
            Path.GetTempPath(), $"cherry-gated-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(new HeatOptions { DataDirectory = directory });
        var worker = new HeatRollingProjectionWorker(
            store,
            null!,
            null!,
            new HeatOptions(),
            new HeatRuntimeMetrics(),
            NullLogger<HeatRollingProjectionWorker>.Instance,
            coordinator);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            worker.ProcessOnceAsync(canceled.Token));

        // Directory creation is the first observable action in ReadChanges.
        // A pre-cancelled waiter makes this ordering assertion deterministic.
        Assert.False(Directory.Exists(directory));
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) => new(status)
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, Task<HttpResponseMessage>> handler) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) => handler(request);
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public List<(HttpMethod Method, string Uri, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.Method, request.RequestUri!.ToString(), body));
            var response = request.RequestUri.AbsolutePath switch
            {
                "/indexes/torrents/settings" => "{\"taskUid\":40}",
                "/tasks/40" => "{\"status\":\"succeeded\"}",
                "/indexes/torrents/documents" => "{\"taskUid\":42}",
                "/indexes/torrents/search" => "{\"hits\":[],\"estimatedTotalHits\":0}",
                "/indexes/torrents" => "{\"primaryKey\":\"id\"}",
                _ => "{}"
            };
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(response, Encoding.UTF8, "application/json")
            };
        }
    }
}
