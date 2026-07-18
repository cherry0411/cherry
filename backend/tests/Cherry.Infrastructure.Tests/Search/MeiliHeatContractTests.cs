using System.Net;
using System.Text;
using Cherry.Domain.Entities;
using Cherry.Infrastructure.Heat;
using Cherry.Infrastructure.Search;
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
        await client.SubmitHeatDocumentsAsync([new HeatProjectionDocument(7, 1, 2, 3, 4)], "torrents",
            CancellationToken.None);
        await client.SearchAsync("中文", "15d", 1, 20, CancellationToken.None);

        Assert.Equal(HttpMethod.Put, handler.Requests[3].Method);
        Assert.DoesNotContain("heat1d", handler.Requests[3].Body);
        Assert.Equal(HttpMethod.Put, handler.Requests[4].Method);
        Assert.DoesNotContain("\"name\"", handler.Requests[4].Body);
        Assert.Contains("\"heat15d\":3", handler.Requests[4].Body);
        Assert.Contains("\"heat15d:desc\"", handler.Requests[5].Body);
        Assert.Contains("\"heat30d\"", handler.Requests[5].Body);
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
        var activeProjection = await coordinator.EnterProjectionAsync(CancellationToken.None);

        var recoveryTask = coordinator.EnterRecoveryAsync(CancellationToken.None).AsTask();
        Assert.False(recoveryTask.IsCompleted);
        var queuedProjectionTask = coordinator.EnterProjectionAsync(CancellationToken.None).AsTask();
        Assert.False(queuedProjectionTask.IsCompleted);

        await activeProjection.DisposeAsync();
        var recovery = await recoveryTask.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(queuedProjectionTask.IsCompleted);

        await recovery.DisposeAsync();
        var queuedProjection = await queuedProjectionTask.WaitAsync(TimeSpan.FromSeconds(1));
        await queuedProjection.DisposeAsync();
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
