using System.Net;
using System.Text;
using Cherry.Domain.Entities;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Heat;
using Cherry.Infrastructure.Repositories;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace Cherry.Infrastructure.Tests.Search;

[Collection("Postgres integration")]
public sealed class SearchOutboxPostgresTests
{
    [Fact]
    public async Task FailedAsyncTaskAndOutage_RetainRows_UntilPolledSuccess()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null)
            return;
        await using var provider = fixture.Provider;
        var hash = HashFor(Guid.NewGuid());
        await InsertLegacyAsync(provider, hash);
        var torrentId = await TorrentIdAsync(provider, hash);

        var responses = new Queue<Func<HttpRequestMessage, HttpResponseMessage>>([
            request => Json(HttpStatusCode.Accepted, "{\"taskUid\":41}"),
            request => Json(HttpStatusCode.OK, "{\"status\":\"failed\",\"error\":{\"message\":\"bad document\"}}"),
            request => Json(HttpStatusCode.Accepted, "{\"taskUid\":42}"),
            request => Json(HttpStatusCode.OK, "{\"status\":\"canceled\",\"error\":{\"message\":\"operator canceled\"}}"),
            request => throw new HttpRequestException("Meili unavailable"),
            request => Json(HttpStatusCode.Accepted, "{\"taskUid\":43}"),
            request => Json(HttpStatusCode.OK, "{\"status\":\"processing\"}"),
            request => Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}")
        ]);
        var worker = Worker(provider, new DelegateHandler(request =>
            Task.FromResult(responses.Dequeue()(request))));

        Assert.Equal(1, await worker.ProcessOnceAsync());
        await using (var scope = provider.CreateAsyncScope())
        {
            var item = await scope.ServiceProvider.GetRequiredService<AppDbContext>()
                .SearchOutbox.SingleAsync(row => row.TorrentId == torrentId);
            Assert.Equal(1, item.AttemptCount);
            Assert.Contains("bad document", item.LastError);
        }

        await Task.Delay(10);
        Assert.Equal(1, await worker.ProcessOnceAsync());
        await using (var scope = provider.CreateAsyncScope())
        {
            var item = await scope.ServiceProvider.GetRequiredService<AppDbContext>()
                .SearchOutbox.SingleAsync(row => row.TorrentId == torrentId);
            Assert.Equal(2, item.AttemptCount);
            Assert.Contains("canceled", item.LastError, StringComparison.OrdinalIgnoreCase);
        }

        await Task.Delay(10);
        Assert.Equal(1, await worker.ProcessOnceAsync());
        await using (var scope = provider.CreateAsyncScope())
        {
            var item = await scope.ServiceProvider.GetRequiredService<AppDbContext>()
                .SearchOutbox.SingleAsync(row => row.TorrentId == torrentId);
            Assert.Equal(3, item.AttemptCount);
            Assert.Contains("unavailable", item.LastError, StringComparison.OrdinalIgnoreCase);
        }

        await Task.Delay(10);
        Assert.Equal(1, await worker.ProcessOnceAsync());
        await using (var scope = provider.CreateAsyncScope())
        {
            Assert.False(await scope.ServiceProvider.GetRequiredService<AppDbContext>()
                .SearchOutbox.AnyAsync(row => row.TorrentId == torrentId));
        }

        var metrics = provider.GetRequiredService<SearchOutboxMetrics>().Snapshot();
        Assert.Equal(3, metrics.Retries);
        Assert.Equal(2, metrics.FailedTasks);
        Assert.Equal(1, metrics.CompletedDocuments);
    }

    [Fact]
    public async Task CrashAfterSucceededTaskBeforeAck_LeavesRowForIdempotentReplay()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null)
            return;
        await using var provider = fixture.Provider;
        var hash = HashFor(Guid.NewGuid());
        await InsertLegacyAsync(provider, hash);
        var torrentId = await TorrentIdAsync(provider, hash);

        // Simulate a worker that submitted an idempotent document upsert and
        // observed success, then died before its generation-fenced DELETE.
        await using (var scope = provider.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
            var claims = await store.ClaimAsync(Guid.NewGuid(), 1, TimeSpan.FromMinutes(1));
            Assert.Single(claims);
            var documents = await store.LoadDocumentsAsync(claims);
            var client = new MeiliSearchClient(new HttpClient(new DelegateHandler(request =>
                Task.FromResult(request.Method == HttpMethod.Put
                    ? Json(HttpStatusCode.Accepted, "{\"taskUid\":88}")
                    : Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}"))))
            {
                BaseAddress = new Uri("http://meili.test")
            });
            var uid = await client.SubmitDocumentsAsync(documents, CancellationToken.None);
            Assert.Equal("succeeded", (await client.GetTaskAsync(uid, CancellationToken.None)).Status);
            // Intentionally no CompleteAsync: this is the crash window.
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            Assert.True(await db.SearchOutbox.AnyAsync(row => row.TorrentId == torrentId));
            await db.SearchOutbox
                .Where(row => row.TorrentId == torrentId)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(row => row.LeaseUntil, DateTime.UtcNow.AddSeconds(-1)));
        }

        var replayRequests = 0;
        var replay = Worker(provider, new DelegateHandler(request =>
        {
            Interlocked.Increment(ref replayRequests);
            return Task.FromResult(request.Method == HttpMethod.Put
                ? Json(HttpStatusCode.Accepted, "{\"taskUid\":89}")
                : Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}"));
        }));
        Assert.Equal(1, await replay.ProcessOnceAsync());
        Assert.Equal(2, replayRequests);

        await using var finalScope = provider.CreateAsyncScope();
        Assert.False(await finalScope.ServiceProvider.GetRequiredService<AppDbContext>()
            .SearchOutbox.AnyAsync(row => row.TorrentId == torrentId));
    }

    [Fact]
    public async Task UpgradeDuringMeiliTask_IsGenerationFenced_AndRebuildIsRecoverable()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null)
            return;
        await using var provider = fixture.Provider;
        var hash = HashFor(Guid.NewGuid());
        await InsertLegacyAsync(provider, hash);
        var torrentId = await TorrentIdAsync(provider, hash);

        var upgraded = false;
        var handler = new DelegateHandler(async request =>
        {
            if (request.Method == HttpMethod.Put)
                return Json(HttpStatusCode.Accepted, "{\"taskUid\":77}");

            if (!upgraded)
            {
                upgraded = true;
                await using var scope = provider.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var connection = (NpgsqlConnection)db.Database.GetDbConnection();
                await connection.OpenAsync();
                await using var transaction = await connection.BeginTransactionAsync();
                await SearchOutboxWriter.EnqueueAsync([torrentId], connection, transaction, CancellationToken.None);
                await transaction.CommitAsync();
            }
            return Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}");
        });

        var worker = Worker(provider, handler);
        Assert.Equal(1, await worker.ProcessOnceAsync());

        await using (var scope = provider.CreateAsyncScope())
        {
            var item = await scope.ServiceProvider.GetRequiredService<AppDbContext>()
                .SearchOutbox.SingleAsync(row => row.TorrentId == torrentId);
            Assert.Equal(2, item.Generation);
            Assert.Null(item.LeaseOwner);
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            await db.SearchOutbox.Where(row => row.TorrentId == torrentId).ExecuteDeleteAsync();
            var rebuilt = await scope.ServiceProvider.GetRequiredService<SearchOutboxStore>()
                .RebuildAsync();
            Assert.True(rebuilt >= 1);
            Assert.True(await db.SearchOutbox.AnyAsync(row => row.TorrentId == torrentId));
        }
    }

    [Fact]
    public async Task ExpiredLease_CanBeClaimedByAnotherWorker()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null)
            return;
        await using var provider = fixture.Provider;
        var hash = HashFor(Guid.NewGuid());
        await InsertLegacyAsync(provider, hash);
        var torrentId = await TorrentIdAsync(provider, hash);

        await using var firstScope = provider.CreateAsyncScope();
        var firstStore = firstScope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
        var first = await firstStore.ClaimAsync(Guid.NewGuid(), 1, TimeSpan.FromMinutes(1));
        Assert.Single(first);

        await using (var expireScope = provider.CreateAsyncScope())
        {
            var db = expireScope.ServiceProvider.GetRequiredService<AppDbContext>();
            await db.SearchOutbox
                .Where(row => row.TorrentId == torrentId)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(row => row.LeaseUntil, DateTime.UtcNow.AddSeconds(-1)));
        }

        await using var secondScope = provider.CreateAsyncScope();
        var second = await secondScope.ServiceProvider.GetRequiredService<SearchOutboxStore>()
            .ClaimAsync(Guid.NewGuid(), 1, TimeSpan.FromMinutes(1));
        Assert.Single(second);
        Assert.Equal(first[0].Generation, second[0].Generation);
    }

    [Fact]
    public async Task StartupRecovery_RebuildsOnlyAProvablyEmptyIndexWithNonEmptyPostgres()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null)
            return;
        await using var provider = fixture.Provider;
        var hash = HashFor(Guid.NewGuid());
        await InsertLegacyAsync(provider, hash);

        var requests = new List<string>();
        var statsCalls = 0;
        var nextTask = 300L;
        var client = new MeiliSearchClient(new HttpClient(new DelegateHandler(request =>
        {
            requests.Add($"{request.Method} {request.RequestUri!.AbsolutePath}");
            var path = request.RequestUri.AbsolutePath;
            if (path.StartsWith("/tasks/", StringComparison.Ordinal))
                return Task.FromResult(Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}"));
            if (path == "/indexes/torrents/stats")
            {
                statsCalls++;
                return Task.FromResult(Json(
                    HttpStatusCode.OK,
                    statsCalls <= 2
                        ? "{\"numberOfDocuments\":0}"
                        : "{\"numberOfDocuments\":1}"));
            }
            if (path == "/indexes/torrents" && request.Method == HttpMethod.Get)
                return Task.FromResult(Json(HttpStatusCode.NotFound, "{}"));
            return Task.FromResult(Json(
                HttpStatusCode.Accepted,
                $"{{\"taskUid\":{Interlocked.Increment(ref nextTask)}}}"));
        }))
        {
            BaseAddress = new Uri("http://meili.test")
        });

        await using var scope = provider.CreateAsyncScope();
        var service = new SearchRecoveryService(
            scope.ServiceProvider.GetRequiredService<AppDbContext>(),
            scope.ServiceProvider.GetRequiredService<SearchOutboxStore>(),
            client,
            new HeatOptions { Enabled = false },
            new SearchRecoveryCoordinator());

        var recovered = await service.RecoverIfProvablyEmptyAsync(CancellationToken.None);
        Assert.NotNull(recovered);
        Assert.True(recovered.MetadataRowsEnqueued >= 1);
        Assert.Contains("DELETE /indexes/torrents", requests);
        Assert.True(await scope.ServiceProvider.GetRequiredService<AppDbContext>()
            .SearchOutbox.AnyAsync());

        var deleteCount = requests.Count(request => request == "DELETE /indexes/torrents");
        Assert.Null(await service.RecoverIfProvablyEmptyAsync(CancellationToken.None));
        Assert.Equal(deleteCount, requests.Count(request => request == "DELETE /indexes/torrents"));
    }

    private static SearchOutboxWorker Worker(ServiceProvider provider, HttpMessageHandler handler)
    {
        var client = new MeiliSearchClient(new HttpClient(handler)
        {
            BaseAddress = new Uri("http://meili.test")
        });
        return new SearchOutboxWorker(
            provider.GetRequiredService<IServiceScopeFactory>(),
            client,
            new SearchOutboxOptions
            {
                BatchSize = 50,
                LeaseDuration = TimeSpan.FromMinutes(1),
                PollInterval = TimeSpan.Zero,
                TaskTimeout = TimeSpan.FromSeconds(2),
                RetryBaseDelay = TimeSpan.FromMilliseconds(1),
                RetryMaxDelay = TimeSpan.FromMilliseconds(1)
            },
            provider.GetRequiredService<SearchOutboxMetrics>(),
            NullLogger<SearchOutboxWorker>.Instance);
    }

    private static async Task InsertLegacyAsync(ServiceProvider provider, string hash)
    {
        await using var scope = provider.CreateAsyncScope();
        var repository = new TorrentRepository(
            scope.ServiceProvider.GetRequiredService<AppDbContext>());
        var inserted = await repository.BulkInsertTorrentsAsync([
            new Torrent
            {
                InfoHash = hash,
                Name = "search-outbox-test",
                TotalLength = 100,
                FileCount = 1,
                Files = [new TorrentFile { PathText = "test.bin", Length = 100 }]
            }
        ]);
        Assert.Contains(hash, inserted);
    }

    private static async Task<long> TorrentIdAsync(ServiceProvider provider, string hash)
    {
        await using var scope = provider.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<AppDbContext>()
            .Torrents.Where(torrent => torrent.InfoHash == hash)
            .Select(torrent => torrent.Id)
            .SingleAsync();
    }

    private static async Task<Fixture?> CreateFixtureAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return null;
        var services = new ServiceCollection();
        services.AddDbContext<AppDbContext>(options => options.UseNpgsql(connectionString));
        services.AddScoped<SearchOutboxStore>();
        services.AddSingleton<SearchOutboxMetrics>();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await db.Database.MigrateAsync();
        // This collection is serial, but the developer/CI database is reused.
        // Keep each worker claim deterministic without deleting authoritative
        // torrent rows owned by other integration tests.
        await db.SearchOutbox.ExecuteDeleteAsync();
        return new Fixture(provider);
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string json) => new(status)
    {
        Content = new StringContent(json, Encoding.UTF8, "application/json")
    };

    private static string HashFor(Guid value) =>
        Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(value.ToByteArray()))
            .ToLowerInvariant();

    private sealed record Fixture(ServiceProvider Provider);

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, Task<HttpResponseMessage>> handle) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) => handle(request);
    }
}
