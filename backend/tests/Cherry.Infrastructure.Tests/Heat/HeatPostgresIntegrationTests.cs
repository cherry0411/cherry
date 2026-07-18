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

namespace Cherry.Infrastructure.Tests.Heat;

[Collection("Postgres integration")]
public sealed class HeatPostgresIntegrationTests
{
    [Fact]
    public async Task ExactSqliteState_SealsToVerified64FramePartialManifest_ThenCanDelete()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null) return;
        await using var provider = fixture.Provider;
        var observedAt = DateTime.UtcNow.AddHours(-1);
        var day = DateOnly.FromDateTime(observedAt);
        var observedHour = (byte)observedAt.Hour;
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-pg-{Guid.NewGuid():N}");
        var knownHash = HashFor(Guid.NewGuid());
        var unknownHash = HashFor(Guid.NewGuid());
        var torrentId = await InsertTorrentAsync(provider, knownHash);
        await DeleteDaysAsync(provider, [day]);

        var options = new HeatOptions
        {
            Enabled = true,
            DataDirectory = directory,
            DailyActorSecret = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray()),
            CoverageStartDay = day.ToString("yyyy-MM-dd"),
            ExpectedCrawlerIds = ["sg-crawler-01", "jp-crawler-01"],
            ChannelCapacity = 8,
            CommitBatchRequests = 8
        };
        var metrics = new HeatRuntimeMetrics();
        var accumulator = new HeatAccumulatorService(
            options, metrics, NullLogger<HeatAccumulatorService>.Instance);
        await accumulator.StartAsync(CancellationToken.None);
        try
        {
            var first = new ChhtBatch(
                "sg-crawler-01", day, observedHour, 7, 1, 1,
                [
                    new ChhtHashGroup(Convert.FromHexString(knownHash), [1, 2]),
                    new ChhtHashGroup(Convert.FromHexString(unknownHash), [9])
                ],
                Enumerable.Repeat((byte)1, 32).ToArray());
            var second = new ChhtBatch(
                "sg-crawler-01", day, observedHour, 7, 2, 2,
                [new ChhtHashGroup(Convert.FromHexString(knownHash), [2, 3])],
                Enumerable.Repeat((byte)2, 32).ToArray());
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await accumulator.SubmitAsync(first, CancellationToken.None)).Status);
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await accumulator.SubmitAsync(second, CancellationToken.None)).Status);
            // Both expected crawlers have receipts. Coverage must still remain
            // partial until each one explicitly commits an exact completion.
            var jpReceiptOnly = new ChhtBatch(
                "jp-crawler-01", day, observedHour, 8, 50, 50,
                [new ChhtHashGroup(Convert.FromHexString(knownHash), [1])],
                Enumerable.Repeat((byte)3, 32).ToArray());
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await accumulator.SubmitAsync(jpReceiptOnly, CancellationToken.None)).Status);
            Assert.True(await accumulator.SealBarrierAsync(day, CancellationToken.None));

            await using (var scope = provider.CreateAsyncScope())
            {
                var sealer = new HeatDaySealer(
                    scope.ServiceProvider.GetRequiredService<AppDbContext>(), options, metrics);
                await sealer.SealAsync(day, accumulator.PathForDay(day), CancellationToken.None);
            }

            await using (var connection = await OpenAsync(fixture.ConnectionString))
            {
                await using var manifest = new NpgsqlCommand(
                    """
                    SELECT coverage_status,entry_count,
                           (SELECT count(*) FROM heat_day_frames frame WHERE frame.day=manifest.day)
                      FROM heat_day_manifests manifest WHERE day=@day
                    """, connection);
                manifest.Parameters.AddWithValue("day", day);
                await using var reader = await manifest.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal(2, reader.GetInt16(0)); // Receipts alone are not completion evidence.
                Assert.Equal(1, reader.GetInt64(1)); // Unknown catalog hash was discarded.
                Assert.Equal(64, reader.GetInt64(2));
                await reader.DisposeAsync();

                var shard = (short)(torrentId & 63);
                await using var frame = new NpgsqlCommand(
                    """
                    SELECT entry_count,payload FROM heat_day_frames
                     WHERE day=@day AND shard=@shard
                    """, connection);
                frame.Parameters.AddWithValue("day", day);
                frame.Parameters.AddWithValue("shard", shard);
                await using var frameReader = await frame.ExecuteReaderAsync();
                Assert.True(await frameReader.ReadAsync());
                var decoded = HeatFrameCodec.Decode(
                    shard, frameReader.GetInt32(0), (byte[])frameReader[1]);
                Assert.Equal([new HeatFrameEntry(torrentId, 3)], decoded);
            }

            HeatDaySealer.DeleteAccumulator(accumulator.PathForDay(day));
            Assert.False(File.Exists(accumulator.PathForDay(day)));
            Assert.False(File.Exists($"{accumulator.PathForDay(day)}-wal"));
        }
        finally
        {
            await accumulator.StopAsync(CancellationToken.None);
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
            await DeleteDaysAsync(provider, [day]);
        }
    }

    [Fact]
    public async Task CompletePartialComplete_AdvancesAndReportsActualWindowCoverage()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null) return;
        await using var provider = fixture.Provider;
        var first = RandomDay();
        var partial = first.AddDays(1);
        var recovered = first.AddDays(2);
        var generation = $"heat-sequence-{Guid.NewGuid():N}";
        var torrentId = await InsertTorrentAsync(provider, HashFor(Guid.NewGuid()));
        await DeleteDaysAsync(provider, [first, partial, recovered]);
        await DeleteGenerationAsync(provider, generation);
        await InsertDayAsync(provider, first, 1, [new HeatFrameEntry(torrentId, 5)]);
        await InsertDayAsync(provider, partial, 2, [new HeatFrameEntry(torrentId, 999)]);
        await InsertDayAsync(provider, recovered, 1, [new HeatFrameEntry(torrentId, 2)]);
        await InsertWatermarkAsync(provider, generation, first.AddDays(-1), 0);

        var documents = new List<string>();
        long task = 100;
        var client = Client(async request =>
        {
            if (request.Method == HttpMethod.Put)
            {
                documents.Add(await request.Content!.ReadAsStringAsync());
                return Json(HttpStatusCode.Accepted, $"{{\"taskUid\":{Interlocked.Increment(ref task)}}}");
            }
            return Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}");
        });
        var options = ProjectionOptions(first, generation);
        var worker = Worker(provider, client, options);
        try
        {
            await AdvanceToAsync(provider, worker, generation, first);
            Assert.Equal(1, (await ReadWatermarkAsync(provider, generation)).Mask);
            await AdvanceToAsync(provider, worker, generation, partial);
            Assert.Equal(2, (await ReadWatermarkAsync(provider, generation)).Mask);
            await AdvanceToAsync(provider, worker, generation, recovered);
            var status = await ReadWatermarkAsync(provider, generation);
            Assert.Equal(5, status.Mask); // D and D-2 complete; D-1 explicitly partial.
            Assert.Equal(2, HeatCoverage.Count(status.Mask, 15));

            Assert.Equal(2, documents.Count);
            Assert.Contains("\"heat3d\":5", documents[0]);
            Assert.Contains("\"heat3d\":7", documents[1]);
            Assert.Contains("\"heat7d\":7", documents[1]);
            Assert.DoesNotContain("999", string.Concat(documents));

            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            using var searchClient = new HttpClient(new DelegateHandler(_ =>
                Task.FromResult(Json(HttpStatusCode.OK, "{\"hits\":[],\"estimatedTotalHits\":0}"))))
            {
                BaseAddress = new Uri("http://meili.test")
            };
            var repository = new TorrentRepository(
                db,
                new MeiliSearchClient(searchClient),
                heatOptions: options,
                heatStatusCache: new HeatProjectionStatusCache());
            var (_, _, asOf, sevenDayCoverage) =
                await repository.SearchAsync("", "7d", 1, 20);
            Assert.Equal(recovered.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc), asOf);
            Assert.Equal(48, sevenDayCoverage);

            await SetCoverageMaskAsync(provider, generation, (1 << 14) | 1);
            var highMaskRepository = new TorrentRepository(
                db,
                new MeiliSearchClient(searchClient),
                heatOptions: options,
                heatStatusCache: new HeatProjectionStatusCache());
            var (_, _, _, fifteenDayCoverage) =
                await highMaskRepository.SearchAsync("", "15d", 1, 20);
            Assert.Equal(48, fifteenDayCoverage); // Exercises PostgreSQL integer/GetInt32 and D-14.
        }
        finally
        {
            await DeleteGenerationAsync(provider, generation);
            await DeleteDaysAsync(provider, [first, partial, recovered]);
        }
    }

    [Fact]
    public async Task PendingFailedRetry_DoesNotAdvanceWatermarkOrGcUntilSucceeded()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null) return;
        await using var provider = fixture.Provider;
        var target = RandomDay();
        var old = target.AddDays(-16);
        var generation = $"heat-task-{Guid.NewGuid():N}";
        var torrentId = await InsertTorrentAsync(provider, HashFor(Guid.NewGuid()));
        await DeleteDaysAsync(provider, [old, target]);
        await DeleteGenerationAsync(provider, generation);
        await InsertDayAsync(provider, old, 1, []);
        await InsertDayAsync(provider, target, 1, [new HeatFrameEntry(torrentId, 4)]);
        await InsertWatermarkAsync(provider, generation, target.AddDays(-1), 0);

        var submissions = 0;
        var task101Polls = 0;
        var client = Client(request =>
        {
            if (request.Method == HttpMethod.Put)
            {
                submissions++;
                return Task.FromResult(Json(
                    HttpStatusCode.Accepted,
                    submissions == 1 ? "{\"taskUid\":101}" : "{\"taskUid\":102}"));
            }
            var path = request.RequestUri!.AbsolutePath;
            if (path == "/tasks/101")
            {
                task101Polls++;
                return Task.FromResult(Json(
                    HttpStatusCode.OK,
                    task101Polls == 1
                        ? "{\"status\":\"processing\"}"
                        : "{\"status\":\"failed\",\"error\":{\"message\":\"retry me\"}}"));
            }
            return Task.FromResult(Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}"));
        });
        var worker = Worker(provider, client, ProjectionOptions(target, generation));
        try
        {
            Assert.True(await worker.ProcessOnceAsync()); // Submit task 101.
            await AssertPendingAsync(provider, generation, target, 101);
            await AssertNotAdvancedAsync(provider, generation, target.AddDays(-1), old);

            Assert.True(await worker.ProcessOnceAsync()); // Task 101 still processing.
            await AssertPendingAsync(provider, generation, target, 101);
            await AssertNotAdvancedAsync(provider, generation, target.AddDays(-1), old);

            Assert.True(await worker.ProcessOnceAsync()); // Failed 101 is retried as 102.
            await AssertPendingAsync(provider, generation, target, 102);
            await AssertNotAdvancedAsync(provider, generation, target.AddDays(-1), old);

            Assert.True(await worker.ProcessOnceAsync()); // 102 succeeds; watermark and GC commit.
            Assert.Equal(target, (await ReadWatermarkAsync(provider, generation)).Day);
            Assert.False(await DayExistsAsync(provider, old));
            Assert.Equal(0, await TaskCountAsync(provider, generation, target));
            Assert.Equal(2, submissions);
        }
        finally
        {
            await DeleteGenerationAsync(provider, generation);
            await DeleteDaysAsync(provider, [old, target]);
        }
    }

    [Fact]
    public async Task FullRebuild_UsesLatestRetainedWindowAfterCoverageStartWasGarbageCollected()
    {
        var fixture = await CreateFixtureAsync();
        if (fixture is null) return;
        await using var provider = fixture.Provider;
        var coverageStart = RandomDay();
        var target = coverageStart.AddDays(31);
        var retainedDays = Enumerable.Range(1, 31)
            .Select(offset => coverageStart.AddDays(offset))
            .ToArray();
        var generation = $"heat-gc-rebuild-{Guid.NewGuid():N}";
        var torrentId = await InsertTorrentAsync(provider, HashFor(Guid.NewGuid()));
        await DeleteDaysAsync(provider, retainedDays);
        await DeleteGenerationAsync(provider, generation);
        foreach (var day in retainedDays)
        {
            await InsertDayAsync(
                provider,
                day,
                1,
                day == target ? [new HeatFrameEntry(torrentId, 9)] : []);
        }
        await InsertWatermarkAsync(provider, generation, target.AddDays(-1), 0);
        await RequestRebuildAsync(provider, generation);

        var documents = new List<string>();
        var task = 200L;
        var client = Client(async request =>
        {
            if (request.Method == HttpMethod.Put)
            {
                documents.Add(await request.Content!.ReadAsStringAsync());
                return Json(HttpStatusCode.Accepted, $"{{\"taskUid\":{Interlocked.Increment(ref task)}}}");
            }
            return Json(HttpStatusCode.OK, "{\"status\":\"succeeded\"}");
        });
        var worker = Worker(provider, client, ProjectionOptions(coverageStart, generation));
        try
        {
            await AdvanceToAsync(provider, worker, generation, target);

            var status = await ReadWatermarkAsync(provider, generation);
            Assert.Equal(target, status.Day);
            Assert.Equal(15, HeatCoverage.Count(status.Mask, 15));
            var payload = Assert.Single(documents);
            Assert.Contains($"\"id\":{torrentId}", payload);
            Assert.Contains("\"heat3d\":9", payload);
        }
        finally
        {
            await DeleteGenerationAsync(provider, generation);
            await DeleteDaysAsync(provider, retainedDays);
        }
    }

    private static HeatProjectionWorker Worker(
        ServiceProvider provider,
        MeiliSearchClient client,
        HeatOptions options) =>
        new(
            provider.GetRequiredService<IServiceScopeFactory>(),
            client,
            options,
            new HeatRuntimeMetrics(),
            NullLogger<HeatProjectionWorker>.Instance,
            new HeatProjectionStatusCache());

    private static HeatOptions ProjectionOptions(DateOnly start, string generation) => new()
    {
        Enabled = true,
        CoverageStartDay = start.ToString("yyyy-MM-dd"),
        IndexGeneration = generation,
        IndexUid = "torrents",
        ProjectionBatchSize = 100
    };

    private static MeiliSearchClient Client(
        Func<HttpRequestMessage, Task<HttpResponseMessage>> handler) =>
        new(new HttpClient(new DelegateHandler(handler)) { BaseAddress = new Uri("http://meili.test") });

    private static async Task AdvanceToAsync(
        ServiceProvider provider,
        HeatProjectionWorker worker,
        string generation,
        DateOnly target)
    {
        for (var attempt = 0; attempt < 10; attempt++)
        {
            await worker.ProcessOnceAsync();
            if ((await ReadWatermarkAsync(provider, generation)).Day == target) return;
        }
        throw new Xunit.Sdk.XunitException($"Projection did not reach {target}");
    }

    private static async Task<long> InsertTorrentAsync(ServiceProvider provider, string hash)
    {
        await using var scope = provider.CreateAsyncScope();
        var repository = new TorrentRepository(scope.ServiceProvider.GetRequiredService<AppDbContext>());
        await repository.BulkInsertTorrentsAsync([
            new Torrent
            {
                InfoHash = hash,
                Name = $"heat-{hash[..8]}",
                TotalLength = 1,
                FileCount = 1,
                Files = [new TorrentFile { PathText = "x", Length = 1 }]
            }
        ]);
        return await scope.ServiceProvider.GetRequiredService<AppDbContext>()
            .Torrents.Where(torrent => torrent.InfoHash == hash)
            .Select(torrent => torrent.Id)
            .SingleAsync();
    }

    private static async Task InsertDayAsync(
        ServiceProvider provider,
        DateOnly day,
        short coverage,
        IReadOnlyList<HeatFrameEntry> entries)
    {
        var frames = HeatFrameCodec.Encode(entries);
        await using var connection = await OpenAsync(provider);
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var manifest = new NpgsqlCommand(
            """
            INSERT INTO heat_day_manifests(
                day,status,coverage_status,codec_version,shard_count,entry_count,manifest_sha256)
            VALUES(@day,1,@coverage,1,64,@entries,@digest)
            """, connection, transaction))
        {
            manifest.Parameters.AddWithValue("day", day);
            manifest.Parameters.AddWithValue("coverage", coverage);
            manifest.Parameters.AddWithValue("entries", entries.Count);
            manifest.Parameters.AddWithValue("digest", HeatFrameCodec.ManifestDigest(day, frames));
            await manifest.ExecuteNonQueryAsync();
        }
        foreach (var frame in frames)
        {
            await using var insert = new NpgsqlCommand(
                """
                INSERT INTO heat_day_frames(day,shard,codec_version,entry_count,payload_sha256,payload)
                VALUES(@day,@shard,1,@entries,@digest,@payload)
                """, connection, transaction);
            insert.Parameters.AddWithValue("day", day);
            insert.Parameters.AddWithValue("shard", frame.Shard);
            insert.Parameters.AddWithValue("entries", frame.EntryCount);
            insert.Parameters.AddWithValue("digest", frame.Sha256);
            insert.Parameters.AddWithValue("payload", frame.Payload);
            await insert.ExecuteNonQueryAsync();
        }
        await transaction.CommitAsync();
    }

    private static async Task InsertWatermarkAsync(
        ServiceProvider provider,
        string generation,
        DateOnly projectedThrough,
        int mask)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO heat_projection_watermarks(
                index_generation,index_uid,projected_through,coverage_mask,rebuild_required)
            VALUES(@generation,'torrents',@day,@mask,FALSE)
            """, connection);
        command.Parameters.AddWithValue("generation", generation);
        command.Parameters.AddWithValue("day", projectedThrough);
        command.Parameters.AddWithValue("mask", mask);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<(DateOnly? Day, int Mask)> ReadWatermarkAsync(
        ServiceProvider provider,
        string generation)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            SELECT projected_through,coverage_mask
              FROM heat_projection_watermarks WHERE index_generation=@generation
            """, connection);
        command.Parameters.AddWithValue("generation", generation);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (
            reader.IsDBNull(0) ? null : reader.GetFieldValue<DateOnly>(0),
            reader.GetInt32(1));
    }

    private static async Task SetCoverageMaskAsync(
        ServiceProvider provider,
        string generation,
        int mask)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            UPDATE heat_projection_watermarks
               SET coverage_mask=@mask,updated_at=NOW()
             WHERE index_generation=@generation
            """, connection);
        command.Parameters.AddWithValue("mask", mask);
        command.Parameters.AddWithValue("generation", generation);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task RequestRebuildAsync(ServiceProvider provider, string generation)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            UPDATE heat_projection_watermarks
               SET rebuild_required=TRUE,updated_at=NOW()
             WHERE index_generation=@generation
            """,
            connection);
        command.Parameters.AddWithValue("generation", generation);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    private static async Task AssertPendingAsync(
        ServiceProvider provider,
        string generation,
        DateOnly day,
        long task)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            SELECT pending_task_uid FROM heat_projection_tasks
             WHERE index_generation=@generation AND target_day=@day
                   AND pending_task_uid IS NOT NULL
            """, connection);
        command.Parameters.AddWithValue("generation", generation);
        command.Parameters.AddWithValue("day", day);
        Assert.Equal(task, Convert.ToInt64(await command.ExecuteScalarAsync()));
    }

    private static async Task AssertNotAdvancedAsync(
        ServiceProvider provider,
        string generation,
        DateOnly expected,
        DateOnly retainedDay)
    {
        Assert.Equal(expected, (await ReadWatermarkAsync(provider, generation)).Day);
        Assert.True(await DayExistsAsync(provider, retainedDay));
    }

    private static async Task<int> TaskCountAsync(
        ServiceProvider provider,
        string generation,
        DateOnly day)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            """
            SELECT count(*) FROM heat_projection_tasks
             WHERE index_generation=@generation AND target_day=@day
            """, connection);
        command.Parameters.AddWithValue("generation", generation);
        command.Parameters.AddWithValue("day", day);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async Task<bool> DayExistsAsync(ServiceProvider provider, DateOnly day)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS(SELECT 1 FROM heat_day_manifests WHERE day=@day)", connection);
        command.Parameters.AddWithValue("day", day);
        return (bool)(await command.ExecuteScalarAsync() ?? false);
    }

    private static async Task DeleteGenerationAsync(ServiceProvider provider, string generation)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            "DELETE FROM heat_projection_watermarks WHERE index_generation=@generation", connection);
        command.Parameters.AddWithValue("generation", generation);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DeleteDaysAsync(ServiceProvider provider, IReadOnlyList<DateOnly> days)
    {
        await using var connection = await OpenAsync(provider);
        await using var command = new NpgsqlCommand(
            "DELETE FROM heat_day_manifests WHERE day=ANY(@days)", connection);
        command.Parameters.AddWithValue("days", days.ToArray());
        await command.ExecuteNonQueryAsync();
    }

    private static DateOnly RandomDay() =>
        DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-Random.Shared.Next(60, 7_000));

    private static string HashFor(Guid value) =>
        Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(value.ToByteArray()))
            .ToLowerInvariant();

    private static async Task<Fixture?> CreateFixtureAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString)) return null;
        var services = new ServiceCollection();
        services.AddDbContext<AppDbContext>(options => options.UseNpgsql(connectionString));
        var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await db.Database.MigrateAsync();
        return new Fixture(provider, connectionString);
    }

    private static async Task<NpgsqlConnection> OpenAsync(ServiceProvider provider)
    {
        await using var scope = provider.CreateAsyncScope();
        var configured = scope.ServiceProvider.GetRequiredService<AppDbContext>()
            .Database.GetConnectionString()!;
        return await OpenAsync(configured);
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) => new(status)
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };

    private sealed record Fixture(ServiceProvider Provider, string ConnectionString);

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, Task<HttpResponseMessage>> handler) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) => handler(request);
    }
}
