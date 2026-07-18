using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Application;

public sealed class IngestServiceTests
{
    [Fact]
    public async Task SubmitBatch_DoesNotAcknowledgeBeforeExactCommitCompletes()
    {
        var enteredRepository = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = new FakeTorrentRepository
        {
            BulkInsert = async (torrents, cancellationToken) =>
            {
                enteredRepository.SetResult();
                await allowCommit.Task.WaitAsync(cancellationToken);
                return torrents.Select(torrent => torrent.InfoHash).ToHashSet();
            }
        };
        var service = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        try
        {
            var submit = service.SubmitBatchAsync(
                Request(Event(HashFor(1))),
                CancellationToken.None);
            await enteredRepository.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.False(submit.IsCompleted);
            allowCommit.SetResult();

            var response = await submit.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, response.Accepted);
            Assert.Equal(0, response.Duplicates);
        }
        finally
        {
            allowCommit.TrySetResult();
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SubmitBatch_ClassifiesDuplicatesFromExactReturningSet()
    {
        var newHash = HashFor(10);
        var existingHash = HashFor(11);
        var repository = new FakeTorrentRepository
        {
            BulkInsert = (torrents, _) => Task.FromResult<IReadOnlySet<string>>(
                new HashSet<string> { newHash })
        };
        var service = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        try
        {
            var response = await service.SubmitBatchAsync(
                Request(Event(newHash), Event(newHash), Event(existingHash)),
                CancellationToken.None);

            Assert.Equal(1, response.Accepted);
            Assert.Equal(2, response.Duplicates);
            Assert.Equal(0, response.Errors);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SubmitBatch_ExactStoreFailureNeverReturnsSuccessfulAck()
    {
        var repository = new FakeTorrentRepository
        {
            BulkInsert = (_, _) => throw new IOException("database unavailable")
        };
        var service = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        try
        {
            var exception = await Assert.ThrowsAsync<IOException>(() =>
                service.SubmitBatchAsync(Request(Event(HashFor(20))), CancellationToken.None));

            Assert.Contains("database unavailable", exception.Message);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SubmitBatch_InvalidHashesAreErrorsAndNeverReachRepository()
    {
        var repository = new FakeTorrentRepository();
        var service = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        try
        {
            var response = await service.SubmitBatchAsync(
                Request(Event("not-a-sha1")),
                CancellationToken.None);

            Assert.Equal(0, response.Accepted);
            Assert.Equal(0, response.Duplicates);
            Assert.Equal(1, response.Errors);
            Assert.Equal(0, repository.BulkInsertCalls);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SubmitBatch_NullJsonFieldsAreErrorsAndNeverReachRepository()
    {
        var repository = new FakeTorrentRepository();
        var service = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        try
        {
            var nullFiles = new CrawlerEvent
            {
                InfoHash = HashFor(31),
                Metadata = new CrawlerMetadata { Name = "test", Files = null! }
            };
            var response = await service.SubmitBatchAsync(
                Request(Event(null!), null!, nullFiles),
                CancellationToken.None);

            Assert.Equal(0, response.Accepted);
            Assert.Equal(0, response.Duplicates);
            Assert.Equal(3, response.Errors);
            Assert.Equal(0, repository.BulkInsertCalls);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    private static IngestService CreateService(FakeTorrentRepository repository) =>
        new(
            new RepositoryScopeFactory(repository),
            NullLogger<IngestService>.Instance,
            new PendingRequestTracker());

    private static BatchIngestRequest Request(params CrawlerEvent[] events) =>
        new() { Events = events.ToList() };

    private static CrawlerEvent Event(string hash) => new()
    {
        Type = "metadata_fetched",
        InstanceId = "test-instance",
        InfoHash = hash,
        Metadata = new CrawlerMetadata
        {
            Name = "test",
            Length = 123,
            FileCount = 1,
            Files = [new CrawlerFile { PathText = "test.bin", Length = 123 }]
        }
    };

    private static string HashFor(int value) =>
        Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(BitConverter.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class RepositoryScopeFactory(ITorrentRepository repository) : IServiceScopeFactory
    {
        public IServiceScope CreateScope() => new RepositoryScope(repository);
    }

    private sealed class RepositoryScope(ITorrentRepository repository) : IServiceScope
    {
        public IServiceProvider ServiceProvider { get; } = new RepositoryServiceProvider(repository);
        public void Dispose() { }
    }

    private sealed class RepositoryServiceProvider(ITorrentRepository repository) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            serviceType == typeof(ITorrentRepository) ? repository : null;
    }

    private sealed class FakeTorrentRepository : ITorrentRepository
    {
        public Func<List<Torrent>, CancellationToken, Task<IReadOnlySet<string>>>? BulkInsert { get; init; }
        public int BulkInsertCalls { get; private set; }

        public Task<IReadOnlySet<string>> BulkInsertTorrentsAsync(
            List<Torrent> torrents,
            CancellationToken ct = default)
        {
            BulkInsertCalls++;
            return BulkInsert?.Invoke(torrents, ct) ??
                   Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
        }

        public Task<IReadOnlySet<string>> AddRejectedHashesAsync(
            IReadOnlyCollection<string> infoHashes,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<(List<Torrent> Items, long Total, DateOnly? HeatAsOfDay, int HeatCoverageDays)> SearchAsync(
            string query,
            string heatWindow,
            int page,
            int pageSize,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<List<Torrent>> GetRecentAsync(int count, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<List<string>> CheckProcessedAsync(List<string> hashes, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<string> StreamProcessedHashesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task BatchUpdatePeerCountsAsync(
            Dictionary<string, int> counts,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task DecayPeerCountsAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<long> GetTotalCountAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<long> GetTodayCountAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task MarkRequestsDoneAsync(
            IEnumerable<string> infoHashes,
            CancellationToken ct = default) => Task.CompletedTask;
    }
}
