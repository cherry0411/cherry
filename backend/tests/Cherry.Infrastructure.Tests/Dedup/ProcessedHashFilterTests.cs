using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Dedup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Dedup;

public sealed class ProcessedHashFilterTests
{
    [Fact]
    public async Task Search_BypassesFilterUntilExactReplayIsComplete()
    {
        var hashes = new[] { HashFor(1), HashFor(2) };
        var repository = new FakeTorrentRepository
        {
            CheckProcessed = candidates =>
            {
                Assert.Equal(hashes, candidates);
                return [hashes[0]];
            }
        };
        var filter = new StubProcessedHashFilter { IsReady = false };
        var service = new SearchService(repository, filter);

        var result = await service.CheckExistsAsync(hashes.ToList(), CancellationToken.None);

        Assert.Equal([hashes[0]], result);
        Assert.Equal(0, filter.MightContainCalls);
    }

    [Fact]
    public async Task Search_ConfirmsEveryFilterPositiveWithExactStore()
    {
        var exact = HashFor(10);
        var collision = HashFor(11);
        var definiteMiss = HashFor(12);
        var repository = new FakeTorrentRepository
        {
            CheckProcessed = candidates =>
            {
                Assert.Equal([exact, collision], candidates);
                return [exact];
            }
        };
        var filter = new StubProcessedHashFilter
        {
            IsReady = true,
            PositiveHashes = new HashSet<string> { exact, collision }
        };
        var service = new SearchService(repository, filter);

        var result = await service.CheckExistsAsync(
            [exact, collision, definiteMiss],
            CancellationToken.None);

        Assert.Equal([exact], result);
        Assert.Equal(3, filter.MightContainCalls);
    }

    [Fact]
    public async Task Coordinator_EnablesFastPathOnlyAfterCompleteExactReplay()
    {
        var first = HashFor(20);
        var second = HashFor(21);
        var replayPaused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var continueReplay = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = new FakeTorrentRepository
        {
            StreamProcessed = cancellationToken => Stream(cancellationToken)
        };
        var rawFilter = new StubDedupFilter();
        using var coordinator = CreateCoordinator(rawFilter, repository);

        async IAsyncEnumerable<string> Stream(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return first;
            replayPaused.SetResult();
            await continueReplay.Task.WaitAsync(cancellationToken);
            yield return second;
        }

        await coordinator.StartAsync(CancellationToken.None);
        try
        {
            await replayPaused.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(coordinator.IsReady);

            continueReplay.SetResult();
            await WaitUntilAsync(() => coordinator.IsReady);

            Assert.True(rawFilter.MightContain(first));
            Assert.True(rawFilter.MightContain(second));
        }
        finally
        {
            continueReplay.TrySetResult();
            await coordinator.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Coordinator_DisablesFastPathWhenNewHashCannotBeRepresented()
    {
        var repository = new FakeTorrentRepository();
        var rawFilter = new StubDedupFilter();
        using var coordinator = CreateCoordinator(rawFilter, repository);
        await coordinator.StartAsync(CancellationToken.None);

        try
        {
            await WaitUntilAsync(() => coordinator.IsReady);
            rawFilter.RejectNewItems = true;

            coordinator.RecordCandidates([HashFor(30)]);

            Assert.False(coordinator.IsReady);
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }
    }

    private static ProcessedHashFilter CreateCoordinator(
        IDedupFilter rawFilter,
        ITorrentRepository repository) =>
        new(
            rawFilter,
            new RepositoryScopeFactory(repository),
            NullLogger<ProcessedHashFilter>.Instance);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition was not reached.");
            await Task.Delay(10);
        }
    }

    private static string HashFor(int value) =>
        Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(BitConverter.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class StubProcessedHashFilter : IProcessedHashFilter
    {
        public bool IsReady { get; init; }
        public HashSet<string> PositiveHashes { get; init; } = [];
        public int MightContainCalls { get; private set; }

        public bool MightContain(string infoHash)
        {
            MightContainCalls++;
            return PositiveHashes.Contains(infoHash);
        }

        public void RecordCandidates(IEnumerable<string> infoHashes) { }
    }

    private sealed class StubDedupFilter : IDedupFilter
    {
        private readonly HashSet<string> _hashes = new(StringComparer.Ordinal);
        public bool RejectNewItems { get; set; }
        public long Count => _hashes.Count;
        public bool MightContain(string infoHash) => _hashes.Contains(infoHash);
        public bool Add(string infoHash) => !RejectNewItems && _hashes.Add(infoHash);
    }

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
        public Func<List<string>, List<string>>? CheckProcessed { get; init; }
        public Func<CancellationToken, IAsyncEnumerable<string>>? StreamProcessed { get; init; }

        public Task<List<string>> CheckProcessedAsync(List<string> hashes, CancellationToken ct = default) =>
            Task.FromResult(CheckProcessed?.Invoke(hashes) ?? []);

        public IAsyncEnumerable<string> StreamProcessedHashesAsync(CancellationToken ct = default) =>
            StreamProcessed?.Invoke(ct) ?? Empty();

        private static async IAsyncEnumerable<string> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<IReadOnlySet<string>> BulkInsertTorrentsAsync(
            List<Torrent> torrents,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<IReadOnlySet<string>> AddRejectedHashesAsync(
            IReadOnlyCollection<string> infoHashes,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<(List<Torrent> Items, long Total)> SearchAsync(
            string query,
            int page,
            int pageSize,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<List<Torrent>> GetRecentAsync(int count, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default) =>
            throw new NotSupportedException();

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
            CancellationToken ct = default) => throw new NotSupportedException();
    }
}
