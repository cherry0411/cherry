using Cherry.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Infrastructure.Dedup;

/// <summary>
/// Coordinates the probabilistic filter with the exact PostgreSQL authority.
/// The fast-path remains disabled until every exact processed hash has been
/// replayed. Any rebuild or insertion failure permanently bypasses the filter
/// for the current process, preserving correctness at the cost of DB traffic.
/// </summary>
public sealed class ProcessedHashFilter : IProcessedHashFilter, IHostedService, IDisposable
{
    private const int Building = 0;
    private const int Ready = 1;
    private const int Disabled = 2;
    private const int RebuildBatchSize = 4_096;

    private readonly IDedupFilter _filter;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ProcessedHashFilter> _logger;
    private readonly string? _snapshotPath;
    private readonly string? _legacyRejectedPath;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _rebuildTask;
    private int _state = Building;

    public ProcessedHashFilter(
        IDedupFilter filter,
        IServiceScopeFactory scopeFactory,
        ILogger<ProcessedHashFilter> logger,
        string? snapshotPath = null,
        string? legacyRejectedPath = null)
    {
        _filter = filter;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _snapshotPath = snapshotPath;
        _legacyRejectedPath = legacyRejectedPath;
    }

    public bool IsReady => Volatile.Read(ref _state) == Ready;

    public bool MightContain(string infoHash) => _filter.MightContain(infoHash);

    public void RecordCandidates(IEnumerable<string> infoHashes)
    {
        if (Volatile.Read(ref _state) == Disabled)
            return;

        foreach (var infoHash in infoHashes)
        {
            try
            {
                if (_filter.MightContain(infoHash))
                    continue;

                if (_filter.Add(infoHash) || _filter.MightContain(infoHash))
                    continue;

                Disable(
                    $"probabilistic filter could not represent hash {infoHash}; exact-store bypass enabled");
                return;
            }
            catch (Exception exception)
            {
                Disable("probabilistic filter update failed; exact-store bypass enabled", exception);
                return;
            }
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_snapshotPath is not null && File.Exists(_snapshotPath))
        {
            _logger.LogWarning(
                "Ignoring Cuckoo snapshot {SnapshotPath} during startup. " +
                "Snapshots are not an exact crash-consistent authority; rebuilding from PostgreSQL before enabling the fast-path.",
                _snapshotPath);
        }

        if (_legacyRejectedPath is not null && File.Exists(_legacyRejectedPath))
        {
            _logger.LogWarning(
                "Ignoring legacy probabilistic rejected-hash snapshot {RejectedPath}. " +
                "Fingerprints cannot be migrated to exact hashes; rejected hashes may be safely re-crawled.",
                _legacyRejectedPath);
        }

        _rebuildTask = RebuildAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping.Cancel();
        if (_rebuildTask is null)
            return;

        try
        {
            await _rebuildTask.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Host shutdown cancellation is expected. The next process rebuilds
            // from the exact store again before using the fast-path.
        }
    }

    private async Task RebuildAsync(CancellationToken cancellationToken)
    {
        long replayed = 0;
        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var repository = scope.ServiceProvider.GetRequiredService<ITorrentRepository>();
            var batch = new List<string>(RebuildBatchSize);

            await foreach (var infoHash in repository.StreamProcessedHashesAsync(cancellationToken))
            {
                batch.Add(infoHash);
                if (batch.Count < RebuildBatchSize)
                    continue;

                RecordCandidates(batch);
                replayed += batch.Count;
                batch.Clear();

                if (Volatile.Read(ref _state) == Disabled)
                    return;
            }

            if (batch.Count > 0)
            {
                RecordCandidates(batch);
                replayed += batch.Count;
            }

            if (Interlocked.CompareExchange(ref _state, Ready, Building) != Building)
                return;

            _logger.LogInformation(
                "Processed-hash filter rebuild complete: {Count} exact hashes replayed; negative fast-path enabled.",
                replayed);

            try
            {
                if (_filter is CuckooFilter cuckooFilter)
                    cuckooFilter.Save();
            }
            catch (Exception exception)
            {
                // The snapshot is only a diagnostic/warm artifact. This process
                // is complete in memory and future processes rebuild from DB.
                _logger.LogError(exception,
                    "Failed to persist rebuilt Cuckoo snapshot; exact query correctness is unaffected.");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Processed-hash filter rebuild canceled after {Count} hashes; fast-path remains disabled.",
                replayed);
        }
        catch (Exception exception)
        {
            Disable(
                $"exact-store rebuild failed after {replayed} hashes; exact-store bypass enabled",
                exception);
        }
    }

    private void Disable(string reason, Exception? exception = null)
    {
        if (Interlocked.Exchange(ref _state, Disabled) == Disabled)
            return;

        if (exception is null)
            _logger.LogError("{Reason}", reason);
        else
            _logger.LogError(exception, "{Reason}", reason);
    }

    public void Dispose() => _stopping.Dispose();
}
