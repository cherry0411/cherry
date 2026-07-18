using System.Threading.Channels;
using Cherry.Application.Dtos;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Application.Services;

/// <summary>
/// Coalesces crawler requests into database batches while preserving a durable
/// acknowledgement boundary: a request completes only after its torrent rows
/// and files have committed in the exact store.
/// </summary>
public sealed class IngestService : IHostedService
{
    private const int MaxDatabaseBatchSize = 5_000;
    private const int MaxQueuedEvents = 100_000;
    private const int WorkItemCapacity = 1_024;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<IngestService> _logger;
    private readonly PendingRequestTracker _tracker;
    private readonly Channel<IngestWorkItem> _channel;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _worker;
    private int _queuedEventCount;

    public IngestService(
        IServiceScopeFactory scopeFactory,
        ILogger<IngestService> logger,
        PendingRequestTracker tracker)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _tracker = tracker;
        _channel = Channel.CreateBounded<IngestWorkItem>(new BoundedChannelOptions(WorkItemCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public int QueueDepth => Volatile.Read(ref _queuedEventCount);

    public async Task<BatchIngestResponse> SubmitBatchAsync(BatchIngestRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Events is null)
            return new BatchIngestResponse(0, 0, 1);

        var prepared = new List<PreparedEvent>(request.Events.Count);
        var errors = 0;

        foreach (var crawlerEvent in request.Events)
        {
            if (!TryPrepare(crawlerEvent, out var preparedEvent))
            {
                errors++;
                continue;
            }

            prepared.Add(preparedEvent);
        }

        if (prepared.Count == 0)
            return new BatchIngestResponse(0, 0, errors);

        var reservedDepth = Interlocked.Add(ref _queuedEventCount, prepared.Count);
        if (reservedDepth > MaxQueuedEvents)
        {
            Interlocked.Add(ref _queuedEventCount, -prepared.Count);
            return new BatchIngestResponse(0, 0, errors, Backpressure: true);
        }

        var completion = new TaskCompletionSource<BatchIngestResponse>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var workItem = new IngestWorkItem(prepared, errors, completion);

        try
        {
            await _channel.Writer.WriteAsync(workItem, ct);
        }
        catch
        {
            Interlocked.Add(ref _queuedEventCount, -prepared.Count);
            throw;
        }

        // Cancellation after enqueue may leave the exact commit in progress.
        // A client retry is safe because PostgreSQL's unique key is authoritative.
        return await completion.Task.WaitAsync(ct);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _worker = ProcessLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _channel.Writer.TryComplete();
        if (_worker is null)
            return;

        try
        {
            await _worker.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _stopping.Cancel();
            try
            {
                await _worker;
            }
            catch (OperationCanceledException)
            {
                // Pending callers are completed with cancellation by the worker.
            }
        }
    }

    private async Task ProcessLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (!_channel.Reader.TryRead(out var first))
                    continue;

                var workItems = new List<IngestWorkItem> { first };
                var eventCount = first.Events.Count;

                // Requests already contain up to 512 events. Drain immediately
                // available requests to retain the old 5k-row COPY efficiency
                // without acknowledging any request before the shared commit.
                while (eventCount < MaxDatabaseBatchSize &&
                       _channel.Reader.TryRead(out var next))
                {
                    workItems.Add(next);
                    eventCount += next.Events.Count;
                }

                try
                {
                    await ProcessWorkItemsAsync(workItems, cancellationToken);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception,
                        "Exact ingest transaction failed for {Count} events; no successful ACK was returned.",
                        eventCount);
                    foreach (var workItem in workItems)
                        workItem.Completion.TrySetException(exception);
                }
                finally
                {
                    Interlocked.Add(ref _queuedEventCount, -eventCount);
                }
            }
        }
        finally
        {
            var exception = new OperationCanceledException(
                "Ingest service stopped before the exact transaction completed.");
            while (_channel.Reader.TryRead(out var pending))
            {
                Interlocked.Add(ref _queuedEventCount, -pending.Events.Count);
                pending.Completion.TrySetException(exception);
            }
        }
    }

    private async Task ProcessWorkItemsAsync(
        IReadOnlyList<IngestWorkItem> workItems,
        CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var repository = scope.ServiceProvider.GetRequiredService<ITorrentRepository>();
        var prepared = workItems.SelectMany(item => item.Events).ToList();
        var torrents = prepared.Select(item => item.Torrent).ToList();

        var inserted = await repository.BulkInsertTorrentsAsync(torrents, cancellationToken);

        // Pending-request completion follows the committed torrent transaction.
        // If this bookkeeping fails, callers receive an error and retry safely.
        if (!_tracker.IsEmpty)
        {
            var pendingHashes = _tracker.Filter(torrents.Select(t => t.InfoHash));
            if (pendingHashes.Count > 0)
            {
                await repository.MarkRequestsDoneAsync(pendingHashes, cancellationToken);
                _tracker.Untrack(pendingHashes);
            }
        }

        var unclaimedInserts = new HashSet<string>(inserted, StringComparer.Ordinal);
        foreach (var workItem in workItems)
        {
            var accepted = 0;
            foreach (var preparedEvent in workItem.Events)
            {
                // Exactly one occurrence claims each INSERT ... RETURNING hash;
                // all other occurrences are exact duplicates.
                if (unclaimedInserts.Remove(preparedEvent.Torrent.InfoHash))
                    accepted++;
            }

            var duplicates = workItem.Events.Count - accepted;
            workItem.Completion.TrySetResult(
                new BatchIngestResponse(accepted, duplicates, workItem.Errors));
        }

        _logger.LogInformation(
            "Exact batch committed: {Total} events -> {Inserted} new",
            prepared.Count,
            inserted.Count);
    }

    private static bool TryPrepare(CrawlerEvent? crawlerEvent, out PreparedEvent preparedEvent)
    {
        preparedEvent = default;
        if (crawlerEvent?.Metadata is null ||
            crawlerEvent.Metadata.Name is null ||
            crawlerEvent.Metadata.Files is null ||
            crawlerEvent.Metadata.Files.Any(file => file is null || file.PathText is null) ||
            string.IsNullOrWhiteSpace(crawlerEvent.InfoHash))
            return false;

        var infoHash = crawlerEvent.InfoHash.Trim().ToLowerInvariant();
        if (infoHash.Length != 40 ||
            !infoHash.All(character => character is >= 'a' and <= 'f' or >= '0' and <= '9'))
        {
            return false;
        }

        var metadata = crawlerEvent.Metadata;
        var torrent = new Torrent
        {
            InfoHash = infoHash,
            Name = metadata.Name,
            TotalLength = metadata.Length,
            FileCount = metadata.FileCount,
            Files = metadata.Files.Select(file => new TorrentFile
            {
                PathText = file.PathText,
                Length = file.Length
            }).ToList()
        };

        preparedEvent = new PreparedEvent(torrent);
        return true;
    }

    private sealed record IngestWorkItem(
        List<PreparedEvent> Events,
        int Errors,
        TaskCompletionSource<BatchIngestResponse> Completion);

    private readonly record struct PreparedEvent(Torrent Torrent);
}
