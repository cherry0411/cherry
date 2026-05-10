using System.Threading.Channels;
using Cherry.Application.Dtos;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Application.Services;

public class IngestService : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IDedupFilter _dedup;
    private readonly ILogger<IngestService> _logger;
    private readonly PendingRequestTracker _tracker;
    private readonly Channel<CrawlerEvent> _channel;
    private readonly CancellationTokenSource _cts = new();

    public IngestService(
        IServiceScopeFactory scopeFactory,
        IDedupFilter dedup,
        ILogger<IngestService> logger,
        PendingRequestTracker tracker)
    {
        _scopeFactory = scopeFactory;
        _dedup = dedup;
        _logger = logger;
        _tracker = tracker;
        _channel = Channel.CreateBounded<CrawlerEvent>(new BoundedChannelOptions(100_000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public int QueueDepth => _channel.Reader.Count;

    public async Task<BatchIngestResponse> SubmitBatchAsync(BatchIngestRequest request, CancellationToken ct)
    {
        // 背压保护：channel 超 80% 时拒绝新请求，让爬虫退避
        if (_channel.Reader.Count > 80_000)
            return new BatchIngestResponse(0, 0, 0, Backpressure: true);

        var accepted = 0;
        var duplicates = 0;
        var errors = 0;

        foreach (var evt in request.Events)
        {
            if (evt.Metadata is null)
                continue;

            if (string.IsNullOrWhiteSpace(evt.InfoHash))
            {
                errors++;
                continue;
            }

            if (!_dedup.Add(evt.InfoHash))
            {
                duplicates++;
                continue;
            }

            await _channel.Writer.WriteAsync(evt, ct);
            accepted++;
        }

        return new BatchIngestResponse(accepted, duplicates, errors);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Run two concurrent batch-processing workers to keep up under burst load.
        _ = ProcessLoop(_cts.Token);
        _ = ProcessLoop(_cts.Token);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal the ProcessLoop to drain remaining items, then stop.
        _channel.Writer.Complete();
        _cts.CancelAfter(TimeSpan.FromSeconds(15)); // hard-stop after 15 s if drain stalls
        return Task.CompletedTask;
    }

    private async Task ProcessLoop(CancellationToken ct)
    {
        var batch = new List<CrawlerEvent>();
        const int batchSize = 5000;

        while (await _channel.Reader.WaitToReadAsync(ct))
        {
            batch.Clear();
            while (batch.Count < batchSize && _channel.Reader.TryRead(out var evt))
                batch.Add(evt);

            if (batch.Count == 0) continue;

            try
            {
                await ProcessBatchAsync(batch, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process batch of {Count} events", batch.Count);
            }
        }
    }

    private async Task ProcessBatchAsync(List<CrawlerEvent> events, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var repo = scope.ServiceProvider.GetRequiredService<ITorrentRepository>();

        var torrents = new List<Torrent>();

        foreach (var evt in events)
        {
            var meta = evt.Metadata!;
            torrents.Add(new Torrent
            {
                InfoHash = evt.InfoHash.ToLowerInvariant(),
                Name = meta.Name,
                PieceLength = meta.PieceLength,
                TotalLength = meta.Length,
                FileCount = meta.FileCount,
                IsPrivate = meta.Private,
                Source = evt.InstanceId.Length > 32 ? evt.InstanceId[..32] : evt.InstanceId,
                PeerUpdatedAt = DateTime.UtcNow,
                Files = meta.Files.Select(f => new TorrentFile
                {
                    PathText = f.PathText,
                    Length = f.Length
                }).ToList()
            });
        }

        var inserted = await repo.BulkInsertTorrentsAsync(torrents, ct);
        var sources = events.Select(e => e.InstanceId).Distinct().OrderBy(s => s);
        _logger.LogInformation("Batch processed: {Total} events → {Inserted} new from [{Sources}]",
            events.Count, inserted, string.Join(", ", sources));

        // Only hit DB if at least one hash in this batch matches a pending request
        if (!_tracker.IsEmpty)
        {
            var pendingHashes = _tracker.Filter(torrents.Select(t => t.InfoHash));
            if (pendingHashes.Count > 0)
            {
                await repo.MarkRequestsDoneAsync(pendingHashes, ct);
                _tracker.Untrack(pendingHashes);
            }
        }
    }
}
