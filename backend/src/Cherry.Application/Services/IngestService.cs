using System.Threading.Channels;
using Cherry.Application.Dtos;
using Cherry.Domain.Entities;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Cherry.Domain.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Application.Services;

public class IngestService : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IDedupFilter _dedup;
    private readonly ILogger<IngestService> _logger;
    private readonly Channel<CrawlerEvent> _channel;
    private readonly CancellationTokenSource _cts = new();

    public IngestService(IServiceScopeFactory scopeFactory, IDedupFilter dedup, ILogger<IngestService> logger)
    {
        _scopeFactory = scopeFactory;
        _dedup = dedup;
        _logger = logger;
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
            // Skip events that are not metadata ingest candidates
            if (evt.Metadata is null)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(evt.InfoHash))
            {
                errors++;
                continue;
            }

            // Try dedup filter first (fast path)
            if (!_dedup.Add(evt.InfoHash))
            {
                duplicates++;
                continue;
            }

            // Wait to enqueue with backpressure
            await _channel.Writer.WriteAsync(evt, ct);
            accepted++;
        }

        return new BatchIngestResponse(accepted, duplicates, errors);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = ProcessLoop(_cts.Token);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        _channel.Writer.Complete();
        return Task.CompletedTask;
    }

    private async Task ProcessLoop(CancellationToken ct)
    {
        var batch = new List<CrawlerEvent>();
        // Batch size tuned for ~50-100MB per COPY operation
        const int batchSize = 5000;

        while (await _channel.Reader.WaitToReadAsync(ct))
        {
            batch.Clear();
            while (batch.Count < batchSize && _channel.Reader.TryRead(out var evt))
            {
                batch.Add(evt);
            }

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
            var torrent = new Torrent
            {
                InfoHash = evt.InfoHash.ToLowerInvariant(),
                Name = meta.Name,
                PieceLength = meta.PieceLength,
                TotalLength = meta.Length,
                FileCount = meta.FileCount,
                IsPrivate = meta.Private,
                Source = evt.InstanceId.Length > 32 ? evt.InstanceId[..32] : evt.InstanceId,
                Files = meta.Files.Select(f => new TorrentFile
                {
                    PathText = f.PathText,
                    Length = f.Length
                }).ToList()
            };
            torrents.Add(torrent);
        }

        var inserted = await repo.BulkInsertTorrentsAsync(torrents, ct);
        var sources = events.Select(e => e.InstanceId).Distinct().OrderBy(s => s);
        _logger.LogInformation("Batch processed: {Total} events → {Inserted} new from [{Sources}]", events.Count, inserted, string.Join(", ", sources));

        // Sync to Meilisearch
        if (inserted > 0)
        {
            var meiliUrl = scope.ServiceProvider.GetRequiredService<IConfiguration>()["MeiliSearch:Url"];
            if (!string.IsNullOrWhiteSpace(meiliUrl))
            {
                try
                {
                    var docs = torrents.Select(t => new
                    {
                        infoHash = t.InfoHash,
                        name = t.Name,
                        totalLength = t.TotalLength,
                        fileCount = t.FileCount,
                        isPrivate = t.IsPrivate,
                        peerCount = t.PeerCount,
                        createdAt = new DateTimeOffset(t.CreatedAt).ToUnixTimeMilliseconds()
                    });
                    var json = JsonSerializer.Serialize(docs);
                    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                    await client.PostAsync($"{meiliUrl}/indexes/torrents/documents",
                        new StringContent(json, Encoding.UTF8, "application/json"), ct);
                }
                catch { /* best-effort */ }
            }
        }
    }
}
