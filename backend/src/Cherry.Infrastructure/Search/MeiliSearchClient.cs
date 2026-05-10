using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cherry.Domain.Entities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Infrastructure.Search;

public class MeiliSearchClient
{
    private readonly HttpClient _http;

    public MeiliSearchClient(HttpClient http)
    {
        _http = http;
    }

    public async Task EnsureIndexAsync(CancellationToken ct)
    {
        var settings = JsonSerializer.Serialize(new
        {
            searchableAttributes = new[] { "name" },
            sortableAttributes = new[] { "createdAt", "fileCount", "peerCount", "totalLength" },
            filterableAttributes = new[] { "fileCount", "totalLength", "isPrivate", "peerCount" },
            rankingRules = new[] { "sort", "createdAt:desc", "words", "exactness" },
            typoTolerance = new
            {
                minWordSizeForTypos = new { oneTypo = 5, twoTypos = 8 },
                disableOnWords = Array.Empty<string>(),
                disableOnAttributes = Array.Empty<string>()
            }
        });
        var content = new StringContent(settings, Encoding.UTF8, "application/json");
        var body = JsonSerializer.Serialize(new { uid = "torrents", primaryKey = "infoHash" });
        await _http.PostAsync("/indexes", new StringContent(body, Encoding.UTF8, "application/json"), ct);
        await _http.PatchAsync("/indexes/torrents/settings", content, ct);
    }

    /// <summary>
    /// Sends a batch of torrent documents to Meilisearch. Returns true on success.
    /// </summary>
    public async Task<bool> IndexDocumentsAsync(List<Torrent> torrents, CancellationToken ct)
    {
        var docs = torrents.Select(t => new
        {
            infoHash = t.InfoHash,
            name = t.Name,
            totalLength = t.TotalLength,
            fileCount = t.FileCount,
            isPrivate = t.IsPrivate,
            peerCount = t.PeerCount,
            createdAt = new DateTimeOffset(t.CreatedAt, TimeSpan.Zero).ToUnixTimeMilliseconds()
        }).ToList();

        var body = JsonSerializer.Serialize(docs);
        var content = new StringContent(body, Encoding.UTF8, "application/json");
        var response = await _http.PostAsync("/indexes/torrents/documents", content, ct);
        return response.IsSuccessStatusCode;
    }

    public async Task<MeiliSearchResult?> SearchAsync(string query, int page, int pageSize, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            q = query,
            offset = (page - 1) * pageSize,
            limit = pageSize,
            sort = new[] { "peerCount:desc" },
            attributesToRetrieve = new[] { "infoHash" },
            matchingStrategy = SearchHelper.IsCjkQuery(query) ? "all" : "last"
        });

        var content = new StringContent(body, Encoding.UTF8, "application/json");
        var response = await _http.PostAsync("/indexes/torrents/search", content, ct);
        if (!response.IsSuccessStatusCode) return null;
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<MeiliSearchResult>(json);
    }
}

/// <summary>
/// Decouples DB ingest from Meilisearch writes. Buffers documents and flushes
/// every 30 seconds (or when 500 docs accumulated) to reduce index pressure.
/// </summary>
public sealed class MeiliIndexQueue : IHostedService, IDisposable
{
    private readonly MeiliSearchClient _client;
    private readonly ILogger<MeiliIndexQueue> _logger;
    private readonly List<Torrent> _buffer;
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;

    public MeiliIndexQueue(MeiliSearchClient client, ILogger<MeiliIndexQueue> logger)
    {
        _client = client;
        _logger = logger;
        _buffer = [];
    }

    /// <summary>Enqueue documents for async indexing. Non-blocking.</summary>
    public void Enqueue(List<Torrent> batch)
    {
        if (batch.Count == 0) return;
        lock (_lock) _buffer.AddRange(batch);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _loop = RunLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        if (_loop != null) await _loop;
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(10), ct); }
            catch (OperationCanceledException) { break; }
            List<Torrent> batch;
            lock (_lock)
            {
                if (_buffer.Count == 0) continue;
                batch = [.. _buffer];
                _buffer.Clear();
            }
            await IndexWithRetryAsync(batch, ct);
        }
        // Final flush on shutdown
        List<Torrent> final;
        lock (_lock) { final = [.. _buffer]; _buffer.Clear(); }
        if (final.Count > 0) await IndexWithRetryAsync(final, CancellationToken.None);
    }

    private async Task IndexWithRetryAsync(List<Torrent> batch, CancellationToken ct)
    {
        const int maxAttempts = 3;
        var total = batch.Count;
        var delay = TimeSpan.FromSeconds(1);
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                var ok = await _client.IndexDocumentsAsync(batch, ct);
                if (ok)
                {
                    _logger.LogInformation("Meilisearch: {Count} docs indexed", total);
                    return;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Meilisearch indexing failed (attempt {Attempt}/{Max})", attempt, maxAttempts);
            }
            if (attempt < maxAttempts) await Task.Delay(delay, ct);
            delay *= 2;
        }
        _logger.LogError("Meilisearch indexing failed after {Max} attempts for {Count} docs", maxAttempts, total);
    }

    public void Dispose() => _cts.Dispose();
}

public class MeiliSearchResult
{
    [JsonPropertyName("hits")]
    public List<MeiliHit> Hits { get; set; } = [];

    [JsonPropertyName("estimatedTotalHits")]
    public long EstimatedTotalHits { get; set; }
}

public class MeiliHit
{
    [JsonPropertyName("infoHash")]
    public string InfoHash { get; set; } = string.Empty;
}

public static class SearchHelper
{
    public static bool IsCjkQuery(string query)
    {
        foreach (var c in query)
            if (c >= 0x4E00 && c <= 0x9FFF)
                return true;
        return false;
    }
}


