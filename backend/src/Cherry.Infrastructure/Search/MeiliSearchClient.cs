using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cherry.Domain.Entities;

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
            sortableAttributes = new[] { "firstSeen" },
            filterableAttributes = Array.Empty<string>(),
            rankingRules = new[] { "words", "typo", "proximity", "attribute", "sort", "exactness" },
            typoTolerance = new
            {
                minWordSizeForTypos = new { oneTypo = 5, twoTypos = 8 },
                disableOnWords = Array.Empty<string>(),
                disableOnAttributes = Array.Empty<string>()
            }
        });
        var content = new StringContent(settings, Encoding.UTF8, "application/json");
        var body = JsonSerializer.Serialize(new { uid = "torrents", primaryKey = "id" });
        await _http.PostAsync("/indexes", new StringContent(body, Encoding.UTF8, "application/json"), ct);
        await _http.PatchAsync("/indexes/torrents/settings", content, ct);
    }

    public async Task<long> SubmitDocumentsAsync(
        IReadOnlyCollection<Torrent> torrents,
        CancellationToken ct)
    {
        var docs = torrents.Select(t => new
        {
            id = t.Id,
            name = t.Name,
            firstSeen = new DateTimeOffset(t.CreatedAt, TimeSpan.Zero).ToUnixTimeMilliseconds()
        }).ToList();

        var body = JsonSerializer.Serialize(docs);
        var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await _http.PostAsync("/indexes/torrents/documents", content, ct);
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"Meilisearch document submission returned {(int)response.StatusCode}: {Bound(responseBody)}");
        }

        try
        {
            using var document = JsonDocument.Parse(responseBody);
            if (document.RootElement.TryGetProperty("taskUid", out var taskUid) &&
                taskUid.TryGetInt64(out var value))
                return value;
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException(
                $"Meilisearch returned an invalid task response: {Bound(responseBody)}",
                exception);
        }

        throw new InvalidOperationException(
            $"Meilisearch response did not contain a numeric taskUid: {Bound(responseBody)}");
    }

    public async Task<MeiliTaskState> GetTaskAsync(long taskUid, CancellationToken ct)
    {
        using var response = await _http.GetAsync($"/tasks/{taskUid}", ct);
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"Meilisearch task {taskUid} returned {(int)response.StatusCode}: {Bound(responseBody)}");
        }

        using var document = JsonDocument.Parse(responseBody);
        var root = document.RootElement;
        if (!root.TryGetProperty("status", out var statusElement))
            throw new InvalidOperationException($"Meilisearch task {taskUid} omitted status");
        var status = statusElement.GetString() ?? string.Empty;
        string? error = null;
        if (root.TryGetProperty("error", out var errorElement) &&
            errorElement.ValueKind == JsonValueKind.Object &&
            errorElement.TryGetProperty("message", out var messageElement))
            error = messageElement.GetString();
        return new MeiliTaskState(status, error);
    }

    public async Task<MeiliSearchResult?> SearchAsync(string query, int page, int pageSize, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            q = query,
            offset = (page - 1) * pageSize,
            limit = pageSize,
            sort = new[] { "firstSeen:desc" },
            attributesToRetrieve = new[] { "id" },
            matchingStrategy = SearchHelper.IsCjkQuery(query) ? "all" : "last"
        });

        var content = new StringContent(body, Encoding.UTF8, "application/json");
        var response = await _http.PostAsync("/indexes/torrents/search", content, ct);
        if (!response.IsSuccessStatusCode) return null;
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<MeiliSearchResult>(json);
    }

    private static string Bound(string value) =>
        value.Length <= 1024 ? value : value[..1024];
}

public sealed record MeiliTaskState(string Status, string? Error);

public class MeiliSearchResult
{
    [JsonPropertyName("hits")]
    public List<MeiliHit> Hits { get; set; } = [];

    [JsonPropertyName("estimatedTotalHits")]
    public long EstimatedTotalHits { get; set; }
}

public class MeiliHit
{
    [JsonPropertyName("id")]
    public long Id { get; set; }
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


