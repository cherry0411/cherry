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
        using (var get = await _http.GetAsync("/indexes/torrents", ct))
        {
            if (get.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                var body = JsonSerializer.Serialize(new { uid = "torrents", primaryKey = "id" });
                using var create = await _http.PostAsync(
                    "/indexes", new StringContent(body, Encoding.UTF8, "application/json"), ct);
                await RequireSuccessfulTaskAsync(create, "index creation", ct);
            }
            else
            {
                var responseBody = await get.Content.ReadAsStringAsync(ct);
                if (!get.IsSuccessStatusCode)
                    throw new HttpRequestException($"Meilisearch index lookup returned {(int)get.StatusCode}: {Bound(responseBody)}");
                using var index = JsonDocument.Parse(responseBody);
                if (!index.RootElement.TryGetProperty("primaryKey", out var primaryKey) ||
                    !string.Equals(primaryKey.GetString(), "id", StringComparison.Ordinal))
                    throw new InvalidOperationException("Meilisearch torrents index must use primaryKey 'id'");
            }
        }
        var settings = JsonSerializer.Serialize(new
        {
            searchableAttributes = new[] { "name" },
            sortableAttributes = new[] { "firstSeen", "heat1d", "heat7d", "heat15d", "heat30d" },
            filterableAttributes = Array.Empty<string>(),
            rankingRules = new[] { "words", "typo", "proximity", "attribute", "exactness", "sort" },
            typoTolerance = new
            {
                minWordSizeForTypos = new { oneTypo = 5, twoTypos = 8 },
                disableOnWords = Array.Empty<string>(),
                disableOnAttributes = Array.Empty<string>()
            }
        });
        var content = new StringContent(settings, Encoding.UTF8, "application/json");
        using var response = await _http.PatchAsync("/indexes/torrents/settings", content, ct);
        await RequireSuccessfulTaskAsync(response, "settings update", ct);
    }

    public async Task ResetIndexAsync(CancellationToken ct)
    {
        using (var response = await _http.DeleteAsync("/indexes/torrents", ct))
        {
            if (response.StatusCode != System.Net.HttpStatusCode.NotFound)
                await RequireSuccessfulTaskAsync(response, "index deletion", ct);
        }
        await EnsureIndexAsync(ct);
    }

    public async Task<long> GetDocumentCountAsync(CancellationToken ct)
    {
        using var response = await _http.GetAsync("/indexes/torrents/stats", ct);
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException(
                $"Meilisearch index stats returned {(int)response.StatusCode}: {Bound(responseBody)}");
        try
        {
            using var document = JsonDocument.Parse(responseBody);
            if (document.RootElement.TryGetProperty("numberOfDocuments", out var count) &&
                count.TryGetInt64(out var value) && value >= 0)
                return value;
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                $"Meilisearch index stats returned invalid JSON: {Bound(responseBody)}",
                exception);
        }
        throw new InvalidDataException("Meilisearch index stats omitted numberOfDocuments");
    }

    private async Task RequireSuccessfulTaskAsync(
        HttpResponseMessage response,
        string operation,
        CancellationToken ct)
    {
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Meilisearch {operation} returned {(int)response.StatusCode}: {Bound(responseBody)}");
        using var document = JsonDocument.Parse(responseBody);
        if (!document.RootElement.TryGetProperty("taskUid", out var uid) || !uid.TryGetInt64(out var taskUid))
            throw new InvalidOperationException($"Meilisearch {operation} omitted taskUid");
        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (true)
        {
            var state = await GetTaskAsync(taskUid, ct);
            if (string.Equals(state.Status, "succeeded", StringComparison.OrdinalIgnoreCase)) return;
            if (string.Equals(state.Status, "failed", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(state.Status, "canceled", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Meilisearch {operation} task {taskUid} failed: {state.Error ?? state.Status}");
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Meilisearch {operation} task {taskUid} timed out");
            await Task.Delay(100, ct);
        }
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
        using var request = new HttpRequestMessage(HttpMethod.Put, "/indexes/torrents/documents") { Content = content };
        using var response = await _http.SendAsync(request, ct);
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

    public async Task<long> SubmitHeatDocumentsAsync(
        IReadOnlyCollection<Cherry.Infrastructure.Heat.HeatProjectionDocument> documents,
        string indexUid,
        CancellationToken ct)
    {
        if (documents.Any(document =>
                document.Id < 0 || document.Heat1d < 0 || document.Heat7d < 0 ||
                document.Heat15d < 0 || document.Heat30d < 0))
            throw new InvalidDataException("Heat projection documents cannot contain negative values");
        var body = JsonSerializer.Serialize(documents.Select(document => new
        {
            id = document.Id,
            heat1d = document.Heat1d,
            heat7d = document.Heat7d,
            heat15d = document.Heat15d,
            heat30d = document.Heat30d
        }));
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/indexes/{indexUid}/documents")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        using var response = await _http.SendAsync(request, ct);
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Meilisearch heat submission returned {(int)response.StatusCode}: {Bound(responseBody)}");
        using var json = JsonDocument.Parse(responseBody);
        if (json.RootElement.TryGetProperty("taskUid", out var uid) && uid.TryGetInt64(out var value))
            return value;
        throw new InvalidOperationException("Meilisearch heat response omitted taskUid");
    }

    public async Task<MeiliSearchResult> SearchAsync(
        string query, string heatWindow, int page, int pageSize, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            q = query,
            offset = (page - 1) * pageSize,
            limit = pageSize,
            sort = new[] { $"heat{heatWindow}:desc", "firstSeen:desc" },
            attributesToRetrieve = new[] { "id", "heat1d", "heat7d", "heat15d", "heat30d" },
            matchingStrategy = string.IsNullOrWhiteSpace(query)
                ? "last"
                : SearchHelper.IsCjkQuery(query) ? "all" : "last"
        });

        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await _http.PostAsync("/indexes/torrents/search", content, ct);
        var json = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new MeiliSearchUnavailableException(response.StatusCode, Bound(json));
        try
        {
            return JsonSerializer.Deserialize<MeiliSearchResult>(json)
                   ?? throw new JsonException("empty search response");
        }
        catch (JsonException exception)
        {
            throw new MeiliSearchUnavailableException(
                response.StatusCode,
                $"invalid JSON: {Bound(json)}",
                exception);
        }
    }

    private static string Bound(string value) =>
        value.Length <= 1024 ? value : value[..1024];
}

public sealed record MeiliTaskState(string Status, string? Error);

public sealed class MeiliSearchUnavailableException : Exception
{
    public System.Net.HttpStatusCode StatusCode { get; }

    public MeiliSearchUnavailableException(
        System.Net.HttpStatusCode statusCode,
        string detail,
        Exception? innerException = null)
        : base($"Meilisearch search returned {(int)statusCode}: {detail}", innerException)
    {
        StatusCode = statusCode;
    }
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
    [JsonPropertyName("id")]
    public long Id { get; set; }
    [JsonPropertyName("heat1d")] public long Heat1d { get; set; }
    [JsonPropertyName("heat7d")] public long Heat7d { get; set; }
    [JsonPropertyName("heat15d")] public long Heat15d { get; set; }
    [JsonPropertyName("heat30d")] public long Heat30d { get; set; }
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


