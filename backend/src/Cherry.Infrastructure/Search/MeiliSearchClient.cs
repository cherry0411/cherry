using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cherry.Infrastructure.Search;

public class MeiliSearchClient
{
    private readonly HttpClient _http;

    public MeiliSearchClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<MeiliSearchResult?> SearchAsync(string query, int page, int pageSize, string? fileType, CancellationToken ct)
    {
        var sort = "peerCount:desc";
        var filter = "";
        if (!string.IsNullOrWhiteSpace(fileType))
            filter = $"fileCount > 0"; // fileType filter handled post-query

        var url = "/indexes/torrents/search";
        var body = JsonSerializer.Serialize(new
        {
            q = query,
            offset = (page - 1) * pageSize,
            limit = pageSize,
            sort = new[] { sort },
            filter = filter,
            attributesToRetrieve = new[] { "infoHash" },
            matchingStrategy = SearchHelper.IsCjkQuery(query) ? "all" : "last"
        });

        var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        var response = await _http.PostAsync(url, content, ct);
        if (!response.IsSuccessStatusCode) return null;
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<MeiliSearchResult>(json);
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
    [JsonPropertyName("infoHash")]
    public string InfoHash { get; set; } = string.Empty;
}

// Helper: returns true if query contains CJK characters (needs stricter matching)
public static class SearchHelper
{
    public static bool IsCjkQuery(string query)
    {
        foreach (var c in query)
            if (c >= 0x4E00 && c <= 0x9FFF) // CJK Unified Ideographs
                return true;
        return false;
    }
}
