using System.Text;
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
        // Create index if not exists, then update settings
        var body = JsonSerializer.Serialize(new { uid = "torrents", primaryKey = "infoHash" });
        await _http.PostAsync("/indexes", new StringContent(body, Encoding.UTF8, "application/json"), ct);
        await _http.PatchAsync("/indexes/torrents/settings", content, ct);
    }

    public async Task IndexDocumentsAsync(List<Cherry.Domain.Entities.Torrent> torrents, CancellationToken ct)
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
        }).ToList();

        var body = JsonSerializer.Serialize(docs);
        var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        await _http.PostAsync("/indexes/torrents/documents", content, ct);
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
            if (c >= 0x4E00 && c <= 0x9FFF)
                return true;
        return false;
    }
}

