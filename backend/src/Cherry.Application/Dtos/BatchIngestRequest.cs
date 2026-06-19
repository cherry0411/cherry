using System.Text.Json.Serialization;

namespace Cherry.Application.Dtos;

public class BatchIngestRequest
{
    [JsonPropertyName("events")]
    public List<CrawlerEvent> Events { get; init; } = [];
}

public class CrawlerEvent
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; init; }

    [JsonPropertyName("instance_id")]
    public string InstanceId { get; init; } = string.Empty;

    [JsonPropertyName("info_hash")]
    public string InfoHash { get; init; } = string.Empty;

    [JsonPropertyName("metadata")]
    public CrawlerMetadata? Metadata { get; init; }
}

public class CrawlerMetadata
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("piece_length")]
    public int PieceLength { get; init; }

    [JsonPropertyName("length")]
    public long Length { get; init; }

    [JsonPropertyName("file_count")]
    public int FileCount { get; init; }

    [JsonPropertyName("private")]
    public bool Private { get; init; }

    [JsonPropertyName("files")]
    public List<CrawlerFile> Files { get; init; } = [];
}

public class CrawlerFile
{
    [JsonPropertyName("path_text")]
    public string PathText { get; init; } = string.Empty;

    [JsonPropertyName("length")]
    public long Length { get; init; }
}

public class PeerCountsRequest
{
    [JsonPropertyName("hashes")]
    public Dictionary<string, int> Hashes { get; init; } = [];
}
