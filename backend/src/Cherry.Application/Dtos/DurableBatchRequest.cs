using System.Text.Json.Serialization;

namespace Cherry.Application.Dtos;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableBatchRequest
{
    [JsonPropertyName("schema_version")]
    public int SchemaVersion { get; init; }

    [JsonPropertyName("crawler_id")]
    public string? CrawlerId { get; init; }

    [JsonPropertyName("epoch")]
    public ulong Epoch { get; init; }

    [JsonPropertyName("start_sequence")]
    public ulong StartSequence { get; init; }

    [JsonPropertyName("end_sequence")]
    public ulong EndSequence { get; init; }

    [JsonPropertyName("payload_sha256")]
    public string? PayloadSha256 { get; init; }

    [JsonPropertyName("events")]
    public List<DurableBatchEvent>? Events { get; init; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableBatchEvent
{
    [JsonPropertyName("info_hash")]
    public string? InfoHash { get; init; }

    [JsonPropertyName("encoding")]
    public string? Encoding { get; init; }

    [JsonPropertyName("first_seen")]
    public DateTimeOffset? FirstSeen { get; init; }

    [JsonPropertyName("decision_code")]
    public short DecisionCode { get; init; }

    [JsonPropertyName("normalized")]
    public DurableNormalizedMetadata? Normalized { get; init; }

    [JsonPropertyName("summary")]
    public DurableSummaryMetadata? Summary { get; init; }

}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableNormalizedMetadata
{
    [JsonPropertyName("name")]
    public string? Name { get; init; }

    [JsonPropertyName("total_length")]
    public ulong TotalLength { get; init; }

    [JsonPropertyName("files")]
    public List<DurableBatchFile>? Files { get; init; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableBatchFile
{
    [JsonPropertyName("path")]
    public string? Path { get; init; }

    [JsonPropertyName("length")]
    public ulong Length { get; init; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableSummaryMetadata
{
    [JsonPropertyName("name")]
    public string? Name { get; init; }

    [JsonPropertyName("total_length")]
    public ulong TotalLength { get; init; }

    [JsonPropertyName("file_count")]
    public uint FileCount { get; init; }

    [JsonPropertyName("representative_files")]
    public List<DurableBatchFile>? RepresentativeFiles { get; init; }

    [JsonPropertyName("extensions")]
    public List<DurableExtensionSummary>? Extensions { get; init; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableExtensionSummary
{
    [JsonPropertyName("extension")]
    public string? Extension { get; init; }

    [JsonPropertyName("files")]
    public uint Files { get; init; }

    [JsonPropertyName("bytes")]
    public ulong Bytes { get; init; }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class DurableBatchResponse
{
    [JsonPropertyName("crawler_id")]
    public required string CrawlerId { get; init; }

    [JsonPropertyName("epoch")]
    public required ulong Epoch { get; init; }

    [JsonPropertyName("start_sequence")]
    public required ulong StartSequence { get; init; }

    [JsonPropertyName("end_sequence")]
    public required ulong EndSequence { get; init; }

    [JsonPropertyName("payload_sha256")]
    public required string PayloadSha256 { get; init; }

    [JsonPropertyName("accepted")]
    public required int Accepted { get; init; }

    [JsonPropertyName("duplicates")]
    public required int Duplicates { get; init; }

    [JsonPropertyName("errors")]
    public int Errors { get; init; }

    [JsonPropertyName("committed")]
    public required bool Committed { get; init; }

    [JsonPropertyName("expected_start")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public ulong? ExpectedStart { get; init; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; init; }
}
