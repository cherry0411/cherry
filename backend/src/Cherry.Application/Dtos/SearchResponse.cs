using System.Text.Json.Serialization;

namespace Cherry.Application.Dtos;

public record SearchResponse(
    List<TorrentDto> Items,
    long Total,
    int Page,
    int PageSize,
    DateTime? HeatAsOfUtc,
    int HeatCoverageHours
);

public record TorrentDto(
    string InfoHash,
    string MagnetLink,
    string Name,
    long TotalLength,
    int FileCount,
    DateTime CreatedAt,
    List<TorrentFileDto>? Files,
    long Heat24h,
    long Heat3d,
    long Heat7d,
    long Heat15d
);

public record TorrentFileDto(
    string PathText,
    long Length
);

public record TorrentRequestDto
{
    [JsonPropertyName("info_hash")]
    public string InfoHash { get; init; } = string.Empty;
}
