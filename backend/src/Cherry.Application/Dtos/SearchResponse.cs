namespace Cherry.Application.Dtos;

public record SearchResponse(
    List<TorrentDto> Items,
    long Total,
    int Page,
    int PageSize
);

public record TorrentDto(
    string InfoHash,
    string MagnetLink,
    string Name,
    long TotalLength,
    int FileCount,
    bool IsPrivate,
    DateTime CreatedAt,
    List<TorrentFileDto>? Files
);

public record TorrentFileDto(
    string PathText,
    long Length
);
