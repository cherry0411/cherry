using Cherry.Application.Dtos;
using Cherry.Domain.Interfaces;

namespace Cherry.Application.Services;

public class SearchService
{
    private readonly ITorrentRepository _repo;

    public SearchService(ITorrentRepository repo)
    {
        _repo = repo;
    }

    public async Task<SearchResponse> SearchAsync(SearchRequest request, CancellationToken ct = default)
    {
        var (items, total) = await _repo.SearchAsync(
            request.Query,
            request.Page,
            request.PageSize,
            request.FileType,
            ct);

        var dtos = items.Select(t => new TorrentDto(
            InfoHash: t.InfoHash,
            MagnetLink: $"magnet:?xt=urn:btih:{t.InfoHash}",
            Name: t.Name,
            TotalLength: t.TotalLength,
            FileCount: t.FileCount,
            IsPrivate: t.IsPrivate,
            CreatedAt: t.CreatedAt,
            Files: null // Don't include files in search results for performance
        )).ToList();

        return new SearchResponse(dtos, total, request.Page, request.PageSize);
    }

    public async Task<TorrentDto?> GetDetailAsync(string infoHash, CancellationToken ct = default)
    {
        var t = await _repo.GetByInfoHashAsync(infoHash.ToLowerInvariant(), ct);
        if (t is null) return null;

        return new TorrentDto(
            InfoHash: t.InfoHash,
            MagnetLink: $"magnet:?xt=urn:btih:{t.InfoHash}",
            Name: t.Name,
            TotalLength: t.TotalLength,
            FileCount: t.FileCount,
            IsPrivate: t.IsPrivate,
            CreatedAt: t.CreatedAt,
            Files: t.Files.Select(f => new TorrentFileDto(f.PathText, f.Length)).ToList()
        );
    }
}
