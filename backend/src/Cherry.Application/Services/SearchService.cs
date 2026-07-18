using Cherry.Application.Dtos;
using Cherry.Domain.Interfaces;

namespace Cherry.Application.Services;

public class SearchService
{
    private readonly ITorrentRepository _repo;
    private readonly IProcessedHashFilter _processedHashFilter;

    public SearchService(ITorrentRepository repo, IProcessedHashFilter processedHashFilter)
    {
        _repo = repo;
        _processedHashFilter = processedHashFilter;
    }

    public async Task<SearchResponse> SearchAsync(SearchRequest request, CancellationToken ct = default)
    {
        var (items, total, heatAsOfUtc, heatCoverageHours) = await _repo.SearchAsync(
            request.Query,
            request.HeatWindow,
            request.Page,
            request.PageSize,
            ct);

        var dtos = items.Select(t => new TorrentDto(
            InfoHash: t.InfoHash,
            MagnetLink: $"magnet:?xt=urn:btih:{t.InfoHash}",
            Name: t.Name,
            TotalLength: t.TotalLength,
            FileCount: t.FileCount,
            CreatedAt: t.CreatedAt,
            Files: null,
            Heat24h: t.Heat24h,
            Heat3d: t.Heat3d,
            Heat7d: t.Heat7d,
            Heat15d: t.Heat15d
        )).ToList();

        return new SearchResponse(dtos, total, request.Page, request.PageSize, heatAsOfUtc, heatCoverageHours);
    }

    public async Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct)
    {
        // The probabilistic filter is only a negative fast-path after a complete
        // exact-store replay. Every positive is confirmed by PostgreSQL. During
        // startup rebuild or after any filter failure, query all candidates.
        var candidates = _processedHashFilter.IsReady
            ? hashes.Where(_processedHashFilter.MightContain).ToList()
            : hashes;

        return candidates.Count == 0
            ? []
            : await _repo.CheckProcessedAsync(candidates, ct);
    }

    public async Task<List<TorrentDto>> GetRecentAsync(CancellationToken ct = default)
    {
        var items = await _repo.GetRecentAsync(100, ct);
        return items.Select(t => new TorrentDto(
            InfoHash: t.InfoHash,
            MagnetLink: $"magnet:?xt=urn:btih:{t.InfoHash}",
            Name: t.Name,
            TotalLength: t.TotalLength,
            FileCount: t.FileCount,
            CreatedAt: t.CreatedAt,
            Files: null,
            Heat24h: 0,
            Heat3d: 0,
            Heat7d: 0,
            Heat15d: 0
        )).ToList();
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
            CreatedAt: t.CreatedAt,
            Files: t.Files.Select(f => new TorrentFileDto(f.PathText, f.Length)).ToList(),
            Heat24h: 0,
            Heat3d: 0,
            Heat7d: 0,
            Heat15d: 0
        );
    }
}
