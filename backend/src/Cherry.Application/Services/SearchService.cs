using Cherry.Application.Dtos;
using Cherry.Domain.Interfaces;

namespace Cherry.Application.Services;

public class SearchService
{
    private readonly ITorrentRepository _repo;
    private readonly IDedupFilter _dedup;

    public SearchService(ITorrentRepository repo, IDedupFilter dedup)
    {
        _repo = repo;
        _dedup = dedup;
    }

    public async Task<SearchResponse> SearchAsync(SearchRequest request, CancellationToken ct = default)
    {
        var (items, total) = await _repo.SearchAsync(
            request.Query,
            request.Page,
            request.PageSize,
            ct);

        var dtos = items.Select(t => new TorrentDto(
            InfoHash: t.InfoHash,
            MagnetLink: $"magnet:?xt=urn:btih:{t.InfoHash}",
            Name: t.Name,
            TotalLength: t.TotalLength,
            FileCount: t.FileCount,
            IsPrivate: t.IsPrivate,
            PeerCount: t.PeerCount,
            CreatedAt: t.CreatedAt,
            Files: null
        )).ToList();

        return new SearchResponse(dtos, total, request.Page, request.PageSize);
    }

    public async Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct)
    {
        // A1: Use CuckooFilter as a fast pre-filter.
        // Only hashes that pass the probabilistic check are confirmed against the DB.
        // False-positive rate is ~0.1% — negligible for the crawler's dedup use case.
        var candidates = hashes.Where(h => _dedup.MightContain(h)).ToList();
        if (candidates.Count == 0) return [];
        return await _repo.CheckExistsAsync(candidates, ct);
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
            IsPrivate: t.IsPrivate,
            PeerCount: t.PeerCount,
            CreatedAt: t.CreatedAt,
            Files: null
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
            IsPrivate: t.IsPrivate,
            PeerCount: t.PeerCount,
            CreatedAt: t.CreatedAt,
            Files: t.Files.Select(f => new TorrentFileDto(f.PathText, f.Length)).ToList()
        );
    }
}
