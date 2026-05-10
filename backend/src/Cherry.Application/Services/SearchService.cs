using Cherry.Application.Dtos;
using Cherry.Domain.Interfaces;

namespace Cherry.Application.Services;

public class SearchService
{
    private readonly ITorrentRepository _repo;
    private readonly IDedupFilter _dedup;
    private readonly IRejectedHashStore _rejected;

    public SearchService(ITorrentRepository repo, IDedupFilter dedup, IRejectedHashStore rejected)
    {
        _repo = repo;
        _dedup = dedup;
        _rejected = rejected;
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
        var result = new HashSet<string>(hashes.Count, StringComparer.Ordinal);

        // Fast-path: hashes in the rejected store are "already processed" —
        // return them immediately without a DB round-trip.
        var remaining = new List<string>(hashes.Count);
        foreach (var h in hashes)
        {
            if (_rejected.Contains(h))
                result.Add(h);
            else
                remaining.Add(h);
        }

        // DB check for the rest (cuckoo pre-filter eliminates most misses).
        if (remaining.Count > 0)
        {
            var candidates = remaining.Where(h => _dedup.MightContain(h)).ToList();
            if (candidates.Count > 0)
            {
                var dbFound = await _repo.CheckExistsAsync(candidates, ct);
                foreach (var h in dbFound)
                    result.Add(h);
            }
        }

        return [.. result];
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
