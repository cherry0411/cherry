using Cherry.Application.Dtos;
using Cherry.Domain.Interfaces;

namespace Cherry.Application.Services;

public class StatsService
{
    private readonly ITorrentRepository _repo;
    private readonly IDedupFilter _dedup;

    public StatsService(ITorrentRepository repo, IDedupFilter dedup)
    {
        _repo = repo;
        _dedup = dedup;
    }

    public async Task<StatsResponse> GetStatsAsync(CancellationToken ct = default)
    {
        var total = await _repo.GetTotalCountAsync(ct);
        var today = await _repo.GetTodayCountAsync(ct);

        return new StatsResponse(
            TotalTorrents: total,
            TodayNew: today,
            DedupFilterSize: _dedup.Count,
            ServerTime: DateTime.UtcNow
        );
    }
}
