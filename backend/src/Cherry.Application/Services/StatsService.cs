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
        var durableIngest = await _repo.GetDurableIngestStatisticsAsync(ct);

        return new StatsResponse(
            TotalTorrents: total,
            PgCatalogEstimate: total,
            TodayNew: today,
            DedupFilterSize: _dedup.Count,
            DurableIngest: durableIngest,
            ServerTime: DateTime.UtcNow
        );
    }
}
