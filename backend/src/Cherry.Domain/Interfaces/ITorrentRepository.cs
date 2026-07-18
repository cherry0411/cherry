using Cherry.Domain.Entities;

namespace Cherry.Domain.Interfaces;

public interface ITorrentRepository
{
    Task<IReadOnlySet<string>> BulkInsertTorrentsAsync(List<Torrent> torrents, CancellationToken ct = default);
    Task<IReadOnlySet<string>> AddRejectedHashesAsync(
        IReadOnlyCollection<string> infoHashes,
        CancellationToken ct = default);
    Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default);
    Task<(List<Torrent> Items, long Total, DateTime? HeatAsOfUtc, int HeatCoverageHours)> SearchAsync(
        string query, string heatWindow, int page, int pageSize, CancellationToken ct = default);
    Task<List<Torrent>> GetRecentAsync(int count, CancellationToken ct = default);
    Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default);
    Task<List<string>> CheckProcessedAsync(List<string> hashes, CancellationToken ct = default);
    IAsyncEnumerable<string> StreamProcessedHashesAsync(CancellationToken ct = default);
    Task<long> GetTotalCountAsync(CancellationToken ct = default);
    Task<long> GetTodayCountAsync(CancellationToken ct = default);
    Task MarkRequestsDoneAsync(IEnumerable<string> infoHashes, CancellationToken ct = default);
}
