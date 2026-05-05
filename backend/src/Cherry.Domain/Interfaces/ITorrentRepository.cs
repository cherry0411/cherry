using Cherry.Domain.Entities;

namespace Cherry.Domain.Interfaces;

public interface ITorrentRepository
{
    Task<long> BulkInsertTorrentsAsync(List<Torrent> torrents, CancellationToken ct = default);
    Task<Torrent?> GetByInfoHashAsync(string infoHash, CancellationToken ct = default);
    Task<(List<Torrent> Items, long Total)> SearchAsync(
        string query, int page, int pageSize, string? fileType = null, CancellationToken ct = default);
    Task<List<Torrent>> GetRecentAsync(int count, CancellationToken ct = default);
    Task<List<string>> CheckExistsAsync(List<string> hashes, CancellationToken ct = default);
    Task BatchUpdatePeerCountsAsync(Dictionary<string, int> counts, CancellationToken ct = default);
    Task<long> GetTotalCountAsync(CancellationToken ct = default);
    Task<long> GetTodayCountAsync(CancellationToken ct = default);
}
