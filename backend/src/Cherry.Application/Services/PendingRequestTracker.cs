namespace Cherry.Application.Services;

/// <summary>
/// 内存索引，记录当前所有 status=pending 的 TorrentRequest infohash。
/// 用于在 ingest 批处理前快速过滤，避免绝大多数 batch（无对应请求）触发无意义的 DB UPDATE。
/// 生命周期：singleton。启动时由 Program.cs 从 DB 初始化，运行期随请求增删。
/// </summary>
public sealed class PendingRequestTracker
{
    private readonly HashSet<string> _set = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    public void Track(string hash)
    {
        lock (_lock) { _set.Add(hash); }
    }

    public void TrackMany(IEnumerable<string> hashes)
    {
        lock (_lock) { foreach (var h in hashes) _set.Add(h); }
    }

    /// <summary>返回 hashes 中在追踪集合里的子集。若为空则无需走 DB。</summary>
    public List<string> Filter(IEnumerable<string> hashes)
    {
        lock (_lock) { return hashes.Where(h => _set.Contains(h)).ToList(); }
    }

    public void Untrack(IEnumerable<string> hashes)
    {
        lock (_lock) { foreach (var h in hashes) _set.Remove(h); }
    }

    public bool IsEmpty
    {
        get { lock (_lock) { return _set.Count == 0; } }
    }
}
