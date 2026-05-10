using Cherry.Domain.Interfaces;

namespace Cherry.Infrastructure.Dedup;

/// <summary>
/// Stores infohashes that were explicitly rejected by crawler filter rules.
/// Uses a dedicated CuckooFilter so rejected hashes do not pollute the main
/// dedup filter and are never confused with hashes that exist in the database.
/// </summary>
public sealed class RejectedHashStore : IRejectedHashStore, IDisposable
{
    private readonly CuckooFilter _filter;

    public RejectedHashStore(string? persistPath = null)
    {
        // 10 M slots is ample for decades of rejected hashes at realistic rates.
        _filter = new CuckooFilter(capacity: 10_000_000, persistPath: persistPath);
    }

    /// <summary>
    /// Records <paramref name="infoHash"/> as rejected.
    /// Returns <c>true</c> if this was a new insertion, <c>false</c> if the
    /// hash was already present (or the filter is full).
    /// </summary>
    public bool Add(string infoHash) => _filter.Add(infoHash);

    /// <summary>
    /// Returns <c>true</c> when <paramref name="infoHash"/> is almost certainly
    /// in the rejected set (probabilistic; ~0.1% false-positive rate).
    /// </summary>
    public bool Contains(string infoHash) => _filter.MightContain(infoHash);

    /// <summary>Number of hashes in the rejected set.</summary>
    public long Count => _filter.Count;

    /// <summary>Persists the filter to disk (no-op when no path was configured).</summary>
    public void Save() => _filter.Save();

    public void Dispose() => _filter.Dispose();
}
