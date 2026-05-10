namespace Cherry.Domain.Interfaces;

/// <summary>
/// Tracks infohashes that were rejected by crawler filter rules so they are
/// not re-processed on subsequent crawl sessions.
/// </summary>
public interface IRejectedHashStore
{
    /// <summary>
    /// Records <paramref name="infoHash"/> as rejected.
    /// Returns <c>true</c> for a new insertion.
    /// </summary>
    bool Add(string infoHash);

    /// <summary>
    /// Returns <c>true</c> when the hash is almost certainly in the rejected
    /// set (probabilistic, ~0.1 % false-positive rate is acceptable here).
    /// </summary>
    bool Contains(string infoHash);
}
