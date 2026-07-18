namespace Cherry.Domain.Interfaces;

/// <summary>
/// Optional probabilistic negative fast-path in front of the exact processed
/// hash store. Callers must bypass it unless <see cref="IsReady"/> is true and
/// must confirm every positive result with the exact store.
/// </summary>
public interface IProcessedHashFilter
{
    bool IsReady { get; }

    /// <summary>
    /// Returns a probabilistic membership result. This method must not be used
    /// as an exact authority.
    /// </summary>
    bool MightContain(string infoHash);

    /// <summary>
    /// Records hashes before an exact-store commit. False positives caused by a
    /// later rollback are harmless; an insertion failure disables the fast-path.
    /// </summary>
    void RecordCandidates(IEnumerable<string> infoHashes);
}
