namespace Cherry.Domain.Interfaces;

/// <summary>
/// Probabilistic negative fast-path. A <c>false</c> membership result is
/// definitive; a <c>true</c> result must be confirmed by an exact authority.
/// </summary>
public interface IDedupFilter
{
    bool MightContain(string infoHash);
    bool Add(string infoHash);
    long Count { get; }
}
