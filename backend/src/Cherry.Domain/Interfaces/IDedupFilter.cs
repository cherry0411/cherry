namespace Cherry.Domain.Interfaces;

public interface IDedupFilter
{
    bool MightContain(string infoHash);
    bool Add(string infoHash);
    long Count { get; }
}
