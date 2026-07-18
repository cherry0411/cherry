namespace Cherry.Domain.Entities;

/// <summary>
/// One compact, versioned detail payload per catalog row. The payload contains
/// only the retained file entries and extension aggregates; it deliberately
/// does not encode a normalized/summary retention label.
/// </summary>
public sealed class TorrentDetail
{
    public long TorrentId { get; set; }
    public byte[] Payload { get; set; } = [];
}
