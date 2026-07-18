namespace Cherry.Domain.Entities;

public class Torrent
{
    public string InfoHash { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int PieceLength { get; set; }
    public long TotalLength { get; set; }
    public int FileCount { get; set; }
    public bool IsPrivate { get; set; }
    public string? Source { get; set; }
    public string? PolicyId { get; set; }
    public string? Region { get; set; }
    public MetadataRetentionLevel RetainedLevel { get; set; } = MetadataRetentionLevel.Normalized;
    public bool NeedsRefetch { get; set; }
    public int PeerCount { get; set; }
    public DateTime? PeerUpdatedAt { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    public List<TorrentFile> Files { get; set; } = [];
    public List<TorrentExtensionSummary> ExtensionSummaries { get; set; } = [];
}
