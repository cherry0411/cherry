namespace Cherry.Domain.Entities;

public class Torrent
{
    public long Id { get; set; }
    public string InfoHash { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public long TotalLength { get; set; }
    public int FileCount { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public List<TorrentFile> Files { get; set; } = [];
    public List<TorrentExtensionSummary> ExtensionSummaries { get; set; } = [];
}
