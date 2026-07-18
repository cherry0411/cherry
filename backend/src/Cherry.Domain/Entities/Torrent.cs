using System.ComponentModel.DataAnnotations.Schema;

namespace Cherry.Domain.Entities;

public class Torrent
{
    public long Id { get; set; }
    public string InfoHash { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public long TotalLength { get; set; }
    public int FileCount { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    [NotMapped] public long Heat1d { get; set; }
    [NotMapped] public long Heat7d { get; set; }
    [NotMapped] public long Heat15d { get; set; }
    [NotMapped] public long Heat30d { get; set; }

    public List<TorrentFile> Files { get; set; } = [];
    public List<TorrentExtensionSummary> ExtensionSummaries { get; set; } = [];
}
