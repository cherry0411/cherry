namespace Cherry.Domain.Entities;

public class TorrentFile
{
    public long TorrentId { get; set; }
    // Transient ingest correlation only. PostgreSQL stores the compact TorrentId.
    public string InfoHash { get; set; } = string.Empty;
    public string PathText { get; set; } = string.Empty;
    public long Length { get; set; }
}
