namespace Cherry.Domain.Entities;

public class TorrentFile
{
    public long Id { get; set; }
    public long TorrentId { get; set; }
    public string PathText { get; set; } = string.Empty;
    public long Length { get; set; }

    public Torrent Torrent { get; set; } = null!;
}
