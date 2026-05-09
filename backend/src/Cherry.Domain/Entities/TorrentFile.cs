namespace Cherry.Domain.Entities;

public class TorrentFile
{
    public string InfoHash { get; set; } = string.Empty;
    public string PathText { get; set; } = string.Empty;
    public long Length { get; set; }
}
