namespace Cherry.Domain.Entities;

public class TorrentRequest
{
    public long Id { get; set; }
    public string InfoHash { get; set; } = string.Empty;
    public string Status { get; set; } = "pending"; // pending, done
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
