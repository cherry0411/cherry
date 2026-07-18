namespace Cherry.Domain.Entities;

public sealed class DurableBatchReceipt
{
    public string CrawlerId { get; set; } = string.Empty;
    public long Epoch { get; set; }
    public long LastStartSequence { get; set; }
    public long LastEndSequence { get; set; }
    public string LastPayloadSha256 { get; set; } = string.Empty;
    public int LastAccepted { get; set; }
    public int LastDuplicates { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
