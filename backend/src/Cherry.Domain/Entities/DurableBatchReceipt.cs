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
    public long TotalDelivered { get; set; }
    public long TotalAccepted { get; set; }
    public long TotalDuplicates { get; set; }
    public long TotalMetadataCommitted { get; set; }
    public long TotalPolicyCommitted { get; set; }
    public long TotalCommittedBatches { get; set; }
    public DateTime CountersStartedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
