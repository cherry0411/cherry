namespace Cherry.Domain.Entities;

public enum MetadataRetentionLevel : short
{
    HashOnly = 1,
    Summary = 2,
    Normalized = 3
}

public enum MetadataDecisionAction : short
{
    HashOnly = 1,
    Reject = 2
}

public sealed class MetadataDecision
{
    public byte[] InfoHash { get; set; } = [];
    public MetadataDecisionAction Action { get; set; }
    public MetadataRetentionLevel RetainedLevel { get; set; } = MetadataRetentionLevel.HashOnly;
    public bool NeedsRefetch { get; set; }
    public string? PolicyId { get; set; }
    public string Reason { get; set; } = string.Empty;
    public DateTime? FirstSeen { get; set; }
    public string? Region { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

public sealed class TorrentExtensionSummary
{
    public string InfoHash { get; set; } = string.Empty;
    public string Extension { get; set; } = string.Empty;
    public int FileCount { get; set; }
    public long TotalLength { get; set; }
}
