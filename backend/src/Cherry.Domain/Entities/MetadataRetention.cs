namespace Cherry.Domain.Entities;

// Historical migration compatibility only. The compact runtime model does not
// persist or expose a retention level.
public enum MetadataRetentionLevel : short
{
    HashOnly = 1,
    Summary = 2,
    Normalized = 3
}

public enum MetadataDecisionCode : short
{
    HashOnly = 1,
    Reject = 2,
    HashOnlyFileCap = 3,
    RejectFileCap = 4,
    InvalidMetadata = 5
}

public sealed class MetadataDecision
{
    public byte[] InfoHash { get; set; } = [];
    public MetadataDecisionCode DecisionCode { get; set; }
}

public sealed class TorrentExtensionSummary
{
    public long TorrentId { get; set; }
    // Transient ingest correlation only. PostgreSQL stores the compact TorrentId.
    public string InfoHash { get; set; } = string.Empty;
    public string Extension { get; set; } = string.Empty;
    public int FileCount { get; set; }
    public long TotalLength { get; set; }
}
