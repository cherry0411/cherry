namespace Cherry.Domain.Entities;

/// <summary>
/// A compact, coalescing marker that says the authoritative torrent row must
/// be projected to search. The search document itself is never duplicated
/// here and no raw metadata is retained.
/// </summary>
public sealed class SearchOutboxItem
{
    public string InfoHash { get; set; } = string.Empty;
    public long Generation { get; set; } = 1;
    public DateTime EnqueuedAt { get; set; } = DateTime.UtcNow;
    public DateTime AvailableAt { get; set; } = DateTime.UtcNow;
    public Guid? LeaseOwner { get; set; }
    public DateTime? LeaseUntil { get; set; }
    public int AttemptCount { get; set; }
    public string? LastError { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
