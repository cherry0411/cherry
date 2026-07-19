namespace Cherry.Domain.Entities;

public sealed record DurableCrawlerEpochStatistics(
    string CrawlerId,
    long Epoch,
    long DeliveredRecords,
    long AcceptedRecords,
    long DuplicateRecords,
    long MetadataCommitted,
    long PolicyCommitted,
    long CommittedBatches,
    DateTime CountersStartedAt,
    DateTime ReceiptUpdatedAt,
    DateTime? LastCountedCommitAt);

public sealed record DurableIngestStatistics(
    long DeliveredRecords,
    long AcceptedRecords,
    long DuplicateRecords,
    long MetadataCommitted,
    long PolicyCommitted,
    long CommittedBatches,
    DateTime? CountersStartedAt,
    DateTime? LastCommittedAt,
    IReadOnlyList<DurableCrawlerEpochStatistics> CrawlerEpochs)
{
    public static readonly DurableIngestStatistics Empty = new(
        0, 0, 0, 0, 0, 0, null, null, []);
}
