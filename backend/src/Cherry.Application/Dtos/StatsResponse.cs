using Cherry.Domain.Entities;

namespace Cherry.Application.Dtos;

public record StatsResponse(
    // Legacy alias retained for existing clients. This is a PostgreSQL catalog
    // estimate, not a durable ingest counter.
    long TotalTorrents,
    long PgCatalogEstimate,
    long TodayNew,
    long DedupFilterSize,
    DurableIngestStatistics DurableIngest,
    DateTime ServerTime
);
