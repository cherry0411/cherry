namespace Cherry.Application.Dtos;

public record StatsResponse(
    long TotalTorrents,
    long TodayNew,
    long DedupFilterSize,
    DateTime ServerTime
);
