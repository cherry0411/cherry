using System.Globalization;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Cherry.Infrastructure.Heat;

public sealed class HeatOptions
{
    private static readonly byte[] UnknownCrawlerKey =
        SHA256.HashData(Encoding.UTF8.GetBytes("cherry/heat/unknown-crawler/fail-closed/v1"));
    private byte[]? _decodedSecret;
    private byte[]? _decodedDailyActorSecret;
    private readonly ConcurrentDictionary<string, byte[]> _decodedCrawlerSecrets = new(StringComparer.Ordinal);
    public bool Enabled { get; init; }
    public string SharedSecret { get; init; } = string.Empty;
    public string DailyActorSecret { get; init; } = string.Empty;
    public Dictionary<string, string> CrawlerSecrets { get; init; } = new(StringComparer.Ordinal);
    public string DataDirectory { get; init; } = "data/heat";
    public string IndexUid { get; init; } = "torrents";
    public string IndexGeneration { get; init; } = "torrents-heat-v2";
    public string? CoverageStartDay { get; init; }
    public string[] ExpectedCrawlerIds { get; init; } = [];
    public int LateGraceMinutes { get; init; } = 30;
    public int MaxRequestBytes { get; init; } = 8 * 1024 * 1024;
    public int MaxRecordsPerBatch { get; init; } = 250_000;
    public int ChannelCapacity { get; init; } = 64;
    public int CommitBatchRequests { get; init; } = 8;
    public int ProjectionBatchSize { get; init; } = 500;
    public int LifecyclePollSeconds { get; init; } = 30;
    public long RollingMaxBytes { get; init; } = 5L * 1024 * 1024 * 1024;
    public long RollingMinFreeBytes { get; init; } = 2L * 1024 * 1024 * 1024;

    public DateOnly? ParsedCoverageStartDay =>
        DateOnly.TryParseExact(
            CoverageStartDay,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out var value)
            ? value
            : null;

    public HeatOptions Normalize(string contentRoot)
    {
        var path = Path.IsPathRooted(DataDirectory)
            ? DataDirectory
            : Path.GetFullPath(Path.Combine(contentRoot, DataDirectory));
        return new HeatOptions
        {
            Enabled = Enabled,
            SharedSecret = SharedSecret,
            DailyActorSecret = DailyActorSecret,
            CrawlerSecrets = CrawlerSecrets
                .Where(pair => !string.IsNullOrWhiteSpace(pair.Key) && !string.IsNullOrWhiteSpace(pair.Value))
                .ToDictionary(pair => pair.Key.Trim(), pair => pair.Value.Trim(), StringComparer.Ordinal),
            DataDirectory = path,
            IndexUid = string.IsNullOrWhiteSpace(IndexUid) ? "torrents" : IndexUid.Trim(),
            IndexGeneration = string.IsNullOrWhiteSpace(IndexGeneration)
                ? "torrents-heat-v2"
                : IndexGeneration.Trim(),
            CoverageStartDay = CoverageStartDay,
            ExpectedCrawlerIds = ExpectedCrawlerIds
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Select(value => value.Trim())
                .Distinct(StringComparer.Ordinal)
                .ToArray(),
            LateGraceMinutes = Math.Clamp(LateGraceMinutes, 0, 12 * 60),
            MaxRequestBytes = Math.Clamp(MaxRequestBytes, 1024, 64 * 1024 * 1024),
            MaxRecordsPerBatch = Math.Clamp(MaxRecordsPerBatch, 1, 1_000_000),
            ChannelCapacity = Math.Clamp(ChannelCapacity, 1, 4096),
            CommitBatchRequests = Math.Clamp(CommitBatchRequests, 1, 64),
            ProjectionBatchSize = Math.Clamp(ProjectionBatchSize, 1, 5000),
            LifecyclePollSeconds = Math.Clamp(LifecyclePollSeconds, 5, 3600),
            RollingMaxBytes = Math.Clamp(RollingMaxBytes, 64L * 1024 * 1024, 64L * 1024 * 1024 * 1024),
            RollingMinFreeBytes = Math.Clamp(RollingMinFreeBytes, 256L * 1024 * 1024, 32L * 1024 * 1024 * 1024)
        };
    }

    public byte[] DecodeSecret()
    {
        if (!Enabled) return [];
        var cached = Volatile.Read(ref _decodedSecret);
        if (cached is not null) return cached;
        try
        {
            var secret = Convert.FromBase64String(SharedSecret);
            if (secret.Length < 32)
                throw new InvalidOperationException("Heat:SharedSecret must decode to at least 32 bytes");
            return Interlocked.CompareExchange(ref _decodedSecret, secret, null) ?? secret;
        }
        catch (FormatException exception)
        {
            throw new InvalidOperationException("Heat:SharedSecret must be base64", exception);
        }
    }

    public byte[] DecodeDailyActorSecret()
    {
        if (!Enabled) return [];
        var cached = Volatile.Read(ref _decodedDailyActorSecret);
        if (cached is not null) return cached;
        var secret = DecodeBase64Secret(DailyActorSecret, "Heat:DailyActorSecret");
        return Interlocked.CompareExchange(ref _decodedDailyActorSecret, secret, null) ?? secret;
    }

    public byte[] DecodeSecret(string crawlerId)
    {
        if (!Enabled) return [];
        if (!CrawlerSecrets.TryGetValue(crawlerId, out var encoded))
            return string.IsNullOrWhiteSpace(SharedSecret)
                ? UnknownCrawlerKey
                : DecodeSecret(); // Test/migration compatibility; Program forbids this for expected crawlers.
        return _decodedCrawlerSecrets.GetOrAdd(crawlerId, _ => DecodeBase64Secret(
            encoded, $"Heat:CrawlerSecrets:{crawlerId}"));
    }

    // Empty remains useful for protocol/unit fixtures. Production startup
    // requires a non-empty list, so an authenticated but unconfigured crawler
    // can never contribute observations or completion evidence.
    public bool IsExpectedCrawler(string crawlerId) =>
        ExpectedCrawlerIds.Length == 0 ||
        ExpectedCrawlerIds.Contains(crawlerId, StringComparer.Ordinal);

    public void ValidateExpectedCrawlerSecrets()
    {
        var distinctKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var crawlerId in ExpectedCrawlerIds)
        {
            if (!CrawlerSecrets.ContainsKey(crawlerId))
                throw new InvalidOperationException(
                    $"Heat:CrawlerSecrets:{crawlerId} is required; expected crawlers may not share transport HMAC keys");
            var key = DecodeSecret(crawlerId);
            if (!distinctKeys.Add(Convert.ToHexString(SHA256.HashData(key))))
                throw new InvalidOperationException(
                    "Heat:CrawlerSecrets values must be distinct for every expected crawler");
        }
    }

    public void ValidateDailyActorSecretIsolation()
    {
        var dailyDigest = SHA256.HashData(DecodeDailyActorSecret());
        foreach (var crawlerId in ExpectedCrawlerIds)
            if (CryptographicOperations.FixedTimeEquals(
                    dailyDigest,
                    SHA256.HashData(DecodeSecret(crawlerId))))
                throw new InvalidOperationException(
                    "Heat:DailyActorSecret must be distinct from every crawler transport key");
    }

    private static byte[] DecodeBase64Secret(string encoded, string name)
    {
        try
        {
            var secret = Convert.FromBase64String(encoded);
            if (secret.Length < 32)
                throw new InvalidOperationException($"{name} must decode to at least 32 bytes");
            return secret;
        }
        catch (FormatException exception)
        {
            throw new InvalidOperationException($"{name} must be base64", exception);
        }
    }
}

public sealed class HeatRuntimeMetrics
{
    private long _acceptedBatches;
    private long _acceptedRecords;
    private long _replayedBatches;
    private long _rejectedBatches;
    private long _sealedDays;
    private long _projectedDocuments;
    private long _lastCommitTicks;
    private long _lastSealTicks;
    private long _lastProjectionTicks;
    private string? _lastFailure;

    public void Accepted(int records, bool replay)
    {
        Interlocked.Increment(ref _acceptedBatches);
        Interlocked.Add(ref _acceptedRecords, records);
        if (replay) Interlocked.Increment(ref _replayedBatches);
        Interlocked.Exchange(ref _lastCommitTicks, DateTime.UtcNow.Ticks);
    }

    public void Rejected() => Interlocked.Increment(ref _rejectedBatches);

    public void Sealed()
    {
        Interlocked.Increment(ref _sealedDays);
        Interlocked.Exchange(ref _lastSealTicks, DateTime.UtcNow.Ticks);
    }

    public void Projected(int documents)
    {
        Interlocked.Add(ref _projectedDocuments, documents);
        Interlocked.Exchange(ref _lastProjectionTicks, DateTime.UtcNow.Ticks);
    }

    public void Fail(Exception exception) =>
        Volatile.Write(ref _lastFailure, $"{exception.GetType().Name}: {exception.Message}"[..Math.Min(512, exception.GetType().Name.Length + exception.Message.Length + 2)]);

    public void ClearFailure() => Volatile.Write(ref _lastFailure, null);

    public HeatMetricsSnapshot Snapshot(bool enabled) => new(
        enabled,
        Interlocked.Read(ref _acceptedBatches),
        Interlocked.Read(ref _acceptedRecords),
        Interlocked.Read(ref _replayedBatches),
        Interlocked.Read(ref _rejectedBatches),
        Interlocked.Read(ref _sealedDays),
        Interlocked.Read(ref _projectedDocuments),
        ReadTime(ref _lastCommitTicks),
        ReadTime(ref _lastSealTicks),
        ReadTime(ref _lastProjectionTicks),
        Volatile.Read(ref _lastFailure));

    private static DateTime? ReadTime(ref long ticks)
    {
        var value = Interlocked.Read(ref ticks);
        return value == 0 ? null : new DateTime(value, DateTimeKind.Utc);
    }
}

public sealed record HeatMetricsSnapshot(
    bool Enabled,
    long AcceptedBatches,
    long AcceptedRecords,
    long ReplayedBatches,
    long RejectedBatches,
    long SealedDays,
    long ProjectedDocuments,
    DateTime? LastCommitAt,
    DateTime? LastSealAt,
    DateTime? LastProjectionAt,
    string? LastFailure);
