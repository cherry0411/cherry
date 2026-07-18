using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Cherry.Infrastructure.Heat;

public sealed record ChhtHashGroup(byte[] InfoHash, IReadOnlyList<long> ActorFingerprints);

public sealed record ChhtBatch(
    string CrawlerId,
    DateOnly Day,
    ulong Epoch,
    ulong Sequence,
    ulong EndSequence,
    IReadOnlyList<ChhtHashGroup> Groups,
    byte[] PayloadSha256)
{
    public int RecordCount => Groups.Sum(group => group.ActorFingerprints.Count);
}

public sealed record ChhtCompletion(
    string CrawlerId,
    DateOnly Day,
    ulong Epoch,
    ulong StartSequence,
    ulong NextSequence,
    bool Clean);

public enum ChhtProtocolError { Malformed, Authentication, Expired }
public sealed record ChhtClosedReceipt(
    string Crawler,
    DateOnly Day,
    ulong Epoch,
    ulong StartSequence,
    ulong EndSequence,
    byte[] PayloadSha256);
public sealed record ChhtClosedCompletion(
    string Crawler,
    DateOnly Day,
    ulong Epoch,
    ulong StartSequence,
    ulong NextSequence,
    bool Clean);

public sealed class ChhtProtocolException(
    string message,
    ChhtProtocolError error = ChhtProtocolError.Malformed,
    ChhtClosedReceipt? closedReceipt = null,
    ChhtClosedCompletion? closedCompletion = null) : Exception(message)
{
    public ChhtProtocolError Error { get; } = error;
    public ChhtClosedReceipt? ClosedReceipt { get; } = closedReceipt;
    public ChhtClosedCompletion? ClosedCompletion { get; } = closedCompletion;
}

public static partial class ChhtV1Protocol
{
    public const string MediaType = "application/vnd.cherry.heat-v1";
    public const string CompletionMediaType = "application/vnd.cherry.heat-completion-v1";
    public const int HeaderBytes = 9;
    private static readonly byte[] Magic = "CHHT"u8.ToArray();

    [GeneratedRegex("^[A-Za-z0-9._-]{1,64}$", RegexOptions.CultureInvariant)]
    private static partial Regex CrawlerIdPattern();

    // CHHT v1 wire is shared with the Go crawler:
    // magic[4] | version[1] | unix UTC day u32be | group count canonical uvarint |
    // repeated strictly sorted (hash20 | actor count canonical uvarint |
    // strictly sorted unique actor u64be).
    public static ChhtBatch ParseAndAuthenticate(
        ReadOnlySpan<byte> payload,
        string crawlerId,
        ulong epoch,
        ulong sequence,
        ulong endSequence,
        string signatureHex,
        string payloadSha256Hex,
        ReadOnlySpan<byte> secret,
        HeatOptions options,
        DateTime utcNow)
    {
        if (!CrawlerIdPattern().IsMatch(crawlerId))
            throw new ChhtProtocolException("Invalid X-CHHT-Crawler identifier");
        if (epoch == 0 || sequence == 0 || endSequence < sequence || endSequence == ulong.MaxValue)
            throw new ChhtProtocolException("Invalid CHHT epoch or sequence range");
        if (payload.Length < HeaderBytes + 1 || payload.Length > options.MaxRequestBytes)
            throw new ChhtProtocolException("CHHT payload size is outside configured bounds");
        if (!payload[..4].SequenceEqual(Magic) || payload[4] != 1)
            throw new ChhtProtocolException("Invalid CHHT magic or version");

        var unixDay = BinaryPrimitives.ReadUInt32BigEndian(payload[5..9]);
        DateOnly day;
        try
        {
            day = DateOnly.FromDayNumber(checked((int)unixDay + DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber));
        }
        catch (Exception exception) when (exception is OverflowException or ArgumentOutOfRangeException)
        {
            throw new ChhtProtocolException("CHHT day is outside supported range");
        }
        var payloadDigest = SHA256.HashData(payload);
        ValidatePayloadDigest(payloadDigest, payloadSha256Hex);
        Authenticate(payload, crawlerId, epoch, sequence, endSequence, payloadSha256Hex, signatureHex, secret);
        if (!options.IsExpectedCrawler(crawlerId))
            throw new ChhtProtocolException("Invalid X-CHHT-Crawler authentication", ChhtProtocolError.Authentication);

        var offset = HeaderBytes;
        var groupCountValue = ReadCanonicalUVarint(payload, ref offset);
        if (groupCountValue is 0 || groupCountValue > (ulong)options.MaxRecordsPerBatch)
            throw new ChhtProtocolException("CHHT group count is outside configured bounds");
        var groupCount = checked((int)groupCountValue);
        var groups = new ChhtHashGroup[groupCount];
        byte[]? previousHash = null;
        long totalRecords = 0;
        for (var groupIndex = 0; groupIndex < groupCount; groupIndex++)
        {
            if (payload.Length - offset < 21)
                throw new ChhtProtocolException("Truncated CHHT hash group");
            var hash = payload.Slice(offset, 20).ToArray();
            offset += 20;
            if (previousHash is not null && previousHash.AsSpan().SequenceCompareTo(hash) >= 0)
                throw new ChhtProtocolException("CHHT hashes must be strictly sorted and unique");
            previousHash = hash;
            var actorCountValue = ReadCanonicalUVarint(payload, ref offset);
            if (actorCountValue is 0 || actorCountValue > (ulong)options.MaxRecordsPerBatch)
                throw new ChhtProtocolException("CHHT actor count is outside configured bounds");
            totalRecords = checked(totalRecords + (long)actorCountValue);
            if (totalRecords > options.MaxRecordsPerBatch)
                throw new ChhtProtocolException("CHHT total actor count exceeds configured maximum");
            var actors = new long[checked((int)actorCountValue)];
            ulong previousActor = 0;
            for (var actorIndex = 0; actorIndex < actors.Length; actorIndex++)
            {
                if (payload.Length - offset < 8)
                    throw new ChhtProtocolException("Truncated CHHT actor fingerprint");
                var actor = BinaryPrimitives.ReadUInt64BigEndian(payload.Slice(offset, 8));
                offset += 8;
                if (actorIndex > 0 && actor <= previousActor)
                    throw new ChhtProtocolException("CHHT actors must be strictly sorted and unique");
                previousActor = actor;
                actors[actorIndex] = unchecked((long)actor);
            }
            groups[groupIndex] = new ChhtHashGroup(hash, actors);
        }
        if (offset != payload.Length)
            throw new ChhtProtocolException("CHHT payload has trailing bytes");

        var batch = new ChhtBatch(crawlerId, day, epoch, sequence, endSequence, groups, payloadDigest);
        if (!IsDayOpen(day, utcNow, options.LateGraceMinutes))
            throw new ChhtProtocolException(
                "CHHT day is outside the open UTC window",
                ChhtProtocolError.Expired,
                new ChhtClosedReceipt(crawlerId, day, epoch, sequence, endSequence, payloadDigest));
        return batch;
    }

    public static byte[] ComputeSignature(
        ReadOnlySpan<byte> payload,
        string crawlerId,
        ulong epoch,
        ulong sequence,
        ulong endSequence,
        string payloadSha256Hex,
        ReadOnlySpan<byte> secret)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, secret);
        var prefix = Encoding.UTF8.GetBytes($"CHHT/1\n{crawlerId}\n{epoch}\n{sequence}\n{endSequence}\n{payloadSha256Hex.ToLowerInvariant()}\n");
        hmac.AppendData(prefix);
        hmac.AppendData(payload);
        return hmac.GetHashAndReset();
    }

    public static ChhtCompletion ParseAndAuthenticateCompletion(
        string crawlerId,
        string dayText,
        ulong epoch,
        ulong startSequence,
        ulong nextSequence,
        string clean,
        string signatureHex,
        ReadOnlySpan<byte> secret,
        HeatOptions options,
        DateTime utcNow)
    {
        if (!CrawlerIdPattern().IsMatch(crawlerId))
            throw new ChhtProtocolException("Invalid X-CHHT-Crawler identifier");
        if (!DateOnly.TryParseExact(dayText, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var day) || day.ToString("yyyy-MM-dd") != dayText)
            throw new ChhtProtocolException("Invalid X-CHHT-Day");
        if (epoch == 0 || startSequence == 0 || nextSequence < startSequence || clean != "1")
            throw new ChhtProtocolException("Invalid CHHT completion identity");
        if (signatureHex.Length != 64)
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
        byte[] provided;
        try { provided = Convert.FromHexString(signatureHex); }
        catch (FormatException)
        {
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
        }
        var expected = ComputeCompletionSignature(
            crawlerId, day, epoch, startSequence, nextSequence, secret);
        if (provided.Length != expected.Length || !CryptographicOperations.FixedTimeEquals(provided, expected))
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
        if (!options.IsExpectedCrawler(crawlerId))
            throw new ChhtProtocolException("Invalid X-CHHT-Crawler authentication", ChhtProtocolError.Authentication);
        var completion = new ChhtCompletion(crawlerId, day, epoch, startSequence, nextSequence, true);
        if (!IsDayOpen(day, utcNow, options.LateGraceMinutes))
            throw new ChhtProtocolException(
                "CHHT completion UTC day is closed",
                ChhtProtocolError.Expired,
                closedCompletion: new ChhtClosedCompletion(
                    crawlerId, day, epoch, startSequence, nextSequence, true));
        return completion;
    }

    public static byte[] ComputeCompletionSignature(
        string crawlerId,
        DateOnly day,
        ulong epoch,
        ulong startSequence,
        ulong nextSequence,
        ReadOnlySpan<byte> secret)
    {
        var canonical = Encoding.UTF8.GetBytes(
            $"CHHT-COMPLETE/1\n{crawlerId}\n{day:yyyy-MM-dd}\n{epoch}\n{startSequence}\n{nextSequence}\n1\n");
        return HMACSHA256.HashData(secret, canonical);
    }

    private static void Authenticate(
        ReadOnlySpan<byte> payload,
        string crawlerId,
        ulong epoch,
        ulong sequence,
        ulong endSequence,
        string payloadSha256Hex,
        string signatureHex,
        ReadOnlySpan<byte> secret)
    {
        if (signatureHex.Length != 64)
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
        byte[] provided;
        try { provided = Convert.FromHexString(signatureHex); }
        catch (FormatException) { throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication); }
        if (provided.Length != 32)
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
        var expected = ComputeSignature(payload, crawlerId, epoch, sequence, endSequence, payloadSha256Hex, secret);
        if (!CryptographicOperations.FixedTimeEquals(provided, expected))
            throw new ChhtProtocolException("Invalid X-CHHT-Signature", ChhtProtocolError.Authentication);
    }

    private static void ValidatePayloadDigest(ReadOnlySpan<byte> digest, string digestHex)
    {
        if (digestHex.Length != 64)
            throw new ChhtProtocolException("Invalid X-CHHT-Payload-SHA256", ChhtProtocolError.Authentication);
        byte[] provided;
        try { provided = Convert.FromHexString(digestHex); }
        catch (FormatException) { throw new ChhtProtocolException("Invalid X-CHHT-Payload-SHA256", ChhtProtocolError.Authentication); }
        if (provided.Length != 32 || !CryptographicOperations.FixedTimeEquals(provided, digest))
            throw new ChhtProtocolException("Invalid X-CHHT-Payload-SHA256", ChhtProtocolError.Authentication);
    }

    private static ulong ReadCanonicalUVarint(ReadOnlySpan<byte> data, ref int offset)
    {
        ulong value = 0;
        var start = offset;
        for (var shift = 0; shift <= 63; shift += 7)
        {
            if (offset >= data.Length) throw new ChhtProtocolException("Truncated CHHT uvarint");
            var current = data[offset++];
            if (shift == 63 && current > 1) throw new ChhtProtocolException("CHHT uvarint overflow");
            value |= (ulong)(current & 0x7f) << shift;
            if ((current & 0x80) != 0) continue;
            if (offset - start > 1 && current == 0)
                throw new ChhtProtocolException("Non-canonical CHHT uvarint");
            return value;
        }
        throw new ChhtProtocolException("CHHT uvarint overflow");
    }

    private static bool IsDayOpen(DateOnly day, DateTime utcNow, int graceMinutes)
    {
        var normalized = utcNow.Kind == DateTimeKind.Utc ? utcNow : utcNow.ToUniversalTime();
        var today = DateOnly.FromDateTime(normalized);
        if (day == today) return true;
        var graceEnd = normalized.Date.AddMinutes(graceMinutes);
        return day == today.AddDays(-1) && normalized <= graceEnd;
    }
}
