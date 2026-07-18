using System.Text;
using Cherry.Application.Dtos;
using Cherry.Domain.Entities;

namespace Cherry.Application.Services;

public sealed class DurableBatchValidationException(string message) : Exception(message);

public sealed record ValidatedDurableBatch(
    List<Torrent> Torrents,
    List<MetadataDecision> Decisions,
    int EventCount);

public static class DurableBatchValidator
{
    public const int MaxEventsPerBatch = 5_000;
    public const int MaxFilesPerEvent = 10_000;
    public const int MaxRepresentativeFiles = 64;
    public const int MaxExtensionSummaries = 128;

    public static ValidatedDurableBatch ValidateAndMap(DurableBatchRequest request)
    {
        if (request.SchemaVersion != 2)
            throw Invalid("schema_version must be 2");
        ValidateBoundedText(request.CrawlerId, "crawler_id", 256, allowEmpty: false);
        if (request.Epoch == 0 || request.Epoch > long.MaxValue)
            throw Invalid("epoch must be between 1 and Int64.MaxValue");
        if (request.StartSequence == 0 || request.StartSequence > long.MaxValue)
            throw Invalid("start_sequence must be between 1 and Int64.MaxValue");
        if (request.EndSequence == 0 || request.EndSequence > long.MaxValue)
            throw Invalid("end_sequence must be between 1 and Int64.MaxValue");
        if (request.EndSequence < request.StartSequence)
            throw Invalid("end_sequence must not precede start_sequence");
        if (!IsLowerHex(request.PayloadSha256, 64))
            throw Invalid("payload_sha256 must be 64 lowercase hexadecimal characters");

        var events = request.Events
            ?? throw Invalid("events is required");
        if (events.Count is 0 or > MaxEventsPerBatch)
            throw Invalid($"events must contain between 1 and {MaxEventsPerBatch} entries");
        if (request.EndSequence - request.StartSequence + 1 != (ulong)events.Count)
            throw Invalid("the inclusive sequence range must equal the number of events");

        var torrents = new List<Torrent>(events.Count);
        var decisions = new List<MetadataDecision>(events.Count);
        for (var index = 0; index < events.Count; index++)
            ValidateAndMapEvent(events[index], index, torrents, decisions);
        return new ValidatedDurableBatch(torrents, decisions, events.Count);
    }

    private static void ValidateAndMapEvent(
        DurableBatchEvent? item,
        int index,
        List<Torrent> torrents,
        List<MetadataDecision> decisions)
    {
        var field = $"events[{index}]";
        if (item is null)
            throw Invalid($"{field} cannot be null");
        if (!IsLowerHex(item.InfoHash, 40))
            throw Invalid($"{field}.info_hash must be 40 lowercase hexadecimal characters");

        if (item.FirstSeen is { Offset: var offset } && offset != TimeSpan.Zero)
            throw Invalid($"{field}.first_seen must use UTC");

        switch (item.Encoding)
        {
            case "normalized" when item.Normalized is not null &&
                                   item.Summary is null &&
                                   item.DecisionCode == 0:
                torrents.Add(MapNormalized(item, field));
                return;
            case "summary" when item.Summary is not null &&
                                item.Normalized is null &&
                                item.DecisionCode == 0:
                torrents.Add(MapSummary(item, field));
                return;
            case "hash_only" when item.Normalized is null && item.Summary is null:
                decisions.Add(MapDecision(item, item.DecisionCode, reject: false, field));
                return;
            case "reject" when item.Normalized is null && item.Summary is null:
                decisions.Add(MapDecision(item, item.DecisionCode, reject: true, field));
                return;
            default:
                throw Invalid($"{field}.encoding, body, and decision_code do not match");
        }
    }

    private static Torrent MapNormalized(DurableBatchEvent item, string field)
    {
        var normalized = item.Normalized!;
        ValidateBoundedText(normalized.Name, $"{field}.normalized.name", 16 * 1024, allowEmpty: true);
        ValidateLength(normalized.TotalLength, $"{field}.normalized.total_length");
        var files = normalized.Files
            ?? throw Invalid($"{field}.normalized.files is required");
        if (files.Count is 0 or > MaxFilesPerEvent)
            throw Invalid($"{field}.normalized.files must contain between 1 and {MaxFilesPerEvent} entries");

        ulong sum = 0;
        var mappedFiles = new List<TorrentFile>(files.Count);
        for (var fileIndex = 0; fileIndex < files.Count; fileIndex++)
        {
            var file = ValidateFile(files[fileIndex], $"{field}.normalized.files[{fileIndex}]");
            try
            {
                sum = checked(sum + file.Length);
            }
            catch (OverflowException)
            {
                throw Invalid($"{field}.normalized file lengths overflow UInt64");
            }
            mappedFiles.Add(MapFile(item.InfoHash!, file));
        }
        if (sum != normalized.TotalLength)
            throw Invalid($"{field}.normalized file lengths must sum to total_length");

        return BaseTorrent(
            item,
            normalized.Name!,
            normalized.TotalLength,
            mappedFiles.Count,
            mappedFiles,
            []);
    }

    private static Torrent MapSummary(DurableBatchEvent item, string field)
    {
        var summary = item.Summary!;
        ValidateBoundedText(summary.Name, $"{field}.summary.name", 16 * 1024, allowEmpty: true);
        ValidateLength(summary.TotalLength, $"{field}.summary.total_length");
        if (summary.FileCount == 0 || summary.FileCount > int.MaxValue)
            throw Invalid($"{field}.summary.file_count must be between 1 and Int32.MaxValue");

        var representatives = summary.RepresentativeFiles ?? [];
        if (representatives.Count > MaxRepresentativeFiles || representatives.Count > summary.FileCount)
            throw Invalid($"{field}.summary.representative_files exceeds its bounded file count");
        var mappedFiles = new List<TorrentFile>(representatives.Count);
        for (var fileIndex = 0; fileIndex < representatives.Count; fileIndex++)
        {
            var file = ValidateFile(
                representatives[fileIndex],
                $"{field}.summary.representative_files[{fileIndex}]");
            mappedFiles.Add(MapFile(item.InfoHash!, file));
        }

        var extensions = summary.Extensions ?? [];
        if (extensions.Count > MaxExtensionSummaries)
            throw Invalid($"{field}.summary.extensions exceeds {MaxExtensionSummaries} entries");
        ulong extensionFiles = 0;
        ulong extensionBytes = 0;
        var seenExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var mappedExtensions = new List<TorrentExtensionSummary>(extensions.Count);
        for (var extensionIndex = 0; extensionIndex < extensions.Count; extensionIndex++)
        {
            var extension = extensions[extensionIndex]
                ?? throw Invalid($"{field}.summary.extensions[{extensionIndex}] cannot be null");
            ValidateBoundedText(
                extension.Extension,
                $"{field}.summary.extensions[{extensionIndex}].extension",
                32,
                allowEmpty: false);
            if (extension.Files == 0 || extension.Files > int.MaxValue ||
                !seenExtensions.Add(extension.Extension!))
                throw Invalid($"{field}.summary.extensions[{extensionIndex}] has invalid or duplicate counts");
            ValidateLength(extension.Bytes, $"{field}.summary.extensions[{extensionIndex}].bytes");
            try
            {
                extensionFiles = checked(extensionFiles + extension.Files);
                extensionBytes = checked(extensionBytes + extension.Bytes);
            }
            catch (OverflowException)
            {
                throw Invalid($"{field}.summary extension aggregates overflow UInt64");
            }
            mappedExtensions.Add(new TorrentExtensionSummary
            {
                InfoHash = item.InfoHash!,
                Extension = extension.Extension!,
                FileCount = checked((int)extension.Files),
                TotalLength = checked((long)extension.Bytes)
            });
        }
        if (extensionFiles > summary.FileCount || extensionBytes > summary.TotalLength)
            throw Invalid($"{field}.summary extension aggregates exceed torrent totals");

        return BaseTorrent(
            item,
            summary.Name!,
            summary.TotalLength,
            checked((int)summary.FileCount),
            mappedFiles,
            mappedExtensions);
    }

    private static MetadataDecision MapDecision(
        DurableBatchEvent item,
        short rawDecisionCode,
        bool reject,
        string field)
    {
        if (!Enum.IsDefined(typeof(MetadataDecisionCode), rawDecisionCode))
            throw Invalid($"{field}.{item.Encoding}.decision_code is not recognized");
        var decisionCode = (MetadataDecisionCode)rawDecisionCode;
        var matchesEncoding = reject
            ? decisionCode is MetadataDecisionCode.Reject or MetadataDecisionCode.RejectFileCap
            : decisionCode is MetadataDecisionCode.HashOnly or
                MetadataDecisionCode.HashOnlyFileCap or
                MetadataDecisionCode.InvalidMetadata;
        if (!matchesEncoding)
            throw Invalid($"{field}.{item.Encoding}.decision_code does not match its encoding");
        return new MetadataDecision
        {
            InfoHash = Convert.FromHexString(item.InfoHash!),
            DecisionCode = decisionCode
        };
    }

    private static Torrent BaseTorrent(
        DurableBatchEvent item,
        string name,
        ulong totalLength,
        int fileCount,
        List<TorrentFile> files,
        List<TorrentExtensionSummary> extensions)
    {
        var observedAt = item.FirstSeen?.UtcDateTime ?? DateTime.UtcNow;
        return new Torrent
        {
            InfoHash = item.InfoHash!,
            Name = name,
            TotalLength = checked((long)totalLength),
            FileCount = fileCount,
            CreatedAt = observedAt,
            Files = files,
            ExtensionSummaries = extensions
        };
    }

    private static DurableBatchFile ValidateFile(DurableBatchFile? file, string field)
    {
        if (file is null)
            throw Invalid($"{field} cannot be null");
        ValidateBoundedText(file.Path, $"{field}.path", 16 * 1024, allowEmpty: false);
        ValidateLength(file.Length, $"{field}.length");
        return file;
    }

    private static TorrentFile MapFile(string infoHash, DurableBatchFile file) => new()
    {
        InfoHash = infoHash,
        PathText = file.Path!,
        Length = checked((long)file.Length)
    };

    private static void ValidateLength(ulong value, string field)
    {
        if (value > long.MaxValue)
            throw Invalid($"{field} exceeds Int64.MaxValue");
    }

    private static void ValidateBoundedText(
        string? value,
        string field,
        int maxUtf8Bytes,
        bool allowEmpty,
        bool required = true)
    {
        if (value is null)
        {
            if (required)
                throw Invalid($"{field} is required");
            return;
        }
        if (!allowEmpty && value.Length == 0)
            throw Invalid($"{field} is required");
        if (value.Contains('\0'))
            throw Invalid($"{field} must not contain NUL");
        if (Encoding.UTF8.GetByteCount(value) > maxUtf8Bytes)
            throw Invalid($"{field} exceeds {maxUtf8Bytes} UTF-8 bytes");
    }

    private static bool IsLowerHex(string? value, int expectedLength) =>
        value is { Length: var length } && length == expectedLength &&
        value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');

    private static DurableBatchValidationException Invalid(string message) => new(message);
}
