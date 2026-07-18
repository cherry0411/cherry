using System.Buffers;
using System.Text;
using Cherry.Domain.Entities;

namespace Cherry.Infrastructure.Storage;

public sealed record DecodedTorrentDetail(
    List<TorrentFile> Files,
    List<TorrentExtensionSummary> ExtensionSummaries);

/// <summary>
/// Deterministic compact detail format. Version 1 is:
/// <code>
///   u8 version (=1)
///   uvarint file-entry-count
///   repeated: uvarint common-path-prefix-bytes, uvarint suffix-bytes,
///             suffix UTF-8 bytes, uvarint length
///   uvarint extension-count
///   repeated: uvarint extension-bytes, extension UTF-8 bytes,
///             uvarint file-count, uvarint total-length
/// </code>
/// The format intentionally has no normalized/summary discriminator.
/// Exact duplicate path/length pairs are retained for lossless compatibility
/// with legacy catalogs; only decreasing path/length order is non-canonical.
/// </summary>
public static class TorrentDetailCodec
{
    public const byte CurrentVersion = 1;
    public const int MaxFileEntries = 10_000;
    public const int MaxExtensionEntries = 128;
    public const int MaxPathUtf8Bytes = 16 * 1024;
    public const int MaxExtensionUtf8Bytes = 32;
    // Durable HTTP input is capped at 64 MiB, so a valid compact payload cannot
    // require more bytes than its JSON source. Keeping the same hard ceiling
    // bounds one corrupt-row/API allocation on a 2C4G host. The writer checks
    // before growth, so crossing the limit never causes a 128 MiB resize.
    public const int MaxPayloadBytes = 64 * 1024 * 1024;

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static byte[] Encode(
        IReadOnlyCollection<TorrentFile> files,
        IReadOnlyCollection<TorrentExtensionSummary> extensions)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(extensions);
        if (files.Count > MaxFileEntries)
            throw Invalid($"file entry count exceeds {MaxFileEntries}");
        if (extensions.Count > MaxExtensionEntries)
            throw Invalid($"extension entry count exceeds {MaxExtensionEntries}");

        var encodedFiles = files.Select(file =>
        {
            ArgumentNullException.ThrowIfNull(file);
            if (file.Length < 0)
                throw Invalid("file length cannot be negative");
            return new EncodedFile(
                EncodeText(file.PathText, MaxPathUtf8Bytes, "file path", allowEmpty: false),
                file.Length);
        }).ToList();
        encodedFiles.Sort(EncodedFileComparer.Instance);

        var encodedExtensions = extensions.Select(extension =>
        {
            ArgumentNullException.ThrowIfNull(extension);
            if (extension.FileCount <= 0)
                throw Invalid("extension file count must be positive");
            if (extension.TotalLength < 0)
                throw Invalid("extension total length cannot be negative");
            return new EncodedExtension(
                EncodeText(
                    extension.Extension,
                    MaxExtensionUtf8Bytes,
                    "extension",
                    allowEmpty: false),
                extension.FileCount,
                extension.TotalLength);
        }).ToList();
        encodedExtensions.Sort(EncodedExtensionComparer.Instance);
        for (var index = 1; index < encodedExtensions.Count; index++)
        {
            if (encodedExtensions[index - 1].Name.AsSpan()
                .SequenceEqual(encodedExtensions[index].Name))
            {
                throw Invalid("extension entries must have unique exact names");
            }
        }

        var writer = new BoundedBufferWriter(MaxPayloadBytes);
        WriteByte(writer, CurrentVersion);
        WriteVarint(writer, checked((ulong)encodedFiles.Count));
        ReadOnlySpan<byte> previousPath = [];
        foreach (var file in encodedFiles)
        {
            var path = file.Path.AsSpan();
            var prefixLength = CommonPrefixLength(previousPath, path);
            WriteVarint(writer, checked((ulong)prefixLength));
            WriteVarint(writer, checked((ulong)(path.Length - prefixLength)));
            WriteBytes(writer, path[prefixLength..]);
            WriteVarint(writer, checked((ulong)file.Length));
            previousPath = file.Path;
            EnsurePayloadBound(writer.WrittenCount);
        }

        WriteVarint(writer, checked((ulong)encodedExtensions.Count));
        foreach (var extension in encodedExtensions)
        {
            WriteVarint(writer, checked((ulong)extension.Name.Length));
            WriteBytes(writer, extension.Name);
            WriteVarint(writer, checked((ulong)extension.FileCount));
            WriteVarint(writer, checked((ulong)extension.TotalLength));
            EnsurePayloadBound(writer.WrittenCount);
        }
        EnsurePayloadBound(writer.WrittenCount);
        return writer.WrittenSpan.ToArray();
    }

    public static DecodedTorrentDetail Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length > MaxPayloadBytes)
            throw Invalid($"payload exceeds {MaxPayloadBytes} bytes");
        var reader = new DetailReader(payload);
        if (reader.ReadByte("version") != CurrentVersion)
            throw Invalid("unsupported detail payload version");

        var fileCount = reader.ReadBoundedCount(MaxFileEntries, "file entry count");
        var files = new List<TorrentFile>(fileCount);
        byte[] previousPath = [];
        long previousLength = 0;
        for (var index = 0; index < fileCount; index++)
        {
            var prefixLength = reader.ReadBoundedCount(
                previousPath.Length,
                $"file[{index}] common prefix");
            var suffixLength = reader.ReadBoundedCount(
                MaxPathUtf8Bytes,
                $"file[{index}] suffix length");
            if (prefixLength + suffixLength > MaxPathUtf8Bytes)
                throw Invalid($"file[{index}] path exceeds {MaxPathUtf8Bytes} UTF-8 bytes");

            var pathBytes = new byte[prefixLength + suffixLength];
            previousPath.AsSpan(0, prefixLength).CopyTo(pathBytes);
            reader.ReadBytes(suffixLength, $"file[{index}] suffix").CopyTo(pathBytes.AsSpan(prefixLength));
            if (CommonPrefixLength(previousPath, pathBytes) != prefixLength)
                throw Invalid($"file[{index}] uses a non-canonical prefix length");
            var length = reader.ReadInt64($"file[{index}] length");
            var ordering = pathBytes.AsSpan().SequenceCompareTo(previousPath);
            if (index > 0 && (ordering < 0 || ordering == 0 && length < previousLength))
                throw Invalid("file entries are not in canonical order");

            files.Add(new TorrentFile
            {
                PathText = DecodeText(pathBytes, "file path", allowEmpty: false),
                Length = length
            });
            previousPath = pathBytes;
            previousLength = length;
        }

        var extensionCount = reader.ReadBoundedCount(
            MaxExtensionEntries,
            "extension entry count");
        var extensions = new List<TorrentExtensionSummary>(extensionCount);
        byte[]? previousExtension = null;
        for (var index = 0; index < extensionCount; index++)
        {
            var nameLength = reader.ReadBoundedCount(
                MaxExtensionUtf8Bytes,
                $"extension[{index}] name length");
            var nameBytes = reader.ReadBytes(nameLength, $"extension[{index}] name").ToArray();
            if (previousExtension is not null &&
                nameBytes.AsSpan().SequenceCompareTo(previousExtension) <= 0)
            {
                throw Invalid("extension entries are not in canonical order");
            }
            var name = DecodeText(nameBytes, "extension", allowEmpty: false);
            var aggregateFileCount = reader.ReadPositiveInt32($"extension[{index}] file count");
            var aggregateLength = reader.ReadInt64($"extension[{index}] total length");
            extensions.Add(new TorrentExtensionSummary
            {
                Extension = name,
                FileCount = aggregateFileCount,
                TotalLength = aggregateLength
            });
            previousExtension = nameBytes;
        }

        if (!reader.End)
            throw Invalid("detail payload has trailing bytes");
        return new DecodedTorrentDetail(files, extensions);
    }

    private static byte[] EncodeText(string? value, int maximumBytes, string field, bool allowEmpty)
    {
        if (value is null || !allowEmpty && value.Length == 0)
            throw Invalid($"{field} is required");
        if (value.Contains('\0'))
            throw Invalid($"{field} cannot contain NUL");
        byte[] bytes;
        try
        {
            bytes = StrictUtf8.GetBytes(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw Invalid($"{field} is not valid Unicode", exception);
        }
        if (bytes.Length > maximumBytes)
            throw Invalid($"{field} exceeds {maximumBytes} UTF-8 bytes");
        return bytes;
    }

    private static string DecodeText(ReadOnlySpan<byte> value, string field, bool allowEmpty)
    {
        if (!allowEmpty && value.IsEmpty)
            throw Invalid($"{field} is required");
        string decoded;
        try
        {
            decoded = StrictUtf8.GetString(value);
        }
        catch (DecoderFallbackException exception)
        {
            throw Invalid($"{field} is not valid UTF-8", exception);
        }
        if (decoded.Contains('\0'))
            throw Invalid($"{field} cannot contain NUL");
        return decoded;
    }

    private static int CommonPrefixLength(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var length = Math.Min(left.Length, right.Length);
        var index = 0;
        while (index < length && left[index] == right[index])
            index++;
        return index;
    }

    private static void WriteVarint(IBufferWriter<byte> writer, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        var length = 0;
        do
        {
            var next = (byte)(value & 0x7f);
            value >>= 7;
            if (value != 0)
                next |= 0x80;
            buffer[length++] = next;
        } while (value != 0);
        WriteBytes(writer, buffer[..length]);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        var target = writer.GetSpan(1);
        target[0] = value;
        writer.Advance(1);
    }

    private static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        var target = writer.GetSpan(value.Length);
        value.CopyTo(target);
        writer.Advance(value.Length);
    }

    private static void EnsurePayloadBound(int size)
    {
        if (size > MaxPayloadBytes)
            throw Invalid($"payload exceeds {MaxPayloadBytes} bytes");
    }

    private static InvalidDataException Invalid(string message, Exception? inner = null) =>
        new(message, inner);

    private sealed record EncodedFile(byte[] Path, long Length);
    private sealed record EncodedExtension(byte[] Name, int FileCount, long TotalLength);

    private sealed class EncodedFileComparer : IComparer<EncodedFile>
    {
        public static EncodedFileComparer Instance { get; } = new();
        public int Compare(EncodedFile? left, EncodedFile? right)
        {
            if (ReferenceEquals(left, right)) return 0;
            if (left is null) return -1;
            if (right is null) return 1;
            var path = left.Path.AsSpan().SequenceCompareTo(right.Path);
            return path != 0 ? path : left.Length.CompareTo(right.Length);
        }
    }

    private sealed class EncodedExtensionComparer : IComparer<EncodedExtension>
    {
        public static EncodedExtensionComparer Instance { get; } = new();
        public int Compare(EncodedExtension? left, EncodedExtension? right)
        {
            if (ReferenceEquals(left, right)) return 0;
            if (left is null) return -1;
            if (right is null) return 1;
            return left.Name.AsSpan().SequenceCompareTo(right.Name);
        }
    }

    private sealed class BoundedBufferWriter(int maximumBytes) : IBufferWriter<byte>
    {
        private readonly ArrayBufferWriter<byte> _inner = new(256);

        public int WrittenCount => _inner.WrittenCount;
        public ReadOnlySpan<byte> WrittenSpan => _inner.WrittenSpan;

        public void Advance(int count) => _inner.Advance(count);

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _inner.GetMemory(sizeHint);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _inner.GetSpan(sizeHint);
        }

        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint < 0 || sizeHint > maximumBytes - _inner.WrittenCount)
                throw Invalid($"payload exceeds {maximumBytes} bytes");
        }
    }

    private ref struct DetailReader(ReadOnlySpan<byte> payload)
    {
        private readonly ReadOnlySpan<byte> _payload = payload;
        private int _position;

        public bool End => _position == _payload.Length;

        public byte ReadByte(string field)
        {
            if (_position >= _payload.Length)
                throw Invalid($"{field} is truncated");
            return _payload[_position++];
        }

        public ReadOnlySpan<byte> ReadBytes(int count, string field)
        {
            if (count < 0 || count > _payload.Length - _position)
                throw Invalid($"{field} is truncated");
            var value = _payload.Slice(_position, count);
            _position += count;
            return value;
        }

        public int ReadBoundedCount(int maximum, string field)
        {
            var value = ReadVarint(field);
            if (value > (ulong)maximum)
                throw Invalid($"{field} exceeds {maximum}");
            return checked((int)value);
        }

        public int ReadPositiveInt32(string field)
        {
            var value = ReadVarint(field);
            if (value is 0 or > int.MaxValue)
                throw Invalid($"{field} must be between 1 and {int.MaxValue}");
            return checked((int)value);
        }

        public long ReadInt64(string field)
        {
            var value = ReadVarint(field);
            if (value > long.MaxValue)
                throw Invalid($"{field} exceeds {long.MaxValue}");
            return checked((long)value);
        }

        private ulong ReadVarint(string field)
        {
            var start = _position;
            ulong value = 0;
            for (var index = 0; index < 10; index++)
            {
                var next = ReadByte(field);
                if (index == 9 && (next & 0x7f) > 1)
                    throw Invalid($"{field} overflows UInt64");
                value |= (ulong)(next & 0x7f) << (index * 7);
                if ((next & 0x80) == 0)
                {
                    if (_position - start != VarintLength(value))
                        throw Invalid($"{field} uses a non-canonical varint");
                    return value;
                }
            }
            throw Invalid($"{field} has an unterminated varint");
        }

        private static int VarintLength(ulong value)
        {
            var length = 1;
            while (value >= 0x80)
            {
                value >>= 7;
                length++;
            }
            return length;
        }
    }
}
