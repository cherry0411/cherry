using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Cherry.Infrastructure.Heat;

public sealed record HeatActorHourCount(byte[] InfoHash, uint Count);

public enum HeatActorHourFrameState : byte
{
    Open = 1,
    Provisional = 2,
    Sealed = 3
}

public enum HeatActorHourCoverage : byte
{
    Unknown = 1,
    Partial = 2,
    Complete = 3
}

public sealed record HeatActorHourFrame(
    long UnixHour,
    HeatActorHourFrameState State,
    HeatActorHourCoverage Coverage,
    HeatActorHourPartialReason PartialReasons,
    int FalsePositivePpm,
    int PairCapacity,
    long NewPairs,
    long ProbableDuplicates,
    long Bypassed,
    IReadOnlyList<HeatActorHourCount> Entries);

public static class HeatActorHourFrameCodec
{
    private static ReadOnlySpan<byte> Magic => "CHAH"u8;
    private const byte Version = 1;
    private const int HeaderBytes = 60;
    private const int ChecksumBytes = 32;

    public static byte[] Encode(HeatActorHourFrame frame)
    {
        ValidateFrame(frame);
        var writer = new ArrayBufferWriter<byte>(
            checked(HeaderBytes + ChecksumBytes + frame.Entries.Count * 8));
        Write(writer, Magic);
        WriteByte(writer, Version);
        WriteByte(writer, (byte)frame.State);
        WriteByte(writer, (byte)frame.Coverage);
        WriteByte(writer, 0);
        WriteInt64(writer, frame.UnixHour);
        WriteUInt32(writer, (uint)frame.PartialReasons);
        WriteUInt32(writer, checked((uint)frame.FalsePositivePpm));
        WriteUInt64(writer, checked((ulong)frame.PairCapacity));
        WriteUInt64(writer, checked((ulong)frame.NewPairs));
        WriteUInt64(writer, checked((ulong)frame.ProbableDuplicates));
        WriteUInt64(writer, checked((ulong)frame.Bypassed));
        WriteUInt32(writer, checked((uint)frame.Entries.Count));

        Span<byte> previous = stackalloc byte[20];
        foreach (var entry in frame.Entries)
        {
            var prefix = CommonPrefix(previous, entry.InfoHash);
            WriteByte(writer, checked((byte)prefix));
            Write(writer, entry.InfoHash.AsSpan(prefix));
            WriteVarUInt32(writer, entry.Count);
            entry.InfoHash.CopyTo(previous);
        }

        var checksum = SHA256.HashData(writer.WrittenSpan);
        Write(writer, checksum);
        return writer.WrittenSpan.ToArray();
    }

    public static HeatActorHourFrame Decode(ReadOnlySpan<byte> encoded)
    {
        if (encoded.Length < HeaderBytes + ChecksumBytes)
            throw new InvalidDataException("Actor-hour frame is truncated");
        var body = encoded[..^ChecksumBytes];
        Span<byte> expected = stackalloc byte[ChecksumBytes];
        SHA256.HashData(body, expected);
        if (!CryptographicOperations.FixedTimeEquals(expected, encoded[^ChecksumBytes..]))
            throw new InvalidDataException("Actor-hour frame checksum mismatch");

        var offset = 0;
        if (!Read(body, ref offset, Magic.Length).SequenceEqual(Magic))
            throw new InvalidDataException("Actor-hour frame magic mismatch");
        if (ReadByte(body, ref offset) != Version)
            throw new InvalidDataException("Unsupported actor-hour frame version");
        var state = (HeatActorHourFrameState)ReadByte(body, ref offset);
        var coverage = (HeatActorHourCoverage)ReadByte(body, ref offset);
        if (ReadByte(body, ref offset) != 0)
            throw new InvalidDataException("Actor-hour frame reserved bytes are non-zero");
        var unixHour = ReadInt64(body, ref offset);
        var reasons = (HeatActorHourPartialReason)ReadUInt32(body, ref offset);
        var falsePositivePpm = checked((int)ReadUInt32(body, ref offset));
        var pairCapacity = checked((int)ReadUInt64(body, ref offset));
        var newPairs = checked((long)ReadUInt64(body, ref offset));
        var probableDuplicates = checked((long)ReadUInt64(body, ref offset));
        var bypassed = checked((long)ReadUInt64(body, ref offset));
        var entryCount = checked((int)ReadUInt32(body, ref offset));
        if (entryCount > (body.Length - offset) / 2)
            throw new InvalidDataException("Actor-hour frame entry count exceeds payload");

        var entries = new HeatActorHourCount[entryCount];
        var previous = new byte[20];
        for (var index = 0; index < entryCount; index++)
        {
            var prefix = ReadByte(body, ref offset);
            if (prefix > 20)
                throw new InvalidDataException("Actor-hour frame hash prefix is invalid");
            var hash = new byte[20];
            previous.AsSpan(0, prefix).CopyTo(hash);
            Read(body, ref offset, 20 - prefix).CopyTo(hash.AsSpan(prefix));
            if (prefix != CommonPrefix(previous, hash))
                throw new InvalidDataException("Actor-hour frame hash prefix is non-canonical");
            var count = ReadVarUInt32(body, ref offset);
            if (count == 0)
                throw new InvalidDataException("Actor-hour frame count must be positive");
            if (index > 0 && previous.AsSpan().SequenceCompareTo(hash) >= 0)
                throw new InvalidDataException("Actor-hour frame hashes are not strictly sorted");
            entries[index] = new HeatActorHourCount(hash, count);
            previous = hash;
        }
        if (offset != body.Length)
            throw new InvalidDataException("Actor-hour frame has trailing payload bytes");

        var frame = new HeatActorHourFrame(
            unixHour,
            state,
            coverage,
            reasons,
            falsePositivePpm,
            pairCapacity,
            newPairs,
            probableDuplicates,
            bypassed,
            entries);
        ValidateFrame(frame);
        return frame;
    }

    private static void ValidateFrame(HeatActorHourFrame frame)
    {
        if (frame.UnixHour < 0)
            throw new InvalidDataException("Actor-hour frame hour must be non-negative");
        if (!Enum.IsDefined(frame.State) || !Enum.IsDefined(frame.Coverage))
            throw new InvalidDataException("Actor-hour frame state or coverage is invalid");
        if (frame.FalsePositivePpm is <= 0 or >= 1_000_000)
            throw new InvalidDataException("Actor-hour frame false-positive PPM is invalid");
        if (frame.PairCapacity <= 0 || frame.NewPairs < 0 ||
            frame.NewPairs > frame.PairCapacity || frame.ProbableDuplicates < 0 || frame.Bypassed < 0)
            throw new InvalidDataException("Actor-hour frame counters are invalid");
        const HeatActorHourPartialReason knownReasons =
            HeatActorHourPartialReason.OpenHour |
            HeatActorHourPartialReason.ProcessRestart |
            HeatActorHourPartialReason.PairCapacity |
            HeatActorHourPartialReason.HashCapacity |
            HeatActorHourPartialReason.ObservationFailure |
            HeatActorHourPartialReason.ClockGap |
            HeatActorHourPartialReason.ObserverQueueLoss |
            HeatActorHourPartialReason.ClockRollback;
        if ((frame.PartialReasons & ~knownReasons) != 0)
            throw new InvalidDataException("Actor-hour frame partial reason is invalid");
        var openReason = frame.PartialReasons.HasFlag(HeatActorHourPartialReason.OpenHour);
        if ((frame.State == HeatActorHourFrameState.Open) != openReason ||
            (frame.State == HeatActorHourFrameState.Open &&
             frame.Coverage != HeatActorHourCoverage.Partial) ||
            (frame.State != HeatActorHourFrameState.Sealed &&
             frame.Coverage == HeatActorHourCoverage.Complete) ||
            (frame.Coverage == HeatActorHourCoverage.Complete &&
             frame.PartialReasons != HeatActorHourPartialReason.None))
            throw new InvalidDataException("Actor-hour frame state and coverage are inconsistent");
        byte[]? previous = null;
        long total = 0;
        foreach (var entry in frame.Entries)
        {
            if (entry.InfoHash.Length != 20 || entry.Count == 0)
                throw new InvalidDataException("Actor-hour frame entry is invalid");
            if (previous is not null && previous.AsSpan().SequenceCompareTo(entry.InfoHash) >= 0)
                throw new InvalidDataException("Actor-hour frame hashes must be strictly sorted");
            total = checked(total + entry.Count);
            previous = entry.InfoHash;
        }
        if (total != frame.NewPairs)
            throw new InvalidDataException("Actor-hour frame entry total does not match new pairs");
    }

    private static int CommonPrefix(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var length = Math.Min(left.Length, right.Length);
        var index = 0;
        while (index < length && left[index] == right[index]) index++;
        return index;
    }

    private static void Write(ArrayBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        value.CopyTo(writer.GetSpan(value.Length));
        writer.Advance(value.Length);
    }

    private static void WriteByte(ArrayBufferWriter<byte> writer, byte value)
    {
        writer.GetSpan(1)[0] = value;
        writer.Advance(1);
    }

    private static void WriteInt64(ArrayBufferWriter<byte> writer, long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(writer.GetSpan(sizeof(long)), value);
        writer.Advance(sizeof(long));
    }

    private static void WriteUInt32(ArrayBufferWriter<byte> writer, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(writer.GetSpan(sizeof(uint)), value);
        writer.Advance(sizeof(uint));
    }

    private static void WriteUInt64(ArrayBufferWriter<byte> writer, ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(writer.GetSpan(sizeof(ulong)), value);
        writer.Advance(sizeof(ulong));
    }

    private static void WriteVarUInt32(ArrayBufferWriter<byte> writer, uint value)
    {
        while (value >= 0x80)
        {
            WriteByte(writer, (byte)(value | 0x80));
            value >>= 7;
        }
        WriteByte(writer, (byte)value);
    }

    private static ReadOnlySpan<byte> Read(ReadOnlySpan<byte> source, ref int offset, int count)
    {
        if (count < 0 || offset > source.Length - count)
            throw new InvalidDataException("Actor-hour frame is truncated");
        var value = source.Slice(offset, count);
        offset += count;
        return value;
    }

    private static byte ReadByte(ReadOnlySpan<byte> source, ref int offset) =>
        Read(source, ref offset, 1)[0];

    private static long ReadInt64(ReadOnlySpan<byte> source, ref int offset) =>
        BinaryPrimitives.ReadInt64BigEndian(Read(source, ref offset, sizeof(long)));

    private static uint ReadUInt32(ReadOnlySpan<byte> source, ref int offset) =>
        BinaryPrimitives.ReadUInt32BigEndian(Read(source, ref offset, sizeof(uint)));

    private static ulong ReadUInt64(ReadOnlySpan<byte> source, ref int offset) =>
        BinaryPrimitives.ReadUInt64BigEndian(Read(source, ref offset, sizeof(ulong)));

    private static uint ReadVarUInt32(ReadOnlySpan<byte> source, ref int offset)
    {
        uint value = 0;
        for (var shift = 0; shift <= 28; shift += 7)
        {
            var current = ReadByte(source, ref offset);
            if (shift == 28 && current > 0x0f)
                throw new InvalidDataException("Actor-hour frame varint overflows");
            value |= (uint)(current & 0x7f) << shift;
            if ((current & 0x80) == 0)
            {
                if (shift > 0 && current == 0)
                    throw new InvalidDataException("Actor-hour frame varint is non-canonical");
                return value;
            }
        }
        throw new InvalidDataException("Actor-hour frame varint is invalid");
    }
}

public interface IHeatActorHourFrameSink
{
    ValueTask WriteAsync(HeatActorHourFrame frame, CancellationToken cancellationToken);
}

/// <summary>
/// Publishes an immutable sealed frame with a same-directory temp file, file
/// content flush, atomic rename, and a Linux parent-directory fsync. Replays
/// are idempotent and conflicting content is rejected.
/// </summary>
public sealed class FileHeatActorHourFrameSink(string directory) : IHeatActorHourFrameSink
{
    public async ValueTask WriteAsync(
        HeatActorHourFrame frame,
        CancellationToken cancellationToken)
    {
        if (frame.State != HeatActorHourFrameState.Sealed)
            throw new InvalidOperationException(
                "Only a frozen, sealed actor-hour frame may be published");
        var encoded = HeatActorHourFrameCodec.Encode(frame);
        Directory.CreateDirectory(directory);
        var destination = Path.Combine(directory, $"actor-hour-{frame.UnixHour}.chah");
        if (await IsExistingReplayAsync(destination, encoded, cancellationToken))
        {
            FlushDirectoryOnLinux(directory);
            return;
        }

        var temporary = Path.Combine(
            directory, $".{Path.GetFileName(destination)}.{Guid.NewGuid():N}.tmp");
        try
        {
            await using (var stream = new FileStream(
                             temporary,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None,
                             64 * 1024,
                             FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await stream.WriteAsync(encoded, cancellationToken);
                await stream.FlushAsync(cancellationToken);
                stream.Flush(flushToDisk: true);
            }
            try
            {
                File.Move(temporary, destination, overwrite: false);
                FlushDirectoryOnLinux(directory);
            }
            catch (IOException) when (File.Exists(destination))
            {
                if (!await IsExistingReplayAsync(destination, encoded, cancellationToken))
                    throw;
                FlushDirectoryOnLinux(directory);
            }
        }
        finally
        {
            if (File.Exists(temporary)) File.Delete(temporary);
        }
    }

    public static async ValueTask<HeatActorHourFrame> ReadAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var encoded = await File.ReadAllBytesAsync(path, cancellationToken);
        return HeatActorHourFrameCodec.Decode(encoded);
    }

    private static async ValueTask<bool> IsExistingReplayAsync(
        string path,
        byte[] encoded,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(path)) return false;
        var existing = await File.ReadAllBytesAsync(path, cancellationToken);
        if (existing.AsSpan().SequenceEqual(encoded)) return true;
        throw new InvalidDataException(
            $"Actor-hour frame {Path.GetFileName(path)} conflicts with an existing frame");
    }

    private static void FlushDirectoryOnLinux(string path)
    {
        if (!OperatingSystem.IsLinux()) return;
        const int readOnly = 0;
        const int directory = 0x10000;
        const int closeOnExec = 0x80000;
        var handle = Open(path, readOnly | directory | closeOnExec);
        if (handle < 0)
            throw new IOException(
                $"Unable to open actor-hour frame directory for fsync (errno {Marshal.GetLastPInvokeError()})");
        try
        {
            if (Fsync(handle) != 0)
                throw new IOException(
                    $"Unable to fsync actor-hour frame directory (errno {Marshal.GetLastPInvokeError()})");
        }
        finally
        {
            _ = Close(handle);
        }
    }

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int Open(string path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int Fsync(int fileDescriptor);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int Close(int fileDescriptor);
}
