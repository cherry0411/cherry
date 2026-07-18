using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Cherry.Infrastructure.Heat;

public readonly record struct HeatFrameEntry(long TorrentId, long Count);

public sealed record EncodedHeatFrame(short Shard, int EntryCount, byte[] Payload, byte[] Sha256);

public static class HeatFrameCodec
{
    public const short Version = 1;
    public const int ShardCount = 64;

    public static IReadOnlyList<EncodedHeatFrame> Encode(IEnumerable<HeatFrameEntry> orderedEntries)
    {
        var buffers = Enumerable.Range(0, ShardCount).Select(_ => new ArrayBufferWriter<byte>()).ToArray();
        var previous = new ulong[ShardCount];
        var counts = new int[ShardCount];
        long lastId = -1;
        foreach (var entry in orderedEntries)
        {
            if (entry.TorrentId < 0 || entry.TorrentId <= lastId || entry.Count <= 0)
                throw new InvalidDataException("Heat frame input must have strictly increasing non-negative IDs and positive counts");
            lastId = entry.TorrentId;
            var shard = (int)(entry.TorrentId & 63);
            var quotient = (ulong)entry.TorrentId >> 6;
            WriteUVarint(buffers[shard], quotient - previous[shard]);
            WriteUVarint(buffers[shard], checked((ulong)entry.Count));
            previous[shard] = quotient;
            counts[shard]++;
        }

        return Enumerable.Range(0, ShardCount)
            .Select(shard =>
            {
                var payload = buffers[shard].WrittenSpan.ToArray();
                return new EncodedHeatFrame((short)shard, counts[shard], payload, SHA256.HashData(payload));
            })
            .ToArray();
    }

    public static IReadOnlyList<HeatFrameEntry> Decode(short shard, int entryCount, ReadOnlySpan<byte> payload)
    {
        if (shard is < 0 or >= ShardCount || entryCount < 0)
            throw new InvalidDataException("Invalid heat frame metadata");
        var result = new HeatFrameEntry[entryCount];
        var offset = 0;
        ulong quotient = 0;
        for (var index = 0; index < entryCount; index++)
        {
            quotient = checked(quotient + ReadUVarint(payload, ref offset));
            var count = ReadUVarint(payload, ref offset);
            if (count == 0 || quotient > ((ulong)long.MaxValue >> 6))
                throw new InvalidDataException("Heat frame entry is outside supported bounds");
            var id = checked((long)((quotient << 6) | (ushort)shard));
            if (index > 0 && id <= result[index - 1].TorrentId)
                throw new InvalidDataException("Heat frame IDs are not canonical");
            result[index] = new HeatFrameEntry(id, checked((long)count));
        }
        if (offset != payload.Length)
            throw new InvalidDataException("Heat frame payload has trailing bytes");
        return result;
    }

    public static byte[] ManifestDigest(DateOnly day, IReadOnlyList<EncodedHeatFrame> frames)
    {
        if (frames.Count != ShardCount) throw new ArgumentException("A manifest requires exactly 64 frames", nameof(frames));
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteInt32BigEndian(header[..4], day.DayNumber);
        hash.AppendData(header[..4]);
        foreach (var frame in frames.OrderBy(frame => frame.Shard))
        {
            BinaryPrimitives.WriteInt16BigEndian(header[..2], frame.Shard);
            BinaryPrimitives.WriteInt32BigEndian(header[2..6], frame.EntryCount);
            hash.AppendData(header[..6]);
            hash.AppendData(frame.Sha256);
        }
        return hash.GetHashAndReset();
    }

    private static void WriteUVarint(IBufferWriter<byte> output, ulong value)
    {
        Span<byte> bytes = stackalloc byte[10];
        var length = 0;
        while (value >= 0x80)
        {
            bytes[length++] = (byte)((value & 0x7f) | 0x80);
            value >>= 7;
        }
        bytes[length++] = (byte)value;
        output.Write(bytes[..length]);
    }

    private static ulong ReadUVarint(ReadOnlySpan<byte> data, ref int offset)
    {
        ulong value = 0;
        var start = offset;
        for (var shift = 0; shift <= 63; shift += 7)
        {
            if (offset >= data.Length) throw new InvalidDataException("Truncated heat frame varint");
            var current = data[offset++];
            if (shift == 63 && current > 1) throw new InvalidDataException("Heat frame varint overflow");
            value |= (ulong)(current & 0x7f) << shift;
            if ((current & 0x80) == 0)
            {
                if (offset - start > 1 && current == 0)
                    throw new InvalidDataException("Non-canonical heat frame varint");
                return value;
            }
        }
        throw new InvalidDataException("Heat frame varint overflow");
    }
}
