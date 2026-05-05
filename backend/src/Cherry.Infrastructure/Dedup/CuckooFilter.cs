using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using Cherry.Domain.Interfaces;

namespace Cherry.Infrastructure.Dedup;

public class CuckooFilter : IDedupFilter, IDisposable
{
    private readonly int _bucketCount;
    private readonly ulong[] _buckets;
    private long _count;
    private readonly int _maxKicks = 500;
    private readonly string? _persistPath;

    public long Count => Volatile.Read(ref _count);

    public CuckooFilter(long capacity = 100_000_000, string? persistPath = null)
    {
        _bucketCount = (int)(capacity / 4) + 1;
        _buckets = new ulong[_bucketCount];
        _persistPath = persistPath;
        if (persistPath != null) Load();
    }

    public void Save()
    {
        if (_persistPath == null) return;
        try
        {
            var dir = Path.GetDirectoryName(_persistPath);
            if (dir != null) Directory.CreateDirectory(dir);
            var tmp = _persistPath + ".tmp";
            using var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write);
            using var gz = new GZipStream(fs, CompressionLevel.Fastest);
            using var bw = new BinaryWriter(gz);
            bw.Write(_bucketCount);
            bw.Write(Volatile.Read(ref _count));
            var buf = new byte[_bucketCount * sizeof(ulong)];
            Buffer.BlockCopy(_buckets, 0, buf, 0, buf.Length);
            bw.Write(buf);
            File.Move(tmp, _persistPath, overwrite: true);
        }
        catch { }
    }

    private void Load()
    {
        if (_persistPath == null || !File.Exists(_persistPath)) return;
        try
        {
            using var fs = new FileStream(_persistPath, FileMode.Open, FileAccess.Read);
            using var gz = new GZipStream(fs, CompressionMode.Decompress);
            using var br = new BinaryReader(gz);
            var count = br.ReadInt32();
            if (count != _bucketCount) return;
            _count = br.ReadInt64();
            var buf = br.ReadBytes(_bucketCount * sizeof(ulong));
            if (buf.Length == _buckets.Length * sizeof(ulong))
                Buffer.BlockCopy(buf, 0, _buckets, 0, buf.Length);
        }
        catch { }
    }

    public bool MightContain(string infoHash)
    {
        var fp = Fingerprint(infoHash);
        var (i1, i2) = BucketIndices(infoHash);

        return BucketContains(i1, fp) || BucketContains(i2, fp);
    }

    public bool Add(string infoHash)
    {
        var fp = Fingerprint(infoHash);
        if (fp == 0) fp = 1;

        var (i1, i2) = BucketIndices(infoHash);

        if (TryInsertIntoBucket(i1, fp) || TryInsertIntoBucket(i2, fp))
        {
            Interlocked.Increment(ref _count);
            return true;
        }

        var idx = (Random.Shared.Next() & 1) == 0 ? i1 : i2;
        for (var n = 0; n < _maxKicks; n++)
        {
            var slot = Random.Shared.Next(4);
            var oldFp = SwapSlot(idx, slot, fp);
            if (oldFp == 0)
            {
                Interlocked.Increment(ref _count);
                return true;
            }
            fp = oldFp;
            idx = AltIndex(idx, fp);
        }

        return false;
    }

    private bool BucketContains(int bucket, ushort fp)
    {
        var packed = Volatile.Read(ref _buckets[bucket]);
        return ((packed & 0xFFFF) == fp) ||
               (((packed >> 16) & 0xFFFF) == fp) ||
               (((packed >> 32) & 0xFFFF) == fp) ||
               (((packed >> 48) & 0xFFFF) == fp);
    }

    private bool TryInsertIntoBucket(int bucket, ushort fp)
    {
        var fp64 = (ulong)fp;
        while (true)
        {
            var packed = Volatile.Read(ref _buckets[bucket]);
            ulong newPacked;
            if ((packed & 0xFFFF) == 0) newPacked = packed | fp64;
            else if (((packed >> 16) & 0xFFFF) == 0) newPacked = packed | (fp64 << 16);
            else if (((packed >> 32) & 0xFFFF) == 0) newPacked = packed | (fp64 << 32);
            else if (((packed >> 48) & 0xFFFF) == 0) newPacked = packed | (fp64 << 48);
            else return false;

            if (Interlocked.CompareExchange(ref _buckets[bucket], newPacked, packed) == packed)
                return true;
        }
    }

    private ushort SwapSlot(int bucket, int slot, ushort newFp)
    {
        var shift = slot * 16;
        var mask = 0xFFFFul << shift;
        while (true)
        {
            var packed = Volatile.Read(ref _buckets[bucket]);
            var oldFp = (ushort)((packed >> shift) & 0xFFFF);
            var newPacked = (packed & ~mask) | ((ulong)newFp << shift);
            if (Interlocked.CompareExchange(ref _buckets[bucket], newPacked, packed) == packed)
                return oldFp;
        }
    }

    private int AltIndex(int idx, ushort fp) => (int)(((uint)idx ^ ((uint)fp * 0x5bd1e995)) % (uint)_bucketCount);

    private (int, int) BucketIndices(string infoHash)
    {
        var bytes = Encoding.UTF8.GetBytes(infoHash);
        var hash = SHA256.HashData(bytes);
        var h1 = BitConverter.ToUInt64(hash, 0);
        var h2 = BitConverter.ToUInt32(hash, 8);
        return (
            (int)(h1 % (ulong)_bucketCount),
            (int)(h2 % (uint)_bucketCount)
        );
    }

    private ushort Fingerprint(string infoHash)
    {
        var bytes = Encoding.UTF8.GetBytes(infoHash);
        var hash = SHA256.HashData(bytes);
        var h = BitConverter.ToUInt32(hash, 12);
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        var fp = (ushort)(h & 0xFFFF);
        if (fp == 0) fp = 1;
        return fp;
    }

    public void Dispose()
    {
    }
}
