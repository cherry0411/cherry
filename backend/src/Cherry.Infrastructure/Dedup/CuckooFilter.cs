using System.Buffers.Binary;
using System.IO.Compression;
using Cherry.Domain.Interfaces;

namespace Cherry.Infrastructure.Dedup;

public class CuckooFilter : IDedupFilter, IDisposable
{
    private readonly int _bucketCount;
    private readonly ulong[] _buckets;
    private long _count;
    private readonly int _maxKicks = 500;
    private readonly string? _persistPath;
    // Protects _buckets array during Save() snapshot: concurrent Add/MightContain hold ReadLock,
    // Save() holds WriteLock only for the brief BlockCopy snapshot window.
    private readonly ReaderWriterLockSlim _saveLock = new(LockRecursionPolicy.NoRecursion);

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

                // Take a consistent snapshot of the buckets array under write lock.
                // The lock window is only the BlockCopy (microseconds), not the I/O.
                byte[] buf;
                long count;
                _saveLock.EnterWriteLock();
                try
                {
                    count = Volatile.Read(ref _count);
                    buf = new byte[_bucketCount * sizeof(ulong)];
                    Buffer.BlockCopy(_buckets, 0, buf, 0, buf.Length);
                }
                finally { _saveLock.ExitWriteLock(); }

                using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var gz = new GZipStream(fs, CompressionLevel.Fastest))
                using (var bw = new BinaryWriter(gz))
                {
                    bw.Write(_bucketCount);
                    bw.Write(count);
                    bw.Write(buf);
                    bw.Flush();
                    gz.Flush();
                    fs.Flush(true);
                }

                File.Move(tmp, _persistPath, overwrite: true);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"CuckooFilter.Save failed: {ex}");
            }
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
            var expectedBytes = _bucketCount * sizeof(ulong);
            var buf = new byte[expectedBytes];
            // ReadExactly on the underlying stream ensures all bytes are read even through GZip
            gz.ReadExactly(buf, 0, expectedBytes);
            Buffer.BlockCopy(buf, 0, _buckets, 0, buf.Length);
        }
        catch { }
    }

    public bool MightContain(string infoHash)
    {
        var (i1, i2, fp) = ComputeHash(infoHash);
        return BucketContains(i1, fp) || BucketContains(i2, fp);
    }

    public bool Add(string infoHash)
    {
        var (i1, i2, fp) = ComputeHash(infoHash);

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

    /// <summary>
    /// InfoHash is already a 40-char hex string (SHA1 output) ¡ª high entropy throughout.
    /// Parse the first 16 hex chars (= 8 bytes) directly into two uint64 halves;
    /// derive bucket indices and fingerprint without any additional hashing.
    /// Zero allocations: stackalloc + Convert.FromHexString into a fixed span.
    /// </summary>
    private (int i1, int i2, ushort fp) ComputeHash(string infoHash)
    {
        // Parse hex chars directly from the 40-char infohash string.
        // infohash is SHA1 hex ¡ª high entropy, no extra hashing needed.
        var lo = Convert.FromHexString(infoHash.AsSpan(0, 16));   // 8 bytes
        var hi = Convert.FromHexString(infoHash.AsSpan(16, 8));   // 4 bytes

        var h1 = BinaryPrimitives.ReadUInt64BigEndian(lo);
        var h2 = BinaryPrimitives.ReadUInt32BigEndian(hi);

        var i1 = (int)(h1 % (ulong)_bucketCount);
        var i2 = (int)(h2 % (uint)_bucketCount);

        // Derive fingerprint from chars 24-27 (2 bytes)
        var fpBytes = Convert.FromHexString(infoHash.AsSpan(24, 4)); // 2 bytes
        var fpRaw = BinaryPrimitives.ReadUInt16BigEndian(fpBytes);
        // Mix to spread fingerprint bits (avoid fp==0 edge case)
        fpRaw ^= (ushort)(fpRaw >> 8);
        if (fpRaw == 0) fpRaw = 1;

        return (i1, i2, fpRaw);
    }

    public void Dispose()
    {
        _saveLock.Dispose();
    }
}
