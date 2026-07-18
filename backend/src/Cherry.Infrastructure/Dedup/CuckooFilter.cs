using System.Buffers.Binary;
using System.IO.Compression;
using System.Security.Cryptography;
using Cherry.Domain.Interfaces;

namespace Cherry.Infrastructure.Dedup;

/// <summary>
/// A probabilistic membership filter. A negative result is definitive, but a
/// positive result can be a fingerprint collision and must be confirmed by the
/// exact authority before data is discarded or reported as present.
/// </summary>
public class CuckooFilter : IDedupFilter, IDisposable
{
    private const int SlotsPerBucket = 4;
    private const int MaxKicks = 500;
    private const int SnapshotVersion = 1;
    private const int ChecksumLength = 32;
    private const int SnapshotHeaderLength = 72;
    private const int CompressedLengthOffset = 32;

    private static readonly byte[] SnapshotMagic = "CHCKFLT\0"u8.ToArray();

    private readonly int _bucketCount;
    private readonly ulong[] _buckets;
    private readonly string? _persistPath;
    private readonly ReaderWriterLockSlim _operationLock =
        new(LockRecursionPolicy.NoRecursion);
    private readonly object _saveGate = new();
    private long _count;

    public long Count => Volatile.Read(ref _count);

    public CuckooFilter(
        long capacity = 100_000_000,
        string? persistPath = null,
        bool loadPersistedSnapshot = true)
    {
        _bucketCount = ComputeBucketCount(capacity);
        _buckets = new ulong[_bucketCount];
        _persistPath = persistPath;

        if (persistPath is not null && loadPersistedSnapshot)
            Load();
    }

    /// <summary>
    /// Saves a versioned, checksummed snapshot using an atomic replace. Any I/O
    /// or serialization failure is propagated to the caller.
    /// </summary>
    public void Save()
    {
        if (_persistPath is null)
            return;

        lock (_saveGate)
        {
            var directory = Path.GetDirectoryName(_persistPath);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            var temporaryPath = _persistPath + ".tmp";

            try
            {
                byte[] payload;
                long count;

                // Readers may continue, but writers are excluded while the
                // consistent in-memory image is copied.
                _operationLock.EnterReadLock();
                try
                {
                    count = _count;
                    payload = new byte[checked(_bucketCount * sizeof(ulong))];
                    Buffer.BlockCopy(_buckets, 0, payload, 0, payload.Length);
                }
                finally
                {
                    _operationLock.ExitReadLock();
                }

                var checksum = SHA256.HashData(payload);

                using (var stream = new FileStream(
                           temporaryPath,
                           FileMode.Create,
                           FileAccess.ReadWrite,
                           FileShare.None))
                {
                    using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
                    {
                        writer.Write(SnapshotMagic);
                        writer.Write(SnapshotVersion);
                        writer.Write(_bucketCount);
                        writer.Write(count);
                        writer.Write((long)payload.Length);
                        writer.Write(0L); // Patched after compression completes.
                        writer.Write(checksum);
                        writer.Flush();
                    }

                    if (stream.Position != SnapshotHeaderLength)
                        throw new InvalidOperationException("Cuckoo snapshot header length is inconsistent.");

                    var payloadOffset = stream.Position;
                    using (var gzip = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
                    {
                        gzip.Write(payload);
                    }

                    var compressedLength = stream.Position - payloadOffset;
                    stream.Position = CompressedLengthOffset;
                    using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
                    {
                        writer.Write(compressedLength);
                        writer.Flush();
                    }

                    stream.Flush(flushToDisk: true);
                }

                File.Move(temporaryPath, _persistPath, overwrite: true);
            }
            catch
            {
                try
                {
                    if (File.Exists(temporaryPath))
                        File.Delete(temporaryPath);
                }
                catch (Exception cleanupException)
                {
                    Console.Error.WriteLine(
                        $"Failed to clean up CuckooFilter temporary snapshot '{temporaryPath}': {cleanupException}");
                }

                throw;
            }
        }
    }

    private void Load()
    {
        if (_persistPath is null || !File.Exists(_persistPath))
            return;

        try
        {
            using var stream = new FileStream(_persistPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (stream.Length >= 2)
            {
                var first = stream.ReadByte();
                var second = stream.ReadByte();
                stream.Position = 0;
                if (first == 0x1F && second == 0x8B)
                {
                    throw InvalidSnapshot(
                        "unsupported legacy format; rebuild the filter from the exact store");
                }
            }

            if (stream.Length < SnapshotHeaderLength)
                throw InvalidSnapshot("snapshot is truncated before the complete header");

            using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true);
            var magic = reader.ReadBytes(SnapshotMagic.Length);
            if (!magic.AsSpan().SequenceEqual(SnapshotMagic))
            {
                throw InvalidSnapshot(
                    "unsupported legacy format or invalid magic; rebuild the filter from the exact store");
            }

            var version = reader.ReadInt32();
            if (version != SnapshotVersion)
            {
                throw InvalidSnapshot(
                    $"unsupported snapshot version {version}; expected {SnapshotVersion}");
            }

            var persistedBucketCount = reader.ReadInt32();
            if (persistedBucketCount != _bucketCount)
            {
                throw InvalidSnapshot(
                    $"bucket count {persistedBucketCount} does not match configured count {_bucketCount}");
            }

            var persistedCount = reader.ReadInt64();
            var payloadLength = reader.ReadInt64();
            var compressedLength = reader.ReadInt64();
            var expectedChecksum = reader.ReadBytes(ChecksumLength);
            if (expectedChecksum.Length != ChecksumLength)
                throw InvalidSnapshot("snapshot checksum is truncated");

            var expectedPayloadLength = checked((long)_bucketCount * sizeof(ulong));
            if (payloadLength != expectedPayloadLength)
            {
                throw InvalidSnapshot(
                    $"payload length {payloadLength} does not match expected length {expectedPayloadLength}");
            }

            if (persistedCount < 0 || persistedCount > (long)_bucketCount * SlotsPerBucket)
                throw InvalidSnapshot($"entry count {persistedCount} is outside the valid range");

            if (compressedLength <= 0 || compressedLength != stream.Length - SnapshotHeaderLength)
            {
                throw InvalidSnapshot(
                    $"compressed length {compressedLength} does not match file length {stream.Length}");
            }

            var payload = new byte[checked((int)payloadLength)];
            using (var gzip = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true))
            {
                gzip.ReadExactly(payload);
                if (gzip.ReadByte() != -1)
                    throw InvalidSnapshot("snapshot contains trailing uncompressed data");
            }

            var actualChecksum = SHA256.HashData(payload);
            if (!CryptographicOperations.FixedTimeEquals(actualChecksum, expectedChecksum))
                throw InvalidSnapshot("snapshot checksum does not match its payload");

            var occupiedSlots = CountOccupiedSlots(payload);
            if (persistedCount != occupiedSlots)
            {
                throw InvalidSnapshot(
                    $"entry count {persistedCount} does not match {occupiedSlots} occupied slots");
            }

            Buffer.BlockCopy(payload, 0, _buckets, 0, payload.Length);
            _count = persistedCount;
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (EndOfStreamException exception)
        {
            throw InvalidSnapshot("snapshot ended unexpectedly", exception);
        }
    }

    public bool MightContain(string infoHash)
    {
        var (i1, i2, fingerprint) = ComputeHash(infoHash);

        _operationLock.EnterReadLock();
        try
        {
            return BucketContains(i1, fingerprint) || BucketContains(i2, fingerprint);
        }
        finally
        {
            _operationLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Adds a fingerprint if it is not already represented by its candidate
    /// bucket pair. Returns false for a duplicate/fingerprint collision or when
    /// no relocation path can be found. This is not an exact uniqueness check.
    /// </summary>
    public bool Add(string infoHash)
    {
        var (i1, i2, fingerprint) = ComputeHash(infoHash);

        // An exclusive mutation section makes duplicate detection plus insertion
        // one operation and prevents readers from observing a relocation midway.
        _operationLock.EnterWriteLock();
        try
        {
            if (BucketContains(i1, fingerprint) || BucketContains(i2, fingerprint))
                return false;

            if (TryInsertIntoBucket(i1, fingerprint) || TryInsertIntoBucket(i2, fingerprint))
            {
                _count++;
                return true;
            }

            if (!TryRelocateAndInsert(i1, i2, fingerprint))
                return false;

            _count++;
            return true;
        }
        finally
        {
            _operationLock.ExitWriteLock();
        }
    }

    private bool TryRelocateAndInsert(int i1, int i2, ushort newFingerprint)
    {
        // Search first and mutate only after a complete path to an empty slot is
        // known. Thus a failed insertion can never evict an existing fingerprint.
        var nodes = new List<RelocationNode>(MaxKicks);
        var queue = new Queue<int>();
        var visitedSlots = new HashSet<long>();

        EnqueueBucket(i1, parent: -1);
        EnqueueBucket(i2, parent: -1);

        while (queue.Count > 0 && nodes.Count <= MaxKicks)
        {
            var nodeIndex = queue.Dequeue();
            var node = nodes[nodeIndex];
            var alternateBucket = AltIndex(node.Bucket, node.Fingerprint);
            var emptySlot = FindEmptySlot(alternateBucket);

            if (emptySlot >= 0)
            {
                CommitRelocation(nodes, nodeIndex, alternateBucket, emptySlot, newFingerprint);
                return true;
            }

            EnqueueBucket(alternateBucket, nodeIndex);
        }

        return false;

        void EnqueueBucket(int bucket, int parent)
        {
            for (var slot = 0; slot < SlotsPerBucket && nodes.Count < MaxKicks; slot++)
            {
                var slotId = ((long)bucket * SlotsPerBucket) + slot;
                if (!visitedSlots.Add(slotId))
                    continue;

                var fingerprint = GetSlot(bucket, slot);
                if (fingerprint == 0)
                    continue;

                nodes.Add(new RelocationNode(bucket, slot, fingerprint, parent));
                queue.Enqueue(nodes.Count - 1);
            }
        }
    }

    private void CommitRelocation(
        IReadOnlyList<RelocationNode> nodes,
        int leafIndex,
        int emptyBucket,
        int emptySlot,
        ushort newFingerprint)
    {
        var leaf = nodes[leafIndex];
        SetSlot(emptyBucket, emptySlot, leaf.Fingerprint);

        var childIndex = leafIndex;
        while (nodes[childIndex].Parent >= 0)
        {
            var child = nodes[childIndex];
            var parent = nodes[child.Parent];
            SetSlot(child.Bucket, child.Slot, parent.Fingerprint);
            childIndex = child.Parent;
        }

        var root = nodes[childIndex];
        SetSlot(root.Bucket, root.Slot, newFingerprint);
    }

    private bool BucketContains(int bucket, ushort fingerprint)
    {
        var packed = _buckets[bucket];
        return ((packed & 0xFFFF) == fingerprint) ||
               (((packed >> 16) & 0xFFFF) == fingerprint) ||
               (((packed >> 32) & 0xFFFF) == fingerprint) ||
               (((packed >> 48) & 0xFFFF) == fingerprint);
    }

    private bool TryInsertIntoBucket(int bucket, ushort fingerprint)
    {
        var emptySlot = FindEmptySlot(bucket);
        if (emptySlot < 0)
            return false;

        SetSlot(bucket, emptySlot, fingerprint);
        return true;
    }

    private int FindEmptySlot(int bucket)
    {
        var packed = _buckets[bucket];
        for (var slot = 0; slot < SlotsPerBucket; slot++)
        {
            if (((packed >> (slot * 16)) & 0xFFFF) == 0)
                return slot;
        }

        return -1;
    }

    private ushort GetSlot(int bucket, int slot) =>
        (ushort)((_buckets[bucket] >> (slot * 16)) & 0xFFFF);

    private void SetSlot(int bucket, int slot, ushort fingerprint)
    {
        var shift = slot * 16;
        var mask = 0xFFFFUL << shift;
        _buckets[bucket] = (_buckets[bucket] & ~mask) | ((ulong)fingerprint << shift);
    }

    // bucketCount (n) is even and FingerprintSum(fp) (H) is odd. Defining
    // alternate(i, fp) = (H - i) mod n gives an exact involution:
    // alternate(alternate(i, fp), fp) = H - (H - i) = i (mod n).
    // H being odd also prevents a self-pair, because 2i cannot equal H mod an
    // even n. This retains arbitrary-sized bucket arrays without power-of-two
    // rounding waste while preserving the cuckoo pair invariant.
    private int AltIndex(int bucket, ushort fingerprint)
    {
        var sum = FingerprintSum(fingerprint);
        var alternate = ((long)sum - bucket) % _bucketCount;
        return (int)(alternate < 0 ? alternate + _bucketCount : alternate);
    }

    private uint FingerprintSum(ushort fingerprint)
    {
        var value = (uint)fingerprint;
        value ^= value >> 7;
        value *= 0x5BD1E995u;
        value ^= value >> 15;

        var sum = value % (uint)_bucketCount;
        return (sum & 1) == 0 ? sum + 1 : sum;
    }

    private (int i1, int i2, ushort fingerprint) ComputeHash(string infoHash)
    {
        ArgumentNullException.ThrowIfNull(infoHash);
        if (infoHash.Length != 40)
            throw new ArgumentException("InfoHash must contain exactly 40 hexadecimal characters.", nameof(infoHash));

        Span<byte> bytes = stackalloc byte[20];
        var status = Convert.FromHexString(
            infoHash.AsSpan(),
            bytes,
            out var charsConsumed,
            out var bytesWritten);
        if (status != System.Buffers.OperationStatus.Done ||
            charsConsumed != infoHash.Length ||
            bytesWritten != bytes.Length)
        {
            throw new ArgumentException(
                "InfoHash must contain exactly 40 hexadecimal characters.",
                nameof(infoHash));
        }

        var primaryHash = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        var i1 = (int)(primaryHash % (ulong)_bucketCount);

        var fingerprint = BinaryPrimitives.ReadUInt16BigEndian(bytes[12..]);
        fingerprint ^= (ushort)(fingerprint >> 8);
        if (fingerprint == 0)
            fingerprint = 1;

        var i2 = AltIndex(i1, fingerprint);
        return (i1, i2, fingerprint);
    }

    private static int ComputeBucketCount(long capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be positive.");

        var minimumBuckets = Math.Max(2L, checked((capacity + SlotsPerBucket - 1) / SlotsPerBucket));
        var bucketCount = (minimumBuckets & 1) == 0 ? minimumBuckets : checked(minimumBuckets + 1);
        if (bucketCount > int.MaxValue / sizeof(ulong))
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity is too large for this implementation.");

        return (int)bucketCount;
    }

    private static long CountOccupiedSlots(ReadOnlySpan<byte> payload)
    {
        long occupied = 0;
        for (var offset = 0; offset < payload.Length; offset += sizeof(ushort))
        {
            if (BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]) != 0)
                occupied++;
        }

        return occupied;
    }

    private InvalidDataException InvalidSnapshot(string detail, Exception? innerException = null) =>
        new($"CuckooFilter snapshot '{_persistPath}' is invalid: {detail}.", innerException);

    public void Dispose() => _operationLock.Dispose();

    private readonly record struct RelocationNode(
        int Bucket,
        int Slot,
        ushort Fingerprint,
        int Parent);
}
