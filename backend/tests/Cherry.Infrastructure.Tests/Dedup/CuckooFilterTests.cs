using System.Collections.Concurrent;
using System.IO.Compression;
using Cherry.Infrastructure.Dedup;
using Xunit;

namespace Cherry.Infrastructure.Tests.Dedup;

public sealed class CuckooFilterTests
{
    [Fact]
    public void Add_Duplicate_DoesNotIncreaseCountOrOccupyAnotherSlot()
    {
        using var filter = new CuckooFilter(capacity: 128);
        var hash = HashFor(42);

        Assert.True(filter.Add(hash));
        Assert.False(filter.Add(hash));
        Assert.False(filter.Add(hash.ToUpperInvariant()));
        Assert.Equal(1, filter.Count);
        Assert.True(filter.MightContain(hash));
    }

    [Fact]
    public void ConcurrentAdd_OfSameHash_HasSingleWinner()
    {
        using var filter = new CuckooFilter(capacity: 128);
        var hash = HashFor(7);
        var successes = 0;

        Parallel.For(0, 1_000, _ =>
        {
            if (filter.Add(hash))
                Interlocked.Increment(ref successes);
        });

        Assert.Equal(1, successes);
        Assert.Equal(1, filter.Count);
        Assert.True(filter.MightContain(hash));
    }

    [Fact]
    public void ConcurrentAdd_OfRandomHashes_PreservesEverySuccessfulInsert()
    {
        using var filter = new CuckooFilter(capacity: 4_096);
        var successful = new ConcurrentBag<string>();

        Parallel.For(0, 3_500, value =>
        {
            var hash = HashFor(value, seed: 71);
            if (filter.Add(hash))
                successful.Add(hash);
        });

        Assert.Equal(successful.Count, filter.Count);
        Assert.All(successful, hash => Assert.True(filter.MightContain(hash)));
    }

    [Theory]
    [InlineData(17)]
    [InlineData(29)]
    [InlineData(41)]
    [InlineData(53)]
    public void RandomInsertions_NearNinetyPercentLoad_NeverLoseSuccessfulItems(int seed)
    {
        // 504 produces 126 buckets: deliberately even but not a power of two,
        // exercising the modular alternate-bucket involution.
        const int physicalCapacity = 504;
        const int targetCount = 453;
        using var filter = new CuckooFilter(capacity: physicalCapacity);
        var random = new Random(seed);
        var successful = new List<string>(targetCount);

        for (var attempt = 0; attempt < 10_000 && successful.Count < targetCount; attempt++)
        {
            var bytes = new byte[20];
            random.NextBytes(bytes);
            var hash = Convert.ToHexString(bytes).ToLowerInvariant();

            if (!filter.Add(hash))
                continue;

            successful.Add(hash);
            Assert.All(successful, item => Assert.True(filter.MightContain(item)));
        }

        Assert.True(successful.Count >= targetCount,
            $"Seed {seed} reached only {successful.Count}/{targetCount} entries.");
        Assert.Equal(successful.Count, filter.Count);
    }

    [Fact]
    public void FailedInsertion_DoesNotLosePreviouslySuccessfulItems()
    {
        using var filter = new CuckooFilter(capacity: 64);
        var successful = new List<string>();
        var observedFailure = false;

        for (var value = 0; value < 100_000; value++)
        {
            var hash = HashFor(value, seed: 31337);
            if (filter.Add(hash))
                successful.Add(hash);
            else if (successful.Count >= 60)
                observedFailure = true;

            Assert.All(successful, item => Assert.True(filter.MightContain(item)));
            if (observedFailure)
                break;
        }

        Assert.True(observedFailure);
    }

    [Fact]
    public void SaveAndLoad_RoundTripsCountAndMembership()
    {
        using var temporary = new TemporaryDirectory();
        var path = Path.Combine(temporary.Path, "cuckoo.dat");
        var hashes = Enumerable.Range(0, 300).Select(value => HashFor(value, 97)).ToArray();

        using (var writer = new CuckooFilter(capacity: 512, persistPath: path))
        {
            foreach (var hash in hashes)
                Assert.True(writer.Add(hash));
            writer.Save();
        }

        using var reader = new CuckooFilter(capacity: 512, persistPath: path);
        Assert.Equal(hashes.Length, reader.Count);
        Assert.All(hashes, hash => Assert.True(reader.MightContain(hash)));
    }

    [Fact]
    public void Save_IoFailure_IsPropagatedToCaller()
    {
        using var temporary = new TemporaryDirectory();
        var fileInPlaceOfDirectory = Path.Combine(temporary.Path, "not-a-directory");
        File.WriteAllText(fileInPlaceOfDirectory, "occupied");
        var path = Path.Combine(fileInPlaceOfDirectory, "cuckoo.dat");
        using var filter = new CuckooFilter(capacity: 128, persistPath: path);
        filter.Add(HashFor(1));

        Assert.ThrowsAny<IOException>(() => filter.Save());
    }

    [Fact]
    public void Load_TruncatedSnapshot_FailsExplicitly()
    {
        using var temporary = new TemporaryDirectory();
        var path = CreateSnapshot(temporary.Path);
        var bytes = File.ReadAllBytes(path);
        File.WriteAllBytes(path, bytes[..^7]);

        var exception = Assert.Throws<InvalidDataException>(
            () => new CuckooFilter(capacity: 512, persistPath: path));

        Assert.Contains("invalid", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Load_CorruptSnapshot_FailsChecksumValidation()
    {
        using var temporary = new TemporaryDirectory();
        var path = CreateSnapshot(temporary.Path);
        var bytes = File.ReadAllBytes(path);
        bytes[40] ^= 0x5A; // First byte of the persisted SHA-256 checksum.
        File.WriteAllBytes(path, bytes);

        var exception = Assert.Throws<InvalidDataException>(
            () => new CuckooFilter(capacity: 512, persistPath: path));

        Assert.Contains("checksum", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Load_SnapshotWithTrailingBytes_FailsLengthValidation()
    {
        using var temporary = new TemporaryDirectory();
        var path = CreateSnapshot(temporary.Path);
        using (var stream = new FileStream(path, FileMode.Append, FileAccess.Write))
            stream.WriteByte(0xFF);

        Assert.Throws<InvalidDataException>(
            () => new CuckooFilter(capacity: 512, persistPath: path));
    }

    [Fact]
    public void Load_LegacySnapshot_IsExplicitlyRejected()
    {
        using var temporary = new TemporaryDirectory();
        var path = Path.Combine(temporary.Path, "legacy.dat");
        using (var stream = File.Create(path))
        using (var gzip = new GZipStream(stream, CompressionLevel.Fastest))
        using (var writer = new BinaryWriter(gzip))
        {
            writer.Write(128);
            writer.Write(0L);
            writer.Write(new byte[128 * sizeof(ulong)]);
        }

        var exception = Assert.Throws<InvalidDataException>(
            () => new CuckooFilter(capacity: 512, persistPath: path));

        Assert.Contains("legacy", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("exact store", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ExactRebuildPolicy_IgnoresLegacySnapshotAndReplacesItSafely()
    {
        using var temporary = new TemporaryDirectory();
        var path = Path.Combine(temporary.Path, "legacy.dat");
        using (var stream = File.Create(path))
        using (var gzip = new GZipStream(stream, CompressionLevel.Fastest))
        using (var writer = new BinaryWriter(gzip))
        {
            writer.Write(128);
            writer.Write(0L);
            writer.Write(new byte[128 * sizeof(ulong)]);
        }

        var rebuiltHash = HashFor(404);
        using (var rebuilding = new CuckooFilter(
                   capacity: 512,
                   persistPath: path,
                   loadPersistedSnapshot: false))
        {
            Assert.True(rebuilding.Add(rebuiltHash));
            rebuilding.Save();
        }

        using var loaded = new CuckooFilter(capacity: 512, persistPath: path);
        Assert.True(loaded.MightContain(rebuiltHash));
        Assert.Equal(1, loaded.Count);
    }

    private static string CreateSnapshot(string directory)
    {
        var path = Path.Combine(directory, "cuckoo.dat");
        using var filter = new CuckooFilter(capacity: 512, persistPath: path);
        foreach (var value in Enumerable.Range(0, 300))
            filter.Add(HashFor(value, 101));
        filter.Save();
        return path;
    }

    private static string HashFor(int value, int seed = 0)
    {
        Span<byte> input = stackalloc byte[8];
        BitConverter.TryWriteBytes(input, value);
        BitConverter.TryWriteBytes(input[4..], seed);
        return Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(input)).ToLowerInvariant();
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"cherry-cuckoo-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
