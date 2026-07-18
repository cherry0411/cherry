using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Cherry.Infrastructure.Heat;

var hashCount = args.Length > 0 ? int.Parse(args[0]) : 10_000;
var actorsPerHash = args.Length > 1 ? int.Parse(args[1]) : 10;
var smallReplayIterations = args.Length > 2 ? int.Parse(args[2]) : 200;
var projectionPageSize = args.Length > 3 ? int.Parse(args[3]) : 500;
if (hashCount <= 0 || actorsPerHash <= 0 || smallReplayIterations <= 0 ||
    projectionPageSize <= 0)
    throw new ArgumentOutOfRangeException(nameof(args));

var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-bench-{Guid.NewGuid():N}");
var store = new HeatRollingStore(new HeatOptions { DataDirectory = directory });
var currentHour = HeatRollingStore.UnixHour(DateTime.UtcNow);
var targetHour = currentHour - 1;

try
{
    var groups = Enumerable.Range(0, hashCount)
        .Select(hashIndex => new ChhtHashGroup(
            Hash(hashIndex),
            Enumerable.Range(0, actorsPerHash)
                .Select(actorIndex => (long)hashIndex * actorsPerHash + actorIndex + 1)
                .ToArray()))
        .ToArray();
    var batch = BatchAt(targetHour, groups, 1);

    var timer = Stopwatch.StartNew();
    await store.ApplyAsync([batch], CancellationToken.None);
    timer.Stop();
    var applyMilliseconds = timer.Elapsed.TotalMilliseconds;

    timer.Restart();
    await store.ApplyAsync([batch], CancellationToken.None);
    timer.Stop();
    var replayApplyMilliseconds = timer.Elapsed.TotalMilliseconds;

    Collect();
    var initial = await ProjectAllPagesAsync(
        store, targetHour, projectionPageSize, actorsPerHash, CancellationToken.None);
    if (initial.ChangeCount != hashCount)
        throw new InvalidDataException("benchmark correctness gate failed");

    var dirtyGroups = Enumerable.Range(0, hashCount)
        .Where(index => index % 10 == 0)
        .Select(index => new ChhtHashGroup(
            Hash(index),
            [(long)hashCount * actorsPerHash + index + 1]))
        .ToArray();
    timer.Restart();
    await store.ApplyAsync([BatchAt(currentHour, dirtyGroups, 2)], CancellationToken.None);
    timer.Stop();
    var dirtyApplyMilliseconds = timer.Elapsed.TotalMilliseconds;

    var incremental = await ProjectAllPagesAsync(
        store, targetHour, projectionPageSize, actorsPerHash, CancellationToken.None);
    var dirtyProjectionMilliseconds = incremental.ElapsedMilliseconds;
    if (incremental.ChangeCount != dirtyGroups.Length)
        throw new InvalidDataException("current-hour exclusion gate failed");

    const int smallReplayRecords = 32;
    var smallGroups = Enumerable.Range(0, smallReplayRecords)
        .Select(index => new ChhtHashGroup(
            Hash(int.MaxValue - index),
            [unchecked(long.MinValue + index)]))
        .ToArray();
    var smallBatch = BatchAt(targetHour, smallGroups, 3);
    ChhtBatch[] smallBatches = [smallBatch];
    await store.ApplyAsync(smallBatches, CancellationToken.None);
    for (var iteration = 0; iteration < 10; iteration++)
        await store.ApplyAsync(smallBatches, CancellationToken.None);

    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var gen2Before = GC.CollectionCount(2);
    timer.Restart();
    for (var iteration = 0; iteration < smallReplayIterations; iteration++)
        await store.ApplyAsync(smallBatches, CancellationToken.None);
    timer.Stop();
    var smallReplayAllocatedBytes =
        GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    var smallReplayGen2Collections = GC.CollectionCount(2) - gen2Before;
    var smallReplaySeconds = timer.Elapsed.TotalSeconds;

    var bytes = Directory.EnumerateFiles(directory, "heat-rolling-24h.sqlite3*")
        .Sum(path => new FileInfo(path).Length);
    var actorPairs = checked(hashCount * actorsPerHash);
    var applyActorPairsPerSecond = actorPairs / (applyMilliseconds / 1000);
    var largeApplyMinimumPairsPerSecond = actorPairs < 100_000
        ? 0
        : actorsPerHash == 1 ? 100_000 : 120_000;
    const double smallReplayMaximumAllocatedBytesPerBatch = 40_000;
    var smallReplayAllocatedBytesPerBatch =
        (double)smallReplayAllocatedBytes / smallReplayIterations;
    var largeApplyGatePassed =
        applyActorPairsPerSecond >= largeApplyMinimumPairsPerSecond;
    var smallReplayAllocationGatePassed =
        smallReplayAllocatedBytesPerBatch <= smallReplayMaximumAllocatedBytesPerBatch &&
        smallReplayGen2Collections == 0;
    const long pagedProjectionMaximumManagedGrowthBytes = 32L * 1024 * 1024;
    const double pagedProjectionMaximumPageCommitMilliseconds = 250;
    var pagedProjectionMemoryGatePassed =
        initial.PeakManagedGrowthBytes <= pagedProjectionMaximumManagedGrowthBytes;
    var pagedProjectionCommitGatePassed =
        initial.MaxPageCommitMilliseconds <= pagedProjectionMaximumPageCommitMilliseconds;
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        hashCount,
        actorPairs,
        projectionPageSize,
        applyMilliseconds,
        applyActorPairsPerSecond,
        largeApplyMinimumPairsPerSecond,
        largeApplyGatePassed,
        replayApplyMilliseconds,
        replayActorPairsPerSecond = hashCount * actorsPerHash / (replayApplyMilliseconds / 1000),
        pagedProjectionMilliseconds = initial.ElapsedMilliseconds,
        pagedProjectionAllocatedBytes = initial.AllocatedBytes,
        pagedProjectionPeakManagedGrowthBytes = initial.PeakManagedGrowthBytes,
        pagedProjectionMaximumManagedGrowthBytes,
        pagedProjectionMemoryGatePassed,
        initial.MaxPageCommitMilliseconds,
        pagedProjectionMaximumPageCommitMilliseconds,
        pagedProjectionCommitGatePassed,
        dirtyHashes = dirtyGroups.Length,
        dirtyApplyMilliseconds,
        dirtyProjectionMilliseconds,
        smallReplayRecords,
        smallReplayIterations,
        smallReplayAllocatedBytesPerBatch,
        smallReplayMaximumAllocatedBytesPerBatch,
        smallReplayGen2Collections,
        smallReplayAllocationGatePassed,
        smallReplayBatchesPerSecond = smallReplayIterations / smallReplaySeconds,
        smallReplayActorPairsPerSecond =
            smallReplayRecords * smallReplayIterations / smallReplaySeconds,
        rollingBytes = bytes,
        bytesPerActorPair = (double)bytes / actorPairs
    }, new JsonSerializerOptions { WriteIndented = true }));
    if (!largeApplyGatePassed)
        throw new InvalidDataException("large rolling apply throughput gate failed");
    if (!smallReplayAllocationGatePassed)
        throw new InvalidDataException("small rolling replay allocation/Gen2 gate failed");
    if (!pagedProjectionMemoryGatePassed)
        throw new InvalidDataException("paged rolling projection memory gate failed");
    if (!pagedProjectionCommitGatePassed)
        throw new InvalidDataException("paged rolling projection commit-latency gate failed");
}
finally
{
    if (Directory.Exists(directory)) Directory.Delete(directory, true);
}

static async Task<(
    int ChangeCount,
    double ElapsedMilliseconds,
    long AllocatedBytes,
    long PeakManagedGrowthBytes,
    double MaxPageCommitMilliseconds)> ProjectAllPagesAsync(
    HeatRollingStore store,
    long targetHour,
    int pageSize,
    long expectedCount,
    CancellationToken cancellationToken)
{
    await store.PrepareForProjectionAsync(targetHour, cancellationToken);
    var baselineManaged = GC.GetTotalMemory(forceFullCollection: false);
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var peakManaged = baselineManaged;
    var maxPageCommitMilliseconds = 0d;
    var changeCount = 0;
    long cursor = 0;
    var timer = Stopwatch.StartNew();
    while (true)
    {
        var page = await store.ReadChangesPageAsync(
            targetHour, cursor, pageSize, cancellationToken);
        peakManaged = Math.Max(peakManaged, GC.GetTotalMemory(forceFullCollection: false));
        if (page.Changes.Count > pageSize ||
            page.Changes.Any(change => change.CurrentCount != expectedCount))
            throw new InvalidDataException("paged projection correctness gate failed");
        changeCount = checked(changeCount + page.Changes.Count);
        var commitTimer = Stopwatch.StartNew();
        await store.CommitProjectionPageAsync(
            targetHour,
            page.Changes.Select(change =>
                (change.InfoHash, change.CurrentCount, change.Revision)).ToArray(),
            [],
            cancellationToken);
        commitTimer.Stop();
        maxPageCommitMilliseconds = Math.Max(
            maxPageCommitMilliseconds, commitTimer.Elapsed.TotalMilliseconds);
        peakManaged = Math.Max(peakManaged, GC.GetTotalMemory(forceFullCollection: false));
        if (!page.HasMore)
        {
            await store.FinalizeProjectionAsync(targetHour, cancellationToken);
            break;
        }
        cursor = page.NextHashId;
    }
    timer.Stop();
    return (
        changeCount,
        timer.Elapsed.TotalMilliseconds,
        GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore,
        Math.Max(0, peakManaged - baselineManaged),
        maxPageCommitMilliseconds);
}

static void Collect()
{
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
}

static byte[] Hash(int value)
{
    var hash = new byte[20];
    BinaryPrimitives.WriteInt32BigEndian(hash.AsSpan(16), value);
    return hash;
}

static ChhtBatch BatchAt(long unixHour, IReadOnlyList<ChhtHashGroup> groups, ulong sequence)
{
    var instant = DateTimeOffset.FromUnixTimeSeconds(unixHour * 3600);
    return new ChhtBatch(
        "benchmark",
        DateOnly.FromDateTime(instant.UtcDateTime),
        (byte)instant.Hour,
        1,
        sequence,
        sequence,
        groups,
        SHA256.HashData(BitConverter.GetBytes(sequence)));
}
