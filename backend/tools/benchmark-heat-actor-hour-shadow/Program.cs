using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Cherry.Infrastructure.Heat;

var hashCount = args.Length > 0 ? int.Parse(args[0]) : 100_000;
var actorsPerHash = args.Length > 1 ? int.Parse(args[1]) : 4;
if (hashCount <= 0 || actorsPerHash <= 0)
    throw new ArgumentOutOfRangeException(nameof(args));
var actorPairs = checked(hashCount * actorsPerHash);
var now = DateTimeOffset.UtcNow;
var groups = Enumerable.Range(0, hashCount)
    .Select(hashIndex => new ChhtHashGroup(
        Hash(hashIndex),
        Enumerable.Range(0, actorsPerHash)
            .Select(actorIndex => (long)hashIndex * actorsPerHash + actorIndex + 1)
            .ToArray()))
    .ToArray();
var batch = new ChhtBatch(
    "benchmark",
    DateOnly.FromDateTime(now.UtcDateTime),
    (byte)now.UtcDateTime.Hour,
    1,
    1,
    checked((ulong)actorPairs),
    groups,
    SHA256.HashData("actor-hour-benchmark"u8));

// Warm JIT and the complete blocked-Bloom/count-table path outside the sample.
var warmup = NewStore(Math.Min(hashCount, 10_000), Math.Min(actorPairs, 40_000));
warmup.Observe([batch with { Groups = groups.Take(Math.Min(hashCount, 10_000)).ToArray() }]);

var store = NewStore(hashCount, actorPairs);
GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
GC.WaitForPendingFinalizers();
GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
var timer = Stopwatch.StartNew();
store.Observe([batch]);
timer.Stop();
var uniqueMilliseconds = timer.Elapsed.TotalMilliseconds;
var uniqueAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
timer.Restart();
store.Observe([batch]);
timer.Stop();
var replayMilliseconds = timer.Elapsed.TotalMilliseconds;
var replayAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
var snapshot = store.Snapshot();
var current = snapshot.Current ?? throw new InvalidDataException("missing current actor-hour slot");
var frameAllocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
var frameManagedBefore = GC.GetTotalMemory(forceFullCollection: false);
if (!store.TryCreateFrameSnapshot(HeatActorHourShadowStore.UnixHour(now), out var frame) || frame is null)
    throw new InvalidDataException("actor-hour frame snapshot failed");
timer.Restart();
var encoded = HeatActorHourFrameCodec.Encode(frame);
timer.Stop();
var frameEncodeMilliseconds = timer.Elapsed.TotalMilliseconds;
var frameExportAllocatedBytes =
    GC.GetTotalAllocatedBytes(precise: true) - frameAllocatedBefore;
var frameExportManagedGrowthBytes = Math.Max(
    0, GC.GetTotalMemory(forceFullCollection: false) - frameManagedBefore);
var decoded = HeatActorHourFrameCodec.Decode(encoded);

// Measure the operation that remains after durable ACK completion. The
// consumer is paused so both successful and saturated admission are isolated
// from background processing.
const int admissionSamples = 100_000;
var admissionOptions = new HeatOptions
{
    Enabled = true,
    ActorHourShadowEnabled = true,
    ActorHourShadowPairCapacity = 1_000,
    ActorHourShadowHashCapacity = 1_000,
    ActorHourShadowFalsePositivePpm = 1,
    ActorHourShadowQueueCapacity = admissionSamples,
    ActorHourShadowQueueRecordCapacity = admissionSamples + 1
};
var admissionProcessor = new BlockingBenchmarkProcessor();
var admissionBatch = new ChhtBatch(
    "admission-benchmark",
    DateOnly.FromDateTime(now.UtcDateTime),
    (byte)now.UtcDateTime.Hour,
    1,
    1,
    1,
    [new ChhtHashGroup(Hash(-1), [1L])],
    SHA256.HashData("actor-hour-admission-benchmark"u8));
var admissionObserver = new HeatActorHourShadowObserver(admissionOptions, admissionProcessor);
var admissionBatches = new[] { admissionBatch };
for (var index = 0; index < admissionSamples; index++)
    if (!admissionObserver.TryEnqueue(admissionBatches))
        throw new InvalidDataException("actor-hour admission warmup saturated unexpectedly");
await admissionObserver.StartAsync(CancellationToken.None);
await WaitUntilAsync(() => admissionObserver.Snapshot().QueuedRecords == 0);
admissionProcessor.BlockNext();
if (!admissionObserver.TryEnqueue(admissionBatches) ||
    !admissionProcessor.Started.Wait(TimeSpan.FromSeconds(10)))
    throw new InvalidDataException("actor-hour admission benchmark failed to pause its consumer");
GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
GC.WaitForPendingFinalizers();
GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
timer.Restart();
var admissionAccepted = 0;
for (var index = 0; index < admissionSamples; index++)
    if (admissionObserver.TryEnqueue(admissionBatches)) admissionAccepted++;
timer.Stop();
var admissionMilliseconds = timer.Elapsed.TotalMilliseconds;
var admissionAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
timer.Restart();
var saturatedAccepted = 0;
for (var index = 0; index < admissionSamples; index++)
    if (admissionObserver.TryEnqueue(admissionBatches))
        saturatedAccepted++;
timer.Stop();
var saturatedMilliseconds = timer.Elapsed.TotalMilliseconds;
var saturatedAllocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
admissionProcessor.Release.Set();
await WaitUntilAsync(() => admissionObserver.Snapshot().QueuedRecords == 0);
var admissionSnapshot = admissionObserver.Snapshot();
await admissionObserver.StopAsync(CancellationToken.None);

var probableFirstPassFalsePositives = actorPairs - current.NewPairs;
var correctnessGatePassed =
    current.Bypassed == 0 &&
    current.ActiveHashes == hashCount &&
    decoded.Entries.Count == hashCount &&
    decoded.Entries.Aggregate(0L, (sum, entry) => sum + entry.Count) == current.NewPairs &&
    admissionAccepted == admissionSamples &&
    admissionSnapshot.QueuedRecords == 0 &&
    admissionSnapshot.ProcessedRecords == admissionSamples * 2L + 1 &&
    saturatedAccepted == 0 &&
    admissionSnapshot.DroppedRecords == admissionSamples &&
    admissionSnapshot.RecordCapacityRecords == admissionSamples &&
    admissionSnapshot.PendingForwardRecords == 0 &&
    probableFirstPassFalsePositives <= Math.Max(10, actorPairs / 10_000);
var uniquePairsPerSecond = actorPairs / (uniqueMilliseconds / 1_000);
const double minimumUniquePairsPerSecond = 100_000;
var throughputGatePassed = uniquePairsPerSecond >= minimumUniquePairsPerSecond;

Console.WriteLine(JsonSerializer.Serialize(new
{
    hashCount,
    actorsPerHash,
    actorPairs,
    uniqueMilliseconds,
    uniquePairsPerSecond,
    minimumUniquePairsPerSecond,
    throughputGatePassed,
    replayMilliseconds,
    replayPairsPerSecond = actorPairs / (replayMilliseconds / 1_000),
    uniqueAllocatedBytes,
    uniqueAllocatedBytesPerRecord = (double)uniqueAllocatedBytes / actorPairs,
    replayAllocatedBytes,
    replayAllocatedBytesPerRecord = (double)replayAllocatedBytes / actorPairs,
    current.NewPairs,
    current.ProbableDuplicates,
    current.Bypassed,
    current.BitsSet,
    current.BitCount,
    current.BitFillPpm,
    probableFirstPassFalsePositives,
    current.ActiveHashes,
    snapshot.EstimatedHotPathBytes,
    snapshot.FrameExportMemoryExcluded,
    configuredBytesPerPairCapacity = (double)snapshot.EstimatedHotPathBytes / actorPairs,
    frameBytes = encoded.Length,
    frameBytesPerActiveHash = (double)encoded.Length / hashCount,
    frameEncodeMilliseconds,
    frameExportAllocatedBytes,
    frameExportAllocatedBytesPerActiveHash = (double)frameExportAllocatedBytes / hashCount,
    frameExportManagedGrowthBytes,
    admissionSamples,
    admissionMilliseconds,
    admissionPerSecond = admissionSamples / (admissionMilliseconds / 1_000),
    admissionNanosecondsEach = admissionMilliseconds * 1_000_000 / admissionSamples,
    admissionAllocatedBytes,
    admissionAllocatedBytesEach = (double)admissionAllocatedBytes / admissionSamples,
    saturatedSamples = admissionSamples,
    saturatedMilliseconds,
    saturatedPerSecond = admissionSamples / (saturatedMilliseconds / 1_000),
    saturatedNanosecondsEach = saturatedMilliseconds * 1_000_000 / admissionSamples,
    saturatedAllocatedBytes,
    saturatedAllocatedBytesEach = (double)saturatedAllocatedBytes / admissionSamples,
    correctnessGatePassed
}, new JsonSerializerOptions { WriteIndented = true }));

if (!correctnessGatePassed)
    throw new InvalidDataException("actor-hour shadow correctness gate failed");
if (!throughputGatePassed)
    throw new InvalidDataException("actor-hour shadow throughput gate failed");

HeatActorHourShadowStore NewStore(int activeHashes, int pairs) => new(new HeatOptions
{
    Enabled = true,
    ActorHourShadowEnabled = true,
    ActorHourShadowPairCapacity = Math.Max(1_000, pairs),
    ActorHourShadowHashCapacity = Math.Max(1_000, activeHashes),
    ActorHourShadowFalsePositivePpm = 1
});

static byte[] Hash(int value)
{
    var hash = new byte[20];
    BinaryPrimitives.WriteInt32BigEndian(hash, value);
    BinaryPrimitives.WriteInt32BigEndian(hash.AsSpan(16), value ^ int.MinValue);
    return hash;
}

static async Task WaitUntilAsync(Func<bool> condition)
{
    var deadline = DateTime.UtcNow.AddSeconds(10);
    while (DateTime.UtcNow < deadline)
    {
        if (condition()) return;
        await Task.Delay(1);
    }
    throw new TimeoutException("benchmark observer did not drain before the deadline");
}

sealed class BlockingBenchmarkProcessor : IHeatActorHourBatchObserver
{
    private int _blockNext;
    public ManualResetEventSlim Started { get; } = new(false);
    public ManualResetEventSlim Release { get; } = new(false);

    public void BlockNext()
    {
        Started.Reset();
        Release.Reset();
        Volatile.Write(ref _blockNext, 1);
    }

    public void Observe(IReadOnlyList<ChhtBatch> batches)
    {
        if (Interlocked.Exchange(ref _blockNext, 0) == 0) return;
        Started.Set();
        if (!Release.Wait(TimeSpan.FromSeconds(20)))
            throw new TimeoutException("benchmark consumer was not released");
    }

    public bool TryRecordExternalLossTotal(
        long unixHour,
        long cumulativeRecords,
        HeatActorHourPartialReason reason) => true;
}
