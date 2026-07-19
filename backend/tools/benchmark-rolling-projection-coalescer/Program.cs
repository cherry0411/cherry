using System.Diagnostics;
using System.Text.Json;
using Cherry.Infrastructure.Heat;

var documentCount = args.Length > 0 ? int.Parse(args[0]) : 1_000_000;
var documentsPerPoll = args.Length > 1 ? int.Parse(args[1]) : 50;
const int batchSize = 500;
if (documentCount <= 0 || documentsPerPoll <= 0)
    throw new ArgumentOutOfRangeException(nameof(args));

var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
var coalescer = new HeatRollingProjectionCoalescer(
    batchSize, TimeSpan.FromSeconds(45), time);
coalescer.BeginWindow(123, 0);
var remoteTasks = 0;
var submittedDocuments = 0;
var peakPending = 0;
var stopwatch = Stopwatch.StartNew();
var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);

for (var offset = 0; offset < documentCount; offset += documentsPerPoll)
{
    var count = Math.Min(documentsPerPoll, documentCount - offset);
    for (var index = 0; index < count; index++)
    {
        var id = offset + index + 1L;
        var hash = new byte[20];
        BitConverter.TryWriteBytes(hash, id);
        coalescer.Upsert(new RollingProjectionPendingChange(id, hash, 1, 0, 1));
        peakPending = Math.Max(peakPending, coalescer.Count);
        if (coalescer.IsFlushDue)
            await coalescer.FlushAsync(false, Submit, CancellationToken.None);
    }
    time.Advance(TimeSpan.FromSeconds(1));
    await coalescer.FlushAsync(false, Submit, CancellationToken.None);
}
await coalescer.FlushAsync(true, Submit, CancellationToken.None);
stopwatch.Stop();

var legacyTasks = (documentCount + documentsPerPoll - 1) / documentsPerPoll;
Console.WriteLine(JsonSerializer.Serialize(new
{
    documentCount,
    documentsPerPoll,
    batchSize,
    legacyTasks,
    coalescedTasks = remoteTasks,
    taskReductionPercent = 100d * (legacyTasks - remoteTasks) / legacyTasks,
    submittedDocuments,
    peakPending,
    elapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
    operationsPerSecond = documentCount / stopwatch.Elapsed.TotalSeconds,
    allocatedBytesPerDocument =
        (double)(GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore) / documentCount
}, new JsonSerializerOptions { WriteIndented = true }));

if (submittedDocuments != documentCount || coalescer.Count != 0 || peakPending > batchSize)
    throw new InvalidDataException("rolling projection coalescing correctness gate failed");

Task<bool> Submit(
    long targetHour,
    IReadOnlyList<RollingProjectionPendingChange> changes,
    CancellationToken cancellationToken)
{
    remoteTasks++;
    submittedDocuments += changes.Count;
    return Task.FromResult(true);
}

sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => utcNow;

    public void Advance(TimeSpan value) => utcNow += value;
}
