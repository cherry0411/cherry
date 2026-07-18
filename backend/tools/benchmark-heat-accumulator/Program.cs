using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Cherry.Infrastructure.Heat;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

var batchCount = args.Length > 0 ? int.Parse(args[0]) : 8;
var hashesPerBatch = args.Length > 1 ? int.Parse(args[1]) : 4_096;
var actorsPerHash = args.Length > 2 ? int.Parse(args[2]) : 1;
if (batchCount <= 0 || hashesPerBatch <= 0 || actorsPerHash <= 0)
    throw new ArgumentOutOfRangeException(nameof(args));

var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-accumulator-bench-{Guid.NewGuid():N}");
var options = new HeatOptions
{
    Enabled = true,
    DataDirectory = directory,
    DailyActorSecret = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray()),
    ChannelCapacity = 64,
    CommitBatchRequests = 1,
    RollingMaxBytes = 16L * 1024 * 1024 * 1024,
    RollingMinFreeBytes = 0
};
var service = new HeatAccumulatorService(
    options, new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
var now = DateTime.UtcNow;
var day = DateOnly.FromDateTime(now);
var hour = (byte)now.Hour;
var recordsPerBatch = checked(hashesPerBatch * actorsPerHash);
var batches = Enumerable.Range(0, batchCount)
    .Select(batchIndex => CreateBatch(batchIndex, hashesPerBatch, actorsPerHash, day, hour))
    .ToArray();

try
{
    await service.StartAsync(CancellationToken.None);
    const int warmupRecords = 32;
    var warmup = new ChhtBatch(
        "benchmark-warmup", day, hour, 1, 1, warmupRecords,
        [new ChhtHashGroup(Hash(int.MaxValue), Enumerable.Range(1, warmupRecords).Select(value => (long)value).ToArray())],
        SHA256.HashData("warmup"u8));
    var warmupResult = await service.SubmitAsync(warmup, CancellationToken.None);
    if (warmupResult.Status != HeatAcceptStatus.Accepted || warmupResult.Inserted != warmupRecords)
        throw new InvalidDataException("warmup correctness gate failed");
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var gen2Before = GC.CollectionCount(2);
    var latencies = new double[batchCount];
    var timer = Stopwatch.StartNew();
    for (var index = 0; index < batches.Length; index++)
    {
        var batchTimer = Stopwatch.StartNew();
        var result = await service.SubmitAsync(batches[index], CancellationToken.None);
        batchTimer.Stop();
        latencies[index] = batchTimer.Elapsed.TotalMilliseconds;
        if (result.Status != HeatAcceptStatus.Accepted || result.Inserted != recordsPerBatch)
            throw new InvalidDataException(
                $"new-batch correctness gate failed: status={result.Status} inserted={result.Inserted}");
    }
    timer.Stop();
    var allocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    var gen2Collections = GC.CollectionCount(2) - gen2Before;

    var duplicateSequence = checked((ulong)batchCount * (ulong)recordsPerBatch + 1);
    var duplicateBatch = batches[0] with
    {
        Sequence = duplicateSequence,
        EndSequence = checked(duplicateSequence + (ulong)recordsPerBatch - 1),
        PayloadSha256 = SHA256.HashData(BitConverter.GetBytes(duplicateSequence))
    };
    var duplicateTimer = Stopwatch.StartNew();
    var duplicate = await service.SubmitAsync(duplicateBatch, CancellationToken.None);
    duplicateTimer.Stop();
    if (duplicate.Status != HeatAcceptStatus.Accepted || duplicate.Inserted != 0)
        throw new InvalidDataException("next-sequence duplicate correctness gate failed");

    var nextSequence = checked(duplicateBatch.EndSequence + 1);
    var mixed50 = CreateMixedBatch(
        batchCount, hashesPerBatch, actorsPerHash, 50, day, hour, nextSequence, 50);
    var mixed50Timer = Stopwatch.StartNew();
    var mixed50Result = await service.SubmitAsync(mixed50.Batch, CancellationToken.None);
    mixed50Timer.Stop();
    if (mixed50Result.Status != HeatAcceptStatus.Accepted ||
        mixed50Result.Inserted != mixed50.ExpectedInserted)
        throw new InvalidDataException("50%-existing mixed-pair correctness gate failed");

    nextSequence = checked(mixed50.Batch.EndSequence + 1);
    var mixed90 = CreateMixedBatch(
        batchCount, hashesPerBatch, actorsPerHash, 90, day, hour, nextSequence, 90);
    var mixed90Timer = Stopwatch.StartNew();
    var mixed90Result = await service.SubmitAsync(mixed90.Batch, CancellationToken.None);
    mixed90Timer.Stop();
    if (mixed90Result.Status != HeatAcceptStatus.Accepted ||
        mixed90Result.Inserted != mixed90.ExpectedInserted)
        throw new InvalidDataException("90%-existing mixed-pair correctness gate failed");

    var replayTimer = Stopwatch.StartNew();
    var replay = await service.SubmitAsync(batches[^1], CancellationToken.None);
    replayTimer.Stop();
    if (replay.Status != HeatAcceptStatus.Replay || replay.Inserted != recordsPerBatch)
        throw new InvalidDataException("receipt replay correctness gate failed");

    var conflict = await service.SubmitAsync(
        batches[^1] with { PayloadSha256 = Enumerable.Repeat((byte)0x5a, 32).ToArray() },
        CancellationToken.None);
    if (conflict.Status != HeatAcceptStatus.Conflict)
        throw new InvalidDataException("receipt conflict correctness gate failed");

    var expectedPairs = checked((long)batchCount * recordsPerBatch);
    var expectedStoredPairs = checked(
        expectedPairs + warmupRecords + mixed50.ExpectedInserted + mixed90.ExpectedInserted);
    await using (var connection = new SqliteConnection(
                     $"Data Source={service.PathForDay(day)};Mode=ReadOnly;Pooling=False"))
    {
        await connection.OpenAsync();
        if (await ScalarAsync(connection, "SELECT COUNT(*) FROM hashes") != (long)batchCount * hashesPerBatch + 1 ||
            await ScalarAsync(connection, "SELECT COUNT(*) FROM seen") != expectedStoredPairs ||
            await ScalarAsync(connection, "SELECT COUNT(*) FROM receipts") != batchCount + 4)
            throw new InvalidDataException("daily SQLite row-count gate failed");
    }

    Array.Sort(latencies);
    var totalSeconds = timer.Elapsed.TotalSeconds;
    var recordsPerSecond = expectedPairs / totalSeconds;
    var allocatedBytesPerRecord = (double)allocatedBytes / expectedPairs;
    var minimumRecordsPerSecond = expectedPairs >= 32_768 ? 8_000d : 0d;
    const double maximumAllocatedBytesPerRecord = 750d;
    const long maximumPeakWorkingSetBytes = 512L * 1024 * 1024;
    var throughputGatePassed = recordsPerSecond >= minimumRecordsPerSecond;
    var allocationGatePassed = allocatedBytesPerRecord <= maximumAllocatedBytesPerRecord;
    using var process = Process.GetCurrentProcess();
    process.Refresh();
    var peakWorkingSetBytes = process.PeakWorkingSet64;
    var workingSetGatePassed = peakWorkingSetBytes <= maximumPeakWorkingSetBytes;
    var storeBytes = Directory.EnumerateFiles(directory, "*.sqlite3*")
        .Sum(path => new FileInfo(path).Length);
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        path = "HeatAccumulatorService daily FULL + rolling FULL",
        scope = "directional local regression gate; not a Linux/cgroup/2C4G capacity forecast",
        hashDistribution = "uniform SHA-1; input batches are prebuilt outside timed/allocation region",
        batchCount,
        hashesPerBatch,
        actorsPerHash,
        recordsPerBatch,
        totalRecords = expectedPairs,
        elapsedSeconds = totalSeconds,
        recordsPerSecond,
        minimumRecordsPerSecond,
        throughputGatePassed,
        ackP50Milliseconds = Percentile(latencies, 0.50),
        ackP95Milliseconds = Percentile(latencies, 0.95),
        duplicateAckMilliseconds = duplicateTimer.Elapsed.TotalMilliseconds,
        mixed50Existing = new
        {
            records = mixed50.Batch.RecordCount,
            inserted = mixed50.ExpectedInserted,
            ackMilliseconds = mixed50Timer.Elapsed.TotalMilliseconds
        },
        mixed90Existing = new
        {
            records = mixed90.Batch.RecordCount,
            inserted = mixed90.ExpectedInserted,
            ackMilliseconds = mixed90Timer.Elapsed.TotalMilliseconds
        },
        replayAckMilliseconds = replayTimer.Elapsed.TotalMilliseconds,
        managedPipelineAllocatedBytesPerRecordExcludingPrebuiltInput = allocatedBytesPerRecord,
        maximumManagedPipelineAllocatedBytesPerRecord = maximumAllocatedBytesPerRecord,
        gen2Collections,
        managedAllocationGatePassed = allocationGatePassed,
        peakWorkingSetBytes,
        maximumPeakWorkingSetBytes,
        workingSetGatePassed,
        storeBytes,
        bytesPerRecord = (double)storeBytes / expectedPairs
    }, new JsonSerializerOptions { WriteIndented = true }));
    if (!throughputGatePassed)
        throw new InvalidDataException("full accumulator throughput gate failed");
    if (!allocationGatePassed)
        throw new InvalidDataException("full accumulator allocation gate failed");
    if (!workingSetGatePassed)
        throw new InvalidDataException("full accumulator working-set gate failed");
}
finally
{
    await service.StopAsync(CancellationToken.None);
    if (Directory.Exists(directory)) Directory.Delete(directory, true);
}

static ChhtBatch CreateBatch(
    int batchIndex,
    int hashesPerBatch,
    int actorsPerHash,
    DateOnly day,
    byte hour)
{
    var groups = Enumerable.Range(0, hashesPerBatch)
        .Select(hashIndex =>
        {
            var globalHash = checked(batchIndex * hashesPerBatch + hashIndex);
            return new ChhtHashGroup(
                Hash(globalHash),
                Enumerable.Range(0, actorsPerHash)
                    .Select(actorIndex => checked((long)globalHash * actorsPerHash + actorIndex + 1))
                    .ToArray());
        })
        .ToArray();
    Array.Sort(groups, static (left, right) =>
        left.InfoHash.AsSpan().SequenceCompareTo(right.InfoHash));
    var records = checked((ulong)hashesPerBatch * (ulong)actorsPerHash);
    var sequence = checked((ulong)batchIndex * records + 1);
    return new ChhtBatch(
        "benchmark", day, hour, 1, sequence, checked(sequence + records - 1), groups,
        SHA256.HashData(BitConverter.GetBytes(sequence)));
}

static (ChhtBatch Batch, int ExpectedInserted) CreateMixedBatch(
    int batchCount,
    int hashesPerBatch,
    int actorsPerHash,
    int existingPercent,
    DateOnly day,
    byte hour,
    ulong sequence,
    int phase)
{
    var totalHashes = checked(batchCount * hashesPerBatch);
    var existingRecords = hashesPerBatch * existingPercent / 100;
    var groups = new ChhtHashGroup[hashesPerBatch];
    // 7919 is odd, hence coprime with the power-of-two default corpus. The
    // fallback linear probe preserves uniqueness for arbitrary CLI sizes.
    var used = new HashSet<int>();
    for (var index = 0; index < groups.Length; index++)
    {
        var globalHash = (int)(((long)index * 7_919 + phase) % totalHashes);
        while (!used.Add(globalHash)) globalHash = (globalHash + 1) % totalHashes;
        var actor = index < existingRecords
            ? checked((long)globalHash * actorsPerHash + 1)
            : checked(long.MinValue + (long)phase * totalHashes + globalHash);
        groups[index] = new ChhtHashGroup(Hash(globalHash), [actor]);
    }
    Array.Sort(groups, static (left, right) =>
        left.InfoHash.AsSpan().SequenceCompareTo(right.InfoHash));
    var endSequence = checked(sequence + (ulong)groups.Length - 1);
    return (new ChhtBatch(
            "benchmark", day, hour, 1, sequence, endSequence, groups,
            SHA256.HashData(BitConverter.GetBytes(sequence))),
        groups.Length - existingRecords);
}

static byte[] Hash(int value)
{
    Span<byte> identity = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(identity, value);
    return SHA1.HashData(identity);
}

static async Task<long> ScalarAsync(SqliteConnection connection, string sql)
{
    await using var command = connection.CreateCommand();
    command.CommandText = sql;
    return Convert.ToInt64(await command.ExecuteScalarAsync());
}

static double Percentile(IReadOnlyList<double> sorted, double percentile) =>
    sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(sorted.Count * percentile) - 1)];
