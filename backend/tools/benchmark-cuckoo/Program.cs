using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using Cherry.Infrastructure.Dedup;

var entries = args.Length > 0 ? int.Parse(args[0]) : 500_000;
var parallelism = args.Length > 1 ? int.Parse(args[1]) : Math.Min(2, Environment.ProcessorCount);
var targetLoad = args.Length > 2 ? double.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture) : 0.5;
if (entries <= 0 || parallelism <= 0 || targetLoad is <= 0 or > 0.95)
    throw new ArgumentOutOfRangeException(
        nameof(args),
        "entries and parallelism must be positive; targetLoad must be in (0, 0.95]");

var hashes = new string[entries];
Parallel.For(0, entries, new ParallelOptions { MaxDegreeOfParallelism = parallelism }, value =>
{
    Span<byte> input = stackalloc byte[8];
    BitConverter.TryWriteBytes(input, value);
    BitConverter.TryWriteBytes(input[4..], 0x43485259);
    hashes[value] = Convert.ToHexString(SHA1.HashData(input)).ToLowerInvariant();
});

using var filter = new CuckooFilter(capacity: (long)Math.Ceiling(entries / targetLoad));

var representedByCollision = 0;
var unrepresentedInsertFailures = 0;
var add = Measure(() =>
{
    foreach (var hash in hashes)
    {
        if (filter.Add(hash))
            continue;
        if (filter.MightContain(hash))
            representedByCollision++;
        else
            unrepresentedInsertFailures++;
    }
});

var falseNegatives = 0;
var lookup = Measure(() => Parallel.For(
    0,
    hashes.Length,
    new ParallelOptions { MaxDegreeOfParallelism = parallelism },
    index =>
    {
        if (!filter.MightContain(hashes[index]))
            Interlocked.Increment(ref falseNegatives);
    }));

var duplicateWinners = 0;
var duplicateAdd = Measure(() => Parallel.For(
    0,
    hashes.Length,
    new ParallelOptions { MaxDegreeOfParallelism = parallelism },
    index =>
    {
        if (filter.Add(hashes[index]))
            Interlocked.Increment(ref duplicateWinners);
    }));

var result = new ConcurrentDictionary<string, object?>
{
    ["entries"] = entries,
    ["parallelism"] = parallelism,
    ["target_load"] = targetLoad,
    ["count"] = filter.Count,
    ["false_negatives"] = falseNegatives,
    ["represented_by_fingerprint_collision"] = representedByCollision,
    ["unrepresented_insert_failures"] = unrepresentedInsertFailures,
    ["duplicate_winners"] = duplicateWinners,
    ["sequential_add_ops_per_second"] = Rate(entries, add),
    ["parallel_lookup_ops_per_second"] = Rate(entries, lookup),
    ["parallel_duplicate_add_ops_per_second"] = Rate(entries, duplicateAdd),
    ["sequential_add_elapsed_ms"] = add.TotalMilliseconds,
    ["parallel_lookup_elapsed_ms"] = lookup.TotalMilliseconds,
    ["parallel_duplicate_add_elapsed_ms"] = duplicateAdd.TotalMilliseconds
};

Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(
    result,
    new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));

if (falseNegatives != 0 || duplicateWinners != 0 || unrepresentedInsertFailures != 0)
    Environment.ExitCode = 1;

static TimeSpan Measure(Action action)
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    var stopwatch = Stopwatch.StartNew();
    action();
    stopwatch.Stop();
    return stopwatch.Elapsed;
}

static long Rate(int operations, TimeSpan elapsed) =>
    (long)Math.Round(operations / elapsed.TotalSeconds);
