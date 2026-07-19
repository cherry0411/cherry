using System.Collections.Concurrent;
using System.Security.Cryptography;
using Cherry.Infrastructure.Heat;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatActorHourShadowObserverTests
{
    private static readonly byte[] DailySecret = Enumerable.Repeat((byte)47, 32).ToArray();

    [Fact]
    public async Task FullQueueFailsOpenAndAttributesHourlyLoss()
    {
        var processor = new BlockingProcessor();
        var options = Options(queueCapacity: 1, recordCapacity: 10);
        var observer = new HeatActorHourShadowObserver(options, processor);
        var batch = Batch(records: 1);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(observer.TryEnqueue([batch]));
            Assert.True(processor.Started.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(observer.TryEnqueue([batch]));

            Assert.False(observer.TryEnqueue([batch]));
            var saturated = observer.Snapshot();
            Assert.Equal(1, saturated.QueueFullRecords);
            Assert.Equal(1, saturated.DroppedRecords);
            Assert.Single(saturated.Losses);
            Assert.Equal(1, saturated.Losses[0].QueueFullRecords);

            processor.Release.Set();
            await EventuallyAsync(() =>
                observer.Snapshot().ProcessedRecords == 2 &&
                processor.ExternalLossRecords == 1);
        }
        finally
        {
            processor.Release.Set();
            await observer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConcurrentAdmissionNeverExceedsRecordBudget()
    {
        var processor = new BlockingProcessor();
        var options = Options(queueCapacity: 64, recordCapacity: 10);
        var observer = new HeatActorHourShadowObserver(options, processor);
        var batch = Batch(records: 1);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(observer.TryEnqueue([batch]));
            Assert.True(processor.Started.Wait(TimeSpan.FromSeconds(5)));

            var admissions = await Task.WhenAll(Enumerable.Range(0, 50)
                .Select(_ => Task.Run(() => observer.TryEnqueue([batch]))));
            var snapshot = observer.Snapshot();

            Assert.Equal(9, admissions.Count(value => value));
            Assert.Equal(10, snapshot.QueuedRecords);
            Assert.Equal(10, snapshot.EnqueuedRecords);
            Assert.Equal(41, snapshot.RecordCapacityRecords);
            Assert.Equal(41, snapshot.DroppedRecords);

            processor.Release.Set();
            await EventuallyAsync(() => observer.Snapshot().ProcessedRecords == 10);
        }
        finally
        {
            processor.Release.Set();
            await observer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConcurrentFirstLossInitializationDoesNotEraseOrCrossAttribute()
    {
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 1, recordCapacity: 10),
            new RecordingProcessor());
        var batch = Batch(records: 11);

        var admissions = await Task.WhenAll(Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => observer.TryEnqueue([batch]))));

        Assert.All(admissions, accepted => Assert.False(accepted));
        var snapshot = observer.Snapshot();
        Assert.Equal(1_100, snapshot.RecordCapacityRecords);
        Assert.Equal(1_100, snapshot.DroppedRecords);
        var loss = Assert.Single(snapshot.Losses);
        Assert.Equal(1_100, loss.RecordCapacityRecords);
        Assert.Equal(0, loss.ForwardedRecords);
        Assert.Equal(1_100, loss.PendingForwardRecords);
    }

    [Fact]
    public async Task FailedForwardIsRetriedWithIdempotentCumulativeTotal()
    {
        var processor = new FlakyForwardingProcessor(failures: 1);
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 1), processor);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            var first = Batch(records: 3);
            Assert.False(observer.TryEnqueue([first]));
            await EventuallyAsync(() =>
                processor.Calls >= 2 &&
                processor.AppliedRecords == 3 &&
                observer.Snapshot().PendingForwardRecords == 0);

            var second = Batch(records: 2);
            Assert.False(observer.TryEnqueue([second]));
            await EventuallyAsync(() =>
                processor.AppliedRecords == 5 &&
                observer.Snapshot().PendingForwardRecords == 0);

            Assert.All(processor.AcceptedTotals, total => Assert.True(total is 3 or 5));
            Assert.Equal(5, processor.AppliedRecords);
        }
        finally
        {
            await observer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task EmptyChannelOversizedLossPropagatesWithoutLaterTraffic()
    {
        var options = Options(queueCapacity: 2, recordCapacity: 1);
        var store = new HeatActorHourShadowStore(options);
        var observer = new HeatActorHourShadowObserver(options, store);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            var batch = Batch(records: 3);
            Assert.False(observer.TryEnqueue([batch]));

            await EventuallyAsync(() =>
                store.Snapshot().Current!.DroppedBeforeObserve == 3 &&
                observer.Snapshot().PendingForwardRecords == 0);
            var current = store.Snapshot().Current!;
            Assert.Equal(3, current.Bypassed);
            Assert.True(current.PartialReasons.HasFlag(
                HeatActorHourPartialReason.ObserverQueueLoss));
        }
        finally
        {
            await observer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void RepeatedOversizedLossesCannotConsumeObservationQueueSlots()
    {
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 1),
            new RecordingProcessor());
        var oversized = Batch(records: 2);
        for (var index = 0; index < 100; index++)
            Assert.False(observer.TryEnqueue([oversized]));

        var valid = Batch(records: 1);
        Assert.True(observer.TryEnqueue([valid]));
        var snapshot = observer.Snapshot();
        Assert.Equal(1, snapshot.EnqueuedRecords);
        Assert.Equal(200, snapshot.RecordCapacityRecords);
        Assert.Equal(0, snapshot.QueueFullRecords);
    }

    [Fact]
    public void WarmedSuccessfulAdmissionDerivesCheckedTotalWithoutAllocation()
    {
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 16_384, recordCapacity: 750_000),
            new RecordingProcessor());
        var batch = Batch(records: 1);
        var batches = Enumerable.Repeat(batch, 64).ToArray();

        for (var index = 0; index < 10_000; index++)
            Assert.True(observer.TryEnqueue(batches));

        var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        var accepted = 0;
        for (var index = 0; index < 1_000; index++)
            if (observer.TryEnqueue(batches)) accepted++;
        var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

        Assert.Equal(1_000, accepted);
        Assert.Equal(0, allocated);
        var snapshot = observer.Snapshot();
        Assert.Equal(704_000, snapshot.EnqueuedRecords);
        Assert.Equal(704_000, snapshot.QueuedRecords);
    }

    [Fact]
    public void AdmissionRejectsCheckedTotalOverflowWithoutEscaping()
    {
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 10),
            new RecordingProcessor());
        var huge = Batch(records: 1) with
        {
            Groups =
            [
                new ChhtHashGroup(
                    new byte[20],
                    new CountOnlyActors(int.MaxValue))
            ]
        };

        Assert.False(observer.TryEnqueue([huge, huge]));
        var snapshot = observer.Snapshot();
        Assert.Equal(0, snapshot.EnqueuedRecords);
        Assert.Equal(0, snapshot.DroppedRecords);
        Assert.StartsWith("OverflowException:", snapshot.LastFailure);
    }

    [Fact]
    public async Task ProcessorExceptionIsContainedAndLossIsForwarded()
    {
        var processor = new ThrowingProcessor();
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 10), processor);
        var batch = Batch(records: 2);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(observer.TryEnqueue([batch]));
            await EventuallyAsync(() =>
                observer.Snapshot().ProcessingFailureRecords == 2 &&
                processor.ExternalLossRecords == 2);

            var snapshot = observer.Snapshot();
            Assert.Equal(2, snapshot.DroppedRecords);
            Assert.Equal(0, snapshot.ProcessedRecords);
            Assert.NotNull(snapshot.LastFailureAt);
            Assert.StartsWith("InvalidOperationException:", snapshot.LastFailure);
        }
        finally
        {
            await observer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task OrdinaryExecuteFailureIsContainedAndQueuedWorkIsDrained()
    {
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 10),
            new RecordingProcessor(),
            new ThrowingTimerTimeProvider());
        var batch = Batch(records: 2);
        Assert.True(observer.TryEnqueue([batch]));

        var exception = await Record.ExceptionAsync(() =>
            observer.StartAsync(CancellationToken.None));

        Assert.Null(exception);
        await observer.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(observer.ExecuteTask.IsCompletedSuccessfully);
        var snapshot = observer.Snapshot();
        Assert.Equal(0, snapshot.QueuedRecords);
        Assert.Equal(2, snapshot.StoppedRecords);
        Assert.Equal(2, snapshot.DroppedRecords);
        Assert.StartsWith("InvalidOperationException: synthetic timer failure", snapshot.LastFailure);
        Assert.False(observer.TryEnqueue([batch]));
        await observer.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StoppedObserverRejectsWithoutThrowingAndAttributesLoss()
    {
        var processor = new RecordingProcessor();
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 2, recordCapacity: 10), processor);
        await observer.StartAsync(CancellationToken.None);
        await observer.StopAsync(CancellationToken.None);

        var exception = Record.Exception(() =>
            Assert.False(observer.TryEnqueue([Batch(records: 3)])));

        Assert.Null(exception);
        var snapshot = observer.Snapshot();
        Assert.Equal(3, snapshot.StoppedRecords);
        Assert.Equal(3, snapshot.DroppedRecords);
    }

    [Fact]
    public async Task StopDrainsReservationsAndAccountsQueuedAndConcurrentRejects()
    {
        var processor = new BlockingProcessor();
        var observer = new HeatActorHourShadowObserver(
            Options(queueCapacity: 1, recordCapacity: 10), processor);
        var batch = Batch(records: 1);
        await observer.StartAsync(CancellationToken.None);
        Assert.True(observer.TryEnqueue([batch]));
        Assert.True(processor.Started.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(observer.TryEnqueue([batch]));

        var stopping = observer.StopAsync(CancellationToken.None);
        Assert.False(observer.TryEnqueue([batch]));
        processor.Release.Set();
        await stopping.WaitAsync(TimeSpan.FromSeconds(5));

        var snapshot = observer.Snapshot();
        Assert.Equal(0, snapshot.QueuedRecords);
        Assert.Equal(2, snapshot.EnqueuedRecords);
        Assert.Equal(2, snapshot.StoppedRecords);
        Assert.Equal(0, snapshot.PendingForwardRecords);
        Assert.Equal(2, processor.ExternalLossRecords);
    }

    [Fact]
    public async Task DurableAckIsNotBlockedByObserverProcessing()
    {
        var directory = Path.Combine(
            Path.GetTempPath(), $"cherry-actor-hour-observer-{Guid.NewGuid():N}");
        var processor = new BlockingProcessor();
        var options = Options(queueCapacity: 2, recordCapacity: 10, directory);
        var observer = new HeatActorHourShadowObserver(options, processor);
        var service = new HeatAccumulatorService(
            options,
            new HeatRuntimeMetrics(),
            NullLogger<HeatAccumulatorService>.Instance,
            actorHourObserver: observer);
        await observer.StartAsync(CancellationToken.None);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var submission = service.SubmitAsync(Batch(records: 1), CancellationToken.None);
            Assert.True(await Task.Run(() =>
                processor.Started.Wait(TimeSpan.FromSeconds(5))));

            var result = await submission.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(HeatAcceptStatus.Accepted, result.Status);
            Assert.False(processor.Release.IsSet);
        }
        finally
        {
            processor.Release.Set();
            await service.StopAsync(CancellationToken.None);
            await observer.StopAsync(CancellationToken.None);
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task SaturatedObserverDoesNotDelayBatchedAccumulatorAcks()
    {
        var directory = Path.Combine(
            Path.GetTempPath(), $"cherry-actor-hour-batched-{Guid.NewGuid():N}");
        var processor = new BlockingProcessor();
        var options = Options(
            queueCapacity: 1,
            recordCapacity: 100,
            directory,
            commitBatchRequests: 4);
        var observer = new HeatActorHourShadowObserver(options, processor);
        var service = new HeatAccumulatorService(
            options,
            new HeatRuntimeMetrics(),
            NullLogger<HeatAccumulatorService>.Instance,
            actorHourObserver: observer);
        await observer.StartAsync(CancellationToken.None);
        try
        {
            var prefill = Batch(records: 1);
            Assert.True(observer.TryEnqueue([prefill]));
            Assert.True(processor.Started.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(observer.TryEnqueue([prefill]));

            var batches = Enumerable.Range(0, 4)
                .Select(index => Batch(records: 1) with
                {
                    CrawlerId = $"batched-{index}",
                    PayloadSha256 = SHA256.HashData([checked((byte)(30 + index))])
                })
                .ToArray();
            var submissions = batches
                .Select(batch => service.SubmitAsync(batch, CancellationToken.None))
                .ToArray();
            await service.StartAsync(CancellationToken.None);

            var results = await Task.WhenAll(submissions).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.All(results, result => Assert.Equal(HeatAcceptStatus.Accepted, result.Status));
            Assert.False(processor.Release.IsSet);
            await EventuallyAsync(() => observer.Snapshot().QueueFullRecords == 4);
        }
        finally
        {
            processor.Release.Set();
            await service.StopAsync(CancellationToken.None);
            await observer.StopAsync(CancellationToken.None);
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public void DisabledObserverAllocatesNoChannelOrLossLedgerAndIsInert()
    {
        var options = new HeatOptions
        {
            Enabled = false,
            ActorHourShadowEnabled = true,
            ActorHourShadowPairCapacity = 50_000_000,
            ActorHourShadowHashCapacity = 2_000_000,
            ActorHourShadowQueueCapacity = 1_024,
            ActorHourShadowQueueRecordCapacity = 10_000_000
        };
        var store = new HeatActorHourShadowStore(options);
        var observer = new HeatActorHourShadowObserver(options, store);

        Assert.False(store.Enabled);
        Assert.False(observer.Enabled);
        Assert.False(observer.TryEnqueue([Batch(records: 1)]));
        var snapshot = observer.Snapshot();
        Assert.Equal(0, snapshot.QueueCapacity);
        Assert.Equal(0, snapshot.QueueRecordCapacity);
        Assert.Empty(snapshot.Losses);
        Assert.Equal(0, snapshot.DroppedRecords);

        var slots = (Array)typeof(HeatActorHourShadowStore)
            .GetField("_slots", System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!.GetValue(store)!;
        var storeLosses = (Array)typeof(HeatActorHourShadowStore)
            .GetField("_outOfWindowLosses", System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!.GetValue(store)!;
        var observerLosses = (Array)typeof(HeatActorHourShadowObserver)
            .GetField("_losses", System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!.GetValue(observer)!;
        var channel = typeof(HeatActorHourShadowObserver)
            .GetField("_channel", System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!.GetValue(observer);
        Assert.Empty(slots);
        Assert.Empty(storeLosses);
        Assert.Empty(observerLosses);
        Assert.Null(channel);
    }

    private static HeatOptions Options(
        int queueCapacity,
        int recordCapacity,
        string? directory = null,
        int commitBatchRequests = 1) => new()
    {
        Enabled = true,
        DataDirectory = directory ?? Path.GetTempPath(),
        DailyActorSecret = Convert.ToBase64String(DailySecret),
        ChannelCapacity = 8,
        CommitBatchRequests = commitBatchRequests,
        RollingMaxBytes = 1024L * 1024 * 1024,
        RollingMinFreeBytes = 0,
        ActorHourShadowEnabled = true,
        ActorHourShadowPairCapacity = 1_000,
        ActorHourShadowHashCapacity = 100,
        ActorHourShadowFalsePositivePpm = 1,
        ActorHourShadowQueueCapacity = queueCapacity,
        ActorHourShadowQueueRecordCapacity = recordCapacity
    };

    private static ChhtBatch Batch(int records)
    {
        var now = DateTimeOffset.UtcNow;
        var hash = new byte[20];
        hash[0] = 19;
        return new ChhtBatch(
            "observer-test",
            DateOnly.FromDateTime(now.UtcDateTime),
            checked((byte)now.UtcDateTime.Hour),
            1,
            1,
            checked((ulong)records),
            [new ChhtHashGroup(hash, Enumerable.Range(1, records).Select(x => (long)x).ToArray())],
            SHA256.HashData([19]));
    }

    private static async Task EventuallyAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(10);
        }
        Assert.True(condition(), "Condition was not satisfied before the timeout.");
    }

    private sealed class BlockingProcessor : IHeatActorHourBatchObserver
    {
        private readonly ConcurrentDictionary<long, long> _losses = new();
        public ManualResetEventSlim Started { get; } = new(false);
        public ManualResetEventSlim Release { get; } = new(false);
        public long ExternalLossRecords => _losses.Values.Sum();

        public void Observe(IReadOnlyList<ChhtBatch> batches)
        {
            Started.Set();
            if (!Release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Test processor was not released.");
        }

        public bool TryRecordExternalLossTotal(
            long unixHour,
            long cumulativeRecords,
            HeatActorHourPartialReason reason)
        {
            _losses.AddOrUpdate(
                unixHour, cumulativeRecords,
                (_, current) => Math.Max(current, cumulativeRecords));
            return true;
        }
    }

    private sealed class ThrowingProcessor : IHeatActorHourBatchObserver
    {
        public long ExternalLossRecords;

        public void Observe(IReadOnlyList<ChhtBatch> batches) =>
            throw new InvalidOperationException("synthetic observer failure");

        public bool TryRecordExternalLossTotal(
            long unixHour,
            long cumulativeRecords,
            HeatActorHourPartialReason reason)
        {
            Interlocked.Exchange(ref ExternalLossRecords, cumulativeRecords);
            return true;
        }
    }

    private sealed class RecordingProcessor : IHeatActorHourBatchObserver
    {
        public void Observe(IReadOnlyList<ChhtBatch> batches) { }
        public bool TryRecordExternalLossTotal(
            long unixHour,
            long cumulativeRecords,
            HeatActorHourPartialReason reason) => true;
    }

    private sealed class FlakyForwardingProcessor(int failures) : IHeatActorHourBatchObserver
    {
        private readonly ConcurrentQueue<long> _acceptedTotals = new();
        private int _calls;
        private long _appliedRecords;
        public int Calls => Volatile.Read(ref _calls);
        public long AppliedRecords => Interlocked.Read(ref _appliedRecords);
        public long[] AcceptedTotals => _acceptedTotals.ToArray();

        public void Observe(IReadOnlyList<ChhtBatch> batches) { }

        public bool TryRecordExternalLossTotal(
            long unixHour,
            long cumulativeRecords,
            HeatActorHourPartialReason reason)
        {
            var call = Interlocked.Increment(ref _calls);
            if (call <= failures) return false;
            _acceptedTotals.Enqueue(cumulativeRecords);
            while (true)
            {
                var current = Interlocked.Read(ref _appliedRecords);
                if (current >= cumulativeRecords) return true;
                if (Interlocked.CompareExchange(
                        ref _appliedRecords, cumulativeRecords, current) == current)
                    return true;
            }
        }
    }

    private sealed class CountOnlyActors(int count) : IReadOnlyList<long>
    {
        public int Count => count;
        public long this[int index] => throw new NotSupportedException();
        public IEnumerator<long> GetEnumerator() => throw new NotSupportedException();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class ThrowingTimerTimeProvider : TimeProvider
    {
        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period) =>
            throw new InvalidOperationException("synthetic timer failure");
    }
}
