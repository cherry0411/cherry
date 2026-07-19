using System.Threading.Channels;
using Microsoft.Extensions.Hosting;

namespace Cherry.Infrastructure.Heat;

public sealed record HeatActorHourObserverLossSnapshot(
    long UnixHour,
    long QueueFullRecords,
    long RecordCapacityRecords,
    long StoppedRecords,
    long ProcessingFailureRecords,
    long ForwardedRecords)
{
    public long TotalRecords =>
        QueueFullRecords + RecordCapacityRecords + StoppedRecords + ProcessingFailureRecords;
    public long PendingForwardRecords => Math.Max(0, TotalRecords - ForwardedRecords);
}

public sealed record HeatActorHourObserverSnapshot(
    bool Enabled,
    int QueueCapacity,
    int QueueRecordCapacity,
    long QueuedRecords,
    long EnqueuedBatches,
    long EnqueuedRecords,
    long ProcessedBatches,
    long ProcessedRecords,
    long DroppedBatches,
    long DroppedRecords,
    long QueueFullRecords,
    long RecordCapacityRecords,
    long StoppedRecords,
    long ProcessingFailureRecords,
    long PendingForwardRecords,
    DateTimeOffset? LastFailureAt,
    string? LastFailure,
    HeatActorHourObserverLossSnapshot[] Losses);

/// <summary>
/// Removes all actor-hour Bloom/count work from the durable heat ACK path.
/// Successful admission is only an atomic record-budget reservation plus
/// Channel.TryWrite. Rejection is fail-open and retained in a bounded ledger.
/// </summary>
public sealed class HeatActorHourShadowObserver : BackgroundService
{
    private const int LossSlots = 48;
    private static readonly TimeSpan LossRetryInterval = TimeSpan.FromSeconds(1);
    private readonly HeatOptions _options;
    private readonly IHeatActorHourBatchObserver _processor;
    private readonly TimeProvider _timeProvider;
    private readonly Channel<ObservationEnvelope>? _channel;
    private readonly LossCell[] _losses = [];
    private readonly bool _enabled;
    private int _accepting;
    private long _queuedRecords;
    private long _enqueuedBatches;
    private long _enqueuedRecords;
    private long _processedBatches;
    private long _processedRecords;
    private long _droppedBatches;
    private long _droppedRecords;
    private long _queueFullRecords;
    private long _recordCapacityRecords;
    private long _stoppedRecords;
    private long _processingFailureRecords;
    private long _lastFailureTicks;
    private string? _lastFailure;

    public HeatActorHourShadowObserver(
        HeatOptions options,
        IHeatActorHourBatchObserver processor,
        TimeProvider? timeProvider = null)
    {
        _options = options;
        _processor = processor;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _enabled = options.Enabled && options.ActorHourShadowEnabled;
        if (!_enabled) return;

        _losses = Enumerable.Range(0, LossSlots).Select(_ => new LossCell()).ToArray();
        _channel = Channel.CreateBounded<ObservationEnvelope>(new BoundedChannelOptions(
            Math.Max(1, options.ActorHourShadowQueueCapacity))
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        });
        _accepting = 1;
    }

    public bool Enabled => _enabled;

    public bool TryEnqueue(ChhtBatch[] batches)
    {
        if (!Enabled) return false;
        if (batches.Length == 0) return false;

        int recordCount;
        try
        {
            var total = 0;
            // The accumulator group is normalized to at most 64 batches. Derive
            // the checked total from the immutable admitted batches so the ACK
            // path does not allocate a parallel attribution array.
            for (var index = 0; index < batches.Length; index++)
                total = checked(total + batches[index].RecordCount);
            recordCount = total;
        }
        catch (Exception exception)
        {
            TryRecordFailure(exception);
            return false;
        }
        if (recordCount <= 0) return false;

        try
        {
            return TryEnqueueCore(batches, recordCount);
        }
        catch (Exception exception)
        {
            try
            {
                RecordFailure(exception);
                RecordLoss(batches, recordCount, ObserverLossKind.ProcessingFailure);
            }
            catch
            {
                // A shadow path is never permitted to escape into the authority.
            }
            return false;
        }
    }

    private bool TryEnqueueCore(
        ChhtBatch[] batches,
        int recordCount)
    {
        if (Volatile.Read(ref _accepting) == 0)
        {
            RecordLoss(batches, recordCount, ObserverLossKind.Stopped);
            FlushPendingLosses();
            return false;
        }
        if (recordCount > _options.ActorHourShadowQueueRecordCapacity ||
            !TryReserveRecords(recordCount))
        {
            RecordLoss(batches, recordCount, ObserverLossKind.RecordCapacity);
            return false;
        }

        var reservationOwned = true;
        try
        {
            var envelope = new ObservationEnvelope(batches, recordCount);
            if (!_channel!.Writer.TryWrite(envelope))
            {
                var kind = Volatile.Read(ref _accepting) == 0
                    ? ObserverLossKind.Stopped
                    : ObserverLossKind.QueueFull;
                RecordLoss(
                    batches,
                    recordCount,
                    kind);
                if (kind == ObserverLossKind.Stopped) FlushPendingLosses();
                return false;
            }

            reservationOwned = false; // The single reader now owns release.
            Interlocked.Add(ref _enqueuedBatches, batches.Length);
            Interlocked.Add(ref _enqueuedRecords, recordCount);
            return true;
        }
        finally
        {
            // Covers both TryWrite=false and an unexpected channel exception.
            if (reservationOwned) ReleaseReservation(recordCount);
        }
    }

    public HeatActorHourObserverSnapshot Snapshot()
    {
        var lastFailureTicks = Interlocked.Read(ref _lastFailureTicks);
        var losses = SnapshotLosses();
        return new HeatActorHourObserverSnapshot(
            Enabled,
            Enabled ? _options.ActorHourShadowQueueCapacity : 0,
            Enabled ? _options.ActorHourShadowQueueRecordCapacity : 0,
            Interlocked.Read(ref _queuedRecords),
            Interlocked.Read(ref _enqueuedBatches),
            Interlocked.Read(ref _enqueuedRecords),
            Interlocked.Read(ref _processedBatches),
            Interlocked.Read(ref _processedRecords),
            Interlocked.Read(ref _droppedBatches),
            Interlocked.Read(ref _droppedRecords),
            Interlocked.Read(ref _queueFullRecords),
            Interlocked.Read(ref _recordCapacityRecords),
            Interlocked.Read(ref _stoppedRecords),
            Interlocked.Read(ref _processingFailureRecords),
            losses.Sum(loss => loss.PendingForwardRecords),
            lastFailureTicks == 0
                ? null
                : new DateTimeOffset(lastFailureTicks, TimeSpan.Zero),
            Volatile.Read(ref _lastFailure),
            losses);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Volatile.Write(ref _accepting, 0);
        _channel?.Writer.TryComplete();
        await base.StopAsync(cancellationToken);
        // ExecuteAsync performs the primary final drain. This second confirmed
        // flush closes the small race with a concurrent stopped rejection.
        FlushPendingLosses();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Enabled || _channel is null) return;
        try
        {
            var readTask = _channel.Reader.WaitToReadAsync(stoppingToken).AsTask();
            var tickTask = Task.Delay(LossRetryInterval, _timeProvider, stoppingToken);
            while (!stoppingToken.IsCancellationRequested)
            {
                _ = await Task.WhenAny(readTask, tickTask);

                if (tickTask.IsCompleted)
                {
                    await tickTask;
                    FlushPendingLosses();
                    tickTask = Task.Delay(LossRetryInterval, _timeProvider, stoppingToken);
                }

                if (!readTask.IsCompleted) continue;
                if (!await readTask) break;
                DrainAvailable(stoppingToken);
                readTask = _channel.Reader.WaitToReadAsync(stoppingToken).AsTask();
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            // BackgroundService exceptions stop the default Host. This observer
            // is a fail-open shadow, so retain the failure for health reporting
            // and complete normally instead of taking down durable ingest.
            TryRecordFailure(exception);
        }
        finally
        {
            Volatile.Write(ref _accepting, 0);
            try
            {
                while (_channel.Reader.TryRead(out var envelope))
                {
                    ReleaseReservation(envelope.RecordCount);
                    try
                    {
                        RecordLoss(
                            envelope.Batches, envelope.RecordCount,
                            ObserverLossKind.Stopped);
                    }
                    catch (Exception exception)
                    {
                        // Keep draining other reservations even if one malformed
                        // envelope cannot be attributed.
                        TryRecordFailure(exception);
                    }
                }
            }
            catch (Exception exception)
            {
                TryRecordFailure(exception);
            }
            try
            {
                FlushPendingLosses();
            }
            catch (Exception exception)
            {
                TryRecordFailure(exception);
            }
        }
    }

    private void DrainAvailable(CancellationToken stoppingToken)
    {
        while (_channel!.Reader.TryRead(out var envelope))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                ReleaseReservation(envelope.RecordCount);
                RecordLoss(
                    envelope.Batches, envelope.RecordCount,
                    ObserverLossKind.Stopped);
                continue;
            }
            try
            {
                _processor.Observe(envelope.Batches);
                Interlocked.Add(ref _processedBatches, envelope.Batches.Length);
                Interlocked.Add(ref _processedRecords, envelope.RecordCount);
            }
            catch (Exception exception)
            {
                RecordFailure(exception);
                RecordLoss(
                    envelope.Batches, envelope.RecordCount,
                    ObserverLossKind.ProcessingFailure);
            }
            finally
            {
                ReleaseReservation(envelope.RecordCount);
            }
            FlushPendingLosses();
        }
    }

    private bool TryReserveRecords(int records)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _queuedRecords);
            if (current > _options.ActorHourShadowQueueRecordCapacity - records)
                return false;
            if (Interlocked.CompareExchange(
                    ref _queuedRecords, current + records, current) == current)
                return true;
        }
    }

    private void ReleaseReservation(int records) =>
        Interlocked.Add(ref _queuedRecords, -records);

    private void RecordLoss(
        ChhtBatch[] batches,
        int recordCount,
        ObserverLossKind kind)
    {
        for (var index = 0; index < batches.Length; index++)
        {
            var records = batches[index].RecordCount;
            if (records <= 0) continue;
            long hour;
            try
            {
                hour = HeatActorHourShadowStore.UnixHour(
                    batches[index].Day, batches[index].Hour);
            }
            catch
            {
                hour = HeatActorHourShadowStore.UnixHour(_timeProvider.GetUtcNow());
            }
            RecordHourLoss(hour, records, kind);
        }
        Interlocked.Add(ref _droppedBatches, batches.Length);
        Interlocked.Add(ref _droppedRecords, recordCount);
        ref var counter = ref CounterFor(kind);
        Interlocked.Add(ref counter, recordCount);
    }

    private void RecordHourLoss(long hour, long records, ObserverLossKind kind)
    {
        var cell = _losses[(int)((ulong)hour % LossSlots)];
        lock (cell.Gate)
        {
            if (cell.Hour != hour)
            {
                // Publish the new hour only after the old counters have been
                // cleared. All writers/readers use the same small per-slot lock.
                cell.QueueFullRecords = 0;
                cell.RecordCapacityRecords = 0;
                cell.StoppedRecords = 0;
                cell.ProcessingFailureRecords = 0;
                cell.ForwardedRecords = 0;
                cell.Hour = hour;
            }
            ref var counter = ref CellCounterFor(cell, kind);
            counter = checked(counter + records);
        }
    }

    private void FlushPendingLosses()
    {
        if (!Enabled) return;
        foreach (var cell in _losses)
        {
            long hour;
            long total;
            lock (cell.Gate)
            {
                hour = cell.Hour;
                total = cell.TotalRecords;
                if (hour == long.MinValue || total <= cell.ForwardedRecords) continue;
            }

            bool accepted;
            try
            {
                accepted = _processor.TryRecordExternalLossTotal(
                    hour, total, HeatActorHourPartialReason.ObserverQueueLoss);
            }
            catch (Exception exception)
            {
                RecordFailure(exception);
                accepted = false;
            }
            if (!accepted) continue;

            lock (cell.Gate)
            {
                if (cell.Hour == hour && total > cell.ForwardedRecords)
                    cell.ForwardedRecords = total;
            }
        }
    }

    private HeatActorHourObserverLossSnapshot[] SnapshotLosses()
    {
        if (!Enabled) return [];
        var snapshots = new List<HeatActorHourObserverLossSnapshot>(_losses.Length);
        foreach (var cell in _losses)
        {
            lock (cell.Gate)
            {
                if (cell.Hour == long.MinValue || cell.TotalRecords == 0) continue;
                snapshots.Add(new HeatActorHourObserverLossSnapshot(
                    cell.Hour,
                    cell.QueueFullRecords,
                    cell.RecordCapacityRecords,
                    cell.StoppedRecords,
                    cell.ProcessingFailureRecords,
                    cell.ForwardedRecords));
            }
        }
        return snapshots.OrderBy(loss => loss.UnixHour).ToArray();
    }

    private void RecordFailure(Exception exception) =>
        RecordFailureMessage($"{exception.GetType().Name}: {exception.Message}");

    private void TryRecordFailure(Exception exception)
    {
        try
        {
            RecordFailure(exception);
        }
        catch
        {
            // Failure telemetry must remain subordinate to the shadow path.
        }
    }

    private void RecordFailureMessage(string message)
    {
        Volatile.Write(ref _lastFailure, message[..Math.Min(512, message.Length)]);
        Interlocked.Exchange(ref _lastFailureTicks, _timeProvider.GetUtcNow().UtcTicks);
    }

    private ref long CounterFor(ObserverLossKind kind)
    {
        switch (kind)
        {
            case ObserverLossKind.QueueFull: return ref _queueFullRecords;
            case ObserverLossKind.RecordCapacity: return ref _recordCapacityRecords;
            case ObserverLossKind.Stopped: return ref _stoppedRecords;
            case ObserverLossKind.ProcessingFailure: return ref _processingFailureRecords;
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static ref long CellCounterFor(LossCell cell, ObserverLossKind kind)
    {
        switch (kind)
        {
            case ObserverLossKind.QueueFull: return ref cell.QueueFullRecords;
            case ObserverLossKind.RecordCapacity: return ref cell.RecordCapacityRecords;
            case ObserverLossKind.Stopped: return ref cell.StoppedRecords;
            case ObserverLossKind.ProcessingFailure: return ref cell.ProcessingFailureRecords;
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    // Struct envelopes make warmed successful admission allocation-free.
    private readonly record struct ObservationEnvelope(
        ChhtBatch[] Batches,
        int RecordCount);

    private sealed class LossCell
    {
        public object Gate { get; } = new();
        public long Hour = long.MinValue;
        public long QueueFullRecords;
        public long RecordCapacityRecords;
        public long StoppedRecords;
        public long ProcessingFailureRecords;
        public long ForwardedRecords;
        public long TotalRecords => checked(
            QueueFullRecords + RecordCapacityRecords + StoppedRecords + ProcessingFailureRecords);
    }

    private enum ObserverLossKind
    {
        QueueFull,
        RecordCapacity,
        Stopped,
        ProcessingFailure
    }
}
