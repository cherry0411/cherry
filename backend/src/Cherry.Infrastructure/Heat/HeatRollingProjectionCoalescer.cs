using System.Buffers.Binary;

namespace Cherry.Infrastructure.Heat;

internal sealed record RollingProjectionPendingChange(
    long Id,
    byte[] InfoHash,
    long Count,
    long ProjectedCount,
    long Revision);

/// <summary>
/// Single-consumer, in-memory write combiner. SQLite dirty revisions remain
/// authoritative until the supplied flush callback has both completed its
/// Meilisearch task and committed the matching SQLite revisions.
/// </summary>
internal sealed class HeatRollingProjectionCoalescer
{
    private readonly int _batchSize;
    private readonly TimeSpan _maxDelay;
    private readonly TimeProvider _timeProvider;
    private readonly Dictionary<InfoHashKey, PendingEntry> _pending;
    private long? _targetHour;
    private long _recoveryGeneration = long.MinValue;
    private DateTimeOffset? _oldestEnqueuedAt;

    public HeatRollingProjectionCoalescer(
        int batchSize,
        TimeSpan maxDelay,
        TimeProvider? timeProvider = null)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (maxDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(maxDelay));
        _batchSize = batchSize;
        _maxDelay = maxDelay;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _pending = new Dictionary<InfoHashKey, PendingEntry>(batchSize);
    }

    public int Count => _pending.Count;
    public long? TargetHour => _targetHour;

    /// <summary>
    /// Changes of target hour or destructive index-recovery generation discard
    /// only volatile state. Their SQLite revisions were never acknowledged and
    /// are therefore replayed by the next projection pass.
    /// </summary>
    public bool BeginWindow(long targetHour, long recoveryGeneration)
    {
        if (_targetHour == targetHour && _recoveryGeneration == recoveryGeneration)
            return false;
        _pending.Clear();
        _oldestEnqueuedAt = null;
        _targetHour = targetHour;
        _recoveryGeneration = recoveryGeneration;
        return true;
    }

    public void Upsert(RollingProjectionPendingChange change)
    {
        if (_targetHour is null)
            throw new InvalidOperationException("A rolling projection window must be started first");
        var key = InfoHashKey.Read(change.InfoHash);
        if (_pending.TryGetValue(key, out var current))
            _pending[key] = current with { Change = change };
        else
        {
            if (_pending.Count >= _batchSize)
                throw new InvalidOperationException(
                    "The rolling projection batch must be flushed before adding another hash");
            var enqueuedAt = _timeProvider.GetUtcNow();
            _pending.Add(key, new PendingEntry(change, enqueuedAt));
            if (_oldestEnqueuedAt is null || enqueuedAt < _oldestEnqueuedAt.Value)
                _oldestEnqueuedAt = enqueuedAt;
        }
    }

    /// <summary>
    /// Refreshes a row already retained by a failed/partial prior pass without
    /// admitting a new key. The worker uses this to merge a replayed page into
    /// a full batch before retrying it, avoiding a second submission of the
    /// remaining page snapshot after the retry succeeds.
    /// </summary>
    public bool TryUpdateExisting(RollingProjectionPendingChange change)
    {
        var key = InfoHashKey.Read(change.InfoHash);
        if (!_pending.TryGetValue(key, out var current)) return false;
        _pending[key] = current with { Change = change };
        return true;
    }

    public bool Remove(byte[] infoHash)
    {
        if (!_pending.Remove(InfoHashKey.Read(infoHash), out var removed)) return false;
        if (removed.EnqueuedAt == _oldestEnqueuedAt) RefreshOldestEnqueuedAt();
        return true;
    }

    public bool IsFlushDue
    {
        get
        {
            if (_pending.Count == 0) return false;
            if (_pending.Count >= _batchSize) return true;
            return _timeProvider.GetUtcNow() - _oldestEnqueuedAt!.Value >= _maxDelay;
        }
    }

    public bool IsAtCapacity => _pending.Count >= _batchSize;

    public TimeSpan GetNextDelay(TimeSpan ordinaryPollInterval)
    {
        if (ordinaryPollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ordinaryPollInterval));
        if (_pending.Count == 0) return ordinaryPollInterval;
        if (_pending.Count >= _batchSize) return TimeSpan.Zero;
        var remaining = _oldestEnqueuedAt!.Value + _maxDelay - _timeProvider.GetUtcNow();
        if (remaining <= TimeSpan.Zero) return TimeSpan.Zero;
        return remaining < ordinaryPollInterval ? remaining : ordinaryPollInterval;
    }

    /// <summary>
    /// Removes a batch only after the callback confirms both remote task success
    /// and local revision ACK. False returns and exceptions retain every entry.
    /// </summary>
    public async Task<int> FlushAsync(
        bool force,
        Func<long, IReadOnlyList<RollingProjectionPendingChange>, CancellationToken, Task<bool>> flush,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(flush);
        if (_pending.Count == 0 || (!force && !IsFlushDue)) return 0;
        var targetHour = _targetHour ??
            throw new InvalidOperationException("Pending rolling changes have no target hour");
        var batch = _pending
            .OrderBy(pair => pair.Value.Change.Id)
            .Take(_batchSize)
            .ToArray();
        var changes = batch.Select(pair => pair.Value.Change).ToArray();
        if (!await flush(targetHour, changes, cancellationToken)) return 0;

        var removed = 0;
        foreach (var pair in batch)
        {
            if (!_pending.TryGetValue(pair.Key, out var current) ||
                current.Change.Id != pair.Value.Change.Id ||
                current.Change.Count != pair.Value.Change.Count ||
                current.Change.Revision != pair.Value.Change.Revision)
                continue;
            if (_pending.Remove(pair.Key)) removed++;
        }
        if (removed > 0) RefreshOldestEnqueuedAt();
        return removed;
    }

    private void RefreshOldestEnqueuedAt() =>
        _oldestEnqueuedAt = _pending.Count == 0
            ? null
            : _pending.Values.Min(entry => entry.EnqueuedAt);

    private sealed record PendingEntry(
        RollingProjectionPendingChange Change,
        DateTimeOffset EnqueuedAt);

    private readonly record struct InfoHashKey(ulong First, ulong Second, uint Tail)
    {
        public static InfoHashKey Read(ReadOnlySpan<byte> value)
        {
            if (value.Length != 20)
                throw new InvalidDataException("Rolling projection info hash must be exactly 20 bytes");
            return new InfoHashKey(
                BinaryPrimitives.ReadUInt64LittleEndian(value),
                BinaryPrimitives.ReadUInt64LittleEndian(value[8..]),
                BinaryPrimitives.ReadUInt32LittleEndian(value[16..]));
        }
    }
}
