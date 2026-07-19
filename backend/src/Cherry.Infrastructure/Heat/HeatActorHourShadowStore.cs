using System.Buffers.Binary;
using System.Numerics;
using System.Security.Cryptography;

namespace Cherry.Infrastructure.Heat;

[Flags]
public enum HeatActorHourPartialReason
{
    None = 0,
    OpenHour = 1,
    ProcessRestart = 2,
    PairCapacity = 4,
    HashCapacity = 8,
    ObservationFailure = 16,
    ClockGap = 32,
    ObserverQueueLoss = 64,
    ClockRollback = 128
}

public sealed record HeatActorHourSlotSnapshot(
    long UnixHour,
    bool Current,
    string Coverage,
    HeatActorHourPartialReason PartialReasons,
    long Checks,
    long NewPairs,
    long ProbableDuplicates,
    long Bypassed,
    long DroppedBeforeObserve,
    int ActiveHashes,
    ulong ActorHours,
    long BitsSet,
    long BitCount,
    long BitFillPpm,
    string[] ObservedCrawlerIds);

public sealed record HeatActorHourOutOfWindowLossSnapshot(
    long UnixHour,
    long OldRecords,
    long FutureRecords,
    long DroppedBeforeObserve);

public sealed record HeatActorHourShadowSnapshot(
    bool Enabled,
    bool Volatile,
    DateTimeOffset ProcessStartedAt,
    int PairCapacityPerHour,
    int HashCapacityPerHour,
    int FalsePositivePpm,
    long EstimatedHotPathBytes,
    bool FrameExportMemoryExcluded,
    long Rotations,
    long ClockRollbackDetections,
    long ClockRollbackHours,
    long ClockGapDetections,
    long ClockGapHours,
    long OldHourBypassed,
    long FutureHourBypassed,
    HeatActorHourOutOfWindowLossSnapshot[] OutOfWindowLosses,
    long ObservationFailures,
    DateTimeOffset? LastFailureAt,
    string? LastFailure,
    HeatActorHourSlotSnapshot? Current,
    HeatActorHourSlotSnapshot? Previous);

public interface IHeatActorHourBatchObserver
{
    void Observe(IReadOnlyList<ChhtBatch> batches);
    bool TryRecordExternalLossTotal(
        long unixHour,
        long cumulativeRecords,
        HeatActorHourPartialReason reason);
}

/// <summary>
/// Volatile, bounded shadow accumulator for the proposed actor-hour metric.
/// It never participates in the durable CHHT acknowledgement decision.
/// </summary>
public sealed class HeatActorHourShadowStore : IHeatActorHourBatchObserver
{
    private const int BloomBlockBits = 512;
    private const int BloomBlockWords = BloomBlockBits / 64;
    private const double BloomHeadroom = 1.25;
    private readonly object _gate = new();
    private readonly TimeProvider _timeProvider;
    private readonly HourSlot[] _slots = [];
    private readonly ulong _seedOne;
    private readonly ulong _seedTwo;
    private readonly int _pairCapacity;
    private readonly int _hashCapacity;
    private readonly int _falsePositivePpm;
    private readonly int _bloomHashCount;
    private readonly long _bloomBitCount;
    private readonly long _configuredBytes;
    private readonly HourLoss[] _outOfWindowLosses = [];
    private readonly DateTimeOffset _processStartedAt;
    private int _currentSlot;
    private long _currentHour;
    private long _rotations;
    private long _clockRollbackDetections;
    private long _clockRollbackHours;
    private long _clockGapDetections;
    private long _clockGapHours;
    private long _lastRollbackWallHour = long.MinValue;
    private long _oldHourBypassed;
    private long _futureHourBypassed;
    private long _observationFailures;
    private DateTimeOffset? _lastFailureAt;
    private string? _lastFailure;

    public HeatActorHourShadowStore(HeatOptions options, TimeProvider? timeProvider = null)
    {
        Enabled = options.Enabled && options.ActorHourShadowEnabled;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _processStartedAt = _timeProvider.GetUtcNow();
        _currentHour = UnixHour(_processStartedAt);
        _pairCapacity = options.ActorHourShadowPairCapacity;
        _hashCapacity = Math.Min(
            options.ActorHourShadowHashCapacity,
            options.ActorHourShadowPairCapacity);
        _falsePositivePpm = options.ActorHourShadowFalsePositivePpm;
        if (!Enabled) return;

        if (_pairCapacity <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(options.ActorHourShadowPairCapacity), "Actor-hour pair capacity must be positive");
        if (_hashCapacity <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(options.ActorHourShadowHashCapacity), "Actor-hour hash capacity must be positive");
        if (_falsePositivePpm is <= 0 or >= 1_000_000)
            throw new ArgumentOutOfRangeException(
                nameof(options.ActorHourShadowFalsePositivePpm),
                "Actor-hour false-positive PPM must be between 1 and 999999");

        _slots = new HourSlot[2];
        _outOfWindowLosses = new HourLoss[48];

        var probability = _falsePositivePpm / 1_000_000d;
        var rawBits = -_pairCapacity * Math.Log(probability) / (Math.Log(2) * Math.Log(2));
        var blockCount = checked((long)Math.Ceiling(rawBits * BloomHeadroom / BloomBlockBits));
        blockCount = Math.Max(1, blockCount);
        _bloomBitCount = checked(blockCount * BloomBlockBits);
        _bloomHashCount = Math.Clamp(
            (int)Math.Round(rawBits / _pairCapacity * Math.Log(2)), 1, 16);

        var randomSeed = RandomNumberGenerator.GetBytes(16);
        _seedOne = BinaryPrimitives.ReadUInt64LittleEndian(randomSeed);
        _seedTwo = BinaryPrimitives.ReadUInt64LittleEndian(randomSeed.AsSpan(8));
        if (_seedOne == _seedTwo) _seedTwo ^= 0x9e3779b97f4a7c15UL;

        var words = checked((int)(_bloomBitCount / 64));
        for (var index = 0; index < _slots.Length; index++)
            _slots[index] = new HourSlot(words, _hashCapacity);
        _slots[_currentSlot].Reset(_currentHour, HeatActorHourPartialReason.ProcessRestart);
        _slots[1 - _currentSlot].Reset(
            _currentHour - 1, HeatActorHourPartialReason.ProcessRestart);
        _configuredBytes = checked(
            (long)_slots.Length *
            ((long)words * sizeof(ulong) +
             (long)_slots[0].CountTableLength * CompactCountTable.EntryBytes));
    }

    public bool Enabled { get; }

    public void Observe(IReadOnlyList<ChhtBatch> batches)
    {
        if (!Enabled || batches.Count == 0) return;
        try
        {
            lock (_gate)
            {
                RotateTo(UnixHour(_timeProvider.GetUtcNow()));
                foreach (var batch in batches) ObserveLocked(batch);
            }
        }
        catch (Exception exception)
        {
            // This observer is not part of the durable ACK boundary. Even an
            // unexpected implementation failure must leave the authority path
            // successful; health exposes the loss as partial shadow coverage.
            try
            {
                lock (_gate) RecordFailure(exception, 1, null);
            }
            catch
            {
                // There is intentionally no final throw from a shadow path.
            }
        }
    }

    public HeatActorHourShadowSnapshot Snapshot()
    {
        if (!Enabled)
            return new HeatActorHourShadowSnapshot(
                false, true, _processStartedAt, _pairCapacity, _hashCapacity,
                _falsePositivePpm, 0, true, 0, 0, 0, 0, 0, 0, 0, [],
                0, null, null, null, null);
        lock (_gate)
        {
            RotateTo(UnixHour(_timeProvider.GetUtcNow()));
            var current = _slots[_currentSlot];
            var previous = _slots[1 - _currentSlot];
            return new HeatActorHourShadowSnapshot(
                true,
                true,
                _processStartedAt,
                _pairCapacity,
                _hashCapacity,
                _falsePositivePpm,
                _configuredBytes,
                true,
                _rotations,
                _clockRollbackDetections,
                _clockRollbackHours,
                _clockGapDetections,
                _clockGapHours,
                _oldHourBypassed,
                _futureHourBypassed,
                SnapshotOutOfWindowLosses(),
                _observationFailures,
                _lastFailureAt,
                _lastFailure,
                current.Valid ? SnapshotSlot(current, current: true) : null,
                previous.Valid ? SnapshotSlot(previous, current: false) : null);
        }
    }

    public bool TryRecordExternalLossTotal(
        long unixHour,
        long cumulativeRecords,
        HeatActorHourPartialReason reason)
    {
        if (!Enabled) return false;
        if (cumulativeRecords <= 0) return true;
        try
        {
            lock (_gate)
            {
                RotateTo(UnixHour(_timeProvider.GetUtcNow()));
                HourSlot? slot = null;
                if (unixHour == _currentHour)
                    slot = _slots[_currentSlot];
                else if (unixHour == _currentHour - 1 &&
                         _slots[1 - _currentSlot].Valid &&
                         _slots[1 - _currentSlot].Hour == unixHour)
                    slot = _slots[1 - _currentSlot];
                if (slot is not null)
                {
                    if (cumulativeRecords > slot.ExternalLossTotalApplied)
                    {
                        var delta = cumulativeRecords - slot.ExternalLossTotalApplied;
                        slot.Bypassed = checked(slot.Bypassed + delta);
                        slot.DroppedBeforeObserve = checked(slot.DroppedBeforeObserve + delta);
                        slot.ExternalLossTotalApplied = cumulativeRecords;
                    }
                    slot.PartialReasons |= reason;
                }
                else
                {
                    var index = (int)((ulong)unixHour % (uint)_outOfWindowLosses.Length);
                    ref var loss = ref _outOfWindowLosses[index];
                    if (!loss.Valid || loss.Hour != unixHour)
                        loss = new HourLoss { Valid = true, Hour = unixHour };
                    if (cumulativeRecords > loss.ExternalLossTotalApplied)
                    {
                        var delta = cumulativeRecords - loss.ExternalLossTotalApplied;
                        if (unixHour > _currentHour)
                            loss.FutureRecords = checked(loss.FutureRecords + delta);
                        else
                            loss.OldRecords = checked(loss.OldRecords + delta);
                        loss.DroppedBeforeObserve = checked(loss.DroppedBeforeObserve + delta);
                        loss.ExternalLossTotalApplied = cumulativeRecords;
                    }
                }
            }
            return true;
        }
        catch
        {
            // The caller retains its cumulative watermark and retries. Returning
            // false is essential: an unconfirmed coverage update may not be
            // silently treated as durable within this process.
            return false;
        }
    }

    public bool TryCreateFrameSnapshot(long unixHour, out HeatActorHourFrame? frame)
    {
        frame = null;
        if (!Enabled) return false;
        lock (_gate)
        {
            RotateTo(UnixHour(_timeProvider.GetUtcNow()));
            HourSlot? slot = null;
            foreach (var candidate in _slots)
                if (candidate.Valid && candidate.Hour == unixHour)
                {
                    slot = candidate;
                    break;
                }
            if (slot is null) return false;
            var isCurrent = slot.Hour == _currentHour;
            var reasons = slot.PartialReasons |
                          (isCurrent ? HeatActorHourPartialReason.OpenHour : 0);
            var state = isCurrent
                ? HeatActorHourFrameState.Open
                : HeatActorHourFrameState.Provisional;
            var coverage = reasons == HeatActorHourPartialReason.None
                ? HeatActorHourCoverage.Unknown
                : HeatActorHourCoverage.Partial;
            frame = new HeatActorHourFrame(
                slot.Hour,
                state,
                coverage,
                reasons,
                _falsePositivePpm,
                _pairCapacity,
                slot.NewPairs,
                slot.ProbableDuplicates,
                slot.Bypassed,
                slot.Counts.ExportSorted());
            return true;
        }
    }

    public static long UnixHour(DateTimeOffset value) =>
        HeatRollingStore.UnixHour(value.UtcDateTime);

    public static long UnixHour(DateOnly day, byte hour)
    {
        return HeatRollingStore.UnixHour(day, hour);
    }

    private void ObserveLocked(ChhtBatch batch)
    {
        long targetHour;
        try
        {
            targetHour = UnixHour(batch.Day, batch.Hour);
        }
        catch (Exception exception)
        {
            RecordFailure(exception, batch.RecordCount, null);
            return;
        }

        HourSlot? slot;
        if (targetHour == _currentHour)
            slot = _slots[_currentSlot];
        else if (targetHour == _currentHour - 1 &&
                 _slots[1 - _currentSlot].Valid &&
                 _slots[1 - _currentSlot].Hour == targetHour)
            slot = _slots[1 - _currentSlot];
        else
        {
            if (targetHour > _currentHour)
            {
                _futureHourBypassed = checked(_futureHourBypassed + batch.RecordCount);
                RecordOutOfWindowLoss(
                    targetHour, batch.RecordCount, future: true,
                    droppedBeforeObserve: false);
            }
            else
            {
                _oldHourBypassed = checked(_oldHourBypassed + batch.RecordCount);
                RecordOutOfWindowLoss(
                    targetHour, batch.RecordCount, future: false,
                    droppedBeforeObserve: false);
            }
            return;
        }

        slot.ObservedCrawlerIds.Add(batch.CrawlerId);
        try
        {
            foreach (var group in batch.Groups)
            {
                if (group.InfoHash.Length != 20)
                    throw new InvalidDataException("Actor-hour info hash must be exactly 20 bytes");
                var key = InfoHashKey.Read(group.InfoHash);
                var countEntry = -1;
                var countEntryUnavailable = false;
                for (var actorIndex = 0; actorIndex < group.ActorFingerprints.Count; actorIndex++)
                {
                    var signedActor = group.ActorFingerprints[actorIndex];
                    slot.Checks++;
                    var actor = unchecked((ulong)signedActor);
                    var h1 = PairHash(key, actor, _seedOne);
                    var h2 = PairHash(key, actor, _seedTwo) | 1UL;
                    if (slot.Bloom.MightContain(h1, h2, _bloomHashCount))
                    {
                        slot.ProbableDuplicates++;
                        continue;
                    }
                    if (slot.NewPairs >= _pairCapacity)
                    {
                        slot.Bypassed++;
                        slot.PartialReasons |= HeatActorHourPartialReason.PairCapacity;
                        continue;
                    }
                    if (countEntry < 0 && !countEntryUnavailable)
                    {
                        if (!slot.Counts.TryGetOrAdd(key, out countEntry))
                        {
                            countEntryUnavailable = true;
                            slot.PartialReasons |= HeatActorHourPartialReason.HashCapacity;
                        }
                    }
                    if (countEntryUnavailable)
                    {
                        slot.Bypassed++;
                        continue;
                    }

                    slot.Counts.Increment(countEntry);
                    slot.BitsSet += slot.Bloom.Add(h1, h2, _bloomHashCount);
                    slot.NewPairs++;
                }
            }
        }
        catch (Exception exception)
        {
            RecordFailure(exception, batch.RecordCount, slot);
        }
    }

    private void RotateTo(long wallHour)
    {
        if (wallHour < _currentHour)
        {
            if (_lastRollbackWallHour != wallHour)
            {
                _clockRollbackDetections++;
                _clockRollbackHours = checked(
                    _clockRollbackHours + _currentHour - wallHour);
                _slots[0].PartialReasons |= HeatActorHourPartialReason.ClockRollback;
                _slots[1].PartialReasons |= HeatActorHourPartialReason.ClockRollback;
                _lastRollbackWallHour = wallHour;
            }
            return;
        }
        if (wallHour == _currentHour)
        {
            // Returning to the authoritative hour ends a rollback episode. A
            // later rollback to the same wall hour must be counted anew.
            _lastRollbackWallHour = long.MinValue;
            return;
        }
        _lastRollbackWallHour = long.MinValue;
        if (wallHour == _currentHour + 1)
        {
            _currentSlot = 1 - _currentSlot;
            _slots[_currentSlot].Reset(wallHour, HeatActorHourPartialReason.None);
        }
        else
        {
            _clockGapDetections++;
            _clockGapHours = checked(_clockGapHours + wallHour - _currentHour - 1);
            _currentSlot = 0;
            _slots[_currentSlot].Reset(wallHour, HeatActorHourPartialReason.ClockGap);
            _slots[1 - _currentSlot].Reset(
                wallHour - 1, HeatActorHourPartialReason.ClockGap);
        }
        _rotations++;
        _currentHour = wallHour;
    }

    private void RecordFailure(Exception exception, int records, HourSlot? slot)
    {
        _observationFailures = checked(_observationFailures + Math.Max(1, records));
        _lastFailureAt = _timeProvider.GetUtcNow();
        var message = $"{exception.GetType().Name}: {exception.Message}";
        _lastFailure = message[..Math.Min(512, message.Length)];
        if (slot is not null)
        {
            slot.Bypassed = checked(slot.Bypassed + Math.Max(1, records));
            slot.PartialReasons |= HeatActorHourPartialReason.ObservationFailure;
        }
    }

    private void RecordOutOfWindowLoss(
        long hour,
        long records,
        bool future,
        bool droppedBeforeObserve)
    {
        var index = (int)((ulong)hour % (uint)_outOfWindowLosses.Length);
        ref var loss = ref _outOfWindowLosses[index];
        if (!loss.Valid || loss.Hour != hour)
            loss = new HourLoss { Valid = true, Hour = hour };
        if (future)
            loss.FutureRecords = checked(loss.FutureRecords + records);
        else
            loss.OldRecords = checked(loss.OldRecords + records);
        if (droppedBeforeObserve)
            loss.DroppedBeforeObserve = checked(loss.DroppedBeforeObserve + records);
    }

    private HeatActorHourOutOfWindowLossSnapshot[] SnapshotOutOfWindowLosses() =>
        _outOfWindowLosses
            .Where(loss => loss.Valid)
            .OrderBy(loss => loss.Hour)
            .Select(loss => new HeatActorHourOutOfWindowLossSnapshot(
                loss.Hour, loss.OldRecords, loss.FutureRecords,
                loss.DroppedBeforeObserve))
            .ToArray();

    private HeatActorHourSlotSnapshot SnapshotSlot(HourSlot slot, bool current)
    {
        var reasons = slot.PartialReasons |
                      (current ? HeatActorHourPartialReason.OpenHour : 0);
        var coverage = current
            ? "open-partial"
            : reasons == HeatActorHourPartialReason.None ? "provisional" : "partial";
        return new HeatActorHourSlotSnapshot(
            slot.Hour,
            current,
            coverage,
            reasons,
            slot.Checks,
            slot.NewPairs,
            slot.ProbableDuplicates,
            slot.Bypassed,
            slot.DroppedBeforeObserve,
            slot.Counts.Count,
            slot.Counts.Sum,
            slot.BitsSet,
            _bloomBitCount,
            _bloomBitCount == 0 ? 0 : slot.BitsSet * 1_000_000 / _bloomBitCount,
            slot.ObservedCrawlerIds.Order(StringComparer.Ordinal).ToArray());
    }

    private static ulong PairHash(InfoHashKey key, ulong actor, ulong seed)
    {
        var value = Mix(key.First ^ seed);
        value = Mix(value ^ key.Second);
        value = Mix(value ^ ((ulong)key.Tail << 32 | key.Tail));
        return Mix(value ^ actor);
    }

    private static ulong Mix(ulong value)
    {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9UL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebUL;
        return value ^ (value >> 31);
    }

    private sealed class HourSlot
    {
        public HourSlot(int bloomWords, int hashCapacity)
        {
            Bloom = new BlockedBloom(bloomWords);
            Counts = new CompactCountTable(hashCapacity);
        }

        public bool Valid { get; private set; }
        public long Hour { get; private set; }
        public BlockedBloom Bloom { get; }
        public CompactCountTable Counts { get; }
        public HashSet<string> ObservedCrawlerIds { get; } = new(StringComparer.Ordinal);
        public HeatActorHourPartialReason PartialReasons { get; set; }
        public long Checks { get; set; }
        public long NewPairs { get; set; }
        public long ProbableDuplicates { get; set; }
        public long Bypassed { get; set; }
        public long DroppedBeforeObserve { get; set; }
        public long ExternalLossTotalApplied { get; set; }
        public long BitsSet { get; set; }
        public int CountTableLength => Counts.TableLength;

        public void Reset(long hour, HeatActorHourPartialReason reasons)
        {
            Bloom.Clear();
            Counts.Clear();
            ObservedCrawlerIds.Clear();
            Hour = hour;
            PartialReasons = reasons;
            Checks = 0;
            NewPairs = 0;
            ProbableDuplicates = 0;
            Bypassed = 0;
            DroppedBeforeObserve = 0;
            ExternalLossTotalApplied = 0;
            BitsSet = 0;
            Valid = true;
        }

        public void Invalidate()
        {
            Valid = false;
            ObservedCrawlerIds.Clear();
        }
    }

    private struct HourLoss
    {
        public bool Valid;
        public long Hour;
        public long OldRecords;
        public long FutureRecords;
        public long DroppedBeforeObserve;
        public long ExternalLossTotalApplied;
    }

    private sealed class BlockedBloom
    {
        private readonly ulong[] _words;
        private readonly ulong _blocks;

        public BlockedBloom(int words)
        {
            if (words <= 0 || words % BloomBlockWords != 0)
                throw new ArgumentOutOfRangeException(nameof(words));
            _words = new ulong[words];
            _blocks = (ulong)(words / BloomBlockWords);
        }

        public void Clear() => Array.Clear(_words);

        public bool MightContain(ulong h1, ulong h2, int hashes)
        {
            var baseWord = (h1 >> 9) % _blocks * BloomBlockWords;
            var stateOne = h1;
            var stateTwo = h2;
            for (var index = 0; index < hashes; index++)
            {
                var bit = NextBit(ref stateOne, ref stateTwo);
                if ((_words[checked((int)(baseWord + bit / 64))] &
                     (1UL << (int)(bit & 63))) == 0)
                    return false;
            }
            return true;
        }

        public int Add(ulong h1, ulong h2, int hashes)
        {
            var added = 0;
            var baseWord = (h1 >> 9) % _blocks * BloomBlockWords;
            var stateOne = h1;
            var stateTwo = h2;
            for (var index = 0; index < hashes; index++)
            {
                var bit = NextBit(ref stateOne, ref stateTwo);
                var word = checked((int)(baseWord + bit / 64));
                var mask = 1UL << (int)(bit & 63);
                if ((_words[word] & mask) != 0) continue;
                _words[word] |= mask;
                added++;
            }
            return added;
        }

        // xoroshiro128+ expands the two 64-bit key hashes into independent-ish
        // positions. Simple double hashing in a 512-bit block has only about
        // 2^17 distinct progressions and creates a measurable collision floor
        // long before the configured Bloom false-positive rate is reached.
        private static ulong NextBit(ref ulong stateOne, ref ulong stateTwo)
        {
            var result = BitOperations.RotateLeft(stateOne + stateTwo, 17) + stateOne;
            stateTwo ^= stateOne;
            stateOne = BitOperations.RotateLeft(stateOne, 49) ^ stateTwo ^ (stateTwo << 21);
            stateTwo = BitOperations.RotateLeft(stateTwo, 28);
            return result & (BloomBlockBits - 1);
        }
    }

    internal readonly record struct InfoHashKey(ulong First, ulong Second, uint Tail)
    {
        public static InfoHashKey Read(ReadOnlySpan<byte> hash) => new(
            BinaryPrimitives.ReadUInt64BigEndian(hash),
            BinaryPrimitives.ReadUInt64BigEndian(hash[8..]),
            BinaryPrimitives.ReadUInt32BigEndian(hash[16..]));

        public byte[] ToArray()
        {
            var bytes = new byte[20];
            BinaryPrimitives.WriteUInt64BigEndian(bytes, First);
            BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(8), Second);
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(16), Tail);
            return bytes;
        }
    }

    private sealed class CompactCountTable
    {
        public const int EntryBytes = 24;
        private readonly Entry[] _entries;
        private readonly int _maxCount;

        public CompactCountTable(int maxCount)
        {
            _maxCount = maxCount;
            var required = checked((int)Math.Ceiling(maxCount / 0.70));
            var length = 2;
            while (length < required)
            {
                if (length > 1 << 29)
                    throw new ArgumentOutOfRangeException(nameof(maxCount));
                length <<= 1;
            }
            _entries = new Entry[length];
        }

        public int Count { get; private set; }
        public ulong Sum { get; private set; }
        public int TableLength => _entries.Length;

        public bool TryGetOrAdd(InfoHashKey key, out int index)
        {
            var mask = _entries.Length - 1;
            index = (int)(Mix(key.First ^ key.Second ^ key.Tail) & (uint)mask);
            while (true)
            {
                ref var entry = ref _entries[index];
                if (entry.Count == 0)
                {
                    if (Count >= _maxCount)
                    {
                        index = -1;
                        return false;
                    }
                    entry.First = key.First;
                    entry.Second = key.Second;
                    entry.Tail = key.Tail;
                    Count++;
                    return true;
                }
                if (entry.First == key.First && entry.Second == key.Second && entry.Tail == key.Tail)
                    return true;
                index = (index + 1) & mask;
            }
        }

        public void Increment(int index)
        {
            ref var entry = ref _entries[index];
            if (entry.Count == uint.MaxValue) return;
            entry.Count++;
            Sum++;
        }

        public HeatActorHourCount[] ExportSorted()
        {
            var result = new HeatActorHourCount[Count];
            var offset = 0;
            foreach (var entry in _entries)
            {
                if (entry.Count == 0) continue;
                result[offset++] = new HeatActorHourCount(
                    new InfoHashKey(entry.First, entry.Second, entry.Tail).ToArray(), entry.Count);
            }
            Array.Sort(result, static (left, right) => left.InfoHash.AsSpan().SequenceCompareTo(right.InfoHash));
            return result;
        }

        public void Clear()
        {
            Array.Clear(_entries);
            Count = 0;
            Sum = 0;
        }

        private struct Entry
        {
            public ulong First;
            public ulong Second;
            public uint Tail;
            public uint Count;
        }
    }
}
