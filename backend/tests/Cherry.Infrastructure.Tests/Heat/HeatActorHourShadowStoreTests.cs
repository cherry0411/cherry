using System.Security.Cryptography;
using Cherry.Infrastructure.Heat;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatActorHourShadowStoreTests
{
    private static readonly byte[] DailySecret = Enumerable.Repeat((byte)31, 32).ToArray();

    [Fact]
    public void CrossCrawlerAndReplayDeduplicateGloballyWithinHour()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 10, 15));
        var store = Store(clock);
        var hash = Hash(1);
        var sg = BatchAt(clock.GetUtcNow(), "sg-1", hash, [1, 2], 1, 1);
        var jp = BatchAt(clock.GetUtcNow(), "jp-1", hash, [2, 3], 1, 1);

        store.Observe([sg, jp, sg]);

        var snapshot = store.Snapshot();
        Assert.True(snapshot.Enabled);
        Assert.True(snapshot.Volatile);
        Assert.NotNull(snapshot.Current);
        Assert.Equal(6, snapshot.Current!.Checks);
        Assert.Equal(3, snapshot.Current.NewPairs);
        Assert.Equal(3, snapshot.Current.ProbableDuplicates);
        Assert.Equal(1, snapshot.Current.ActiveHashes);
        Assert.Equal(3UL, snapshot.Current.ActorHours);
        Assert.Equal(["jp-1", "sg-1"], snapshot.Current.ObservedCrawlerIds);
        Assert.Equal("open-partial", snapshot.Current.Coverage);
        Assert.True(snapshot.Current.PartialReasons.HasFlag(HeatActorHourPartialReason.OpenHour));
        Assert.True(snapshot.Current.PartialReasons.HasFlag(HeatActorHourPartialReason.ProcessRestart));
    }

    [Fact]
    public void SameActorCountsAgainNextHourAndPreviousSlotStillDeduplicates()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 23, 55));
        var store = Store(clock);
        var hash = Hash(2);
        var first = BatchAt(clock.GetUtcNow(), "sg-1", hash, [long.MinValue, long.MaxValue], 2, 1);
        store.Observe([first]);

        clock.Advance(TimeSpan.FromMinutes(10));
        var second = BatchAt(clock.GetUtcNow(), "jp-1", hash, [long.MinValue], 3, 1);
        store.Observe([second, first]);

        var snapshot = store.Snapshot();
        Assert.Equal(1, snapshot.Rotations);
        Assert.Equal(1, snapshot.Current!.NewPairs);
        Assert.Equal(2, snapshot.Previous!.NewPairs);
        Assert.Equal(2, snapshot.Previous.ProbableDuplicates);
        Assert.Equal(DateOnly.FromDateTime(clock.GetUtcNow().UtcDateTime),
            DateOnly.FromDateTime(UtcDate(snapshot.Current.UnixHour)));
    }

    [Fact]
    public void PairAndHashCapacityBypassWithoutPublishingGhostBloomBits()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 10, 0));
        var pairLimited = Store(clock, pairCapacity: 2, hashCapacity: 2);
        var thirdActor = BatchAt(clock.GetUtcNow(), "sg-1", Hash(3), [1, 2, 3], 4, 1);
        pairLimited.Observe([thirdActor, thirdActor]);

        var pairs = pairLimited.Snapshot().Current!;
        Assert.Equal(2, pairs.NewPairs);
        Assert.Equal(2, pairs.ProbableDuplicates);
        Assert.Equal(2, pairs.Bypassed);
        Assert.True(pairs.PartialReasons.HasFlag(HeatActorHourPartialReason.PairCapacity));

        var hashLimited = Store(clock, pairCapacity: 10, hashCapacity: 1);
        var firstHash = BatchAt(clock.GetUtcNow(), "sg-1", Hash(4), [1], 5, 1);
        var secondHash = BatchAt(clock.GetUtcNow(), "sg-1", Hash(5), [1], 5, 2);
        var firstHashAgain = BatchAt(clock.GetUtcNow(), "sg-1", Hash(4), [2], 5, 3);
        hashLimited.Observe([firstHash, secondHash, secondHash, firstHashAgain]);

        var hashes = hashLimited.Snapshot().Current!;
        Assert.Equal(2, hashes.NewPairs);
        Assert.Equal(2, hashes.Bypassed);
        Assert.Equal(1, hashes.ActiveHashes);
        Assert.Equal(2UL, hashes.ActorHours);
        Assert.True(hashes.PartialReasons.HasFlag(HeatActorHourPartialReason.HashCapacity));
    }

    [Fact]
    public void OldAndFutureHoursFailOpenAndClockJumpMarksCoveragePartial()
    {
        var start = Utc(2026, 7, 19, 10, 0);
        var clock = new MutableTimeProvider(start);
        var store = Store(clock);
        var hash = Hash(6);
        store.Observe([
            BatchAt(start, "sg-1", hash, [1], 6, 1),
            BatchAt(start.AddHours(-1), "jp-1", hash, [9], 7, 1)
        ]);
        Assert.Equal(1, store.Snapshot().Previous!.NewPairs);

        clock.Advance(TimeSpan.FromHours(1));
        _ = store.Snapshot();
        store.Observe([
            BatchAt(start.AddHours(-1), "sg-1", hash, [2, 3], 6, 2),
            BatchAt(start.AddHours(2), "jp-1", hash, [4, 5, 6], 6, 3)
        ]);
        var bypassed = store.Snapshot();
        Assert.Equal(2, bypassed.OldHourBypassed);
        Assert.Equal(3, bypassed.FutureHourBypassed);
        Assert.Collection(bypassed.OutOfWindowLosses,
            loss =>
            {
                Assert.Equal(HeatActorHourShadowStore.UnixHour(start.AddHours(-1)), loss.UnixHour);
                Assert.Equal(2, loss.OldRecords);
                Assert.Equal(0, loss.FutureRecords);
            },
            loss =>
            {
                Assert.Equal(HeatActorHourShadowStore.UnixHour(start.AddHours(2)), loss.UnixHour);
                Assert.Equal(0, loss.OldRecords);
                Assert.Equal(3, loss.FutureRecords);
            });

        clock.Advance(TimeSpan.FromHours(3));
        var jumped = store.Snapshot();
        Assert.NotNull(jumped.Previous);
        Assert.True(jumped.Current!.PartialReasons.HasFlag(HeatActorHourPartialReason.ClockGap));
        Assert.True(jumped.Previous!.PartialReasons.HasFlag(HeatActorHourPartialReason.ClockGap));
        Assert.Equal("open-partial", jumped.Current.Coverage);
        Assert.Equal(1, jumped.ClockGapDetections);
        Assert.Equal(2, jumped.ClockGapHours);
    }

    [Fact]
    public void ClockRollbackIsEpisodeBasedAndMarksBothSlotsPartial()
    {
        var start = Utc(2026, 7, 19, 10, 0);
        var clock = new MutableTimeProvider(start);
        var store = Store(clock);

        clock.Advance(TimeSpan.FromHours(1));
        _ = store.Snapshot();
        clock.Advance(TimeSpan.FromHours(-2));

        var first = store.Snapshot();
        Assert.Equal(1, first.ClockRollbackDetections);
        Assert.Equal(2, first.ClockRollbackHours);
        Assert.True(first.Current!.PartialReasons.HasFlag(HeatActorHourPartialReason.ClockRollback));
        Assert.True(first.Previous!.PartialReasons.HasFlag(HeatActorHourPartialReason.ClockRollback));

        var sameEpisode = store.Snapshot();
        Assert.Equal(1, sameEpisode.ClockRollbackDetections);
        Assert.Equal(2, sameEpisode.ClockRollbackHours);

        clock.Advance(TimeSpan.FromHours(2));
        _ = store.Snapshot();
        clock.Advance(TimeSpan.FromHours(-2));
        var second = store.Snapshot();
        Assert.Equal(2, second.ClockRollbackDetections);
        Assert.Equal(4, second.ClockRollbackHours);
    }

    [Fact]
    public async Task ConcurrentObserversRemainLinearizable()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 10, 0));
        var store = Store(clock, pairCapacity: 10_000, hashCapacity: 10);
        var batch = BatchAt(
            clock.GetUtcNow(), "sg-1", Hash(7), Enumerable.Range(1, 100).Select(x => (long)x).ToArray(), 7, 1);

        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() => store.Observe([batch]))));

        var current = store.Snapshot().Current!;
        Assert.Equal(6_400, current.Checks);
        Assert.Equal(100, current.NewPairs);
        Assert.Equal(6_300, current.ProbableDuplicates);
        Assert.Equal(100UL, current.ActorHours);
    }

    [Fact]
    public void ObservationFailureNeverEscapesAndMarksCurrentCoveragePartial()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 10, 0));
        var store = Store(clock);
        var invalid = BatchAt(clock.GetUtcNow(), "sg-1", new byte[19], [1], 7, 1);

        var exception = Record.Exception(() => store.Observe([invalid]));

        Assert.Null(exception);
        var snapshot = store.Snapshot();
        Assert.Equal(1, snapshot.ObservationFailures);
        Assert.Equal(1, snapshot.Current!.Bypassed);
        Assert.True(snapshot.Current.PartialReasons.HasFlag(
            HeatActorHourPartialReason.ObservationFailure));
        Assert.StartsWith("InvalidDataException:", snapshot.LastFailure);
    }

    [Fact]
    public void ExternalLossCumulativeTotalIsIdempotentAndMarksCoveragePartial()
    {
        var clock = new MutableTimeProvider(Utc(2026, 7, 19, 10, 0));
        var store = Store(clock);
        var hour = HeatActorHourShadowStore.UnixHour(clock.GetUtcNow());

        Assert.True(store.TryRecordExternalLossTotal(
            hour, 5, HeatActorHourPartialReason.ObserverQueueLoss));
        Assert.True(store.TryRecordExternalLossTotal(
            hour, 5, HeatActorHourPartialReason.ObserverQueueLoss));
        Assert.True(store.TryRecordExternalLossTotal(
            hour, 8, HeatActorHourPartialReason.ObserverQueueLoss));

        var current = store.Snapshot().Current!;
        Assert.Equal(8, current.DroppedBeforeObserve);
        Assert.Equal(8, current.Bypassed);
        Assert.True(current.PartialReasons.HasFlag(
            HeatActorHourPartialReason.ObserverQueueLoss));
    }

    [Fact]
    public void DisabledStoreAllocatesNoConfiguredTablesAndOptionsBoundHashCapacity()
    {
        var disabled = new HeatActorHourShadowStore(new HeatOptions());
        var snapshot = disabled.Snapshot();
        Assert.False(snapshot.Enabled);
        Assert.Equal(0, snapshot.EstimatedHotPathBytes);
        Assert.True(snapshot.FrameExportMemoryExcluded);

        var normalized = new HeatOptions
        {
            ActorHourShadowEnabled = true,
            ActorHourShadowPairCapacity = 2_000,
            ActorHourShadowHashCapacity = 20_000,
            ActorHourShadowFalsePositivePpm = int.MaxValue
        }.Normalize(Path.GetTempPath());
        Assert.False(normalized.ActorHourShadowEnabled);
        Assert.Equal(2_000, normalized.ActorHourShadowHashCapacity);
        Assert.Equal(100_000, normalized.ActorHourShadowFalsePositivePpm);
    }

    [Fact]
    public async Task AccumulatorShadowObservesOnlyAuthoritativelyAcceptedOrReplayBatches()
    {
        var directory = TemporaryDirectory();
        var now = DateTimeOffset.UtcNow;
        var clock = new MutableTimeProvider(now);
        var options = Options(directory, pairCapacity: 1_000, hashCapacity: 100);
        var shadow = new HeatActorHourShadowStore(options, clock);
        var observer = new HeatActorHourShadowObserver(options, shadow, clock);
        var service = new HeatAccumulatorService(
            options,
            new HeatRuntimeMetrics(),
            NullLogger<HeatAccumulatorService>.Instance,
            actorHourShadow: shadow,
            actorHourObserver: observer);
        var hash = Hash(8);
        var acceptedBatch = BatchAt(now, "sg-1", hash, [1], 8, 1);
        var overlap = BatchAt(now, "jp-1", hash, [1], 9, 1);
        await observer.StartAsync(CancellationToken.None);
        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await service.SubmitAsync(acceptedBatch, CancellationToken.None)).Status);
            Assert.Equal(HeatAcceptStatus.Replay,
                (await service.SubmitAsync(acceptedBatch, CancellationToken.None)).Status);
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await service.SubmitAsync(overlap, CancellationToken.None)).Status);
            Assert.Equal(HeatAcceptStatus.Conflict,
                (await service.SubmitAsync(
                    acceptedBatch with { PayloadSha256 = SHA256.HashData([99]) },
                    CancellationToken.None)).Status);

            await EventuallyAsync(() => observer.Snapshot().ProcessedRecords == 3);
            var current = shadow.Snapshot().Current!;
            Assert.Equal(1, current.NewPairs);
            Assert.Equal(2, current.ProbableDuplicates);
            Assert.Equal(3, current.Checks);

            await using var sqlite = await OpenReadOnlyAsync(service.PathForDay(DateOnly.FromDateTime(now.UtcDateTime)));
            Assert.Equal(1L, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM seen"));
            Assert.Equal(2L, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM receipts"));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            await observer.StopAsync(CancellationToken.None);
            DeleteDirectory(directory);
        }
    }

    private static HeatActorHourShadowStore Store(
        MutableTimeProvider clock,
        int pairCapacity = 1_000,
        int hashCapacity = 100) =>
        new(Options(Path.GetTempPath(), pairCapacity, hashCapacity), clock);

    private static HeatOptions Options(string directory, int pairCapacity, int hashCapacity) => new()
    {
        Enabled = true,
        DataDirectory = directory,
        DailyActorSecret = Convert.ToBase64String(DailySecret),
        ChannelCapacity = 8,
        CommitBatchRequests = 1,
        RollingMaxBytes = 1024L * 1024 * 1024,
        RollingMinFreeBytes = 0,
        ActorHourShadowEnabled = true,
        ActorHourShadowPairCapacity = pairCapacity,
        ActorHourShadowHashCapacity = hashCapacity,
        ActorHourShadowFalsePositivePpm = 1
    };

    private static ChhtBatch BatchAt(
        DateTimeOffset timestamp,
        string crawler,
        byte[] hash,
        IReadOnlyList<long> actors,
        ulong epoch,
        ulong sequence)
    {
        var day = DateOnly.FromDateTime(timestamp.UtcDateTime);
        var groups = new[] { new ChhtHashGroup(hash, actors) };
        return new ChhtBatch(
            crawler,
            day,
            (byte)timestamp.UtcDateTime.Hour,
            epoch,
            sequence,
            checked(sequence + (ulong)actors.Count - 1),
            groups,
            SHA256.HashData([checked((byte)(sequence % 255))]));
    }

    private static byte[] Hash(int value)
    {
        var hash = new byte[20];
        BitConverter.TryWriteBytes(hash, value);
        hash[19] = checked((byte)(value % 251));
        return hash;
    }

    private static DateTimeOffset Utc(int year, int month, int day, int hour, int minute) =>
        new(year, month, day, hour, minute, 0, TimeSpan.Zero);

    private static DateTime UtcDate(long unixHour) =>
        DateTimeOffset.FromUnixTimeSeconds(unixHour * 3_600).UtcDateTime;

    private static string TemporaryDirectory() =>
        Path.Combine(Path.GetTempPath(), $"cherry-actor-hour-test-{Guid.NewGuid():N}");

    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path)) Directory.Delete(path, true);
    }

    private static async Task<SqliteConnection> OpenReadOnlyAsync(string path)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false
        }.ToString());
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<long> ScalarAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync());
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

    private sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        public override DateTimeOffset GetUtcNow() => _utcNow;
        public void Advance(TimeSpan duration) => _utcNow += duration;
    }
}
