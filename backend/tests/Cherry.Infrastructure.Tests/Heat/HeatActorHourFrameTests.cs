using Cherry.Infrastructure.Heat;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatActorHourFrameTests
{
    [Fact]
    public void CompactCodecRoundTripsAndRejectsCorruption()
    {
        var frame = Frame(500_000);
        var encoded = HeatActorHourFrameCodec.Encode(frame);
        var decoded = HeatActorHourFrameCodec.Decode(encoded);

        Assert.Equal(frame.UnixHour, decoded.UnixHour);
        Assert.Equal(HeatActorHourFrameState.Sealed, decoded.State);
        Assert.Equal(HeatActorHourCoverage.Partial, decoded.Coverage);
        Assert.Equal(frame.PartialReasons, decoded.PartialReasons);
        Assert.Equal(frame.FalsePositivePpm, decoded.FalsePositivePpm);
        Assert.Equal(frame.NewPairs, decoded.NewPairs);
        Assert.Collection(decoded.Entries,
            entry =>
            {
                Assert.Equal(frame.Entries[0].InfoHash, entry.InfoHash);
                Assert.Equal(3U, entry.Count);
            },
            entry =>
            {
                Assert.Equal(frame.Entries[1].InfoHash, entry.InfoHash);
                Assert.Equal(300U, entry.Count);
            });
        Assert.True(encoded.Length < 60 + 32 + 2 * (20 + 5));

        encoded[20] ^= 0x40;
        Assert.Throws<InvalidDataException>(() => HeatActorHourFrameCodec.Decode(encoded));
        Assert.Throws<InvalidDataException>(() => HeatActorHourFrameCodec.Encode(
            frame with { Entries = frame.Entries.Reverse().ToArray() }));
    }

    [Fact]
    public async Task FileSinkPublishesAtomicallyAndRejectsConflictingHour()
    {
        var directory = Path.Combine(
            Path.GetTempPath(), $"cherry-actor-hour-frame-{Guid.NewGuid():N}");
        var sink = new FileHeatActorHourFrameSink(directory);
        var frame = Frame(500_001);
        try
        {
            await sink.WriteAsync(frame, CancellationToken.None);
            await sink.WriteAsync(frame, CancellationToken.None);

            var files = Directory.GetFiles(directory);
            Assert.Single(files);
            Assert.EndsWith(".chah", files[0]);
            var decoded = await FileHeatActorHourFrameSink.ReadAsync(files[0], CancellationToken.None);
            Assert.Equal(frame.UnixHour, decoded.UnixHour);
            Assert.Equal(2, decoded.Entries.Count);
            Assert.Empty(Directory.GetFiles(directory, "*.tmp"));

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await sink.WriteAsync(frame with { NewPairs = frame.NewPairs + 1 }, CancellationToken.None));
            var unchanged = await FileHeatActorHourFrameSink.ReadAsync(files[0], CancellationToken.None);
            Assert.Equal(frame.NewPairs, unchanged.NewPairs);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task ProvisionalCoverageCannotBePublishedOrMasqueradeAsComplete()
    {
        var directory = Path.Combine(
            Path.GetTempPath(), $"cherry-actor-hour-frame-{Guid.NewGuid():N}");
        var provisional = Frame(500_002) with
        {
            State = HeatActorHourFrameState.Provisional,
            Coverage = HeatActorHourCoverage.Unknown,
            PartialReasons = HeatActorHourPartialReason.None
        };
        var decoded = HeatActorHourFrameCodec.Decode(
            HeatActorHourFrameCodec.Encode(provisional));
        Assert.Equal(HeatActorHourCoverage.Unknown, decoded.Coverage);

        var sink = new FileHeatActorHourFrameSink(directory);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await sink.WriteAsync(provisional, CancellationToken.None));
        Assert.False(Directory.Exists(directory));
        Assert.Throws<InvalidDataException>(() => HeatActorHourFrameCodec.Encode(
            provisional with { Coverage = HeatActorHourCoverage.Complete }));
    }

    [Fact]
    public void StoreFrameContainsSortedRawHashesAndObservableCoverage()
    {
        var now = DateTimeOffset.UtcNow;
        var clock = new FixedTimeProvider(now);
        var options = new HeatOptions
        {
            Enabled = true,
            ActorHourShadowEnabled = true,
            ActorHourShadowPairCapacity = 1_000,
            ActorHourShadowHashCapacity = 100,
            ActorHourShadowFalsePositivePpm = 1
        };
        var store = new HeatActorHourShadowStore(options, clock);
        var high = Enumerable.Repeat((byte)0xff, 20).ToArray();
        var low = new byte[20];
        store.Observe([
            Batch(now, "sg-1", high, 1),
            Batch(now, "jp-1", low, 2)
        ]);

        Assert.True(store.TryCreateFrameSnapshot(
            HeatActorHourShadowStore.UnixHour(now), out var frame));
        Assert.NotNull(frame);
        Assert.Equal(HeatActorHourFrameState.Open, frame!.State);
        Assert.Equal(HeatActorHourCoverage.Partial, frame.Coverage);
        Assert.Equal(low, frame.Entries[0].InfoHash);
        Assert.Equal(high, frame.Entries[1].InfoHash);
        Assert.True(frame.PartialReasons.HasFlag(HeatActorHourPartialReason.OpenHour));
        Assert.True(frame.PartialReasons.HasFlag(HeatActorHourPartialReason.ProcessRestart));
        _ = HeatActorHourFrameCodec.Decode(HeatActorHourFrameCodec.Encode(frame));
    }

    private static HeatActorHourFrame Frame(long hour)
    {
        var first = new byte[20];
        first[19] = 1;
        var second = (byte[])first.Clone();
        second[19] = 2;
        return new HeatActorHourFrame(
            hour,
            HeatActorHourFrameState.Sealed,
            HeatActorHourCoverage.Partial,
            HeatActorHourPartialReason.ProcessRestart,
            1_000,
            1_000_000,
            303,
            10,
            2,
            [new HeatActorHourCount(first, 3), new HeatActorHourCount(second, 300)]);
    }

    private static ChhtBatch Batch(DateTimeOffset now, string crawler, byte[] hash, long actor) =>
        new(
            crawler,
            DateOnly.FromDateTime(now.UtcDateTime),
            (byte)now.UtcDateTime.Hour,
            1,
            1,
            1,
            [new ChhtHashGroup(hash, [actor])],
            new byte[32]);

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
