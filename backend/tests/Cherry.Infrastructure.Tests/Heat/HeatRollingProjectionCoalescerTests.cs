using Cherry.Infrastructure.Heat;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatRollingProjectionCoalescerTests
{
    [Fact]
    public async Task PartialTailWaitsUntilMaxDelay()
    {
        var time = new MutableTimeProvider(new DateTimeOffset(2026, 7, 19, 0, 0, 0, TimeSpan.Zero));
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        for (var index = 0; index < 20; index++) coalescer.Upsert(Change(index));
        var calls = 0;

        Assert.Equal(0, await coalescer.FlushAsync(false, Flush, CancellationToken.None));
        time.Advance(TimeSpan.FromSeconds(44));
        Assert.Equal(0, await coalescer.FlushAsync(false, Flush, CancellationToken.None));
        Assert.Equal(0, calls);
        Assert.Equal(20, coalescer.Count);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(20, await coalescer.FlushAsync(false, Flush, CancellationToken.None));
        Assert.Equal(1, calls);
        Assert.Equal(0, coalescer.Count);

        Task<bool> Flush(
            long targetHour,
            IReadOnlyList<RollingProjectionPendingChange> changes,
            CancellationToken cancellationToken)
        {
            calls++;
            Assert.Equal(123, targetHour);
            Assert.Equal(20, changes.Count);
            return Task.FromResult(true);
        }
    }

    [Fact]
    public async Task ContinuousSmallWritesDoNotResetOldestDeadline()
    {
        var time = new MutableTimeProvider(new DateTimeOffset(2026, 7, 19, 0, 0, 0, TimeSpan.Zero));
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        coalescer.Upsert(Change(1));

        for (var id = 2; id <= 5; id++)
        {
            time.Advance(TimeSpan.FromSeconds(10));
            coalescer.Upsert(Change(id));
        }
        coalescer.Upsert(Change(1, revision: 2));

        Assert.False(coalescer.IsFlushDue);
        time.Advance(TimeSpan.FromSeconds(5));
        Assert.True(coalescer.IsFlushDue);

        IReadOnlyList<RollingProjectionPendingChange>? submitted = null;
        Assert.Equal(5, await coalescer.FlushAsync(
            false,
            (_, changes, _) =>
            {
                submitted = changes;
                return Task.FromResult(true);
            },
            CancellationToken.None));
        Assert.Equal(2, submitted!.Single(change => change.Id == 1).Revision);
    }

    [Fact]
    public async Task FullBatchFlushesImmediately()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        for (var index = 0; index < 500; index++) coalescer.Upsert(Change(index));

        Assert.True(coalescer.IsFlushDue);
        Assert.Equal(500, await coalescer.FlushAsync(
            false,
            (_, changes, _) => Task.FromResult(changes.Count == 500),
            CancellationToken.None));
        Assert.Equal(0, coalescer.Count);
    }

    [Fact]
    public void FullBatchIsAHardAdmissionBoundAfterFailedFlush()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        for (var index = 0; index < 500; index++) coalescer.Upsert(Change(index));

        Assert.Throws<InvalidOperationException>(() => coalescer.Upsert(Change(501)));
        coalescer.Upsert(Change(499, revision: 2));
        Assert.Equal(500, coalescer.Count);
    }

    [Fact]
    public async Task ReplayedFullBatchRefreshesRevisionsAndSubmitsOnlyOnce()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        for (var index = 0; index < 500; index++) coalescer.Upsert(Change(index));

        var replayed = Enumerable.Range(0, 500)
            .Select(index => Change(index, revision: 2))
            .Where(change => !coalescer.TryUpdateExisting(change))
            .ToArray();

        Assert.Empty(replayed);
        var calls = 0;
        Assert.Equal(500, await coalescer.FlushAsync(
            false,
            (_, changes, _) =>
            {
                calls++;
                Assert.All(changes, change => Assert.Equal(2, change.Revision));
                return Task.FromResult(true);
            },
            CancellationToken.None));
        Assert.Equal(1, calls);
        Assert.Equal(0, coalescer.Count);
    }

    [Fact]
    public async Task ExpiredPartialCanBeFilledBeforeItsSingleFlush()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        for (var index = 0; index < 200; index++) coalescer.Upsert(Change(index));
        time.Advance(TimeSpan.FromSeconds(45));
        Assert.True(coalescer.IsFlushDue);

        for (var index = 200; index < 500; index++)
            coalescer.Upsert(Change(index));

        var calls = 0;
        Assert.Equal(500, await coalescer.FlushAsync(
            false,
            (_, changes, _) =>
            {
                calls++;
                Assert.Equal(500, changes.Count);
                return Task.FromResult(true);
            },
            CancellationToken.None));
        Assert.Equal(1, calls);
        Assert.Equal(0, coalescer.Count);
    }

    [Fact]
    public async Task FailedOrRejectedFlushRetainsPendingForReplay()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        coalescer.Upsert(Change(1));

        await Assert.ThrowsAsync<InvalidOperationException>(() => coalescer.FlushAsync(
            true,
            (_, _, _) => throw new InvalidOperationException("remote task failed"),
            CancellationToken.None));
        Assert.Equal(1, coalescer.Count);

        Assert.Equal(0, await coalescer.FlushAsync(
            true,
            (_, _, _) => Task.FromResult(false),
            CancellationToken.None));
        Assert.Equal(1, coalescer.Count);

        Assert.Equal(1, await coalescer.FlushAsync(
            true,
            (_, _, _) => Task.FromResult(true),
            CancellationToken.None));
        Assert.Equal(0, coalescer.Count);
    }

    [Fact]
    public async Task CancellationRetainsPendingForRestartReplay()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        coalescer.Upsert(Change(1));
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => coalescer.FlushAsync(
            true,
            (_, _, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult(true);
            },
            stopping.Token));
        Assert.Equal(1, coalescer.Count);
    }

    [Fact]
    public async Task SuccessfulFlushDoesNotRemoveNewerRevisionAddedDuringCallback()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        coalescer.Upsert(Change(1));

        Assert.Equal(0, await coalescer.FlushAsync(
            true,
            (_, _, _) =>
            {
                coalescer.Upsert(Change(1, revision: 2));
                return Task.FromResult(true);
            },
            CancellationToken.None));
        Assert.Equal(1, coalescer.Count);

        Assert.Equal(1, await coalescer.FlushAsync(
            true,
            (_, changes, _) => Task.FromResult(changes.Single().Revision == 2),
            CancellationToken.None));
    }

    [Fact]
    public void WindowOrRecoveryChangeDropsOnlyVolatilePendingState()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        Assert.True(coalescer.BeginWindow(123, 0));
        coalescer.Upsert(Change(1));

        Assert.False(coalescer.BeginWindow(123, 0));
        Assert.Equal(1, coalescer.Count);
        Assert.True(coalescer.BeginWindow(123, 1));
        Assert.Equal(0, coalescer.Count);

        coalescer.Upsert(Change(2));
        Assert.True(coalescer.BeginWindow(124, 1));
        Assert.Equal(0, coalescer.Count);
        Assert.Equal(124, coalescer.TargetHour);
    }

    [Fact]
    public void NextDelayWakesIdlePollAtBatchDeadline()
    {
        var time = new MutableTimeProvider(DateTimeOffset.UtcNow);
        var coalescer = NewCoalescer(time);
        coalescer.BeginWindow(123, 0);
        coalescer.Upsert(Change(1));

        Assert.Equal(TimeSpan.FromSeconds(30), coalescer.GetNextDelay(TimeSpan.FromSeconds(30)));
        time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(15), coalescer.GetNextDelay(TimeSpan.FromSeconds(30)));
        time.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(TimeSpan.Zero, coalescer.GetNextDelay(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void RollingProjectionMaxDelayIsNormalized()
    {
        var low = new HeatOptions { RollingProjectionMaxDelaySeconds = 0 }
            .Normalize(Path.GetTempPath());
        var high = new HeatOptions { RollingProjectionMaxDelaySeconds = 1000 }
            .Normalize(Path.GetTempPath());

        Assert.Equal(1, low.RollingProjectionMaxDelaySeconds);
        Assert.Equal(300, high.RollingProjectionMaxDelaySeconds);
    }

    private static HeatRollingProjectionCoalescer NewCoalescer(TimeProvider time) =>
        new(500, TimeSpan.FromSeconds(45), time);

    private static RollingProjectionPendingChange Change(long id, long revision = 1)
    {
        var hash = new byte[20];
        BitConverter.TryWriteBytes(hash, id);
        return new RollingProjectionPendingChange(id, hash, id + revision, id, revision);
    }

    private sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;

        public void Advance(TimeSpan value) => utcNow += value;
    }
}
