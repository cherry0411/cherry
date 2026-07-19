using Cherry.Infrastructure.Search;
using Xunit;

namespace Cherry.Infrastructure.Tests.Search;

public sealed class SearchOutboxCoalescerTests
{
    private static readonly DateTime Now = new(2026, 7, 19, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Disabled_DispatchesPartialBatchImmediately()
    {
        var options = Options(enabled: false);

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(1, Now),
            Now);

        Assert.True(decision.Dispatch);
        Assert.Equal(TimeSpan.Zero, decision.Delay);
    }

    [Fact]
    public void FullLowWaterWindow_DispatchesWithoutDelay()
    {
        var options = Options();

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(options.CoalescingMinBatchSize, Now),
            Now);

        Assert.True(decision.Dispatch);
        Assert.Equal(TimeSpan.Zero, decision.Delay);
    }

    [Fact]
    public void PartialFreshWindow_WaitsOnlyOnePoll()
    {
        var options = Options();

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(12, Now.AddSeconds(-10)),
            Now);

        Assert.False(decision.Dispatch);
        Assert.Equal(TimeSpan.FromMilliseconds(500), decision.Delay);
    }

    [Fact]
    public void FinalPoll_IsClampedToDurableFreshnessDeadline()
    {
        var options = Options();

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(12, Now.AddMilliseconds(-29_750)),
            Now);

        Assert.False(decision.Dispatch);
        Assert.Equal(TimeSpan.FromMilliseconds(250), decision.Delay);
    }

    [Theory]
    [InlineData(30)]
    [InlineData(45)]
    public void PartialExpiredWindow_DispatchesAtOrAfterDeadline(int ageSeconds)
    {
        var options = Options();

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(12, Now.AddSeconds(-ageSeconds)),
            Now);

        Assert.True(decision.Dispatch);
        Assert.Equal(TimeSpan.Zero, decision.Delay);
    }

    [Fact]
    public void FutureClockSkew_DoesNotExtendBeyondConfiguredDelay()
    {
        var options = Options();

        var decision = SearchOutboxCoalescer.Decide(
            options,
            new SearchOutboxReadyWindow(12, Now.AddSeconds(5)),
            Now);

        Assert.False(decision.Dispatch);
        Assert.Equal(TimeSpan.FromMilliseconds(500), decision.Delay);
    }

    private static SearchOutboxOptions Options(bool enabled = true) =>
        new SearchOutboxOptions
        {
            BatchSize = 500,
            CoalescingEnabled = enabled,
            CoalescingMinBatchSize = 400,
            CoalescingMaxQueueDelay = TimeSpan.FromSeconds(30),
            CoalescingPollInterval = TimeSpan.FromMilliseconds(500)
        }.Normalize();
}
