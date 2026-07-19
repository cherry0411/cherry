using Cherry.Infrastructure.Search;
using Xunit;

namespace Cherry.Infrastructure.Tests.Search;

public sealed class SearchOutboxOptionsTests
{
    [Fact]
    public void Normalize_ClampsBatchAndKeepsLeaseBeyondTaskDeadline()
    {
        var normalized = new SearchOutboxOptions
        {
            BatchSize = -1,
            CoalescingEnabled = true,
            CoalescingMinBatchSize = 10_000,
            CoalescingMaxQueueDelay = TimeSpan.Zero,
            CoalescingPollInterval = TimeSpan.Zero,
            PollInterval = TimeSpan.FromSeconds(1),
            TaskTimeout = TimeSpan.FromSeconds(10),
            LeaseDuration = TimeSpan.FromSeconds(1),
            IdleDelay = TimeSpan.Zero,
            RetryBaseDelay = TimeSpan.Zero,
            RetryMaxDelay = TimeSpan.FromMilliseconds(1)
        }.Normalize();

        Assert.Equal(1, normalized.BatchSize);
        Assert.True(normalized.CoalescingEnabled);
        Assert.Equal(normalized.BatchSize, normalized.CoalescingMinBatchSize);
        Assert.True(normalized.CoalescingMaxQueueDelay > TimeSpan.Zero);
        Assert.True(normalized.CoalescingPollInterval > TimeSpan.Zero);
        Assert.True(normalized.LeaseDuration >= normalized.TaskTimeout + TimeSpan.FromSeconds(30));
        Assert.True(normalized.IdleDelay > TimeSpan.Zero);
        Assert.True(normalized.RetryBaseDelay > TimeSpan.Zero);
        Assert.True(normalized.RetryMaxDelay >= normalized.RetryBaseDelay);
    }

    [Fact]
    public void Defaults_KeepCoalescingDisabledForSafeDeployment()
    {
        var normalized = new SearchOutboxOptions().Normalize();

        Assert.False(normalized.CoalescingEnabled);
        Assert.Equal(500, normalized.CoalescingMinBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(30), normalized.CoalescingMaxQueueDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(500), normalized.CoalescingPollInterval);
    }

    [Fact]
    public void Metrics_ExposeBatchFillAndCoalescingCost()
    {
        var metrics = new SearchOutboxMetrics();

        metrics.RecordCoalescedDispatch(TimeSpan.FromSeconds(4));
        metrics.RecordSuccess(420, TimeSpan.FromSeconds(2));
        var snapshot = metrics.Snapshot();

        Assert.Equal(1, snapshot.CompletedTasks);
        Assert.Equal(420, snapshot.CompletedDocuments);
        Assert.Equal(420, snapshot.LastBatchDocuments);
        Assert.Equal(1, snapshot.CoalescedDispatches);
        Assert.Equal(4, snapshot.CoalescingWaitSeconds);
    }
}
