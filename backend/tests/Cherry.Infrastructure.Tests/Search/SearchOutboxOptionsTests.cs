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
            PollInterval = TimeSpan.FromSeconds(1),
            TaskTimeout = TimeSpan.FromSeconds(10),
            LeaseDuration = TimeSpan.FromSeconds(1),
            IdleDelay = TimeSpan.Zero,
            RetryBaseDelay = TimeSpan.Zero,
            RetryMaxDelay = TimeSpan.FromMilliseconds(1)
        }.Normalize();

        Assert.Equal(1, normalized.BatchSize);
        Assert.True(normalized.LeaseDuration >= normalized.TaskTimeout + TimeSpan.FromSeconds(30));
        Assert.True(normalized.IdleDelay > TimeSpan.Zero);
        Assert.True(normalized.RetryBaseDelay > TimeSpan.Zero);
        Assert.True(normalized.RetryMaxDelay >= normalized.RetryBaseDelay);
    }
}
