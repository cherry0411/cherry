namespace Cherry.Infrastructure.Search;

internal readonly record struct SearchOutboxCoalescingDecision(
    bool Dispatch,
    TimeSpan Delay);

/// <summary>
/// Keeps search projection tasks full near the outbox tail without delaying a
/// real backlog. The deadline is anchored to the oldest durable enqueue time,
/// so process restarts cannot reset the freshness bound.
/// </summary>
internal static class SearchOutboxCoalescer
{
    public static SearchOutboxCoalescingDecision Decide(
        SearchOutboxOptions options,
        SearchOutboxReadyWindow window,
        DateTime utcNow)
    {
        if (!options.CoalescingEnabled ||
            window.Count >= options.CoalescingMinBatchSize ||
            window.OldestEnqueuedAt is null)
        {
            return new SearchOutboxCoalescingDecision(true, TimeSpan.Zero);
        }

        var age = utcNow - window.OldestEnqueuedAt.Value;
        if (age < TimeSpan.Zero)
            age = TimeSpan.Zero;
        var remaining = options.CoalescingMaxQueueDelay - age;
        if (remaining <= TimeSpan.Zero)
            return new SearchOutboxCoalescingDecision(true, TimeSpan.Zero);

        var delay = remaining < options.CoalescingPollInterval
            ? remaining
            : options.CoalescingPollInterval;
        return new SearchOutboxCoalescingDecision(false, delay);
    }
}
