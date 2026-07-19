using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Infrastructure.Search;

public sealed class SearchOutboxOptions
{
    public int BatchSize { get; init; } = 500;
    public bool CoalescingEnabled { get; init; }
    public int CoalescingMinBatchSize { get; init; } = 500;
    public TimeSpan CoalescingMaxQueueDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan CoalescingPollInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan TaskTimeout { get; init; } = TimeSpan.FromMinutes(2);
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    public SearchOutboxOptions Normalize()
    {
        var poll = ClampPositive(
            PollInterval,
            TimeSpan.FromMilliseconds(250),
            TimeSpan.FromSeconds(5));
        var taskTimeout = ClampPositive(
            TaskTimeout,
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(30));
        var leaseMargin = TimeSpan.FromTicks(Math.Max(
            TimeSpan.FromSeconds(30).Ticks,
            poll.Ticks * 2));
        var minimumLease = taskTimeout + leaseMargin;
        var requestedLease = ClampPositive(
            LeaseDuration,
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1));
        var retryBase = ClampPositive(
            RetryBaseDelay,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMinutes(5));
        var retryMax = ClampPositive(
            RetryMaxDelay,
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1));
        var batchSize = Math.Clamp(BatchSize, 1, 5_000);
        return new SearchOutboxOptions
        {
            BatchSize = batchSize,
            CoalescingEnabled = CoalescingEnabled,
            CoalescingMinBatchSize = Math.Clamp(CoalescingMinBatchSize, 1, batchSize),
            CoalescingMaxQueueDelay = ClampPositive(
                CoalescingMaxQueueDelay,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(5)),
            CoalescingPollInterval = ClampPositive(
                CoalescingPollInterval,
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromSeconds(5)),
            PollInterval = poll,
            TaskTimeout = taskTimeout,
            LeaseDuration = requestedLease < minimumLease ? minimumLease : requestedLease,
            IdleDelay = ClampPositive(
                IdleDelay,
                TimeSpan.FromSeconds(2),
                TimeSpan.FromMinutes(1)),
            RetryBaseDelay = retryBase,
            RetryMaxDelay = retryMax < retryBase ? retryBase : retryMax
        };
    }

    private static TimeSpan ClampPositive(TimeSpan value, TimeSpan fallback, TimeSpan maximum) =>
        value <= TimeSpan.Zero ? fallback : value > maximum ? maximum : value;
}

public sealed record SearchOutboxRuntimeMetrics(
    long CompletedDocuments,
    long CompletedTasks,
    long Retries,
    long FailedTasks,
    long CoalescedDispatches,
    double CoalescingWaitSeconds,
    int LastBatchDocuments,
    double LastTaskLatencySeconds,
    string? LastError);

public sealed class SearchOutboxMetrics
{
    private long _completedDocuments;
    private long _completedTasks;
    private long _retries;
    private long _failedTasks;
    private long _coalescedDispatches;
    private long _coalescingWaitTicks;
    private int _lastBatchDocuments;
    private long _lastTaskLatencyTicks;
    private string? _lastError;

    public void RecordSuccess(int documents, TimeSpan taskLatency)
    {
        Interlocked.Add(ref _completedDocuments, documents);
        Interlocked.Increment(ref _completedTasks);
        Interlocked.Exchange(ref _lastBatchDocuments, documents);
        Interlocked.Exchange(ref _lastTaskLatencyTicks, taskLatency.Ticks);
        Volatile.Write(ref _lastError, null);
    }

    public void RecordCoalescedDispatch(TimeSpan wait)
    {
        if (wait <= TimeSpan.Zero)
            return;
        Interlocked.Increment(ref _coalescedDispatches);
        Interlocked.Add(ref _coalescingWaitTicks, wait.Ticks);
    }

    public void RecordFailure(string error, bool failedTask)
    {
        Interlocked.Increment(ref _retries);
        if (failedTask)
            Interlocked.Increment(ref _failedTasks);
        Volatile.Write(ref _lastError, error);
    }

    public SearchOutboxRuntimeMetrics Snapshot() => new(
        Interlocked.Read(ref _completedDocuments),
        Interlocked.Read(ref _completedTasks),
        Interlocked.Read(ref _retries),
        Interlocked.Read(ref _failedTasks),
        Interlocked.Read(ref _coalescedDispatches),
        TimeSpan.FromTicks(Interlocked.Read(ref _coalescingWaitTicks)).TotalSeconds,
        Volatile.Read(ref _lastBatchDocuments),
        TimeSpan.FromTicks(Interlocked.Read(ref _lastTaskLatencyTicks)).TotalSeconds,
        Volatile.Read(ref _lastError));
}

/// <summary>
/// Projects PostgreSQL-authoritative torrents into Meilisearch. A claim is
/// acknowledged only after the asynchronous Meilisearch task succeeds.
/// </summary>
public sealed class SearchOutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly MeiliSearchClient _client;
    private readonly SearchOutboxOptions _options;
    private readonly SearchOutboxMetrics _metrics;
    private readonly ILogger<SearchOutboxWorker> _logger;
    private readonly SearchRecoveryCoordinator _recoveryCoordinator;

    public SearchOutboxWorker(
        IServiceScopeFactory scopeFactory,
        MeiliSearchClient client,
        SearchOutboxOptions options,
        SearchOutboxMetrics metrics,
        ILogger<SearchOutboxWorker> logger,
        SearchRecoveryCoordinator? recoveryCoordinator = null)
    {
        _scopeFactory = scopeFactory;
        _client = client;
        _options = options.Normalize();
        _metrics = metrics;
        _logger = logger;
        _recoveryCoordinator = recoveryCoordinator ?? new SearchRecoveryCoordinator();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Search outbox coalescing enabled={Enabled}, min batch={MinBatch}, max queue delay={MaxQueueDelay}, poll={PollInterval}",
            _options.CoalescingEnabled,
            _options.CoalescingMinBatchSize,
            _options.CoalescingMaxQueueDelay,
            _options.CoalescingPollInterval);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await ProcessOnceAsync(stoppingToken);
                if (processed == 0)
                    await Task.Delay(_options.IdleDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Search outbox loop failed before a batch could be settled");
                await Task.Delay(_options.RetryBaseDelay, stoppingToken);
            }
        }
    }

    public async Task<int> ProcessOnceAsync(CancellationToken cancellationToken = default)
    {
        var coalescing = await AwaitCoalescingWindowAsync(cancellationToken);
        if (!coalescing.Ready)
            return 0;

        await using var projection =
            await _recoveryCoordinator.EnterProjectionAsync(cancellationToken);
        var owner = Guid.NewGuid();
        List<SearchOutboxClaim> claims;
        List<Cherry.Domain.Entities.Torrent> documents;
        await using (var scope = _scopeFactory.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
            claims = await store.ClaimAsync(
                owner,
                _options.BatchSize,
                _options.LeaseDuration,
                cancellationToken);
            if (claims.Count == 0)
                return 0;
            _metrics.RecordCoalescedDispatch(coalescing.Waited);
            documents = await store.LoadDocumentsAsync(claims, cancellationToken);
        }

        if (documents.Count == 0)
        {
            await CompleteAsync(owner, claims, cancellationToken);
            return claims.Count;
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var taskUid = await _client.SubmitDocumentsAsync(documents, cancellationToken);
            var state = await WaitForCompletionAsync(taskUid, cancellationToken);
            if (!string.Equals(state.Status, "succeeded", StringComparison.OrdinalIgnoreCase))
                throw new MeiliTaskFailedException(taskUid, state.Error ?? state.Status);

            var completed = await CompleteAsync(owner, claims, cancellationToken);
            _metrics.RecordSuccess(completed, stopwatch.Elapsed);
            _logger.LogInformation(
                "Meilisearch task {TaskUid} succeeded; acknowledged {Count}/{Claimed} outbox rows",
                taskUid,
                completed,
                claims.Count);
            return claims.Count;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Do not convert shutdown into a retry. The bounded lease makes the
            // entire claim recoverable by this or another worker.
            throw;
        }
        catch (Exception exception)
        {
            var failedTask = exception is MeiliTaskFailedException;
            var error = exception.Message;
            var delay = RetryDelay(claims.Max(claim => claim.AttemptCount) + 1);
            await using var scope = _scopeFactory.CreateAsyncScope();
            var store = scope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
            var retained = await store.FailAsync(
                owner,
                claims,
                error,
                delay,
                cancellationToken);
            _metrics.RecordFailure(error, failedTask);
            _logger.LogWarning(
                exception,
                "Meilisearch delivery failed; retained {Count}/{Claimed} rows for retry in {Delay}",
                retained,
                claims.Count,
                delay);
            return claims.Count;
        }
    }

    private async Task<CoalescingOutcome> AwaitCoalescingWindowAsync(
        CancellationToken cancellationToken)
    {
        if (!_options.CoalescingEnabled)
            return new CoalescingOutcome(true, TimeSpan.Zero);

        var waited = TimeSpan.Zero;
        while (true)
        {
            SearchOutboxReadyWindow window;
            await using (var scope = _scopeFactory.CreateAsyncScope())
            {
                var store = scope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
                window = await store.GetReadyWindowAsync(
                    _options.CoalescingMinBatchSize,
                    cancellationToken);
            }

            if (window.Count == 0)
                return new CoalescingOutcome(false, waited);

            var decision = SearchOutboxCoalescer.Decide(_options, window, DateTime.UtcNow);
            if (decision.Dispatch)
                return new CoalescingOutcome(true, waited);

            await Task.Delay(decision.Delay, cancellationToken);
            waited += decision.Delay;
        }
    }

    private async Task<MeiliTaskState> WaitForCompletionAsync(
        long taskUid,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + _options.TaskTimeout;
        while (true)
        {
            var state = await _client.GetTaskAsync(taskUid, cancellationToken);
            if (string.Equals(state.Status, "succeeded", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(state.Status, "failed", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(state.Status, "canceled", StringComparison.OrdinalIgnoreCase))
                return state;
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Meilisearch task {taskUid} did not finish before the timeout");
            await Task.Delay(_options.PollInterval, cancellationToken);
        }
    }

    private async Task<int> CompleteAsync(
        Guid owner,
        IReadOnlyCollection<SearchOutboxClaim> claims,
        CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<SearchOutboxStore>();
        return await store.CompleteAsync(owner, claims, cancellationToken);
    }

    private TimeSpan RetryDelay(int attempt)
    {
        var factor = Math.Pow(2, Math.Clamp(attempt - 1, 0, 16));
        var ticks = Math.Min(
            _options.RetryMaxDelay.Ticks,
            _options.RetryBaseDelay.Ticks * factor);
        return TimeSpan.FromTicks((long)ticks);
    }

    private sealed class MeiliTaskFailedException(long taskUid, string error)
        : Exception($"Meilisearch task {taskUid} failed: {error}");

    private readonly record struct CoalescingOutcome(bool Ready, TimeSpan Waited);
}
