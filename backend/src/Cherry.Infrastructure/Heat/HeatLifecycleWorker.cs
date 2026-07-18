using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Infrastructure.Heat;

public sealed class HeatLifecycleWorker : BackgroundService
{
    private readonly HeatOptions _options;
    private readonly HeatAccumulatorService _accumulator;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly HeatRuntimeMetrics _metrics;
    private readonly ILogger<HeatLifecycleWorker> _logger;

    public HeatLifecycleWorker(
        HeatOptions options,
        HeatAccumulatorService accumulator,
        IServiceScopeFactory scopeFactory,
        HeatRuntimeMetrics metrics,
        ILogger<HeatLifecycleWorker> logger)
    {
        _options = options;
        _accumulator = accumulator;
        _scopeFactory = scopeFactory;
        _metrics = metrics;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SealEligibleDaysAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception exception)
            {
                _metrics.Fail(exception);
                _logger.LogError(exception, "Heat day sealing pass failed");
            }
            await Task.Delay(TimeSpan.FromSeconds(_options.LifecyclePollSeconds), stoppingToken);
        }
    }

    public async Task SealEligibleDaysAsync(CancellationToken cancellationToken)
    {
        var start = _options.ParsedCoverageStartDay
            ?? throw new InvalidOperationException("Heat:CoverageStartDay is required when heat is enabled");
        var now = DateTime.UtcNow;
        var today = DateOnly.FromDateTime(now);
        var latest = now <= now.Date.AddMinutes(_options.LateGraceMinutes)
            ? today.AddDays(-2)
            : today.AddDays(-1);
        await using var scope = _scopeFactory.CreateAsyncScope();
        var sealer = scope.ServiceProvider.GetRequiredService<HeatDaySealer>();
        var missing = await sealer.MissingDaysAsync(start, latest, 4, cancellationToken);
        foreach (var day in missing)
        {
            if (!await _accumulator.SealBarrierAsync(day, cancellationToken)) return;
            var path = _accumulator.PathForDay(day);
            try
            {
                await sealer.SealAsync(day, path, cancellationToken);
                HeatDaySealer.DeleteAccumulator(path);
                // Coverage completeness is stored in the manifest; the day may
                // intentionally be partial when one required crawler was absent.
                _logger.LogInformation("Sealed UTC heat day {Day}", day);
            }
            catch
            {
                _accumulator.AllowSealRetry(day);
                throw;
            }
        }
    }
}
