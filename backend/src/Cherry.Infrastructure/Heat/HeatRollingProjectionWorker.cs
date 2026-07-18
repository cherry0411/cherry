using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Heat;

/// <summary>
/// Once per UTC hour projects exact unique actors whose last observation is in
/// the current 24 hourly buckets. Only the final integer enters Meilisearch;
/// PostgreSQL and backups never receive the stable rolling actor token.
/// </summary>
public sealed class HeatRollingProjectionWorker : BackgroundService
{
    private readonly HeatRollingStore _store;
    private readonly IServiceScopeFactory _scopes;
    private readonly MeiliSearchClient _meili;
    private readonly HeatOptions _options;
    private readonly HeatRuntimeMetrics _metrics;
    private readonly ILogger<HeatRollingProjectionWorker> _logger;
    private readonly SearchRecoveryCoordinator _recovery;

    public HeatRollingProjectionWorker(
        HeatRollingStore store,
        IServiceScopeFactory scopes,
        MeiliSearchClient meili,
        HeatOptions options,
        HeatRuntimeMetrics metrics,
        ILogger<HeatRollingProjectionWorker> logger,
        SearchRecoveryCoordinator recovery)
    {
        _store = store;
        _scopes = scopes;
        _meili = meili;
        _options = options;
        _metrics = metrics;
        _logger = logger;
        _recovery = recovery;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // A restart or outage makes the active hour incomplete. Coverage grows
        // again only after the next fully observed UTC hour.
        await _store.MarkRuntimeStartAsync(
            HeatRollingStore.UnixHour(DateTime.UtcNow) + 1,
            stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception)
            {
                _metrics.Fail(exception, "rolling-projection");
                _logger.LogError(exception, "Rolling 24h heat projection failed");
            }
            await Task.Delay(TimeSpan.FromSeconds(_options.LifecyclePollSeconds), stoppingToken);
        }
    }

    public async Task<bool> ProcessOnceAsync(CancellationToken cancellationToken = default)
    {
        var progressed = await ProcessOnceCoreAsync(cancellationToken);
        _metrics.ClearFailure("rolling-projection");
        return progressed;
    }

    private async Task<bool> ProcessOnceCoreAsync(CancellationToken cancellationToken)
    {
        // The recovery lease must cover the snapshot itself. Otherwise a
        // worker can read pre-reset state, wait behind destructive recovery,
        // then commit that stale snapshot into the freshly recreated index.
        await using var projection = await _recovery.EnterProjectionAsync(cancellationToken);
        while (true)
        {
            // Only complete UTC buckets are eligible. A large backlog can span
            // an hour boundary; abandon its old cursor immediately because the
            // per-actor top-two hours cannot reconstruct target+2 exactly.
            var targetHour = ClosedTargetHour();
            await _store.PrepareForProjectionAsync(targetHour, cancellationToken);
            if (ClosedTargetHour() != targetHour) continue;
            var page = await _store.ReadChangesPageAsync(
                targetHour,
                afterHashId: 0,
                pageSize: _options.ProjectionBatchSize,
                cancellationToken: cancellationToken);
            if (ClosedTargetHour() != targetHour) continue;
            if (page.Changes.Count == 0)
            {
                if (page.ProjectedHour is not null && page.ProjectedHour >= targetHour)
                    return false;
                await _store.FinalizeProjectionAsync(targetHour, cancellationToken);
                if (ClosedTargetHour() != targetHour) continue;
                return true;
            }

            await using var scope = _scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            var connection = (NpgsqlConnection)db.Database.GetDbConnection();
            if (connection.State != System.Data.ConnectionState.Open)
                await connection.OpenAsync(cancellationToken);

            var restartAtNewTarget = false;
            while (true)
            {
                if (ClosedTargetHour() != targetHour)
                {
                    restartAtNewTarget = true;
                    break;
                }
                var mapped = await MapAsync(connection, page.Changes, cancellationToken);
                if (ClosedTargetHour() != targetHour)
                {
                    restartAtNewTarget = true;
                    break;
                }
                var documents = mapped
                    .Where(row => row.Count != row.ProjectedCount)
                    .OrderBy(row => row.Id)
                    .Select(row => new HourlyHeatProjectionDocument(row.Id, row.Count))
                    .ToArray();
                if (documents.Length != 0)
                {
                    var task = await _meili.SubmitHourlyHeatDocumentsAsync(
                        documents, _options.IndexUid, cancellationToken);
                    await WaitForTaskAsync(task, cancellationToken);
                    _metrics.Projected(documents.Length);
                    // The old document may have landed, but without the SQLite
                    // ACK its dirty revision is replayed immediately at the new
                    // target and corrects the idempotent Meili document.
                    if (ClosedTargetHour() != targetHour)
                    {
                        restartAtNewTarget = true;
                        break;
                    }
                }

                await _store.CommitProjectionPageAsync(
                    targetHour,
                    mapped.Select(row => (row.InfoHash, row.Count, row.Revision)).ToArray(),
                    Unmapped(page.Changes, mapped),
                    cancellationToken);
                if (ClosedTargetHour() != targetHour)
                {
                    restartAtNewTarget = true;
                    break;
                }
                if (!page.HasMore)
                {
                    await _store.FinalizeProjectionAsync(targetHour, cancellationToken);
                    if (ClosedTargetHour() != targetHour)
                    {
                        restartAtNewTarget = true;
                        break;
                    }
                    return true;
                }

                page = await _store.ReadChangesPageAsync(
                    targetHour,
                    page.NextHashId,
                    _options.ProjectionBatchSize,
                    cancellationToken);
                if (ClosedTargetHour() != targetHour)
                {
                    restartAtNewTarget = true;
                    break;
                }
                if (page.Changes.Count == 0)
                {
                    await _store.FinalizeProjectionAsync(targetHour, cancellationToken);
                    if (ClosedTargetHour() != targetHour)
                    {
                        restartAtNewTarget = true;
                        break;
                    }
                    return true;
                }
            }
            if (restartAtNewTarget) continue;
        }
    }

    private static long ClosedTargetHour() =>
        HeatRollingStore.UnixHour(DateTime.UtcNow) - 1;

    private static async Task<IReadOnlyList<MappedChange>> MapAsync(
        NpgsqlConnection connection,
        IReadOnlyList<RollingHeatChange> changes,
        CancellationToken cancellationToken)
    {
        var result = new List<MappedChange>(changes.Count);
        foreach (var chunk in changes.Chunk(5000))
        {
            await using var command = new NpgsqlCommand(
                """
                SELECT torrent.id,incoming.info_hash,incoming.actor_count,
                       incoming.projected_count,incoming.revision
                  FROM unnest(@hashes::bytea[],@counts::bigint[],@projected::bigint[],@revisions::bigint[])
                       AS incoming(info_hash,actor_count,projected_count,revision)
                  JOIN torrents torrent ON torrent.info_hash=incoming.info_hash
                 ORDER BY torrent.id
                """, connection);
            command.Parameters.AddWithValue(
                "hashes", NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                chunk.Select(change => change.InfoHash).ToArray());
            command.Parameters.AddWithValue(
                "counts", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
                chunk.Select(change => change.CurrentCount).ToArray());
            command.Parameters.AddWithValue(
                "projected", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
                chunk.Select(change => change.ProjectedCount).ToArray());
            command.Parameters.AddWithValue(
                "revisions", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
                chunk.Select(change => change.Revision).ToArray());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                result.Add(new MappedChange(
                    reader.GetInt64(0),
                    (byte[])reader[1],
                    reader.GetInt64(2),
                    reader.GetInt64(3),
                    reader.GetInt64(4)));
        }
        return result;
    }

    private async Task WaitForTaskAsync(long taskUid, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (true)
        {
            var state = await _meili.GetTaskAsync(taskUid, cancellationToken);
            if (string.Equals(state.Status, "succeeded", StringComparison.OrdinalIgnoreCase)) return;
            if (string.Equals(state.Status, "failed", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(state.Status, "canceled", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException(
                    $"Meilisearch rolling heat task {taskUid} failed: {state.Error ?? state.Status}");
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Meilisearch rolling heat task {taskUid} timed out");
            await Task.Delay(100, cancellationToken);
        }
    }

    private static IReadOnlyList<(byte[] InfoHash, long Revision)> Unmapped(
        IReadOnlyList<RollingHeatChange> changes,
        IReadOnlyList<MappedChange> mapped)
    {
        var known = mapped
            .Select(row => Convert.ToHexString(row.InfoHash))
            .ToHashSet(StringComparer.Ordinal);
        return changes
            .Where(change => !known.Contains(Convert.ToHexString(change.InfoHash)))
            .Select(change => (change.InfoHash, change.Revision))
            .ToArray();
    }

    private sealed record MappedChange(
        long Id,
        byte[] InfoHash,
        long Count,
        long ProjectedCount,
        long Revision);
}
