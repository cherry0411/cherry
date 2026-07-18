using System.Buffers.Binary;
using System.Security.Cryptography;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Cherry.Infrastructure.Heat;

/// <summary>
/// Replays sealed UTC days in order. PostgreSQL stores only immutable compressed
/// daily frames and a tiny resumable cursor; per-torrent heat exists only in Meili.
/// </summary>
public sealed class HeatProjectionWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly MeiliSearchClient _meili;
    private readonly HeatOptions _options;
    private readonly HeatRuntimeMetrics _metrics;
    private readonly ILogger<HeatProjectionWorker> _logger;
    private readonly HeatProjectionStatusCache _statusCache;
    private readonly SearchRecoveryCoordinator _recoveryCoordinator;

    public HeatProjectionWorker(
        IServiceScopeFactory scopes,
        MeiliSearchClient meili,
        HeatOptions options,
        HeatRuntimeMetrics metrics,
        ILogger<HeatProjectionWorker> logger,
        HeatProjectionStatusCache? statusCache = null,
        SearchRecoveryCoordinator? recoveryCoordinator = null)
    {
        _scopes = scopes;
        _meili = meili;
        _options = options;
        _metrics = metrics;
        _logger = logger;
        _statusCache = statusCache ?? new HeatProjectionStatusCache();
        _recoveryCoordinator = recoveryCoordinator ?? new SearchRecoveryCoordinator();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var progressed = await ProcessOnceAsync(stoppingToken);
                if (!progressed) await Task.Delay(TimeSpan.FromSeconds(_options.LifecyclePollSeconds), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception)
            {
                _metrics.Fail(exception);
                _logger.LogError(exception, "Heat projection pass failed");
                await Task.Delay(TimeSpan.FromSeconds(_options.LifecyclePollSeconds), stoppingToken);
            }
        }
    }

    public async Task<bool> ProcessOnceAsync(CancellationToken cancellationToken = default)
    {
        await using var projection =
            await _recoveryCoordinator.EnterProjectionAsync(cancellationToken);
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var connection = (NpgsqlConnection)db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open) await connection.OpenAsync(cancellationToken);
        await EnsureWatermarkAsync(connection, cancellationToken);
        var watermark = await ReadWatermarkAsync(connection, cancellationToken);
        var rebuild = watermark.Rebuild || watermark.Day is null;
        var target = rebuild
            ? await LatestSealedDayAsync(connection, cancellationToken)
            : watermark.Day!.Value.AddDays(1);
        if (target is null) return false;
        if (!await IsSealedDayAsync(connection, target.Value, cancellationToken)) return false;

        var projectionCoverageMask = 0;
        for (short shard = 0; shard < HeatFrameCodec.ShardCount; shard++)
        {
            var source = await LoadSourceAsync(connection, target.Value, shard, cancellationToken);
            if (source is null) return false; // Only missing/unsealed days stop; sealed partial days advance as unknown.
            projectionCoverageMask = source.CoverageMask;
            var documents = rebuild
                ? HeatProjectionMath.BuildFull(target.Value, shard, source.Frames)
                : HeatProjectionMath.BuildIncremental(target.Value, shard, source.Frames);
            var progressed = await ProjectShardAsync(
                connection, target.Value, shard, source.Digest, documents, cancellationToken);
            if (!progressed)
            {
                // A Meili task normally completes quickly, but an immediate
                // tight loop can monopolize a small storage host while it is
                // indexing. Keep latency low while capping status polling.
                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
                return true;
            }
        }

        await using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
        {
            await using var update = new NpgsqlCommand(
                """
                UPDATE heat_projection_watermarks
                   SET projected_through=@day,coverage_mask=@mask,
                       rebuild_required=FALSE,updated_at=NOW()
                 WHERE index_generation=@generation;
                DELETE FROM heat_projection_tasks
                 WHERE index_generation=@generation AND target_day=@day;
                DELETE FROM heat_day_manifests WHERE day < @gc;
                """, connection, transaction);
            update.Parameters.AddWithValue("day", target.Value);
            update.Parameters.AddWithValue("mask", projectionCoverageMask);
            update.Parameters.AddWithValue("generation", _options.IndexGeneration);
            update.Parameters.AddWithValue("gc", target.Value.AddDays(-30));
            await update.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        _statusCache.Set(target.Value, projectionCoverageMask);
        return true;
    }

    private async Task<bool> ProjectShardAsync(
        NpgsqlConnection connection,
        DateOnly target,
        short shard,
        byte[] sourceDigest,
        IReadOnlyList<HeatProjectionDocument> documents,
        CancellationToken ct)
    {
        await using (var create = new NpgsqlCommand(
            """
            INSERT INTO heat_projection_tasks(index_generation,target_day,shard,source_manifest_sha256)
            VALUES(@generation,@day,@shard,@digest)
            ON CONFLICT(index_generation,target_day,shard) DO NOTHING
            """, connection))
        {
            create.Parameters.AddWithValue("generation", _options.IndexGeneration);
            create.Parameters.AddWithValue("day", target);
            create.Parameters.AddWithValue("shard", shard);
            create.Parameters.AddWithValue("digest", sourceDigest);
            await create.ExecuteNonQueryAsync(ct);
        }

        long? afterId;
        long? pendingTask;
        long? pendingEnd;
        byte[] storedSource;
        short status;
        await using (var read = new NpgsqlCommand(
            """
            SELECT after_id,pending_task_uid,range_end_id,source_manifest_sha256,status
              FROM heat_projection_tasks
             WHERE index_generation=@generation AND target_day=@day AND shard=@shard
            """, connection))
        {
            read.Parameters.AddWithValue("generation", _options.IndexGeneration);
            read.Parameters.AddWithValue("day", target);
            read.Parameters.AddWithValue("shard", shard);
            await using var reader = await read.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) throw new InvalidDataException("Projection cursor disappeared");
            afterId = reader.IsDBNull(0) ? null : reader.GetInt64(0);
            pendingTask = reader.IsDBNull(1) ? null : reader.GetInt64(1);
            pendingEnd = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            storedSource = (byte[])reader[3];
            status = reader.GetInt16(4);
        }
        if (!storedSource.AsSpan().SequenceEqual(sourceDigest))
            throw new InvalidDataException("Projection source changed after cursor creation");
        if (status == 1) return true;

        if (pendingTask is not null)
        {
            var state = await _meili.GetTaskAsync(pendingTask.Value, ct);
            if (string.Equals(state.Status, "succeeded", StringComparison.OrdinalIgnoreCase))
            {
                await using var success = new NpgsqlCommand(
                    """
                    UPDATE heat_projection_tasks
                       SET after_id=range_end_id,range_start_id=NULL,range_end_id=NULL,
                           pending_task_uid=NULL,payload_sha256=NULL,updated_at=NOW()
                     WHERE index_generation=@generation AND target_day=@day AND shard=@shard
                           AND pending_task_uid=@task
                    """, connection);
                AddCursorParameters(success, target, shard);
                success.Parameters.AddWithValue("task", pendingTask.Value);
                await success.ExecuteNonQueryAsync(ct);
                afterId = pendingEnd;
            }
            else if (string.Equals(state.Status, "failed", StringComparison.OrdinalIgnoreCase) ||
                     string.Equals(state.Status, "canceled", StringComparison.OrdinalIgnoreCase))
            {
                await using var failed = new NpgsqlCommand(
                    """
                    UPDATE heat_projection_tasks
                       SET range_start_id=NULL,range_end_id=NULL,pending_task_uid=NULL,
                           payload_sha256=NULL,updated_at=NOW()
                     WHERE index_generation=@generation AND target_day=@day AND shard=@shard
                    """, connection);
                AddCursorParameters(failed, target, shard);
                await failed.ExecuteNonQueryAsync(ct);
            }
            else return false;
        }

        var batch = documents
            .Where(document => afterId is null || document.Id > afterId)
            .Take(_options.ProjectionBatchSize)
            .ToArray();
        if (batch.Length == 0)
        {
            await using var done = new NpgsqlCommand(
                """
                UPDATE heat_projection_tasks SET status=1,updated_at=NOW()
                 WHERE index_generation=@generation AND target_day=@day AND shard=@shard
                """, connection);
            AddCursorParameters(done, target, shard);
            await done.ExecuteNonQueryAsync(ct);
            return true;
        }

        var payloadDigest = ProjectionPayloadDigest(batch);
        var taskUid = await _meili.SubmitHeatDocumentsAsync(batch, _options.IndexUid, ct);
        await using var pending = new NpgsqlCommand(
            """
            UPDATE heat_projection_tasks
               SET range_start_id=@start,range_end_id=@end,pending_task_uid=@task,
                   payload_sha256=@payload,updated_at=NOW()
             WHERE index_generation=@generation AND target_day=@day AND shard=@shard
                   AND pending_task_uid IS NULL
            """, connection);
        AddCursorParameters(pending, target, shard);
        pending.Parameters.AddWithValue("start", batch[0].Id);
        pending.Parameters.AddWithValue("end", batch[^1].Id);
        pending.Parameters.AddWithValue("task", taskUid);
        pending.Parameters.AddWithValue("payload", payloadDigest);
        await pending.ExecuteNonQueryAsync(ct);
        _metrics.Projected(batch.Length);
        return false;
    }

    private async Task<ProjectionSource?> LoadSourceAsync(
        NpgsqlConnection connection,
        DateOnly target,
        short shard,
        CancellationToken ct)
    {
        var coverageStart = _options.ParsedCoverageStartDay!.Value;
        var start = target.AddDays(-30);
        var manifests = new Dictionary<DateOnly, byte[]>();
        var frames = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>();
        await using var command = new NpgsqlCommand(
            """
            SELECT manifest.day,manifest.status,manifest.coverage_status,manifest.manifest_sha256,
                   frame.entry_count,frame.payload_sha256,frame.payload
              FROM heat_day_manifests manifest
              JOIN heat_day_frames frame ON frame.day=manifest.day AND frame.shard=@shard
             WHERE manifest.day BETWEEN @start AND @target
             ORDER BY manifest.day
            """, connection);
        command.Parameters.AddWithValue("shard", shard);
        command.Parameters.AddWithValue("start", start);
        command.Parameters.AddWithValue("target", target);
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var day = reader.GetFieldValue<DateOnly>(0);
                if (reader.GetInt16(1) != 1) return null;
                var payload = (byte[])reader[6];
                if (!SHA256.HashData(payload).AsSpan().SequenceEqual((byte[])reader[5]))
                    throw new InvalidDataException($"Heat frame checksum mismatch for {day}/{shard}");
                manifests[day] = (byte[])reader[3];
                if (reader.GetInt16(2) == 1)
                    frames[day] = HeatFrameCodec.Decode(shard, reader.GetInt32(4), payload);
            }
        }
        for (var day = start; day <= target; day = day.AddDays(1))
            if (day >= coverageStart && !manifests.ContainsKey(day)) return null;

        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> date = stackalloc byte[4];
        foreach (var pair in manifests.OrderBy(pair => pair.Key))
        {
            BinaryPrimitives.WriteInt32BigEndian(date, pair.Key.DayNumber);
            digest.AppendData(date);
            digest.AppendData(pair.Value);
        }
        var coverageMask = 0;
        for (var offset = 0; offset < 30; offset++)
            if (frames.ContainsKey(target.AddDays(-offset))) coverageMask |= 1 << offset;
        return new ProjectionSource(frames, digest.GetHashAndReset(), coverageMask);
    }

    private async Task EnsureWatermarkAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO heat_projection_watermarks(index_generation,index_uid)
            VALUES(@generation,@uid)
            ON CONFLICT(index_generation) DO NOTHING
            """, connection);
        command.Parameters.AddWithValue("generation", _options.IndexGeneration);
        command.Parameters.AddWithValue("uid", _options.IndexUid);
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<(DateOnly? Day, bool Rebuild)> ReadWatermarkAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT projected_through,rebuild_required,index_uid FROM heat_projection_watermarks WHERE index_generation=@generation",
            connection);
        command.Parameters.AddWithValue("generation", _options.IndexGeneration);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new InvalidDataException("Heat watermark missing");
        if (!string.Equals(reader.GetString(2), _options.IndexUid, StringComparison.Ordinal))
            throw new InvalidDataException("Heat index generation is already bound to another index UID");
        return (reader.IsDBNull(0) ? null : reader.GetFieldValue<DateOnly>(0), reader.GetBoolean(1));
    }

    private static async Task<DateOnly?> LatestSealedDayAsync(
        NpgsqlConnection connection,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT MAX(day) FROM heat_day_manifests
             WHERE status=1 AND coverage_status IN (1,2) AND shard_count=64
            """, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : (DateOnly)value;
    }

    private static async Task<bool> IsSealedDayAsync(NpgsqlConnection connection, DateOnly day, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT EXISTS(
              SELECT 1 FROM heat_day_manifests
               WHERE day=@day AND status=1 AND coverage_status IN (1,2) AND shard_count=64)
            """, connection);
        command.Parameters.AddWithValue("day", day);
        return (bool)(await command.ExecuteScalarAsync(ct) ?? false);
    }

    private void AddCursorParameters(NpgsqlCommand command, DateOnly day, short shard)
    {
        command.Parameters.AddWithValue("generation", _options.IndexGeneration);
        command.Parameters.AddWithValue("day", day);
        command.Parameters.AddWithValue("shard", shard);
    }

    private static byte[] ProjectionPayloadDigest(IEnumerable<HeatProjectionDocument> documents)
    {
        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> row = stackalloc byte[40];
        foreach (var document in documents)
        {
            BinaryPrimitives.WriteInt64BigEndian(row[0..8], document.Id);
            BinaryPrimitives.WriteInt64BigEndian(row[8..16], document.Heat1d);
            BinaryPrimitives.WriteInt64BigEndian(row[16..24], document.Heat7d);
            BinaryPrimitives.WriteInt64BigEndian(row[24..32], document.Heat15d);
            BinaryPrimitives.WriteInt64BigEndian(row[32..40], document.Heat30d);
            digest.AppendData(row);
        }
        return digest.GetHashAndReset();
    }

    private sealed record ProjectionSource(
        IReadOnlyDictionary<DateOnly, IReadOnlyList<HeatFrameEntry>> Frames,
        byte[] Digest,
        int CoverageMask);
}

public static class HeatCoverage
{
    public static int Count(int mask, int windowDays)
    {
        if (windowDays is not (1 or 7 or 15 or 30))
            throw new ArgumentOutOfRangeException(nameof(windowDays));
        var selected = mask & ((1 << windowDays) - 1);
        return System.Numerics.BitOperations.PopCount((uint)selected);
    }
}
