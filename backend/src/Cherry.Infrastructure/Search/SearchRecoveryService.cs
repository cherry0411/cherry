using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Heat;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Cherry.Infrastructure.Search;

public sealed record SearchRecoveryResult(
    long MetadataRowsEnqueued,
    long VerifiedEmptyDocuments,
    bool HeatRebuildRequested,
    DateOnly? HeatTargetDay);

public sealed record SearchRecoveryHeatStatus(
    bool Enabled,
    string IndexGeneration,
    DateOnly? ProjectedThrough,
    bool RebuildRequired,
    long PendingTasks);

public sealed record SearchRecoveryStatus(
    long? AuthoritativeDocuments,
    long? MeiliDocuments,
    SearchRecoveryHeatStatus Heat);

/// <summary>
/// Coordinates reconstruction of the disposable Meilisearch projection. The
/// physical index is deleted and recreated before PostgreSQL atomically queues
/// all metadata and requests a full heat projection, so stale heat can never
/// survive an in-place generation reset.
/// </summary>
public sealed class SearchRecoveryService
{
    public const string Confirmation = "DELETE_AND_REBUILD_TORRENTS_INDEX";

    private readonly AppDbContext _db;
    private readonly SearchOutboxStore _outbox;
    private readonly MeiliSearchClient _meili;
    private readonly HeatOptions _heat;
    private readonly SearchRecoveryCoordinator _coordinator;

    public SearchRecoveryService(
        AppDbContext db,
        SearchOutboxStore outbox,
        MeiliSearchClient meili,
        HeatOptions heat,
        SearchRecoveryCoordinator coordinator)
    {
        _db = db;
        _outbox = outbox;
        _meili = meili;
        _heat = heat;
        _coordinator = coordinator;
    }

    public async Task<SearchRecoveryResult> RecoverAsync(
        string confirmation,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(confirmation, Confirmation, StringComparison.Ordinal))
            throw new SearchRecoveryConfirmationException();

        await using var recovery = await _coordinator.EnterRecoveryAsync(cancellationToken);
        return await RecoverUnderLockAsync(cancellationToken);
    }

    /// <summary>
    /// Startup-only conservative recovery. An empty projection is rebuilt only
    /// when PostgreSQL proves that the authoritative catalog is non-empty. A
    /// non-empty (possibly partial) Meili index is never reset automatically.
    /// </summary>
    public async Task<SearchRecoveryResult?> RecoverIfProvablyEmptyAsync(
        CancellationToken cancellationToken)
    {
        await using var recovery = await _coordinator.EnterRecoveryAsync(cancellationToken);
        if (!await _db.Torrents.AsNoTracking().AnyAsync(cancellationToken))
            return null;
        if (await _meili.GetDocumentCountAsync(cancellationToken) != 0)
            return null;
        return await RecoverUnderLockAsync(cancellationToken);
    }

    private async Task<SearchRecoveryResult> RecoverUnderLockAsync(
        CancellationToken cancellationToken)
    {

        // Meili tasks are ordered. Waiting for delete, create, and settings tasks
        // while projection workers are gated proves the new physical index is
        // empty before any authoritative replay is queued.
        await _meili.ResetIndexAsync(cancellationToken);
        var emptyDocuments = await _meili.GetDocumentCountAsync(cancellationToken);
        if (emptyDocuments != 0)
            throw new InvalidDataException(
                $"Recreated Meilisearch index contains {emptyDocuments} documents before replay");

        await using var transaction = await _db.Database.BeginTransactionAsync(cancellationToken);
        var enqueued = await _outbox.RebuildAsync(cancellationToken);
        DateOnly? heatTarget = null;
        var heatRequested = false;

        if (_heat.Enabled)
        {
            var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
            var npgsqlTransaction = (NpgsqlTransaction)transaction.GetDbTransaction();
            heatTarget = await LatestSealedDayAsync(connection, npgsqlTransaction, cancellationToken);
            if (heatTarget is not null)
            {
                await using var reset = new NpgsqlCommand(
                    """
                    INSERT INTO heat_projection_watermarks(index_generation,index_uid)
                    VALUES(@generation,@uid)
                    ON CONFLICT(index_generation) DO NOTHING;
                    DELETE FROM heat_projection_tasks WHERE index_generation=@generation;
                    UPDATE heat_projection_watermarks
                       SET rebuild_required=TRUE,updated_at=NOW()
                     WHERE index_generation=@generation;
                    """,
                    connection,
                    npgsqlTransaction);
                reset.Parameters.AddWithValue("generation", _heat.IndexGeneration);
                reset.Parameters.AddWithValue("uid", _heat.IndexUid);
                await reset.ExecuteNonQueryAsync(cancellationToken);
                heatRequested = true;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return new SearchRecoveryResult(enqueued, emptyDocuments, heatRequested, heatTarget);
    }

    public async Task<SearchRecoveryStatus> GetStatusAsync(
        bool verifyDocumentCounts,
        CancellationToken cancellationToken)
    {
        long? authoritativeDocuments = null;
        long? meiliDocuments = null;
        if (verifyDocumentCounts)
        {
            authoritativeDocuments = await _db.Torrents.LongCountAsync(cancellationToken);
            meiliDocuments = await _meili.GetDocumentCountAsync(cancellationToken);
        }
        if (!_heat.Enabled)
            return new SearchRecoveryStatus(
                authoritativeDocuments,
                meiliDocuments,
                new SearchRecoveryHeatStatus(false, _heat.IndexGeneration, null, false, 0));

        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            SELECT watermark.projected_through,
                   watermark.rebuild_required,
                   (SELECT COUNT(*) FROM heat_projection_tasks task
                     WHERE task.index_generation=@generation)
              FROM heat_projection_watermarks watermark
             WHERE watermark.index_generation=@generation
            """,
            connection);
        command.Parameters.AddWithValue("generation", _heat.IndexGeneration);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return new SearchRecoveryStatus(
                authoritativeDocuments,
                meiliDocuments,
                new SearchRecoveryHeatStatus(true, _heat.IndexGeneration, null, false, 0));
        return new SearchRecoveryStatus(
            authoritativeDocuments,
            meiliDocuments,
            new SearchRecoveryHeatStatus(
                true,
                _heat.IndexGeneration,
                reader.IsDBNull(0) ? null : reader.GetFieldValue<DateOnly>(0),
                reader.GetBoolean(1),
                reader.GetInt64(2)));
    }

    private static async Task<DateOnly?> LatestSealedDayAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT MAX(day) FROM heat_day_manifests
             WHERE status=1 AND coverage_status IN (1,2) AND shard_count=64
            """,
            connection,
            transaction);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DBNull or null ? null : (DateOnly)value;
    }
}

public sealed class SearchRecoveryConfirmationException : Exception
{
    public SearchRecoveryConfirmationException()
        : base($"confirmation must equal {SearchRecoveryService.Confirmation}") { }
}
