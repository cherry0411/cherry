using Cherry.Domain.Entities;
using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Search;

public sealed record SearchOutboxClaim(
    long TorrentId,
    long Generation,
    int AttemptCount,
    DateTime EnqueuedAt);

public sealed record SearchOutboxBacklog(
    long Depth,
    long Due,
    long Retrying,
    long RetryAttempts,
    double OldestAgeSeconds);

/// <summary>
/// Transaction helper shared by all authoritative torrent write paths.
/// </summary>
public static class SearchOutboxWriter
{
    public static async Task EnqueueAsync(
        IEnumerable<long> torrentIds,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var ids = torrentIds
            .Distinct()
            .ToArray();
        if (ids.Length == 0)
            return;

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO search_outbox (
                torrent_id,
                generation,
                enqueued_at,
                available_at,
                lease_owner,
                lease_until,
                attempt_count,
                last_error,
                updated_at)
            SELECT torrent_id, 1, NOW(), NOW(), NULL, NULL, 0, NULL, NOW()
              FROM unnest(@torrent_ids::bigint[]) AS incoming(torrent_id)
            ON CONFLICT (torrent_id) DO UPDATE
                SET generation = search_outbox.generation + 1,
                    enqueued_at = NOW(),
                    available_at = NOW(),
                    lease_owner = NULL,
                    lease_until = NULL,
                    attempt_count = 0,
                    last_error = NULL,
                    updated_at = NOW()
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("torrent_ids", ids);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

/// <summary>
/// Durable claim/ack/retry operations. Claims are generation-fenced so an old
/// Meilisearch task cannot acknowledge a newer metadata projection.
/// </summary>
public sealed class SearchOutboxStore
{
    private readonly AppDbContext _db;

    public SearchOutboxStore(AppDbContext db)
    {
        _db = db;
    }

    public async Task<List<SearchOutboxClaim>> ClaimAsync(
        Guid owner,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 5_000);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration), "Lease duration must be positive.");
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await using var command = new NpgsqlCommand(
                """
                WITH candidates AS (
                    SELECT torrent_id
                      FROM search_outbox
                     WHERE available_at <= NOW()
                       AND (lease_until IS NULL OR lease_until <= NOW())
                     ORDER BY available_at, torrent_id
                     FOR UPDATE SKIP LOCKED
                     LIMIT @batch_size)
                UPDATE search_outbox AS item
                   SET lease_owner = @owner,
                       lease_until = NOW() + @lease_duration,
                       updated_at = NOW()
                  FROM candidates
                 WHERE item.torrent_id = candidates.torrent_id
                RETURNING item.torrent_id,
                          item.generation,
                          item.attempt_count,
                          item.enqueued_at
                """,
                connection,
                transaction);
            command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
            command.Parameters.AddWithValue("owner", NpgsqlDbType.Uuid, owner);
            command.Parameters.AddWithValue("lease_duration", NpgsqlDbType.Interval, leaseDuration);

            var claims = new List<SearchOutboxClaim>(batchSize);
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    claims.Add(new SearchOutboxClaim(
                        reader.GetInt64(0),
                        reader.GetInt64(1),
                        reader.GetInt32(2),
                        reader.GetDateTime(3)));
                }
            }

            await transaction.CommitAsync(cancellationToken);
            return claims;
        }
        catch
        {
            if (transaction.Connection is not null)
                await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    public async Task<List<Torrent>> LoadDocumentsAsync(
        IReadOnlyCollection<SearchOutboxClaim> claims,
        CancellationToken cancellationToken = default)
    {
        if (claims.Count == 0)
            return [];
        var ids = claims.Select(claim => claim.TorrentId).ToArray();
        return await _db.Torrents
            .AsNoTracking()
            .Where(torrent => ids.Contains(torrent.Id))
            .ToListAsync(cancellationToken);
    }

    public async Task<int> CompleteAsync(
        Guid owner,
        IReadOnlyCollection<SearchOutboxClaim> claims,
        CancellationToken cancellationToken = default)
    {
        if (claims.Count == 0)
            return 0;
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            DELETE FROM search_outbox AS item
             USING unnest(@torrent_ids::bigint[], @generations::bigint[])
                       AS claim(torrent_id, generation)
             WHERE item.torrent_id = claim.torrent_id
               AND item.generation = claim.generation
               AND item.lease_owner = @owner
            """,
            connection);
        command.Parameters.AddWithValue(
            "torrent_ids",
            claims.Select(claim => claim.TorrentId).ToArray());
        command.Parameters.AddWithValue(
            "generations",
            claims.Select(claim => claim.Generation).ToArray());
        command.Parameters.AddWithValue("owner", NpgsqlDbType.Uuid, owner);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> FailAsync(
        Guid owner,
        IReadOnlyCollection<SearchOutboxClaim> claims,
        string error,
        TimeSpan retryDelay,
        CancellationToken cancellationToken = default)
    {
        if (claims.Count == 0)
            return 0;
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            UPDATE search_outbox AS item
               SET attempt_count = item.attempt_count + 1,
                   last_error = @error,
                   available_at = NOW() + @retry_delay,
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = NOW()
              FROM unnest(@torrent_ids::bigint[], @generations::bigint[])
                        AS claim(torrent_id, generation)
             WHERE item.torrent_id = claim.torrent_id
               AND item.generation = claim.generation
               AND item.lease_owner = @owner
            """,
            connection);
        command.Parameters.AddWithValue("error", NpgsqlDbType.Varchar, BoundError(error));
        command.Parameters.AddWithValue("retry_delay", NpgsqlDbType.Interval, retryDelay);
        command.Parameters.AddWithValue(
            "torrent_ids",
            claims.Select(claim => claim.TorrentId).ToArray());
        command.Parameters.AddWithValue(
            "generations",
            claims.Select(claim => claim.Generation).ToArray());
        command.Parameters.AddWithValue("owner", NpgsqlDbType.Uuid, owner);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<long> RebuildAsync(CancellationToken cancellationToken = default)
    {
        return await _db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO search_outbox (
                torrent_id,
                generation,
                enqueued_at,
                available_at,
                lease_owner,
                lease_until,
                attempt_count,
                last_error,
                updated_at)
            SELECT id, 1, NOW(), NOW(), NULL, NULL, 0, NULL, NOW()
              FROM torrents
            ON CONFLICT (torrent_id) DO UPDATE
                SET generation = search_outbox.generation + 1,
                    enqueued_at = NOW(),
                    available_at = NOW(),
                    lease_owner = NULL,
                    lease_until = NULL,
                    attempt_count = 0,
                    last_error = NULL,
                    updated_at = NOW()
            """,
            cancellationToken);
    }

    public async Task<SearchOutboxBacklog> GetBacklogAsync(
        CancellationToken cancellationToken = default)
    {
        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE available_at <= NOW()
                         AND (lease_until IS NULL OR lease_until <= NOW())),
                   COUNT(*) FILTER (WHERE attempt_count > 0),
                   COALESCE(SUM(attempt_count), 0),
                   COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(enqueued_at))), 0)
              FROM search_outbox
            """,
            connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return new SearchOutboxBacklog(0, 0, 0, 0, 0);
        return new SearchOutboxBacklog(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            reader.GetInt64(3),
            Convert.ToDouble(reader.GetValue(4)));
    }

    private static string BoundError(string error)
    {
        var normalized = string.IsNullOrWhiteSpace(error) ? "unknown Meilisearch error" : error.Trim();
        return normalized.Length <= 1024 ? normalized : normalized[..1024];
    }
}
