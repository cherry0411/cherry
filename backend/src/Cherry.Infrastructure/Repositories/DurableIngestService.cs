using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Cherry.Infrastructure.Repositories;

public sealed record DurableIngestResult(bool IsConflict, DurableBatchResponse Response);

/// <summary>
/// Commits torrent rows, bounded summary detail, exact policy decisions, and
/// the crawler receipt in one PostgreSQL transaction. This intentionally does
/// not call TorrentRepository's self-committing bulk insert path.
/// </summary>
public sealed class DurableIngestService
{
    private readonly AppDbContext _db;
    private readonly IProcessedHashFilter _processedHashFilter;
    private readonly MeiliIndexQueue? _meiliQueue;

    public DurableIngestService(
        AppDbContext db,
        IProcessedHashFilter processedHashFilter,
        MeiliIndexQueue? meiliQueue = null)
    {
        _db = db;
        _processedHashFilter = processedHashFilter;
        _meiliQueue = meiliQueue;
    }

    public async Task<DurableIngestResult> IngestAsync(
        ParsedDurableBatch parsed,
        CancellationToken cancellationToken = default)
    {
        var request = parsed.Request;
        var validated = DurableBatchValidator.ValidateAndMap(request);
        var crawlerId = request.CrawlerId!;
        var epoch = checked((long)request.Epoch);
        var start = checked((long)request.StartSequence);
        var end = checked((long)request.EndSequence);

        var connection = (NpgsqlConnection)_db.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await InsertReceiptSeedAsync(
                connection,
                transaction,
                crawlerId,
                epoch,
                cancellationToken);
            var receipt = await LockReceiptAsync(
                connection,
                transaction,
                crawlerId,
                epoch,
                cancellationToken);
            var expectedStart = checked((ulong)receipt.LastEndSequence + 1UL);

            if (!string.Equals(
                    request.PayloadSha256,
                    parsed.CalculatedPayloadSha256,
                    StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken);
                return Conflict(request, expectedStart, "payload_sha256 does not match the raw events JSON bytes");
            }

            if (start == receipt.LastStartSequence &&
                end == receipt.LastEndSequence &&
                string.Equals(
                    request.PayloadSha256,
                    receipt.LastPayloadSha256,
                    StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken);
                return new DurableIngestResult(
                    IsConflict: false,
                    Ack(
                        request,
                        receipt.LastAccepted,
                        receipt.LastDuplicates,
                        committed: true));
            }

            if (request.StartSequence != expectedStart)
            {
                await transaction.RollbackAsync(cancellationToken);
                return Conflict(request, expectedStart, "sequence gap, overlap, or conflicting replay");
            }

            var uniqueTorrents = validated.Torrents
                .GroupBy(torrent => torrent.InfoHash, StringComparer.Ordinal)
                .Select(group => group
                    .OrderByDescending(torrent => torrent.RetainedLevel)
                    .First())
                .ToList();
            var uniqueDecisions = validated.Decisions
                .GroupBy(
                    decision => Convert.ToHexString(decision.InfoHash),
                    StringComparer.Ordinal)
                .Select(group => group
                    .OrderByDescending(decision => decision.Action)
                    .First())
                .ToList();

            await ExactHashTransactionLock.AcquireAsync(
                uniqueTorrents.Select(torrent => torrent.InfoHash)
                    .Concat(uniqueDecisions.Select(decision =>
                        Convert.ToHexString(decision.InfoHash).ToLowerInvariant())),
                connection,
                transaction,
                cancellationToken);

            // Warming before commit avoids a post-commit negative window. A
            // rollback can only introduce harmless probabilistic positives.
            _processedHashFilter.RecordCandidates(
                uniqueTorrents.Select(torrent => torrent.InfoHash)
                    .Concat(uniqueDecisions.Select(decision =>
                        Convert.ToHexString(decision.InfoHash).ToLowerInvariant())));

            var changedTorrentHashes = await InsertTorrentsAsync(
                uniqueTorrents,
                connection,
                transaction,
                cancellationToken);
            await ExactHashTransactionLock.DeleteDecisionsForTorrentsAsync(
                uniqueTorrents.Select(torrent => torrent.InfoHash).ToArray(),
                connection,
                transaction,
                cancellationToken);
            if (changedTorrentHashes.Count > 0)
            {
                await DeletePriorDetailsAsync(
                    changedTorrentHashes,
                    connection,
                    transaction,
                    cancellationToken);
            }

            var files = uniqueTorrents
                .Where(torrent => changedTorrentHashes.Contains(torrent.InfoHash))
                .SelectMany(torrent => torrent.Files)
                .ToList();
            if (files.Count > 0)
                await CopyFilesAsync(files, connection, cancellationToken);
            var extensions = uniqueTorrents
                .Where(torrent => changedTorrentHashes.Contains(torrent.InfoHash))
                .SelectMany(torrent => torrent.ExtensionSummaries)
                .ToList();
            if (extensions.Count > 0)
                await CopyExtensionSummariesAsync(extensions, connection, cancellationToken);

            var changedDecisionHashes = await InsertDecisionsAsync(
                uniqueDecisions,
                connection,
                transaction,
                cancellationToken);

            var accepted = changedTorrentHashes.Count + changedDecisionHashes.Count;
            var duplicates = validated.EventCount - accepted;
            await UpdateReceiptAsync(
                connection,
                transaction,
                crawlerId,
                epoch,
                start,
                end,
                request.PayloadSha256!,
                accepted,
                duplicates,
                cancellationToken);

            await transaction.CommitAsync(cancellationToken);

            if (_meiliQueue is not null && accepted > 0)
            {
                _meiliQueue.Enqueue(
                    uniqueTorrents
                        .Where(torrent => changedTorrentHashes.Contains(torrent.InfoHash))
                        .ToList());
            }

            return new DurableIngestResult(
                IsConflict: false,
                Ack(request, accepted, duplicates, committed: true));
        }
        catch
        {
            if (transaction.Connection is not null)
                await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task InsertReceiptSeedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string crawlerId,
        long epoch,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO durable_batch_receipts (
                crawler_id,
                epoch,
                last_start_sequence,
                last_end_sequence,
                last_payload_sha256,
                last_accepted,
                last_duplicates)
            VALUES (@crawler_id, @epoch, 0, 0, '', 0, 0)
            ON CONFLICT (crawler_id, epoch) DO NOTHING
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("crawler_id", NpgsqlDbType.Varchar, crawlerId);
        command.Parameters.AddWithValue("epoch", NpgsqlDbType.Bigint, epoch);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<ReceiptState> LockReceiptAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string crawlerId,
        long epoch,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT last_start_sequence,
                   last_end_sequence,
                   last_payload_sha256,
                   last_accepted,
                   last_duplicates
              FROM durable_batch_receipts
             WHERE crawler_id = @crawler_id AND epoch = @epoch
               FOR UPDATE
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("crawler_id", NpgsqlDbType.Varchar, crawlerId);
        command.Parameters.AddWithValue("epoch", NpgsqlDbType.Bigint, epoch);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            throw new InvalidOperationException("Failed to create or lock the durable batch receipt.");
        return new ReceiptState(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetInt32(3),
            reader.GetInt32(4));
    }

    private static async Task<HashSet<string>> InsertTorrentsAsync(
        List<Torrent> torrents,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (torrents.Count == 0)
            return new HashSet<string>(StringComparer.Ordinal);

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO torrents (
                info_hash,
                name,
                piece_length,
                total_length,
                file_count,
                is_private,
                source,
                policy_id,
                region,
                retained_level,
                needs_refetch,
                created_at,
                updated_at)
            SELECT * FROM unnest(
                @hashes::varchar[],
                @names::text[],
                @piece_lengths::int[],
                @total_lengths::bigint[],
                @file_counts::int[],
                @is_private::bool[],
                @sources::varchar[],
                @policy_ids::varchar[],
                @regions::varchar[],
                @retained_levels::smallint[],
                @needs_refetch::bool[],
                @created_at::timestamptz[],
                @updated_at::timestamptz[])
            ON CONFLICT (info_hash) DO UPDATE
                SET name = EXCLUDED.name,
                    piece_length = EXCLUDED.piece_length,
                    total_length = EXCLUDED.total_length,
                    file_count = EXCLUDED.file_count,
                    is_private = EXCLUDED.is_private,
                    source = EXCLUDED.source,
                    policy_id = EXCLUDED.policy_id,
                    region = EXCLUDED.region,
                    retained_level = EXCLUDED.retained_level,
                    needs_refetch = EXCLUDED.needs_refetch,
                    created_at = LEAST(torrents.created_at, EXCLUDED.created_at),
                    updated_at = NOW()
              WHERE torrents.retained_level < EXCLUDED.retained_level
            RETURNING info_hash
            """,
            connection,
            transaction);

        command.Parameters.AddWithValue("hashes", torrents.Select(t => t.InfoHash).ToArray());
        command.Parameters.AddWithValue("names", torrents.Select(t => t.Name).ToArray());
        command.Parameters.AddWithValue("piece_lengths", torrents.Select(t => t.PieceLength).ToArray());
        command.Parameters.AddWithValue("total_lengths", torrents.Select(t => t.TotalLength).ToArray());
        command.Parameters.AddWithValue("file_counts", torrents.Select(t => t.FileCount).ToArray());
        command.Parameters.AddWithValue("is_private", torrents.Select(t => t.IsPrivate).ToArray());
        command.Parameters.Add(new NpgsqlParameter<string?[]>(
            "sources",
            NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = torrents.Select(t => t.Source).ToArray()
        });
        command.Parameters.Add(new NpgsqlParameter<string?[]>(
            "policy_ids",
            NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = torrents.Select(t => t.PolicyId).ToArray()
        });
        command.Parameters.Add(new NpgsqlParameter<string?[]>(
            "regions",
            NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = torrents.Select(t => t.Region).ToArray()
        });
        command.Parameters.AddWithValue(
            "retained_levels",
            NpgsqlDbType.Array | NpgsqlDbType.Smallint,
            torrents.Select(t => (short)t.RetainedLevel).ToArray());
        command.Parameters.AddWithValue(
            "needs_refetch",
            torrents.Select(t => t.NeedsRefetch).ToArray());
        command.Parameters.AddWithValue(
            "created_at",
            NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
            torrents.Select(t => t.CreatedAt).ToArray());
        command.Parameters.AddWithValue(
            "updated_at",
            NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
            torrents.Select(t => t.UpdatedAt).ToArray());

        var inserted = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            inserted.Add(reader.GetString(0));
        return inserted;
    }

    private static async Task DeletePriorDetailsAsync(
        HashSet<string> infoHashes,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            DELETE FROM torrent_files WHERE info_hash = ANY(@hashes);
            DELETE FROM torrent_extension_summaries WHERE info_hash = ANY(@hashes);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("hashes", infoHashes.ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<HashSet<string>> InsertDecisionsAsync(
        List<MetadataDecision> decisions,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (decisions.Count == 0)
            return new HashSet<string>(StringComparer.Ordinal);

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO metadata_decisions (
                info_hash,
                action,
                retained_level,
                needs_refetch,
                policy_id,
                reason,
                first_seen,
                region,
                updated_at)
            SELECT decode(hash, 'hex'),
                   action,
                   retained_level,
                   needs_refetch,
                   policy_id,
                   reason,
                   first_seen,
                   region,
                   NOW()
              FROM unnest(
                  @hashes::text[],
                  @actions::smallint[],
                  @retained_levels::smallint[],
                  @needs_refetch::bool[],
                  @policy_ids::varchar[],
                  @reasons::varchar[],
                  @first_seen::timestamptz[],
                   @regions::varchar[])
                   AS d(hash, action, retained_level, needs_refetch, policy_id, reason, first_seen, region)
             WHERE NOT EXISTS (
                       SELECT 1
                         FROM torrents AS torrent
                        WHERE torrent.info_hash = d.hash)
            ON CONFLICT (info_hash) DO UPDATE
                SET action = EXCLUDED.action,
                    retained_level = EXCLUDED.retained_level,
                    needs_refetch = EXCLUDED.needs_refetch,
                    policy_id = EXCLUDED.policy_id,
                    reason = EXCLUDED.reason,
                    first_seen = COALESCE(metadata_decisions.first_seen, EXCLUDED.first_seen),
                    region = EXCLUDED.region,
                    updated_at = NOW()
              WHERE metadata_decisions.action < EXCLUDED.action
            RETURNING encode(info_hash, 'hex')
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue(
            "hashes",
            decisions.Select(d => Convert.ToHexString(d.InfoHash).ToLowerInvariant()).ToArray());
        command.Parameters.AddWithValue(
            "actions",
            NpgsqlDbType.Array | NpgsqlDbType.Smallint,
            decisions.Select(d => (short)d.Action).ToArray());
        command.Parameters.AddWithValue(
            "retained_levels",
            NpgsqlDbType.Array | NpgsqlDbType.Smallint,
            decisions.Select(d => (short)d.RetainedLevel).ToArray());
        command.Parameters.AddWithValue(
            "needs_refetch",
            decisions.Select(d => d.NeedsRefetch).ToArray());
        command.Parameters.Add(new NpgsqlParameter<string?[]>(
            "policy_ids",
            NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = decisions.Select(d => d.PolicyId).ToArray()
        });
        command.Parameters.AddWithValue("reasons", decisions.Select(d => d.Reason).ToArray());
        command.Parameters.Add(new NpgsqlParameter<DateTime?[]>(
            "first_seen",
            NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
        {
            Value = decisions.Select(d => d.FirstSeen).ToArray()
        });
        command.Parameters.Add(new NpgsqlParameter<string?[]>(
            "regions",
            NpgsqlDbType.Array | NpgsqlDbType.Varchar)
        {
            Value = decisions.Select(d => d.Region).ToArray()
        });

        var changed = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            changed.Add(reader.GetString(0));
        return changed;
    }

    private static async Task CopyFilesAsync(
        List<TorrentFile> files,
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var writer = await connection.BeginBinaryImportAsync(
            "COPY torrent_files (info_hash, path_text, length) FROM STDIN (FORMAT BINARY)",
            cancellationToken);
        foreach (var file in files)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(file.InfoHash, NpgsqlDbType.Varchar, cancellationToken);
            await writer.WriteAsync(file.PathText, NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(file.Length, NpgsqlDbType.Bigint, cancellationToken);
        }
        await writer.CompleteAsync(cancellationToken);
    }

    private static async Task CopyExtensionSummariesAsync(
        List<TorrentExtensionSummary> extensions,
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var writer = await connection.BeginBinaryImportAsync(
            "COPY torrent_extension_summaries (info_hash, extension, file_count, total_length) FROM STDIN (FORMAT BINARY)",
            cancellationToken);
        foreach (var extension in extensions)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(extension.InfoHash, NpgsqlDbType.Varchar, cancellationToken);
            await writer.WriteAsync(extension.Extension, NpgsqlDbType.Varchar, cancellationToken);
            await writer.WriteAsync(extension.FileCount, NpgsqlDbType.Integer, cancellationToken);
            await writer.WriteAsync(extension.TotalLength, NpgsqlDbType.Bigint, cancellationToken);
        }
        await writer.CompleteAsync(cancellationToken);
    }

    private static async Task UpdateReceiptAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string crawlerId,
        long epoch,
        long start,
        long end,
        string checksum,
        int accepted,
        int duplicates,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            UPDATE durable_batch_receipts
               SET last_start_sequence = @start,
                   last_end_sequence = @end,
                   last_payload_sha256 = @checksum,
                   last_accepted = @accepted,
                   last_duplicates = @duplicates,
                   updated_at = NOW()
             WHERE crawler_id = @crawler_id AND epoch = @epoch
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("start", NpgsqlDbType.Bigint, start);
        command.Parameters.AddWithValue("end", NpgsqlDbType.Bigint, end);
        command.Parameters.AddWithValue("checksum", NpgsqlDbType.Varchar, checksum);
        command.Parameters.AddWithValue("accepted", NpgsqlDbType.Integer, accepted);
        command.Parameters.AddWithValue("duplicates", NpgsqlDbType.Integer, duplicates);
        command.Parameters.AddWithValue("crawler_id", NpgsqlDbType.Varchar, crawlerId);
        command.Parameters.AddWithValue("epoch", NpgsqlDbType.Bigint, epoch);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException("Durable batch receipt update affected an unexpected number of rows.");
    }

    private static DurableIngestResult Conflict(
        DurableBatchRequest request,
        ulong expectedStart,
        string error) =>
        new(
            IsConflict: true,
            Ack(
                request,
                accepted: 0,
                duplicates: 0,
                committed: false,
                expectedStart,
                error));

    private static DurableBatchResponse Ack(
        DurableBatchRequest request,
        int accepted,
        int duplicates,
        bool committed,
        ulong? expectedStart = null,
        string? error = null) =>
        new()
        {
            CrawlerId = request.CrawlerId!,
            Epoch = request.Epoch,
            StartSequence = request.StartSequence,
            EndSequence = request.EndSequence,
            PayloadSha256 = request.PayloadSha256!,
            Accepted = accepted,
            Duplicates = duplicates,
            Committed = committed,
            ExpectedStart = expectedStart,
            Error = error
        };

    private sealed record ReceiptState(
        long LastStartSequence,
        long LastEndSequence,
        string LastPayloadSha256,
        int LastAccepted,
        int LastDuplicates);
}
