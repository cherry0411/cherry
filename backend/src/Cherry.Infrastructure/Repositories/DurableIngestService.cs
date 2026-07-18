using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Search;
using Cherry.Infrastructure.Storage;
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

    public DurableIngestService(
        AppDbContext db,
        IProcessedHashFilter processedHashFilter)
    {
        _db = db;
        _processedHashFilter = processedHashFilter;
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
                    // Prefer a complete normalized body over a bounded summary
                    // if the same hash occurs twice inside one wire batch. This
                    // is transient selection; no retention marker is persisted.
                    .OrderByDescending(torrent => torrent.Files.Count == torrent.FileCount)
                    .First())
                .ToList();
            var uniqueDecisions = validated.Decisions
                .GroupBy(
                    decision => Convert.ToHexString(decision.InfoHash),
                    StringComparer.Ordinal)
                .Select(group => group
                    .OrderByDescending(decision => IsReject(decision.DecisionCode))
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

            var insertedTorrents = await InsertTorrentsAsync(
                uniqueTorrents,
                connection,
                transaction,
                cancellationToken);
            await ExactHashTransactionLock.DeleteDecisionsForTorrentsAsync(
                uniqueTorrents.Select(torrent => torrent.InfoHash).ToArray(),
                connection,
                transaction,
                cancellationToken);
            var details = uniqueTorrents
                .Where(torrent => insertedTorrents.ContainsKey(torrent.InfoHash))
                .Select(torrent => new TorrentDetail
                {
                    TorrentId = insertedTorrents[torrent.InfoHash],
                    Payload = TorrentDetailCodec.Encode(
                        torrent.Files,
                        torrent.ExtensionSummaries)
                })
                .ToList();
            if (details.Count > 0)
                await CopyDetailsAsync(details, connection, cancellationToken);

            var changedDecisionHashes = await InsertDecisionsAsync(
                uniqueDecisions,
                connection,
                transaction,
                cancellationToken);

            await SearchOutboxWriter.EnqueueAsync(
                insertedTorrents.Values,
                connection,
                transaction,
                cancellationToken);

            var accepted = insertedTorrents.Count + changedDecisionHashes.Count;
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

    private static async Task<Dictionary<string, long>> InsertTorrentsAsync(
        List<Torrent> torrents,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (torrents.Count == 0)
            return new Dictionary<string, long>(StringComparer.Ordinal);

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO torrents (
                info_hash,
                name,
                total_length,
                file_count,
                created_at)
            SELECT decode(hash, 'hex'), name, total_length, file_count, created_at
              FROM unnest(
                @hashes::text[],
                @names::text[],
                @total_lengths::bigint[],
                @file_counts::int[],
                @created_at::timestamptz[])
                   AS incoming(hash, name, total_length, file_count, created_at)
            ON CONFLICT (info_hash) DO NOTHING
            RETURNING id, encode(info_hash, 'hex')
            """,
            connection,
            transaction);

        command.Parameters.AddWithValue("hashes", torrents.Select(t => t.InfoHash).ToArray());
        command.Parameters.AddWithValue("names", torrents.Select(t => t.Name).ToArray());
        command.Parameters.AddWithValue("total_lengths", torrents.Select(t => t.TotalLength).ToArray());
        command.Parameters.AddWithValue("file_counts", torrents.Select(t => t.FileCount).ToArray());
        command.Parameters.AddWithValue(
            "created_at",
            NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
            torrents.Select(t => t.CreatedAt).ToArray());

        var inserted = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            inserted.Add(reader.GetString(1), reader.GetInt64(0));
        return inserted;
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
                decision_code)
            SELECT decode(hash, 'hex'), decision_code
              FROM unnest(
                  @hashes::text[],
                  @decision_codes::smallint[])
                   AS d(hash, decision_code)
             WHERE NOT EXISTS (
                       SELECT 1
                         FROM torrents AS torrent
                        WHERE torrent.info_hash = decode(d.hash, 'hex'))
            ON CONFLICT (info_hash) DO NOTHING
            RETURNING encode(info_hash, 'hex')
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue(
            "hashes",
            decisions.Select(d => Convert.ToHexString(d.InfoHash).ToLowerInvariant()).ToArray());
        command.Parameters.AddWithValue(
            "decision_codes",
            NpgsqlDbType.Array | NpgsqlDbType.Smallint,
            decisions.Select(d => (short)d.DecisionCode).ToArray());

        var changed = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            changed.Add(reader.GetString(0));
        return changed;
    }

    private static async Task CopyDetailsAsync(
        List<TorrentDetail> details,
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var writer = await connection.BeginBinaryImportAsync(
            "COPY torrent_details (torrent_id, payload) FROM STDIN (FORMAT BINARY)",
            cancellationToken);
        foreach (var detail in details)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(detail.TorrentId, NpgsqlDbType.Bigint, cancellationToken);
            await writer.WriteAsync(detail.Payload, NpgsqlDbType.Bytea, cancellationToken);
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

    private static bool IsReject(MetadataDecisionCode code) =>
        code is MetadataDecisionCode.Reject or MetadataDecisionCode.RejectFileCap;
}
