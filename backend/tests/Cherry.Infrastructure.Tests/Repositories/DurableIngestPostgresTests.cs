using System.Security.Cryptography;
using System.Text;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Cherry.Infrastructure.Tests.Repositories;

[Collection("Postgres integration")]
public sealed class DurableIngestPostgresTests
{
    [Fact]
    public async Task TorrentAndDecisionStates_AreCrossTableDeduplicated()
    {
        var connectionString = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var db = new AppDbContext(options);
        await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await db.Database.MigrateAsync();

        var crawlerId = $"durable-cross-table-{Guid.NewGuid():N}";
        var infoHash = HashFor(Guid.NewGuid());
        var infoHashBytes = Convert.FromHexString(infoHash);
        var service = new DurableIngestService(db, new RecordingProcessedHashFilter());

        var hashOnly = await service.IngestAsync(
            DecisionBatch(crawlerId, 1, 1, infoHash, "hash_only"));
        Assert.Equal(1, hashOnly.Response.Accepted);
        Assert.Equal(0, hashOnly.Response.Duplicates);
        Assert.True(await db.MetadataDecisions.AnyAsync(
            decision => decision.InfoHash == infoHashBytes));

        var normalized = await service.IngestAsync(Batch(crawlerId, 1, 2, infoHash));
        Assert.Equal(1, normalized.Response.Accepted);
        Assert.Equal(0, normalized.Response.Duplicates);
        Assert.True(await db.Torrents.AnyAsync(torrent => torrent.InfoHash == infoHash));
        Assert.False(await db.MetadataDecisions.AnyAsync(
            decision => decision.InfoHash == infoHashBytes));

        var redundantReject = await service.IngestAsync(
            DecisionBatch(crawlerId, 1, 3, infoHash, "reject"));
        Assert.Equal(0, redundantReject.Response.Accepted);
        Assert.Equal(1, redundantReject.Response.Duplicates);
        Assert.False(await db.MetadataDecisions.AnyAsync(
            decision => decision.InfoHash == infoHashBytes));
        Assert.Equal(1, await db.Torrents.CountAsync(torrent => torrent.InfoHash == infoHash));
    }

    [Fact]
    public async Task ReceiptAndMetadata_AreAtomicReplayableAndStrictlySequential()
    {
        var connectionString = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var db = new AppDbContext(options);
        await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        await db.Database.MigrateAsync();

        var crawlerId = $"durable-test-{Guid.NewGuid():N}";
        const ulong epoch = 1;
        var firstHash = HashFor(Guid.NewGuid());
        var summaryHash = HashFor(Guid.NewGuid());
        var hashOnlyHash = HashFor(Guid.NewGuid());
        var rejectHash = HashFor(Guid.NewGuid());
        var service = new DurableIngestService(db, new RecordingProcessedHashFilter());

        var first = Batch(crawlerId, epoch, 1, firstHash);
        var firstResult = await service.IngestAsync(first);

        Assert.False(firstResult.IsConflict);
        Assert.True(firstResult.Response.Committed);
        Assert.Equal(crawlerId, firstResult.Response.CrawlerId);
        Assert.Equal(epoch, firstResult.Response.Epoch);
        Assert.Equal(1UL, firstResult.Response.StartSequence);
        Assert.Equal(1UL, firstResult.Response.EndSequence);
        Assert.Equal(first.Request.PayloadSha256, firstResult.Response.PayloadSha256);
        Assert.Equal(1, firstResult.Response.Accepted);
        Assert.Equal(0, firstResult.Response.Duplicates);

        var replay = await service.IngestAsync(first);

        Assert.False(replay.IsConflict);
        Assert.True(replay.Response.Committed);
        Assert.Equal(firstResult.Response.Accepted, replay.Response.Accepted);
        Assert.Equal(firstResult.Response.Duplicates, replay.Response.Duplicates);
        Assert.Equal(1, await db.Torrents.CountAsync(t => t.InfoHash == firstHash));
        Assert.Equal(1, await db.TorrentFiles.CountAsync(f => f.InfoHash == firstHash));

        var gapHash = HashFor(Guid.NewGuid());
        var gap = await service.IngestAsync(Batch(crawlerId, epoch, 3, gapHash));
        Assert.True(gap.IsConflict);
        Assert.False(gap.Response.Committed);
        Assert.Equal(2UL, gap.Response.ExpectedStart);
        Assert.False(await db.Torrents.AnyAsync(t => t.InfoHash == gapHash));

        var checksumConflictHash = HashFor(Guid.NewGuid());
        var checksumConflictBatch = Batch(crawlerId, epoch, 2, checksumConflictHash);
        checksumConflictBatch = checksumConflictBatch with
        {
            Request = CopyWithChecksum(checksumConflictBatch.Request, new string('0', 64))
        };
        var checksumConflict = await service.IngestAsync(checksumConflictBatch);
        Assert.True(checksumConflict.IsConflict);
        Assert.False(checksumConflict.Response.Committed);
        Assert.Equal(2UL, checksumConflict.Response.ExpectedStart);
        Assert.False(await db.Torrents.AnyAsync(t => t.InfoHash == checksumConflictHash));

        var second = MixedBatch(
            crawlerId,
            epoch,
            2,
            summaryHash,
            hashOnlyHash,
            rejectHash);
        var secondResult = await service.IngestAsync(second);
        Assert.False(secondResult.IsConflict);
        Assert.True(secondResult.Response.Committed);
        Assert.Equal(3, secondResult.Response.Accepted);
        Assert.Equal(0, secondResult.Response.Duplicates);
        Assert.Equal(4UL, secondResult.Response.EndSequence);

        var secondReplay = await service.IngestAsync(second);
        Assert.False(secondReplay.IsConflict);
        Assert.True(secondReplay.Response.Committed);
        Assert.Equal(3, secondReplay.Response.Accepted);
        Assert.Equal(0, secondReplay.Response.Duplicates);

        var overlap = await service.IngestAsync(first);
        Assert.True(overlap.IsConflict);
        Assert.False(overlap.Response.Committed);
        Assert.Equal(5UL, overlap.Response.ExpectedStart);

        var receipt = await db.DurableBatchReceipts.SingleAsync(
            item => item.CrawlerId == crawlerId && item.Epoch == (long)epoch);
        Assert.Equal(2, receipt.LastStartSequence);
        Assert.Equal(4, receipt.LastEndSequence);
        Assert.Equal(second.Request.PayloadSha256, receipt.LastPayloadSha256);
        Assert.Equal(3, receipt.LastAccepted);
        Assert.Equal(0, receipt.LastDuplicates);
        Assert.Equal(2, await db.Torrents.CountAsync(
            torrent => torrent.InfoHash == firstHash || torrent.InfoHash == summaryHash));
        Assert.Equal(2, await db.TorrentFiles.CountAsync(
            file => file.InfoHash == firstHash || file.InfoHash == summaryHash));

        var summary = await db.Torrents.SingleAsync(torrent => torrent.InfoHash == summaryHash);
        Assert.Equal(MetadataRetentionLevel.Summary, summary.RetainedLevel);
        Assert.False(summary.NeedsRefetch);
        Assert.Equal(100, summary.FileCount);
        Assert.Equal(1, await db.TorrentExtensionSummaries.CountAsync(
            extension => extension.InfoHash == summaryHash));

        var hashOnly = await db.MetadataDecisions.SingleAsync(
            decision => decision.InfoHash == Convert.FromHexString(hashOnlyHash));
        Assert.Equal(MetadataDecisionAction.HashOnly, hashOnly.Action);
        Assert.False(hashOnly.NeedsRefetch);
        var reject = await db.MetadataDecisions.SingleAsync(
            decision => decision.InfoHash == Convert.FromHexString(rejectHash));
        Assert.Equal(MetadataDecisionAction.Reject, reject.Action);
        Assert.False(reject.NeedsRefetch);

        var repository = new TorrentRepository(db, processedHashFilter: new RecordingProcessedHashFilter());
        var terminallyProcessed = await repository.CheckProcessedAsync(
            [firstHash, summaryHash, hashOnlyHash, rejectHash]);
        Assert.Contains(firstHash, terminallyProcessed);
        Assert.Contains(summaryHash, terminallyProcessed);
        Assert.Contains(hashOnlyHash, terminallyProcessed);
        Assert.Contains(rejectHash, terminallyProcessed);
    }

    private static ParsedDurableBatch Batch(
        string crawlerId,
        ulong epoch,
        ulong sequence,
        string infoHash)
    {
        var eventsJson = "[{" +
                         "\"info_hash\":\"" + infoHash + "\"," +
                         "\"encoding\":\"normalized\"," +
                         "\"policy_id\":\"integration-test\"," +
                         "\"first_seen\":\"2026-07-18T00:00:00Z\"," +
                         "\"region\":\"test\"," +
                         "\"normalized\":{" +
                         "\"name\":\"integration-test\"," +
                         "\"total_length\":100," +
                         "\"piece_length\":16384," +
                         "\"files\":[{\"path\":\"test.bin\",\"length\":100}]" +
                         "}}]";
        return BatchFromEvents(crawlerId, epoch, sequence, eventsJson);
    }

    private static ParsedDurableBatch MixedBatch(
        string crawlerId,
        ulong epoch,
        ulong startSequence,
        string summaryHash,
        string hashOnlyHash,
        string rejectHash)
    {
        var eventsJson = "[" +
                         "{\"info_hash\":\"" + summaryHash + "\"," +
                         "\"encoding\":\"summary\"," +
                         "\"policy_id\":\"integration-test\"," +
                         "\"first_seen\":\"2026-07-18T00:00:00Z\"," +
                         "\"region\":\"jp\"," +
                         "\"summary\":{" +
                         "\"name\":\"large-summary\"," +
                         "\"total_length\":1000," +
                         "\"file_count\":100," +
                         "\"representative_files\":[{\"path\":\"sample.bin\",\"length\":10}]," +
                         "\"extensions\":[{\"extension\":\".bin\",\"files\":10,\"bytes\":500}]" +
                         "}}," +
                         "{\"info_hash\":\"" + hashOnlyHash + "\"," +
                         "\"encoding\":\"hash_only\"," +
                         "\"policy_id\":\"integration-test\"," +
                         "\"region\":\"jp\"," +
                         "\"hash_only\":{\"reason\":\"capacity\"}}," +
                         "{\"info_hash\":\"" + rejectHash + "\"," +
                         "\"encoding\":\"reject\"," +
                         "\"policy_id\":\"integration-test\"," +
                         "\"region\":\"jp\"," +
                         "\"reject\":{\"reason\":\"policy\"}}" +
                         "]";
        return BatchFromEvents(crawlerId, epoch, startSequence, eventsJson);
    }

    private static ParsedDurableBatch DecisionBatch(
        string crawlerId,
        ulong epoch,
        ulong sequence,
        string infoHash,
        string encoding)
    {
        var eventsJson = "[{" +
                         "\"info_hash\":\"" + infoHash + "\"," +
                         "\"encoding\":\"" + encoding + "\"," +
                         "\"policy_id\":\"integration-test\"," +
                         "\"region\":\"test\"," +
                         "\"" + encoding + "\":{\"reason\":\"test\"}" +
                         "}]";
        return BatchFromEvents(crawlerId, epoch, sequence, eventsJson);
    }

    private static ParsedDurableBatch BatchFromEvents(
        string crawlerId,
        ulong epoch,
        ulong startSequence,
        string eventsJson)
    {
        // Count top-level objects without reserializing the checksum-covered
        // JSON. The helpers above use one occurrence of \"info_hash\" per event.
        var eventCount = eventsJson.Split("\"info_hash\"", StringSplitOptions.None).Length - 1;
        var endSequence = startSequence + checked((ulong)eventCount) - 1;
        var checksum = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(eventsJson)))
            .ToLowerInvariant();
        var json = "{" +
                   "\"crawler_id\":\"" + crawlerId + "\"," +
                   "\"epoch\":" + epoch + "," +
                   "\"start_sequence\":" + startSequence + "," +
                   "\"end_sequence\":" + endSequence + "," +
                   "\"payload_sha256\":\"" + checksum + "\"," +
                   "\"events\":" + eventsJson +
                   "}";
        return DurableBatchPayloadParser.Parse(Encoding.UTF8.GetBytes(json));
    }

    private static Cherry.Application.Dtos.DurableBatchRequest CopyWithChecksum(
        Cherry.Application.Dtos.DurableBatchRequest request,
        string checksum) =>
        new()
        {
            CrawlerId = request.CrawlerId,
            Epoch = request.Epoch,
            StartSequence = request.StartSequence,
            EndSequence = request.EndSequence,
            PayloadSha256 = checksum,
            Events = request.Events
        };

    private static string HashFor(Guid value) =>
        Convert.ToHexString(SHA1.HashData(value.ToByteArray())).ToLowerInvariant();

    private sealed class RecordingProcessedHashFilter : IProcessedHashFilter
    {
        public HashSet<string> Recorded { get; } = new(StringComparer.Ordinal);
        public bool IsReady => false;
        public bool MightContain(string infoHash) => Recorded.Contains(infoHash);
        public void RecordCandidates(IEnumerable<string> infoHashes) => Recorded.UnionWith(infoHashes);
    }
}
