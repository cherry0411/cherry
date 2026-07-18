using Cherry.Domain.Entities;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Cherry.Infrastructure.Tests.Repositories;

[Collection("Postgres integration")]
public sealed class TorrentRepositoryPostgresTests
{
    [Fact]
    public async Task ExactAuthority_RoundTripsTorrentAndRejectedHashes()
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
        var filter = new RecordingProcessedHashFilter();
        var repository = new TorrentRepository(db, processedHashFilter: filter);
        // Use per-run identities so the real PostgreSQL test is repeatable
        // against a reused developer/CI database.
        var torrentHash = HashFor(Guid.NewGuid());
        var rejectedHash = HashFor(Guid.NewGuid());
        var missingHash = HashFor(Guid.NewGuid());

        var firstInsert = await repository.BulkInsertTorrentsAsync(
            [Torrent(torrentHash)],
            CancellationToken.None);
        var duplicateInsert = await repository.BulkInsertTorrentsAsync(
            [Torrent(torrentHash)],
            CancellationToken.None);
        var firstReject = await repository.AddRejectedHashesAsync(
            [rejectedHash],
            CancellationToken.None);
        var duplicateReject = await repository.AddRejectedHashesAsync(
            [rejectedHash],
            CancellationToken.None);

        Assert.Equal([torrentHash], firstInsert);
        Assert.Empty(duplicateInsert);
        Assert.Equal([rejectedHash], firstReject);
        Assert.Empty(duplicateReject);
        Assert.Contains(torrentHash, filter.Recorded);
        Assert.Contains(rejectedHash, filter.Recorded);

        var processed = await repository.CheckProcessedAsync(
            [torrentHash, rejectedHash, missingHash],
            CancellationToken.None);
        Assert.Equal(
            new HashSet<string> { torrentHash, rejectedHash },
            processed.ToHashSet());

        var streamed = new HashSet<string>();
        await foreach (var hash in repository.StreamProcessedHashesAsync())
            streamed.Add(hash);
        Assert.Contains(torrentHash, streamed);
        Assert.Contains(rejectedHash, streamed);
    }

    private static Torrent Torrent(string infoHash) => new()
    {
        InfoHash = infoHash,
        Name = "integration-test",
        PieceLength = 16_384,
        TotalLength = 100,
        FileCount = 1,
        Source = "test",
        Files = [new TorrentFile { PathText = "test.bin", Length = 100 }]
    };

    private static string HashFor(Guid value) =>
        Convert.ToHexString(System.Security.Cryptography.SHA1.HashData(value.ToByteArray()))
            .ToLowerInvariant();

    private sealed class RecordingProcessedHashFilter : IProcessedHashFilter
    {
        public HashSet<string> Recorded { get; } = new(StringComparer.Ordinal);
        public bool IsReady => false;
        public bool MightContain(string infoHash) => Recorded.Contains(infoHash);
        public void RecordCandidates(IEnumerable<string> infoHashes) => Recorded.UnionWith(infoHashes);
    }
}
