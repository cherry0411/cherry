using System.Buffers.Binary;
using System.Security.Cryptography;
using Cherry.Infrastructure.Heat;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatAccumulatorServiceTests
{
    private static readonly byte[] DailySecret = Enumerable.Repeat((byte)11, 32).ToArray();

    [Fact]
    public async Task BulkDailyStagePreserves4096SparseRecordsReplayAndConflict()
    {
        var directory = TemporaryDirectory();
        var service = Service(directory);
        await service.StartAsync(CancellationToken.None);
        var now = DateTime.UtcNow;
        var day = DateOnly.FromDateTime(now);
        var groups = Enumerable.Range(0, 4_096)
            .Select(index => new ChhtHashGroup(Hash(index), [index + 1L]))
            .ToArray();
        var batch = Batch("jp-1", day, (byte)now.Hour, 7, 1, groups, 1);
        try
        {
            var accepted = await service.SubmitAsync(batch, CancellationToken.None);
            var replay = await service.SubmitAsync(batch, CancellationToken.None);
            var boundaryGroups = Enumerable.Range(4_096, 4_097)
                .Select(index => new ChhtHashGroup(Hash(index), [index + 1L]))
                .ToArray();
            var boundary = await service.SubmitAsync(
                Batch("jp-1", day, (byte)now.Hour, 7, 4_097, boundaryGroups, 9),
                CancellationToken.None);
            var conflict = await service.SubmitAsync(
                batch with { PayloadSha256 = Enumerable.Repeat((byte)0x5a, 32).ToArray() },
                CancellationToken.None);

            Assert.Equal(HeatAcceptStatus.Accepted, accepted.Status);
            Assert.Equal(4_096, accepted.Received);
            Assert.Equal(4_096, accepted.Inserted);
            Assert.Equal(4_097UL, accepted.ExpectedSequence);
            Assert.Equal(HeatAcceptStatus.Replay, replay.Status);
            Assert.Equal(4_096, replay.Inserted);
            Assert.Equal(HeatAcceptStatus.Accepted, boundary.Status);
            Assert.Equal(4_097, boundary.Inserted);
            Assert.Equal(8_194UL, boundary.ExpectedSequence);
            Assert.Equal(HeatAcceptStatus.Conflict, conflict.Status);
            Assert.Equal(0, conflict.Inserted);

            await using var sqlite = await OpenReadOnlyAsync(service.PathForDay(day));
            Assert.Equal(8_193, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM hashes"));
            Assert.Equal(8_193, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM seen"));
            Assert.Equal(2, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM receipts"));
            Assert.Equal(8_193, await ScalarAsync(sqlite, "SELECT SUM(inserted_count) FROM receipts"));
            Assert.Equal(UInt64Bytes(8_194), await BlobAsync(
                sqlite, "SELECT next_sequence FROM receipt_heads"));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task GroupCommitClearsStagingAndCountsOverlappingMultiActorCommandsExactly()
    {
        var directory = TemporaryDirectory();
        var service = Service(directory, commitBatchRequests: 2);
        var now = DateTime.UtcNow;
        var day = DateOnly.FromDateTime(now);
        var hashA = Hash(1);
        var hashB = Hash(2);
        var first = Batch("sg-1", day, (byte)now.Hour, 9, 1,
            [new ChhtHashGroup(hashA, [1, 2]), new ChhtHashGroup(hashB, [1])], 2);
        var second = Batch("sg-1", day, (byte)now.Hour, 9, 4,
            [new ChhtHashGroup(hashA, [2, 3]), new ChhtHashGroup(hashB, [1, 2])], 3);
        // Queue both commands before starting the single reader so this always
        // exercises two PersistAsync calls in one daily FULL transaction.
        var firstTask = service.SubmitAsync(first, CancellationToken.None);
        var secondTask = service.SubmitAsync(second, CancellationToken.None);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var results = await Task.WhenAll(firstTask, secondTask);
            Assert.Collection(results,
                result =>
                {
                    Assert.Equal(HeatAcceptStatus.Accepted, result.Status);
                    Assert.Equal(3, result.Inserted);
                },
                result =>
                {
                    Assert.Equal(HeatAcceptStatus.Accepted, result.Status);
                    Assert.Equal(2, result.Inserted);
                });

            var otherCrawler = Batch("jp-1", day, (byte)now.Hour, 4, 1,
                [new ChhtHashGroup(hashA, [1, 3]), new ChhtHashGroup(hashB, [2])], 4);
            var duplicate = await service.SubmitAsync(otherCrawler, CancellationToken.None);
            Assert.Equal(HeatAcceptStatus.Accepted, duplicate.Status);
            Assert.Equal(0, duplicate.Inserted);

            await using var sqlite = await OpenReadOnlyAsync(service.PathForDay(day));
            Assert.Equal(2, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM hashes"));
            Assert.Equal(5, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM seen"));
            Assert.Equal([3L, 2L], await Int64RowsAsync(sqlite,
                "SELECT COUNT(*) FROM seen GROUP BY hash_id ORDER BY hash_id"));
            Assert.Equal([3L, 2L], await Int64RowsAsync(sqlite,
                "SELECT inserted_count FROM receipts WHERE crawler_id='sg-1' ORDER BY start_sequence"));
            Assert.Equal(0, await ScalarAsync(sqlite,
                "SELECT inserted_count FROM receipts WHERE crawler_id='jp-1'"));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task BulkDailyStageKeepsIndependentDayScopedHmacEncoding()
    {
        var directory = TemporaryDirectory();
        var service = Service(directory);
        await service.StartAsync(CancellationToken.None);
        var now = DateTime.UtcNow;
        var currentDay = DateOnly.FromDateTime(now);
        var previousDay = currentDay.AddDays(-1);
        long[] actors = [long.MinValue, -1, 0, long.MaxValue];
        var current = Batch("sg-1", currentDay, (byte)now.Hour, 1, 1,
            [new ChhtHashGroup(Hash(10), actors)], 5);
        var previous = Batch("sg-1", previousDay, (byte)now.Hour, 1, 1,
            [new ChhtHashGroup(Hash(10), actors)], 6);
        try
        {
            Assert.Equal(4, (await service.SubmitAsync(current, CancellationToken.None)).Inserted);
            Assert.Equal(4, (await service.SubmitAsync(previous, CancellationToken.None)).Inserted);

            await using var currentSqlite = await OpenReadOnlyAsync(service.PathForDay(currentDay));
            await using var previousSqlite = await OpenReadOnlyAsync(service.PathForDay(previousDay));
            var currentStored = await Int64RowsAsync(currentSqlite, "SELECT actor FROM seen ORDER BY actor");
            var previousStored = await Int64RowsAsync(previousSqlite, "SELECT actor FROM seen ORDER BY actor");
            var currentExpected = actors.Select(actor => ExpectedDailyActor(currentDay, actor)).Order().ToArray();
            var previousExpected = actors.Select(actor => ExpectedDailyActor(previousDay, actor)).Order().ToArray();
            Assert.Equal(currentExpected, currentStored);
            Assert.Equal(previousExpected, previousStored);
            Assert.False(currentStored.SequenceEqual(previousStored));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            DeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task GroupCommitStagingFailureRollsBackEarlierCommandAndRemainsRetryable()
    {
        var directory = TemporaryDirectory();
        var service = Service(directory, commitBatchRequests: 2);
        var now = DateTime.UtcNow;
        var day = DateOnly.FromDateTime(now);
        var valid = Batch("sg-1", day, (byte)now.Hour, 11, 1,
            [new ChhtHashGroup(Hash(20), [1])], 7);
        var invalid = Batch("sg-1", day, (byte)now.Hour, 11, 2,
            [new ChhtHashGroup(new byte[19], [2])], 8);
        var firstTask = service.SubmitAsync(valid, CancellationToken.None);
        var secondTask = service.SubmitAsync(invalid, CancellationToken.None);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var failed = await Task.WhenAll(firstTask, secondTask);
            Assert.All(failed, result => Assert.Equal(HeatAcceptStatus.Failed, result.Status));

            await using (var sqlite = await OpenReadOnlyAsync(service.PathForDay(day)))
            {
                Assert.Equal(0, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM hashes"));
                Assert.Equal(0, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM seen"));
                Assert.Equal(0, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM receipts"));
                Assert.Equal(0, await ScalarAsync(sqlite, "SELECT COUNT(*) FROM receipt_heads"));
            }

            var retry = await service.SubmitAsync(valid, CancellationToken.None);
            Assert.Equal(HeatAcceptStatus.Accepted, retry.Status);
            Assert.Equal(1, retry.Inserted);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            DeleteDirectory(directory);
        }
    }

    private static HeatAccumulatorService Service(string directory, int commitBatchRequests = 1)
    {
        var options = new HeatOptions
        {
            Enabled = true,
            DataDirectory = directory,
            DailyActorSecret = Convert.ToBase64String(DailySecret),
            ChannelCapacity = 8,
            CommitBatchRequests = commitBatchRequests,
            RollingMaxBytes = 1024L * 1024 * 1024,
            RollingMinFreeBytes = 0
        };
        return new HeatAccumulatorService(
            options, new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
    }

    private static ChhtBatch Batch(
        string crawler,
        DateOnly day,
        byte hour,
        ulong epoch,
        ulong sequence,
        IReadOnlyList<ChhtHashGroup> groups,
        byte payloadTag)
    {
        var records = checked((ulong)groups.Sum(group => group.ActorFingerprints.Count));
        return new ChhtBatch(
            crawler, day, hour, epoch, sequence, checked(sequence + records - 1), groups,
            SHA256.HashData([payloadTag]));
    }

    private static long ExpectedDailyActor(DateOnly day, long actor)
    {
        Span<byte> context = stackalloc byte["cherry/heat/storage-day/v2\0"u8.Length + 4];
        "cherry/heat/storage-day/v2\0"u8.CopyTo(context);
        BinaryPrimitives.WriteInt32BigEndian(context[^4..], day.DayNumber);
        var dailyKey = HMACSHA256.HashData(DailySecret, context);
        Span<byte> actorBytes = stackalloc byte[8];
        Span<byte> digest = stackalloc byte[32];
        BinaryPrimitives.WriteUInt64BigEndian(actorBytes, unchecked((ulong)actor));
        HMACSHA256.HashData(dailyKey, actorBytes, digest);
        return unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(digest));
    }

    private static byte[] Hash(int value)
    {
        var hash = new byte[20];
        BinaryPrimitives.WriteInt32BigEndian(hash.AsSpan(16), value);
        return hash;
    }

    private static byte[] UInt64Bytes(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        return bytes;
    }

    private static string TemporaryDirectory() =>
        Path.Combine(Path.GetTempPath(), $"cherry-heat-accumulator-{Guid.NewGuid():N}");

    private static void DeleteDirectory(string directory)
    {
        if (Directory.Exists(directory)) Directory.Delete(directory, true);
    }

    private static async Task<SqliteConnection> OpenReadOnlyAsync(string path)
    {
        var connection = new SqliteConnection($"Data Source={path};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<long> ScalarAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private static async Task<byte[]> BlobAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (byte[])(await command.ExecuteScalarAsync() ?? throw new InvalidDataException("Missing BLOB"));
    }

    private static async Task<long[]> Int64RowsAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<long>();
        while (await reader.ReadAsync()) rows.Add(reader.GetInt64(0));
        return rows.ToArray();
    }
}
