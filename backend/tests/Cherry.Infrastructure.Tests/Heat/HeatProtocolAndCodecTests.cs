using System.Security.Cryptography;
using Cherry.Infrastructure.Heat;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatProtocolAndCodecTests
{
    // Golden bytes produced by the Go CHHT v1 encoder (uvarint + big endian fields).
    private const string GoldenBody =
        "4348485401000050ae02000102030405060708090a0b0c0d0e0f1011121302" +
        "00000000000000010000000000000002fffefdfcfbfaf9f8f7f6f5f4f3f2" +
        "f1f0efeeedec01000000000000002a";
    private const string GoldenSha =
        "2f260987e99b008f8a5cac8100a7287aaa1e8c7cb3cbf4dd975bba65fe0df387";
    private const string GoldenSignature =
        "17c7ce782ca1bdeb70a934b7081854253e585e4500df406622a29fc6f2fe0a7a";

    [Fact]
    public void ParsesExactSharedGoGoldenVector()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var options = Options();
        var batch = ChhtV1Protocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103, GoldenSignature, GoldenSha,
            "0123456789abcdef0123456789abcdef"u8,
            options, day.ToDateTime(TimeOnly.FromTimeSpan(TimeSpan.FromHours(12)), DateTimeKind.Utc));

        Assert.Equal(day, batch.Day);
        Assert.Equal(3, batch.RecordCount);
        Assert.Equal([1L, 2L], batch.Groups[0].ActorFingerprints);
    }

    [Fact]
    public void RejectsNonCanonicalUvarint()
    {
        var canonical = Convert.FromHexString(GoldenBody);
        var payload = canonical[..9].Concat(new byte[] { 0x82, 0x00 }).Concat(canonical[10..]).ToArray();
        var digest = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
        var secret = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var signature = Convert.ToHexString(
            ChhtV1Protocol.ComputeSignature(payload, "sg-1", 9, 100, 109, digest, secret)).ToLowerInvariant();
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        Assert.Throws<ChhtProtocolException>(() => ChhtV1Protocol.ParseAndAuthenticate(
            payload, "sg-1", 9, 100, 109, signature, digest, secret, Options(),
            day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
    }

    [Fact]
    public void ClosedDayIsCheckedOnlyAfterCanonicalAuthentication()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var secret = "0123456789abcdef0123456789abcdef"u8.ToArray();
        var closed = Assert.Throws<ChhtProtocolException>(() => ChhtV1Protocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103,
            GoldenSignature, GoldenSha, secret, Options(),
            day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Expired, closed.Error);
        Assert.NotNull(closed.ClosedReceipt);
        Assert.Equal(GoldenSha, Convert.ToHexString(closed.ClosedReceipt!.PayloadSha256).ToLowerInvariant());

        var unauthenticated = Assert.Throws<ChhtProtocolException>(() => ChhtV1Protocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103,
            new string('0', 64), GoldenSha, secret, Options(),
            day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, unauthenticated.Error);
        Assert.Null(unauthenticated.ClosedReceipt);
    }

    [Fact]
    public void CompletionUsesDomainSeparatedAuthenticatedCanonicalIdentity()
    {
        var secret = "0123456789abcdef0123456789abcdef"u8.ToArray();
        var day = new DateOnly(2026, 7, 20);
        const string signature = "db60e250cfc7b952ab946e3ff9f36615fb8915d9d9420f38aee383ed941ec4ae";
        Assert.Equal(signature, Convert.ToHexString(ChhtV1Protocol.ComputeCompletionSignature(
            "jp-crawler-01", day, 72_623_859_790_382_856, 100, 104, secret)).ToLowerInvariant());

        var parsed = ChhtV1Protocol.ParseAndAuthenticateCompletion(
            "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1", signature,
            secret, Options(), day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc));
        Assert.True(parsed.Clean);

        var unauthenticated = Assert.Throws<ChhtProtocolException>(() =>
            ChhtV1Protocol.ParseAndAuthenticateCompletion(
                "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1",
                new string('0', 64), secret, Options(),
                day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, unauthenticated.Error);
        Assert.Null(unauthenticated.ClosedCompletion);

        var closed = Assert.Throws<ChhtProtocolException>(() =>
            ChhtV1Protocol.ParseAndAuthenticateCompletion(
                "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1",
                signature, secret, Options(),
                day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Expired, closed.Error);
        Assert.NotNull(closed.ClosedCompletion);
    }

    [Fact]
    public void ExpectedCrawlersRequireIndependentTransportSecrets()
    {
        var encoded = Convert.ToBase64String(Enumerable.Repeat((byte)7, 32).ToArray());
        var options = new HeatOptions
        {
            Enabled = true,
            ExpectedCrawlerIds = ["sg-1", "jp-1"],
            CrawlerSecrets = new Dictionary<string, string> { ["sg-1"] = encoded }
        };
        Assert.Throws<InvalidOperationException>(options.ValidateExpectedCrawlerSecrets);
        var complete = new HeatOptions
        {
            Enabled = true,
            ExpectedCrawlerIds = ["sg-1", "jp-1"],
            CrawlerSecrets = new Dictionary<string, string>
            {
                ["sg-1"] = encoded,
                ["jp-1"] = Convert.ToBase64String(Enumerable.Repeat((byte)8, 32).ToArray())
            }
        };
        complete.ValidateExpectedCrawlerSecrets();
        Assert.Equal(32, complete.DecodeSecret("jp-1").Length);

        var duplicated = new HeatOptions
        {
            Enabled = true,
            ExpectedCrawlerIds = ["sg-1", "jp-1"],
            CrawlerSecrets = new Dictionary<string, string> { ["sg-1"] = encoded, ["jp-1"] = encoded }
        };
        Assert.Throws<InvalidOperationException>(duplicated.ValidateExpectedCrawlerSecrets);
    }

    [Fact]
    public void AuthenticatedButUnexpectedCrawlerIsRejected()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var options = new HeatOptions
        {
            Enabled = true,
            ExpectedCrawlerIds = ["jp-crawler-01"],
            MaxRequestBytes = 1024 * 1024,
            MaxRecordsPerBatch = 100
        };
        var unknownSecret = options.DecodeSecret("rogue-crawler");
        var signature = Convert.ToHexString(ChhtV1Protocol.ComputeSignature(
            payload, "rogue-crawler", 1, 1, 1, GoldenSha, unknownSecret)).ToLowerInvariant();

        var batchError = Assert.Throws<ChhtProtocolException>(() =>
            ChhtV1Protocol.ParseAndAuthenticate(
                payload, "rogue-crawler", 1, 1, 1, signature, GoldenSha,
                unknownSecret, options,
                day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, batchError.Error);

        var completionSignature = Convert.ToHexString(ChhtV1Protocol.ComputeCompletionSignature(
            "rogue-crawler", day, 1, 1, 1, unknownSecret)).ToLowerInvariant();
        var completionError = Assert.Throws<ChhtProtocolException>(() =>
            ChhtV1Protocol.ParseAndAuthenticateCompletion(
                "rogue-crawler", day.ToString("yyyy-MM-dd"), 1, 1, 1, "1",
                completionSignature, unknownSecret, options,
                day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, completionError.Error);
    }

    [Fact]
    public void FrameRoundTripAndProjectionBoundariesAreExact()
    {
        var entries = new[]
        {
            new HeatFrameEntry(64, 3),
            new HeatFrameEntry(65, 4),
            new HeatFrameEntry(128, 5)
        };
        var frames = HeatFrameCodec.Encode(entries);
        Assert.Equal(entries.Where(entry => (entry.TorrentId & 63) == 0),
            HeatFrameCodec.Decode(0, frames[0].EntryCount, frames[0].Payload));

        var target = new DateOnly(2026, 7, 18);
        var source = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>
        {
            [target] = [new(64, 10)],
            [target.AddDays(-1)] = [new(64, 1)],
            [target.AddDays(-7)] = [new(64, 7)],
            [target.AddDays(-15)] = [new(64, 15)],
            [target.AddDays(-30)] = [new(64, 30)],
            [target.AddDays(-2)] = [new(65, 999)]
        };
        var document = Assert.Single(HeatProjectionMath.BuildIncremental(target, 0, source));
        Assert.Equal(new HeatProjectionDocument(64, 10, 11, 18, 33), document);
    }

    [Fact]
    public void ProjectionOverflowStopsFailClosed()
    {
        var target = new DateOnly(2026, 7, 18);
        var source = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>
        {
            [target] = [new(64, long.MaxValue)],
            [target.AddDays(-1)] = [new(64, 1)]
        };
        Assert.Throws<OverflowException>(() => HeatProjectionMath.BuildFull(target, 0, source));
    }

    [Fact]
    public void ExplicitPartialDayDoesNotContributeButLaterDayRecovers()
    {
        var first = new DateOnly(2026, 7, 16);
        var completeFirst = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>
        {
            [first] = [new(64, 5)]
        };
        Assert.Equal(5, Assert.Single(HeatProjectionMath.BuildFull(first, 0, completeFirst)).Heat1d);

        // July 17 is a sealed partial day, represented by an absent frame in
        // projection math (its manifest remains in the source digest).
        var partial = HeatProjectionMath.BuildIncremental(first.AddDays(1), 0, completeFirst);
        Assert.Equal(0, Assert.Single(partial).Heat1d);

        var recoveredSource = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>(completeFirst)
        {
            [first.AddDays(2)] = [new(64, 2)]
        };
        var recovered = Assert.Single(HeatProjectionMath.BuildIncremental(first.AddDays(2), 0, recoveredSource));
        Assert.Equal(2, recovered.Heat1d);
        Assert.Equal(7, recovered.Heat7d);
        var mask = (1 << 0) | (1 << 2); // D complete, D-1 partial, D-2 complete.
        Assert.Equal(2, HeatCoverage.Count(mask, 7));
    }

    [Fact]
    public async Task AccumulatorReplayAndSequenceConflictAreDurable()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-{Guid.NewGuid():N}");
        var options = Options(directory);
        var service = new HeatAccumulatorService(
            options, new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var day = DateOnly.FromDateTime(DateTime.UtcNow);
            var batch = new ChhtBatch("sg-1", day, 1, 100, 109,
                [new ChhtHashGroup(new byte[20], [1, 2])], new byte[32]);
            var accepted = await service.SubmitAsync(batch, CancellationToken.None);
            var replay = await service.SubmitAsync(batch, CancellationToken.None);
            var conflict = await service.SubmitAsync(batch with { PayloadSha256 = Enumerable.Repeat((byte)1, 32).ToArray() },
                CancellationToken.None);
            var next = await service.SubmitAsync(batch with
            {
                Sequence = 110,
                EndSequence = 110,
                Groups = [new ChhtHashGroup(new byte[20], [2, 3])],
                PayloadSha256 = Enumerable.Repeat((byte)2, 32).ToArray()
            }, CancellationToken.None);

            Assert.Equal(HeatAcceptStatus.Accepted, accepted.Status);
            Assert.Equal(2, accepted.Inserted);
            Assert.Equal(HeatAcceptStatus.Replay, replay.Status);
            Assert.Equal(HeatAcceptStatus.Conflict, conflict.Status);
            Assert.Equal(HeatAcceptStatus.Accepted, next.Status);
            Assert.Equal(1, next.Inserted);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task SealBarrierRejectsEveryLaterAdmission()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-{Guid.NewGuid():N}");
        var service = new HeatAccumulatorService(
            Options(directory), new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var day = DateOnly.FromDateTime(DateTime.UtcNow);
            Assert.True(await service.SealBarrierAsync(day, CancellationToken.None));
            var batch = new ChhtBatch("sg-1", day, 1, 1, 1,
                [new ChhtHashGroup(new byte[20], [1])], new byte[32]);
            var attempts = await Task.WhenAll(Enumerable.Range(0, 64)
                .Select(_ => service.SubmitAsync(batch, CancellationToken.None)));
            Assert.All(attempts, result => Assert.Equal(HeatAcceptStatus.Conflict, result.Status));
            Assert.False(File.Exists(service.PathForDay(day)));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task CompletionRequiresExactReceiptChainAndFreezesCrawlerDay()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-{Guid.NewGuid():N}");
        var service = new HeatAccumulatorService(
            Options(directory), new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var day = DateOnly.FromDateTime(DateTime.UtcNow);
            var batch = new ChhtBatch("sg-1", day, 7, 100, 109,
                [new ChhtHashGroup(new byte[20], [1])], new byte[32]);
            Assert.Equal(HeatAcceptStatus.Accepted,
                (await service.SubmitAsync(batch, CancellationToken.None)).Status);

            var gap = await service.SubmitCompletionAsync(
                new ChhtCompletion("sg-1", day, 7, 99, 110, true), CancellationToken.None);
            Assert.Equal(HeatCompletionStatus.Conflict, gap.Status);
            var completion = new ChhtCompletion("sg-1", day, 7, 100, 110, true);
            Assert.Equal(HeatCompletionStatus.Accepted,
                (await service.SubmitCompletionAsync(completion, CancellationToken.None)).Status);
            Assert.Equal(HeatCompletionStatus.Replay,
                (await service.SubmitCompletionAsync(completion, CancellationToken.None)).Status);
            Assert.Equal(HeatCompletionStatus.Conflict,
                (await service.SubmitCompletionAsync(completion with { NextSequence = 111 },
                    CancellationToken.None)).Status);
            Assert.Equal(HeatAcceptStatus.Conflict,
                (await service.SubmitAsync(batch with
                {
                    Sequence = 110, EndSequence = 110,
                    PayloadSha256 = Enumerable.Repeat((byte)2, 32).ToArray()
                }, CancellationToken.None)).Status);

            await using var sqlite = await OpenSqliteAsync(service.PathForDay(day));
            Assert.True(await HeatDaySealer.HasVerifiedCompletionAsync(sqlite, "sg-1", CancellationToken.None));
            Assert.False(await HeatDaySealer.HasVerifiedCompletionAsync(sqlite, "jp-1", CancellationToken.None));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task EmptyDayCompletionIsExplicitAndIdempotent()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-{Guid.NewGuid():N}");
        var service = new HeatAccumulatorService(
            Options(directory), new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
        await service.StartAsync(CancellationToken.None);
        try
        {
            var day = DateOnly.FromDateTime(DateTime.UtcNow);
            var completion = new ChhtCompletion("sg-1", day, 9, 42, 42, true);
            Assert.Equal(HeatCompletionStatus.Accepted,
                (await service.SubmitCompletionAsync(completion, CancellationToken.None)).Status);
            Assert.Equal(HeatCompletionStatus.Replay,
                (await service.SubmitCompletionAsync(completion, CancellationToken.None)).Status);
            await using var sqlite = await OpenSqliteAsync(service.PathForDay(day));
            Assert.True(await HeatDaySealer.HasVerifiedCompletionAsync(sqlite, "sg-1", CancellationToken.None));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            Directory.Delete(directory, true);
        }
    }

    private static HeatOptions Options(string? directory = null) => new HeatOptions
    {
        Enabled = true,
        DataDirectory = directory ?? ".",
        MaxRequestBytes = 1024 * 1024,
        MaxRecordsPerBatch = 100,
        ChannelCapacity = 8
    };

    private static async Task<SqliteConnection> OpenSqliteAsync(string path)
    {
        var connection = new SqliteConnection($"Data Source={path};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync();
        return connection;
    }
}
