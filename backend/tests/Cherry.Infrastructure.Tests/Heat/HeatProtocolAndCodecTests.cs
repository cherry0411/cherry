using System.Security.Cryptography;
using System.Buffers.Binary;
using Cherry.Infrastructure.Heat;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Cherry.Infrastructure.Tests.Heat;

public sealed class HeatProtocolAndCodecTests
{
    // Golden bytes produced by the Go CHHT v2 encoder (uvarint + big endian fields).
    private const string GoldenBody =
        "4348485402000050ae0c02000102030405060708090a0b0c0d0e0f1011121302" +
        "00000000000000010000000000000002fffefdfcfbfaf9f8f7f6f5f4f3f2" +
        "f1f0efeeedec01000000000000002a";
    private const string GoldenSha =
        "f9677f7b639d9e92f4a4d1710ca5aec8da298618ba8c5c27ae0274bb549f1369";
    private const string GoldenSignature =
        "f412c893652e24aa3999b4d42db2417764b17fc737a45675a502c0c8c8700a7e";

    [Fact]
    public void ParsesExactSharedGoGoldenVector()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var options = Options();
        var batch = ChhtProtocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103, GoldenSignature, GoldenSha,
            "0123456789abcdef0123456789abcdef"u8,
            options, day.ToDateTime(TimeOnly.FromTimeSpan(TimeSpan.FromHours(12)), DateTimeKind.Utc));

        Assert.Equal(day, batch.Day);
        Assert.Equal(12, batch.Hour);
        Assert.Equal(3, batch.RecordCount);
        Assert.Equal([1L, 2L], batch.Groups[0].ActorFingerprints);
    }

    [Fact]
    public void RejectsNonCanonicalUvarint()
    {
        var canonical = Convert.FromHexString(GoldenBody);
        var payload = canonical[..10].Concat(new byte[] { 0x82, 0x00 }).Concat(canonical[11..]).ToArray();
        var digest = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
        var secret = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var signature = Convert.ToHexString(
            ChhtProtocol.ComputeSignature(payload, "sg-1", 9, 100, 109, digest, secret)).ToLowerInvariant();
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        Assert.Throws<ChhtProtocolException>(() => ChhtProtocol.ParseAndAuthenticate(
            payload, "sg-1", 9, 100, 109, signature, digest, secret, Options(),
            day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
    }

    [Fact]
    public void RejectsAuthenticatedFutureUtcHour()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var error = Assert.Throws<ChhtProtocolException>(() => ChhtProtocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103,
            GoldenSignature, GoldenSha,
            "0123456789abcdef0123456789abcdef"u8,
            Options(),
            day.ToDateTime(new TimeOnly(11, 30), DateTimeKind.Utc)));
        Assert.Contains("future UTC hour", error.Message);
        Assert.Equal(ChhtProtocolError.Future, error.Error);
        Assert.Null(error.ClosedReceipt);
    }

    [Fact]
    public void AuthenticatedTomorrowBatchIsRetryableAndNeverGetsClosedReceipt()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var unixDay = BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(5, 4));
        BinaryPrimitives.WriteUInt32BigEndian(payload.AsSpan(5, 4), unixDay + 1);
        var secret = "0123456789abcdef0123456789abcdef"u8.ToArray();
        var digest = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
        var signature = Convert.ToHexString(ChhtProtocol.ComputeSignature(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103, digest, secret))
            .ToLowerInvariant();
        var today = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays((int)unixDay);

        var error = Assert.Throws<ChhtProtocolException>(() => ChhtProtocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103,
            signature, digest, secret, Options(),
            today.ToDateTime(new TimeOnly(23, 59), DateTimeKind.Utc)));

        Assert.Equal(ChhtProtocolError.Future, error.Error);
        Assert.Null(error.ClosedReceipt);
    }

    [Fact]
    public void ClosedDayIsCheckedOnlyAfterCanonicalAuthentication()
    {
        var payload = Convert.FromHexString(GoldenBody);
        var day = DateOnly.FromDateTime(DateTime.UnixEpoch).AddDays(20_654);
        var secret = "0123456789abcdef0123456789abcdef"u8.ToArray();
        var closed = Assert.Throws<ChhtProtocolException>(() => ChhtProtocol.ParseAndAuthenticate(
            payload, "jp-crawler-01", 72_623_859_790_382_856, 100, 103,
            GoldenSignature, GoldenSha, secret, Options(),
            day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Expired, closed.Error);
        Assert.NotNull(closed.ClosedReceipt);
        Assert.Equal(GoldenSha, Convert.ToHexString(closed.ClosedReceipt!.PayloadSha256).ToLowerInvariant());

        var unauthenticated = Assert.Throws<ChhtProtocolException>(() => ChhtProtocol.ParseAndAuthenticate(
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
        Assert.Equal(signature, Convert.ToHexString(ChhtProtocol.ComputeCompletionSignature(
            "jp-crawler-01", day, 72_623_859_790_382_856, 100, 104, secret)).ToLowerInvariant());

        var parsed = ChhtProtocol.ParseAndAuthenticateCompletion(
            "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1", signature,
            secret, Options(), day.AddDays(1).ToDateTime(new TimeOnly(0, 10), DateTimeKind.Utc));
        Assert.True(parsed.Clean);

        var future = Assert.Throws<ChhtProtocolException>(() =>
            ChhtProtocol.ParseAndAuthenticateCompletion(
                "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1",
                signature, secret, Options(),
                day.ToDateTime(new TimeOnly(23, 59), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Future, future.Error);
        Assert.Null(future.ClosedCompletion);

        var unauthenticated = Assert.Throws<ChhtProtocolException>(() =>
            ChhtProtocol.ParseAndAuthenticateCompletion(
                "jp-crawler-01", "2026-07-20", 72_623_859_790_382_856, 100, 104, "1",
                new string('0', 64), secret, Options(),
                day.AddDays(2).ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, unauthenticated.Error);
        Assert.Null(unauthenticated.ClosedCompletion);

        var closed = Assert.Throws<ChhtProtocolException>(() =>
            ChhtProtocol.ParseAndAuthenticateCompletion(
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
        var signature = Convert.ToHexString(ChhtProtocol.ComputeSignature(
            payload, "rogue-crawler", 1, 1, 1, GoldenSha, unknownSecret)).ToLowerInvariant();

        var batchError = Assert.Throws<ChhtProtocolException>(() =>
            ChhtProtocol.ParseAndAuthenticate(
                payload, "rogue-crawler", 1, 1, 1, signature, GoldenSha,
                unknownSecret, options,
                day.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc)));
        Assert.Equal(ChhtProtocolError.Authentication, batchError.Error);

        var completionSignature = Convert.ToHexString(ChhtProtocol.ComputeCompletionSignature(
            "rogue-crawler", day, 1, 1, 1, unknownSecret)).ToLowerInvariant();
        var completionError = Assert.Throws<ChhtProtocolException>(() =>
            ChhtProtocol.ParseAndAuthenticateCompletion(
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
        Assert.Equal(new DailyHeatProjectionDocument(64, 11, 11, 18), document);
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
        Assert.Equal(5, Assert.Single(HeatProjectionMath.BuildFull(first, 0, completeFirst)).Heat3d);

        // July 17 is a sealed partial day, represented by an absent frame in
        // projection math (its manifest remains in the source digest).
        var partial = HeatProjectionMath.BuildIncremental(first.AddDays(1), 0, completeFirst);
        Assert.Empty(partial);

        var recoveredSource = new Dictionary<DateOnly, IReadOnlyList<HeatFrameEntry>>(completeFirst)
        {
            [first.AddDays(2)] = [new(64, 2)]
        };
        var recovered = Assert.Single(HeatProjectionMath.BuildIncremental(first.AddDays(2), 0, recoveredSource));
        Assert.Equal(7, recovered.Heat3d);
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
            var batch = new ChhtBatch("sg-1", day, (byte)DateTime.UtcNow.Hour, 1, 100, 109,
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
            var batch = new ChhtBatch("sg-1", day, (byte)DateTime.UtcNow.Hour, 1, 1, 1,
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
            var batch = new ChhtBatch("sg-1", day, (byte)DateTime.UtcNow.Hour, 7, 100, 109,
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

    [Fact]
    public async Task Rolling24hCountsExactUniqueActorsAndExcludesCurrentPartialHour()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var current = HeatRollingStore.UnixHour(DateTime.UtcNow);
        var target = current - 1;
        var hash = new byte[20];
        hash[0] = 7;
        try
        {
            await store.ApplyAsync(
                [
                    BatchAt(target - 23, hash, [1], 1),
                    BatchAt(target, hash, [1, 3], 2),
                    BatchAt(current, hash, [1, 2], 3),
                    BatchAt(target - 24, hash, [4], 4)
                ],
                CancellationToken.None);

            var first = await store.ReadChangesAsync(target, CancellationToken.None);
            var firstChange = Assert.Single(first.Changes);
            Assert.Equal(2, firstChange.CurrentCount); // actors 1 and 3; actor 1 counts once.

            await store.CommitProjectionAsync(
                target, [(hash, 2L, firstChange.Revision)], [], CancellationToken.None);
            await store.ApplyAsync(
                [BatchAt(target, hash, [5], 5)],
                CancellationToken.None);
            var late = await store.ReadChangesAsync(target, CancellationToken.None);
            var lateChange = Assert.Single(late.Changes);
            Assert.Equal(target, late.ProjectedHour);
            Assert.Equal(3, lateChange.CurrentCount);
            Assert.Equal(2, lateChange.ProjectedCount);
            await store.CommitProjectionAsync(
                target, [(hash, 3L, lateChange.Revision)], [], CancellationToken.None);

            // The remaining current-hour actor is deferred, not regrouped on
            // every lifecycle poll for the same closed-hour target.
            Assert.Empty((await store.ReadChangesAsync(target, CancellationToken.None)).Changes);

            var next = await store.ReadChangesAsync(target + 1, CancellationToken.None);
            var nextChange = Assert.Single(next.Changes);
            Assert.Equal(4, nextChange.CurrentCount); // current-only actor 2 is now complete.

            await Assert.ThrowsAsync<InvalidDataException>(() => store.ApplyAsync(
                [BatchAt(current + 1, hash, [9], 5)], CancellationToken.None));
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingStaleCommitCannotClearLateCompletedHourActor()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var target = HeatRollingStore.UnixHour(DateTime.UtcNow) - 1;
        var hash = new byte[20];
        hash[0] = 8;
        try
        {
            await store.ApplyAsync([BatchAt(target, hash, [1], 1)], CancellationToken.None);
            var stale = Assert.Single(
                (await store.ReadChangesAsync(target, CancellationToken.None)).Changes);

            // Deterministically interleave a late actor after snapshot and before
            // the Meili acknowledgement/commit of that stale snapshot.
            await store.ApplyAsync([BatchAt(target, hash, [2], 2)], CancellationToken.None);
            await store.CommitProjectionAsync(
                target, [(hash, stale.CurrentCount, stale.Revision)], [], CancellationToken.None);

            var retry = Assert.Single(
                (await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
            Assert.Equal(2, retry.CurrentCount);
            Assert.Equal(1, retry.ProjectedCount);
            Assert.True(retry.Revision > stale.Revision);

            await store.CommitProjectionAsync(
                target, [(hash, retry.CurrentCount, retry.Revision)], [], CancellationToken.None);
            Assert.Empty((await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingUnmappedHashesRetryHourlyThenGarbageCollectAfterExpiry()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var target = HeatRollingStore.UnixHour(DateTime.UtcNow) - 1;
        var hash = new byte[20];
        hash[0] = 9;
        try
        {
            await store.ApplyAsync([BatchAt(target, hash, [1], 1)], CancellationToken.None);
            var first = Assert.Single(
                (await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
            await store.CommitProjectionAsync(
                target, [], [(hash, first.Revision)], CancellationToken.None);

            Assert.Empty((await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
            var retry = Assert.Single(
                (await store.ReadChangesAsync(target + 1, CancellationToken.None)).Changes);
            await store.CommitProjectionAsync(
                target + 1, [], [(hash, retry.Revision)], CancellationToken.None);

            var expired = Assert.Single(
                (await store.ReadChangesAsync(target + 25, CancellationToken.None)).Changes);
            Assert.Equal(0, expired.CurrentCount);
            await store.CommitProjectionAsync(
                target + 25, [], [(hash, expired.Revision)], CancellationToken.None);
            Assert.Empty((await store.ReadChangesAsync(target + 26, CancellationToken.None)).Changes);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingCurrentHourRowsDeferButLateClosedHourMutationRetriesImmediately()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var target = HeatRollingStore.UnixHour(DateTime.UtcNow) - 1;
        var hashes = Enumerable.Range(1, 128)
            .Select(value => SHA256.HashData(BitConverter.GetBytes(value))[..20])
            .ToArray();
        try
        {
            await store.ApplyAsync(
                hashes.Select((hash, index) => BatchAt(
                    target + 1, hash, [17L], checked((ulong)index + 1))).ToArray(),
                CancellationToken.None);
            var initial = (await store.ReadChangesAsync(target, CancellationToken.None)).Changes;
            Assert.Equal(hashes.Length, initial.Count);
            Assert.All(initial, change => Assert.Equal(0, change.CurrentCount));
            await store.CommitProjectionAsync(
                target,
                initial.Select(change =>
                    (change.InfoHash, change.CurrentCount, change.Revision)).ToArray(),
                [],
                CancellationToken.None);

            Assert.Empty((await store.ReadChangesAsync(target, CancellationToken.None)).Changes);

            // The same actor arrives late for the closed hour. It becomes the
            // previous_seen_hour behind its current-hour observation; the
            // trigger clears deferral and the revision CAS forces reprocessing.
            await store.ApplyAsync(
                [BatchAt(target, hashes[0], [17L], 10_000)], CancellationToken.None);
            var late = Assert.Single(
                (await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
            Assert.Equal(1, late.CurrentCount);
            await store.CommitProjectionAsync(
                target, [(late.InfoHash, late.CurrentCount, late.Revision)], [],
                CancellationToken.None);

            var nextHour = (await store.ReadChangesAsync(target + 1, CancellationToken.None)).Changes;
            Assert.Equal(hashes.Length, nextHour.Count);
            Assert.All(nextHour, change => Assert.Equal(1, change.CurrentCount));
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingCoverageStaysUnknownWithoutAuthenticatedCrawlerHourClosures()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var current = HeatRollingStore.UnixHour(DateTime.UtcNow);
        try
        {
            await store.MarkRuntimeStartAsync(current + 1, CancellationToken.None);
            await store.CommitProjectionAsync(current - 1, [], [], CancellationToken.None);
            Assert.Equal(0, (await store.GetStatusAsync(CancellationToken.None)).CoverageHours);

            // Neither storage uptime, projected numeric values, a zero-event
            // hour, nor a restart proves that both crawler spools reached an
            // authenticated ACK closure for the hour.
            await store.CommitProjectionAsync(current + 1, [], [], CancellationToken.None);
            Assert.Equal(0, (await store.GetStatusAsync(CancellationToken.None)).CoverageHours);
            await store.CommitProjectionAsync(current + 24, [], [], CancellationToken.None);
            Assert.Equal(0, (await store.GetStatusAsync(CancellationToken.None)).CoverageHours);

            await store.MarkRuntimeStartAsync(current + 30, CancellationToken.None);
            Assert.Equal(0, (await store.GetStatusAsync(CancellationToken.None)).CoverageHours);
            await store.CommitProjectionAsync(current + 30, [], [], CancellationToken.None);
            Assert.Equal(0, (await store.GetStatusAsync(CancellationToken.None)).CoverageHours);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingPrivacyMaintenanceSecurelyDeletesAndReclaimsExpiredPages()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var store = new HeatRollingStore(Options(directory));
        var target = HeatRollingStore.UnixHour(DateTime.UtcNow) - 1;
        var hash = new byte[20];
        hash[0] = 10;
        var actors = Enumerable.Range(1, 5_000).Select(value => (long)value).ToArray();
        try
        {
            await store.ApplyAsync([BatchAt(target, hash, actors, 1)], CancellationToken.None);
            var initial = Assert.Single(
                (await store.ReadChangesAsync(target, CancellationToken.None)).Changes);
            await store.CommitProjectionAsync(
                target, [(hash, initial.CurrentCount, initial.Revision)], [], CancellationToken.None);
            var peak = await store.GetPrivacyStatusAsync(CancellationToken.None);
            Assert.True(peak.SecureDelete);

            var expired = Assert.Single(
                (await store.ReadChangesAsync(target + 24, CancellationToken.None)).Changes);
            Assert.Equal(0, expired.CurrentCount);
            // The expiration transaction itself is the privacy boundary. It
            // must truncate its WAL even when daily VACUUM is not due yet.
            var expirationWal = store.Path + "-wal";
            Assert.True(!File.Exists(expirationWal) || new FileInfo(expirationWal).Length == 0);
            await store.CommitProjectionAsync(
                target + 24, [(hash, 0L, expired.Revision)], [], CancellationToken.None);
            await store.RunPrivacyMaintenanceAsync(
                target + 24, force: true, CancellationToken.None);

            var compacted = await store.GetPrivacyStatusAsync(CancellationToken.None);
            Assert.True(compacted.SecureDelete);
            Assert.Equal(0, compacted.FreePages);
            Assert.True(compacted.PageCount < peak.PageCount);
            var walPath = store.Path + "-wal";
            Assert.True(!File.Exists(walPath) || new FileInfo(walPath).Length == 0);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task RollingCapacityWatermarkFailsBeforeAcknowledgableIngest()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-rolling-{Guid.NewGuid():N}");
        var options = Options(directory);
        var store = new HeatRollingStore(new HeatOptions
        {
            Enabled = true,
            DataDirectory = directory,
            DailyActorSecret = options.DailyActorSecret,
            RollingMaxBytes = 1,
            RollingMinFreeBytes = 0
        });
        try
        {
            var exception = await Assert.ThrowsAsync<HeatRollingCapacityException>(
                () => store.PrepareForIngestAsync(CancellationToken.None));
            Assert.True(exception.Status.Exhausted);
            Assert.True(exception.Status.UsedBytes > exception.Status.MaxBytes);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task AccumulatorCapacityFailureDoesNotCommitDailyReceiptOrAck()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"cherry-heat-{Guid.NewGuid():N}");
        var defaults = Options(directory);
        var options = new HeatOptions
        {
            Enabled = true,
            DataDirectory = directory,
            DailyActorSecret = defaults.DailyActorSecret,
            RollingMaxBytes = 1,
            RollingMinFreeBytes = 0,
            ChannelCapacity = 8,
            CommitBatchRequests = 1
        };
        var service = new HeatAccumulatorService(
            options, new HeatRuntimeMetrics(), NullLogger<HeatAccumulatorService>.Instance);
        await service.StartAsync(CancellationToken.None);
        var now = DateTime.UtcNow;
        var batch = new ChhtBatch(
            "sg-1", DateOnly.FromDateTime(now), (byte)now.Hour, 1, 1, 1,
            [new ChhtHashGroup(new byte[20], [1])], new byte[32]);
        try
        {
            var result = await service.SubmitAsync(batch, CancellationToken.None);
            Assert.Equal(HeatAcceptStatus.Failed, result.Status);
            Assert.Equal("Heat storage capacity exhausted", result.Error);
            Assert.False(File.Exists(service.PathForDay(batch.Day)));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    private static ChhtBatch BatchAt(
        long unixHour,
        byte[] hash,
        IReadOnlyList<long> actors,
        ulong sequence)
    {
        var instant = DateTimeOffset.FromUnixTimeSeconds(unixHour * 3600);
        return new ChhtBatch(
            "sg-1",
            DateOnly.FromDateTime(instant.UtcDateTime),
            (byte)instant.Hour,
            1,
            sequence,
            sequence,
            [new ChhtHashGroup(hash, actors)],
            SHA256.HashData(BitConverter.GetBytes(sequence)));
    }

    private static HeatOptions Options(string? directory = null) => new HeatOptions
    {
        Enabled = true,
        DataDirectory = directory ?? ".",
        DailyActorSecret = Convert.ToBase64String(Enumerable.Repeat((byte)11, 32).ToArray()),
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
