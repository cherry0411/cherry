using Cherry.Domain.Entities;
using Cherry.Infrastructure.Storage;
using Xunit;

namespace Cherry.Infrastructure.Tests.Storage;

public sealed class TorrentDetailCodecTests
{
    [Fact]
    public void Encode_IsDeterministicAndRoundTripsWithoutRetentionKind()
    {
        var files = new[]
        {
            new TorrentFile { PathText = "目录/第二集.mkv", Length = 200 },
            new TorrentFile { PathText = "目录/第一集.mkv", Length = 100 },
            new TorrentFile { PathText = "readme.txt", Length = 3 }
        };
        var extensions = new[]
        {
            new TorrentExtensionSummary { Extension = "txt", FileCount = 1, TotalLength = 3 },
            new TorrentExtensionSummary { Extension = "mkv", FileCount = 2, TotalLength = 300 }
        };

        var first = TorrentDetailCodec.Encode(files, extensions);
        var second = TorrentDetailCodec.Encode(files.Reverse().ToArray(), extensions.Reverse().ToArray());
        var decoded = TorrentDetailCodec.Decode(first);

        Assert.Equal(first, second);
        Assert.Equal(TorrentDetailCodec.CurrentVersion, first[0]);
        Assert.Equal(
            new[] { "readme.txt", "目录/第一集.mkv", "目录/第二集.mkv" },
            decoded.Files.Select(file => file.PathText));
        Assert.Equal(new long[] { 3, 100, 200 }, decoded.Files.Select(file => file.Length));
        Assert.Equal(new[] { "mkv", "txt" }, decoded.ExtensionSummaries.Select(item => item.Extension));
        var payloadText = System.Text.Encoding.UTF8.GetString(first);
        Assert.DoesNotContain("normalized", payloadText);
        Assert.DoesNotContain("summary", payloadText);
    }

    [Fact]
    public void DuplicatePathAndLength_AreAllowedForLegacyCompatibility()
    {
        var payload = TorrentDetailCodec.Encode(
            [
                new TorrentFile { PathText = "duplicate.bin", Length = 1 },
                new TorrentFile { PathText = "duplicate.bin", Length = 1 }
            ],
            []);

        var decoded = TorrentDetailCodec.Decode(payload);

        Assert.Equal(2, decoded.Files.Count);
        Assert.All(decoded.Files, file =>
        {
            Assert.Equal("duplicate.bin", file.PathText);
            Assert.Equal(1, file.Length);
        });
    }

    [Fact]
    public void EmptyDetail_HasMinimalCanonicalRepresentation()
    {
        var payload = TorrentDetailCodec.Encode([], []);

        Assert.Equal(new byte[] { 1, 0, 0 }, payload);
        var decoded = TorrentDetailCodec.Decode(payload);
        Assert.Empty(decoded.Files);
        Assert.Empty(decoded.ExtensionSummaries);
    }

    [Fact]
    public void VersionOne_MatchesBenchmarkCrossLanguageFixture()
    {
        var payload = TorrentDetailCodec.Encode(
            [
                new TorrentFile { PathText = "a/c.bin", Length = 130 },
                new TorrentFile { PathText = "a/b.txt", Length = 3 }
            ],
            [
                new TorrentExtensionSummary { Extension = ".txt", FileCount = 1, TotalLength = 3 },
                new TorrentExtensionSummary { Extension = ".bin", FileCount = 1, TotalLength = 130 }
            ]);

        Assert.Equal(
            "01020007612f622e747874030205632e62696e820102042e62696e018201042e7478740103",
            Convert.ToHexString(payload).ToLowerInvariant());
    }

    [Theory]
    [MemberData(nameof(MaliciousPayloads))]
    public void Decode_RejectsMalformedOrNonCanonicalPayload(byte[] payload)
    {
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Decode(payload));
    }

    [Fact]
    public void Encode_RejectsInvalidInputsBeforePersistence()
    {
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [new TorrentFile { PathText = "bad\0path", Length = 1 }], []));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [new TorrentFile { PathText = "", Length = 1 }], []));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [new TorrentFile { PathText = "negative", Length = -1 }], []));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [new TorrentFile { PathText = "\ud800", Length = 1 }], []));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [],
            [new TorrentExtensionSummary { Extension = "x", FileCount = 0, TotalLength = 0 }]));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [],
            [new TorrentExtensionSummary { Extension = "", FileCount = 1, TotalLength = 0 }]));
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(
            [],
            [
                new TorrentExtensionSummary { Extension = "mkv", FileCount = 1, TotalLength = 1 },
                new TorrentExtensionSummary { Extension = "mkv", FileCount = 1, TotalLength = 1 }
            ]));

        var tooMany = Enumerable.Range(0, TorrentDetailCodec.MaxFileEntries + 1)
            .Select(index => new TorrentFile { PathText = index.ToString(), Length = index })
            .ToArray();
        Assert.Throws<InvalidDataException>(() => TorrentDetailCodec.Encode(tooMany, []));
    }

    public static IEnumerable<object[]> MaliciousPayloads()
    {
        yield return [Array.Empty<byte>()];
        yield return [new byte[] { 2, 0, 0 }]; // unsupported version
        yield return [new byte[] { 1, 0x80, 0, 0 }]; // overlong count
        yield return [new byte[] { 1, 1 }]; // truncated file
        yield return [new byte[] { 1, 1, 1, 0, 0, 0 }]; // first prefix exceeds previous path
        yield return [new byte[] { 1, 1, 0, 1, 0xff, 0, 0 }]; // invalid UTF-8
        yield return [new byte[] { 1, 0, 0, 0 }]; // trailing byte
        yield return [new byte[] { 1, 2, 0, 1, (byte)'b', 0, 0, 1, (byte)'a', 0, 0 }];
        yield return [new byte[] { 1, 2, 0, 2, (byte)'a', (byte)'b', 0, 0, 2, (byte)'a', (byte)'c', 0, 0 }];
        yield return [new byte[] { 1, 2, 0, 1, (byte)'a', 2, 1, 0, 1, 0 }]; // equal path, decreasing length
        yield return [new byte[] { 1, 1, 0, 1, 0, 0, 0 }]; // NUL in path
        yield return [new byte[]
        {
            1, 1, 0, 1, (byte)'x',
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            0
        }]; // file length exceeds Int64
        yield return [new byte[] { 1, 0, 0x81, 0x01 }]; // 129 extension rows
    }
}
