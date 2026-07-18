using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Cherry.Application.Dtos;
using Cherry.Application.Services;
using Cherry.Domain.Entities;
using Xunit;

namespace Cherry.Infrastructure.Tests.Application;

public sealed class DurableBatchProtocolTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public void ValidateAndMap_RejectsMissingOrUnsupportedSchemaVersion(int schemaVersion)
    {
        var request = ValidRequest();
        var incompatible = new DurableBatchRequest
        {
            SchemaVersion = schemaVersion,
            CrawlerId = request.CrawlerId,
            Epoch = request.Epoch,
            StartSequence = request.StartSequence,
            EndSequence = request.EndSequence,
            PayloadSha256 = request.PayloadSha256,
            Events = request.Events
        };

        var exception = Assert.Throws<DurableBatchValidationException>(() =>
            DurableBatchValidator.ValidateAndMap(incompatible));
        Assert.Contains("schema_version", exception.Message);
    }

    [Fact]
    public void Parse_HashesTheExactRawEventsArrayBytes()
    {
        const string rawEvents = "[\n  { \"info_hash\" : \"0000000000000000000000000000000000000001\", \"encoding\":\"hash_only\", \"decision_code\":1 }\n]";
        var json = "{" +
                   "\"schema_version\":2," +
                   "\"crawler_id\":\"crawler-a\"," +
                   "\"epoch\":1," +
                   "\"start_sequence\":1," +
                   "\"end_sequence\":1," +
                   "\"payload_sha256\":\"" + new string('0', 64) + "\"," +
                   "\"events\":" + rawEvents +
                   "}";

        var parsed = DurableBatchPayloadParser.Parse(Encoding.UTF8.GetBytes(json));
        var expected = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(rawEvents)))
            .ToLowerInvariant();

        Assert.Equal(expected, parsed.CalculatedPayloadSha256);
        Assert.Single(parsed.Request.Events!);
    }

    [Fact]
    public void Parse_RejectsDuplicateTopLevelEvents()
    {
        var json = "{" +
                   "\"schema_version\":2," +
                   "\"crawler_id\":\"crawler-a\"," +
                   "\"epoch\":1," +
                   "\"start_sequence\":1," +
                   "\"end_sequence\":1," +
                   "\"payload_sha256\":\"" + new string('0', 64) + "\"," +
                   "\"events\":[]," +
                   "\"events\":[]" +
                   "}";

        var exception = Assert.Throws<JsonException>(() =>
            DurableBatchPayloadParser.Parse(Encoding.UTF8.GetBytes(json)));

        Assert.Contains("Duplicate", exception.Message);
    }

    [Fact]
    public void Parse_RejectsUnknownLegacyRawFields()
    {
        var json = "{" +
                   "\"schema_version\":2," +
                   "\"crawler_id\":\"crawler-a\"," +
                   "\"epoch\":1," +
                   "\"start_sequence\":1," +
                   "\"end_sequence\":1," +
                   "\"payload_sha256\":\"" + new string('0', 64) + "\"," +
                   "\"events\":[{" +
                   "\"info_hash\":\"0000000000000000000000000000000000000001\"," +
                   "\"encoding\":\"normalized\"," +
                   "\"raw\":\"must-not-be-ignored\"," +
                   "\"normalized\":{\"name\":\"a\",\"total_length\":1,\"files\":[{\"path\":\"a\",\"length\":1}]}" +
                   "}]}";

        Assert.Throws<JsonException>(() =>
            DurableBatchPayloadParser.Parse(Encoding.UTF8.GetBytes(json)));
    }

    [Fact]
    public void ParseAndValidate_AcceptsTheCompleteFourWayTypedWireUnion()
    {
        var eventsJson = "[" +
                         "{\"info_hash\":\"" + HashFor(11) + "\",\"encoding\":\"normalized\"," +
                         "\"normalized\":{\"name\":\"full\",\"total_length\":1," +
                         "\"files\":[{\"path\":\"full.bin\",\"length\":1}]}}," +
                         "{\"info_hash\":\"" + HashFor(12) + "\",\"encoding\":\"summary\"," +
                         "\"summary\":{\"name\":\"summary\",\"total_length\":100,\"file_count\":10," +
                         "\"representative_files\":[{\"path\":\"sample.bin\",\"length\":1}]," +
                         "\"extensions\":[{\"extension\":\".bin\",\"files\":10,\"bytes\":100}]}}," +
                         "{\"info_hash\":\"" + HashFor(13) + "\",\"encoding\":\"hash_only\",\"decision_code\":3}," +
                         "{\"info_hash\":\"" + HashFor(14) + "\",\"encoding\":\"reject\",\"decision_code\":4}" +
                         "]";
        var checksum = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(eventsJson)))
            .ToLowerInvariant();
        var json = "{" +
                   "\"schema_version\":2," +
                   "\"crawler_id\":\"crawler-a\",\"epoch\":1," +
                   "\"start_sequence\":1,\"end_sequence\":4," +
                   "\"payload_sha256\":\"" + checksum + "\"," +
                   "\"events\":" + eventsJson + "}";

        var parsed = DurableBatchPayloadParser.Parse(Encoding.UTF8.GetBytes(json));
        var validated = DurableBatchValidator.ValidateAndMap(parsed.Request);

        Assert.Equal(checksum, parsed.CalculatedPayloadSha256);
        Assert.Equal(4, validated.EventCount);
        Assert.Equal(2, validated.Torrents.Count);
        Assert.Equal(2, validated.Decisions.Count);
        Assert.Contains(validated.Decisions, decision =>
            decision.DecisionCode == MetadataDecisionCode.HashOnlyFileCap);
        Assert.Contains(validated.Decisions, decision =>
            decision.DecisionCode == MetadataDecisionCode.RejectFileCap);
    }

    [Fact]
    public void ValidateAndMap_MapsNormalizedMetadata()
    {
        var request = ValidRequest();

        var validated = DurableBatchValidator.ValidateAndMap(request);

        var torrent = Assert.Single(validated.Torrents);
        Assert.Empty(validated.Decisions);
        Assert.Equal(request.Events![0].InfoHash, torrent.InfoHash);
        Assert.Equal("example", torrent.Name);
        Assert.Equal(123, torrent.TotalLength);
        Assert.Equal(123, Assert.Single(torrent.Files).Length);
    }

    [Fact]
    public void ValidateAndMap_PreservesBoundedSummaryAndMarksItIncomplete()
    {
        var summary = new DurableBatchEvent
        {
            InfoHash = HashFor(2),
            Encoding = "summary",
            Summary = new DurableSummaryMetadata
            {
                Name = "large",
                TotalLength = 1_000,
                FileCount = 100,
                RepresentativeFiles = [new DurableBatchFile { Path = "sample.bin", Length = 10 }],
                Extensions =
                [
                    new DurableExtensionSummary { Extension = ".bin", Files = 10, Bytes = 500 }
                ]
            }
        };
        var request = ValidRequest(events: [summary]);

        var validated = DurableBatchValidator.ValidateAndMap(request);

        var torrent = Assert.Single(validated.Torrents);
        Assert.Equal(100, torrent.FileCount);
        Assert.Single(torrent.Files);
        Assert.Single(torrent.ExtensionSummaries);
    }

    [Theory]
    [InlineData("hash_only", MetadataDecisionCode.HashOnlyFileCap)]
    [InlineData("reject", MetadataDecisionCode.RejectFileCap)]
    public void ValidateAndMap_PreservesTypedDecisions(
        string encoding,
        MetadataDecisionCode expectedCode)
    {
        var item = new DurableBatchEvent
        {
            InfoHash = HashFor(3),
            Encoding = encoding,
            DecisionCode = (short)expectedCode
        };

        var validated = DurableBatchValidator.ValidateAndMap(ValidRequest(events: [item]));

        Assert.Empty(validated.Torrents);
        var decision = Assert.Single(validated.Decisions);
        Assert.Equal(expectedCode, decision.DecisionCode);
    }

    [Fact]
    public void ValidateAndMap_RejectsSequenceRangeThatDoesNotMatchEvents()
    {
        var request = ValidRequest(endSequence: 2);

        var exception = Assert.Throws<DurableBatchValidationException>(() =>
            DurableBatchValidator.ValidateAndMap(request));

        Assert.Contains("sequence range", exception.Message);
    }

    private static DurableBatchRequest ValidRequest(
        ulong endSequence = 1,
        List<DurableBatchEvent>? events = null) =>
        new()
        {
            SchemaVersion = 2,
            CrawlerId = "crawler-a",
            Epoch = 1,
            StartSequence = 1,
            EndSequence = endSequence,
            PayloadSha256 = new string('0', 64),
            Events = events ??
            [
                new DurableBatchEvent
                {
                    InfoHash = HashFor(1),
                    Encoding = "normalized",
                    FirstSeen = DateTimeOffset.UnixEpoch,
                    Normalized = new DurableNormalizedMetadata
                    {
                        Name = "example",
                        TotalLength = 123,
                        Files = [new DurableBatchFile { Path = "example.bin", Length = 123 }]
                    }
                }
            ]
        };

    private static string HashFor(int value) => value.ToString("x40");
}
