using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cherry.Application.Dtos;

namespace Cherry.Application.Services;

public sealed record ParsedDurableBatch(
    DurableBatchRequest Request,
    string CalculatedPayloadSha256);

public static class DurableBatchPayloadParser
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = false,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };

    public static ParsedDurableBatch Parse(ReadOnlyMemory<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
            throw new DurableBatchValidationException("Request body is empty.");

        var reader = new Utf8JsonReader(utf8Json.Span, new JsonReaderOptions
        {
            AllowTrailingCommas = false,
            CommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 64
        });

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("The durable batch request must be a JSON object.");

        var propertyNames = new HashSet<string>(StringComparer.Ordinal);
        ReadOnlySpan<byte> rawEvents = default;
        var foundEvents = false;
        var foundEnd = false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                foundEnd = true;
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected a property name in the durable batch request.");

            var propertyName = reader.GetString()
                ?? throw new JsonException("A request property name cannot be null.");
            if (!propertyNames.Add(propertyName))
                throw new JsonException($"Duplicate top-level property '{propertyName}'.");

            if (!reader.Read())
                throw new JsonException($"Property '{propertyName}' has no value.");

            var valueStart = checked((int)reader.TokenStartIndex);
            using var value = JsonDocument.ParseValue(ref reader);
            if (!string.Equals(propertyName, "events", StringComparison.Ordinal))
                continue;

            if (value.RootElement.ValueKind != JsonValueKind.Array)
                throw new JsonException("Property 'events' must be a JSON array.");

            var valueEnd = checked((int)reader.BytesConsumed);
            rawEvents = utf8Json.Span[valueStart..valueEnd];
            foundEvents = true;
        }

        if (!foundEnd)
            throw new JsonException("The durable batch request is incomplete.");
        if (reader.Read())
            throw new JsonException("Trailing JSON content is not allowed.");
        if (!foundEvents)
            throw new JsonException("Property 'events' is required.");

        var request = JsonSerializer.Deserialize<DurableBatchRequest>(utf8Json.Span, SerializerOptions)
            ?? throw new JsonException("The durable batch request cannot be null.");
        var checksum = Convert.ToHexString(SHA256.HashData(rawEvents)).ToLowerInvariant();
        return new ParsedDurableBatch(request, checksum);
    }
}
