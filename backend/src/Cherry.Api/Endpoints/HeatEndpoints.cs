using System.Globalization;
using Cherry.Infrastructure.Heat;

namespace Cherry.Api.Endpoints;

public static class HeatEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapPost("/api/v1/heat/batches", AcceptAsync)
            .WithTags("Heat")
            .WithSummary("Durably accept a canonical CHHT v1 heat batch")
            .Produces(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status401Unauthorized)
            .Produces(StatusCodes.Status409Conflict)
            .Produces(StatusCodes.Status413PayloadTooLarge)
            .Produces(StatusCodes.Status429TooManyRequests)
            .Produces(StatusCodes.Status503ServiceUnavailable);
        app.MapPost("/api/v1/heat/completions", CompleteAsync)
            .WithTags("Heat")
            .WithSummary("Durably close a lossless crawler UTC day")
            .Produces(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status401Unauthorized)
            .Produces(StatusCodes.Status409Conflict)
            .Produces(StatusCodes.Status429TooManyRequests)
            .Produces(StatusCodes.Status503ServiceUnavailable);
    }

    private static async Task<IResult> AcceptAsync(
        HttpRequest request,
        HeatOptions options,
        HeatAccumulatorService? accumulator,
        CancellationToken cancellationToken)
    {
        if (!options.Enabled || accumulator is null)
            return Results.Json(new { error = "Heat ingestion is disabled" }, statusCode: 503);
        if (!string.Equals(request.ContentType?.Split(';', 2)[0].Trim(), ChhtV1Protocol.MediaType,
                StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new { error = $"Content-Type must be {ChhtV1Protocol.MediaType}" });
        if (request.ContentLength is > 0 && request.ContentLength > options.MaxRequestBytes)
            return Results.Json(new { error = "CHHT request is too large" }, statusCode: 413);

        try
        {
            var crawler = RequiredHeader(request, "X-CHHT-Crawler");
            var epoch = UInt64Header(request, "X-CHHT-Epoch");
            var sequence = UInt64Header(request, "X-CHHT-Sequence");
            var endSequence = UInt64Header(request, "X-CHHT-End-Sequence");
            var signature = RequiredHeader(request, "X-CHHT-Signature");
            var digest = RequiredHeader(request, "X-CHHT-Payload-SHA256");
            var payload = await ReadBoundedAsync(request.Body, options.MaxRequestBytes, cancellationToken);
            var batch = ChhtV1Protocol.ParseAndAuthenticate(
                payload, crawler, epoch, sequence, endSequence, signature, digest,
                options.DecodeSecret(crawler), options, DateTime.UtcNow);
            var result = await accumulator.SubmitAsync(batch, cancellationToken);
            var response = new
            {
                crawler = batch.CrawlerId,
                day = batch.Day,
                epoch = batch.Epoch,
                startSequence = batch.Sequence,
                endSequence = batch.EndSequence,
                payloadSha256 = Convert.ToHexString(batch.PayloadSha256).ToLowerInvariant(),
                received = result.Received,
                inserted = result.Inserted,
                nextSequence = result.ExpectedSequence,
                replay = result.Status == HeatAcceptStatus.Replay,
                code = result.Error == "UTC day is sealing or sealed" ? "day_closed" : null,
                error = result.Error
            };
            return result.Status switch
            {
                HeatAcceptStatus.Accepted or HeatAcceptStatus.Replay => Results.Ok(response),
                HeatAcceptStatus.Conflict when result.Error == "UTC day is sealing or sealed" =>
                    Results.Json(response, statusCode: 410),
                HeatAcceptStatus.Conflict => Results.Json(response, statusCode: 409),
                HeatAcceptStatus.Backpressure => Results.Json(response, statusCode: 429),
                HeatAcceptStatus.Failed when result.Error == "Heat storage unavailable" =>
                    Results.Json(response, statusCode: 507),
                _ => Results.Json(response, statusCode: 503)
            };
        }
        catch (RequestTooLargeException exception)
        {
            return Results.Json(new { error = exception.Message }, statusCode: 413);
        }
        catch (ChhtProtocolException exception)
        {
            return exception.Error switch
            {
                ChhtProtocolError.Expired when exception.ClosedReceipt is { } receipt => Results.Json(
                    new
                    {
                        code = "day_closed",
                        crawler = receipt.Crawler,
                        day = receipt.Day,
                        epoch = receipt.Epoch,
                        startSequence = receipt.StartSequence,
                        endSequence = receipt.EndSequence,
                        nextSequence = checked(receipt.EndSequence + 1),
                        payloadSha256 = Convert.ToHexString(receipt.PayloadSha256).ToLowerInvariant(),
                        error = "CHHT UTC day is closed"
                    }, statusCode: 410),
                ChhtProtocolError.Authentication => Results.Json(
                    new { error = "Invalid CHHT authentication" }, statusCode: 401),
                _ => Results.BadRequest(new { error = "Invalid CHHT request" })
            };
        }
        catch (FormatException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
    }

    private static async Task<IResult> CompleteAsync(
        HttpRequest request,
        HeatOptions options,
        HeatAccumulatorService? accumulator,
        CancellationToken cancellationToken)
    {
        if (!options.Enabled || accumulator is null)
            return Results.Json(new { error = "Heat ingestion is disabled" }, statusCode: 503);
        if (!string.Equals(request.ContentType?.Split(';', 2)[0].Trim(), ChhtV1Protocol.CompletionMediaType,
                StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new { error = $"Content-Type must be {ChhtV1Protocol.CompletionMediaType}" });
        try
        {
            var crawler = RequiredHeader(request, "X-CHHT-Crawler");
            var day = RequiredHeader(request, "X-CHHT-Day");
            var epoch = UInt64Header(request, "X-CHHT-Epoch");
            var startSequence = UInt64Header(request, "X-CHHT-Start-Sequence");
            var nextSequence = UInt64Header(request, "X-CHHT-Next-Sequence");
            var clean = RequiredHeader(request, "X-CHHT-Clean");
            var signature = RequiredHeader(request, "X-CHHT-Signature");
            if ((await ReadBoundedAsync(request.Body, 1, cancellationToken)).Length != 0)
                return Results.BadRequest(new { error = "CHHT completion body must be empty" });
            var completion = ChhtV1Protocol.ParseAndAuthenticateCompletion(
                crawler, day, epoch, startSequence, nextSequence, clean, signature,
                options.DecodeSecret(crawler), options, DateTime.UtcNow);
            var result = await accumulator.SubmitCompletionAsync(completion, cancellationToken);
            var response = new
            {
                crawler = completion.CrawlerId,
                day = completion.Day,
                epoch = completion.Epoch,
                startSequence = completion.StartSequence,
                nextSequence = completion.NextSequence,
                clean = completion.Clean,
                replay = result.Status == HeatCompletionStatus.Replay,
                code = result.Error == "UTC day is sealing or sealed"
                    ? "day_closed"
                    : result.Status == HeatCompletionStatus.Conflict ? "completion_conflict" : null,
                error = result.Error
            };
            return result.Status switch
            {
                HeatCompletionStatus.Accepted or HeatCompletionStatus.Replay => Results.Ok(response),
                HeatCompletionStatus.Conflict when result.Error == "UTC day is sealing or sealed" =>
                    Results.Json(response, statusCode: 410),
                HeatCompletionStatus.Conflict => Results.Json(response, statusCode: 409),
                HeatCompletionStatus.Backpressure => Results.Json(response, statusCode: 429),
                HeatCompletionStatus.Failed when result.Error == "Heat storage unavailable" =>
                    Results.Json(response, statusCode: 507),
                _ => Results.Json(response, statusCode: 503)
            };
        }
        catch (RequestTooLargeException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
        catch (ChhtProtocolException exception)
        {
            return exception.Error switch
            {
                ChhtProtocolError.Expired when exception.ClosedCompletion is { } receipt => Results.Json(
                    new
                    {
                        code = "day_closed",
                        crawler = receipt.Crawler,
                        day = receipt.Day,
                        epoch = receipt.Epoch,
                        startSequence = receipt.StartSequence,
                        nextSequence = receipt.NextSequence,
                        clean = receipt.Clean,
                        error = "CHHT UTC day is closed"
                    }, statusCode: 410),
                ChhtProtocolError.Authentication => Results.Json(
                    new { error = "Invalid CHHT authentication" }, statusCode: 401),
                _ => Results.BadRequest(new { error = "Invalid CHHT completion" })
            };
        }
        catch (FormatException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
    }

    private static string RequiredHeader(HttpRequest request, string name)
    {
        var values = request.Headers[name];
        if (values.Count != 1 || string.IsNullOrWhiteSpace(values[0]))
            throw new FormatException($"Exactly one {name} header is required");
        return values[0]!;
    }

    private static ulong UInt64Header(HttpRequest request, string name)
    {
        var value = RequiredHeader(request, name);
        if (!ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            throw new FormatException($"{name} must be an unsigned decimal integer");
        return parsed;
    }

    private static async Task<byte[]> ReadBoundedAsync(Stream stream, int limit, CancellationToken ct)
    {
        using var output = new MemoryStream();
        var buffer = new byte[64 * 1024];
        while (true)
        {
            var read = await stream.ReadAsync(buffer, ct);
            if (read == 0) return output.ToArray();
            if (output.Length + read > limit) throw new RequestTooLargeException("CHHT request is too large");
            await output.WriteAsync(buffer.AsMemory(0, read), ct);
        }
    }

    private sealed class RequestTooLargeException(string message) : Exception(message);
}
