namespace Cherry.Application.Dtos;

/// <summary>
/// <paramref name="Accepted"/> counts rows committed to the exact store before
/// this response was produced. A failed commit is surfaced as an HTTP failure,
/// never as an accepted batch.
/// </summary>
public record BatchIngestResponse(int Accepted, int Duplicates, int Errors, bool Backpressure = false);
