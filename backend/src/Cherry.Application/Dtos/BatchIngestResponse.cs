namespace Cherry.Application.Dtos;

public record BatchIngestResponse(int Accepted, int Duplicates, int Errors, bool Backpressure = false);
