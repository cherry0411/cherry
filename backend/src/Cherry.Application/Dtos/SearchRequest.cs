namespace Cherry.Application.Dtos;

public record SearchRequest(string Query, int Page = 1, int PageSize = 20, string? FileType = null);
