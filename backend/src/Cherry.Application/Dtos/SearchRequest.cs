namespace Cherry.Application.Dtos;

public record SearchRequest(string Query, string HeatWindow = "7d", int Page = 1, int PageSize = 20);
