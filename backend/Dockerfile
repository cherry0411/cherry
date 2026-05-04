# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Cherry.Domain/Cherry.Domain.csproj src/Cherry.Domain/
COPY src/Cherry.Application/Cherry.Application.csproj src/Cherry.Application/
COPY src/Cherry.Infrastructure/Cherry.Infrastructure.csproj src/Cherry.Infrastructure/
COPY src/Cherry.Api/Cherry.Api.csproj src/Cherry.Api/
RUN dotnet restore src/Cherry.Api/Cherry.Api.csproj
COPY src/ .
RUN dotnet publish src/Cherry.Api/Cherry.Api.csproj -c Release -o /app

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:5070
EXPOSE 5070
ENTRYPOINT ["dotnet", "Cherry.Api.dll"]
