FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

# Layer 1: copy project files and restore (cached unless csproj changes)
COPY Cherry.slnx ./
COPY src/Cherry.Domain/Cherry.Domain.csproj src/Cherry.Domain/
COPY src/Cherry.Application/Cherry.Application.csproj src/Cherry.Application/
COPY src/Cherry.Infrastructure/Cherry.Infrastructure.csproj src/Cherry.Infrastructure/
COPY src/Cherry.Api/Cherry.Api.csproj src/Cherry.Api/
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet restore src/Cherry.Api/Cherry.Api.csproj

# Layer 2: copy source and publish (only runs when source changes)
COPY src/ src/
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet publish src/Cherry.Api/Cherry.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:5070
EXPOSE 5070
ENTRYPOINT ["dotnet", "Cherry.Api.dll"]
