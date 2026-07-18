using Cherry.Api.Endpoints;
using Cherry.Application.Services;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Dedup;
using Cherry.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);

// Database
var connStr = builder.Configuration.GetConnectionString("Default")
              ?? throw new InvalidOperationException("Connection string 'Default' is required. Set it in appsettings.json or via ConnectionStrings:Default env variable.");

builder.Services.AddDbContextPool<AppDbContext>(options =>
{
    options.UseNpgsql(connStr, npgsql =>
    {
        npgsql.CommandTimeout(120);
        npgsql.EnableRetryOnFailure(3);
    });
}, poolSize: 64);

// MeiliSearch (optional)
var meiliUrl = builder.Configuration["MeiliSearch:Url"];
if (!string.IsNullOrWhiteSpace(meiliUrl))
{
    builder.Services.AddHttpClient<Cherry.Infrastructure.Search.MeiliSearchClient>(c =>
    {
        c.BaseAddress = new Uri(meiliUrl);
        c.Timeout = TimeSpan.FromSeconds(5);
    });
    builder.Services.AddSingleton<Cherry.Infrastructure.Search.MeiliIndexQueue>();
    builder.Services.AddHostedService(sp =>
        sp.GetRequiredService<Cherry.Infrastructure.Search.MeiliIndexQueue>());
}

// Core services
var dedupPath = Path.Combine(builder.Environment.ContentRootPath, "data", "cuckoo.dat");
var rejectedPath = Path.Combine(builder.Environment.ContentRootPath, "data", "rejected.dat");
var dedupCapacity = builder.Configuration.GetValue<long?>("Ingest:DedupCapacity") ?? 100_000_000;

// Production deliberately does not trust snapshots after a crash. The hosted
// coordinator rebuilds this empty filter from PostgreSQL before enabling it.
var dedup = new CuckooFilter(
    capacity: dedupCapacity,
    persistPath: dedupPath,
    loadPersistedSnapshot: false);
builder.Services.AddSingleton<IDedupFilter>(dedup);
builder.Services.AddSingleton(sp => new ProcessedHashFilter(
    dedup,
    sp.GetRequiredService<IServiceScopeFactory>(),
    sp.GetRequiredService<ILogger<ProcessedHashFilter>>(),
    dedupPath,
    rejectedPath));
builder.Services.AddSingleton<IProcessedHashFilter>(sp =>
    sp.GetRequiredService<ProcessedHashFilter>());
builder.Services.AddHostedService(sp =>
    sp.GetRequiredService<ProcessedHashFilter>());
builder.Services.AddSingleton<Cherry.Application.Services.PendingRequestTracker>();
builder.Services.AddScoped<ITorrentRepository, TorrentRepository>();
builder.Services.AddScoped<DurableIngestService>();
builder.Services.AddScoped<SearchService>();
builder.Services.AddScoped<StatsService>();
builder.Services.AddSingleton<IngestService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<IngestService>());
builder.Services.AddCors();

// Infrastructure
builder.Services.AddOutputCache(options =>
{
    options.DefaultExpirationTimeSpan = TimeSpan.FromSeconds(30);
});

// Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Backend API",
        Version = "v1"
    });
});

var app = builder.Build();

var apiKey = builder.Configuration["ApiKey"];
const string durableCrawlerPath = "/api/v1/torrents/batch/durable";
var protectedCrawlerPaths = new[]
{
    "/api/v1/torrents/batch",
    "/api/v1/torrents/peers",
    "/api/v1/torrents/reject"
};

app.Use(async (context, next) =>
{
    var normalizedPath = context.Request.Path.Value?.TrimEnd('/');
    if (string.Equals(normalizedPath, durableCrawlerPath, StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsJsonAsync(new
            {
                error = "Durable crawler ingestion is disabled until ApiKey is configured"
            });
            return;
        }

        var provided = context.Request.Headers["X-API-Key"].ToString();
        if (!string.Equals(provided, apiKey, StringComparison.Ordinal))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await context.Response.WriteAsJsonAsync(new { error = "Invalid API key" });
            return;
        }
    }
    else if (!string.IsNullOrWhiteSpace(apiKey) &&
        protectedCrawlerPaths.Any(path => context.Request.Path.Equals(path, StringComparison.OrdinalIgnoreCase)))
    {
        var provided = context.Request.Headers["X-API-Key"].ToString();
        if (!string.Equals(provided, apiKey, StringComparison.Ordinal))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await context.Response.WriteAsJsonAsync(new { error = "Invalid API key" });
            return;
        }
    }

    await next();
});

// Auto-apply EF Core migrations and ensure pg_trgm extension
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    // Extension must exist before migration (trigram index depends on it)
    await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    var pending = await db.Database.GetPendingMigrationsAsync();
    if (pending.Any())
        await db.Database.MigrateAsync();
}

// Init Meilisearch index settings
if (!string.IsNullOrWhiteSpace(meiliUrl))
{
    try
    {
        using var meiliScope = app.Services.CreateScope();
        var meiliInit = meiliScope.ServiceProvider.GetService<Cherry.Infrastructure.Search.MeiliSearchClient>();
        if (meiliInit != null) await meiliInit.EnsureIndexAsync(CancellationToken.None);
    }
    catch { }
}

// Seed PendingRequestTracker: load pending hashes from DB, then mark any that
// already have a matching torrent row as done (handles the crash-recovery edge case).
{
    await using var trackerScope = app.Services.CreateAsyncScope();
    var db = trackerScope.ServiceProvider.GetRequiredService<AppDbContext>();
    var tracker = app.Services.GetRequiredService<Cherry.Application.Services.PendingRequestTracker>();

    var pendingHashes = await db.TorrentRequests
        .Where(r => r.Status == "pending")
        .Select(r => r.InfoHash)
        .ToListAsync();

    if (pendingHashes.Count > 0)
    {
        // Find pending hashes that already have a torrent row (crash-recovery)
        var alreadyIngested = await db.Torrents
            .Where(t => pendingHashes.Contains(t.InfoHash))
            .Select(t => t.InfoHash)
            .ToListAsync();

        if (alreadyIngested.Count > 0)
        {
            await db.TorrentRequests
                .Where(r => r.Status == "pending" && alreadyIngested.Contains(r.InfoHash))
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.Status, "done"));
            pendingHashes = pendingHashes.Except(alreadyIngested).ToList();
        }

        if (pendingHashes.Count > 0)
            tracker.TrackMany(pendingHashes);
    }
}

// CORS — allow independent frontend deployment
app.UseCors(policy => policy
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader());

app.UseOutputCache();

// Swagger UI
app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Cherry API v1");
    options.RoutePrefix = "swagger";
});

// Map endpoints
TorrentEndpoints.Map(app);
StatsEndpoints.Map(app);

// Health check
app.MapGet("/health", (IProcessedHashFilter processedHashFilter) => Results.Ok(new
    {
        status = "healthy",
        processed_hash_fast_path_ready = processedHashFilter.IsReady,
        time = DateTime.UtcNow
    }))
    .WithTags("Health");

await app.RunAsync();
