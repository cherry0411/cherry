using Cherry.Api.Endpoints;
using Cherry.Application.Services;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Dedup;
using Cherry.Infrastructure.Repositories;
using Cherry.Infrastructure.Heat;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);

var heatOptions = (builder.Configuration.GetSection("Heat").Get<HeatOptions>() ?? new HeatOptions())
    .Normalize(builder.Environment.ContentRootPath);
if (heatOptions.Enabled)
{
    if (heatOptions.ParsedCoverageStartDay is null)
        throw new InvalidOperationException("Heat:CoverageStartDay must be yyyy-MM-dd when heat is enabled");
    if (heatOptions.ExpectedCrawlerIds.Length == 0)
        throw new InvalidOperationException("Heat:ExpectedCrawlerIds must list every required crawler when heat is enabled");
    heatOptions.ValidateExpectedCrawlerSecrets();
    heatOptions.ValidateDailyActorSecretIsolation();
    if (!string.Equals(heatOptions.IndexUid, "torrents", StringComparison.Ordinal))
        throw new InvalidOperationException("Heat:IndexUid must be 'torrents' for the current thin search index");
}
builder.Services.AddSingleton(heatOptions);
builder.Services.AddSingleton<HeatRuntimeMetrics>();
builder.Services.AddSingleton<HeatProjectionStatusCache>();
builder.Services.AddSingleton<HeatRollingStore>();
builder.Services.AddSingleton<Cherry.Infrastructure.Search.SearchRecoveryCoordinator>();
builder.Services.AddSingleton<HeatAccumulatorService>();
if (heatOptions.Enabled)
{
    builder.Services.AddHostedService(sp => sp.GetRequiredService<HeatAccumulatorService>());
    builder.Services.AddScoped<HeatDaySealer>();
    builder.Services.AddHostedService<HeatLifecycleWorker>();
}

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
        var meiliApiKey = builder.Configuration["MeiliSearch:ApiKey"]
                          ?? builder.Configuration["MEILI_MASTER_KEY"]
                          ?? builder.Configuration["MEILI_API_KEY"];
        if (!string.IsNullOrWhiteSpace(meiliApiKey))
        {
            c.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", meiliApiKey);
        }
    });
    builder.Services.AddSingleton(new Cherry.Infrastructure.Search.SearchOutboxOptions
    {
        BatchSize = Math.Clamp(
            builder.Configuration.GetValue<int?>("MeiliSearch:OutboxBatchSize") ?? 500,
            1,
            5_000),
        LeaseDuration = TimeSpan.FromSeconds(
            Math.Clamp(
                builder.Configuration.GetValue<double?>("MeiliSearch:OutboxLeaseSeconds") ?? 300,
                1,
                3_600)),
        PollInterval = TimeSpan.FromMilliseconds(
            Math.Clamp(
                builder.Configuration.GetValue<double?>("MeiliSearch:TaskPollMilliseconds") ?? 250,
                10,
                5_000)),
        TaskTimeout = TimeSpan.FromSeconds(
            Math.Clamp(
                builder.Configuration.GetValue<double?>("MeiliSearch:TaskTimeoutSeconds") ?? 120,
                1,
                1_800)),
        IdleDelay = TimeSpan.FromMilliseconds(
            Math.Clamp(
                builder.Configuration.GetValue<double?>("MeiliSearch:OutboxIdleMilliseconds") ?? 2_000,
                10,
                60_000))
    });
    builder.Services.AddHostedService<Cherry.Infrastructure.Search.SearchOutboxWorker>();
    builder.Services.AddScoped<Cherry.Infrastructure.Search.SearchRecoveryService>();
    if (heatOptions.Enabled)
    {
        builder.Services.AddHostedService<HeatProjectionWorker>();
        builder.Services.AddHostedService<HeatRollingProjectionWorker>();
    }
}
builder.Services.AddSingleton<Cherry.Infrastructure.Search.SearchOutboxMetrics>();
builder.Services.AddScoped<Cherry.Infrastructure.Search.SearchOutboxStore>();

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
const string searchOutboxStatsPath = "/api/v1/search/outbox/stats";
const string searchOutboxRebuildPath = "/api/v1/search/outbox/rebuild";
const string searchRecoveryPath = "/api/v1/search/outbox/recover-empty-index";
var protectedCrawlerPaths = new[]
{
    "/api/v1/torrents/batch",
    "/api/v1/torrents/peers",
    "/api/v1/torrents/reject"
};

app.Use(async (context, next) =>
{
    var normalizedPath = context.Request.Path.Value?.TrimEnd('/');
    if (string.Equals(normalizedPath, durableCrawlerPath, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(normalizedPath, searchOutboxStatsPath, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(normalizedPath, searchOutboxRebuildPath, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(normalizedPath, searchRecoveryPath, StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsJsonAsync(new
            {
                error = "This durable operation is disabled until ApiKey is configured"
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

// Init Meilisearch index settings. If a lost Meili volume was just recreated,
// PostgreSQL is authoritative and startup conservatively schedules a full
// metadata + heat replay. Any recovery failure aborts startup loudly; serving
// a silent, permanently empty search index is not an acceptable degraded mode.
if (!string.IsNullOrWhiteSpace(meiliUrl))
{
    await using var meiliScope = app.Services.CreateAsyncScope();
    var meiliInit = meiliScope.ServiceProvider.GetRequiredService<Cherry.Infrastructure.Search.MeiliSearchClient>();
    await meiliInit.EnsureIndexAsync(CancellationToken.None);
    var recovery = meiliScope.ServiceProvider.GetRequiredService<Cherry.Infrastructure.Search.SearchRecoveryService>();
    var recovered = await recovery.RecoverIfProvablyEmptyAsync(CancellationToken.None);
    if (recovered is not null)
    {
        app.Logger.LogWarning(
            "Detected an empty Meilisearch index with a non-empty PostgreSQL catalog; queued {MetadataRows} metadata rows, daily heat rebuild={HeatRebuild}, rolling heat rebuild={RollingHeatRebuild}",
            recovered.MetadataRowsEnqueued,
            recovered.HeatRebuildRequested,
            recovered.RollingHeatRebuildRequested);
    }
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
SearchOutboxEndpoints.Map(app);
HeatEndpoints.Map(app);

// Health check
app.MapGet("/health", async (IProcessedHashFilter processedHashFilter, HeatOptions heat,
    HeatRuntimeMetrics heatMetrics, HeatRollingStore rolling, CancellationToken cancellationToken) => Results.Ok(new
    {
        status = "healthy",
        processed_hash_fast_path_ready = processedHashFilter.IsReady,
        heat = heatMetrics.Snapshot(heat.Enabled),
        rolling_heat_capacity = heat.Enabled
            ? await rolling.GetCapacityStatusAsync(cancellationToken)
            : null,
        time = DateTime.UtcNow
    }))
    .WithTags("Health");

await app.RunAsync();
