using Cherry.Api.Endpoints;
using Cherry.Application.Services;
using Cherry.Domain.Interfaces;
using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Dedup;
using Cherry.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;
using Npgsql;

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

// MeiliSearch (optional — falls back to PG trigram if not configured)
var meiliUrl = builder.Configuration["MeiliSearch:Url"];
if (!string.IsNullOrWhiteSpace(meiliUrl))
{
    builder.Services.AddHttpClient<Cherry.Infrastructure.Search.MeiliSearchClient>(c =>
    {
        c.BaseAddress = new Uri(meiliUrl);
        c.Timeout = TimeSpan.FromSeconds(5);
    });
}

// Core services
var dedupPath = Path.Combine(builder.Environment.ContentRootPath, "data", "cuckoo.dat");
var dedup = new CuckooFilter(capacity: 100_000_000, persistPath: dedupPath);
builder.Services.AddSingleton<IDedupFilter>(dedup);
builder.Services.AddScoped<ITorrentRepository, TorrentRepository>();
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

// Persist CuckooFilter on graceful shutdown
app.Lifetime.ApplicationStopping.Register(() => dedup.Save());

// Auto-apply EF Core migrations and ensure pg_trgm extension
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    // Extension must exist before migration (trigram index depends on it)
    await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    var pending = await db.Database.GetPendingMigrationsAsync();
    if (pending.Any())
    {
        try { await db.Database.MigrateAsync(); }
        catch (PostgresException ex) when (ex.SqlState == "42P07")
        {
            // Tables already exist from manual creation — record the migration
            await db.Database.ExecuteSqlRawAsync(
                $"INSERT INTO \"__EFMigrationsHistory\" VALUES ('{pending.Last()}', '10.0.0')");
        }
    }
}

// CORS — allow independent frontend deployment
app.UseCors(policy => policy
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader());

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
app.MapGet("/health", () => Results.Ok(new { status = "healthy", time = DateTime.UtcNow }))
    .WithTags("Health");

await app.RunAsync();
