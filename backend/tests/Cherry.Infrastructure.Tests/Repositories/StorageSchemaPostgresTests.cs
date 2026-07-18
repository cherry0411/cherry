using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace Cherry.Infrastructure.Tests.Repositories;

[Collection("Postgres integration")]
public sealed class StorageSchemaPostgresTests
{
    [Fact]
    public async Task CurrentSchema_UsesCompactCatalogAndOmitsWideLegacyIndexes()
    {
        var connectionString = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using (var db = new AppDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pg_trgm");
            await db.Database.MigrateAsync();
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT indexname
              FROM pg_indexes
             WHERE schemaname = 'public'
               AND tablename IN ('torrents', 'torrent_files')
            """,
            connection);
        var indexes = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            indexes.Add(reader.GetString(0));

        Assert.Contains("PK_torrents", indexes);
        Assert.Contains("ux_torrents_info_hash", indexes);
        Assert.Contains("idx_torrent_files_torrent_id", indexes);
        Assert.DoesNotContain("idx_torrent_files_info_hash", indexes);
        Assert.DoesNotContain("idx_torrents_peer_count", indexes);
        Assert.DoesNotContain("idx_torrents_name_trgm", indexes);
        Assert.DoesNotContain("idx_torrent_files_path", indexes);
    }
}
