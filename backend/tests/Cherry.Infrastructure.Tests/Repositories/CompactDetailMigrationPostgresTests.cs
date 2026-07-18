using Cherry.Infrastructure.Data;
using Cherry.Infrastructure.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using Xunit;

namespace Cherry.Infrastructure.Tests.Repositories;

[Collection("Postgres integration")]
public sealed class CompactDetailMigrationPostgresTests
{
    [Fact]
    public async Task LegacyRows_UpDownUp_AreLosslessAndInvalidRowsFailClosed()
    {
        var configured = Environment.GetEnvironmentVariable("CHERRY_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(configured))
            return;

        var databaseName = $"cherry_detail_{Guid.NewGuid():N}";
        var source = new NpgsqlConnectionStringBuilder(configured) { Pooling = false };
        var admin = new NpgsqlConnectionStringBuilder(source.ConnectionString)
        {
            Database = "postgres",
            Pooling = false
        };
        var database = new NpgsqlConnectionStringBuilder(source.ConnectionString)
        {
            Database = databaseName,
            Pooling = false
        };
        await CreateDatabaseAsync(admin.ConnectionString, databaseName);
        try
        {
            await using (var connection = new NpgsqlConnection(database.ConnectionString))
            {
                await connection.OpenAsync();
                await using var extension = new NpgsqlCommand(
                    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
                    connection);
                await extension.ExecuteNonQueryAsync();
            }

            var options = new DbContextOptionsBuilder<AppDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options;
            await using var db = new AppDbContext(options);
            var migrator = db.GetService<IMigrator>();
            await migrator.MigrateAsync("20260718140000_CompactCatalog");
            await db.Database.ExecuteSqlRawAsync(SeedSql);

            await Assert.ThrowsAsync<PostgresException>(() => migrator.MigrateAsync());
            Assert.False(await TableExistsAsync(database.ConnectionString, "torrent_details"));
            await db.Database.ExecuteSqlRawAsync(
                "DELETE FROM torrent_files WHERE path_text = ''");

            var filesBefore = await ReadRowsAsync(
                database.ConnectionString,
                "SELECT torrent_id || '|' || path_text || '|' || length FROM torrent_files ORDER BY torrent_id,path_text,length");
            var extensionsBefore = await ReadRowsAsync(
                database.ConnectionString,
                "SELECT torrent_id || '|' || extension || '|' || file_count || '|' || total_length FROM torrent_extension_summaries ORDER BY torrent_id,extension");

            await migrator.MigrateAsync();
            Assert.False(await TableExistsAsync(database.ConnectionString, "torrent_files"));
            Assert.False(await TableExistsAsync(database.ConnectionString, "torrent_extension_summaries"));
            Assert.Equal("l", await ScalarAsync(
                database.ConnectionString,
                "SELECT attcompression::text FROM pg_attribute WHERE attrelid='torrent_details'::regclass AND attname='payload'"));
            var payloads = await ReadPayloadsAsync(database.ConnectionString);
            Assert.Equal(3, payloads.Count);
            Assert.Equal(4, TorrentDetailCodec.Decode(payloads[0]).Files.Count);
            Assert.Empty(TorrentDetailCodec.Decode(payloads[2]).Files);

            await migrator.MigrateAsync("20260718140000_CompactCatalog");
            Assert.Equal(filesBefore, await ReadRowsAsync(
                database.ConnectionString,
                "SELECT torrent_id || '|' || path_text || '|' || length FROM torrent_files ORDER BY torrent_id,path_text,length"));
            Assert.Equal(extensionsBefore, await ReadRowsAsync(
                database.ConnectionString,
                "SELECT torrent_id || '|' || extension || '|' || file_count || '|' || total_length FROM torrent_extension_summaries ORDER BY torrent_id,extension"));

            await migrator.MigrateAsync();
            var secondPayloads = await ReadPayloadsAsync(database.ConnectionString);
            Assert.Equal(payloads, secondPayloads, ByteArrayComparer.Instance);
        }
        finally
        {
            NpgsqlConnection.ClearAllPools();
            await DropDatabaseAsync(admin.ConnectionString, databaseName);
        }
    }

    private static async Task CreateDatabaseAsync(string connectionString, string databaseName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"CREATE DATABASE {Quote(databaseName)}",
            connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropDatabaseAsync(string connectionString, string databaseName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"DROP DATABASE IF EXISTS {Quote(databaseName)} WITH (FORCE)",
            connection);
        await command.ExecuteNonQueryAsync();
    }

    private static string Quote(string identifier) =>
        '"' + identifier.Replace("\"", "\"\"") + '"';

    private static async Task<bool> TableExistsAsync(string connectionString, string table)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT to_regclass(@table) IS NOT NULL",
            connection);
        command.Parameters.AddWithValue("table", table);
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<string> ScalarAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<List<string>> ReadRowsAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetString(0));
        return rows;
    }

    private static async Task<List<byte[]>> ReadPayloadsAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT payload FROM torrent_details ORDER BY torrent_id",
            connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<byte[]>();
        while (await reader.ReadAsync())
            rows.Add(reader.GetFieldValue<byte[]>(0));
        return rows;
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static ByteArrayComparer Instance { get; } = new();
        public bool Equals(byte[]? left, byte[]? right) =>
            left is not null && right is not null && left.AsSpan().SequenceEqual(right);
        public int GetHashCode(byte[] value) => value.Length;
    }

    private const string SeedSql =
        """
        INSERT INTO torrents (info_hash,name,total_length,file_count,created_at) VALUES
        (decode(repeat('01',20),'hex'),'normalized',300,4,'2026-07-18T00:00:00Z'),
        (decode(repeat('02',20),'hex'),'summary',1000,100,'2026-07-18T00:00:01Z'),
        (decode(repeat('03',20),'hex'),'empty-summary',1,1,'2026-07-18T00:00:02Z');
        INSERT INTO torrent_files(torrent_id,path_text,length)
        SELECT id,'目录/第二集.mkv',200 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'目录/第一集.mkv',97 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'readme.txt',3 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'readme.txt',3 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'sample/file.bin',10 FROM torrents WHERE name='summary'
        UNION ALL SELECT id,'',0 FROM torrents WHERE name='empty-summary';
        INSERT INTO torrent_extension_summaries(torrent_id,extension,file_count,total_length)
        SELECT id,'mkv',2,297 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'txt',1,3 FROM torrents WHERE name='normalized'
        UNION ALL SELECT id,'.bin',10,500 FROM torrents WHERE name='summary';
        """;
}
