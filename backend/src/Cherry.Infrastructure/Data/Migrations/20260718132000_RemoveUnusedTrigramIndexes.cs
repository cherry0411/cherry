using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

/// <summary>
/// Full-text search is served exclusively by Meilisearch. PostgreSQL only
/// hydrates Meili-ranked hashes and loads detail rows by info_hash, so these
/// write-heavy trigram indexes were not read by any application query.
/// </summary>
[DbContext(typeof(AppDbContext))]
[Migration("20260718132000_RemoveUnusedTrigramIndexes")]
public sealed class RemoveUnusedTrigramIndexes : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "idx_torrent_files_path",
            table: "torrent_files");

        migrationBuilder.DropIndex(
            name: "idx_torrents_name_trgm",
            table: "torrents");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateIndex(
                name: "idx_torrent_files_path",
                table: "torrent_files",
                column: "path_text")
            .Annotation("Npgsql:IndexMethod", "GIN")
            .Annotation("Npgsql:IndexOperators", new[] { "gin_trgm_ops" });

        migrationBuilder.CreateIndex(
                name: "idx_torrents_name_trgm",
                table: "torrents",
                column: "name")
            .Annotation("Npgsql:IndexMethod", "GIN")
            .Annotation("Npgsql:IndexOperators", new[] { "gin_trgm_ops" });
    }
}
