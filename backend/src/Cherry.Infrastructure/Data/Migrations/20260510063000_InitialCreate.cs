using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "torrent_files",
                columns: table => new
                {
                    info_hash = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                    path_text = table.Column<string>(type: "text", nullable: false),
                    length = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "torrent_requests",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    info_hash = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                    status = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_torrent_requests", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "torrents",
                columns: table => new
                {
                    info_hash = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                    name = table.Column<string>(type: "text", nullable: false),
                    piece_length = table.Column<int>(type: "integer", nullable: false),
                    total_length = table.Column<long>(type: "bigint", nullable: false),
                    file_count = table.Column<int>(type: "integer", nullable: false),
                    is_private = table.Column<bool>(type: "boolean", nullable: false),
                    source = table.Column<string>(type: "character varying(32)", maxLength: 32, nullable: true),
                    peer_count = table.Column<int>(type: "integer", nullable: false, defaultValue: 0),
                    peer_updated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()"),
                    updated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_torrents", x => x.info_hash);
                });

            migrationBuilder.CreateIndex(
                name: "idx_torrent_files_info_hash",
                table: "torrent_files",
                column: "info_hash");

            migrationBuilder.CreateIndex(
                name: "idx_torrent_files_path",
                table: "torrent_files",
                column: "path_text")
                .Annotation("Npgsql:IndexMethod", "GIN")
                .Annotation("Npgsql:IndexOperators", new[] { "gin_trgm_ops" });

            migrationBuilder.CreateIndex(
                name: "idx_torrent_requests_hash",
                table: "torrent_requests",
                column: "info_hash");

            migrationBuilder.CreateIndex(
                name: "idx_torrent_requests_status",
                table: "torrent_requests",
                column: "status");

            migrationBuilder.CreateIndex(
                name: "idx_torrents_created",
                table: "torrents",
                column: "created_at");

            migrationBuilder.CreateIndex(
                name: "idx_torrents_name_trgm",
                table: "torrents",
                column: "name")
                .Annotation("Npgsql:IndexMethod", "GIN")
                .Annotation("Npgsql:IndexOperators", new[] { "gin_trgm_ops" });

            migrationBuilder.CreateIndex(
                name: "idx_torrents_peer_count",
                table: "torrents",
                column: "peer_count");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "torrent_files");

            migrationBuilder.DropTable(
                name: "torrent_requests");

            migrationBuilder.DropTable(
                name: "torrents");
        }
    }
}
