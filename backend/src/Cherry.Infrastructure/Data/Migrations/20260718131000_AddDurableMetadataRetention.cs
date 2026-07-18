using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260718131000_AddDurableMetadataRetention")]
public sealed class AddDurableMetadataRetention : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<bool>(
            name: "needs_refetch",
            table: "torrents",
            type: "boolean",
            nullable: false,
            defaultValue: false);

        migrationBuilder.AddColumn<string>(
            name: "policy_id",
            table: "torrents",
            type: "character varying(256)",
            maxLength: 256,
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "region",
            table: "torrents",
            type: "character varying(64)",
            maxLength: 64,
            nullable: true);

        migrationBuilder.AddColumn<short>(
            name: "retained_level",
            table: "torrents",
            type: "smallint",
            nullable: false,
            defaultValue: (short)MetadataRetentionLevel.Normalized);

        migrationBuilder.CreateTable(
            name: "metadata_decisions",
            columns: table => new
            {
                info_hash = table.Column<byte[]>(type: "bytea", nullable: false),
                action = table.Column<short>(type: "smallint", nullable: false),
                retained_level = table.Column<short>(type: "smallint", nullable: false),
                needs_refetch = table.Column<bool>(type: "boolean", nullable: false),
                policy_id = table.Column<string>(
                    type: "character varying(256)",
                    maxLength: 256,
                    nullable: true),
                reason = table.Column<string>(
                    type: "character varying(1024)",
                    maxLength: 1024,
                    nullable: false),
                first_seen = table.Column<DateTime>(
                    type: "timestamp with time zone",
                    nullable: true),
                region = table.Column<string>(
                    type: "character varying(64)",
                    maxLength: 64,
                    nullable: true),
                updated_at = table.Column<DateTime>(
                    type: "timestamp with time zone",
                    nullable: false,
                    defaultValueSql: "NOW()")
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_metadata_decisions", row => row.info_hash);
                table.CheckConstraint("CK_metadata_decisions_action", "action IN (1, 2)");
                table.CheckConstraint(
                    "CK_metadata_decisions_info_hash",
                    "octet_length(info_hash) = 20");
                table.CheckConstraint(
                    "CK_metadata_decisions_refetch",
                    "action = 1 OR NOT needs_refetch");
                table.CheckConstraint(
                    "CK_metadata_decisions_retained_level",
                    "retained_level = 1");
            });

        migrationBuilder.CreateTable(
            name: "torrent_extension_summaries",
            columns: table => new
            {
                info_hash = table.Column<string>(
                    type: "character varying(40)",
                    maxLength: 40,
                    nullable: false),
                extension = table.Column<string>(
                    type: "character varying(32)",
                    maxLength: 32,
                    nullable: false),
                file_count = table.Column<int>(type: "integer", nullable: false),
                total_length = table.Column<long>(type: "bigint", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "PK_torrent_extension_summaries",
                    row => new { row.info_hash, row.extension });
            });

        migrationBuilder.AddCheckConstraint(
            name: "CK_torrents_refetch",
            table: "torrents",
            sql: "retained_level = 2 OR NOT needs_refetch");

        migrationBuilder.AddCheckConstraint(
            name: "CK_torrents_retained_level",
            table: "torrents",
            sql: "retained_level IN (2, 3)");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "metadata_decisions");
        migrationBuilder.DropTable(name: "torrent_extension_summaries");
        migrationBuilder.DropCheckConstraint(name: "CK_torrents_refetch", table: "torrents");
        migrationBuilder.DropCheckConstraint(name: "CK_torrents_retained_level", table: "torrents");
        migrationBuilder.DropColumn(name: "needs_refetch", table: "torrents");
        migrationBuilder.DropColumn(name: "policy_id", table: "torrents");
        migrationBuilder.DropColumn(name: "region", table: "torrents");
        migrationBuilder.DropColumn(name: "retained_level", table: "torrents");
    }
}
