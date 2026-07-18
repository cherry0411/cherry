using System;
using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260718133000_AddSearchOutbox")]
public sealed class AddSearchOutbox : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "search_outbox",
            columns: table => new
            {
                info_hash = table.Column<string>(type: "character varying(40)", maxLength: 40, nullable: false),
                generation = table.Column<long>(type: "bigint", nullable: false),
                enqueued_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()"),
                available_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()"),
                lease_owner = table.Column<Guid>(type: "uuid", nullable: true),
                lease_until = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                attempt_count = table.Column<int>(type: "integer", nullable: false, defaultValue: 0),
                last_error = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: true),
                updated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()")
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_search_outbox", x => x.info_hash);
                table.CheckConstraint("CK_search_outbox_attempts", "attempt_count >= 0");
                table.CheckConstraint("CK_search_outbox_generation", "generation > 0");
                table.CheckConstraint(
                    "CK_search_outbox_lease",
                    "(lease_owner IS NULL AND lease_until IS NULL) OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL)");
                table.ForeignKey(
                    name: "FK_search_outbox_torrents_info_hash",
                    column: x => x.info_hash,
                    principalTable: "torrents",
                    principalColumn: "info_hash",
                    onDelete: ReferentialAction.Cascade);
            });

        migrationBuilder.CreateIndex(
            name: "idx_search_outbox_due",
            table: "search_outbox",
            columns: new[] { "available_at", "lease_until" });

        // Deployment must not depend on an operator remembering to rebuild:
        // coalesce every pre-existing authoritative row into one projection
        // marker. New writes use the same table transactionally.
        migrationBuilder.Sql(
            """
            INSERT INTO search_outbox (info_hash, generation)
            SELECT info_hash, 1 FROM torrents
            ON CONFLICT (info_hash) DO NOTHING
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "search_outbox");
    }
}
