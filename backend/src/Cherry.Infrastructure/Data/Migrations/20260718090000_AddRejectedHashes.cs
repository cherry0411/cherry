using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

/// <summary>
/// Adds the compact exact authority for hashes explicitly rejected by crawler
/// rules. The previous 16-bit probabilistic snapshot cannot be migrated because
/// it does not retain the original hashes.
/// </summary>
[DbContext(typeof(AppDbContext))]
[Migration("20260718090000_AddRejectedHashes")]
public sealed class AddRejectedHashes : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "rejected_hashes",
            columns: table => new
            {
                info_hash = table.Column<byte[]>(type: "bytea", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_rejected_hashes", row => row.info_hash);
                table.CheckConstraint(
                    "CK_rejected_hashes_sha1_length",
                    "octet_length(info_hash) = 20");
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "rejected_hashes");
    }
}
