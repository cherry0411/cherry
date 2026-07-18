using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260718130000_AddDurableBatchReceipts")]
public sealed class AddDurableBatchReceipts : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "durable_batch_receipts",
            columns: table => new
            {
                crawler_id = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                epoch = table.Column<long>(type: "bigint", nullable: false),
                last_start_sequence = table.Column<long>(type: "bigint", nullable: false),
                last_end_sequence = table.Column<long>(type: "bigint", nullable: false),
                last_payload_sha256 = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                last_accepted = table.Column<int>(type: "integer", nullable: false),
                last_duplicates = table.Column<int>(type: "integer", nullable: false),
                updated_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "NOW()")
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "PK_durable_batch_receipts",
                    row => new { row.crawler_id, row.epoch });
                table.CheckConstraint("CK_durable_batch_receipts_epoch", "epoch > 0");
                table.CheckConstraint(
                    "CK_durable_batch_receipts_state",
                    "(last_start_sequence = 0 AND last_end_sequence = 0 AND last_payload_sha256 = '' " +
                    "AND last_accepted = 0 AND last_duplicates = 0) OR " +
                    "(last_start_sequence > 0 AND last_end_sequence >= last_start_sequence " +
                    "AND char_length(last_payload_sha256) = 64 AND last_accepted >= 0 AND last_duplicates >= 0)");
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "durable_batch_receipts");
    }
}
