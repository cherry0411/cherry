using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260718150000_AddHeatCoverageMask")]
public sealed class AddHeatCoverageMask : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            ALTER TABLE heat_projection_watermarks
                ADD COLUMN coverage_mask integer NOT NULL DEFAULT 0,
                ADD CONSTRAINT ck_heat_projection_mask
                    CHECK (coverage_mask BETWEEN 0 AND 1073741823);
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            ALTER TABLE heat_projection_watermarks
                DROP CONSTRAINT IF EXISTS ck_heat_projection_mask,
                DROP COLUMN IF EXISTS coverage_mask;
            """);
    }
}
