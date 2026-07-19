using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260719040000_AddDurableIngestCounters")]
public sealed class AddDurableIngestCounters : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // Existing receipts cannot reconstruct historic accepted/duplicate
        // classifications. Start an explicit zero baseline instead of
        // publishing partial history as a lifetime total. IF NOT EXISTS keeps
        // this compatible with a pre-provisioned schema during rolling deploys.
        migrationBuilder.Sql(
            """
            ALTER TABLE durable_batch_receipts
                ADD COLUMN IF NOT EXISTS total_delivered bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_accepted bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_duplicates bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_metadata_committed bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_policy_committed bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_committed_batches bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS counters_started_at timestamp with time zone NOT NULL DEFAULT NOW();

            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                      FROM pg_constraint
                     WHERE conname = 'CK_durable_batch_receipts_counters'
                       AND conrelid = 'durable_batch_receipts'::regclass)
                THEN
                    ALTER TABLE durable_batch_receipts
                        ADD CONSTRAINT "CK_durable_batch_receipts_counters" CHECK (
                            total_delivered >= 0
                            AND total_accepted >= 0
                            AND total_duplicates >= 0
                            AND total_metadata_committed >= 0
                            AND total_policy_committed >= 0
                            AND total_committed_batches >= 0
                            AND total_delivered = total_accepted + total_duplicates
                            AND total_accepted = total_metadata_committed + total_policy_committed);
                END IF;
            END $$;
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            ALTER TABLE durable_batch_receipts
                DROP CONSTRAINT IF EXISTS "CK_durable_batch_receipts_counters",
                DROP COLUMN IF EXISTS counters_started_at,
                DROP COLUMN IF EXISTS total_committed_batches,
                DROP COLUMN IF EXISTS total_policy_committed,
                DROP COLUMN IF EXISTS total_metadata_committed,
                DROP COLUMN IF EXISTS total_duplicates,
                DROP COLUMN IF EXISTS total_accepted,
                DROP COLUMN IF EXISTS total_delivered;
            """);
    }
}
