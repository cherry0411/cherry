using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class DurableBatchReceiptConfiguration : IEntityTypeConfiguration<DurableBatchReceipt>
{
    public void Configure(EntityTypeBuilder<DurableBatchReceipt> builder)
    {
        builder.ToTable("durable_batch_receipts", table =>
        {
            table.HasCheckConstraint("CK_durable_batch_receipts_epoch", "epoch > 0");
            table.HasCheckConstraint(
                "CK_durable_batch_receipts_state",
                "(last_start_sequence = 0 AND last_end_sequence = 0 AND last_payload_sha256 = '' " +
                "AND last_accepted = 0 AND last_duplicates = 0) OR " +
                "(last_start_sequence > 0 AND last_end_sequence >= last_start_sequence " +
                "AND char_length(last_payload_sha256) = 64 AND last_accepted >= 0 AND last_duplicates >= 0)");
            table.HasCheckConstraint(
                "CK_durable_batch_receipts_counters",
                "total_delivered >= 0 AND total_accepted >= 0 AND total_duplicates >= 0 " +
                "AND total_metadata_committed >= 0 AND total_policy_committed >= 0 " +
                "AND total_committed_batches >= 0 " +
                "AND total_delivered = total_accepted + total_duplicates " +
                "AND total_accepted = total_metadata_committed + total_policy_committed");
        });

        builder.HasKey(x => new { x.CrawlerId, x.Epoch });
        builder.Property(x => x.CrawlerId)
            .HasColumnName("crawler_id")
            .HasMaxLength(256)
            .IsRequired();
        builder.Property(x => x.Epoch).HasColumnName("epoch");
        builder.Property(x => x.LastStartSequence).HasColumnName("last_start_sequence");
        builder.Property(x => x.LastEndSequence).HasColumnName("last_end_sequence");
        builder.Property(x => x.LastPayloadSha256)
            .HasColumnName("last_payload_sha256")
            .HasMaxLength(64)
            .IsRequired();
        builder.Property(x => x.LastAccepted).HasColumnName("last_accepted");
        builder.Property(x => x.LastDuplicates).HasColumnName("last_duplicates");
        builder.Property(x => x.TotalDelivered).HasColumnName("total_delivered");
        builder.Property(x => x.TotalAccepted).HasColumnName("total_accepted");
        builder.Property(x => x.TotalDuplicates).HasColumnName("total_duplicates");
        builder.Property(x => x.TotalMetadataCommitted).HasColumnName("total_metadata_committed");
        builder.Property(x => x.TotalPolicyCommitted).HasColumnName("total_policy_committed");
        builder.Property(x => x.TotalCommittedBatches).HasColumnName("total_committed_batches");
        builder.Property(x => x.CountersStartedAt)
            .HasColumnName("counters_started_at")
            .HasDefaultValueSql("NOW()");
        builder.Property(x => x.UpdatedAt)
            .HasColumnName("updated_at")
            .HasDefaultValueSql("NOW()");
    }
}
