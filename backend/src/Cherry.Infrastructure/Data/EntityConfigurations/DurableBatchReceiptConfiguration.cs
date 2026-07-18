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
        builder.Property(x => x.UpdatedAt)
            .HasColumnName("updated_at")
            .HasDefaultValueSql("NOW()");
    }
}
