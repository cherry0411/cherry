using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class SearchOutboxItemConfiguration : IEntityTypeConfiguration<SearchOutboxItem>
{
    public void Configure(EntityTypeBuilder<SearchOutboxItem> builder)
    {
        builder.ToTable("search_outbox", table =>
        {
            table.HasCheckConstraint("CK_search_outbox_generation", "generation > 0");
            table.HasCheckConstraint("CK_search_outbox_attempts", "attempt_count >= 0");
            table.HasCheckConstraint(
                "CK_search_outbox_lease",
                "(lease_owner IS NULL AND lease_until IS NULL) OR " +
                "(lease_owner IS NOT NULL AND lease_until IS NOT NULL)");
        });

        builder.HasKey(item => item.TorrentId);
        builder.Property(item => item.TorrentId).HasColumnName("torrent_id");
        builder.Property(item => item.Generation).HasColumnName("generation");
        builder.Property(item => item.EnqueuedAt)
            .HasColumnName("enqueued_at")
            .HasDefaultValueSql("NOW()");
        builder.Property(item => item.AvailableAt)
            .HasColumnName("available_at")
            .HasDefaultValueSql("NOW()");
        builder.Property(item => item.LeaseOwner).HasColumnName("lease_owner");
        builder.Property(item => item.LeaseUntil).HasColumnName("lease_until");
        builder.Property(item => item.AttemptCount)
            .HasColumnName("attempt_count")
            .HasDefaultValue(0);
        builder.Property(item => item.LastError)
            .HasColumnName("last_error")
            .HasMaxLength(1024);
        builder.Property(item => item.UpdatedAt)
            .HasColumnName("updated_at")
            .HasDefaultValueSql("NOW()");

        builder.HasIndex(item => new { item.AvailableAt, item.LeaseUntil })
            .HasDatabaseName("idx_search_outbox_due");
        builder.HasOne<Torrent>()
            .WithMany()
            .HasForeignKey(item => item.TorrentId)
            .OnDelete(DeleteBehavior.Cascade);
    }
}
