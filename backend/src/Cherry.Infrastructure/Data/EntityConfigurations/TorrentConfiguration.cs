using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentConfiguration : IEntityTypeConfiguration<Torrent>
{
    public void Configure(EntityTypeBuilder<Torrent> builder)
    {
        builder.ToTable("torrents", table =>
        {
            table.HasCheckConstraint(
                "CK_torrents_retained_level",
                "retained_level IN (2, 3)");
            table.HasCheckConstraint(
                "CK_torrents_refetch",
                "retained_level = 2 OR NOT needs_refetch");
        });

        builder.HasKey(x => x.InfoHash);

        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasMaxLength(40).IsRequired();
        builder.Property(x => x.Name).HasColumnName("name").HasColumnType("text").IsRequired();
        builder.Property(x => x.PieceLength).HasColumnName("piece_length").IsRequired();
        builder.Property(x => x.TotalLength).HasColumnName("total_length").IsRequired();
        builder.Property(x => x.FileCount).HasColumnName("file_count").IsRequired();
        builder.Property(x => x.IsPrivate).HasColumnName("is_private").IsRequired();
        builder.Property(x => x.Source).HasColumnName("source").HasMaxLength(32);
        builder.Property(x => x.PolicyId).HasColumnName("policy_id").HasMaxLength(256);
        builder.Property(x => x.Region).HasColumnName("region").HasMaxLength(64);
        builder.Property(x => x.RetainedLevel)
            .HasColumnName("retained_level")
            .HasConversion<short>()
            .HasDefaultValue(MetadataRetentionLevel.Normalized)
            // Normalized is both the CLR initializer and the database default.
            // An explicit sentinel tells EF that omitting this value on an
            // INSERT is intentional instead of treating enum zero as special.
            .HasSentinel(MetadataRetentionLevel.Normalized);
        builder.Property(x => x.NeedsRefetch)
            .HasColumnName("needs_refetch")
            .HasDefaultValue(false);
        builder.Property(x => x.PeerCount).HasColumnName("peer_count").HasDefaultValue(0);
        builder.Property(x => x.PeerUpdatedAt).HasColumnName("peer_updated_at");
        builder.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("NOW()");
        // Keep the runtime model aligned with the initial migration/model
        // snapshot so automatic migrations do not fail pending-model validation.
        builder.Property(x => x.UpdatedAt)
            .HasColumnName("updated_at")
            .HasDefaultValueSql("NOW()");

        builder.HasIndex(x => x.CreatedAt).HasDatabaseName("idx_torrents_created");
        builder.HasIndex(x => x.PeerCount).HasDatabaseName("idx_torrents_peer_count");
        builder.Ignore(x => x.Files);
        builder.Ignore(x => x.ExtensionSummaries);
    }
}
