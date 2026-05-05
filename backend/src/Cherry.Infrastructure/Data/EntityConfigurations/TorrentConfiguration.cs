using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentConfiguration : IEntityTypeConfiguration<Torrent>
{
    public void Configure(EntityTypeBuilder<Torrent> builder)
    {
        builder.ToTable("torrents");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasMaxLength(40).IsRequired();
        builder.Property(x => x.Name).HasColumnName("name").HasColumnType("text").IsRequired();
        builder.Property(x => x.PieceLength).HasColumnName("piece_length").IsRequired();
        builder.Property(x => x.TotalLength).HasColumnName("total_length").IsRequired();
        builder.Property(x => x.FileCount).HasColumnName("file_count").IsRequired();
        builder.Property(x => x.IsPrivate).HasColumnName("is_private").IsRequired();
        builder.Property(x => x.Source).HasColumnName("source").HasMaxLength(32);
        builder.Property(x => x.PeerCount).HasColumnName("peer_count").HasDefaultValue(0);
        builder.Property(x => x.PeerUpdatedAt).HasColumnName("peer_updated_at");
        builder.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("NOW()");
        builder.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasDefaultValueSql("NOW()");

        builder.HasIndex(x => x.InfoHash).IsUnique().HasDatabaseName("uq_torrents_info_hash");
        builder.HasIndex(x => x.CreatedAt).HasDatabaseName("idx_torrents_created");
        builder.HasIndex(x => x.PeerCount).HasDatabaseName("idx_torrents_peer_count");
        builder.HasIndex(x => x.Name)
            .HasDatabaseName("idx_torrents_name_trgm")
            .HasMethod("GIN")
            .HasOperators("gin_trgm_ops");

        builder.HasMany(x => x.Files)
            .WithOne(f => f.Torrent)
            .HasForeignKey(f => f.TorrentId)
            .OnDelete(DeleteBehavior.Cascade);
    }
}
