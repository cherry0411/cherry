using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentConfiguration : IEntityTypeConfiguration<Torrent>
{
    public void Configure(EntityTypeBuilder<Torrent> builder)
    {
        builder.ToTable("torrents", table => table.HasCheckConstraint(
            "CK_torrents_info_hash",
            "octet_length(info_hash) = 20"));

        builder.HasKey(x => x.Id);
        builder.Property(x => x.Id).HasColumnName("id").UseIdentityByDefaultColumn();
        builder.Property(x => x.InfoHash)
            .HasColumnName("info_hash")
            .HasColumnType("bytea")
            .HasConversion(
                value => Convert.FromHexString(value),
                value => Convert.ToHexString(value).ToLowerInvariant())
            .IsRequired();
        builder.Property(x => x.Name).HasColumnName("name").HasColumnType("text").IsRequired();
        builder.Property(x => x.TotalLength).HasColumnName("total_length").IsRequired();
        builder.Property(x => x.FileCount).HasColumnName("file_count").IsRequired();
        builder.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("NOW()");

        builder.HasIndex(x => x.CreatedAt).HasDatabaseName("idx_torrents_created");
        builder.HasIndex(x => x.InfoHash)
            .IsUnique()
            .HasDatabaseName("ux_torrents_info_hash");
        builder.Ignore(x => x.Files);
        builder.Ignore(x => x.ExtensionSummaries);
    }
}
