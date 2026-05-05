using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentRequestConfiguration : IEntityTypeConfiguration<TorrentRequest>
{
    public void Configure(EntityTypeBuilder<TorrentRequest> builder)
    {
        builder.ToTable("torrent_requests");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasMaxLength(40).IsRequired();
        builder.Property(x => x.Status).HasColumnName("status").HasMaxLength(16).IsRequired();
        builder.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("NOW()");
        builder.HasIndex(x => x.InfoHash).HasDatabaseName("idx_torrent_requests_hash");
        builder.HasIndex(x => x.Status).HasDatabaseName("idx_torrent_requests_status");
    }
}
