using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentFileConfiguration : IEntityTypeConfiguration<TorrentFile>
{
    public void Configure(EntityTypeBuilder<TorrentFile> builder)
    {
        builder.ToTable("torrent_files");

        builder.HasKey(x => new { x.Id, x.TorrentId });

        builder.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
        builder.Property(x => x.TorrentId).HasColumnName("torrent_id").IsRequired();
        builder.Property(x => x.PathText).HasColumnName("path_text").HasColumnType("text").IsRequired();
        builder.Property(x => x.Length).HasColumnName("length").IsRequired();

        builder.HasIndex(x => x.PathText)
            .HasDatabaseName("idx_torrent_files_path")
            .HasMethod("GIN")
            .HasOperators("gin_trgm_ops");
    }
}
