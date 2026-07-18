using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public class TorrentFileConfiguration : IEntityTypeConfiguration<TorrentFile>
{
    public void Configure(EntityTypeBuilder<TorrentFile> builder)
    {
        builder.ToTable("torrent_files");

        builder.HasNoKey();

        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasMaxLength(40).IsRequired();
        builder.Property(x => x.PathText).HasColumnName("path_text").HasColumnType("text").IsRequired();
        builder.Property(x => x.Length).HasColumnName("length").IsRequired();

        builder.HasIndex(x => x.InfoHash).HasDatabaseName("idx_torrent_files_info_hash");

    }
}
