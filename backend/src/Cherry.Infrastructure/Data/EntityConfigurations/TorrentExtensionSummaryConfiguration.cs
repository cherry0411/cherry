using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class TorrentExtensionSummaryConfiguration : IEntityTypeConfiguration<TorrentExtensionSummary>
{
    public void Configure(EntityTypeBuilder<TorrentExtensionSummary> builder)
    {
        builder.ToTable("torrent_extension_summaries");
        builder.HasKey(x => new { x.InfoHash, x.Extension });
        builder.Property(x => x.InfoHash)
            .HasColumnName("info_hash")
            .HasMaxLength(40);
        builder.Property(x => x.Extension)
            .HasColumnName("extension")
            .HasMaxLength(32);
        builder.Property(x => x.FileCount).HasColumnName("file_count");
        builder.Property(x => x.TotalLength).HasColumnName("total_length");
    }
}
