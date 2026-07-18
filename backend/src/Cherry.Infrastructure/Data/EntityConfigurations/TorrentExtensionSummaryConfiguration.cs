using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class TorrentExtensionSummaryConfiguration : IEntityTypeConfiguration<TorrentExtensionSummary>
{
    public void Configure(EntityTypeBuilder<TorrentExtensionSummary> builder)
    {
        builder.ToTable("torrent_extension_summaries");
        builder.HasKey(x => new { x.TorrentId, x.Extension });
        builder.Property(x => x.TorrentId).HasColumnName("torrent_id");
        builder.Ignore(x => x.InfoHash);
        builder.Property(x => x.Extension)
            .HasColumnName("extension")
            .HasMaxLength(32);
        builder.Property(x => x.FileCount).HasColumnName("file_count");
        builder.Property(x => x.TotalLength).HasColumnName("total_length");
        builder.HasOne<Torrent>()
            .WithMany()
            .HasForeignKey(x => x.TorrentId)
            .OnDelete(DeleteBehavior.Cascade);
    }
}
