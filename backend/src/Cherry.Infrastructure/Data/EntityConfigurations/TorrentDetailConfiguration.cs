using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class TorrentDetailConfiguration : IEntityTypeConfiguration<TorrentDetail>
{
    public void Configure(EntityTypeBuilder<TorrentDetail> builder)
    {
        builder.ToTable("torrent_details", table => table.HasCheckConstraint(
            "CK_torrent_details_payload",
            "octet_length(payload) BETWEEN 3 AND 67108864"));
        builder.HasKey(detail => detail.TorrentId);
        builder.Property(detail => detail.TorrentId).HasColumnName("torrent_id");
        builder.Property(detail => detail.Payload)
            .HasColumnName("payload")
            .HasColumnType("bytea")
            .UseCompressionMethod("lz4")
            .IsRequired();
        builder.HasOne<Torrent>()
            .WithOne()
            .HasForeignKey<TorrentDetail>(detail => detail.TorrentId)
            .OnDelete(DeleteBehavior.Cascade);
    }
}
