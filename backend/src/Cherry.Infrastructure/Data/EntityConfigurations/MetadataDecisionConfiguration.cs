using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cherry.Infrastructure.Data.EntityConfigurations;

public sealed class MetadataDecisionConfiguration : IEntityTypeConfiguration<MetadataDecision>
{
    public void Configure(EntityTypeBuilder<MetadataDecision> builder)
    {
        builder.ToTable("metadata_decisions", table =>
        {
            table.HasCheckConstraint(
                "CK_metadata_decisions_info_hash",
                "octet_length(info_hash) = 20");
            table.HasCheckConstraint(
                "CK_metadata_decisions_action",
                "action IN (1, 2)");
            table.HasCheckConstraint(
                "CK_metadata_decisions_retained_level",
                "retained_level = 1");
            table.HasCheckConstraint(
                "CK_metadata_decisions_refetch",
                "action = 1 OR NOT needs_refetch");
        });

        builder.HasKey(x => x.InfoHash);
        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasColumnType("bytea");
        builder.Property(x => x.Action)
            .HasColumnName("action")
            .HasConversion<short>();
        builder.Property(x => x.RetainedLevel)
            .HasColumnName("retained_level")
            .HasConversion<short>();
        builder.Property(x => x.NeedsRefetch).HasColumnName("needs_refetch");
        builder.Property(x => x.PolicyId)
            .HasColumnName("policy_id")
            .HasMaxLength(256);
        builder.Property(x => x.Reason)
            .HasColumnName("reason")
            .HasMaxLength(1024)
            .IsRequired();
        builder.Property(x => x.FirstSeen).HasColumnName("first_seen");
        builder.Property(x => x.Region)
            .HasColumnName("region")
            .HasMaxLength(64);
        builder.Property(x => x.UpdatedAt)
            .HasColumnName("updated_at")
            .HasDefaultValueSql("NOW()");
    }
}
