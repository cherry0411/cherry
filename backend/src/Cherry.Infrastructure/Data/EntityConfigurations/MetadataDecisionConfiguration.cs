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
                "CK_metadata_decisions_code",
                "decision_code BETWEEN 1 AND 5");
        });

        builder.HasKey(x => x.InfoHash);
        builder.Property(x => x.InfoHash).HasColumnName("info_hash").HasColumnType("bytea");
        builder.Property(x => x.DecisionCode)
            .HasColumnName("decision_code")
            .HasConversion<short>();
    }
}
