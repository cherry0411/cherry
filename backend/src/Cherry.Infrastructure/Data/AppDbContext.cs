using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cherry.Infrastructure.Data;

public class AppDbContext : DbContext
{
    public DbSet<DurableBatchReceipt> DurableBatchReceipts => Set<DurableBatchReceipt>();
    public DbSet<MetadataDecision> MetadataDecisions => Set<MetadataDecision>();
    public DbSet<SearchOutboxItem> SearchOutbox => Set<SearchOutboxItem>();
    public DbSet<Torrent> Torrents => Set<Torrent>();
    public DbSet<TorrentExtensionSummary> TorrentExtensionSummaries => Set<TorrentExtensionSummary>();
    public DbSet<TorrentFile> TorrentFiles => Set<TorrentFile>();
    public DbSet<TorrentRequest> TorrentRequests => Set<TorrentRequest>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
    }
}
