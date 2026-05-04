using Cherry.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cherry.Infrastructure.Data;

public class AppDbContext : DbContext
{
    public DbSet<Torrent> Torrents => Set<Torrent>();
    public DbSet<TorrentFile> TorrentFiles => Set<TorrentFile>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
    }
}
