using System;
using System.Threading.Tasks;
using Npgsql;
using Cherry.Infrastructure.Dedup;

class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: generate-cuckoo <pg-connection-string> <output-path> [capacity]");
            return 2;
        }

        var connStr = args[0];
        var outPath = args[1];
        var capacity = 100_000_000L;
        if (args.Length >= 3 && long.TryParse(args[2], out var c)) capacity = c;

        Console.WriteLine($"Connecting to PG and building cuckoo filter -> {outPath} (capacity={capacity})");

        // This is an exact rebuild tool: never trust or merge an existing
        // snapshot (including the unsupported legacy gzip format).
        using var filter = new CuckooFilter(
            capacity: capacity,
            persistPath: outPath,
            loadPersistedSnapshot: false);

        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Stream the complete exact processed-hash authority. A snapshot that
        // omitted rejected hashes could only be used after another full replay;
        // including both tables keeps this diagnostic/offline tool honest.
        await using (var cmd = new NpgsqlCommand(
                         """
                         SELECT info_hash FROM torrents
                         UNION ALL
                         SELECT encode(info_hash, 'hex') FROM rejected_hashes
                         """,
                         conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            long count = 0;
            long representedByCollision = 0;
            while (await reader.ReadAsync())
            {
                var ih = reader.GetString(0);
                if (!string.IsNullOrEmpty(ih))
                {
                    var normalized = ih.ToLowerInvariant();
                    if (!filter.Add(normalized))
                    {
                        if (!filter.MightContain(normalized))
                        {
                            throw new InvalidOperationException(
                                $"Cuckoo filter capacity exhausted after {count} exact hashes; snapshot was not saved.");
                        }

                        representedByCollision++;
                    }
                    count++;
                    if ((count & 0x3FFF) == 0) // log every 16384
                    {
                        Console.WriteLine($"Added {count} hashes...");
                    }
                }
            }
            Console.WriteLine(
                $"Total hashes processed: {count} ({representedByCollision} represented by fingerprint collision)");
        }

        Console.WriteLine("Saving cuckoo filter to disk...");
        filter.Save();
        Console.WriteLine("Done.");
        return 0;
    }
}
