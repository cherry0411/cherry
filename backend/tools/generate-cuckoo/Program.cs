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

        var filter = new CuckooFilter(capacity: capacity, persistPath: outPath);

        // If file exists, we still recreate from DB; clear in-memory buckets by creating a fresh instance
        // The constructor may have loaded existing data; to ensure fresh, create a new empty filter if file exists
        if (System.IO.File.Exists(outPath))
        {
            filter = new CuckooFilter(capacity: capacity, persistPath: outPath);
            // we'll overwrite later via Save
        }

        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Stream all info_hash rows
        await using (var cmd = new NpgsqlCommand("SELECT info_hash FROM torrents", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            long count = 0;
            while (await reader.ReadAsync())
            {
                var ih = reader.GetString(0);
                if (!string.IsNullOrEmpty(ih))
                {
                    filter.Add(ih.ToLowerInvariant());
                    count++;
                    if ((count & 0x3FFF) == 0) // log every 16384
                    {
                        Console.WriteLine($"Added {count} hashes...");
                    }
                }
            }
            Console.WriteLine($"Total hashes processed: {count}");
        }

        Console.WriteLine("Saving cuckoo filter to disk...");
        filter.Save();
        Console.WriteLine("Done.");
        return 0;
    }
}
