using Npgsql;

namespace Cherry.Infrastructure.Repositories;

internal static class ExactHashTransactionLock
{
    public static async Task AcquireAsync(
        IEnumerable<string> infoHashes,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var hashes = infoHashes
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (hashes.Length == 0)
            return;

        await using var command = new NpgsqlCommand(
            """
            SELECT pg_advisory_xact_lock(hashtextextended(hash, 0))
              FROM unnest(@hashes::text[]) AS locked(hash)
             ORDER BY hash
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("hashes", hashes);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task DeleteDecisionsForTorrentsAsync(
        IReadOnlyCollection<string> infoHashes,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (infoHashes.Count == 0)
            return;

        await using var command = new NpgsqlCommand(
            """
            DELETE FROM metadata_decisions
             WHERE info_hash IN (
                 SELECT decode(hash, 'hex')
                   FROM unnest(@hashes::text[]) AS incoming(hash))
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("hashes", infoHashes.ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
