using Xunit;

namespace Cherry.Infrastructure.Tests.Repositories;

// The integration classes share CHERRY_TEST_POSTGRES and apply the same EF
// migration history. Serializing them prevents one test's startup DDL from
// racing another test's first query while keeping all in-memory tests parallel.
[CollectionDefinition("Postgres integration", DisableParallelization = true)]
public sealed class PostgresIntegrationCollection;
