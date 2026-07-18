# Storage / search optimization ledger

This ledger records each isolated storage or search experiment, its evidence,
rollback, and the next decision gate. The system remains in iterative
optimization; entries are not declarations of a final or stable configuration.
Permanent raw bencode retention remains **0%** throughout.

## Iteration S-001: remove unused PostgreSQL trigram indexes

### Hypothesis and scope

The application search path calls Meilisearch and then hydrates its ranked
`infoHash` hits from PostgreSQL. Detail lookup reads `torrent_files` only by
`info_hash`. No application query searches `torrents.name` or
`torrent_files.path_text` with `LIKE`, `ILIKE`, trigram similarity, or a
PostgreSQL full-text operator. Therefore the following GIN indexes have write,
WAL, and disk cost but no read benefit in the current architecture:

- `idx_torrents_name_trgm`
- `idx_torrent_files_path`

This iteration changes only those indexes. It retains the torrent primary key,
`idx_torrent_files_info_hash`, recent-time index, and peer-count index. It does
not change the four retention actions, the `> 2000` summary policy, the durable
ACK protocol, Meilisearch settings, or crawler behavior.

### Reproducible synthetic corpus

PostgreSQL 17 with `pg_trgm`, two otherwise identical schemas:

- 10,000 torrents with mixed Chinese and Latin names;
- 20 paths per torrent (200,000 file rows), mixed Chinese and Latin paths;
- the indexed schema has the two trigram GIN indexes;
- the lean schema has only the indexes used by current application queries;
- each bulk insert is measured with
  `EXPLAIN (ANALYZE, BUFFERS, WAL, SUMMARY)`;
- sizes come from `pg_relation_size`, `pg_indexes_size`, and
  `pg_total_relation_size` after `ANALYZE`.

The corpus generator is deterministic: torrent hashes are 40-character values
derived from `md5(i::text)`, and names/paths select Chinese or Latin templates
from `i % 3` and `file_number % 4`. Re-run at 100k/1M torrents before choosing
production storage sizing; this small corpus establishes direction, not final
capacity.

### Result (2026-07-18)

| Operation | With unused GIN | Lean | Change |
|---|---:|---:|---:|
| Insert 10k torrents | 170.0 ms | 57.1 ms | 66.4% less time (2.98x throughput) |
| Torrent insert WAL | 8.84 MB | 4.59 MB | 48.1% less WAL |
| Insert 200k file rows | 4,294.7 ms | 1,030.0 ms | 76.0% less time (4.17x throughput) |
| File insert WAL | 186.87 MB | 51.46 MB | 72.5% less WAL |
| Name trigram index | 3.55 MB | 0 | removed |
| Path trigram index | 12 MB | 0 | removed |
| Combined table + index size | about 43.3 MB | about 27.8 MB | about 35.9% less |

`pg_stat_user_indexes.idx_scan` was zero for both GIN indexes. Source inspection
also confirms this is structural, not an artifact of a short observation
window: PostgreSQL receives only exact hash hydration and detail queries.

### Implementation and verification

- migration `20260718132000_RemoveUnusedTrigramIndexes` drops both indexes;
- `Down` recreates both GIN/trigram definitions;
- the EF model snapshot omits them;
- a PostgreSQL integration regression asserts the current schema omits the two
  GIN indexes while retaining the exact file-hash lookup index;
- `dotnet ef migrations has-pending-model-changes` reports no changes;
- all 36 pre-existing tests plus the new schema test pass on a fresh PostgreSQL
  database;
- explicit migration `Down` to `20260718131000` recreated both indexes, and
  migration `Up` removed both again.

### Search-quality impact and rollback

Current search behavior is unchanged because Meilisearch remains the only
full-text query engine. Exact detail lookup remains indexed. If a PostgreSQL
name/path fallback is later introduced, first roll back this migration (or add a
purpose-built index proven against the real query corpus), then enable that
fallback. Recreating a large GIN index is an online-operations event and must be
scheduled with disk/WAL headroom.

## P0 found but deliberately not mixed into S-001: durable search indexing

`MeiliIndexQueue` is currently process memory only. It clears its buffer before
the HTTP call, retries three times, and then logs and discards the batch.
`IndexDocumentsAsync` treats HTTP acceptance as success without polling the
Meilisearch task to `succeeded`. A process crash, extended Meili outage, or an
asynchronously failed Meili task can therefore leave committed PostgreSQL rows
permanently absent from search until an operator performs an unimplemented full
rebuild. Authoritative metadata is safe, but search freshness/completeness is
not.

The next isolated P0 should add a PostgreSQL transactional search outbox (or an
equally durable monotonic rebuild cursor), idempotent batch submission, Meili
task-status polling, retry/dead-letter visibility, and a full rebuild command.
Required metrics: outbox depth, oldest age, documents/second, task latency,
retry count, terminal failures, and PostgreSQL-to-Meili visibility lag.

## Storage-server experiment gate

Before using live traffic to choose the final schema or Meili document shape,
capture a versioned zero-raw corpus and run at least these fixed experiments:

1. Retention distribution and bytes per accepted event for
   `normalized/summary/hash_only/reject`, including file-count/path-byte
   percentiles and the `> 2000` summary boundary.
2. PostgreSQL ingest throughput, WAL bytes/event, checkpoint latency, table/
   index bytes/event, autovacuum lag, and duplicate/upgrade ratios at the
   crawler's measured sustained and burst rates.
3. Meili index bytes/document, indexing documents/second, task latency, RSS,
   cold/warm p50/p95 search latency, and result-quality judgments over Chinese,
   Latin, mixed-script, exact-name, partial-name, and filename queries.
4. Failure tests: PostgreSQL restart, Meili outage/restart, API restart between
   commit and indexing, outbox replay, disk high-watermark, backup/restore, and
   full Meili rebuild from PostgreSQL/normalized storage.
5. A/B document shapes: name only; name plus bounded representative file
   aliases; and a compact deduplicated search-alias field. Do not index every
   path by default—the winner must preserve filename-query quality at lower
   bytes/document and indexing CPU.

Every subsequent iteration should record the immutable code/config revision,
corpus ID and checksum, warm-up period, measurement window, control/treatment,
result, inference, rollback trigger, and next hypothesis here.
