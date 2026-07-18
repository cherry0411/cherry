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

## Iteration S-002 (pre-registered): transactional Meilisearch outbox

### Hypothesis and isolated scope

Replacing the process-memory `MeiliIndexQueue` with a PostgreSQL outbox written
in the same transaction as each accepted torrent will remove the permanent
search-loss windows without extending the crawler's PostgreSQL ACK latency to
include Meilisearch. This iteration changes search delivery durability only. It
does not change metadata retention, the crawler protocol, search document
shape/ranking, or permanent raw-byte retention (which remains 0%).

### Design fixed before implementation

- One compact outbox row per `info_hash`; transactional UPSERT increments a
  generation and resets retry/lease state whenever authoritative searchable
  metadata is inserted or upgraded. This bounds backlog storage and makes
  repeated ingest idempotent while preserving an upgrade that races a worker.
- Workers claim due rows with `FOR UPDATE SKIP LOCKED` and a time-bounded lease.
  They load the current torrent rows from PostgreSQL rather than persisting a
  second document copy in the outbox.
- A claim is deleted only after Meilisearch accepts the document batch, returns
  a parseable `taskUid`, and that task reaches `succeeded`. HTTP 202 alone is
  not an acknowledgement. Network errors, outage, timeout, and Meili `failed`
  tasks retain the row, record a bounded error, increment attempts, and schedule
  exponential retry. Delete predicates include claim owner and generation so a
  concurrent metadata upgrade cannot be acknowledged by an older task.
- PostgreSQL metadata and durable crawler receipts commit before the worker
  talks to Meilisearch. The durable ingest response therefore remains
  independent of Meili availability.
- Backlog depth, oldest enqueued age, due/retrying counts, completed document
  count, task latency, retry count, and last error are exposed as basic
  operational state. A rebuild operation repopulates the outbox from all
  authoritative torrent rows without storing raw metadata.

### Pre-registered failure matrix and success criteria

The implementation is accepted only if real-PostgreSQL plus mock-Meili tests
show: (1) metadata and outbox commit/rollback together for both durable and
legacy ingest; (2) an API/worker crash before delivery leaves a claim
recoverable after lease expiry; (3) outage/submission errors retain rows; (4) a
202 followed by a failed task retains rows and records a retry; (5) only a
polled `succeeded` task removes the matching generation; (6) a concurrent
upgrade survives acknowledgement of the older generation; and (7) a full
rebuild restores a missing outbox deterministically. EF pending-model and
package-vulnerability checks must remain clean.

### Rollback

Disable the outbox worker while retaining its table, then restore the previous
in-memory queue registration if an unexpected search-delivery regression is
found. Migration `Down` drops only the outbox table/indexes. PostgreSQL remains
the authority, so rebuilding Meilisearch from `torrents` is the recovery path;
rollback never requires replaying raw bencode or crawler events.

### Implementation and fault-test result (2026-07-18)

S-002 is implemented locally and remains an iteration, not a final storage or
search configuration:

- migration `20260718133000_AddSearchOutbox` creates the coalescing outbox after
  S-001 and seeds one marker for every pre-existing torrent; its tested `Down`
  removes only the outbox and its tested `Up` recreates it;
- both the durable crawler transaction and the legacy torrent transaction add
  or refresh the marker before PostgreSQL commit. Their API acknowledgement
  paths contain no Meilisearch call;
- each write stores only the 40-character authority key plus delivery state,
  never raw bencode, pieces, file lists, or a duplicate search document;
- workers use `FOR UPDATE SKIP LOCKED`, bounded leases, generation-fenced
  completion, bounded errors, and exponential retries. Unsafe batch/interval
  configuration is clamped, and the effective lease is always longer than the
  configured Meili task timeout plus a polling margin;
- Meili document submission must return a numeric `taskUid`; the worker polls
  `/tasks/{taskUid}` and deletes only after `succeeded`. `failed`, `canceled`,
  malformed responses, HTTP/network outage, and timeout all retain the marker;
- optional `MeiliSearch:ApiKey`, `MEILI_MASTER_KEY`, or `MEILI_API_KEY` is sent
  as a Bearer credential; outbox stats and rebuild operations fail closed behind
  the application's API key;
- `/api/v1/search/outbox/stats` exposes persistent depth/due/retrying/oldest-age
  state and process counters; `/api/v1/search/outbox/rebuild` coalesces the full
  PostgreSQL authority back into the outbox.

Real PostgreSQL 17 plus scripted mock-Meili tests cover transactional markers
for both ingest paths, 202 followed by `failed` and `canceled`, submission
outage, processing-to-success polling, expired-lease recovery, a process crash
after observed Meili success but before PostgreSQL completion, idempotent
replay, concurrent generation upgrade fencing, and full rebuild. The complete
42-test suite passed twice consecutively against PostgreSQL. Build completed
with zero warnings/errors, EF reports no pending model change, the migration
Down/Up round trip passed, and the transitive package vulnerability audit is
clean.

### Remaining measurement gate

Correctness is established, but batch size, polling cadence, lease duration,
retry curve, and Meili document shape are not declared optimal. On the storage
server, measure outbox rows/second and bytes/row, PostgreSQL WAL/event, Meili
documents/second and task p95, visibility lag, backlog recovery slope after a
fixed outage, search quality, index bytes/document, and RSS. Isolate those
variables one at a time against a versioned zero-raw corpus before changing the
defaults.

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
