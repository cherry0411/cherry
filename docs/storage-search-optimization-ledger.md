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

## Iteration S-003 (pre-registered): ranking quality and outbox batch response

This diagnostic is split into two isolated experiments on local real services.
Both use PostgreSQL 17 image digest
`sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193`
and the deployment-compose Meilisearch 1.45.1 digest
`sha256:ac40212f9e5a7526d8007586e3e46fb0441d29dd36c7b02fa2341d2c9a1f6493`.
The deterministic corpus contains normalized fields only and no raw bencode,
pieces, or torrent reconstruction data.

### S-003A — ranking-rule diagnostic

- **Hypothesis:** current ranking puts query `sort=peerCount:desc` and
  `createdAt:desc` before `words`/`exactness`, allowing popular, recent partial
  matches to outrank exact/relevant titles. Moving relevance rules before
  `sort` should improve Recall@20, nDCG@10, and MRR without changing document
  shape, query, corpus, typo settings, filter/sort attributes, or hardware.
- **Corpus/judgments:** versioned deterministic Chinese, Latin, mixed-script,
  exact-title, partial-token, filename, and alias-only cases plus seeded noise.
  Relevance is graded 0–3 and checksummed independently from code. Alias-only
  terms are deliberately absent from `name`; because the production search
  document does not store aliases/files, their model-level Recall@20 ceiling is
  zero. They remain in the headline all-query metric so this limitation cannot
  be hidden.
- **A (control):** production-equivalent searchable document and settings:
  `searchableAttributes=[name]`, ranking
  `sort,createdAt:desc,words,exactness`, and explicit query
  `sort=[peerCount:desc]`.
- **B (treatment):** only the same four ranking rules are reordered to the
  legal relevance-first sequence `words,exactness,sort,createdAt:desc`; the
  explicit peer sort stays present. No new ranking rule is introduced, so the
  experiment isolates priority rather than silently changing typo/proximity
  behavior or removing a product sort.
- **Measurements:** Recall@20, nDCG@10, MRR and zero-result rate overall and by
  query class; first-pass cold-proxy and repeated warm p50/p95; index bytes/doc,
  Meili RSS, Meili version, effective settings JSON and SHA-256. Each arm uses a
  fresh independent index; insertion and settings tasks must reach `succeeded`.
- **Decision:** retain B only if nDCG@10 and MRR improve without Recall@20
  regression outside the structurally impossible alias-only class, warm p95
  does not regress by more than 20%, and storage/RSS remain within noise.
  Synthetic evidence is directional only and does not authorize production
  deployment before a real-query judgment set.
- **Rollback:** delete the B index or restore A settings. PostgreSQL and the
  production `torrents` index are never modified.

### S-003B — outbox batch-size screen

- **Hypothesis:** batch 500 amortizes PostgreSQL claim/load/ack and Meili task
  overhead better than 100 without the per-task latency/RSS cost of 1000; the
  optimal point is a response curve, not an assumed default.
- **Only changed variable:** normalized `SearchOutboxOptions.BatchSize` is
  `100`, `500`, or `1000`. Corpus, PostgreSQL schema/data, Meili settings,
  task-poll interval, lease, HTTP client, worker count (one), and container
  resources stay fixed. Every arm starts from the same re-seeded outbox and a
  fresh empty index.
- **Measurements:** end-to-end documents/second, per-task p50/p95, task count,
  final outbox depth, PostgreSQL and Meili RSS, Meili index bytes/doc, and exact
  indexed-document count. Warm-up is one unreported batch; measurement drains
  the remaining fixed corpus. Failed tasks or count mismatch invalidate an
  arm.
- **Decision:** prefer the smallest batch within 5% of maximum throughput when
  its task p95 and RSS are no worse; this short synthetic screen cannot by
  itself change the production default.
- **Rollback:** discard the isolated benchmark database/index/containers. No
  application configuration or migration changes are part of the experiment.

### S-003C — current schema and 1/7/15/30-day heat audit

This is a source-and-migration audit, not an authorization to mutate the
production schema. The repeatable harness applies the real EF migrations to an
isolated PostgreSQL 17 database and records all columns, indexes, empty-relation
sizes, migration IDs, and a canonical schema checksum. It also retrieves the
effective Meilisearch settings rather than trusting only the submitted JSON.

The current PostgreSQL model has eight business tables:

- `torrents`: 40-character hex hash primary key, name, piece length, total
  length, file count, private flag, source, one cumulative peer count and its
  update time, created/updated times, policy/region/retention/refetch state;
- `torrent_files`: one heap row per path and length, no primary key, plus a
  hash lookup B-tree;
- `torrent_extension_summaries` and `metadata_decisions` for the durable
  retention actions;
- `rejected_hashes`, which overlaps the reject authority represented by
  `metadata_decisions` while the legacy interface remains alive;
- `durable_batch_receipts`, `search_outbox`, and `torrent_requests` for
  transport/search delivery/manual fetch workflow.

The current Meilisearch document repeats the 40-character hash and stores only
`name,totalLength,fileCount,isPrivate,peerCount,createdAt`. It cannot retrieve a
filename or alias because neither is projected. Its current rule order is
`sort,createdAt:desc,words,exactness`, and every query requests
`peerCount:desc`, so popularity and age are evaluated before textual
relevance.

#### Field-removal hypotheses to measure before migration

- With raw reconstruction explicitly out of scope, `piece_length` has no
  search/detail use and is a direct removal candidate. Metadata `updated_at`
  is redundant for immutable content; `needs_refetch` is incompatible with the
  no-refetch requirement.
- Change the authority key from hex `varchar(40)` to `bytea(20)` and evaluate a
  narrow internal `bigint` ID for joins and Meili hydration. Store low-cardinal
  source/region/policy as codes rather than repeated strings. Pack private and
  retention state into flags. These are hypotheses until a shadow migration
  proves referential and API equivalence.
- Do not keep every file as a permanent PostgreSQL row. Retain a bounded,
  zero-raw normalized compact record or summary in compressed immutable
  storage; discard/summarize above the configured file/path budget. Project
  only a measured, deduplicated alias subset to Meili. This preserves the
  filename-search experiment without paying one search term and one PG heap
  tuple for every path.
- After legacy route removal, merge `rejected_hashes` and the hash-only/reject
  decision authority into one compact table. Replace free-form repeated
  reason/policy strings with versioned codes. Keep receipts and outbox because
  they close correctness windows; compact their hash/identity representation
  instead of deleting them. Keep `torrent_requests` outside the permanent
  catalog with a TTL if manual fetch remains a product feature.

#### Why current `peer_count` cannot mean recent heat

The crawler increments only when a metadata request key is already in its
capacity-bounded in-process LRU or the hash is already remote-known. A first
downloadable observation is not incremented. The LRU key is
`(infohash,IP,port)`; eviction or restart makes the same peer new again, while
the two regional processes cannot see each other's keys. The 20-second upload
contains only aggregate `{hash:count}` values, with no crawler/batch receipt or
peer identity. The server blindly adds the count. Therefore cross-region and
post-eviction observations double count; a retried request can double count;
and the current crawler loses the swapped batch on an HTTP failure. Decay is a
separate manually invoked endpoint that halves every eligible value again on
each invocation. Peer changes do not currently enqueue a new Meili document,
so the value used for query sorting can also be stale.

#### Lowest-storage exact-window candidate

Pre-register a shadow model with a narrow catalog ID:

```text
torrent_peer_last_seen
  torrent_id       bigint
  peer_fp          bytea(16)       # keyed hash of canonical family/IP/port
  last_seen_hour   integer
  primary key (torrent_id, peer_fp)

torrent_heat
  torrent_id       bigint primary key
  unique_1d        integer
  unique_7d        integer
  unique_15d       integer
  unique_30d       integer
  as_of_hour       integer
```

Crawler sighting batches use their own durable
`crawler_id,epoch,sequence,payload-checksum` receipt. Central UPSERT takes the
maximum hour for the same torrent/peer, so response replay and two regions do
not multiply the observation. Rows older than 30 days are removed. An hourly
materialization counts distinct latest observations at the four thresholds;
it never sums per-day uniques, which would count a peer seen on two days twice.
Only the latest peer relation and four materialized integers persist—no
long-term event stream and no IP. The 128-bit keyed fingerprint is
operationally exact with a quantifiable negligible collision risk; a demand for
mathematical exactness requires the materially larger canonical endpoint.

The first Meili shadow shape should expose four sortable heat integers (or
quantized tiers only if a real corpus proves indistinguishable quality). The
default product query uses weekly heat as a tie-breaker, not as the first
relevance decision: `sort=[heat7d:desc,createdAt:desc]` with a candidate full
rule set `words,typo,proximity,attribute,exactness,sort`. The S-003A treatment
deliberately tests only a reorder of the existing four rules first; adding
default relevance rules, aliases, and heat is reserved for separate
single-variable iterations.

Rollback for every S-003C proposal is to drop only its shadow tables/index and
restore the prior Meili shadow index. No current table, peer endpoint, or
production search setting is changed in S-003.

### S-003 execution record (2026-07-18)

The authoritative local diagnostic command was:

```powershell
docker pull getmeili/meilisearch:v1.45.1@sha256:ac40212f9e5a7526d8007586e3e46fb0441d29dd36c7b02fa2341d2c9a1f6493
python -m py_compile scripts/benchmark/storage_search_s003.py
python -m unittest scripts.benchmark.test_storage_search_s003 -q
python scripts/benchmark/storage_search_s003.py
```

The seven harness tests passed. The final result is local run
`20260718T104129Z`, corpus `cherry-search-s003-v1`, corpus SHA-256
`b709686a9c168500a3d63adadd7e4b2b626ff9c45f319b06e906c3e3e2994bf0`,
harness `s003-harness-v2`, harness SHA-256
`2783001f583c9a4fb45ef4d5370b64e35d4a4447dc188d0b7c611cebcf3c2709`.
It used PostgreSQL 17.10, Docker 28.3.2, and the deployment-compose
Meilisearch 1.45.1 image; every container was limited to 2 CPUs and 1 GiB for
this diagnostic. All owned containers were removed after the run. Permanent
raw-bencode retention was 0%.

#### Ranking result

| Arm | Recall@20 | nDCG@10 | MRR | zero-result | cold-proxy p50/p95 | warm p50/p95 | Meili DB bytes/doc | document-store bytes/doc | RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A current | 0.0000 | 0.0000 | 0.0000 | 0.3000 | 49.08 / 51.71 ms | 47.93 / 52.35 ms | 816.3 | 143.36 | 88.2 MiB |
| B relevance first | 0.4500 | 0.5195 | 0.6000 | 0.3000 | 48.51 / 52.27 ms | 48.02 / 52.11 ms | 848.3 | 143.36 | 88.7 MiB |

For the seven name-searchable cases alone, B measured Recall@20 `0.6429`,
nDCG@10 `0.7422`, and MRR `0.8571`; A returned non-empty but judged-irrelevant
top-20 results for all seven. All three filename/alias-only cases had no
result in both arms, exactly matching the declared model ceiling of zero.

Submitted-setting checksums were
`72f45b2bf792bf71754c25b7028c3ebd9d3644d48f529d79101bdd4eaa80096b`
(A) and
`766dc69d0a59782a054db1c90071d54eb5599743164ee60424b372b9cc7d6347`
(B). Settings read back from Meilisearch 1.45.1 had checksums
`7dd6ecb702aafe3ad664fddc18c9ac7b6dc3fe84df2b780001ffed905302b016`
(A) and
`5ebeb979f3f3be4d359933f6e8472cf6e0d11f1687a37223e3025f2d2c39e829`
(B). Every settings/index task reached `succeeded`.

The result establishes the predicted failure mechanism, not its absolute
production prevalence. The corpus intentionally places 28 very high-peer,
recent partial or competing matches ahead of a judged title—more than the
20-result cutoff—so A=0 is an adversarial stress result from ten synthetic
queries, not an estimate that production search currently has zero recall.
One mixed query also shows that hand judgments and equally matching generated
distractors need refinement. No production ranking change is authorized until
real query/relevance judgments repeat the direction. The unchanged
143.36-byte Meili document-store measure and sub-1% RSS/latency differences
show no material cost signal; the 3.9% total-database delta across independent
fresh processes is treated as short-run page/compaction noise, not a ranking
storage effect.

The next isolated ranking iterations are: (1) collect privacy-safe real-query
judgments; (2) compare removing or demoting explicit heat sort; (3) add the
default typo/proximity/attribute rules as one separate treatment; and (4)
compare bounded alias shapes. Rollback remains restoring A's read-back-verified
settings; the current production index was never touched.

#### Outbox batch result

Each arm used its own newly migrated/seeded PostgreSQL process, one disposable
warm-up Meili process, and one fresh measured Meili process. Thus PostgreSQL
shared buffers, allocator high-water marks, relation churn, and Meili index
state could not carry across batch arms.

| Batch | docs/s | tasks | pipeline p50/p95 | Meili task p95 | PG RSS | Meili RSS | final DB bytes/doc |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 457.0 | 100 | 202.72 / 262.53 ms | 240.45 ms | 103.9 MiB | 101.2 MiB | 2167.6 |
| 500 | 1469.6 | 20 | 309.91 / 373.03 ms | 324.33 ms | 103.7 MiB | 93.4 MiB | 2238.5 |
| 1000 | 2174.8 | 10 | 397.99 / 713.62 ms | 604.02 ms | 104.2 MiB | 103.1 MiB | 2195.9 |

Every arm claimed, loaded, indexed, task-polled, generation-acknowledged, and
removed exactly 10,000 rows; final outbox depth was zero and Meili contained
exactly 10,000 documents. Batch 1000 won this short run's throughput but had
roughly 1.9x batch 500's pipeline p95. The result does not change the deployment
default of 500: the driver faithfully executes the PostgreSQL SQL and real
Meili task protocol but excludes .NET/EF, production networking, concurrent
search traffic, visibility-lag SLOs, and long-run compaction. The 10k database
bytes/doc values are transient and non-monotonic, so they are not capacity
estimates. The next gate is a randomized repeated 500/1000 comparison under a
fixed live-shaped ingest rate, concurrent queries, backlog recovery, and disk
steady state.

#### Real-schema audit result

The harness successfully applied all six migrations through
`20260718133000_AddSearchOutbox` to PostgreSQL 17.10. The canonical
columns/indexes/migrations checksum is
`8ac972380e73d1a0fa56fb5a2fa65fea3833d509cb758bb30df73b211d243d9c`.
Empty total-relation baselines were: `torrents` 32 KiB; `search_outbox` and
`torrent_requests` 24 KiB each; `metadata_decisions`, `rejected_hashes`, and
`torrent_files` 16 KiB each; `durable_batch_receipts` and
`torrent_extension_summaries` 8 KiB each. These empty allocations are schema
verification only, not per-row sizing. The full result JSON records every
column, index definition, migration ID, and nullable/type property.

#### Historical pilots retained, but invalid for deployment inference

| Local run | Meili | outbox docs/s for 100 / 500 / 1000 | Status |
|---|---|---:|---|
| `20260718T102937Z` | 1.43.0 | 771 / 3082 / 3877 | invalid: shared PostgreSQL process made cache/RSS arm-order dependent; wrong deployment version |
| `20260718T103345Z` | 1.43.0 | 440 / 1218 / 2101 | invalid for deployment: isolated PG fix, but wrong Meili version and no real-schema audit |
| `20260718T103704Z` | 1.43.0 | 456 / 1411 / 1446 | invalid for deployment: real-schema audit present, but wrong Meili version/harness version |
| `20260718T104129Z` | 1.45.1 | 457 / 1470 / 2175 | final S-003 directional diagnostic |

All four pilots produced the same synthetic ranking quality numbers, which is
consistent with the rule-order mechanism, but only the final deployment-image
run is used for the reported checkpoint. The large absolute throughput spread
is itself evidence that a single short local run cannot size a storage server
or promote a batch setting.
