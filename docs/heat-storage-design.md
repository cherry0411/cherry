# Heat and compact-key design ledger

This document records the evidence and open decisions for recent-activity
ranking. It is an iteration ledger, not a declaration of a final schema.
Permanent raw bencode retention remains **0%**.

## Required semantics

- Search must expose independent `heatWindow=24h|3d|7d|15d` and optional
  discovery-age filtering; the heat window must not be confused with first-seen
  time.
- The 24h product value is exact unique actor/hash across the latest 24 complete
  UTC hours. The 3/7/15-day values remain sums of exact actor-days. Port/node-ID
  changes, HTTP replay and observation in both regions must not inflate either.
- Raw IP addresses, DHT node IDs and peer endpoints must not be retained in
  PostgreSQL, Meilisearch, archives or backups.
- Inbound `get_peers`, direct `announce_peer`, and peers repeated by an active
  lookup are different signals. The crawler's own active lookups must not form
  a positive feedback loop in demand heat.
- Heat transport needs its own stable crawler/epoch/sequence receipt. Metadata
  durability and heat evidence may use different loss budgets, but neither may
  silently turn a replay into extra heat.
- Meilisearch text relevance remains ahead of the selected heat field. Heat is
  a tie-breaker for non-empty searches and the primary ordering for an empty
  hot-list query.

## Retired predecessor was not a heat authority

The pre-CHHT crawler incremented `peer_count` only after a metadata request hit
its finite `(infohash, IP, port)` LRU or after the hash is already in the
remote-known cache. A first usable peer is not counted. The 20-second HTTP path
sends only `{hash: count}`, has no identity or receipt, drops the swapped map on
failure, and cannot deduplicate between regions. PostgreSQL accumulates the
integer and a manually invoked endpoint halves values whose last update is more
than seven days old. Meilisearch is not updated by this path. Therefore the
field is neither a peer count nor a 1/7/15/30-day activity measure and must be
retired rather than migrated as truth. The implemented CHHT/SQLite/frame path
described below replaces it; `peer_count` is not present in the compact catalog.

## K-001: compact internal key cost on real PostgreSQL 17

### Question

Does changing the catalog to an internal `bigint` ID plus a unique 20-byte
binary info hash save enough child-table space to justify migration complexity?
The comparison deliberately includes the extra compact catalog primary-key and
unique-hash indexes.

### Fixed environment and corpus

- PostgreSQL `17.10-alpine`, image digest
  `sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193`.
- Docker limit: 2 CPUs, 2 GiB RAM, 2 GiB tmpfs data directory.
- `wide`: `varchar(40)` primary key repeated in every child row and index.
- `compact`: identity `bigint` primary key, `bytea(20) UNIQUE` authority key,
  and `bigint` child references.
- Identical deterministic logical data: 100,000 torrents, ten file rows per
  torrent (1,000,000 rows), and five 16-byte actor fingerprints per torrent
  (500,000 recent-activity rows).
- Both shapes have one lookup index on files and a composite heat primary key.
  The run used `ANALYZE` before `pg_total_relation_size` measurements.

### Result (2026-07-18)

| Relation group | Wide hash | Compact ID | Change |
|---|---:|---:|---:|
| 100k catalog | 20.61 MB | 17.30 MB | 16.1% smaller |
| 1M file rows + index | 119.91 MB | 79.58 MB | 33.6% smaller |
| 500k actor rows + PK | 106.77 MB | 68.87 MB | 35.5% smaller |
| Combined | 247.29 MB | 165.75 MB | 33.0% smaller |
| 1M file insert wall time | 13.261 s | 7.593 s | 42.7% lower |
| 500k actor insert wall time | 9.002 s | 5.977 s | 33.6% lower |

### Inference and limits

The direction is strong enough to keep compact internal IDs in the target
design: repeated child keys dominate the extra catalog index. It is not a
production capacity estimate. Names and paths are synthetic, the data directory
is tmpfs, no foreign keys were enabled, WAL bytes were not isolated, and the
test has one fixed fan-out. Before migrating a non-empty production database,
repeat on the captured zero-raw corpus with actual file-count and active-actor
distributions, record WAL/checkpoint cost, and test online backfill headroom.

Rollback gate: retain the old key columns until row-count, hash uniqueness,
foreign-key, API lookup, outbox replay and Meilisearch rebuild checks all pass.
The fresh storage host may start directly on the compact shape, but an existing
host must use a staged add/backfill/dual-read/cutover/drop sequence.

## K-002: actor-fingerprint width on real PostgreSQL 17

### Fixed comparison

Using the same PostgreSQL image and limits as K-001, each arm stored 500,000
rows with the same logical shape: `torrent_id bigint`, one actor fingerprint,
`last_seen_hour int`, `signal smallint`, and a composite primary key. Only the
fingerprint type changed. The deterministic input distribution and row count
were identical between arms.

### Result (2026-07-18)

| Fingerprint type | Table + indexes | Insert wall time |
|---|---:|---:|
| `bytea(16)` | 68.87 MB | 6.032 s |
| `uuid` (128-bit) | 64.84 MB | 6.718 s |
| `bigint` (64-bit) | 54.48 MB | 3.182 s |

The fixed-width `uuid` representation is 5.9% smaller than `bytea(16)` in this
shape. A 64-bit key is 16.0% smaller than UUID (20.9% smaller than bytea) and
was about twice as fast to insert in this isolated tmpfs run.

### Collision and adversarial gate

A fingerprint collision only merges two actors for the same torrent because
`torrent_id` remains part of the key. Under an ideal keyed 64-bit hash, even an
extreme torrent with one million distinct actors has an approximate birthday
collision probability of 2.7e-8; ordinary torrents are far below that bound.
This is an undercount, not a false popularity increase. The bound assumes a
secret, keyed fingerprint and non-adversarial key compromise. It does not by
itself resolve NAT semantics, actor-identifier spoofing, key rotation, or the
WAL cost of retaining exact rows. The target therefore keeps 64-bit as the
storage-leading candidate, with a keyed construction, explicit key epoch, and
a 128-bit shadow sample required before promotion.

## H-001 decision: unique network actor-day for production v1

A reverse audit rejected full 30-day actor last-seen as the production default.
An additional PostgreSQL 17 measurement of a row carrying a `bigint` torrent
ID, 64-bit fingerprint, last-get/announce hours and region mask measured about
107.8 bytes per actor-hash pair (146.6 bytes with a binary hash key and 203.9
bytes with the old hex key). At a hypothetical 50 million new actor-hash pairs
per day, a 30-day table can reach roughly 162 GB before WAL, dead tuples,
backups and page cache. A TTL B-tree amplifies every time update; omitting it
makes expiration scan the large relation. The table is exact only for an
imperfect network identity proxy, so this cost is not justified as the first
production authority.

Production v1 defines heat as **unique network actor-days**:

```text
IPv4 actor = HMAC64(secret, family || exact public IPv4)
IPv6 actor = HMAC64(secret, family || public /64 prefix)

ephemeral SQLite per UTC day:
  hashes(id, info_hash20 unique)
  seen(hash_id, actor_fp64) primary key(hash_id, actor_fp64)
  receipts(crawler_id, epoch, start_sequence, end_sequence, digest)
  completions(crawler_id, epoch, start_sequence, next_sequence, clean=1)

durable PostgreSQL:
  heat_day_frames(day, shard, codec_version, entry_count,
                  coverage_status, checksum, compressed_payload)
  heat_projection_state(index_generation, projected_through, target_day,
                        shard, after_id, pending_task_uid, payload_digest)
```

The hot path does not write PostgreSQL. One exact SQLite/WAL file per UTC day
accepts both pre- and post-metadata heat, acknowledges only after a FULL commit,
and makes replay idempotent with `INSERT OR IGNORE`. After the late-arrival
grace window, finalization groups by raw hash, batch-maps searchable catalog
IDs and writes 64 immutable daily shards. Each shard uses `id & 63`; sorted
`id >> 6` values are delta-uvarint encoded with their uvarint count. Actor
identities are deleted only after the sealed frame manifest is durable and
verified. PostgreSQL never receives actor identities and has no long-lived
per-torrent heat row.

Only inbound `get_peers` is admitted to the primary v1 activity metric. Active
lookup responses, metadata fetch success, pending/user lookups and all crawler
generated traffic are excluded. `announce_peer` stays shadow-only until the
current fixed token is replaced with a rolling source-IP-bound HMAC token; an
unvalidated announce is trivially forgeable and cannot be called supply heat.
Raw addresses, ports and node IDs never leave crawler memory. Because v1 never
compares actors across days, both regions derive the same day key from a master
secret and may rotate it at each UTC boundary; only the previous day key is
retained for the bounded late-arrival window. A key must never rotate inside an
open day, which would silently double-count that day.

The four product values are sums of daily actor-days, not distinct actors over
the whole window. This intentional definition counts a consistently active
actor once per day and a same-day retry only once. UI/API terminology must say
network activity/heat, never peers, users or seeders. If product evidence later
requires strict cross-day uniqueness, compare adaptive sparse HLL or exact
last-seen using a deterministic 1% oracle sample.

If the last successful projection is `D-1`, the only IDs whose vectors can
change are in `D ∪ D-1 ∪ D-7 ∪ D-15 ∪ D-30`. For each affected ID the projector
streams the current 30 frames, computes absolute 1/7/15/30-day values, skips an
unchanged vector and uses an idempotent Meilisearch partial PUT. Missed days
must be replayed sequentially; initial or new-index rebuilds use the complete
30-day union. A sealed empty day is explicit. A missing or unsealed day halts;
a sealed partial day advances with zero contribution and a cleared coverage
bit, so later good days remain usable without fabricating observations. Frame GC follows the
`projected_through` watermark, so outages may temporarily retain more than 31
days.

Catalog-proven heat may create an `{id, heat*}` Meili stub. The metadata outbox
also uses partial PUT and never sends heat fields, so later metadata preserves
the vector. This removes the need for a PostgreSQL current-heat row or an extra
hydration outbox; API responses hydrate the Meili-ranked IDs from PostgreSQL.

Scale gate: if exact current-day state projects above 5 GiB, heat WAL exceeds
25% of metadata-ingest WAL, heat ingest adds over ten CPU percentage points, or
ACK p99 exceeds 250 ms, activate the tested CRC-framed append-log fallback.
Redis is not a default dependency: exact Sets cost more memory/AOF than SQLite,
HLL can err in both directions and the tested Bloom conservatively undercounted.
A bounded Redis Bloom may be added only as a disposable current-day cache if
the product later requires live intraday ranking; it never becomes the daily
authority.

## H-002 decision: authenticated fail-closed daily completion

Receipt presence is not coverage proof. A day is `complete` only when every
configured `ExpectedCrawlerId` has an authenticated, idempotent completion in
that day's SQLite file and sealing re-verifies a single-epoch receipt chain
that starts and ends at the completion's exact sequence anchors. Missing,
conflicting, discontinuous, multi-epoch, dirty, or late completion evidence is
`partial`. Empty days require an explicit completion with `start == next`.

Batch and completion authentication resolve a crawler-specific Base64 transport
key. Production startup fails if any expected crawler lacks its own key; the
cross-region actor master remains shared but cannot authenticate transport.
The crawler persists a spool-epoch-bound boundary ledger. UTC rollover takes a
writer barrier, and completion waits until the ordinary spool cursor proves all
records through the day's exclusive end were durably receipted. Queue drop,
writer loss, negative day-closed receipt, first partial startup, clock jump,
abnormal restart, or any same-day graceful stop/restart marks the day dirty.
This intentionally permits false `partial` and forbids false `complete`.

## H-003 decision: exact rolling 24h plus compressed daily 3/7/15d

CHHT v2 adds the UTC hour and carries a stable rolling actor fingerprint. The
backend rejects any authenticated bucket newer than its current UTC hour. The
hourly projector targets `currentHour - 1`, so a partial current hour never
appears in search. A repeated actor across any number of those 24 buckets counts
once, rather than once per actor-hour.

The stable actor is written only to host-local `heat-rolling-24h.sqlite3`. Its
rows expire outside the rolling window, the database is disposable, and both
backup creation and restore verification reject archives containing it. Before
the daily SQLite write, the backend re-HMACs that stable token with a distinct
storage-only per-day key. PostgreSQL receives only sealed compressed daily
counts; Meili receives only final integers.

Every rolling connection enables SQLite `secure_delete=ON`, so expired actor
cells are overwritten rather than left on the freelist. Every completed expiry
transaction performs a checked `wal_checkpoint(TRUNCATE)`; a busy checkpoint is
a retryable failure, never a successful privacy claim. Once per 24 projected
hours, a freelist/size gate may additionally run `VACUUM`; this bounds peak
pages without paying a full-file rewrite every poll on 2C/4G.
The rolling database and its WAL remain excluded from every backup. Physical
privacy verification checks secure-delete, zero freelist after forced
maintenance, page reclamation, and a truncated WAL.

The rolling file is also bounded independently from the daily authority: the
default hard maximum is 5 GiB and ingest requires at least 2 GiB free on its
filesystem. Expiration and WAL truncation run before the admission check;
crossing either watermark yields a non-ACKed capacity failure so crawler spools
retain the batch for retry rather than filling the storage host.

Rolling freshness is reported as `HeatAsOfUtc` at the end of the latest complete
projected hour plus `HeatCoverageHours`. Storage uptime cannot prove crawler or
tunnel completeness, so the current safe phase reports rolling coverage as
unknown/`0`; it never regrows coverage from storage runtime alone.
Daily 3/7/15-day coverage remains complete-day coverage multiplied by 24. Redis
is not required: it adds a durability/memory component without improving exact
deduplication or the compressed daily authority.

The next coverage phase requires a crawler-specific authenticated hour closure,
including zero-event hours. Each closure binds crawler ID, spool epoch, UTC
hour, start sequence, and exclusive end sequence, and is emitted only after the
ordinary spool cursor proves every range through that hour is ACKed. Backend
acceptance must be replay-idempotent, reject conflicts/epoch changes/gaps and
future/current incomplete hours, and retain a 24-bit completeness mask only
where every `ExpectedCrawlerId` closed the same hour. Tunnel backlog or crawler
restart may delay/lose coverage but can never fabricate it. Numeric heat may
continue updating independently while coverage remains zero.

An authenticated late batch for the already projected complete target hour is
reprojected immediately only when its exact count differs from the last Meili
value. Dirty state caused solely by the current incomplete hour compares equal
and therefore does not create a 30-second projection loop. Hashes not yet in the
PostgreSQL catalog enter an hourly deferred retry set; they are mapped at most
once per target hour and their active/deferred/dirty/dictionary rows are removed
after rolling expiry. This bounds CPU rescans without dropping pre-metadata heat.
