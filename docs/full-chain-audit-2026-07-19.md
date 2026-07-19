# Cherry full-chain audit — 2026-07-19

Status: active optimization audit. This is not a champion declaration or a
stability-test sign-off.

## Why “700k in 10 hours” was not an actionable rate

The dashboard mixed three different counters:

- `totalTorrents` came from PostgreSQL `pg_class.reltuples`. It changes after
  `ANALYZE` and is an estimate, not a time-window ingest counter.
- the old “metadata rate” used the process-local processed-hash/Cuckoo-filter
  count. It includes non-metadata decisions, rebuilds on API startup, and is not
  an end-to-end committed rate.
- `/health.heat.acceptedRecords` counts heat observations since the current API
  process started. It is not a metadata counter.

The trusted funnel is therefore:

`valid metadata at crawler -> durable crawler spool -> delivered batch records
-> first-written PostgreSQL torrents -> completed Meilisearch outbox rows`.

At 11:54 CST the exact PostgreSQL torrent count was 807,059 while the two
durable receipt sequences totalled about 1.159 million delivered records. This
implies roughly 30% cross-region/session duplication before accounting for old
receipt epochs. A 30-second paired sample at 11:57:38–11:58:08 measured 18.3
delivered records/s but 14.77 new torrent rows/s. These are the correct units
for future A/B decisions.

## Server state and evidence

All times below are CST on 2026-07-19 unless stated otherwise.

### Singapore crawler (`43.161.252.140`, 2C4G)

- service: `cherry-picker-metadata.service`, 96 DHT identities, same-port
  restart at 12:04:32 after a two-minute failed configuration preflight;
  durable spool preserved the data.
- deployed binary provenance was `c5356f4`, older than the retry-observer
  schema in repository commit `12df154`. Strict JSON config correctly rejected
  the unknown field. Future feature flags require binary/schema preflight.
- before restart: about 124% CPU and 2.49 GiB RSS. At 12:28 after restart:
  about 164% CPU and 1.73 GiB RSS.
- the durable cursor at 12:33 had `next_sequence=447048` and
  `acked_sequence=447017`: only 31 records pending after a transient storage
  timeout. The 38 MB active spool segment is retained history, not a 38 MB
  unacknowledged backlog.

Hourly conversion decay before restart:

| Window | metadata/s | DHT packets/s | request dedupe | TCP dial failure |
|---|---:|---:|---:|---:|
| 06:58 | 17.22 | 14,081 | 57.8% | 82.5% |
| 07:58 | 11.75 | 17,962 | 71.5% | — |
| 08:58 | 7.22 | 14,196 | 72.3% | — |
| 09:58 | 10.21 | 20,700 | 75.3% | — |
| 10:58 | 10.87 | 18,962 | 78.4% | — |
| 11:58 | 11.02 | 21,988 | 78.9% | 85.2% |

DHT traffic increased while conversion fell. CPU/network input is therefore
not the first limiting stage; candidate novelty and failure-state aging are.

The same-port restart initially produced 57.1 metadata/s, then averaged 48.18/s
over the first 8.5 minutes. By 12:28 the latest window was back to 8.33/s and
the last twenty windows averaged 17.37/s. Across the longer paired central
sample, only 45.9% of delivered records were globally new. Automatic restarts
would inflate the crawler-local number and duplicate/storage cost; they are a
diagnostic control, not the intended fix.

### Japan crawler (`43.165.167.154`, 2C4G)

- Tencent Cloud reported the instance running. The TAT terminal was
  unavailable during this audit, so no unverified CPU/RSS claim is made.
- the storage receipt remained live: sequence 779,666 at 12:32:47, proving the
  crawler-to-storage path was delivering.
- an earlier paired 30-second receipt sample measured 8.2 delivered records/s.

### Storage/search server (`43.167.177.68`, 2C4G)

- exact torrents: 841,846 at 12:31.
- Meilisearch outbox: 3,117 rows, oldest 12:27:48 CST and newest 12:31:59 CST.
  It had fallen from about 29.8k after the memory rebalance, so projection
  throughput improved, but freshness still lagged by about four minutes.
- live memory limits were rebalanced from API/PG/Meili 576/1280/1152 MiB to
  768/1152/1344 MiB. At 12:32 usage was approximately API 299 MiB, PG 446 MiB,
  and Meili 1.05 GiB; no cgroup OOM or OOM kill was observed.
- despite that recovery, three current `iostat` samples showed 65.8–79.7%
  iowait. IO PSI full was 66.5% over 10 seconds and memory PSI full was 16.5%.
- Meili had accumulated roughly 78.8 GB read / 693 GB write block IO. The API
  worker submitted 500-document tasks roughly every 15–20 seconds while
  draining backlog. Meilisearch projection write amplification is now the
  primary storage-side bottleneck.
- a crawler-side `/health` call briefly took 6.4 seconds and durable delivery
  hit 10-second timeouts. The spool absorbed the event and the cursor later
  returned to a 31-record backlog; there was no observed data loss.
- PostgreSQL was explicitly verified with `archive_mode=off`; WAL archiving is
  not the production cause.

## Ranked bottlenecks and implementation order

### P0 — make measurements replay-safe and stage-specific

Add durable receipt totals for delivered, accepted, duplicates, new metadata,
policy rows and batches in the same PostgreSQL transaction as the receipt.
Expose these per crawler/epoch and globally. Keep the PostgreSQL catalog
estimate labelled as an estimate. The dashboard must calculate rates only from
persistent counter deltas and server timestamps.

Rollback: API image only; migration is additive and old rows start from an
explicit zero-baseline timestamp.

### P0 — stop near-empty Meili tasks from amplifying writes

Coalesce outbox documents near the head of the queue with a maximum freshness
deadline, while processing full batches without delay. First production target:
2,000 documents, at most 30–60 seconds coalescing. Measure completed docs/s,
task latency, outbox oldest age, Meili block-write delta, IO PSI and API memory.

Rollback conditions: search oldest age exceeds the declared bound plus task
latency for two windows, API exceeds 85% memory, task failures appear, or new
torrent commit latency regresses. Disable coalescing and restore batch 500.

### P1 — expire only failed `(infohash, peer)` attempt reservations

Use a fixed, non-sliding cooldown for request attempts. Do not expire successful
metadata hashes or backend-known hashes. Normalize payloads before inserting in
the success cache, so a malformed peer cannot suppress a later valid response.
Run same-port A/B with 8-minute and then longer cooldowns. Judge central new
metadata/s, not crawler-local `meta_sent` alone.

Rollback: `dedupe.expire_metadata_attempts=false`.

### P1 — separate announce and active-lookup admission

The shared FIFO permits lower-yield active lookup work to displace direct
`announce_peer` candidates. Add bounded source queues with work-conserving
weighted dequeue; start at 75% announce / 25% lookup and expose admitted,
dropped and queue-depth counters by source.

Rollback: `metadata.source_scheduler_enabled=false`.

### P1 — repair crawler durable group commit

The previous wake semantics allowed each concurrent submit to prematurely end
the coalescing timer, producing groups of one or two records and excessive
`fsync`. Keep the oldest-request deadline fixed and flush only on a full group,
timer, shutdown or spool-capacity recovery. Expose group-size and fsync-time
counters before changing the production batch/delay.

### P2 — network and protocol exploration after novelty recovery

Only after the P1 A/B tests improve globally unique commits should lookup rate,
wire workers, DHT identities, port/identity rotation, routing-table diversity,
kernel socket buffers and regional work partitioning be retuned. Current packet
and CPU evidence shows that increasing raw traffic first would mostly increase
drops, failed dials and duplicates.

## Experimental rules

- warm the same port and persisted node IDs for at least 10 minutes unless the
  experiment is explicitly a restart diagnostic.
- use paired 15-minute windows for ordinary changes; use a 5-minute early-stop
  guard for severe regressions and extend noisy results rather than declaring a
  winner.
- record binary commit, config SHA-256, crawler epoch, receipt start/end,
  PostgreSQL exact count delta, Meili outbox depth/oldest age, resource pressure,
  hypothesis and rollback before enabling a flag.
- compare two regions by central new-metadata counters and uniqueness. Local
  `meta_sent`, receipt sequence, estimated catalog size and Heat counters are
  supporting metrics only.
