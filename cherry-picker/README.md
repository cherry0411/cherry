# cherry-picker

`cherry-picker` is the first Go MVP for a high-throughput BitTorrent DHT crawler.

## Current MVP scope

- Self-contained crawl-mode DHT node implemented inside `internal/dht`.
- Optional metadata fetch pipeline using the existing peer wire implementation.
- Batched exporters for `stdout`, local JSONL file, or HTTP bulk POST.
- Non-blocking event submission so exporter backpressure does not stall the announce hot path.
- Periodic worker stats events for basic observability.
- Pooled UDP packet buffers so the receive path does not reuse mutable packet memory across queued packets.

## Project layout

- `cmd/cherry-picker`: process entrypoint.
- `internal/config`: environment-based runtime config.
- `internal/app`: combined-mode application wiring.
- `internal/export`: batch exporter and sink adapters.
- `internal/pipeline`: normalized event schema.

## Run modes

- `combined`: DHT crawl + peer event export + metadata fetch.
- `discovery`: DHT crawl + peer event export only.
- `metadata`: DHT crawl + metadata fetch export only.

Role defaults are normalized in code. `discovery` forces metadata fetch off, `metadata` forces peer event export off, and `combined` enables both.

## Config files

Set `CHERRY_PICKER_CONFIG` to a JSON file to run with a checked-in config instead of a long environment variable list.

- `configs/discovery.json`: discovery-only role tuned for peer export.
- `configs/metadata.json`: metadata-only role tuned for metadata export.

Durations use Go duration strings such as `2s`, `30s`, or `10m`.

## Environment variables

- `CHERRY_PICKER_ROLE`: `combined`, `discovery`, or `metadata`.
- `CHERRY_PICKER_INSTANCE_ID`: logical instance identifier.
- `CHERRY_PICKER_LISTEN_ADDR`: UDP listen address, default `:6881`.
- `CHERRY_PICKER_DHT_INSTANCES`: number of consecutive UDP listeners starting at `CHERRY_PICKER_LISTEN_ADDR`; explicit `CHERRY_PICKER_LISTEN_ADDRS` takes precedence.
- `CHERRY_PICKER_DHT_PRIME_NODES`: comma-separated bootstrap endpoints; useful when proxy fake-IP DNS breaks UDP bootstrap.
- `CHERRY_PICKER_DHT_MODE`: `crawl` or `standard`, default `crawl`.
- `CHERRY_PICKER_DHT_ACTIVE_LOOKUP`: actively query the routing table for hashes observed in inbound `get_peers` requests.
- `CHERRY_PICKER_DHT_LOOKUP_NODES`: closest nodes queried per active lookup.
- `CHERRY_PICKER_DHT_LOOKUP_DHTS`: identities used for each observed hash.
- `CHERRY_PICKER_DHT_LOOKUP_QUEUE`: backlog for newly observed live hashes.
- `CHERRY_PICKER_DHT_LOOKUP_RATE`: maximum hashes actively queried per second.
- `CHERRY_PICKER_DHT_LOOKUP_WORKERS`: workers sharing the global lookup rate.
- `CHERRY_PICKER_DHT_LOOKUP_FOLLOWUPS`: bounded iterative depth, from `0` to `8`.
- `CHERRY_PICKER_DHT_LOOKUP_SPREAD`: diversify bounded chains across returned nodes.
- `CHERRY_PICKER_DHT_SAMPLE_INFOHASHES`: enable low-priority BEP 51 sampling.
- `CHERRY_PICKER_DHT_SAMPLE_RATE`: maximum BEP 51 sample queries per second.
- `CHERRY_PICKER_EMIT_PEER_EVENTS`: `true` or `false`.
- `CHERRY_PICKER_EVENT_QUEUE`: in-memory exporter queue size.
- `CHERRY_PICKER_METADATA_ENABLED`: enable metadata fetch workers.
- `CHERRY_PICKER_METADATA_BLACKLIST`: peer wire blacklist size.
- `CHERRY_PICKER_METADATA_REQUEST_QUEUE`: metadata request queue size.
- `CHERRY_PICKER_METADATA_WORKERS`: metadata worker concurrency.
- `CHERRY_PICKER_EXPORTER`: `stdout`, `file`, or `http`.
- `CHERRY_PICKER_EXPORTER_FILE`: target file for the `file` exporter.
- `CHERRY_PICKER_EXPORTER_URL`: target endpoint for the `http` exporter.
- `CHERRY_PICKER_EXPORTER_BATCH`: batch size.
- `CHERRY_PICKER_EXPORTER_FLUSH`: flush interval like `2s`.
- `CHERRY_PICKER_EXPORTER_TIMEOUT`: HTTP exporter timeout.
- `CHERRY_PICKER_EXPORTER_HTTP_RETRIES`: HTTP batch retry count.
- `CHERRY_PICKER_EXPORTER_RETRY_BACKOFF`: per-attempt retry backoff base duration.
- `CHERRY_PICKER_CRAWLER_ID`: stable durable receipt identity; required with a spool and preserved across process restarts.
- `CHERRY_PICKER_SPOOL_DIR`: enables the typed pre-send durable spool for the HTTP exporter.
- `CHERRY_PICKER_SPOOL_MAX_BYTES`: local spool disk bound; defaults to 4 GiB when durable mode is enabled.
- `CHERRY_API_KEY`: required by the durable HTTP exporter and sent as `X-API-Key`.
- `CHERRY_PICKER_DEDUPE_PEER_TTL` and `CHERRY_PICKER_DEDUPE_METADATA_TTL`:
  reserved policy inputs. The current hot-path caches are capacity-bounded LRUs,
  not TTL caches; experiments must not claim a TTL treatment until the runtime
  metrics and expiry implementation are wired.
- `CHERRY_PICKER_PPROF_ADDR`: optional local Go profiling listener, for example `127.0.0.1:6060`.

### Inbound `get_peers` heat

Heat export is a separate, default-off channel for daily popularity evidence.
It observes only validated external inbound `get_peers` requests; metadata
responses, announces, sampling and active outbound lookups never feed it.
Private, local, reserved and configured crawler addresses are rejected. Public
IPv4 addresses retain the complete address for pseudonymization; public IPv6
addresses are reduced to `/64`. The source address is immediately converted to
a UTC-day-scoped 64-bit HMAC actor and is never written to disk or exported.
Ports, node IDs and regions are not collected.

- `CHERRY_PICKER_HEAT_ENABLED`: enable the channel; default `false`.
- `CHERRY_PICKER_HEAT_ENDPOINT`: HTTPS CHHT v1 ingestion endpoint. Plain HTTP is accepted only for loopback tests.
- `CHERRY_PICKER_HEAT_CRAWLER_ID`: stable 1-64 byte receipt identity using ASCII letters, digits, `.`, `_` or `-`.
- `CHERRY_PICKER_HEAT_MASTER_SECRET_FILE`: shared actor-pseudonym master secret file.
- `CHERRY_PICKER_HEAT_HMAC_SECRET_FILE`: raw CHHT HMAC signing secret file.
- `CHERRY_PICKER_HEAT_SPOOL_DIR`: dedicated segmented durable spool directory.
- `CHERRY_PICKER_HEAT_SPOOL_MAX_BYTES`: hard bound on current spool disk usage; default 512 MiB.
- `CHERRY_PICKER_HEAT_KNOWN_CRAWLERS`: comma-separated IPs/CIDRs excluded from heat.
- `CHERRY_PICKER_HEAT_QUEUE`, `CHERRY_PICKER_HEAT_BATCH`: bounded admission queue and delivery batch sizes.
- `CHERRY_PICKER_HEAT_FLUSH`, `CHERRY_PICKER_HEAT_HTTP_TIMEOUT`, `CHERRY_PICKER_HEAT_RETRY_BACKOFF`: Go duration values for batching and delivery.

Both secret files must be regular files containing at least 32 raw bytes and
must be mode `0600` on production Linux. One final CRLF/LF written by an editor
is removed; all other bytes are significant. `HMACSecretFile` contains the
original raw secret bytes. In production, backend
`Heat__CrawlerSecrets__{crawler-id}` is the Base64 encoding of that crawler's
raw transport key; every expected crawler must have a different key. The
legacy single `Heat__SharedSecret` remains only for tests/migration and does not
satisfy production startup validation. The Base64 text itself is not the HMAC
key. Transport keys only derive `X-CHHT-Signature` and are never sent. The actor
master secret is shared across regions so actor-day identities deduplicate, but
it is separate from every transport key and never reaches storage.

The compact body starts with `CHHT`, version 1 and the UTC day, followed by
strictly sorted raw 20-byte infohash groups and sorted unique big-endian
64-bit actors. Delivery sends decimal `X-CHHT-Epoch`, `X-CHHT-Sequence` and
`X-CHHT-End-Sequence`, plus lowercase SHA-256 and HMAC headers. The HMAC input
is exactly `CHHT/1\n{crawler}\n{epoch}\n{start}\n{end}\n{payloadSha256}\n`
followed by the raw request body. The interoperable fixed vector is
`internal/heat/testdata/chht_v1_golden.json`.

At a proven UTC boundary the crawler also sends an empty-body
`POST /api/v1/heat/completions`. Its signature input is exactly
`CHHT-COMPLETE/1\n{crawler}\n{day}\n{epoch}\n{start}\n{next}\n1\n`.
`start` is the day's first spool sequence and `next` is its exclusive durable
end. The backend stores the completion in the same per-day SQLite transaction
stream and accepts it only when the crawler's single-epoch receipts form an
exact contiguous chain from `start` to `next` (or `start == next` for an empty
day). An identical replay is idempotent; a conflict or any later new batch is
rejected.

Admission is non-blocking. A full memory queue increments `QueueDropped`; a
full/unwritable spool increments retry/failure metrics and never silently
claims durability. Once a batch has crossed `fsync`, endpoint errors and lost
responses replay exactly the same body and receipt. The cursor advances only
for an HTTP 200 JSON receipt whose crawler/day/epoch/range/digest match and
whose `nextSequence` is exactly `endSequence + 1`; an arbitrary 2xx, malformed
body or mismatched proxy response is retried without deletion. A strictly
authenticated HTTP 410 with `code=day_closed` and the same complete receipt
identity is an explicit durable negative receipt: the crawler advances so an
irrecoverably closed old day does not block current data, increments
`ClosedDayRejectedRecords/Batches`, and logs that coverage is partial. It is
never counted as exported or as a successful ACK. PostgreSQL's partial day
manifest is the durable fact; these crawler counters are diagnostic and exact
rejected-batch counts are not preserved over a restart. Acknowledged sealed
segments are reclaimed during continuous production. Metadata delivery, its
spool, and the experiment oracle remain independent. The crash/directory-sync
guarantees target Linux; Windows support is for development and cannot provide
the same power-loss directory semantics.

The crawler's `heat.completion.json` is a small fsync/rename ledger bound to the
spool epoch. A writer barrier prevents a new UTC day from overtaking earlier
queue entries. Queue overflow, forced writer loss, closed-day negative receipt,
clock jump, first partial startup day, abnormal restart, and even a graceful
same-day stop/restart poison that day. Dirty days never send a completion.
Therefore restart recovery may conservatively report `partial`, but cannot
turn locally unpersisted observations into `complete`.

## Example

```powershell
$env:CHERRY_PICKER_CONFIG = "configs/discovery.json"
go run ./cmd/cherry-picker
```

## systemd deployment

Sample unit files live in `deploy/systemd`.

- `deploy/systemd/cherry-picker-discovery.service`
- `deploy/systemd/cherry-picker-metadata.service`

Both units expect:

- the binary at `/opt/cherry-picker/cherry-picker`
- configs under `/etc/cherry-picker/*.json`
- runtime user/group `cherry-picker`

Adjust those paths if your deployment layout differs.

## Throughput notes

- Hot-path dedupe now uses a sharded LRU to avoid a single global mutex under heavy announce/get_peers fan-in.
- Remote existence checks are batched in parallel workers, so metadata enqueue no longer waits behind one slow `/check` loop.
- Checked-in configs are intentionally aggressive. On a small host, prefer overriding `CHERRY_PICKER_DHT_PACKET_WORKERS`, `CHERRY_PICKER_METADATA_WORKERS`, and `CHERRY_PICKER_EVENT_QUEUE` downward.

## Durable zero-raw delivery

For production HTTP delivery, set a stable crawler ID, spool directory, API key
and the normal batch endpoint. When the configured endpoint ends in
`/api/v1/torrents/batch`, durable mode upgrades it to
`/api/v1/torrents/batch/durable`; an explicitly supplied durable/custom endpoint
is left unchanged.

Only the versioned `normalized`, `summary`, `hash_only`, or `reject` record is
written to the spool. Record schema v2 and durable envelope schema v2 omit
region, policy identity, piece length, and free-text decision reasons.
`normalized`/`summary` retain only bounded zero-raw details; `hash_only` and
`reject` are bodyless and use the closed numeric `decision_code` set shared
with the backend (1 through 5). Raw bencode, `pieces`, and piece hashes are
never written to the spool or HTTP body. A metadata hash becomes locally known
only after its record has crossed the group-fsync boundary. The sender keeps
one stable batch in flight and advances the local cursor only after an
identity-, checksum-, range-, count-, and commit-matching ACK.

Record v1 is intentionally not auto-migrated: durable production had not been
deployed when v2 was introduced, and rewriting an unacknowledged log in place
would make its receipt/checksum history ambiguous. Opening a directory that
still contains v1 frames fails with `spool: incompatible record schema` and
does not modify or delete the directory. Stop the crawler, archive the complete
old spool directory for audit, then point `CHERRY_PICKER_SPOOL_DIR` at a new
empty directory. Do not edit the cursor or segment files by hand. There is no
lossless v1-to-v2 exporter because deleted free-text policy fields have no
durable meaning in v2.

## 2-core / 4-GB profiles

The root `docker-compose.yml` contains the common 2-core/4-GB limits:

- `GOMAXPROCS=2`, a 3-GB Go soft memory limit, and a 3.5-GB container hard limit.
- One UDP reader and one packet worker per identity, a 512-packet queue, and 1,000 routing-table nodes per identity.
- Bounded active lookups (32 closest nodes, at most 100 observed hashes/second).

Docker Desktop with a TUN proxy is intentionally defaulted to two identities:

```powershell
docker compose up -d
```

On the tested Windows host, one and two identities remained manageable, while four or more caused the Docker Engine API to time out. For maximum throughput, run the crawler natively on Windows or on a public Linux host. Native Windows testing reached its throughput knee at 96 identities (128 added no packet throughput) and can be started with:

```powershell
$env:GOMAXPROCS = "2"
$env:CHERRY_PICKER_MEM_LIMIT_MB = "3072"
$env:CHERRY_PICKER_LISTEN_ADDR = ":20003"
$env:CHERRY_PICKER_DHT_INSTANCES = "96"
go run ./cmd/cherry-picker
```

On a public 2-core/4-GB Linux server, the measured native profile uses 96
identities and two strictly-progressing iterative chains per live hash. Use
`configs/metadata-2c4g.json` as the starting point, and keep BEP 51 sampling
disabled when the live-hash queue is continuously busy:

```bash
GOMAXPROCS=2 \
CHERRY_PICKER_MEM_LIMIT_MB=3072 \
CHERRY_PICKER_CONFIG=configs/metadata-2c4g.json \
./cherry-picker
```

Apply the root `scripts/tune-crawler-os.sh` on native Linux. Its bounded socket
buffers and descriptor limits are sized for 96 listeners on 4 GB; the legacy
multi-million-entry conntrack and 128 MB-per-socket settings were intentionally
removed.

Before a full deployment, validate the current network path with a known public infohash:

```powershell
go run ./cmd/metadata-probe -hash <40-hex-infohash> -prime-nodes "<ip:port,...>" -timeout 90s
```

`dht_recv`/`dht_handled` greater than zero confirms BEP 5 UDP reachability. A nonzero `connected` count with `handshake=0` means TCP is being accepted but the BitTorrent extension handshake is blocked or intercepted upstream; tuning worker counts cannot repair that network path.

## Current validation

- `go test ./...` passes.
- HTTP export retries are covered with a local `httptest` server.
- metadata normalization and config-file loading are covered by unit tests.
