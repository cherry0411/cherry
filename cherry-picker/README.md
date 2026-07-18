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
- `CHERRY_PICKER_DEDUPE_PEER_TTL`: local dedupe TTL for repeated peer events.
- `CHERRY_PICKER_DEDUPE_METADATA_TTL`: local dedupe TTL for repeated metadata work.
- `CHERRY_PICKER_PPROF_ADDR`: optional local Go profiling listener, for example `127.0.0.1:6060`.

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
