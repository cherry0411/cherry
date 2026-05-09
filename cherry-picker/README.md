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

- `CHERRY_PICKER_ROLE`: `combined` or `discovery`.
- `CHERRY_PICKER_INSTANCE_ID`: logical instance identifier.
- `CHERRY_PICKER_LISTEN_ADDR`: UDP listen address, default `:6881`.
- `CHERRY_PICKER_DHT_MODE`: `crawl` or `standard`, default `crawl`.
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

## Current validation

- `go test ./...` passes.
- HTTP export retries are covered with a local `httptest` server.
- metadata normalization and config-file loading are covered by unit tests.
