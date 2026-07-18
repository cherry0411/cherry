# Crawler optimization framework

The optimization target is **globally new metadata per unit time under a fixed
2-vCPU/4-GiB resource envelope**. Raw downloads are diagnostic only: restarting
a process clears its local caches and can make the same metadata look new again.

## Measurement contract

Every run has an immutable directory under `bench/runs/` containing:

- the effective config and its SHA-256;
- binary SHA-256 plus the commit/source/config identifiers printed by the binary;
- warm/cold identity mode, port, node-ID cohort, host/kernel/sysctl snapshot;
- complete crawler log and 30-second CPU/RSS/network/oracle samples;
- per-run kernel UDP receive/send buffer error deltas;
- uniqueness-oracle snapshots at the exact measurement boundaries;
- a normalized `result.json` and an append-only `bench/index.jsonl` record.

The included `benchmark-sink` persists only 21 bytes per processed infohash. It
implements the crawler's batch/check/reject endpoints without PostgreSQL or the
full API, so it is suitable as a global-uniqueness oracle on the 2C4G crawler
host. Its CPU and RSS remain part of the host resource budget. The same endpoint
can later be shared by multiple regions.

Three run modes deliberately answer different questions:

| Mode | Node IDs | Intended use |
|---|---|---|
| `steady` | reused cohort | normal tuning and long-run comparisons |
| `warm-restart` | reused cohort | explicitly quantify process-cache reset boosts |
| `cold` | fresh run-local IDs | quantify bootstrap behavior only |

Ports do not rotate implicitly. Port and node-ID changes are separate variables,
which prevents a “cold identity + new port + new binary” bundle from becoming an
uninterpretable experiment. The manifest also records how many cohort node-ID
files existed before startup, so the first nominally steady run of a new cohort
cannot be mistaken for a genuinely warm restart.

## Run one benchmark

```bash
scripts/run-crawler-benchmark.sh \
  --label baseline-b246e43 \
  --variant A \
  --mode steady \
  --cohort primary \
  --warmup 10m \
  --measure 60m \
  --set discovery.lookup_rate=300
```

`--set PATH=JSON_VALUE` is repeatable. This keeps the framework flexible: a new
hypothesis can be tested without adding a hard-coded matrix to the controller.

For a serious A/B, run sequential ABAB blocks rather than two crawlers at once:

```bash
scripts/run-crawler-abab.sh \
  --label routing-refresh \
  --blocks 6 \
  --warmup 10m \
  --measure 60m \
  --config-a configs/baseline.json --binary-a bin/crawler-a \
  --config-b configs/candidate.json --binary-b bin/crawler-b
```

The first arm is randomized. Both arms share the same identity cohort and
global oracle, while only one process consumes the host/network at a time.

## Iteration policy

Before running, record one causal hypothesis, one primary metric, a minimum
worthwhile effect, and resource guardrails. Short screens can use 10-minute
warmup plus 20–60 minutes of measurement. A candidate is not called a durable
win until it survives at least three paired AB blocks and a 6–12 hour run.

Use these decision rules:

1. Primary: global unique metadata/hour after warmup.
2. Reject if RSS exceeds the memory envelope, metadata pause persists, or UDP
   drops materially rise—even if local download count improves.
3. Treat startup peak, local `wire_ok`, and per-process `meta_sent` only as funnel
   diagnostics.
4. Diagnose long-run decay using the slope of unique/hour against uptime, not a
   peak or a single average.
5. Change one causal mechanism at a time. Parameter bundles are allowed only for
   exploration and must be decomposed before acceptance.
6. Preserve losing results. Never reuse a label directory or overwrite logs.

The current first diagnostics are routing-table turnover (`nodes`, `node_add`,
`node_rm`, `refresh_q`) and pre-dial wire admission loss (`wire_q_drop`). They
distinguish stale discovery from downstream saturation before either behavior is
changed.
