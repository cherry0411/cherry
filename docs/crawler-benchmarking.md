# Crawler optimization framework

The optimization target is **globally new metadata per unit time under a fixed
2-vCPU/4-GiB resource envelope**. Raw downloads are diagnostic only: restarting
a process clears its local caches and can make the same metadata look new again.

## Measurement contract

Every run has an immutable directory under `bench/runs/` containing:

- the effective config and its SHA-256;
- the tracked template SHA and a treatment SHA derived only from the template
  plus sorted experimental overrides (excluding run-specific IDs/paths);
- binary SHA-256 plus the commit/source/config identifiers printed by the binary;
- warm/cold identity mode, port, node-ID cohort, host/kernel/sysctl snapshot;
- complete crawler log and 30-second CPU/RSS/network/oracle samples;
- per-run kernel UDP receive/send buffer error deltas;
- uniqueness-oracle snapshots at the exact measurement boundaries;
- a normalized `result.json` and an append-only `bench/index.jsonl` record.

The controller re-executes an immutable temporary snapshot of itself. Updating
the deployed script while a long run is active therefore cannot alter its
second half or invalidate its result.

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
  --blocks 12 \
  --warmup 10m \
  --measure 60m \
  --config-a configs/baseline.json --binary-a bin/crawler-a \
  --config-b configs/candidate.json --binary-b bin/crawler-b
```

The first adjacent pair is randomized, then `AB` and `BA` pair orders alternate
to balance time/oracle depletion. `--blocks` counts runs, so 12 blocks produce
six treatment pairs. Both arms share the same identity cohort and global oracle,
while only one process consumes the host/network at a time. Before final
confirmation, run `--design aa --experiment routing-refresh` (the label may be
different) to add same-arm time/depletion controls to the same experiment.

Summarize completed blocks (and exclude the first cold cohort run) with:

```bash
scripts/benchmark/compare_benchmarks.py \
  --index /home/ubuntu/cherry/bench/index.jsonl \
  --experiment routing-refresh \
  --warm-only
```

The comparator forms strict adjacent, non-overlapping blocks. It keeps both
`A→B` and `B→A` orders and always reports the effect as `B−A`, so randomizing
the first arm is not accidentally undone during analysis. Same-arm `A→A` or
`B→B` blocks are negative controls for time drift and depletion of the shared
global-uniqueness oracle. Legacy manifests remain readable but produce an
explicit warning when they lack treatment/template hashes.

Each run first passes health gates for runtime-window coverage, RSS, kernel UDP
drops, and oracle sample continuity. The comparison reports absolute deltas,
log-ratios, order-stratified effects, an exact sign test, and a deterministic
paired-bootstrap confidence interval. A candidate is never called durable
without at least six valid treatment pairs, five positive pairs, three valid
negative-control pairs, a confidence interval above zero, and a measured effect
beyond same-arm control noise. Shorter runs are directional screens only.

## Iteration policy

Before running, record one causal hypothesis, one primary metric, a minimum
worthwhile effect, and resource guardrails. Short screens can use a 5–10 minute
warmup plus 20–60 minutes of measurement and should emphasize non-depleting
mechanical funnel metrics. A candidate is not called a durable win until it
survives the comparator's controlled blocks and a 6–12 hour run.

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

Because the persistent oracle is deliberately never reset, the earlier run in
a sequential block consumes easy hashes before the later run. Always randomize
and retain both orders, and calibrate the current time-drift/depletion bias with
same-arm negative-control blocks. A global-unique result that disagrees with
local mechanical efficiency is treated as a depletion warning, not a win.

Monitor gaps are written as missing values rather than zero. The analyzer
rejects missing or non-monotonic samples and averages the next delta across the
full time gap, preventing a failed `/stats` request from becoming a fake rate
spike. Its short-window slope is exposed as `transient_slope`; the old
`decay_slope` key remains as a compatibility alias.

The current first diagnostics are routing-table turnover (`nodes`, `node_add`,
`node_rm`, `refresh_q`) and pre-dial wire admission loss (`wire_q_drop`). They
distinguish stale discovery from downstream saturation before either behavior is
changed.
