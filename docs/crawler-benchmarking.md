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
- egress qdisc drop deltas, which expose burst loss below the process socket;
- uniqueness-oracle snapshots at the exact measurement boundaries;
- oracle check-hash/found deltas, exposing how much of each arm's discovery
  stream was already consumed by prior sequential runs;
- a normalized `result.json` and an append-only `bench/index.jsonl` record.

The index record includes SHA-256 and byte size for the effective config, raw
crawler log, host metrics, environment snapshot, and both oracle boundaries.
Small summaries can therefore be copied to source control while large raw
artifacts remain verifiable on the benchmark host.

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

For config-only experiments, both arms may use the same template and binary;
repeatable `--set-a PATH=JSON_VALUE` and `--set-b PATH=JSON_VALUE` overrides
define each treatment without creating throwaway config files.

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

## Mandatory experiment card

No treatment enters the remote queue until its experiment card is appended to
`docs/crawler-experiment-ledger.md`. Failed, neutral, cancelled, and rolled-back
runs are retained just like winners. Each card must contain:

1. **Question and mechanism**: one falsifiable causal hypothesis, why the single
   changed variable should affect the funnel, and the minimum worthwhile effect.
2. **Frozen inputs**: commit/release and binary SHA, config/template/treatment
   SHA, server/region, resource limit, port, node-ID cohort, oracle mode and
   oracle baseline SHA.
3. **Change boundary**: the exact A and B values plus a list of important values
   deliberately held constant. Bundled exploration must say how it will be
   decomposed before promotion.
4. **Timing and contamination**: start/end UTC, process-restart boundary, the
   excluded restart window, warm-up rule, measurement window, block order, and
   whether the port and node IDs were reused.
5. **Decision contract**: primary metric, diagnostic funnel counters, resource
   guardrails, stop conditions, and evidence that would falsify the hypothesis.
6. **Evidence**: immutable run paths, index/result hashes, every 30-second raw
   window (or a checked-in digest pointing to it), result summary, caveats, and
   the next experiment derived from the result.
7. **Rollback**: exact previous release/config, rollback triggers, commands or
   controller action, and post-rollback health checks. A rollback is a recorded
   outcome, not an erased run.

Use this compact ledger template:

```text
### <experiment-id> — <state: planned/running/decided/rolled-back>
Hypothesis / mechanism:
A / B (only changed variable):
Frozen inputs and controls:
Warm-up / measurement / order:
Primary metric / guardrails / falsification:
Immutable evidence paths:
Result and reasoning:
Rollback point / triggers / verification:
Next action:
```

Chat summaries are not authoritative. The immutable host artifacts and this
ledger are the recovery point after a pause, context compaction, or hand-off.

## Iteration policy

Before running, record one causal hypothesis, one primary metric, a minimum
worthwhile effect, and resource guardrails. A restarted steady block uses the
same port and persistent node-ID cohort. Its first two minutes are always tagged
`restart-contaminated` and excluded from the primary comparison because delayed
UDP responses, empty process caches, and re-created transaction IDs can bias the
early rate.

Warm-up is bounded and adaptive rather than guessed from a single peak:

- minimum 5 minutes, default 10 minutes, maximum 15 minutes after restart;
- inspect three consecutive 30-second windows of peer supply, dial/connect
  conversion, blacklist growth, RSS and global-new rate;
- warm-up may end after the minimum only when those windows have no health-gate
  failure and no obvious one-way bootstrap trend;
- if the stream is still non-stationary at the maximum, start measurement but
  mark the block non-stationary and retain its full slope. Never wait until a
  declining treatment happens to look flat.

A directional mechanism screen then measures at least 5 minutes. A balanced
`ABBA` screen therefore normally costs 40–80 minutes, not four five-minute cold
runs. A candidate that wins the short screen gets a 20–30 minute confirmation,
then becomes only the next baseline and remains open to every unexhausted
optimization direction. A 6–12 hour soak is never started automatically: it is
reserved for the user-authorized stability phase after the global search space
and remaining expected gains have been reported. The soak validates decay,
capacity and stability—it is not the search loop.

Use these decision rules:

1. Primary: global unique metadata/hour after warmup.
2. Reject if RSS exceeds the memory envelope, metadata pause persists, or UDP
   drops materially rise—even if local download count improves.
3. Treat startup peak, local `wire_ok`, and per-process `meta_sent` only as funnel
   diagnostics.
   - The `peer_source_funnel` section of `result.json` splits the
     dial→connect→download funnel by peer source. `announce_*` counters come from
     `announce_peer` senders (they just contacted this node, so their NAT pinhole
     is open and they are provably alive right now); `getpeers_*` counters come
     from third-party `get_peers` "values" (reported by another node, possibly
     stale or dead). `announce_connect_rate_advantage` is
     `announce_connect_rate / getpeers_connect_rate`.
   - Decision rule for prioritizing announce-sourced peers (hypothesis B8): an
     advantage materially above 1 means announce peers convert dials into
     connections better, so preferring them should lift the connect-rate half of
     the observed sustained-rate decay. This is a non-depleting mechanical
     diagnostic and may be screened on short runs; it does not by itself
     constitute a durable win. These counters are zero for legacy logs produced
     before the instrumentation existed, so old runs remain analyzable.
   - `peer_source_funnel` also splits the **pre-dial** stage per source:
     `*_queued` → (`*_blacklisted` | `*_inflight_deduped` | `*_dial`).
     `*_blacklisted_rate` and `*_inflight_rate` are fractions of queued. A funnel
     that starves with a low dial rate but a high blacklisted or inflight share
     means supply is being *discarded* (bad-peer bans or same-(hash,peer) already
     downloading), not that peers are merely unreachable — a different fix than a
     conversion problem.
   - The `blacklist_health` section reports `size`/`max` (gauges, last observed),
     `fill_ratio`, and accumulated `insert_rejected`/`expired_evicted`. The wire
     blacklist silently no-ops inserts once `size >= max` (default 131072), so a
     `fill_ratio` near 1 with rising `insert_rejected` is a blind spot: bad peers
     stop being banned and keep consuming dial workers, depressing the connect
     rate independently of peer supply. Treat a saturated blacklist as a
     diagnostic finding, not a healthy steady state.
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

Balanced order removes the first-order advantage of running earlier, but it
does not make a shared oracle non-depleting: `/check` responses also change the
crawler's work, so later blocks face a different admission stream. Use
`run-crawler-abab.sh --oracle-mode isolated` for confirmation runs. The
controller stops and flushes the managed production sink, freezes one immutable
baseline, and starts every block with a fresh writable overlay. Every arm then
sees the same pre-existing hashes, while hashes discovered during its own
warmup/measurement remain deduplicated normally.

Isolation never merges results by default. Each experiment has a manifest,
baseline digest, per-block overlay digests, controller logs, and a companion
manifest digest under `bench/oracle-experiments/`. Add `--finalize-oracle` only
after deciding that every completed block should enter production. Finalize
validates all 21-byte records, merges metadata before rejections, builds and
fsyncs a temporary production file, then replaces production. Source overlays
remain preserved. If the production digest changes during the experiment,
finalization refuses to run. Shared mode remains the default for compatibility;
its short screens are evidence about direction and mechanical efficiency, not
an unbiased estimate of the durable global effect.

Monitor gaps are written as missing values rather than zero. The analyzer
rejects missing or non-monotonic samples and averages the next delta across the
full time gap, preventing a failed `/stats` request from becoming a fake rate
spike. Its short-window slope is exposed as `transient_slope`; the old
`decay_slope` key remains as a compatibility alias.

### P0 decay observability (O1/O2/O3)

The optimization build after `80591c9` adds observation only; it does not
change cache admission/eviction order, DHT queries, blacklist policy, queue
capacity, worker targets, or download timeouts. It exists to select the next
single-variable experiment from evidence rather than another broad parameter
bundle.

- **O1 — five sharded LRUs.** Every 30 seconds the runtime line reports
  `lru_{ih,peer,mreq,mres,remote}_{len,cap,oldest_s,hit,miss,insert,evict,del_miss}`.
  `len`, `cap`, and `oldest_s` are gauges; other values are window deltas.
  A hit/miss is recorded for `Set`, `Contains`, and `ContainsAndTouch`; `insert`
  means a previously absent key was admitted, `evict` means capacity removed
  the shard tail, and `del_miss` means cleanup tried to remove an absent key.
  `oldest_s` is the maximum coarse last-touch age among the 64 shard tails;
  because every shard tail is its least recently used entry, it remains useful
  under steady churn. Clock precision is one 30-second interval. Collection
  visits only those tails and never scans all entries. A coarse `uint32`
  timestamp increases the aligned entry object from 16 to 24 bytes: about
  8 bytes per resident entry (roughly 13 MiB at 1.6 million residents, 4.2% of
  the existing 192-byte/entry planning estimate). The measured Windows/amd64
  hot-hit path is 0 allocations and roughly 26–46 ns/op, and a full 64-shard
  snapshot is 0 allocations and roughly 0.32–0.41 microseconds.
  `result.json.lru_health` exposes last fill/age gauges plus hit and eviction
  ratios.
- **O2 — wire pressure.** `wire_active` is the autotuner admission ceiling,
  `wire_max` its configured ceiling, and `wire_busy` the instantaneous number
  executing a request. `wire_req_depth/cap` and `wire_resp_depth/cap` locate
  request versus consumer backpressure. Existing `wire_q_drop`,
  `wire_dial_fail`, `wire_hs_fail`, and `wire_dl_fail` remain window counters.
  `result.json.wire_pressure` combines these without changing older fields.
- **O3 — DHT UDP blacklists.** `dht_bl_size/max` sums gauges across every DHT
  identity (96 in the 2C4G profile); `dht_bl_reject/expired` are aggregate
  window deltas. They are intentionally separate from `bl_*`, which describes
  the one peer-wire TCP blacklist. Collection locks each map only long enough
  to read its length and never scans its entries. The analyzer writes the
  additive `dht_blacklist_health` section.

Old runtime lines remain readable because the parser already accepts arbitrary
`key=value` tokens and every new reducer treats missing keys as `None`/zero.
Rollback is a release switch to the preceding immutable binary: no state or
file-format migration exists. Remove the `lastTouched`/counter fields and the
appended runtime tokens only if the measured CPU/RSS guardrail regresses; keep
the parser additions because they are backward compatible and preserve already
captured runs. After rollback, verify the old binary SHA and confirm that the
existing `runtime 30s:` rows and `result.json` generation continue normally.

The current first diagnostics are routing-table turnover (`nodes`, `node_add`,
`node_rm`, `refresh_q`) and pre-dial wire admission loss (`wire_q_drop`). They
distinguish stale discovery from downstream saturation before either behavior is
changed.
