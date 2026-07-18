# Crawler experiment ledger

This is the human-readable decision log. The authoritative machine records are
the immutable run directories and append-only `bench/index.jsonl` on the
benchmark host; new controller records also contain SHA-256 hashes for their raw
artifacts. Times below are UTC on 2026-07-18.

## Resource envelope and objective

- Host: 2 vCPU, 4 GiB RAM, 200 Mbit/s peak network.
- Primary metric: globally new metadata per second from the persistent oracle.
- Guardrails: no OOM/pause, no kernel UDP buffer loss, no wire queue loss, and
  no material egress-qdisc loss regression.
- Restarted blocks exclude the first 2m, use a bounded 5–15m adaptive warm-up
  (10m default), then at least 5m measurement. These are directional screens,
  not durable claims.

## Active experiment cards

### `dual-region-ac7a239-short-A` — truncated calibration

- **Hypothesis / mechanism:** establish a time-aligned SG/JP steady reference
  and measure startup-to-steady funnel decay before selecting the next single
  variable. This is a calibration A, not an optimization claim.
- **A / B:** A is immutable release `20260718T075740Z-ac7a239ee04d`,
  `refresh_nodes=32`, lookup rate unchanged. No B is running yet.
- **Frozen inputs and controls:** 2C4G hosts SG `43.161.252.140` and JP
  `43.165.167.154`; binary SHA prefix `5285bf14754c`; frozen oracle SHA prefix
  `afb79bd40e8d`; fixed UDP port `21000`; persistent cohorts
  `region-sg-v1`/`region-jp-v1`, 96 existing node IDs each. Both regions started
  at `2026-07-18T09:16:37Z`.
- **Warm-up / measurement:** execution was stopped at 300s while the timing rule
  was being corrected. Tag 0–120s as restart-contaminated, 150–270s as warm-up
  transition and 300s as observation start. There is no 600–900s primary A
  window, so this run cannot support a treatment conclusion.
- **Primary / guardrails / falsification:** oracle global-new metadata/minute;
  secondary lookup→peer→queued→dial→connect→download by source, blacklist
  size/rejected/expired and uptime slope. Reject any treatment with pause/OOM,
  DHT or wire-queue loss, or material qdisc regression.
- **Immutable evidence:** SG
  `/home/ubuntu/cherry/bench/runs/20260718T091637Z_dual-region-long-ac7a239-sg_region-sg-v1_5285bf14754c`;
  JP
  `/home/ubuntu/cherry/bench/runs/20260718T091637Z_dual-region-long-ac7a239-jp_region-jp-v1_5285bf14754c`.
  The historical directory label says `long`; the ledger records that execution
  was deliberately truncated to a short calibration. SG artifact SHA-256:
  manifest `b41576c15597`, crawler log `1e2015f62119`, host metrics
  `fbb81258cb6e`, config `e688b4d5f518`; JP: manifest `7613a82b7521`, crawler
  log `d5dea6b5c766`, host metrics `f313da612bf4`, config `5809307ccb72`.
  The controller stopped during warm-up, so neither run has `result.json` or an
  `index.jsonl` entry; this limitation is explicit rather than silently filled.
- **Initial observations:** first minute SG/JP `+283/+219`; 60–120s
  `+427/+367`; at 120s CPU `46%/39%`, RSS `661/748 MiB`, blacklist fill
  `16.9%/14.0%`, with no DHT, wire-queue or blacklist-reject loss. These startup
  values are diagnostics and are not the primary A estimate.
- **Rollback:** no code/config change was deployed. Stop the benchmark-owned
  controller, crawler and sink; verify UDP `:21000` and loopback oracle `:5070`
  are no longer owned by the run. Previous immutable release remains
  `20260718T075740Z-ac7a239ee04d`.
- **Next action:** use the complete 15m funnel to choose one treatment. Current
  candidates are lookup rate `300→600`, blacklist policy only if capacity or
  rejection is actually observed, and randomized crawl transaction-ID origin
  to eliminate same-port delayed-response collisions.

The 30s funnel shows peer supply and blacklist growth accelerating through
300s. At 300s blacklist size was SG/JP `94,385/89,880` of `131,072`, with no
rejected insert or expiry; the instantaneous growth projected saturation near
367/375s. Therefore the old “160s saturation” estimate was a hot-supply case,
not a fixed property, and this truncated run cannot accept or reject a
blacklist treatment. Review also falsified transaction-ID reset as a likely
startup-peak cause: delayed replies carry IDs near the old counter tail and do
not normally collide with a new counter beginning at one before they expire.

### `lookup-rate-300-vs-600-screen1` — planned

- **Hypothesis / mechanism:** with `refresh_nodes=32`, spare CPU/network may turn
  twice as many admitted active lookup hashes into more reachable peers and
  globally new metadata. If lookup exposure rises but downstream supply does
  not, the experiment instead locates the next bottleneck.
- **A / B (only changed variable):** A `lookup_rate=300`; B
  `lookup_rate=600`. `refresh_nodes=32` and all lookup nodes/DHTs/followups,
  instance/worker counts and autotune values remain fixed. Both use immutable
  release `20260718T075740Z-ac7a239ee04d` and binary SHA prefix
  `5285bf14754c`.
- **Timing / order:** both regions start time-aligned, fixed UDP `:21000`, steady
  96-ID cohorts. SG order `ABBA`; JP `BAAB`. Each block is 10m warm-up + 5m
  measurement and must contain exactly ten event-time-aligned 30s measurement
  windows. The first 2m after each restart is explicitly contaminated. Use an
  isolated frozen oracle baseline and a private overlay per block; never
  finalize the production oracle during the experiment.
- **Pairing:** compare only adjacent blocks within a host: SG `AB`,`BA`; JP
  `BA`,`AB`. Region is a stratum; do not subtract SG from JP or pool their
  absolute rates. Report all four B−A deltas and log-ratios.
- **Exposure / funnel:** B active lookup/s must be 1.75–2.15× A while refresh
  queries stay within ±10%. Follow
  `lookup_sent → get_peers queued → dial → connect → download → oracle new` and
  retain announce/get_peers, blacklist and uptime slopes.
- **Hard stop:** crash/OOM, metadata pause, incomplete ten-window measurement,
  oracle continuity below 90%, any kernel UDP buffer/DHT/wire-queue loss, RSS
  above 3.4 GiB, CPU sustained above 190% without throughput gain, or B qdisc
  drop rate exceeding paired A by 200/min. Infrastructure failures are invalid
  runs and never cause the order to be silently shifted.
- **Early stop after first cross-region pair:** stop on any hard gate; stop if
  both B arms have global-new ≤−5% with no download gain; stop if both B arms
  fail 1.5× lookup exposure. Otherwise complete both reversed pairs.
- **Directional retention rule (not a final/stability claim):** at least three
  of four host-local pairs positive, positive median in each region, stratified
  median gain ≥8% relative or ≥2 metadata/s absolute, download rate aligned and
  all resource/drop gates satisfied. A ≤3% combined gain is not worth doubled
  lookup traffic. Regionally divergent results become a regional hypothesis,
  never a global promotion.
- **Rollback:** stop only experiment-owned PIDs, verify `:21000`/`:5070`, retain
  all runs/overlays/index records without finalizing or deleting them. Restore
  A using the same port/cohort and immutable release; never rotate identity as
  part of rollback.

## Completed screens

| Run | Treatment | Identity | Global new/s | Local metadata/s | CPU mean | RSS peak | Decision |
|---|---|---:|---:|---:|---:|---:|---|
| `20260718T022640Z_baseline-c1eca01_A_8919e8b2d064` | refresh 256 | first/cold cohort | 14.597 | 14.589 | 81.6% | 1776 MiB | reconstructed boundary; calibration only |
| `20260718T025200Z_baseline-c1eca01-a2_A_8919e8b2d064` | refresh 256 | warm 96 IDs | 11.886 | 12.548 | 101.1% | 2044 MiB | warm baseline A2 |
| `20260718T031710Z_refresh32-c1eca01-b2_B_8919e8b2d064` | refresh 32 | warm 96 IDs | 39.208 | 44.091 | 109.9% | 1894 MiB | directional win; requires balanced confirmation |
| `20260718T034222Z_baseline-c1eca01-a3_A_8919e8b2d064` | refresh 256 | warm 96 IDs | 6.918 | 9.175 | 115.2% | 1982 MiB | late baseline A3 |
| `20260718T040733Z_refresh32-c1eca01-b3_B_8919e8b2d064` | refresh 32 | warm 96 IDs | 27.738 | 35.309 | 118.5% | 1980 MiB | repeated directional win; paused afterward |

For A2→B2, refresh queries fell from 14.75M to 1.89M, follow-up queries rose
from 0.87M to 1.68M, mean routing nodes rose from 71.9k to 91.7k, and wire
downloads rose from 15.4k to 54.5k. Kernel UDP and wire-queue drops remained
zero. Global new metadata improved 229.9% for 8.7% more CPU and lower peak RSS.

The host qdisc had 8.56M cumulative historical drops. Its count was nearly
flat under refresh 32 but immediately rose again under refresh 256, consistent
with synchronized two-second refresh bursts. Future runs record exact per-run
qdisc deltas instead of relying on spot samples.

For A3→B3, refresh 32 improved global novelty by 300.9% and local metadata
throughput by 284.8% for 2.9% more CPU and effectively equal peak RSS. Refresh
queries fell from 14.75M to 1.84M, mean routing population rose from 74.0k to
90.3k, and wire downloads rose from 11.2k to 43.4k. The root qdisc counter was
unchanged throughout B3, while it rose materially during A3. Across the two
adjacent A→B pairs, the median paired global effect is +263.7% and the mean
absolute gain is 24.07 metadata/s. This remains directional rather than durable:
both pairs used the same order, there are no same-arm controls, and the shared
oracle was not isolated by block.

B3 also quantifies the sustained-rate problem. Its first 30-second local window
produced 1,392 metadata and its last produced 336, even though lookup dispatch
remained near its configured cap and all resource/drop guards stayed healthy.
The decline is downstream of lookup admission: peer yield, dial attempts, and
wire successes fall as hot hashes/peers are deduplicated or temporarily
blacklisted. Restarting may restore a local peak, but must not be promoted unless
an oracle-controlled long run shows enough additional global novelty to repay
bootstrap and repeated-work costs.

## Paused experiments

| Experiment | Causal question | Status |
|---|---|---|
| A3 refresh256 → B3 refresh32 | Does the large routing-refresh result repeat later in the same DHT/oracle timeline? | completed; repeated directional win |
| balanced `BA→AB`, lookup 300 vs 600 at refresh32 | Can spare CPU/network convert twice as many admitted hashes into more metadata? | cancelled before start at user-requested pause |

The lookup-rate screen uses one binary, one warm identity cohort, and one config
template. Only `discovery.lookup_rate` differs; both arms pin
`discovery.refresh_nodes=32`. Its controller was stopped before the first block,
so it produced no partial run or contaminated result.

## Adaptive backlog

Promote or kill each item using the funnel before spending on long confirmation:

1. `lookup_rate`: 300→600, then 900 only if CPU, bandwidth, qdisc, and wire queue
   retain headroom.
2. Lookup breadth: one DHT per infohash at proportionally higher hash rate, to
   test breadth against the current two-identity redundancy at equal initial
   query budget.
3. BEP 51 source mix: reserve a measured fraction of lookup capacity for
   `sample_infohashes` instead of leaving it behind a permanently full passive
   FIFO. Its advertised per-node interval must be respected; compare source
   novelty and peer conversion before increasing sample traffic.
4. DHT identity surface: 96→128 instances if lookup scaling still leaves CPU and
   bandwidth headroom.
5. Admission correctness: compare the `ab3b1fe` seen-reservation rollback binary
   against the same winning config.
6. Refresh floor: 32→16→8 only after higher-value throughput levers, watching
   routing population and follow-up conversion rather than refresh traffic alone.
7. If egress burst loss remains material, separately test staggered DHT refresh
   phases before changing the host qdisc.
8. Sustained-rate policy: after the funnel is efficient, compare an uninterrupted
   run with controlled warm restarts and, separately, rotated identity cohorts.
   A restart is useful only if global—not process-local—novelty rises enough to
   repay bootstrap time and repeated lookup work.

After short screening, each retained improvement becomes the next baseline and
must continue through the unexhausted experiment backlog. Balanced orders and
same-arm controls are evidence gates during iteration. A 6–12 hour steady run
belongs to a separate stability phase and requires explicit user authorization;
it is not an automatic promotion or an indication that optimization is done.

## Framework refinements

The current global oracle is a production-faithful but depleting shared state.
Balanced `AB/BA` order and same-arm controls estimate its time bias, but `/check`
also changes which downloads each later block attempts. The next confirmation
framework will therefore freeze the oracle at experiment start and give each
block a private append-only overlay. This keeps normal within-run deduplication
while making the pre-existing known set identical for every arm. The overlays
are merged into the production oracle only after all blocks finish.

The benchmark sink and paired controller now implement the read-only baseline
plus per-block writable overlays with backward-compatible shared-file behavior.
The isolated controller owns the sink lifecycle, records baseline/overlay and
manifest hashes, and preserves overlays without merging by default. An explicit
`--finalize-oracle` performs validated temporary-file merge and refuses to run
if production changed after the freeze. Deployment to a benchmark host and the
first isolated calibration are still pending.

Protocol review also changes one priority: BEP 51 explicitly supports surveying
the DHT with `sample_infohashes` and an advertised sampling interval. The current
strict-priority queue starves that source whenever passive `get_peers` traffic is
abundant. It should be tested through an explicit weighted capacity share, not
enabled as an unobservable bundle.
