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
- Short 5m-warmup/20m runs are directional screens, not durable claims.

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

After short screening, the chosen configuration must pass balanced orders,
same-arm negative controls, at least six valid treatment pairs, and a 6–12 hour
steady run.

## Framework refinements

The current global oracle is a production-faithful but depleting shared state.
Balanced `AB/BA` order and same-arm controls estimate its time bias, but `/check`
also changes which downloads each later block attempts. The next confirmation
framework will therefore freeze the oracle at experiment start and give each
block a private append-only overlay. This keeps normal within-run deduplication
while making the pre-existing known set identical for every arm. The overlays
are merged into the production oracle only after all blocks finish.

The benchmark sink now implements the read-only baseline plus writable overlay
storage primitive with backward-compatible single-file behavior. Controller
lifecycle, overlay artifact hashing, and post-experiment merge remain pending;
the feature has not been deployed to the paused benchmark host.

Protocol review also changes one priority: BEP 51 explicitly supports surveying
the DHT with `sample_infohashes` and an advertised sampling interval. The current
strict-priority queue starves that source whenever passive `get_peers` traffic is
abundant. It should be tested through an explicit weighted capacity share, not
enabled as an unobservable bundle.
