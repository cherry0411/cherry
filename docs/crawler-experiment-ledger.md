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

For A2→B2, refresh queries fell from 14.75M to 1.89M, follow-up queries rose
from 0.87M to 1.68M, mean routing nodes rose from 71.9k to 91.7k, and wire
downloads rose from 15.4k to 54.5k. Kernel UDP and wire-queue drops remained
zero. Global new metadata improved 229.9% for 8.7% more CPU and lower peak RSS.

The host qdisc had 8.56M cumulative historical drops. Its count was nearly
flat under refresh 32 but immediately rose again under refresh 256, consistent
with synchronized two-second refresh bursts. Future runs record exact per-run
qdisc deltas instead of relying on spot samples.

## Active and queued experiments

| Order | Experiment | Causal question | Status |
|---:|---|---|---|
| 1 | A3 refresh256 → B3 refresh32 | Does the large routing-refresh result repeat later in the same DHT/oracle timeline? | running |
| 2 | balanced `BA→AB`, lookup 300 vs 600 at refresh32 | Can spare CPU/network convert twice as many admitted hashes into more metadata? | queued |

The lookup-rate screen uses one binary, one warm identity cohort, and one config
template. Only `discovery.lookup_rate` differs; both arms pin
`discovery.refresh_nodes=32`.

## Adaptive backlog

Promote or kill each item using the funnel before spending on long confirmation:

1. `lookup_rate`: 300→600, then 900 only if CPU, bandwidth, qdisc, and wire queue
   retain headroom.
2. Lookup breadth: one DHT per infohash at proportionally higher hash rate, to
   test breadth against the current two-identity redundancy at equal initial
   query budget.
3. DHT identity surface: 96→128 instances if lookup scaling still leaves CPU and
   bandwidth headroom.
4. Admission correctness: compare the `ab3b1fe` seen-reservation rollback binary
   against the same winning config.
5. Refresh floor: 32→16→8 only after higher-value throughput levers, watching
   routing population and follow-up conversion rather than refresh traffic alone.
6. If egress burst loss remains material, separately test staggered DHT refresh
   phases before changing the host qdisc.

After short screening, the chosen configuration must pass balanced orders,
same-arm negative controls, at least six valid treatment pairs, and a 6–12 hour
steady run.
