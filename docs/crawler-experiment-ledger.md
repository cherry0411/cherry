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

### `lookup-rate-300-vs-600-screen1` — interrupted screen

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

#### First-pair execution and stop audit

- **Execution:** both regions started at `2026-07-18T09:34:46Z` against frozen
  baseline SHA-256 `afb79bd40e8d4e9c2c3d6745ebf713955894da62cdaee6b960c182ca3df48c5d`.
  SG ran `A(300) -> B(600)` and JP ran `B(600) -> A(300)`. Block 1 completed;
  block 2 was stopped at about 512 seconds, before its measurement interval,
  when the then-active rule treated any crawler DHT packet-channel drop as a
  hard failure. Both experiment manifests say `interrupted`; neither oracle was
  finalized and no partial block has a result or index record.
- **Completed block-1 results:** SG A produced `10,615` oracle-new metadata in
  300 seconds (`35.383/s`) from `15,393` local exports and `16,052` wire
  downloads. JP B produced `8,571` (`28.570/s`) from `11,294` exports and
  `13,116` downloads. These arms are deliberately not subtracted: treatment is
  perfectly confounded with region in this incomplete pair.
- **Why the stop is infrastructure diagnosis, not treatment evidence:** the JP
  block-2 runtime log reports two isolated per-window deltas:
  `35/366,160 = 0.0096%` at `09:56:20Z` and
  `442/391,889 = 0.1128%` at `09:57:50Z`; intervening and following windows are
  zero. `PacketStats.Dropped` is cumulative inside each DHT, but the 30-second
  logger subtracts the prior aggregate, so `dropped=` is a window delta. The
  increment happens only when the non-blocking send from a UDP reader to that
  DHT's `packets` channel takes its `default` branch. It is therefore an
  application jobs-channel overflow, not a Linux UDP loss counter.
- **Packet topology:** each of 96 consecutive DHT sockets (`21000-21095`) has
  `packet_jobs=512`, `packet_workers=2`, and `read_workers=1`: 96 independent
  512-entry channels, 192 packet handlers and 96 UDP readers. Both drop windows
  coincide with `GOGC 200 -> 400` telemetry, which is correlation only; the
  preserved data cannot identify a GC pause as the cause.
- **Host evidence at the JP incident:** 30-second host samples keep
  `UdpRcvbufErrors=0`, `UdpSndbufErrors=0`, and root-qdisc drops `21,223 ->
  21,223`. Ten-minute `sar` buckets spanning the event show eth0 rx/tx error and
  drop rates of zero and softnet `dropd/s=0`; softnet `squeezd/s` is about 1.49,
  indicating some receive-budget pressure without a softnet drop. SG likewise
  has no per-run UDP/qdisc delta and no crawler channel drop. Historical `nstat`
  and per-socket queue samples were not collected by this run, so current
  cumulative values are not back-attributed to it.
- **Exact cross-region overlay diagnostic:** read-only copies were made under
  `C:\Users\Themis\AppData\Local\Temp\cherry-audit-20260718T093446Z` and
  verified against the originals. Full block-1 overlays are SG
  `af020b08f20aaae393926d14266ba2b45dff5f5eba9bc08ab05fa0fd1f03fb28`
  (29,331 metadata) and JP
  `46f52b658310044b381d6784be7c8f100da60635ebfd20c2cf4d79d3ff0ce0ad`
  (25,536). Their full-run metadata intersection/exclusives/union are
  `2,060 / 27,271 / 23,476 / 52,807`. For the exact 300-second suffix, the SG
  slice takes records `[18,862,29,563)` and has SHA-256
  `00f7d00bc75c1c146671dc71409a45d65085dc8a889d6904342f388be05a3774`;
  the JP slice takes `[17,116,25,764)` and has SHA-256
  `bb845ae5ab9c7e7c03f428fa524dfee8c77574ee992cb474599bb93f2ac450af`.
  Metadata is SG `10,615`, JP `8,571`, intersection `702`, SG-exclusive
  `9,913`, JP-exclusive `7,869`, union `18,484` (Jaccard `3.80%`). This only
  diagnoses low regional overlap; because SG is A and JP is B it cannot estimate
  a lookup-rate effect or justify promotion.
- **Immutable evidence:** completed SG/JP runs are
  `/home/ubuntu/cherry/bench/runs/20260718T093446Z_lookup-rate-300-vs-600-screen1-sg-pair1-block1_A_5285bf14754c`
  and
  `/home/ubuntu/cherry/bench/runs/20260718T093446Z_lookup-rate-300-vs-600-screen1-jp-pair1-block1_B_5285bf14754c`;
  result SHA-256 values are `f5ae741499e1...` and `bcb6c9bc47f7...`.
  Interrupted block-2 run directories are
  `/home/ubuntu/cherry/bench/runs/20260718T094949Z_lookup-rate-300-vs-600-screen1-sg-pair1-block2_B_5285bf14754c`
  and
  `/home/ubuntu/cherry/bench/runs/20260718T094949Z_lookup-rate-300-vs-600-screen1-jp-pair1-block2_A_5285bf14754c`;
  crawler-log SHA-256 values are `cb6c7ba18d58...` and `86196a779727...`, and
  their partial overlay SHA-256 values are `e275d6635bfc...` and
  `0b3651bfe22f...`. Experiment manifests are at the corresponding
  `bench/oracle-experiments/20260718T093446Z_*_pair1_{1,2}/manifest.json`, both
  `finalized=false` and `status=interrupted`.
- **Rollback verified:** experiment-owned controllers, crawlers and sinks were
  stopped; the 96-port range and loopback `:5070` were free before the next
  diagnostic. Immutable release `20260718T075740Z-ac7a239ee04d` and both
  96-ID cohorts were unchanged. No binary/config was deployed.

### `lookup300-dht-dropdiag1` — completed same-arm infrastructure diagnostic

- **Question, not promotion:** reproduce the sparse JP jobs-channel drop under
  the same `lookup_rate=300` arm in both regions and align it with kernel,
  softnet, qdisc, socket-queue, CPU, RSS and crawler telemetry. This is a
  same-arm regional diagnostic, not an A/B treatment or champion comparison.
- **Frozen controls:** immutable release `20260718T075740Z-ac7a239ee04d`, binary
  SHA-256 `5285bf14754c4eb4aeed276f42fd2be65a357ade4fe18896ee083a05c13225a8`,
  `refresh_nodes=32`, `lookup_rate=300`, `packet_jobs=512`,
  `packet_workers=2`, `read_workers=1`, fixed regional 96-ID cohorts and UDP
  ports `21000-21095`. Every attempt used a private overlay over frozen baseline
  SHA-256 `afb79bd40e8d4e9c2c3d6745ebf713955894da62cdaee6b960c182ca3df48c5d`;
  finalization remained disabled.
- **Invalid-attempt audit:** the original time-aligned attempts started at
  `10:11:30Z` but their preregistered sampler-v1 rule stopped SG at about 100
  seconds on two kernel/socket loss increments and JP at about 260 seconds on
  one. They remain `invalid-early-stop` and are not salvaged after the fact.
  A later `10:16Z` registration was superseded before start and has a
  `not-started.json` marker. An SG retry inherited the over-strict v1 rule and
  was administratively stopped before inference; its
  `administrative-stop.json` excludes it from all result comparisons.
- **Calibrated retry gate:** for retry2, a roughly 30-second kernel/socket window
  stops only at absolute loss `>=32` and ratio `>=0.01%`, two consecutive
  nonzero windows totaling `>=32`, or one-window loss ratio `>=0.1%`. Crawler
  jobs-channel loss remains the response and stops only after two consecutive
  windows above 1%. Pause/wire-queue loss, RSS above 3.4 GiB, CPU above 190% for
  three 10-second samples, and qdisc growth above 200/min remain hard gates.
  Sampler v2 SHA-256 is
  `efb1a5f019275400078cf6f70c0e68a51edaf31eeece87c0558767e7e038a7d8`.
- **Valid retry2 execution:** SG started at `10:20:00Z`; JP at `10:22:15Z`.
  Each completed 10 minutes warm-up plus 10 minutes measurement with 20/20
  measurement windows and oracle coverage 1.0. SG produced `19,102` global-new
  metadata (`31.837/s`) from `25,315` exports and `25,966` downloads; JP
  produced `21,064` (`35.107/s`) from `26,589` exports and `27,866` downloads.
  CPU means were `111.95%/110.05%`, maximum RSS `1,967.7/1,917.5 MiB`, with no
  benchmark UDP-buffer, qdisc or wire-queue delta.
- **Drop diagnosis:** SG's measurement had `0/7,360,687` crawler packet-channel
  drops. JP had `548/8,119,430 = 0.00675%`, in runtime windows 34, 36, 38 and 40
  (`108`, `161`, `26`, `253`); no two affected windows were consecutive and the
  maximum window ratio was `0.0537%`. Across the full active 20-minute interval,
  SG/JP kernel loss was only `7/13,405,441` and `8/14,584,633` datagrams, with
  per-window maxima of one packet; socket-drop deltas were `6/8`. Both had zero
  `UdpRcvbufErrors`, `UdpSndbufErrors`, IP discard, softnet-drop, qdisc, and
  netdev-drop growth. Maximum aggregate socket RX queues were about `165/348
  KiB`. The sparse JP jobs-channel loss is real but operationally negligible at
  this load, and is not evidence of a kernel or 2C4G resource ceiling.
- **Sampler-tail qualification:** after SG's crawler/result had completed and
  all 96 sockets had disappeared, sampler v2 observed 11 host-global UDP errors
  against seven unrelated datagrams in `[1210,1240)s` and emitted a ratio gate.
  This is outside the crawler interval, with PID/socket count zero, so it does
  not invalidate SG. It does expose a sampler bug: future kernel gates must be
  disabled after the owned PID or socket cohort disappears.
- **Exact same-arm regional union:** read-only measurement suffixes are SG
  records `[26,694,45,959)` (metadata `19,102`, rejects `163`, slice SHA-256
  `7de9951c7f611443019a9d9c3e7b9a92c521cd27d361b54d46554cca9245c73a`)
  and JP `[27,594,48,843)` (metadata `21,064`, rejects `185`, slice SHA-256
  `8d0a21436d33668b17b37df0454865b83bfc02765832834e4b73b520014e10d6`).
  Metadata intersection/exclusives/union are `1,458 / 17,644 / 19,606 /
  38,708` (Jaccard `3.77%`). This supports regional complementarity, not a
  treatment effect; the starts were also offset by 135 seconds.
- **Rate-decay inference:** global-new rate fell from first to second half on
  both hosts: SG `35.112 -> 28.715/s` (`-18.2%`) and JP `38.118 -> 32.288/s`
  (`-15.3%`). SG declined with zero crawler packet-channel drops, so those drops
  cannot be the principal cause of the startup/within-run decay.
- **Immutable evidence:** valid SG/JP runs end in
  `20260718T102000Z_lookup300-dht-dropdiag1-sg-retry2_A300-samearm-diagnostic-retry2_5285bf14754c`
  and
  `20260718T102215Z_lookup300-dht-dropdiag1-jp-retry2_A300-samearm-diagnostic-retry2_5285bf14754c`.
  Result SHA-256 values are `f3ddf481e6c1...` and `c5aaddd9dfdb...`; overlay
  hashes are `c692dd33414a...` and `e9548765e1f6...`; preregistration hashes are
  `1be2f9304492...` and `38d1b7424206...`. All diagnostic sidecars and gates are
  in the matching `bench/diagnostics/20260718T10{2000,2215}Z_*` directories.
- **Next single-variable candidate (not started):** do not spend the next arm on
  packet workers or queue depth. Restart the entire balanced
  `lookup_rate=300 -> 600` screen from fresh adjacent host-local `AB/BA` pairs,
  using the calibrated infrastructure gates above. The old completed arms cannot
  be paired with new missing arms after this time gap. No new arm or binary was
  launched by this diagnostic.

### `lookup-rate-300-vs-600-screen2` — completed balanced screen; 600 rejected

- **Preregistered causal question:** does doubling active-lookup admission from
  A `lookup_rate=300` to B `lookup_rate=600` increase oracle-new searchable
  metadata? SG used `ABBA` and JP `BAAB`; comparison is restricted to the four
  adjacent host-local pairs. Every block ran 10 minutes warm-up plus 5 minutes
  measurement with exactly ten measurement windows and oracle coverage 1.0.
  Preregistration SHA-256 values are SG
  `c9953758d57bc60487196ab1769b2bfc376af4c72f009e2de79aebb82b7ff6be`
  and JP
  `70a7eecc46da65bc84a91b32212f03133a2ada76338208fc3882126ed5a23c51`.
- **Frozen controls and isolation:** immutable release
  `20260718T075740Z-ac7a239ee04d`, binary SHA-256
  `5285bf14754c4eb4aeed276f42fd2be65a357ade4fe18896ee083a05c13225a8`,
  template-config SHA-256
  `d0d5a44c268aab03d9efbe5cf626530a7f7079fb353332b1cacf1470a4dde7b7`,
  `refresh_nodes=32`, 96 persistent IDs and UDP ports `21000-21095` per
  region. Every block used its own overlay over the identical frozen baseline
  `afb79bd40e8d4e9c2c3d6745ebf713955894da62cdaee6b960c182ca3df48c5d`.
  Both manifests are `completed`, `finalized=false`; the source hash was
  unchanged after the experiment and no overlay was merged.

| Region / plan | Block | Arm | Global new/s | Local metadata/s | Downloads | CPU mean | RSS peak MiB | First -> second half/s | Transient slope* |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| SG / `ABBA` | 1 | A | 36.440 | 49.820 | 15,409 | 112.4% | 1,947.3 | 37.313 -> 35.827 | -82.5k |
| SG / `ABBA` | 2 | B | 18.533 | 23.493 | 7,251 | 131.6% | 1,987.1 | 18.652 -> 18.250 | -48.0k |
| SG / `ABBA` | 3 | B | 13.857 | 17.857 | 5,463 | 130.0% | 1,801.2 | 17.777 -> 10.667 | -565.3k |
| SG / `ABBA` | 4 | A | 22.653 | 28.457 | 8,711 | 120.3% | 1,788.9 | 23.215 -> 22.023 | -107.6k |
| JP / `BAAB` | 1 | B | 29.417 | 37.473 | 12,298 | 121.0% | 1,733.3 | 29.162 -> 29.675 | +99.5k |
| JP / `BAAB` | 2 | A | 30.693 | 37.770 | 11,781 | 116.4% | 1,914.3 | 31.124 -> 30.357 | -85.0k |
| JP / `BAAB` | 3 | A | 27.633 | 33.417 | 10,352 | 127.6% | 1,865.1 | 27.820 -> 27.498 | -47.7k |
| JP / `BAAB` | 4 | B | 16.387 | 20.013 | 6,275 | 144.1% | 1,810.6 | 16.652 -> 16.131 | -32.5k |

\* Analyzer slope unit is oracle-new metadata/hour per crawler-uptime hour. A
five-minute regression is a transient diagnostic, not a long-run decay estimate.

| Host-local pair | A new/s | B new/s | B-A new/s | B/A effect | Lookup exposure B/A | Refresh B/A | Download effect |
|---|---:|---:|---:|---:|---:|---:|---:|
| SG `AB` | 36.440 | 18.533 | -17.907 | -49.14% | 2.062 | 0.995 | -52.94% |
| SG `BA` | 22.653 | 13.857 | -8.797 | -38.83% | 2.019 | 0.999 | -37.29% |
| JP `BA` | 30.693 | 29.417 | -1.277 | -4.16% | 2.036 | 1.001 | +4.39% |
| JP `AB` | 27.633 | 16.387 | -11.247 | -40.70% | 2.029 | 1.002 | -39.38% |

- **Decision:** treatment fidelity passed in all four pairs, but B was negative
  in `4/4`. The median paired log effect is `-39.77%` (median absolute loss
  `-10.022 metadata/s`); balanced geometric-mean effect is `-35.15%` and the
  descriptive pooled count effect is `-33.41%`. Region-stratified geometric
  effects are SG `-44.22%` and JP `-24.61%`. This fails every preregistered
  retention condition and eliminates `lookup_rate=600` under this policy. The
  one-sided four-of-four sign probability is `0.0625`; this small screen is
  sufficient to reject a large loss, not to claim long-run stability or call A
  a universal champion. `lookup_rate=300` remains the current reference.
- **Mechanism evidence:** B increased admitted active lookups as intended, but
  announcement-source downloads fell in every pair: SG `-54.60%/-39.82%`, JP
  `-12.77%/-44.01%`. Get-peers-source downloads changed `-36.11%/+4.63%` in SG
  and `+161.46%/+11.47%` in JP; only the early JP B arm offset enough announce
  loss to make total downloads positive, while its oracle-new rate still fell.
  This is consistent with excessive active work cannibalizing the higher-yield
  passive announce/dial path; it is not proof of the exact shared bottleneck.
- **Resource and loss audit:** B used `+4.6` to `+19.2` CPU percentage points,
  but no block approached the 190% gate; peak RSS stayed below 1.99 GiB. The
  largest measurement traffic was about 42.9 Mbit/s RX and 32.0 Mbit/s TX,
  far below the nominal 200 Mbit/s link. All result windows had zero qdisc,
  UDP-buffer and wire-queue growth. SG measurement packet-channel loss was zero;
  JP measurement loss was `0`, `1,353`, `1,373`, and `1,138` packets by block,
  at most `0.021%` of received packets in aggregate. Across full active
  intervals the worst kernel-loss block totaled 24 datagrams, the worst
  approximately-30-second window lost six (`0.0013%`), and every block had zero
  UDP-buffer, softnet, qdisc and netdev-drop growth. Sampler-v3 exited cleanly
  with no gate in all eight blocks.
- **Order, region and decay:** the geometric B effect is more negative when B
  is second (`AB -45.08%`) than when B is first (`BA -23.43%`); SG minus JP
  mean-log contrast is `-0.3013`, AB minus BA is `-0.3323`, and the saturated
  region-by-order interaction is `+0.2955`. With one observation per cell these
  are descriptive, not inferential. Wall-clock supply decay is independently
  visible in adjacent same-arm blocks: SG B block 2 -> 3 fell `25.2%`, and JP A
  block 2 -> 3 fell `10.0%`. Within blocks, seven of eight second halves fell;
  the median change was `-2.8%`, with SG B3 an exceptional `-40.0%`. Balanced
  order removes directional bias from the treatment sign, but does not explain
  the changing environment.
- **Restart/port inference:** every block was a fresh process with a fresh
  private overlay but the same frozen baseline, identity cohort and ports. The
  cross-block decline therefore is not production-oracle depletion or retained
  in-process blacklist state. Repeated controlled restarts did not restore the
  initial SG rate, so automatic restart is not supported as a cure. Ten minutes
  of warm-up also makes delayed replies to a previous process an implausible
  explanation for the measured B result. Port or identity rotation would be a
  different causal treatment and must not be smuggled into rollback.
- **Immutable evidence:** SG experiment and diagnostic roots are
  `/home/ubuntu/cherry/bench/{oracle-experiments,diagnostics}/20260718T110000Z_lookup-rate-300-vs-600-screen2-sg`;
  JP uses the same suffix ending `-jp`. Manifest SHA-256 values are SG
  `a2c5e4c5d39cef9e46b30c7e6057b89c3443b37e4b4b644c5a1c94f8042fbf45`
  and JP
  `a647d886883a4b33c133dcf76ca86fdd5b8a49adb4f9a974b5ce5c105a7c0e7d`.
  Result SHA prefixes by block are SG `de7dc403`, `cbc7e231`, `02f95f0c`,
  `f442608f`; JP `afadd851`, `a7fffe74`, `ae09c20a`, `fbf2c33f`. Read-only
  local copies under
  `C:\Users\Themis\AppData\Local\Temp\cherry-lookup-screen2\artifacts`
  match those hashes. After completion, crawler, sink, sampler and controller
  processes were absent on both hosts; experiment ports were released.
- **Next candidate, not started:** bracket downward with
  `lookup_rate=150` versus the retained 300 reference, holding every other
  knob fixed and using the same isolated, balanced two-region design. Its
  hypothesis is that releasing shared DHT/peer/dial capacity raises the much
  larger passive-announcement yield enough to offset fewer active lookups. If
  150 wins, next bracket `75/225`; if it loses, test 225 between the known 150
  and 300 points. A later code-level candidate is explicit source capacity
  isolation. No new remote arm was launched pending review.

### `lookup-rate-300-vs-150-screen3` — completed; invalid for promotion, no winner

- **Preregistered causal question:** does reducing active-lookup admission from
  A `lookup_rate=300` to B `lookup_rate=150` release enough shared capacity to
  increase oracle-new searchable metadata? SG used `ABBA`, JP used `BAAB`, and
  every block was a fresh process with 10 minutes warm-up plus 5 minutes
  measurement. All eight blocks produced exactly ten measurement windows with
  oracle coverage 1.0. Preregistration SHA-256 values are SG
  `40b6f8eea85a2c574d7e42a3d4f2895a357be9103db31dc562d37823172bc0e8`
  and JP
  `5457bc7c435200fed841ec4e12f5c6ea3059662a182fd0a88fb4f69afc61e0a7`.
- **Frozen controls and deliberate port change:** immutable release
  `20260718T075740Z-ac7a239ee04d`, binary SHA-256
  `5285bf14754c4eb4aeed276f42fd2be65a357ade4fe18896ee083a05c13225a8`,
  template-config SHA-256
  `d0d5a44c268aab03d9efbe5cf626530a7f7079fb353332b1cacf1470a4dde7b7`,
  `refresh_nodes=32`, two lookup DHTs, eight follow-ups, two lookup workers and
  96 persistent regional IDs were fixed. This screen deliberately moved the
  crawler UDP cohort from the previously established `21000-21095` ports to
  fresh `22000-22095`; the isolated sink used 5080. Every block received a
  private overlay over frozen baseline
  `afb79bd40e8d4e9c2c3d6745ebf713955894da62cdaee6b960c182ca3df48c5d`.
  Both manifests are `completed`, `finalized=false`, and the production oracle
  hash remained unchanged.

| Region / plan | Block | Arm | Global new/s | Local metadata/s | Downloads | CPU mean | RSS peak MiB | First -> second half/s | Transient slope* |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| SG / `ABBA` | 1 | A | 21.993 | 30.330 | 11,380 | 45.0% | 1,439.8 | 17.947 -> 25.456 | +780.5k |
| SG / `ABBA` | 2 | B | 36.090 | 47.197 | 14,407 | 100.9% | 1,540.8 | 36.318 -> 36.242 | -126.2k |
| SG / `ABBA` | 3 | B | 26.757 | 33.337 | 10,148 | 108.0% | 1,901.5 | 28.099 -> 24.988 | -314.7k |
| SG / `ABBA` | 4 | A | 26.147 | 32.160 | 9,894 | 123.1% | 1,912.7 | 27.491 -> 25.032 | -181.2k |
| JP / `BAAB` | 1 | B | 0.063 | 0.073 | 41 | 14.3% | 600.7 | 0.033 -> 0.058 | +0.5k |
| JP / `BAAB` | 2 | A | 0.330 | 0.350 | 156 | 14.4% | 636.1 | 0.460 -> 0.225 | -44.8k |
| JP / `BAAB` | 3 | A | 0.070 | 0.073 | 29 | 14.3% | 637.0 | 0.033 -> 0.100 | +2.4k |
| JP / `BAAB` | 4 | B | 0.093 | 0.097 | 41 | 13.9% | 608.9 | 0.112 -> 0.075 | -2.6k |

\* Analyzer slope unit is oracle-new metadata/hour per crawler-uptime hour. The
five-minute slope remains a transient diagnostic, not a long-run decay estimate.

| Host-local pair | A new/s | B new/s | B-A new/s | B/A effect | Lookup exposure B/A | Refresh B/A | Download effect | Eligibility |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| SG `AB` | 21.993 | 36.090 | +14.097 | +64.10% | 0.494 | 0.999 | +26.60% | exposure passed; cold-port/time confounded |
| SG `BA` | 26.147 | 26.757 | +0.610 | +2.33% | 0.502 | 0.998 | +2.57% | passed; below the 3% worthwhile floor |
| JP `BA` | 0.330 | 0.063 | -0.267 | -80.81% | 0.451 | 1.000 | -73.72% | exposure passed; supply-starved/time confounded |
| JP `AB` | 0.070 | 0.093 | +0.023 | +33.33% | 0.884 | 1.001 | +41.38% | failed lookup-exposure gate; tiny count |

- **Decision — no promotable 150/300 winner:** the raw signs are positive in
  `3/4`, but the second JP pair failed treatment fidelity and the JP regional
  geometric effect is negative (`-49.41%`). SG's later, exposure-valid reversed
  pair is only `+2.33%`, inside the preregistered `<=3%` no-change band. The
  descriptive summaries disagree sharply (pooled count `+29.80%`, median-log
  `+16.81%`, balanced geometric mean `-19.04%`) because the first pairs combine
  a new-port ramp with large wall-time and regional supply drift. They must not
  be used to select a parameter. `lookup_rate=150` is neither retained nor
  eliminated; `lookup_rate=300` remains only the reference, not a champion.
- **Port/supply falsification:** ten minutes of per-process warm-up was not a
  sufficient eligibility rule after port rotation. SG A1 was still ramping
  strongly during measurement (`+41.84%` from first to second half), then the
  adjacent B block inherited a much richer passive supply. JP received roughly
  1.17-1.20 million host UDP datagrams per active block and approximately
  0.39-0.40 million DHT packets in each measurement, yet announcement-source
  downloads across all four blocks were only `0, 0, 1, 0`. For comparison, the
  same JP host on the established ports in screen2 produced 16-31 oracle-new
  metadata/s. This does not prove the mechanism of external port propagation,
  but it proves that fresh-port reachability/passive announce supply is an
  independent treatment and that process warm-up alone cannot qualify it.
- **Mechanism diagnostics:** SG announcement-source downloads by block were
  `4,814 / 13,747 / 9,796 / 9,209`, while get-peers-source downloads were
  `6,566 / 660 / 352 / 684`. In the later valid SG pair, halving lookup changed
  announce downloads by `+6.37%`, get-peers downloads by `-48.54%`, and total
  downloads by only `+2.57%`; the oracle-new effect was correspondingly small.
  SG B also fell `25.86%` from block 2 to the adjacent same-arm block 3. JP A
  fell `78.79%` from block 2 to 3, but on a supply-starved base too small for a
  stable percentage. These are wall-time/source diagnostics, not additional
  treatment pairs.
- **Resource and loss audit:** all eight supervisor records are `completed`,
  every `gates` array is empty, and every sampler exited zero. Sidecar peak CPU
  was 124%; peak RSS was 1,972.7 MiB; all active samples retained the owned PID
  and 96 experiment-port sockets. Measurement traffic peaked near 39.7 Mbit/s
  RX and 28.6 Mbit/s TX. Active intervals had zero UDP rcvbuf/sndbuf, softnet
  drop and netdev-drop growth; SG block 1 added one qdisc drop, far below the
  200/minute gate. Wire-queue drops were zero. SG app DHT-channel drops were 57
  in block 3 and 3,193 in block 4 (`0.0015%` and `0.0555%` of received DHT
  packets), below the two-window 1% gate. Socket-drop counters grew by one and
  four in SG blocks 3 and 4 without queue/pause or kernel-buffer gate. The final
  SG sampler row observed 196 rcvbuf errors only after the process exited and
  all 96 sockets disappeared; the preregistration explicitly excludes that
  post-exit interval.
- **Immutable evidence:** SG roots end in
  `20260718T123500Z_lookup-rate-300-vs-150-screen3-sg` under both
  `/home/ubuntu/cherry/bench/oracle-experiments` and `bench/diagnostics`; JP uses
  the corresponding `-jp` suffix. Oracle-manifest SHA-256 values are SG
  `c3eca20308ec7a8cb91f583517c3914e2e0b5a28fc4b4d4e628519c0cf2f5968`
  and JP
  `495632ce2cd09a7e4b2dd4767e1678e41d0bdf78931da7d987c63142c97c10d8`.
  Result SHA prefixes by block are SG `153ae81f`, `f18b82ab`, `f11a2bcf`,
  `4a677452`; JP `814015ab`, `061dbfc6`, `40037440`, `c6d25ad9`. Read-only
  copies under
  `C:\Users\Themis\AppData\Local\Temp\cherry-lookup-screen3\screen3\artifacts`
  match all remote content-set digests (SG oracle/diagnostics/runs:
  `aa2127c6`/`b1f72ca7`/`6e6458bf`; JP:
  `db6156d3`/`76f2ef11`/`a78e42a5`). At the final 13:51Z audit, production
  baseline hashes were unchanged, crawler/sink/sampler/controller processes
  were absent, and ports 5080 plus `22000-22095` were free on both hosts.
- **Next candidate, candidate only / not started:** first run a port-stability
  calibration, not another lookup arm: use the established `21000-21095` cohort
  as the control and separately characterize a fresh or preserved `22000-22095`
  cohort under one fixed `lookup_rate=300` arm until passive announce supply and
  rolling global-new rate meet a preregistered steady-eligibility rule. Then
  rerun balanced 300 versus 150 only on eligible, persistent ports. `225` and
  `75` remain downstream routing options; neither is justified by this invalid
  screen. No new remote process was launched after screen3.

### dual-channel-durable-oracle-v1 — implemented locally, not deployed

Hypothesis / mechanism: production persistence and a frozen experimental oracle
can run simultaneously without contaminating either contract if production
metadata crosses its durable spool/fsync boundary first and the experiment sees
only a separate, typed `infohash + action` projection. Minimum worthwhile
effect is qualitative: zero downloaded durable decisions diverted from central
storage and zero unreported oracle evidence loss.

A / B (only changed variable): architecture change, not a throughput A/B. Before,
`Exporter.HTTPEndpoint` served both metadata export and `/check`, so pointing it
at a private benchmark sink discarded metadata bodies; pointing it at production
made the oracle move between sequential arms. After, `http_endpoint/api_key`
remain production-only while optional `oracle_endpoint/oracle_api_key` own frozen
`/check` plus post-fsync observations.

Frozen inputs and controls: local predecessor `173aaf1`; no crawler server,
port, node ID, frozen baseline, production database, or remote release was
changed. Raw bencode, piece hashes, title and file paths have no representation
in the observation protocol. Legacy empty-oracle config, M/R files, logs and
results remain readable.

Warm-up / measurement / order: no network benchmark is claimed. This is the
measurement-enabling P0 implementation that must precede the first crawler run
which writes continuously to the independent storage server. Future durable
benchmark configs preserve production `http_endpoint` and route each run-local
sink to `oracle_endpoint`.

Primary metric / guardrails / falsification: oracle primary is searchable
unique (`full + summary`) per second; all four action counts remain diagnostic.
Any `oracle_obs_drop`, `oracle_obs_http_fail`, or lifetime
`oracle_obs_invalid=true` hard-invalidates comparison. Queue depth/capacity is
recorded; a configured observer with any hidden loss, an observation containing
metadata content, production ACK latency coupled to oracle HTTP, or changed
legacy behavior falsifies the design.

Immutable evidence paths: source/tests under
`cherry-picker/internal/export/oracle_observer*`,
`cherry-picker/cmd/benchmark-sink`, `cherry-picker/internal/app`, and
`scripts/benchmark`. Local verification: targeted Go packages pass; 36 Python
benchmark tests pass. Full Go/vet/Linux verification is required before commit.

Result and reasoning: the production path calls the bounded non-blocking
observer only after `DurableIngestor.Submit` succeeds. HTTP failure retries but
taints the process lifetime; bounded-channel or shutdown loss increments an
explicit drop counter. Benchmark sink writes 21-byte F/S/H/R records, treats all
four as known, counts only F+S as searchable, reads legacy M/R, and merges with
`full > summary > hash_only > reject` without mutating source overlays. Exact
regional union and the run analyzer use the same semantics.

Rollback point / triggers / verification: before any deployment, rollback is a
release/config switch to `173aaf1` and removal of `oracle_endpoint` from the
effective config; production spool format and backend schema are unchanged.
Trigger on any Go/Python/vet/Linux-build failure, measurable crawler CPU/RSS
regression, observation queue pressure, or mismatch between post-policy durable
submits and oracle action totals. Verify predecessor SHA, legacy `/check`, M/R
oracle loading, and production spool delivery after rollback.

Next action: finish full local verification and review, then deploy only after
the independent storage host is available. The existing remote crawler
experiment continues on its immutable old binary and is not mixed with this
change.

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

## Experiment status

| Experiment | Causal question | Status |
|---|---|---|
| A3 refresh256 → B3 refresh32 | Does the large routing-refresh result repeat later in the same DHT/oracle timeline? | completed; repeated directional win |
| balanced lookup 300 vs 600 at refresh32 | Can spare CPU/network convert twice as many admitted hashes into more metadata? | completed in SG/JP; 600 rejected in 4/4 host-local pairs |
| balanced lookup 300 vs 150 at refresh32 | Does less active work release enough shared capacity to increase higher-yield passive-announcement metadata? | completed 8/8, but invalid for promotion because fresh-port supply failed eligibility; no winner |
| persistent-port supply calibration | When is a port/cohort sufficiently reachable and steady for a sequential lookup A/B? | candidate only; not started |

The completed screen2 used one binary and config template, persistent regional
identity cohorts, and only changed `discovery.lookup_rate`; both arms pinned
`discovery.refresh_nodes=32`. The earlier screen1 remains interrupted and is
excluded rather than spliced into the complete screen2 pairs.

## Adaptive backlog

Promote or kill each item using the funnel before spending on long confirmation:

1. Port/supply eligibility: characterize established versus fresh/preserved ports
   under one fixed arm, then preregister an announce-supply and rolling-rate
   eligibility rule. Port rotation is a causal treatment, not free warm-up.
2. `lookup_rate`: 600 was eliminated despite resource headroom. The first
   300→150 screen is complete but cannot select a winner because the new-port
   supply gate failed; rerun it on eligible persistent ports. Use 225 only after
   a valid 150/300 result places the optimum between them.
3. Source-capacity isolation: if the downward bracket supports announce-path
   cannibalization, give passive announce and active get-peers explicit measured
   quotas instead of relying only on one global lookup-rate cap.
4. Lookup breadth: one DHT per infohash at proportionally higher hash rate, to
   test breadth against the current two-identity redundancy at equal initial
   query budget.
5. BEP 51 source mix: reserve a measured fraction of lookup capacity for
   `sample_infohashes` instead of leaving it behind a permanently full passive
   FIFO. Its advertised per-node interval must be respected; compare source
   novelty and peer conversion before increasing sample traffic.
6. DHT identity surface: 96→128 instances if lookup scaling still leaves CPU and
   bandwidth headroom.
7. Admission correctness: compare the `ab3b1fe` seen-reservation rollback binary
   against the same winning config.
8. Refresh floor: 32→16→8 only after higher-value throughput levers, watching
   routing population and follow-up conversion rather than refresh traffic alone.
9. If egress burst loss remains material, separately test staggered DHT refresh
   phases before changing the host qdisc.
10. Sustained-rate policy: after the funnel is efficient, compare an uninterrupted
   run with controlled warm restarts and, separately, rotated identity cohorts.
   A restart is useful only if global—not process-local—novelty rises enough to
   repay bootstrap time and repeated lookup work.

After short screening, each retained improvement becomes the next baseline and
must continue through the unexhausted experiment backlog. Balanced orders and
same-arm controls are evidence gates during iteration. A 6–12 hour steady run
belongs to a separate stability phase and requires explicit user authorization;
it is not an automatic promotion or an indication that optimization is done.

## Framework refinements

The production oracle is a production-faithful but depleting shared state, so it
must not be used directly for sequential causal arms. Screen2 validates the
replacement framework on both hosts: freeze once, give every block a private
append-only overlay, retain normal within-run deduplication, and compare every
arm against the identical pre-existing known set. Balanced order remains
necessary because DHT supply and host-visible rate still change with wall time.

The benchmark sink and paired controller implement the read-only baseline plus
per-block overlays with backward-compatible shared-file behavior. The isolated
controller owns the sink lifecycle, hashes every baseline/overlay/manifest, and
preserves overlays without merging by default. An explicit `--finalize-oracle`
performs a validated temporary-file merge and refuses if production changed
after the freeze. Screen2 deliberately did not invoke it; future optimization
screens inherit that non-finalizing default.

Protocol review also changes one priority: BEP 51 explicitly supports surveying
the DHT with `sample_infohashes` and an advertised sampling interval. The current
strict-priority queue starves that source whenever passive `get_peers` traffic is
abundant. It should be tested through an explicit weighted capacity share, not
enabled as an unobservable bundle.
