# Scientific crawler benchmark runner

This directory contains the fail-closed sidecar used for formal crawler
experiments. It turns one immutable preregistration into an epoch-aligned
controller, an owned-process sampler, a block supervisor and a signed summary.
The region, experiment name, runtime root, arm order, ports, thresholds and all
artifact paths come from the preregistration; none are deployment constants in
the runner.

The framework is intentionally stricter than an engineering smoke run. A
completed crawler run is formally eligible only when every sampler, result,
manifest, exposure and final-integrity gate is empty. A failed gate preserves
the artifacts but excludes the run from retain/eliminate decisions.

## Files

- `scientific_benchmark_prereg.schema.json` is the v3 machine-readable schema.
- `scientific_benchmark.py` implements the common contract.
- `scientific_benchmark_{start,launcher,sampler,supervisor}.py` are the four
  executable, independently hash-bound entry points.
- `testdata/scientific_benchmark_audit_fixture_v3.json` is a compact eligible
  block/lifecycle fixture; `test_scientific_benchmark.py` also builds an
  offline 96-node fixture and checks
  protocol separation, order, hashes, lifecycle, result gates and the final
  telemetry-line race.

The original v2 experiment artifacts remain immutable. Do not rewrite their
preregistrations as v3 or combine a run missing this sidecar with formal data.

## Preregistration workflow

1. Select immutable binaries, configs, the controller runner, oracle baseline,
   node identity directory and these five framework files. Install executable
   entry points with mode `0755`.
2. Create a v3 JSON document matching the schema. Use an absolute path for
   every path. `artifacts` must contain `framework`, all four entry points,
   `runner`, `binary_a`, `binary_b`, `sink_binary`, `config_a`, `config_b` and
   `oracle_source`, each with its SHA-256.
3. Record the node content-set digest produced by
   `scientific_benchmark.node_content_digest`. This digest intentionally binds
   both content and resolved paths to match the audited v2 experiments.
4. Record exact stdout SHA-256 values for the two preregistered firewall argv
   probes. UFW and raw-table output are different contracts; do not normalize
   them after preregistration.
5. Choose an even block count. An odd seed implies `AB`, then `BA`; an even
   seed implies `BA`, then `AB`. `design.plan` and `design.pairing` are checked
   independently before any process starts.
6. Create the empty diagnostic directory, ensure the oracle experiment
   directory and launch receipt do not exist, then run preflight.

Example (paths are illustrative):

```bash
python3 /opt/cherry/scripts/benchmark/scientific_benchmark_start.py \
  --prereg /home/ubuntu/cherry/prereg/lookup-rate-stage1-sg.json \
  --launch /opt/cherry/scripts/benchmark/scientific_benchmark_launcher.py \
  --preflight-only
```

Submit the exact same files without `--preflight-only` while the minimum lead
time still holds:

```bash
python3 /opt/cherry/scripts/benchmark/scientific_benchmark_start.py \
  --prereg /home/ubuntu/cherry/prereg/lookup-rate-stage1-sg.json \
  --launch /opt/cherry/scripts/benchmark/scientific_benchmark_launcher.py
```

The start command writes one launch receipt containing the preregistration
hash, observed artifact hashes, node digest, firewall audit, PIDs and Linux
process start ticks. The launcher then waits for the registered epoch and
`exec`s the hash-bound controller with `CHERRY_BENCH_ROOT` from `paths`.

The controller runner must implement the existing `run-crawler-abab` contract,
including `--port` and isolated-oracle arguments. It must emit `RUN
run_id=<UTC-prefix>...` and `DONE run_id=...` records and the usual run
`manifest.json`, `crawler.log` and `result.json` artifacts. The run id begins
with `%Y%m%dT%H%M%SZ`; the sampler uses that timestamp as its absolute clock.

## Required audit behavior

For every block, the supervisor enforces all of the following:

- the sampler acquires the PID from the preregistered crawler pidfile within
  the grace period and verifies the exact arm binary SHA;
- every active sample has the exact preregistered socket count (96 for the
  current 2C4G profile) across `/proc/net/udp` and `/proc/net/udp6`;
- TCP/TCP6 sink occupancy is checked separately from UDP/UDP6 port occupancy,
  so port 5080 cannot be misclassified as a DHT socket;
- full-run 30-second runtime rows, 10-second host samples and measurement-only
  result/oracle rows are all complete;
- the final runtime row is reread for one second after owned-process exit before
  an early-exit gate is emitted;
- crawler pause, wire queue loss, internal DHT loss, kernel/socket loss, RSS,
  CPU, qdisc, softnet and netdev gates use numeric preregistered thresholds;
- qdisc is a rate gate, not a zero-growth gate: both the 10-second sidecar and
  `result.json` compare cumulative growth with
  `qdisc_drops_per_active_minute * observed_seconds / 60`. The result-side
  observed duration is `(monitor_samples - 1) *
  result_counter_sample_interval_seconds`; missing counters or duration fail
  closed, while a value exactly on the boundary remains eligible;
- the run manifest binds the expected arm, binary, template config, overrides,
  node path, UDP base port and frozen baseline hash;
- every artifact hash, the node content-set digest and the read-only production
  oracle hash are rechecked after every block and at the end;
- every registered pair contains A and B and passes each schema-defined B/A
  exposure check;
- the isolated oracle manifest is `completed`, explicitly `finalized: false`,
  and its frozen baseline hash equals the preregistered production-oracle hash.

`supervisor-summary.json` is atomically replaced and accompanied by a SHA-256
sidecar. Per-block identity, sampler CSV, JSONL events, logs and hashes remain in
the diagnostic directory. An interrupted or gated run is never silently
resumed in the same paths: preflight rejects existing block/oracle artifacts.

## Failure and rollback

The sampler sends SIGTERM only when the currently running PID still matches the
preregistered crawler pidfile it acquired. The supervisor terminates only its
owned controller; the controller's cleanup owns its runner, crawler and sink.
It never kills processes by name during rollback, rotates node identities,
merges overlays or finalizes the production oracle.

After a failure, preserve all artifacts, verify UDP/UDP6 experiment ports and
the TCP/TCP6 sink port are free, identify the failed gate, and preregister a new
epoch/path. Do not edit a submitted preregistration or reuse its directories.

Run the offline contract tests with:

```bash
python3 -m pytest -q scripts/benchmark/test_scientific_benchmark.py
```
