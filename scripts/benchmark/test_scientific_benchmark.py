import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from scientific_benchmark import (  # noqa: E402
    ContractError,
    all_artifact_hashes,
    controller_command,
    expected_plan,
    node_content_digest,
    parse_proc_ports,
    result_gates,
    runtime_rows,
    sampler_event_gates,
    sha256,
    socket_udp,
    validate_prereg,
    validate_run_manifest,
)


def prereg_fixture(tmp_path: Path) -> dict:
    runtime = tmp_path / "runtime"
    diag = runtime / "bench" / "diagnostics" / "fixture"
    oracle_experiment = runtime / "bench" / "oracle-experiments" / "fixture"
    node_dir = runtime / "state" / "nodes" / "test-region"
    diag.mkdir(parents=True)
    node_dir.mkdir(parents=True)
    for index in range(96):
        (node_dir / f"node-{index:03d}").write_bytes(index.to_bytes(2, "big"))

    artifact_names = (
        "framework", "start", "launcher", "sampler", "supervisor", "runner",
        "binary_a", "binary_b", "sink_binary", "config_a", "config_b", "oracle_source",
    )
    artifacts = {}
    for name in artifact_names:
        path = runtime / "artifacts" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fixture-{name}\n", encoding="utf-8")
        artifacts[name] = {"path": str(path), "sha256": sha256(path)}

    _, node_hash = node_content_digest(node_dir)
    return {
        "schema_version": 3,
        "status": "pre_registered",
        "identity": {
            "experiment": "fixture-experiment",
            "label": "fixture-region-x",
            "region": "region-x",
            "host": "192.0.2.10",
        },
        "schedule": {
            "start_epoch": 4_102_444_800,
            "start_utc": "2100-01-01T00:00:00Z",
            "minimum_submission_lead_seconds": 300,
        },
        "paths": {
            "runtime_root": str(runtime),
            "oracle_experiment_dir": str(oracle_experiment),
            "diagnostic_dir": str(diag),
            "launch_receipt": str(diag / "launch-receipt.json"),
            "node_dir": str(node_dir),
            "crawler_pid_file": str(runtime / "run" / "crawler.pid"),
        },
        "artifacts": artifacts,
        "run": {
            "cohort": "region-x-v9",
            "sink_url": "http://127.0.0.1:5080/api/v1/torrents/batch",
            "environment": {"GOMAXPROCS": "2"},
        },
        "network": {
            "udp_port_start": 21000,
            "udp_port_end": 21095,
            "sink_tcp_port": 5080,
            "node_ids": 96,
        },
        "design": {
            "seed": 1,
            "plan": "AB",
            "blocks": 2,
            "pairing": [[1, 2]],
            "warmup_seconds_per_block": 600,
            "measure_seconds_per_block": 300,
        },
        "arms": {
            "A": {"overrides": ["discovery.lookup_rate=300", "instance_id=region-x-v9"]},
            "B": {"overrides": ["discovery.lookup_rate=150", "instance_id=region-x-v9"]},
        },
        "node_content_set_sha256": node_hash,
        "firewall": {
            "ufw": {"command": ["sudo", "-n", "ufw", "status", "verbose"], "sha256": "a" * 64},
            "raw": {"command": ["sudo", "-n", "iptables", "-t", "raw", "-S"], "sha256": "b" * 64},
        },
        "preflight": {"exclusive_process_names": ["cherry-picker", "benchmark-sink"]},
        "validity": {
            "sample_interval_seconds": 10,
            "sampler_tail_seconds": 60,
            "owner_grace_seconds": 30,
            "controller_done_grace_seconds": 60,
            "owned_socket_count": 96,
            "rss_max_mb": 3481.6,
            "cpu_max_percent": 190,
            "cpu_consecutive_samples": 3,
            "internal_drop_ratio_max": 0.01,
            "internal_drop_consecutive_windows": 2,
            "kernel_immediate_ratio": 0.001,
            "kernel_min_loss": 32,
            "kernel_conditioned_ratio": 0.0001,
            "kernel_two_window_loss": 32,
            "qdisc_drops_per_active_minute": 200,
            "announce_queued_min": 50_000,
            "announce_first_second_ratio": [0.70, 1.30],
            "exposure_checks": [
                {"name": "lookup", "result_path": "discovery.active_lookup_sent", "b_a_range": [0.42, 0.58]},
                {"name": "refresh", "result_path": "discovery.refresh_queries", "b_a_range": [0.90, 1.10]},
            ],
        },
    }


def audit_fixture() -> dict:
    path = Path(__file__).parent / "testdata" / "scientific_benchmark_audit_fixture_v3.json"
    return json.loads(path.read_text(encoding="utf-8"))


def runtime_line(announce: int = 10_000) -> str:
    return audit_fixture()["runtime_line"].replace("ann_q=10000", f"ann_q={announce}")


def good_result() -> dict:
    return audit_fixture()["result"]


def test_schema_drives_paths_region_plan_and_overrides(tmp_path: Path):
    prereg = prereg_fixture(tmp_path)
    validate_prereg(prereg)
    command = controller_command(prereg)
    joined = "\n".join(command)
    assert str(tmp_path / "runtime") in joined
    assert "fixture-experiment" in command
    assert "region-x-v9" in joined
    assert "/home/ubuntu/cherry" not in joined
    assert expected_plan(1, 4) == "ABBA"
    assert expected_plan(2, 4) == "BAAB"


def test_schema_rejects_order_or_socket_contract_drift(tmp_path: Path):
    prereg = prereg_fixture(tmp_path)
    prereg["design"]["plan"] = "BA"
    with pytest.raises(ContractError, match="seed"):
        validate_prereg(prereg)
    prereg["design"]["plan"] = "AB"
    prereg["validity"]["owned_socket_count"] = 95
    with pytest.raises(ContractError, match="owned_socket_count"):
        validate_prereg(prereg)


def test_udp_and_udp6_are_combined_but_tcp_remains_separate(tmp_path: Path):
    header = "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode"
    udp4 = tmp_path / "udp"
    udp6 = tmp_path / "udp6"
    udp4.write_text(header + "\n 1: 00000000:5208 00000000:0000 07 00000001:00000002 00:0 0 0 0 0 0 3\n")
    udp6.write_text(header + "\n 2: 00000000000000000000000000000000:5209 00000000:0000 07 00000004:00000005 00:0 0 0 0 0 0 7\n")
    count, rxq, txq, drops = socket_udp(21000, 96, (udp4, udp6))
    assert (count, rxq, txq, drops) == (2, 7, 5, 10)

    tcp_text = header + "\n 3: 0100007F:13D8 00000000:0000 0A 00000000:00000000\n"
    assert 5080 in parse_proc_ports(tcp_text)
    assert 5080 not in parse_proc_ports(udp4.read_text())


def test_final_runtime_line_is_consumed_before_exit_decision(tmp_path: Path):
    log = tmp_path / "crawler.log"
    log.write_text("\n".join(runtime_line() for _ in range(29)) + "\n", encoding="utf-8")
    assert len(runtime_rows(log)) == 29
    with log.open("a", encoding="utf-8") as output:
        output.write(runtime_line() + "\n")
    # This is the exact re-read performed after the owned PID disappears.
    assert len(runtime_rows(log)) == 30


def test_full_measurement_result_and_lifecycle_fixture_pass(tmp_path: Path):
    prereg = prereg_fixture(tmp_path)
    crawler_log = tmp_path / "crawler.log"
    crawler_log.write_text("\n".join(runtime_line() for _ in range(30)) + "\n", encoding="utf-8")
    gates, summary = result_gates(prereg, "fixture", good_result(), crawler_log)
    assert gates == []
    assert summary["measurement_rows"] == 10
    assert summary["announce_first_second_ratio"] == 1.0

    events = tmp_path / "events.jsonl"
    lifecycle = audit_fixture()["sampler_lifecycle"]
    events.write_text("\n".join(json.dumps(row) for row in lifecycle) + "\n", encoding="utf-8")
    assert sampler_event_gates(events) == []


def test_run_manifest_binds_binary_config_node_baseline_and_arm(tmp_path: Path):
    prereg = prereg_fixture(tmp_path)
    manifest = {
        "variant": "A",
        "binary_sha": prereg["artifacts"]["binary_a"]["sha256"],
        "template_config_sha": prereg["artifacts"]["config_a"]["sha256"],
        "node_dir": prereg["paths"]["node_dir"],
        "port": "21000",
        "oracle_mode": "isolated",
        "oracle_baseline_sha": prereg["artifacts"]["oracle_source"]["sha256"],
        "overrides": prereg["arms"]["A"]["overrides"],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    assert validate_run_manifest(prereg, "A", path) == []
    manifest["template_config_sha"] = "0" * 64
    path.write_text(json.dumps(manifest), encoding="utf-8")
    gates = validate_run_manifest(prereg, "A", path)
    assert any(gate.get("field") == "template_config_sha" for gate in gates)


def test_every_hash_bound_artifact_is_rechecked(tmp_path: Path):
    prereg = prereg_fixture(tmp_path)
    assert all_artifact_hashes(prereg)[0] == []
    Path(prereg["artifacts"]["runner"]["path"]).write_text("mutated\n", encoding="utf-8")
    errors, observed = all_artifact_hashes(prereg)
    assert any("runner" in error for error in errors)
    assert observed["runner"] != prereg["artifacts"]["runner"]["sha256"]


def test_json_schema_file_is_valid_json():
    schema = json.loads((Path(__file__).parent / "scientific_benchmark_prereg.schema.json").read_text())
    assert schema["properties"]["schema_version"]["const"] == 3
