#!/usr/bin/env python3
"""Fail-closed orchestration for preregistered crawler experiments.

The four thin entry points next to this module deliberately share this code so
that schema validation, ownership and audit semantics cannot drift apart.
Only Linux execution is supported; pure parsing and validation helpers remain
portable so the contract can be tested offline.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = 3
RUNTIME_PERIOD_SECONDS = 30


class ContractError(ValueError):
    """The preregistration is incomplete or internally inconsistent."""


def utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def nested(value: Mapping[str, Any], dotted: str) -> Any:
    current: Any = value
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            raise ContractError(f"missing preregistration field: {dotted}")
        current = current[part]
    return current


def artifact(prereg: Mapping[str, Any], name: str) -> tuple[Path, str]:
    item = nested(prereg, f"artifacts.{name}")
    if not isinstance(item, Mapping):
        raise ContractError(f"artifacts.{name} must be an object")
    path = Path(str(nested(item, "path")))
    digest = str(nested(item, "sha256"))
    if not path.is_absolute():
        raise ContractError(f"artifacts.{name}.path must be absolute")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ContractError(f"artifacts.{name}.sha256 must be lowercase SHA-256")
    return path, digest


def node_content_digest(node_dir: Path) -> tuple[int, str]:
    """Hash file content plus resolved paths, matching the audited v2 runs."""
    paths = sorted(path.resolve() for path in node_dir.rglob("*") if path.is_file())
    content = b"".join(f"{sha256(path)}  {path}\n".encode("utf-8") for path in paths)
    return len(paths), bytes_sha256(content)


def expected_plan(seed: int, blocks: int) -> str:
    first_pair = "AB" if seed % 2 else "BA"
    plan = ""
    for block in range(1, blocks + 1):
        pair = (block - 1) // 2
        order = first_pair if pair % 2 == 0 else first_pair[::-1]
        plan += order[0] if block % 2 else order[1]
    return plan


def validate_prereg(prereg: Mapping[str, Any]) -> None:
    required = (
        "schema_version", "status", "identity.experiment", "identity.label",
        "identity.region", "identity.host", "schedule.start_epoch",
        "schedule.start_utc", "schedule.minimum_submission_lead_seconds",
        "paths.runtime_root", "paths.oracle_experiment_dir", "paths.diagnostic_dir",
        "paths.launch_receipt", "paths.node_dir", "paths.crawler_pid_file",
        "run.cohort", "run.sink_url", "network.udp_port_start",
        "network.udp_port_end", "network.sink_tcp_port", "network.node_ids",
        "design.seed", "design.plan", "design.blocks", "design.pairing",
        "design.warmup_seconds_per_block", "design.measure_seconds_per_block",
        "arms.A.overrides", "arms.B.overrides", "node_content_set_sha256",
        "validity.sampler_tail_seconds", "validity.owner_grace_seconds",
        "validity.controller_done_grace_seconds",
        "validity.owned_socket_count", "validity.rss_max_mb",
        "validity.cpu_max_percent", "validity.cpu_consecutive_samples",
        "validity.internal_drop_ratio_max", "validity.internal_drop_consecutive_windows",
        "validity.kernel_immediate_ratio", "validity.kernel_min_loss",
        "validity.kernel_conditioned_ratio", "validity.kernel_two_window_loss",
        "validity.qdisc_drops_per_active_minute",
        "validity.result_counter_sample_interval_seconds", "validity.announce_queued_min",
        "validity.announce_first_second_ratio", "validity.exposure_checks",
        "firewall.ufw.command", "firewall.ufw.sha256",
        "firewall.raw.command", "firewall.raw.sha256",
        "preflight.exclusive_process_names",
    )
    for field in required:
        nested(prereg, field)
    if int(prereg["schema_version"]) != SCHEMA_VERSION:
        raise ContractError(f"schema_version must be {SCHEMA_VERSION}")
    if prereg["status"] != "pre_registered":
        raise ContractError("status must be pre_registered")
    label = str(nested(prereg, "identity.label"))
    if not re.fullmatch(r"[A-Za-z0-9._-]+", label):
        raise ContractError("identity.label must contain only A-Z, a-z, 0-9, dot, underscore or dash")
    for name in (
        "runtime_root", "oracle_experiment_dir", "diagnostic_dir", "launch_receipt",
        "node_dir", "crawler_pid_file",
    ):
        if not Path(str(nested(prereg, f"paths.{name}"))).is_absolute():
            raise ContractError(f"paths.{name} must be absolute")

    for name in (
        "framework", "start", "launcher", "sampler", "supervisor", "runner",
        "binary_a", "binary_b", "sink_binary", "config_a", "config_b", "oracle_source",
    ):
        artifact(prereg, name)
    artifacts = nested(prereg, "artifacts")
    if not isinstance(artifacts, Mapping):
        raise ContractError("artifacts must be an object")
    for name in artifacts:
        artifact(prereg, str(name))

    target_epoch = int(nested(prereg, "schedule.start_epoch"))
    start_utc = str(nested(prereg, "schedule.start_utc"))
    try:
        parsed_start = datetime.datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
    except ValueError as error:
        raise ContractError("schedule.start_utc must be an ISO-8601 timestamp") from error
    if parsed_start.tzinfo is None or int(parsed_start.timestamp()) != target_epoch:
        raise ContractError("schedule.start_utc and schedule.start_epoch must identify the same UTC second")
    if int(nested(prereg, "schedule.minimum_submission_lead_seconds")) <= 0:
        raise ContractError("minimum submission lead must be positive")

    blocks = int(nested(prereg, "design.blocks"))
    seed = int(nested(prereg, "design.seed"))
    plan = str(nested(prereg, "design.plan"))
    if blocks < 2 or blocks % 2:
        raise ContractError("design.blocks must be an even integer >= 2")
    if plan != expected_plan(seed, blocks):
        raise ContractError(f"design.plan does not match seed: expected {expected_plan(seed, blocks)}")
    pairing = nested(prereg, "design.pairing")
    flat: list[int] = []
    if not isinstance(pairing, list):
        raise ContractError("design.pairing must be an array")
    for pair in pairing:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ContractError("every design.pairing entry must contain two block numbers")
        left, right = int(pair[0]), int(pair[1])
        if not (1 <= left <= blocks and 1 <= right <= blocks):
            raise ContractError(f"pair {pair} references a block outside 1..{blocks}")
        if {plan[left - 1], plan[right - 1]} != {"A", "B"}:
            raise ContractError(f"pair {pair} must contain one A and one B")
        flat.extend((left, right))
    if sorted(flat) != list(range(1, blocks + 1)):
        raise ContractError("design.pairing must cover every block exactly once")

    warmup = int(nested(prereg, "design.warmup_seconds_per_block"))
    measure = int(nested(prereg, "design.measure_seconds_per_block"))
    if warmup <= 0 or measure <= 0 or warmup % RUNTIME_PERIOD_SECONDS or measure % RUNTIME_PERIOD_SECONDS:
        raise ContractError("warmup and measure must be positive multiples of 30 seconds")
    if (measure // RUNTIME_PERIOD_SECONDS) % 2:
        raise ContractError("measurement must contain an even number of runtime windows")
    node_ids = int(nested(prereg, "network.node_ids"))
    port_start = int(nested(prereg, "network.udp_port_start"))
    port_end = int(nested(prereg, "network.udp_port_end"))
    sink_port = int(nested(prereg, "network.sink_tcp_port"))
    if not (1 <= port_start <= 65535 and 1 <= port_end <= 65535 and 1 <= sink_port <= 65535):
        raise ContractError("all network ports must be within 1..65535")
    if node_ids <= 0 or port_end - port_start + 1 != node_ids:
        raise ContractError("UDP port range must contain exactly network.node_ids ports")
    if int(nested(prereg, "validity.owned_socket_count")) != node_ids:
        raise ContractError("validity.owned_socket_count must equal network.node_ids")
    node_digest = str(nested(prereg, "node_content_set_sha256"))
    if not re.fullmatch(r"[0-9a-f]{64}", node_digest):
        raise ContractError("node_content_set_sha256 must be lowercase SHA-256")
    sink_url = str(nested(prereg, "run.sink_url"))
    sink_match = re.fullmatch(r"http://127\.0\.0\.1:(\d+)/.+", sink_url)
    if not sink_match or int(sink_match.group(1)) != int(nested(prereg, "network.sink_tcp_port")):
        raise ContractError("run.sink_url must use the preregistered loopback sink TCP port")
    for arm in ("A", "B"):
        overrides = nested(prereg, f"arms.{arm}.overrides")
        if not isinstance(overrides, list) or not all(isinstance(item, str) for item in overrides):
            raise ContractError(f"arms.{arm}.overrides must be an array of strings")
    environment = nested(prereg, "run").get("environment", {})
    if not isinstance(environment, Mapping):
        raise ContractError("run.environment must be an object")
    for firewall in ("ufw", "raw"):
        command = nested(prereg, f"firewall.{firewall}.command")
        digest = str(nested(prereg, f"firewall.{firewall}.sha256"))
        if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
            raise ContractError(f"firewall.{firewall}.command must be a non-empty argv array")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ContractError(f"firewall.{firewall}.sha256 must be lowercase SHA-256")
    checks = nested(prereg, "validity.exposure_checks")
    if not isinstance(checks, list) or not checks:
        raise ContractError("validity.exposure_checks must be non-empty")
    for check in checks:
        if not isinstance(check, Mapping):
            raise ContractError("exposure check must be an object")
        nested(check, "name")
        nested(check, "result_path")
        limits = nested(check, "b_a_range")
        if not isinstance(limits, list) or len(limits) != 2:
            raise ContractError("exposure check b_a_range must contain [low, high]")
        if float(limits[0]) > float(limits[1]):
            raise ContractError("exposure check b_a_range must be ordered low to high")
    announce_limits = nested(prereg, "validity.announce_first_second_ratio")
    if not isinstance(announce_limits, list) or len(announce_limits) != 2:
        raise ContractError("announce_first_second_ratio must contain [low, high]")
    if float(announce_limits[0]) > float(announce_limits[1]):
        raise ContractError("announce_first_second_ratio must be ordered low to high")
    positive_fields = (
        "sampler_tail_seconds", "owner_grace_seconds", "controller_done_grace_seconds",
        "rss_max_mb", "cpu_max_percent", "cpu_consecutive_samples",
        "internal_drop_consecutive_windows", "kernel_min_loss", "kernel_two_window_loss",
        "result_counter_sample_interval_seconds",
    )
    for field in positive_fields:
        if float(nested(prereg, f"validity.{field}")) <= 0:
            raise ContractError(f"validity.{field} must be positive")
    if int(nested(prereg, "validity.qdisc_drops_per_active_minute")) < 0:
        raise ContractError("qdisc drops per active minute cannot be negative")
    if int(nested(prereg, "validity.announce_queued_min")) < 0:
        raise ContractError("announce_queued_min cannot be negative")
    if int(nested(prereg, "validity").get("sample_interval_seconds", 10)) <= 0:
        raise ContractError("sample_interval_seconds must be positive")
    for field in ("internal_drop_ratio_max", "kernel_immediate_ratio", "kernel_conditioned_ratio"):
        value = float(nested(prereg, f"validity.{field}"))
        if not 0 <= value <= 1:
            raise ContractError(f"validity.{field} must be within 0..1")


def load_prereg(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError("preregistration root must be an object")
    validate_prereg(value)
    return value


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def pidfile_alive(path: Path) -> bool:
    try:
        return process_alive(int(path.read_text().strip()))
    except Exception:
        return False


def terminate_pidfile(pid_file: Path) -> None:
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass


def proc_start_ticks(pid: int) -> int:
    return int(Path(f"/proc/{pid}/stat").read_text().split()[21])


def command(command_line: Sequence[str], timeout: int = 15) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command_line, capture_output=True, check=False, timeout=timeout)


def parse_proc_ports(text: str) -> set[int]:
    ports: set[int] = set()
    for line in text.splitlines()[1:]:
        try:
            ports.add(int(line.split()[1].split(":")[1], 16))
        except (IndexError, ValueError):
            pass
    return ports


def occupied_ports(proc_files: Iterable[Path]) -> set[int]:
    ports: set[int] = set()
    for path in proc_files:
        try:
            ports.update(parse_proc_ports(path.read_text()))
        except OSError:
            pass
    return ports


def pid_files(prereg: Mapping[str, Any]) -> tuple[Path, Path]:
    root = Path(str(nested(prereg, "paths.runtime_root"))) / "run"
    label = str(nested(prereg, "identity.label"))
    return root / f"{label}.controller.pid", root / f"{label}.supervisor.pid"


def all_artifact_hashes(prereg: Mapping[str, Any]) -> tuple[list[str], dict[str, str | None]]:
    errors: list[str] = []
    observed: dict[str, str | None] = {}
    for name, item in nested(prereg, "artifacts").items():
        if not isinstance(item, Mapping) or "path" not in item or "sha256" not in item:
            errors.append(f"invalid artifact entry: {name}")
            continue
        path, expected = artifact(prereg, name)
        try:
            actual = sha256(path)
        except OSError as error:
            errors.append(f"unreadable hash-bound artifact {name} ({path}): {error}")
            observed[name] = None
            continue
        observed[name] = actual
        if actual != expected:
            errors.append(f"artifact hash mismatch {name}: expected {expected}, got {actual}")
    return errors, observed


def preflight(prereg: Mapping[str, Any], launch_path: Path, start_path: Path) -> dict[str, Any]:
    errors, observed_hashes = all_artifact_hashes(prereg)
    target = int(nested(prereg, "schedule.start_epoch"))
    lead = target - time.time()
    minimum_lead = int(nested(prereg, "schedule.minimum_submission_lead_seconds"))
    if lead < minimum_lead:
        errors.append(f"insufficient lead: {lead:.3f}s < {minimum_lead}s")

    expected_launch, _ = artifact(prereg, "launcher")
    expected_start, _ = artifact(prereg, "start")
    expected_framework, _ = artifact(prereg, "framework")
    if expected_framework.resolve() != Path(__file__).resolve():
        errors.append("loaded framework module does not match preregistered framework artifact")
    if launch_path.resolve() != expected_launch.resolve():
        errors.append("--launch does not match preregistered launcher artifact")
    if start_path.resolve() != expected_start.resolve():
        errors.append("running start entry point does not match preregistered start artifact")

    node_dir = Path(str(nested(prereg, "paths.node_dir")))
    try:
        node_count, node_digest = node_content_digest(node_dir)
    except OSError as error:
        node_count, node_digest = 0, ""
        errors.append(f"cannot hash node identity set: {error}")
    node_ids = int(nested(prereg, "network.node_ids"))
    if node_count != node_ids:
        errors.append(f"node count mismatch: expected {node_ids}, got {node_count}")
    if node_digest != nested(prereg, "node_content_set_sha256"):
        errors.append(
            "node digest mismatch: expected "
            f"{nested(prereg, 'node_content_set_sha256')}, got {node_digest}"
        )

    for process_name in nested(prereg, "preflight.exclusive_process_names"):
        found = command(["pgrep", "-x", str(process_name)])
        if found.returncode == 0:
            errors.append(f"active {process_name}: {found.stdout.decode(errors='replace').strip()}")

    # UDP/UDP6 and TCP/TCP6 are deliberately read separately. A TCP listener
    # on the sink port must never be mistaken for a crawler UDP conflict.
    udp_ports = occupied_ports((Path("/proc/net/udp"), Path("/proc/net/udp6")))
    tcp_ports = occupied_ports((Path("/proc/net/tcp"), Path("/proc/net/tcp6")))
    port_start = int(nested(prereg, "network.udp_port_start"))
    port_end = int(nested(prereg, "network.udp_port_end"))
    sink_port = int(nested(prereg, "network.sink_tcp_port"))
    udp_conflicts = sorted(set(range(port_start, port_end + 1)) & udp_ports)
    tcp_conflict = sink_port in tcp_ports
    if udp_conflicts:
        errors.append(f"experiment UDP/UDP6 ports occupied: {udp_conflicts}")
    if tcp_conflict:
        errors.append(f"sink TCP/TCP6 port occupied: {sink_port}")

    oracle_dir = Path(str(nested(prereg, "paths.oracle_experiment_dir")))
    diag_dir = Path(str(nested(prereg, "paths.diagnostic_dir")))
    receipt_path = Path(str(nested(prereg, "paths.launch_receipt")))
    if oracle_dir.exists():
        errors.append(f"oracle experiment dir already exists: {oracle_dir}")
    if not diag_dir.exists():
        errors.append(f"diagnostic dir does not exist: {diag_dir}")
    else:
        try:
            if any(path.name.startswith("block-") for path in diag_dir.iterdir()):
                errors.append(f"diagnostic block artifacts already exist: {diag_dir}")
        except OSError as error:
            errors.append(f"cannot inspect diagnostic dir {diag_dir}: {error}")
    if receipt_path.exists():
        errors.append(f"launch receipt already exists: {receipt_path}")

    controller_pid_file, supervisor_pid_file = pid_files(prereg)
    if pidfile_alive(controller_pid_file):
        errors.append(f"active controller pidfile: {controller_pid_file}")
    if pidfile_alive(supervisor_pid_file):
        errors.append(f"active supervisor pidfile: {supervisor_pid_file}")

    firewall_observed: dict[str, str] = {}
    for name in ("ufw", "raw"):
        argv = [str(item) for item in nested(prereg, f"firewall.{name}.command")]
        completed = command(argv)
        if completed.returncode != 0:
            errors.append(
                f"cannot audit firewall {name}: "
                f"{completed.stderr.decode(errors='replace').strip()}"
            )
        digest = bytes_sha256(completed.stdout)
        firewall_observed[name] = digest
        expected = str(nested(prereg, f"firewall.{name}.sha256"))
        if digest != expected:
            errors.append(f"firewall {name} hash mismatch: expected {expected}, got {digest}")

    return {
        "schema_version": 2,
        "status": "passed" if not errors else "failed",
        "checked_at": utc(),
        "host": nested(prereg, "identity.host"),
        "region": nested(prereg, "identity.region"),
        "label": nested(prereg, "identity.label"),
        "target_epoch": target,
        "lead_seconds": lead,
        "errors": errors,
        "observed_artifact_hashes": observed_hashes,
        "node_count": node_count,
        "node_content_set_sha256": node_digest,
        "firewall_hashes": firewall_observed,
        "udp_conflicts": udp_conflicts,
        "tcp_sink_conflict": tcp_conflict,
    }


def start_main(entry_path: Path | None = None, argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Preflight and submit a preregistered experiment")
    parser.add_argument("--prereg", required=True, type=Path)
    parser.add_argument("--launch", required=True, type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args(argv)
    prereg = load_prereg(args.prereg)
    start_path = (entry_path or Path(sys.argv[0])).resolve()
    report = preflight(prereg, args.launch.resolve(), start_path)
    print(json.dumps(report, sort_keys=True))
    if report["status"] != "passed":
        return 1
    if args.preflight_only:
        return 0

    diag_dir = Path(str(nested(prereg, "paths.diagnostic_dir")))
    controller_pid_file, supervisor_pid_file = pid_files(prereg)
    controller_log = diag_dir / "controller.log"
    supervisor_log = diag_dir / "supervisor.log"
    supervisor_path, _ = artifact(prereg, "supervisor")

    controller: subprocess.Popen[bytes] | None = None
    supervisor: subprocess.Popen[bytes] | None = None
    try:
        with controller_log.open("ab", buffering=0) as output:
            controller = subprocess.Popen(
                [str(args.launch.resolve()), "--prereg", str(args.prereg.resolve())],
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        controller_pid_file.write_text(f"{controller.pid}\n", encoding="ascii")
        with supervisor_log.open("ab", buffering=0) as output:
            supervisor = subprocess.Popen(
                [str(supervisor_path), "--prereg", str(args.prereg.resolve()),
                 "--controller-pid-file", str(controller_pid_file)],
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        supervisor_pid_file.write_text(f"{supervisor.pid}\n", encoding="ascii")
        time.sleep(0.2)
        if not process_alive(controller.pid) or not process_alive(supervisor.pid):
            raise RuntimeError("controller or supervisor exited immediately")
    except Exception:
        for process in (supervisor, controller):
            if process and process_alive(process.pid):
                os.kill(process.pid, signal.SIGTERM)
        raise

    receipt = {
        "schema_version": 3,
        "status": "submitted",
        "receipt_id": str(uuid.uuid4()),
        "submitted_at": utc(),
        "host": nested(prereg, "identity.host"),
        "region": nested(prereg, "identity.region"),
        "label": nested(prereg, "identity.label"),
        "target_epoch": nested(prereg, "schedule.start_epoch"),
        "target_utc": nested(prereg, "schedule.start_utc"),
        "controller_pid": controller.pid,
        "controller_start_ticks": proc_start_ticks(controller.pid),
        "supervisor_pid": supervisor.pid,
        "supervisor_start_ticks": proc_start_ticks(supervisor.pid),
        "controller_pid_file": str(controller_pid_file),
        "supervisor_pid_file": str(supervisor_pid_file),
        "controller_log": str(controller_log),
        "supervisor_log": str(supervisor_log),
        "preregistration": str(args.prereg.resolve()),
        "preregistration_sha256": sha256(args.prereg.resolve()),
        "observed_artifact_hashes": report["observed_artifact_hashes"],
        "node_content_set_sha256": nested(prereg, "node_content_set_sha256"),
        "oracle_source_sha256": artifact(prereg, "oracle_source")[1],
        "udp_port_start": nested(prereg, "network.udp_port_start"),
        "udp_port_end": nested(prereg, "network.udp_port_end"),
        "sink_tcp_port": nested(prereg, "network.sink_tcp_port"),
        "preflight": report,
    }
    receipt_path = Path(str(nested(prereg, "paths.launch_receipt")))
    atomic_json(receipt_path, receipt)
    print(json.dumps(receipt, sort_keys=True))
    return 0


def controller_command(prereg: Mapping[str, Any]) -> list[str]:
    runner, _ = artifact(prereg, "runner")
    binary_a, _ = artifact(prereg, "binary_a")
    binary_b, _ = artifact(prereg, "binary_b")
    config_a, _ = artifact(prereg, "config_a")
    config_b, _ = artifact(prereg, "config_b")
    sink_binary, _ = artifact(prereg, "sink_binary")
    oracle_source, _ = artifact(prereg, "oracle_source")
    design = nested(prereg, "design")
    command_line = [
        str(runner),
        "--label", str(nested(prereg, "identity.label")),
        "--experiment", str(nested(prereg, "identity.experiment")),
        "--blocks", str(design["blocks"]),
        "--design", "balanced",
        "--seed", str(design["seed"]),
        "--warmup", f"{design['warmup_seconds_per_block']}s",
        "--measure", f"{design['measure_seconds_per_block']}s",
        "--cohort", str(nested(prereg, "run.cohort")),
        "--config-a", str(config_a), "--config-b", str(config_b),
        "--binary-a", str(binary_a), "--binary-b", str(binary_b),
        "--sink-binary", str(sink_binary),
        "--sink-url", str(nested(prereg, "run.sink_url")),
        "--port", str(nested(prereg, "network.udp_port_start")),
        "--oracle-mode", "isolated",
        "--production-oracle", str(oracle_source),
        "--oracle-experiment-dir", str(nested(prereg, "paths.oracle_experiment_dir")),
    ]
    for arm in ("A", "B"):
        for override in nested(prereg, f"arms.{arm}.overrides"):
            command_line.extend((f"--set-{arm.lower()}", str(override)))
    return command_line


def launcher_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Epoch-aligned preregistered controller launcher")
    parser.add_argument("--prereg", required=True, type=Path)
    args = parser.parse_args(argv)
    prereg = load_prereg(args.prereg)
    target = int(nested(prereg, "schedule.start_epoch"))
    while time.time() < target:
        time.sleep(min(0.2, target - time.time()))

    receipt_path = Path(str(nested(prereg, "paths.launch_receipt")))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    now = time.time()
    receipt.update(
        status="epoch_reached", epoch_reached_at=utc(),
        epoch_delta_seconds=now - target, preexec_pid=os.getpid(),
    )
    atomic_json(receipt_path, receipt)

    command_line = controller_command(prereg)
    environment = os.environ.copy()
    for key, value in nested(prereg, "run").get("environment", {}).items():
        environment[str(key)] = str(value)
    environment["CHERRY_BENCH_ROOT"] = str(nested(prereg, "paths.runtime_root"))
    os.execvpe(command_line[0], command_line, environment)
    return 127


def command_output(command_line: Sequence[str]) -> str:
    try:
        return subprocess.run(
            command_line, text=True, capture_output=True, timeout=3, check=False
        ).stdout
    except Exception:
        return ""


def default_netdev() -> str:
    route = command_output(["ip", "route", "show", "default"])
    for line in route.splitlines():
        fields = line.split()
        if "dev" in fields:
            index = fields.index("dev")
            if index + 1 < len(fields):
                return fields[index + 1]
    try:
        for path in sorted(Path("/sys/class/net").iterdir()):
            if path.name != "lo":
                return path.name
    except OSError:
        pass
    raise RuntimeError("no non-loopback network device found")


def softnet() -> tuple[int, int]:
    dropped = squeezed = 0
    try:
        for line in Path("/proc/net/softnet_stat").read_text().splitlines():
            fields = line.split()
            dropped += int(fields[1], 16)
            squeezed += int(fields[2], 16)
    except (OSError, ValueError, IndexError):
        pass
    return dropped, squeezed


def socket_udp(
    port_start: int,
    instances: int,
    proc_paths: Iterable[Path] = (Path("/proc/net/udp"), Path("/proc/net/udp6")),
) -> tuple[int, int, int, int]:
    """Count owned port-range sockets across both UDP protocol tables."""
    count = rxq = txq = drops = 0
    for proc_path in proc_paths:
        try:
            lines = proc_path.read_text().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            try:
                port = int(fields[1].split(":")[1], 16)
                if not port_start <= port < port_start + instances:
                    continue
                tx_hex, rx_hex = fields[4].split(":")
                txq += int(tx_hex, 16)
                rxq += int(rx_hex, 16)
                drops += int(fields[-1])
                count += 1
            except (ValueError, IndexError):
                continue
    return count, rxq, txq, drops


def qdisc(netdev: str) -> int:
    text = command_output(["tc", "-s", "qdisc", "show", "dev", netdev])
    root = False
    for line in text.splitlines():
        if line.startswith("qdisc "):
            root = " root " in line
            continue
        if root and " Sent " in line:
            match = re.search(r"\(dropped (\d+),", line)
            return int(match.group(1)) if match else 0
    return 0


def qdisc_allowance(drops_per_active_minute: float, active_seconds: float) -> float:
    """Return the preregistered cumulative allowance for an observed interval."""
    if drops_per_active_minute < 0 or active_seconds < 0:
        raise ValueError("qdisc rate and active duration must be non-negative")
    return drops_per_active_minute * active_seconds / 60.0


def netdev_drops(netdev: str) -> tuple[int, int]:
    try:
        for line in Path("/proc/net/dev").read_text().splitlines():
            if line.strip().startswith(f"{netdev}:"):
                fields = line.split(":", 1)[1].split()
                return int(fields[3]), int(fields[11])
    except (OSError, ValueError, IndexError):
        pass
    return 0, 0


def nstat() -> dict[str, int]:
    keys = (
        "IpInDiscards", "IpOutDiscards", "UdpInDatagrams", "UdpInErrors",
        "UdpRcvbufErrors", "UdpSndbufErrors",
    )
    output = {key: 0 for key in keys}
    for line in command_output(["nstat", "-az"]).splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] in output:
            try:
                output[fields[0]] = int(fields[1])
            except ValueError:
                pass
    return output


def owned_process(pid_file: Path) -> tuple[int, float, int, int, str, str]:
    try:
        pid = int(pid_file.read_text().strip())
    except Exception:
        return 0, 0.0, 0, 0, "", ""
    proc_root = Path(f"/proc/{pid}")
    if not proc_root.exists():
        return 0, 0.0, 0, 0, "", ""
    try:
        cpu = float(command_output(["ps", "-p", str(pid), "-o", "%cpu="]).strip() or 0)
    except ValueError:
        cpu = 0.0
    rss = threads = 0
    try:
        for line in (proc_root / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("Threads:"):
                threads = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    try:
        executable = os.readlink(proc_root / "exe")
        executable_sha = sha256(Path(executable))
    except OSError:
        executable = executable_sha = ""
    return pid, cpu, rss, threads, executable, executable_sha


RUNTIME_LINE = re.compile(
    r"dht_recv=(\d+) handled=(\d+) dropped=(\d+).*"
    r"paused=(true|false).*wire_q_drop=(\d+)"
)


def parse_runtime_line(line: str) -> dict[str, int | bool] | None:
    if "runtime 30s:" not in line:
        return None
    match = RUNTIME_LINE.search(line)
    if not match:
        raise ValueError("unparseable runtime window")
    return {
        "dht_recv": int(match[1]),
        "dht_handled": int(match[2]),
        "dht_dropped": int(match[3]),
        "paused": match[4] == "true",
        "wire_q_drop": int(match[5]),
    }


def runtime_rows(log_path: Path) -> list[dict[str, int | bool]]:
    rows: list[dict[str, int | bool]] = []
    for line in log_path.read_text(errors="replace").splitlines():
        parsed = parse_runtime_line(line)
        if parsed is not None:
            rows.append(parsed)
    return rows


def terminate_owned(pid: int, pid_file: Path) -> None:
    if not pid or not Path(f"/proc/{pid}").exists():
        return
    try:
        current = int(pid_file.read_text().strip())
    except Exception:
        current = 0
    if current == pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def sampler_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Owned crawler network/resource sampler")
    parser.add_argument("--prereg", required=True, type=Path)
    parser.add_argument("--diag-dir", required=True, type=Path)
    parser.add_argument("--target-epoch", type=int, required=True)
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--arm", required=True, choices=("A", "B"))
    parser.add_argument("--crawler-log", required=True, type=Path)
    parser.add_argument("--expected-runtime-windows", type=int, required=True)
    args = parser.parse_args(argv)
    prereg = load_prereg(args.prereg)
    validity = nested(prereg, "validity")
    port_start = int(nested(prereg, "network.udp_port_start"))
    instances = int(nested(prereg, "network.node_ids"))
    pid_file = Path(str(nested(prereg, "paths.crawler_pid_file")))
    expected_binary_sha = artifact(prereg, f"binary_{args.arm.lower()}")[1]

    args.diag_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.diag_dir / "host-network-10s.csv"
    events_path = args.diag_dir / "events.jsonl"
    netdev = default_netdev()

    def event(kind: str, **values: object) -> None:
        with events_path.open("a", encoding="utf-8") as output:
            output.write(json.dumps({"utc": utc(), "kind": kind, **values}, sort_keys=True) + "\n")

    fields = [
        "utc", "elapsed_s", "pid", "cpu_pct", "rss_kb", "threads",
        "udp_in_datagrams", "udp_in_errors", "udp_rcvbuf_errors", "udp_sndbuf_errors",
        "ip_in_discards", "ip_out_discards", "softnet_dropped", "softnet_squeezed",
        "qdisc_drops", "netdev_rx_drop", "netdev_tx_drop", "socket_count",
        "socket_rxq_bytes", "socket_txq_bytes", "socket_drops",
    ]
    while time.time() < args.target_epoch:
        time.sleep(min(0.2, args.target_epoch - time.time()))
    event(
        "sampler_start", target_epoch=args.target_epoch, duration=args.duration,
        label=args.run_label, port_start=port_start, port_end=port_start + instances - 1,
        instances=instances, expected_runtime_windows=args.expected_runtime_windows,
        netdev=netdev,
    )

    baseline: dict[str, Any] | None = None
    kernel_previous: dict[str, Any] | None = None
    recent_kernel: list[int] = []
    seen_runtime = consecutive_dht = high_cpu = 0
    owned_pid = 0
    failure = stopped = clean_end = False
    end_epoch = args.target_epoch + args.duration

    def gate(reason: str, **values: object) -> None:
        nonlocal failure, stopped
        event("gate", reason=reason, **values)
        failure = True
        stopped = True
        terminate_owned(owned_pid, pid_file)

    def consume_runtime() -> None:
        nonlocal seen_runtime, consecutive_dht
        if not args.crawler_log.exists():
            return
        try:
            rows = runtime_rows(args.crawler_log)
        except ValueError:
            gate("unparseable_runtime_window", runtime_index=seen_runtime + 1)
            return
        for row in rows[seen_runtime:]:
            received = int(row["dht_recv"])
            dropped = int(row["dht_dropped"])
            ratio = dropped / received if received else 0.0
            consecutive_dht = (
                consecutive_dht + 1
                if ratio > float(validity["internal_drop_ratio_max"])
                else 0
            )
            seen_runtime += 1
            event("runtime_window", runtime_index=seen_runtime, dht_drop_ratio=ratio, **row)
            if bool(row["paused"]) or int(row["wire_q_drop"]) > 0:
                gate("crawler_pause_or_wire_queue_drop", runtime_index=seen_runtime, **row)
            elif consecutive_dht >= int(validity["internal_drop_consecutive_windows"]):
                gate(
                    "consecutive_internal_drop_windows_over_limit",
                    runtime_index=seen_runtime, ratio=ratio, consecutive=consecutive_dht,
                )
            if seen_runtime > args.expected_runtime_windows:
                gate(
                    "too_many_runtime_windows", runtime_windows=seen_runtime,
                    expected=args.expected_runtime_windows,
                )

    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        output.flush()
        while time.time() <= end_epoch:
            elapsed = int(time.time() - args.target_epoch)
            pid, cpu, rss, threads, executable, executable_sha = owned_process(pid_file)
            ns = nstat()
            softnet_dropped, softnet_squeezed = softnet()
            qdisc_drops = qdisc(netdev)
            rx_drop, tx_drop = netdev_drops(netdev)
            socket_count, socket_rxq, socket_txq, socket_drops = socket_udp(port_start, instances)
            row: dict[str, Any] = {
                "utc": utc(), "elapsed_s": elapsed, "pid": pid, "cpu_pct": cpu,
                "rss_kb": rss, "threads": threads,
                "udp_in_datagrams": ns["UdpInDatagrams"], "udp_in_errors": ns["UdpInErrors"],
                "udp_rcvbuf_errors": ns["UdpRcvbufErrors"], "udp_sndbuf_errors": ns["UdpSndbufErrors"],
                "ip_in_discards": ns["IpInDiscards"], "ip_out_discards": ns["IpOutDiscards"],
                "softnet_dropped": softnet_dropped, "softnet_squeezed": softnet_squeezed,
                "qdisc_drops": qdisc_drops, "netdev_rx_drop": rx_drop,
                "netdev_tx_drop": tx_drop, "socket_count": socket_count,
                "socket_rxq_bytes": socket_rxq, "socket_txq_bytes": socket_txq,
                "socket_drops": socket_drops,
            }
            writer.writerow(row)
            output.flush()

            if not owned_pid and pid and socket_count == instances:
                if executable_sha != expected_binary_sha:
                    gate(
                        "owned_binary_sha_mismatch", pid=pid, executable=executable,
                        expected=expected_binary_sha, actual=executable_sha,
                    )
                else:
                    owned_pid = pid
                    event(
                        "owned_process", pid=pid, executable=executable,
                        executable_sha256=executable_sha, log=str(args.crawler_log),
                    )
            if not owned_pid and elapsed >= int(validity["owner_grace_seconds"]):
                gate(
                    "owner_not_acquired_within_grace", elapsed_s=elapsed, pid=pid,
                    socket_count=socket_count, expected_sockets=instances,
                )
            if owned_pid and pid == owned_pid and socket_count != instances:
                gate(
                    "owned_socket_count_mismatch", pid=pid, socket_count=socket_count,
                    expected_sockets=instances, elapsed_s=elapsed,
                )

            active_owned = bool(owned_pid and pid == owned_pid and socket_count == instances)
            if baseline is None and active_owned:
                baseline = row.copy()
                kernel_previous = row.copy()
                event("baseline", **row)

            # Always consume telemetry before inspecting PID exit. If the final
            # line and process exit race, retry the exact log for one second.
            consume_runtime()
            if owned_pid and pid != owned_pid:
                deadline = time.time() + 1.0
                while seen_runtime < args.expected_runtime_windows and time.time() < deadline:
                    time.sleep(0.1)
                    consume_runtime()
                clean_end = seen_runtime == args.expected_runtime_windows
                if not clean_end:
                    gate(
                        "owned_process_ended_before_expected_runtime_windows",
                        owned_pid=owned_pid, current_pid=pid, runtime_windows=seen_runtime,
                        expected_runtime_windows=args.expected_runtime_windows,
                    )
                event(
                    "owned_process_end", owned_pid=owned_pid, current_pid=pid,
                    runtime_windows=seen_runtime, clean=clean_end,
                )
                break

            if baseline and active_owned:
                if kernel_previous and elapsed - int(kernel_previous["elapsed_s"]) >= 30:
                    in_errors = max(0, int(row["udp_in_errors"]) - int(kernel_previous["udp_in_errors"]))
                    datagrams = max(0, int(row["udp_in_datagrams"]) - int(kernel_previous["udp_in_datagrams"]))
                    rcvbuf = max(0, int(row["udp_rcvbuf_errors"]) - int(kernel_previous["udp_rcvbuf_errors"]))
                    sndbuf = max(0, int(row["udp_sndbuf_errors"]) - int(kernel_previous["udp_sndbuf_errors"]))
                    socket_loss = max(0, int(row["socket_drops"]) - int(kernel_previous["socket_drops"]))
                    loss = max(in_errors, rcvbuf, socket_loss) + sndbuf
                    ratio = loss / max(1, datagrams + in_errors)
                    recent_kernel = (recent_kernel + [loss])[-2:]
                    event(
                        "kernel_window", from_elapsed=kernel_previous["elapsed_s"],
                        to_elapsed=elapsed, udp_in_datagrams=datagrams,
                        udp_in_errors=in_errors, udp_rcvbuf_errors=rcvbuf,
                        udp_sndbuf_errors=sndbuf, socket_drops=socket_loss,
                        loss=loss, loss_ratio=ratio,
                    )
                    if ratio >= float(validity["kernel_immediate_ratio"]) and loss > 0:
                        gate("kernel_loss_immediate_ratio", loss=loss, loss_ratio=ratio)
                    elif loss >= int(validity["kernel_min_loss"]) and ratio >= float(validity["kernel_conditioned_ratio"]):
                        gate("kernel_loss_count_and_ratio", loss=loss, loss_ratio=ratio)
                    elif (
                        len(recent_kernel) == 2 and all(value > 0 for value in recent_kernel)
                        and sum(recent_kernel) >= int(validity["kernel_two_window_loss"])
                    ):
                        gate("two_nonzero_kernel_windows_cumulative", recent=recent_kernel)
                    kernel_previous = row.copy()

                if int(row["softnet_dropped"]) > int(baseline["softnet_dropped"]):
                    gate("softnet_drop_growth", delta=int(row["softnet_dropped"]) - int(baseline["softnet_dropped"]))
                if int(row["netdev_rx_drop"]) > int(baseline["netdev_rx_drop"]) or int(row["netdev_tx_drop"]) > int(baseline["netdev_tx_drop"]):
                    gate(
                        "netdev_drop_growth",
                        rx_delta=int(row["netdev_rx_drop"]) - int(baseline["netdev_rx_drop"]),
                        tx_delta=int(row["netdev_tx_drop"]) - int(baseline["netdev_tx_drop"]),
                    )
                if rss > float(validity["rss_max_mb"]) * 1024:
                    gate("rss_over_limit", rss_kb=rss, limit_mb=validity["rss_max_mb"])
                high_cpu = high_cpu + 1 if cpu > float(validity["cpu_max_percent"]) else 0
                if high_cpu >= int(validity["cpu_consecutive_samples"]):
                    gate("cpu_over_limit_consecutive_samples", cpu_pct=cpu, consecutive=high_cpu)
                active_seconds = max(elapsed - int(baseline["elapsed_s"]), 0)
                allowed_qdisc = qdisc_allowance(
                    float(validity["qdisc_drops_per_active_minute"]), active_seconds
                )
                if qdisc_drops - int(baseline["qdisc_drops"]) > allowed_qdisc:
                    gate(
                        "qdisc_growth_over_limit",
                        delta=qdisc_drops - int(baseline["qdisc_drops"]),
                        allowed=allowed_qdisc, active_seconds=active_seconds,
                        drops_per_active_minute=validity["qdisc_drops_per_active_minute"],
                    )
            if stopped:
                break
            sample_interval = int(validity.get("sample_interval_seconds", 10))
            next_tick = args.target_epoch + ((elapsed // sample_interval) + 1) * sample_interval
            time.sleep(max(0, next_tick - time.time()))

    if not owned_pid and not failure:
        gate("owner_never_acquired")
    elif owned_pid and not clean_end and not failure:
        gate(
            "sampler_duration_expired_before_clean_end", runtime_windows=seen_runtime,
            expected_runtime_windows=args.expected_runtime_windows,
        )
    event(
        "sampler_end", runtime_windows=seen_runtime, stopped=stopped,
        owned_pid=owned_pid, clean_end=clean_end, success=not failure and clean_end,
    )
    return 1 if failure or not clean_end else 0


def parse_result_runtime_rows(log_path: Path) -> list[dict[str, int | bool]]:
    rows: list[dict[str, int | bool]] = []
    token = re.compile(r"\b([a-zA-Z][a-zA-Z0-9_]*)=([^\s]+)")
    for line in log_path.read_text(errors="replace").splitlines():
        if "runtime 30s:" not in line:
            continue
        values: dict[str, int | bool] = {}
        for key, raw in token.findall(line.split("runtime 30s:", 1)[1]):
            if raw in ("true", "false"):
                values[key] = raw == "true"
            else:
                try:
                    values[key] = int(raw)
                except ValueError:
                    continue
        required = ("dht_recv", "dropped", "refresh_q", "lookup_sent", "paused", "wire_q_drop", "ann_q")
        if all(key in values for key in required):
            rows.append({
                "dht_recv": int(values["dht_recv"]),
                "dht_dropped": int(values["dropped"]),
                "refresh_queries": int(values["refresh_q"]),
                "lookup_sent": int(values["lookup_sent"]),
                "paused": bool(values["paused"]),
                "wire_queue_drop": int(values["wire_q_drop"]),
                "announce_queued": int(values["ann_q"]),
            })
    return rows


def add_gate(gates: list[dict[str, Any]], reason: str, **values: object) -> None:
    gates.append({"kind": "gate", "reason": reason, **values})


def result_gates(
    prereg: Mapping[str, Any], run_id: str, result: Mapping[str, Any], crawler_log: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validity = nested(prereg, "validity")
    measure_windows = int(nested(prereg, "design.measure_seconds_per_block")) // RUNTIME_PERIOD_SECONDS
    gates: list[dict[str, Any]] = []
    measurement_seconds = result.get("measurement_seconds")
    expected_measurement_seconds = int(nested(prereg, "design.measure_seconds_per_block"))
    if (
        isinstance(measurement_seconds, bool)
        or not isinstance(measurement_seconds, (int, float))
        or float(measurement_seconds) != expected_measurement_seconds
    ):
        add_gate(
            gates, "measurement_seconds_mismatch_or_missing",
            actual=measurement_seconds, expected=expected_measurement_seconds,
        )
    health = result.get("health", {})
    if not isinstance(health, Mapping):
        add_gate(gates, "result_health_missing_or_invalid", actual=health)
        health = {}
    if int(result.get("runtime_windows", -1)) != measure_windows:
        add_gate(gates, "measurement_runtime_windows_mismatch", actual=result.get("runtime_windows"), expected=measure_windows)
    if int(health.get("monitor_samples", -1)) != measure_windows:
        add_gate(gates, "monitor_samples_mismatch", actual=health.get("monitor_samples"), expected=measure_windows)
    if int(health.get("oracle_samples_valid", -1)) != measure_windows:
        add_gate(gates, "oracle_samples_valid_mismatch", actual=health.get("oracle_samples_valid"), expected=measure_windows)
    if float(health.get("oracle_sample_coverage", 0)) != 1.0:
        add_gate(gates, "oracle_coverage_not_one", actual=health.get("oracle_sample_coverage"))
    if int(health.get("oracle_samples_rejected", -1)) != 0:
        add_gate(gates, "oracle_samples_rejected", actual=health.get("oracle_samples_rejected"))
    if float(health.get("runtime_window_coverage", 0)) != 1.0:
        add_gate(gates, "runtime_window_coverage_not_one", actual=health.get("runtime_window_coverage"))

    peer = result.get("peer_source_funnel", {})
    announce_queued = int(peer.get("announce_queued", -1))
    if announce_queued < int(validity["announce_queued_min"]):
        add_gate(gates, "announce_queued_below_minimum", actual=announce_queued, minimum=validity["announce_queued_min"])

    rows = parse_result_runtime_rows(crawler_log)
    measurement_rows = rows[-measure_windows:] if len(rows) >= measure_windows else []
    stability: float | None
    if len(measurement_rows) != measure_windows:
        add_gate(gates, "unparseable_or_missing_measurement_rows", actual=len(measurement_rows), expected=measure_windows)
        stability = None
    else:
        half = measure_windows // 2
        first = sum(int(row["announce_queued"]) for row in measurement_rows[:half])
        second = sum(int(row["announce_queued"]) for row in measurement_rows[half:])
        stability = first / second if second else 0.0
        low, high = validity["announce_first_second_ratio"]
        if not float(low) <= stability <= float(high):
            add_gate(
                gates, "announce_supply_stability_out_of_range", first=first,
                second=second, ratio=stability, allowed=[low, high],
            )
        if any(bool(row["paused"]) or int(row["wire_queue_drop"]) for row in measurement_rows):
            add_gate(gates, "measurement_pause_or_wire_queue_drop")

    resources = result.get("resources", {})
    if not isinstance(resources, Mapping):
        add_gate(gates, "result_resources_missing_or_invalid", actual=resources)
        resources = {}
    for key in ("udp_rcvbuf_errors", "udp_sndbuf_errors"):
        value = resources.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            add_gate(gates, "result_resource_missing_or_invalid", resource=key, actual=value)
        elif value < 0:
            add_gate(gates, "result_resource_negative", resource=key, actual=value)
        elif value != 0:
            add_gate(gates, f"result_{key}_nonzero", actual=value)

    qdisc_drops = resources.get("tx_qdisc_drops")
    monitor_samples = health.get("monitor_samples") if isinstance(health, Mapping) else None
    counter_interval = float(validity["result_counter_sample_interval_seconds"])
    if isinstance(qdisc_drops, bool) or not isinstance(qdisc_drops, (int, float)):
        add_gate(
            gates, "result_resource_missing_or_invalid",
            resource="tx_qdisc_drops", actual=qdisc_drops,
        )
    elif qdisc_drops < 0:
        add_gate(gates, "result_resource_negative", resource="tx_qdisc_drops", actual=qdisc_drops)
    elif isinstance(monitor_samples, bool) or not isinstance(monitor_samples, (int, float)) or monitor_samples < 2:
        add_gate(
            gates, "qdisc_active_duration_unavailable",
            monitor_samples=monitor_samples,
        )
    else:
        active_seconds = (float(monitor_samples) - 1.0) * counter_interval
        allowed_qdisc = qdisc_allowance(
            float(validity["qdisc_drops_per_active_minute"]), active_seconds
        )
        if qdisc_drops > allowed_qdisc:
            add_gate(
                gates, "result_tx_qdisc_rate_over_limit", actual=qdisc_drops,
                allowed=allowed_qdisc, active_seconds=active_seconds,
                drops_per_active_minute=validity["qdisc_drops_per_active_minute"],
            )

    rss_max = resources.get("rss_mb_max")
    if isinstance(rss_max, bool) or not isinstance(rss_max, (int, float)):
        add_gate(gates, "result_resource_missing_or_invalid", resource="rss_mb_max", actual=rss_max)
    elif rss_max > float(validity["rss_max_mb"]):
        add_gate(gates, "result_rss_over_limit", actual=rss_max, limit=validity["rss_max_mb"])
    local = result.get("local_funnel", {})
    if int(local.get("wire_queue_dropped", -1)) != 0:
        add_gate(gates, "result_wire_queue_dropped", actual=local.get("wire_queue_dropped"))

    return gates, {
        "run_id": run_id,
        "global_unique_per_second": result.get("primary", {}).get("global_unique_per_second"),
        "downloads": local.get("wire_download_ok"),
        "announce_queued": announce_queued,
        "announce_first_second_ratio": stability,
        "active_lookup_sent": result.get("discovery", {}).get("active_lookup_sent"),
        "refresh_queries": result.get("discovery", {}).get("refresh_queries"),
        "cpu_pct_mean": resources.get("cpu_pct_mean"),
        "rss_mb_max": resources.get("rss_mb_max"),
        "measurement_rows": len(measurement_rows),
    }


def sampler_event_gates(events_path: Path) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    kinds: list[str] = []
    last: dict[str, Any] | None = None
    try:
        lines = events_path.read_text(errors="replace").splitlines()
    except OSError:
        return [{"kind": "gate", "reason": "missing_sampler_events"}]
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            add_gate(gates, "invalid_sampler_event_json")
            continue
        if not isinstance(event, dict):
            add_gate(gates, "invalid_sampler_event_shape")
            continue
        kinds.append(str(event.get("kind")))
        last = event
        if event.get("kind") == "gate":
            gates.append(event)
    for required in ("sampler_start", "owned_process", "baseline", "owned_process_end", "sampler_end"):
        if required not in kinds:
            add_gate(gates, "missing_sampler_lifecycle_event", event=required)
    if not last or last.get("kind") != "sampler_end" or last.get("success") is not True:
        add_gate(gates, "sampler_did_not_end_successfully")
    return gates


def validate_run_manifest(
    prereg: Mapping[str, Any], arm: str, manifest_path: Path
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        add_gate(gates, "missing_or_invalid_run_manifest", error=str(error))
        return gates
    expected_binary = artifact(prereg, f"binary_{arm.lower()}")[1]
    expected_config = artifact(prereg, f"config_{arm.lower()}")[1]
    expected_oracle = artifact(prereg, "oracle_source")[1]
    expected: dict[str, Any] = {
        "variant": arm,
        "binary_sha": expected_binary,
        "template_config_sha": expected_config,
        "node_dir": str(nested(prereg, "paths.node_dir")),
        "port": str(nested(prereg, "network.udp_port_start")),
        "oracle_mode": "isolated",
        "oracle_baseline_sha": expected_oracle,
        "overrides": nested(prereg, f"arms.{arm}.overrides"),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            add_gate(gates, "run_manifest_mismatch", field=key, expected=value, actual=manifest.get(key))
    return gates


def run_id_epoch(run_id: str) -> int:
    stamp = run_id.split("_", 1)[0]
    try:
        parsed = datetime.datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")
    except ValueError as error:
        raise ContractError(f"run id lacks leading UTC timestamp: {run_id}") from error
    return int(parsed.replace(tzinfo=datetime.timezone.utc).timestamp())


def value_at(result: Mapping[str, Any], dotted: str) -> float:
    value = nested(result, dotted)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"result value is not numeric: {dotted}")
    return float(value)


def supervisor_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail-closed supervisor for preregistered blocks")
    parser.add_argument("--prereg", required=True, type=Path)
    parser.add_argument("--controller-pid-file", required=True, type=Path)
    args = parser.parse_args(argv)
    prereg = load_prereg(args.prereg)
    label = str(nested(prereg, "identity.label"))
    oracle_dir = Path(str(nested(prereg, "paths.oracle_experiment_dir")))
    diag_root = Path(str(nested(prereg, "paths.diagnostic_dir")))
    runs_root = Path(str(nested(prereg, "paths.runtime_root"))) / "bench" / "runs"
    diag_root.mkdir(parents=True, exist_ok=True)
    summary_path = diag_root / "supervisor-summary.json"
    blocks = int(nested(prereg, "design.blocks"))
    warmup = int(nested(prereg, "design.warmup_seconds_per_block"))
    measure = int(nested(prereg, "design.measure_seconds_per_block"))
    duration = warmup + measure + int(nested(prereg, "validity.sampler_tail_seconds"))
    expected_runtime = (warmup + measure) // RUNTIME_PERIOD_SECONDS
    plan = str(nested(prereg, "design.plan"))
    sampler, _ = artifact(prereg, "sampler")
    summary: dict[str, Any] = {
        "schema_version": 3,
        "label": label,
        "started_at": utc(),
        "preregistration_sha256": sha256(args.prereg),
        "blocks": [],
        "pair_gates": [],
    }

    def save(status: str, **values: object) -> None:
        summary.update(status=status, ended_at=utc(), **values)
        atomic_json(summary_path, summary)
        digest = sha256(summary_path)
        summary_path.with_suffix(".json.sha256").write_text(
            f"{digest}  {summary_path.name}\n", encoding="ascii"
        )

    def fail(reason: str, block: int | None = None) -> int:
        # The controller owns its runner/crawler cleanup. Killing only its
        # preregistered PID avoids ever terminating an unrelated replacement.
        terminate_pidfile(args.controller_pid_file)
        save("gate_or_sampler_failure", failed_block=block, failure_reason=reason)
        return 1

    results: dict[int, tuple[str, dict[str, Any]]] = {}
    for block in range(1, blocks + 1):
        controller_log = oracle_dir / f"block-{block:03d}.controller.log"
        run_id = ""
        while not run_id:
            if controller_log.exists():
                match = re.search(
                    r"^RUN run_id=(\S+)", controller_log.read_text(errors="replace"), re.MULTILINE
                )
                if match:
                    run_id = match.group(1)
                    break
            if not pidfile_alive(args.controller_pid_file):
                return fail("controller_exited_before_block", block)
            time.sleep(1)
        arm = plan[block - 1]
        try:
            target_epoch = run_id_epoch(run_id)
        except ContractError as error:
            summary["blocks"].append({
                "block": block, "arm": arm, "run_id": run_id,
                "gates": [{"kind": "gate", "reason": "invalid_run_id", "error": str(error)}],
            })
            return fail("invalid_run_id", block)

        block_dir = diag_root / f"block-{block:03d}-{arm}"
        try:
            block_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            return fail("diagnostic_block_dir_already_exists", block)
        run_dir = runs_root / run_id
        crawler_log = run_dir / "crawler.log"
        identity = {
            "schema_version": 3, "block": block, "arm": arm, "run_id": run_id,
            "run_label": f"{label}-block{block}", "target_epoch": target_epoch,
            "controller_log": str(controller_log), "crawler_log": str(crawler_log),
            "sampler": str(sampler), "sampler_sha256": sha256(sampler),
            "created_at": utc(), "expected_runtime_windows": expected_runtime,
            "sampler_duration": duration,
        }
        atomic_json(block_dir / "identity.json", identity)
        with (block_dir / "sampler.log").open("w", encoding="utf-8") as output:
            completed = subprocess.run([
                str(sampler), "--prereg", str(args.prereg.resolve()),
                "--diag-dir", str(block_dir), "--target-epoch", str(target_epoch),
                "--duration", str(duration), "--run-label", f"{label}-block{block}",
                "--arm", arm,
                "--crawler-log", str(crawler_log),
                "--expected-runtime-windows", str(expected_runtime),
            ], stdout=output, stderr=subprocess.STDOUT, check=False)

        events_path = block_dir / "events.jsonl"
        gates = sampler_event_gates(events_path)
        done = False
        deadline = time.time() + int(nested(prereg, "validity.controller_done_grace_seconds"))
        while time.time() < deadline:
            if controller_log.exists() and re.search(
                rf"^DONE run_id={re.escape(run_id)}\b",
                controller_log.read_text(errors="replace"), re.MULTILINE,
            ):
                done = True
                break
            time.sleep(1)
        result_path = run_dir / "result.json"
        run_manifest_path = run_dir / "manifest.json"
        result_summary: dict[str, Any] = {}
        if not done:
            add_gate(gates, "missing_done_record")
        if not result_path.exists() or not crawler_log.exists():
            add_gate(gates, "missing_result_or_crawler_log")
        else:
            try:
                result = json.loads(result_path.read_text(encoding="utf-8"))
                if not isinstance(result, dict):
                    raise TypeError("result root must be an object")
                post_gates, result_summary = result_gates(prereg, run_id, result, crawler_log)
                gates.extend(post_gates)
                results[block] = (arm, result)
            except (OSError, json.JSONDecodeError, ContractError, KeyError, TypeError, ValueError) as error:
                add_gate(gates, "result_validation_error", error=str(error))
        gates.extend(validate_run_manifest(prereg, arm, run_manifest_path))

        hash_errors, observed = all_artifact_hashes(prereg)
        for error in hash_errors:
            add_gate(gates, "artifact_changed_during_experiment", error=error)
        try:
            node_count, node_digest = node_content_digest(Path(str(nested(prereg, "paths.node_dir"))))
        except OSError as error:
            node_count, node_digest = -1, ""
            add_gate(gates, "node_content_set_unreadable", error=str(error))
        if node_count != int(nested(prereg, "network.node_ids")) or node_digest != nested(prereg, "node_content_set_sha256"):
            add_gate(
                gates, "node_content_set_changed", actual_count=node_count,
                actual_sha256=node_digest,
            )

        block_record = {
            "block": block, "arm": arm, "run_id": run_id,
            "sampler_exit": completed.returncode, "gates": gates,
            "result": result_summary,
            "result_sha256": sha256(result_path) if result_path.exists() else None,
            "run_manifest_sha256": sha256(run_manifest_path) if run_manifest_path.exists() else None,
            "sampler_events_sha256": sha256(events_path) if events_path.exists() else None,
            "observed_artifact_hashes": observed,
            "node_content_set_sha256": node_digest,
        }
        summary["blocks"].append(block_record)
        atomic_json(summary_path, {**summary, "status": "running", "updated_at": utc()})
        if completed.returncode != 0 or gates:
            return fail("block_gate_or_sampler_failure", block)

    for pair_index, pair_blocks in enumerate(nested(prereg, "design.pairing"), start=1):
        pair = [results[int(number)] for number in pair_blocks]
        by_arm = {arm: result for arm, result in pair}
        pair_record: dict[str, Any] = {"pair": pair_index, "blocks": pair_blocks, "gates": []}
        if set(by_arm) != {"A", "B"}:
            add_gate(pair_record["gates"], "pair_missing_arm")
        else:
            for check in nested(prereg, "validity.exposure_checks"):
                name = str(check["name"])
                dotted = str(check["result_path"])
                try:
                    ratio = value_at(by_arm["B"], dotted) / max(1.0, value_at(by_arm["A"], dotted))
                except ContractError as error:
                    add_gate(pair_record["gates"], "exposure_metric_invalid", name=name, error=str(error))
                    continue
                low, high = check["b_a_range"]
                pair_record[f"{name}_b_a"] = ratio
                if not float(low) <= ratio <= float(high):
                    add_gate(
                        pair_record["gates"], "exposure_out_of_range",
                        name=name, ratio=ratio, allowed=[low, high],
                    )
        summary.setdefault("pairs", []).append(pair_record)
        summary["pair_gates"].extend(pair_record["gates"])

    manifest_path = oracle_dir / "manifest.json"
    deadline = time.time() + int(nested(prereg, "validity.controller_done_grace_seconds"))
    manifest: dict[str, Any] | None = None
    while time.time() < deadline:
        try:
            candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(candidate, dict):
                manifest = candidate
            if manifest and manifest.get("status") == "completed":
                break
        except (OSError, json.JSONDecodeError):
            pass
        time.sleep(1)
    if not manifest or manifest.get("status") != "completed" or manifest.get("finalized") is not False:
        add_gate(summary["pair_gates"], "oracle_manifest_not_completed_unfinalized")
    expected_baseline = artifact(prereg, "oracle_source")[1]
    if manifest and manifest.get("baseline_sha") != expected_baseline:
        add_gate(
            summary["pair_gates"], "oracle_manifest_baseline_hash_mismatch",
            expected=expected_baseline, actual=manifest.get("baseline_sha"),
        )
    hash_errors, final_hashes = all_artifact_hashes(prereg)
    for error in hash_errors:
        add_gate(summary["pair_gates"], "final_artifact_changed", error=error)
    try:
        node_count, node_digest = node_content_digest(Path(str(nested(prereg, "paths.node_dir"))))
    except OSError as error:
        node_count, node_digest = -1, ""
        add_gate(summary["pair_gates"], "final_node_content_set_unreadable", error=str(error))
    if node_count != int(nested(prereg, "network.node_ids")) or node_digest != nested(prereg, "node_content_set_sha256"):
        add_gate(summary["pair_gates"], "final_node_content_set_changed")
    summary["final_artifact_hashes"] = final_hashes
    summary["final_node_content_set_sha256"] = node_digest
    if summary["pair_gates"]:
        return fail("pair_or_final_gate")
    save("completed")
    return 0
