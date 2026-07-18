from pathlib import Path
import os
import shutil
import subprocess
import json


ROOT = Path(__file__).resolve().parents[2]


def bash_executable() -> str:
    candidates: list[Path] = []
    if os.name == "nt":
        for root in (
            os.environ.get("ProgramFiles"),
            os.environ.get("ProgramFiles(x86)"),
            r"C:\Program Files",
        ):
            if root:
                candidates.append(Path(root) / "Git" / "bin" / "bash.exe")
    found = shutil.which("bash")
    if found:
        candidates.append(Path(found))
    for candidate in dict.fromkeys(candidates):
        if not candidate.exists():
            continue
        try:
            probe = subprocess.run(
                [str(candidate), "-c", "printf cherry-bash-ok"],
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if probe.returncode == 0 and probe.stdout == "cherry-bash-ok":
            return str(candidate)
    raise RuntimeError("bash is required for storage privacy contract tests")


def bash_path(path: Path) -> str:
    if os.name != "nt":
        return str(path)
    result = subprocess.run(
        [bash_executable(), "-c", "cygpath -u \"$1\"", "--", str(path)],
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
    )
    return result.stdout.strip()


def run_gate(listing: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [bash_executable(), bash_path(ROOT / "scripts" / "storage-secret-privacy-gate.sh")],
        input=listing,
        text=True,
        capture_output=True,
        check=False,
    )


def test_storage_secret_archive_is_strictly_allowlisted() -> None:
    assert run_gate(".env\ncherry-backup.env\n").returncode == 0
    forbidden = run_gate(".env\ncherry-backup.env\ncherry-secrets/heat-actor-master\n")
    assert forbidden.returncode != 0
    assert "forbidden secret archive entry" in forbidden.stderr
    assert run_gate(".env\ncherry-backup.env\netc/shadow\n").returncode != 0


def test_backup_never_archives_crawler_actor_master_or_raw_secret_directory() -> None:
    backup = (ROOT / "scripts" / "backup-storage.sh").read_text(encoding="utf-8")
    assert "-C /etc cherry-secrets" not in backup
    assert "cherry-storage-secret-privacy-gate" in backup
    assert "-C /etc cherry-backup.env" in backup


def test_heat_backup_privacy_match_consumes_full_listing_under_pipefail() -> None:
    for name in ("backup-storage.sh", "verify-storage-backup.sh"):
        script = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert "grep -q 'heat-rolling-24h\\.sqlite3'" not in script
        assert "grep 'heat-rolling-24h\\.sqlite3' >/dev/null" in script
    producer = "for i in $(seq 1 20000); do printf 'heat/day-%s.sqlite3\\n' \"$i\"; done; " \
               "printf 'heat/heat-rolling-24h.sqlite3\\n'"
    result = subprocess.run(
        [bash_executable(), "-c", f"set -o pipefail; {{ {producer}; }} | "
         "grep 'heat-rolling-24h\\.sqlite3' >/dev/null"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_v2_preflight_uses_json_spool_and_preserves_archive_ownership(tmp_path: Path) -> None:
    actual = tmp_path / "actual-heat"
    decoy = tmp_path / "hardcoded-decoy"
    actual.mkdir()
    decoy.mkdir()
    legacy = actual / "heat-000001.spool"
    legacy.write_bytes(b"CHHS" + bytes([1]) + b"legacy-data")
    (decoy / "heat-000001.spool").write_bytes(b"CHHS" + bytes([2]) + b"v2-data")
    config = tmp_path / "metadata.json"
    config.write_text(json.dumps({"heat": {"spool_dir": bash_path(actual)}}), encoding="utf-8")
    script = ROOT / "scripts" / "preflight-heat-spool-v2.sh"
    has_flock = subprocess.run(
        [bash_executable(), "-c", "command -v flock >/dev/null 2>&1"],
        check=False,
    ).returncode == 0
    if has_flock:
        command = [bash_executable(), bash_path(script)]
    else:
        shim_dir = tmp_path / "bin"
        shim_dir.mkdir()
        flock_shim = shim_dir / "flock"
        flock_shim.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8", newline="\n")
        flock_shim.chmod(0o755)
        command = [
            bash_executable(), "-c",
            'PATH="$1:$PATH"; shift; exec "$@"', "--", bash_path(shim_dir),
            bash_path(script),
        ]

    check = subprocess.run(
        [*command, "--config", bash_path(config)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert check.returncode != 0
    assert bash_path(actual) in check.stderr
    assert legacy.read_bytes() == b"CHHS" + bytes([1]) + b"legacy-data"

    before = actual.stat()
    archive = subprocess.run(
        [*command, "--config", bash_path(config), "--archive-v1"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert archive.returncode == 0, archive.stderr
    assert actual.is_dir() and not any(actual.iterdir())
    archived = list(tmp_path.glob("actual-heat.chht-v1-archive-*"))
    assert len(archived) == 1
    assert (archived[0] / legacy.name).read_bytes() == b"CHHS" + bytes([1]) + b"legacy-data"
    after = actual.stat()
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)


def test_v2_preflight_config_without_spool_does_not_check_decoy(tmp_path: Path) -> None:
    config = tmp_path / "metadata.json"
    config.write_text(json.dumps({"heat": {"enabled": False}}), encoding="utf-8")
    result = subprocess.run(
        [bash_executable(), bash_path(ROOT / "scripts" / "preflight-heat-spool-v2.sh"),
         "--config", bash_path(config)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "no spool configured" in result.stdout


def test_unbacked_authority_requires_exact_audited_opt_out() -> None:
    setup = (ROOT / "scripts" / "setup-storage-server.sh").read_text(encoding="utf-8")
    compose = (ROOT / "deploy" / "storage" / "compose.yml").read_text(encoding="utf-8")
    assert 'CHERRY_ALLOW_UNBACKED_AUTHORITY:-}" == "I_ACCEPT_DATA_LOSS"' in setup
    assert 'UNBACKED_MARKER="/var/lib/cherry-backup/UNBACKED_AUTHORITY"' in setup
    assert "systemctl disable --now cherry-storage-backup.timer" in setup
    assert "archive_mode=${CHERRY_PG_ARCHIVE_MODE:-on}" in compose
    assert "desired_archive_mode" in setup and "printf off || printf on" in setup
    assert "postgres-and-heat-are-single-copy-authorities" in setup
