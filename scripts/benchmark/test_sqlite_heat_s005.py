import importlib.util
import sqlite3
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("sqlite_heat_s005.py")
SPEC = importlib.util.spec_from_file_location("sqlite_heat_s005", MODULE_PATH)
assert SPEC and SPEC.loader
s005 = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = s005
SPEC.loader.exec_module(s005)


def test_known_unique_then_replay_workload():
    unique = [s005.observation_pair(i, 100, 25, 123) for i in range(100)]
    replay = [s005.observation_pair(i, 100, 25, 123) for i in range(100, 200)]
    assert len(set(unique)) == 100
    assert set(replay).issubset(set(unique))


def test_long_tail_has_hot_and_cold_hashes():
    counts = {}
    for index in range(100_000):
        torrent, _ = s005.unique_pair(index, 1_000, 123)
        counts[torrent] = counts.get(torrent, 0) + 1
    assert max(counts.values()) > 10 * sorted(counts.values())[len(counts) // 2]


def test_schema_is_without_rowid_and_exact(tmp_path):
    path = tmp_path / "heat.sqlite"
    connection = sqlite3.connect(path)
    pragmas = s005.configure(connection)
    rows = [s005.unique_pair(index, 100, 123) for index in range(1_000)]
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)", rows + rows
        )
    assert connection.execute("SELECT count(*) FROM seen").fetchone()[0] == 1_000
    sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='seen'"
    ).fetchone()[0]
    assert "WITHOUT ROWID" in sql
    assert pragmas["mmap_size"] == 0
    connection.close()


def test_pinned_runtime_and_cache_limit():
    assert s005.PYTHON_IMAGE.endswith(
        "sha256:4ac787b083ff5fa9d64c6f68440088545e1b941142aed716cf9378ee348a9f1b"
    )
    assert s005.CACHE_KIB == 65_536
