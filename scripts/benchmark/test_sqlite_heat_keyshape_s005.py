import importlib
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
s005 = importlib.import_module("sqlite_heat_keyshape_s005")


def test_synthetic_hash_is_exactly_twenty_bytes_and_stable():
    assert len(s005.hash20(123)) == 20
    assert s005.hash20(123) == s005.hash20(123)
    assert s005.hash20(123) != s005.hash20(124)


def test_hash_shape_preserves_exact_count(tmp_path):
    result = s005.run_shape(tmp_path, "hash20", 2_000, 1_000, 100, 100, 2, 123)
    assert result["logical_unique_pairs"] == 1_000
    assert result["integrity_check"] == "ok"
    assert result["final_db_bytes_per_unique"] > 0


def test_local_dictionary_preserves_exact_count(tmp_path):
    result = s005.run_shape(tmp_path, "local_dictionary", 2_000, 1_000, 100, 100, 2, 123)
    assert result["logical_unique_pairs"] == 1_000
    assert result["dictionary_hashes"] == result["active_hashes"]
