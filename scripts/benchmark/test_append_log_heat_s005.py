import importlib
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
s005 = importlib.import_module("append_log_heat_s005")


def test_frame_round_trip_and_torn_tail_truncation(tmp_path):
    path = tmp_path / "heat.frames"
    records = [bytes([index]) * s005.RECORD_BYTES for index in range(10)]
    s005.append_frame(path, records)
    good_size = path.stat().st_size
    with path.open("ab") as handle:
        handle.write(s005.encode_frame(records)[:17])
    assert list(s005.iter_records(path, truncate_torn_tail=True)) == records
    assert path.stat().st_size == good_size


def test_external_dedup_is_exact_under_response_loss(tmp_path):
    path = tmp_path / "heat.frames"
    records = [bytes([index]) * 20 + index.to_bytes(8, "big") for index in range(100)]
    s005.append_frame(path, records)
    s005.append_frame(path, records)
    result = s005.external_dedup(path, tmp_path, 100, 25)
    assert result["logical_unique_pairs"] == 100
    assert result["error"] == 0
