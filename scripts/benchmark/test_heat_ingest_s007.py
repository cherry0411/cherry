from scripts.benchmark.heat_ingest_s007 import build_delivery


def test_delivery_fixture_is_deterministic_and_canonical():
    delivery = build_delivery(
        "s007-sg", 0, 0, 4, 20_000, 12, "2024-10-04", 123, b"s" * 32
    )

    assert delivery.payload.hex() == (
        "434848540200004e200c022075e9617c5cf3cb3913622e104af6aba5006998019743"
        "a1ea31f31252914b16e9fc6b7a43455153814734f99ef68751da030abc35837655"
        "a5c67fb1aae402cfe777b9e5314eb24b5720"
    )
    assert delivery.payload_sha256 == (
        "ea350096ccc02fb69eaed6133a715878d086d7e0ed66712040a9253bf0907eed"
    )
    assert delivery.signature == (
        "a8efc92998f761d1e72480ba12606efbf12e77b2101056e61bc35721d9f4a058"
    )
    assert (delivery.start_sequence, delivery.end_sequence, delivery.canonical_records) == (1, 4, 4)
