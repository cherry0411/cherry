from scripts.benchmark.heat_ingest_s007 import build_delivery


def test_delivery_fixture_is_deterministic_and_canonical():
    delivery = build_delivery(
        "s007-sg", 0, 0, 4, 20_000, "2024-10-04", 123, b"s" * 32
    )

    assert delivery.payload.hex() == (
        "434848540100004e20022075e9617c5cf3cb3913622e104af6aba5006998019743"
        "a1ea31f31252914b16e9fc6b7a43455153814734f99ef68751da030abc35837655"
        "a5c67fb1aae402cfe777b9e5314eb24b5720"
    )
    assert delivery.payload_sha256 == (
        "52f91be9f00da32c2f2bd99cda8f5bdc344e99c9bf5ceaaf3f8b2e50567d69ee"
    )
    assert delivery.signature == (
        "487a573142259b31bcb0cbd7dfbbdd555c6d80d17a3cdb7f19b355310fa8c009"
    )
    assert (delivery.start_sequence, delivery.end_sequence, delivery.canonical_records) == (1, 4, 4)
