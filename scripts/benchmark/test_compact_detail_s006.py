import unittest

from scripts.benchmark.compact_detail_s006 import (
    ExtensionEntry,
    FileEntry,
    build_corpus,
    decode_detail,
    encode_detail,
    uvarint,
)


class CompactDetailS006Tests(unittest.TestCase):
    def test_cross_language_fixture(self):
        payload = encode_detail(
            [FileEntry("a/c.bin", 130), FileEntry("a/b.txt", 3)],
            [ExtensionEntry(".txt", 1, 3), ExtensionEntry(".bin", 1, 130)],
        )
        self.assertEqual(
            "01020007612f622e747874030205632e62696e820102042e62696e018201042e7478740103",
            payload.hex(),
        )
        self.assertEqual(
            ([FileEntry("a/b.txt", 3), FileEntry("a/c.bin", 130)],
             [ExtensionEntry(".bin", 1, 130), ExtensionEntry(".txt", 1, 3)]),
            decode_detail(payload),
        )

    def test_empty_detail(self):
        self.assertEqual(b"\x01\x00\x00", encode_detail([], []))
        self.assertEqual(([], []), decode_detail(b"\x01\x00\x00"))

    def test_noncanonical_and_malformed_payloads(self):
        invalid = [
            b"", b"\x02\x00\x00", b"\x01\x80\x00\x00",
            b"\x01\x00\x00\x00", b"\x01\x01\x00\x01\xff\x00\x00",
        ]
        for payload in invalid:
            with self.subTest(payload=payload.hex()):
                with self.assertRaises((ValueError, UnicodeDecodeError)):
                    decode_detail(payload)

    def test_uvarint_boundaries(self):
        self.assertEqual(b"\x00", uvarint(0))
        self.assertEqual(b"\x7f", uvarint(127))
        self.assertEqual(b"\x80\x01", uvarint(128))
        self.assertEqual(b"\xff" * 8 + b"\x7f", uvarint((1 << 63) - 1))

    def test_corpus_is_deterministic(self):
        left, right = build_corpus(100, 7), build_corpus(100, 7)
        self.assertEqual(left, right)
        self.assertEqual(100, len(left))
        self.assertEqual(2, sum(bool(item.extensions) for item in left))


if __name__ == "__main__":
    unittest.main()
