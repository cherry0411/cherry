"""Determinism and isolation tests for the S-003 Docker benchmark."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("storage_search_s003.py")
SPEC = Path(__file__).with_name("storage_search_s003_corpus.json")
MODULE_SPEC = importlib.util.spec_from_file_location("storage_search_s003", SCRIPT)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
s003 = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = s003
MODULE_SPEC.loader.exec_module(s003)


class CorpusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.corpus = s003.load_corpus(SPEC)

    def test_corpus_is_versioned_deterministic_and_zero_raw(self) -> None:
        self.assertEqual(10_000, self.corpus.manifest["document_count"])
        self.assertEqual(10, self.corpus.manifest["query_count"])
        self.assertEqual(3, self.corpus.manifest["alias_only_query_count"])
        self.assertEqual(0, self.corpus.manifest["permanent_raw_retention_percent"])
        self.assertEqual(
            "b709686a9c168500a3d63adadd7e4b2b626ff9c45f319b06e906c3e3e2994bf0",
            self.corpus.manifest["corpus_sha256"],
        )
        s003.assert_zero_raw(
            {"documents": self.corpus.documents, "queries": self.corpus.queries}
        )

    def test_production_projection_deliberately_omits_aliases(self) -> None:
        projected = s003.search_documents(self.corpus)
        self.assertEqual(len(self.corpus.documents), len(projected))
        self.assertTrue(any(document["aliases"] for document in self.corpus.documents))
        self.assertTrue(all("aliases" not in document for document in projected))
        self.assertEqual(
            {"infoHash", "name", "totalLength", "fileCount", "isPrivate", "peerCount", "createdAt"},
            set(projected[0]),
        )

    def test_alias_only_judgments_have_zero_model_ceiling(self) -> None:
        alias_queries = [query for query in self.corpus.queries if query["aliasOnly"]]
        self.assertEqual(3, len(alias_queries))
        self.assertTrue(all(query["modelRecallCeiling"] == 0.0 for query in alias_queries))


class ExperimentIsolationTests(unittest.TestCase):
    def test_ranking_arms_change_only_ranking_order(self) -> None:
        control = s003.settings_for(s003.RANKING_ARMS["A_current"])
        treatment = s003.settings_for(s003.RANKING_ARMS["B_relevance_first"])
        control_without_ranking = {k: v for k, v in control.items() if k != "rankingRules"}
        treatment_without_ranking = {k: v for k, v in treatment.items() if k != "rankingRules"}
        self.assertEqual(control_without_ranking, treatment_without_ranking)
        self.assertCountEqual(control["rankingRules"], treatment["rankingRules"])
        self.assertNotEqual(control["rankingRules"], treatment["rankingRules"])

    def test_metrics_use_graded_relevance(self) -> None:
        query = {"judgments": {"best": 3, "also": 2}}
        perfect = s003.evaluate_query(query, ["best", "also", "noise"])
        reversed_hits = s003.evaluate_query(query, ["noise", "also", "best"])
        missing = s003.evaluate_query(query, [])
        self.assertEqual(1.0, perfect["recall_at_20"])
        self.assertEqual(1.0, perfect["ndcg_at_10"])
        self.assertEqual(1.0, perfect["mrr"])
        self.assertLess(reversed_hits["ndcg_at_10"], perfect["ndcg_at_10"])
        self.assertEqual(1, missing["zero_result"])

    def test_percentile_interpolates_fixed_samples(self) -> None:
        self.assertEqual(2.5, s003.percentile([1, 2, 3, 4], 0.5))
        self.assertAlmostEqual(3.85, s003.percentile([1, 2, 3, 4], 0.95))

    def test_zero_raw_guard_rejects_binary_and_forbidden_keys(self) -> None:
        with self.assertRaises(AssertionError):
            s003.assert_zero_raw({"raw_bytes": "forbidden"})
        with self.assertRaises(AssertionError):
            s003.assert_zero_raw({"normal": b"forbidden"})


if __name__ == "__main__":
    unittest.main()
