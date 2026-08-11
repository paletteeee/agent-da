import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "configs" / "txnmem_paper_references.json"


class TxnMemPaperReferenceTests(unittest.TestCase):
    def test_reference_catalog_is_complete_unique_and_stably_numbered(self):
        rows = json.loads(CATALOG.read_text(encoding="utf-8"))["references"]

        self.assertGreaterEqual(len(rows), 30)
        self.assertLessEqual(len(rows), 45)
        self.assertEqual([row["id"] for row in rows], [f"R{index:02d}" for index in range(1, len(rows) + 1)])
        self.assertEqual(len({row["id"] for row in rows}), len(rows))
        for row in rows:
            self.assertTrue(
                all(
                    row.get(key)
                    for key in (
                        "authors",
                        "title",
                        "venue",
                        "year",
                        "url",
                        "topics",
                        "verified_source",
                    )
                )
            )
            self.assertIsInstance(row["authors"], list)
            self.assertIsInstance(row["topics"], list)
            self.assertEqual(row["url"], row["verified_source"])

    def test_reference_catalog_covers_all_required_topic_groups(self):
        rows = json.loads(CATALOG.read_text(encoding="utf-8"))["references"]
        topics = {topic for row in rows for topic in row["topics"]}

        self.assertTrue(
            {
                "agent_memory",
                "rag_long_term_memory",
                "agent_benchmarks",
                "transactions_serializability",
                "provenance_lineage",
                "access_control",
                "failure_injection_model_checking",
            }.issubset(topics)
        )


if __name__ == "__main__":
    unittest.main()
