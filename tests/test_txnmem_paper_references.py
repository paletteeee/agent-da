import json
import re
import unittest
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "configs" / "txnmem_paper_references.json"

SOURCE_CLASS_HOSTS = {
    "arxiv_preprint": {"arxiv.org"},
    "arxiv_record_with_journal_reference": {"arxiv.org"},
    "official_proceedings": {"www.usenix.org", "www.vldb.org"},
    "doi_landing": {"doi.org"},
    "official_publication": {"csrc.nist.gov"},
}


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

    def test_reference_catalog_enforces_primary_source_schema(self):
        rows = json.loads(CATALOG.read_text(encoding="utf-8"))["references"]

        for row in rows:
            self.assertIsInstance(row["id"], str)
            self.assertRegex(row["id"], r"^R\d{2}$")
            self.assertTrue(all(isinstance(author, str) and author.strip() for author in row["authors"]))
            self.assertTrue(all(isinstance(topic, str) and topic.strip() for topic in row["topics"]))
            self.assertIsInstance(row["title"], str)
            self.assertTrue(row["title"].strip())
            self.assertIsInstance(row["venue"], str)
            self.assertTrue(row["venue"].strip())
            self.assertIsInstance(row["year"], int)
            self.assertGreaterEqual(row["year"], 1900)

            self.assertIn(row["source_class"], SOURCE_CLASS_HOSTS)
            for field in ("url", "verified_source"):
                self.assertIsInstance(row[field], str)
                parsed = urlparse(row[field])
                self.assertEqual(parsed.scheme, "https")
                self.assertIn(parsed.hostname, SOURCE_CLASS_HOSTS[row["source_class"]])

            if row["source_class"] == "arxiv_preprint":
                self.assertRegex(row["venue"], r"^arXiv preprint arXiv:[^\s]+$")

    def test_reference_catalog_preserves_primary_source_metadata_corrections(self):
        rows = {
            row["id"]: row
            for row in json.loads(CATALOG.read_text(encoding="utf-8"))["references"]
        }

        self.assertEqual(rows["R11"]["venue"], "Proceedings of the ACM SIGMOD Conference 1995")
        self.assertEqual(rows["R14"]["venue"], "Proceedings of the VLDB Endowment")
        self.assertEqual(rows["R14"]["year"], 2013)
        self.assertIn("Aaron Davidson", rows["R14"]["authors"])
        for reference_id in ("R02", "R03", "R04", "R05", "R06", "R07", "R09", "R10"):
            self.assertEqual(rows[reference_id]["source_class"], "arxiv_preprint")
            self.assertTrue(rows[reference_id]["venue"].startswith("arXiv preprint arXiv:"))

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
