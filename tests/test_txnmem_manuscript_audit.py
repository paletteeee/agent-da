from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_manuscript_audit import (  # noqa: E402
    audit_manuscript,
    audit_text,
    load_paper_config,
)


CONFIG = load_paper_config(ROOT / "configs" / "txnmem_ccfa_paper.json")


class ManuscriptAuditTests(unittest.TestCase):
    def _valid_manuscript_text(self) -> str:
        sections = "\n\n".join(f"# {section}" for section in CONFIG["required_sections"])
        claim_ids = "\n".join(f"[[CLAIM:{claim_id}]]" for claim_id in CONFIG["active_claim_ids"])
        boundaries = "\n".join(CONFIG["required_claim_boundaries"])
        return f"{sections}\n\n{claim_ids}\n\n{boundaries}\n"

    def test_rejects_superseded_artifact(self):
        report = audit_text(
            "结果来自 results/real_backend_performance_reps30_v2/results/backend_performance.json",
            root=ROOT,
            config=CONFIG,
        )

        self.assertIn(
            "superseded_artifact", {item["code"] for item in report["findings"]}
        )

    def test_accepts_required_sections_and_active_claims(self):
        with TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "manuscript.md"
            fixture.write_text(self._valid_manuscript_text(), encoding="utf-8")
            report = audit_manuscript(fixture, ROOT, CONFIG)

        self.assertEqual(report["finding_count"], 0)

    def test_rejects_number_not_covered_by_an_active_claim(self):
        report = audit_text(
            self._valid_manuscript_text() + "\n实验结果为 999999。\n",
            root=ROOT,
            config=CONFIG,
        )

        self.assertIn(
            "uncovered_claim_value", {item["code"] for item in report["findings"]}
        )

    def test_rejects_missing_claim_audit_report(self):
        config = dict(CONFIG)
        config["claim_audit_path"] = "results/paper_evidence/missing_claim_audit.json"

        report = audit_text(self._valid_manuscript_text(), root=ROOT, config=config)

        self.assertIn(
            "claim_audit_missing", {item["code"] for item in report["findings"]}
        )

    def test_drafting_mode_only_allows_deferred_sections_to_be_absent(self):
        text = "\n\n".join(
            f"# {section}"
            for section in CONFIG["required_sections"]
            if section not in CONFIG["drafting_optional_sections"]
        )
        text += "\n\n" + "\n".join(
            f"[[CLAIM:{claim_id}]]" for claim_id in CONFIG["active_claim_ids"]
        )
        text += "\n\n" + "\n".join(CONFIG["required_claim_boundaries"])

        report = audit_text(text, root=ROOT, config=CONFIG)

        self.assertNotIn(
            "missing_required_section", {item["code"] for item in report["findings"]}
        )


if __name__ == "__main__":
    unittest.main()
