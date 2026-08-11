from __future__ import annotations

import json
import re
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
    strip_author_annotations,
)


CONFIG = load_paper_config(ROOT / "configs" / "txnmem_ccfa_paper.json")


class ManuscriptAuditTests(unittest.TestCase):
    def _claim_blocks(self) -> str:
        return "\n\n".join(
            f"[[CLAIM:{claim_id}]]\n{boundary}"
            for claim_id, boundary in zip(
                CONFIG["active_claim_ids"], CONFIG["required_claim_boundaries"]
            )
        )

    def _valid_manuscript_text(self) -> str:
        sections = "\n\n".join(f"# {section}" for section in CONFIG["required_sections"])
        return f"{sections}\n\n{self._claim_blocks()}\n"

    def test_rejects_superseded_artifact(self):
        report = audit_text(
            "结果来自 results/real_backend_performance_reps30_v2/results/backend_performance.json",
            root=ROOT,
            config=CONFIG,
        )

        self.assertIn(
            "superseded_artifact", {item["code"] for item in report["findings"]}
        )

    def test_rejects_v6_cross_host_artifact(self):
        report = audit_text(
            "结果来自 results/cross_host_model_load_formal_v6_aggregate/results/model_load_repetition_summary.json",
            root=ROOT,
            config=CONFIG,
        )

        self.assertIn(
            "superseded_artifact", {item["code"] for item in report["findings"]}
        )

    def test_rejects_v7_cross_host_artifact(self):
        report = audit_text(
            "结果来自 results/cross_host_model_load_formal_v7_rep1/results/model_load_summary.json",
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

    def test_rejects_allowed_number_without_claim_binding(self):
        report = audit_text(
            self._valid_manuscript_text() + "\n实验结果为 400。\n",
            root=ROOT,
            config=CONFIG,
        )

        self.assertIn(
            "unmarked_claim_value", {item["code"] for item in report["findings"]}
        )

    def test_rejects_missing_claim_audit_report(self):
        config = dict(CONFIG)
        config["claim_audit_path"] = "results/paper_evidence/missing_claim_audit.json"

        report = audit_text(self._valid_manuscript_text(), root=ROOT, config=config)

        self.assertIn(
            "claim_audit_missing", {item["code"] for item in report["findings"]}
        )

    def test_rejects_claim_audit_without_current_ledger_digest(self):
        current_audit = ROOT / CONFIG["claim_audit_path"]
        payload = json.loads(current_audit.read_text(encoding="utf-8"))
        with TemporaryDirectory() as tmp:
            stale_audit = Path(tmp) / "claim_audit.json"
            config = dict(CONFIG)
            config["claim_audit_path"] = str(stale_audit)

            payload["ledger_sha256"] = "0" * 64
            stale_audit.write_text(json.dumps(payload), encoding="utf-8")
            mismatched = audit_text(self._valid_manuscript_text(), ROOT, config)

            payload.pop("ledger_sha256")
            stale_audit.write_text(json.dumps(payload), encoding="utf-8")
            missing = audit_text(self._valid_manuscript_text(), ROOT, config)

        self.assertIn(
            "claim_audit_ledger_sha256_mismatch",
            {item["code"] for item in mismatched["findings"]},
        )
        self.assertIn(
            "claim_audit_ledger_sha256_missing",
            {item["code"] for item in missing["findings"]},
        )

    def test_drafting_mode_only_allows_deferred_sections_to_be_absent(self):
        text = "\n\n".join(
            f"# {section}"
            for section in CONFIG["required_sections"]
            if section not in CONFIG["drafting_optional_sections"]
        )
        text += "\n\n" + self._claim_blocks()

        report = audit_text(text, root=ROOT, config=CONFIG)

        self.assertNotIn(
            "missing_required_section", {item["code"] for item in report["findings"]}
        )

    def test_current_draft_conforms_to_manuscript_contract(self):
        report = audit_manuscript(ROOT / CONFIG["paper_source_path"], ROOT, CONFIG)

        self.assertEqual(report["finding_count"], 0)

    def test_reader_projection_strips_delimited_author_annotations(self):
        source = (ROOT / CONFIG["paper_source_path"]).read_text(encoding="utf-8")

        reader_text = strip_author_annotations(source)

        self.assertIn("<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->", source)
        self.assertIn("<!-- TXNMEM-AUTHOR-ANNOTATIONS:END -->", source)
        self.assertNotIn("[[CLAIM:", reader_text)
        self.assertNotIn("deterministic controlled simulator evidence", reader_text)
        self.assertNotIn("作者侧证据注释", reader_text)

    def test_task_three_source_contract(self):
        source = (ROOT / CONFIG["paper_source_path"]).read_text(encoding="utf-8")
        reader_text = strip_author_annotations(source)
        abstract = reader_text.split("# 摘要\n", 1)[1].split("关键词：", 1)[0]
        introduction = reader_text.split("# 1 引言", 1)[1].split("# 2 背景与动机", 1)[0]

        self.assertGreaterEqual(len(re.findall(r"[\u4e00-\u9fff]", abstract)), 450)
        self.assertLessEqual(len(re.findall(r"[\u4e00-\u9fff]", abstract)), 650)
        self.assertEqual(
            len([part for part in re.split(r"[。！？]", abstract) if part.strip()]), 6
        )
        self.assertEqual(len(re.findall(r"^- ", introduction, re.MULTILINE)), 4)
        self.assertTrue(all(term in introduction for term in ("地址", "订单", "崩溃", "撤回", "源记录")))
        self.assertEqual(
            set(re.findall(r"\[\[(?:FIG|TABLE):[^]]+\]\]", reader_text)),
            {
                "[[FIG:motivation_timeline]]",
                "[[FIG:architecture]]",
                "[[FIG:commit_protocol]]",
                "[[FIG:provenance_repair]]",
                "[[TABLE:requirements_gap]]",
            },
        )
        self.assertTrue(
            all(
                term in reader_text
                for term in (
                    "F=(A,M,P,T,G)",
                    "独立的 serial reference semantics",
                    "合法线性化",
                    "I-atomicity",
                    "I-commit authorization",
                    "I-scope safety",
                    "I-supersession consistency",
                    "I-provenance closure",
                    "I-recovery consistency",
                    "Agent Memory Transaction",
                    "Policy-Consistent Commit",
                    "Provenance-Driven Repair",
                    "procedure COMMIT(tx)",
                    "procedure REPAIR(seed, reason)",
                )
            )
        )
        self.assertIn("确定性 TxnMem core/reference simulator", reader_text)
        self.assertIn("逐个 memory event", reader_text)
        self.assertIn("invalidation-only", reader_text)
        self.assertIn("stale/recompute 是未来扩展", reader_text)
        self.assertIn("derived_writes", reader_text)
        self.assertIn("supersession_writes", reader_text)


if __name__ == "__main__":
    unittest.main()
