from __future__ import annotations

import json
import re
import hashlib
import shutil
import sys
import unittest
from collections import Counter
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
from txnmem_claim_audit import audit_claim_ledger  # noqa: E402


CONFIG = load_paper_config(ROOT / "configs" / "txnmem_ccfa_paper.json")


class ManuscriptAuditTests(unittest.TestCase):
    TASK_THREE_MARKERS = (
        "[[FIG:motivation_timeline]]",
        "[[TABLE:requirements_gap]]",
        "[[FIG:architecture]]",
        "[[FIG:commit_protocol]]",
        "[[FIG:provenance_repair]]",
    )
    TASK_FOUR_MARKERS = (
        "[[TABLE:system_invariants]]",
        "[[TABLE:workload_family]]",
        "[[TABLE:experimental_setup]]",
        "[[FIG:controlled_results]]",
        "[[TABLE:controlled_results]]",
        "[[FIG:evidence_layers]]",
        "[[TABLE:runtime_results]]",
    )
    APPENDIX_MARKERS = (
        "[[TABLE:claim_ledger]]",
        "[[TABLE:workload_schema]]",
    )

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

    def _isolated_evidence_root(self, destination: Path) -> dict:
        ledger_rel = Path(CONFIG["claim_ledger_path"])
        ledger = json.loads((ROOT / ledger_rel).read_text(encoding="utf-8"))
        ledger["expected_active_claim_count"] = sum(
            claim.get("status") == "active" for claim in ledger["claims"]
        )
        ledger["expected_assertion_count"] = sum(
            len(claim["assertions"])
            for claim in ledger["claims"]
            if claim.get("status") == "active"
        )

        paths = {
            Path(CONFIG["supersession_index_path"]),
            Path("paper_assets/figures/manifest.json"),
        }
        for claim in ledger["claims"]:
            paths.add(Path(claim["artifact_path"]))
            paths.add(Path(claim["manifest"]["path"]))
        figure_manifest = json.loads(
            (ROOT / "paper_assets/figures/manifest.json").read_text(encoding="utf-8")
        )
        for figure in figure_manifest["figures"].values():
            paths.add(Path("paper_assets/figures") / figure["file"])
            paths.update(Path(source["path"]) for source in figure["sources"])
        for relative in sorted(paths):
            source = ROOT / relative
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

        target_ledger = destination / ledger_rel
        target_ledger.parent.mkdir(parents=True, exist_ok=True)
        target_ledger.write_text(
            json.dumps(ledger, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        ledger_hash = hashlib.sha256(target_ledger.read_bytes()).hexdigest()
        target_figure_manifest = destination / "paper_assets/figures/manifest.json"
        figure_manifest = json.loads(target_figure_manifest.read_text(encoding="utf-8"))
        for figure in figure_manifest["figures"].values():
            for source in figure["sources"]:
                if source["path"] == str(ledger_rel):
                    source["sha256"] = ledger_hash
        target_figure_manifest.write_text(
            json.dumps(figure_manifest, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

        report = audit_claim_ledger(destination, ledger_rel)
        self.assertEqual(report["status"], "passed", report["findings"])
        claim_audit = destination / CONFIG["claim_audit_path"]
        claim_audit.parent.mkdir(parents=True, exist_ok=True)
        claim_audit.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return figure_manifest

    def _assert_task_three_markers(self, reader_text: str) -> None:
        self.assertEqual(
            re.findall(r"\[\[(?:FIG|TABLE):[^]]+\]\]", reader_text),
            list(self.TASK_THREE_MARKERS),
        )

    def _assert_all_configured_markers(self, reader_text: str) -> None:
        expected = {
            *(f"[[FIG:{marker}]]" for marker in CONFIG["body_figure_ids"]),
            *(f"[[TABLE:{marker}]]" for marker in CONFIG["body_table_ids"]),
            *(f"[[FIG:{marker}]]" for marker in CONFIG["appendix_figure_ids"]),
            *(f"[[TABLE:{marker}]]" for marker in CONFIG["appendix_table_ids"]),
        }
        observed = re.findall(r"\[\[(?:FIG|TABLE):[^]]+\]\]", reader_text)
        self.assertEqual(
            Counter(observed),
            Counter({marker: 1 for marker in expected}),
        )

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

    def test_rejects_stale_passed_claim_report_even_with_current_ledger_digest(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._isolated_evidence_root(root)
            audit_path = root / CONFIG["claim_audit_path"]
            payload = json.loads(audit_path.read_text(encoding="utf-8"))
            payload["checked_assertion_count"] -= 1
            payload["status"] = "passed"
            payload["finding_count"] = 0
            audit_path.write_text(json.dumps(payload), encoding="utf-8")

            report = audit_text(self._valid_manuscript_text(), root, CONFIG)

        self.assertIn(
            "claim_audit_stale_report", {item["code"] for item in report["findings"]}
        )

    def test_rejects_active_artifact_byte_drift(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._isolated_evidence_root(root)
            artifact = root / "results/paper_evidence/controlled_suite.json"
            artifact.write_bytes(artifact.read_bytes() + b"\n")

            report = audit_text(self._valid_manuscript_text(), root, CONFIG)

        self.assertIn(
            "fresh_claim_audit_failed", {item["code"] for item in report["findings"]}
        )

    def test_rejects_active_claim_manifest_byte_drift(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._isolated_evidence_root(root)
            manifest = root / "configs/workload_families.yaml"
            manifest.write_bytes(manifest.read_bytes() + b"\n")

            report = audit_text(self._valid_manuscript_text(), root, CONFIG)

        self.assertIn(
            "fresh_claim_audit_failed", {item["code"] for item in report["findings"]}
        )

    def test_rejects_figure_source_byte_drift(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._isolated_evidence_root(root)
            source = root / CONFIG["paper_source_path"]
            source.write_bytes(source.read_bytes() + b"\n")

            report = audit_text(self._valid_manuscript_text(), root, CONFIG)

        self.assertIn(
            "figure_source_hash_mismatch",
            {item["code"] for item in report["findings"]},
        )

    def test_rejects_figure_output_byte_drift(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._isolated_evidence_root(root)
            output = root / "paper_assets/figures/architecture.svg"
            output.write_bytes(output.read_bytes() + b"\n")

            report = audit_text(self._valid_manuscript_text(), root, CONFIG)

        self.assertIn(
            "figure_output_hash_mismatch",
            {item["code"] for item in report["findings"]},
        )

    def test_drafting_mode_only_allows_deferred_sections_to_be_absent(self):
        config = dict(CONFIG)
        config["drafting_mode"] = True
        text = "\n\n".join(
            f"# {section}"
            for section in config["required_sections"]
            if section not in config["drafting_optional_sections"]
        )
        text += "\n\n" + self._claim_blocks()

        report = audit_text(text, root=ROOT, config=config)

        self.assertNotIn(
            "missing_required_section", {item["code"] for item in report["findings"]}
        )

    def test_current_draft_conforms_to_manuscript_contract(self):
        report = audit_manuscript(ROOT / CONFIG["paper_source_path"], ROOT, CONFIG)

        self.assertEqual(report["finding_count"], 0)

    def test_toxiproxy_reader_and_status_surfaces_make_no_state_or_atomicity_claim(self):
        paths = (
            ROOT / CONFIG["paper_source_path"],
            ROOT / "docs/current_experiment_report_zh.md",
            ROOT / "docs/formal_paper_task_status_zh.md",
        )
        forbidden = (
            "0 partial commit",
            "partial commit 为 0",
            "无 partial-commit",
            "均无 partial commit",
            "未观察到部分提交",
        )
        for path in paths:
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                for phrase in forbidden:
                    self.assertNotIn(phrase, text)
        boundary = (
            "single-host proxy/fault-response observations; post-fault Qdrant/Neo4j "
            "persistent state was not independently verified; "
            "not atomicity/availability/latency evidence"
        )
        self.assertIn(boundary, CONFIG["required_claim_boundaries"])

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
        self._assert_task_three_markers(reader_text.split("# 6 评估", 1)[0])
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

    def test_task_three_marker_contract_rejects_duplicate_marker(self):
        reader_text = "\n".join(
            (*self.TASK_THREE_MARKERS, "[[FIG:architecture]]")
        )

        with self.assertRaises(AssertionError):
            self._assert_task_three_markers(reader_text)

    def test_task_four_source_contract(self):
        source = (ROOT / CONFIG["paper_source_path"]).read_text(encoding="utf-8")
        reader_text = strip_author_annotations(source)
        evaluation = reader_text.split("# 6 评估", 1)[1].split("# 7 讨论与局限性", 1)[0]
        related_work = reader_text.split("# 8 相关工作", 1)[1].split("# 9 结论", 1)[0]
        references = reader_text.split("# 参考文献", 1)[1].split("# 附录", 1)[0]

        self.assertFalse(CONFIG["drafting_mode"])
        self.assertIn("controlled_mutation_matrix_350", CONFIG["active_claim_ids"])
        self.assertIn(
            "controlled mutation-matrix sensitivity over 350 variant-instance cases; not a production defect rate or universal mutant-coverage claim",
            CONFIG["required_claim_boundaries"],
        )
        self._assert_all_configured_markers(reader_text)
        self.assertTrue(
            all(rq in evaluation for rq in ("RQ1", "RQ2", "RQ3", "RQ4", "RQ5"))
        )
        self.assertTrue(
            all(
                term in source
                for term in (
                    "400 个实例和 2,000 条变体结果",
                    "0/400",
                    "400/400",
                    "350/400",
                    "200/400",
                    "50/400",
                    "100/400",
                    "0.875",
                    "4,000",
                    "0.750",
                    "300/350",
                    "0.8571428571428571",
                    "2、1、6、1",
                    "五次、每次十个 task",
                    "50/50 contract",
                    "50/50 oracle",
                    "110 个 native event",
                    "6 个 execution failure",
                    "+0.0016169580043333333",
                    "5×30",
                    "仅记录经代理的故障与响应路径",
                    "持久双存储状态留待重新核验",
                    "5/5",
                    "30 个 native event",
                    "MMD²",
                    "generator 仍需校准",
                )
            )
        )
        self.assertIn("四个 non-normal 路径", evaluation)
        self.assertIn("不是原子性、可用性或延迟证据", evaluation)
        self.assertIn("一个 Agent-worker host 到一个 model-server host", evaluation)
        self.assertTrue(
            all(term in related_work for term in ("Agent memory", "governed memory/access control", "transaction/provenance", "distributed-system testing"))
        )
        self.assertTrue(all(f"[R{number:02d}]" in related_work for number in range(1, 33)))
        self.assertIn("configs/txnmem_paper_references.json", references)
        self.assertIn("[R01]--[R32]", references)
        self.assertTrue(
            all(
                phrase not in reader_text
                for phrase in ("接近真实分布", "证明等价", "分布等价证明", "生产级性能")
            )
        )


if __name__ == "__main__":
    unittest.main()
