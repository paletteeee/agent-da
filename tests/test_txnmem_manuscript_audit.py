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

    def test_accepts_number_from_nested_active_claim_assertion(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / "configs/paper_claims.json"
            ledger.parent.mkdir(parents=True)
            ledger.write_text(
                json.dumps(
                    {
                        "claims": [
                            {
                                "claim_id": "nested_count",
                                "status": "active",
                                "claim_boundary": "nested count evidence only",
                                "assertions": [
                                    {
                                        "pointer": "/counts",
                                        "operator": "equals",
                                        "expected": {
                                            "attempted": 42,
                                            "feature_enabled": True,
                                            "numeric_label": "77",
                                        },
                                    }
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config = {
                "claim_ledger_path": "configs/paper_claims.json",
                "active_claim_ids": ["nested_count"],
                "required_claim_boundaries": ["nested count evidence only"],
            }

            report = audit_text(
                "实验结果为 42。[[CLAIM:nested_count]] nested count evidence only",
                root=root,
                config=config,
            )

        self.assertNotIn(
            "uncovered_claim_value", {item["code"] for item in report["findings"]}
        )
        self.assertEqual(report["allowed_numeric_values"], ["42"])

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

    def test_evidence_map_covers_every_configured_manuscript_claim(self):
        evidence_map = (ROOT / "docs/paper/evidence_map_zh.md").read_text(
            encoding="utf-8"
        )

        missing_claim_ids = [
            claim_id
            for claim_id in CONFIG["active_claim_ids"]
            if f"`{claim_id}`" not in evidence_map
        ]

        self.assertEqual(missing_claim_ids, [])

    def test_external_baseline_reader_text_preserves_denominator_boundary(self):
        source = (ROOT / CONFIG["paper_source_path"]).read_text(encoding="utf-8")
        reader_text = strip_author_annotations(source)
        external_baseline = reader_text.split(
            "### 外部系统的可观察正确性对照", 1
        )[1].split("## 6.2 RQ2", 1)[0]

        for phrase in (
            "unsupported mapping 与 runtime attempt 均不进入正确性分母",
            "不能据此声称第三方系统存在安全缺陷",
            "不能外推至一般生产行为",
        ):
            self.assertIn(phrase, external_baseline)

    def test_state_verified_toxiproxy_surfaces_report_narrow_readback_claim(self):
        paths = (
            ROOT / CONFIG["paper_source_path"],
            ROOT / "docs/current_experiment_report_zh.md",
            ROOT / "docs/formal_paper_task_status_zh.md",
            ROOT / "docs/paper/evidence_map_zh.md",
        )
        required = (
            "results/submission_evidence/toxiproxy_state_verified_30/aggregate.json",
            "5 个场景 × 30 次重复 = 150 次观测",
            "完整回读 90/90",
            "缺失回读 60/60",
            "`partial` 0/150",
            "`unknown` 0/150",
            "`retry_success` 30/30",
            "两个唯一 memory ID",
            "Qdrant 与 Neo4j",
            "重新计算",
            "p50 25.748 ms",
            "p95 32.029 ms",
            "p99 42.234 ms",
            "76.256 operations/s",
            "`production_latency_claim=false`",
        )
        old_boundary = (
            "post-fault Qdrant/Neo4j persistent state was not independently verified"
        )
        forbidden = (
            "0 partial commit",
            "partial commit 为 0",
            "无 partial-commit",
            "均无 partial commit",
            "未观察到部分提交",
            "atomicity proven",
            "atomicity is proven",
            "proves atomicity",
            "原子性已证明",
            "证明了原子性",
            "原子性得到证明",
            "availability proven",
            "可用性已证明",
            "证明了可用性",
            "linearizability proven",
            "线性一致性已证明",
            "证明了线性一致性",
            "cross-host fault tolerance proven",
            "跨主机容错已证明",
            "跨主机容错已验证",
            "production latency proven",
            "生产延迟已证明",
            "生产延迟已验证",
        )
        for path in paths:
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                for phrase in required:
                    self.assertIn(phrase, text)
                self.assertNotIn(old_boundary, text)
                for phrase in forbidden:
                    self.assertNotIn(phrase, text)
                self.assertNotRegex(text, r"(?<!\d)346(?!\d)")
        boundary = (
            "single-host real Qdrant/Neo4j with deterministic Toxiproxy fault injection "
            "and post-operation readback for the tested workload and five scenarios; "
            "not general distributed transactions, cross-host fault tolerance, "
            "availability, linearizability, or production latency"
        )
        self.assertIn(boundary, CONFIG["required_claim_boundaries"])
        for path in paths:
            with self.subTest(path=path, check="claim_boundary"):
                self.assertIn(boundary, path.read_text(encoding="utf-8"))

        report = (ROOT / "docs/current_experiment_report_zh.md").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "工作树全量单元测试：357 tests passed，3 个 optional-runtime skips，0 failures。",
            report,
        )

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
            all(
                rq in evaluation
                for rq in ("RQ1", "RQ2", "RQ3", "RQ4", "RQ5", "RQ6")
            )
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
                    "5 个场景 × 30 次重复 = 150 次观测",
                    "完整回读 90/90",
                    "缺失回读 60/60",
                    "`partial` 0/150",
                    "`unknown` 0/150",
                    "`retry_success` 30/30",
                    "5/5",
                    "30 个 native event",
                    "MMD²",
                    "generator 仍需校准",
                )
            )
        )
        self.assertIn("两个唯一 memory ID", evaluation)
        self.assertIn("重新计算", evaluation)
        self.assertIn("不是一般分布式事务协议", evaluation)
        self.assertIn("不支持跨主机容错、可用性、线性一致性或生产延迟结论", evaluation)
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
        self.assertIn("回答六个研究问题", reader_text)
        self.assertNotIn("回答五个研究问题", reader_text)

    def test_v10_measurement_results_table_and_scaling_analysis_are_source_bound(self):
        source = (ROOT / CONFIG["paper_source_path"]).read_text(encoding="utf-8")
        reader_text = strip_author_annotations(source)
        rq6 = reader_text.split("## 6.6 RQ6", 1)[1].split(
            "# 7 讨论与局限性", 1
        )[0]
        projection = json.loads(
            (
                ROOT
                / "results/paper_evidence/provenance_performance_v10.json"
            ).read_text(encoding="utf-8")
        )

        self.assertIn("我们围绕六个问题组织评估", reader_text)
        self.assertIn("`[[FIG:provenance_performance_scaling]]`", rq6)
        self.assertIn("`[[TABLE:provenance_performance_v10]]`", rq6)
        self.assertIn("measurement_results", rq6)
        self.assertIn("whole-repetition bootstrap 95% CI", rq6)

        table = rq6.split("`[[TABLE:provenance_performance_v10]]`", 1)[1].split(
            "<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->", 1
        )[0]
        observed_rows = []
        for line in table.splitlines():
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) == 7 and cells[0].replace(",", "").isdigit():
                observed_rows.append(cells)
        expected_rows = [
            [
                f"{row['graph_node_count']:,}",
                str(row["concurrency"]),
                f"{row['p50_ms']:,.3f}",
                f"{row['p95_ms']:,.3f}",
                f"{row['p99_ms']:,.3f}",
                f"{row['throughput_ops_per_second']:.6f}",
                "["
                f"{row['ci95_lower_ops_per_second']:.6f}, "
                f"{row['ci95_upper_ops_per_second']:.6f}"
                "]",
            ]
            for row in projection["cells"]
        ]
        self.assertEqual(observed_rows, expected_rows)
        for phrase in (
            "30.16%",
            "12.20%",
            "19.46%",
            "48.36%",
            "99.27%",
            "32,342.042 ms",
            "4.596 ms",
            "21.312 ms",
            "73.928 ms",
        ):
            self.assertIn(phrase, rq6)
        for directional_statement in (
            "较并发 1 增加 30.16%",
            "增幅为 12.20%",
            "增加到并发 2 后吞吐下降 19.46%",
            "较并发 1 下降 48.36%",
            "降幅为 99.27%",
        ):
            self.assertIn(directional_statement, rq6)
        for overclaim in ("终验通过", "正式成功", "promotion", "生产级性能"):
            self.assertNotIn(overclaim, rq6)


if __name__ == "__main__":
    unittest.main()
