from __future__ import annotations

import hashlib
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_txnmem_paper_figures import REQUIRED_FIGURE_IDS, build_all  # noqa: E402


class PaperFigureBuilderTests(unittest.TestCase):
    def test_all_required_figures_are_generated_with_complete_manifest(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)

            self.assertEqual(set(manifest["figures"]), set(REQUIRED_FIGURE_IDS))
            self.assertEqual(
                set(manifest["figures"]),
                {
                    "motivation_timeline",
                    "architecture",
                    "commit_protocol",
                    "provenance_repair",
                    "controlled_results",
                    "evidence_layers",
                },
            )
            for figure_id, item in manifest["figures"].items():
                with self.subTest(figure_id=figure_id):
                    output = out_dir / item["file"]
                    self.assertTrue(output.is_file())
                    self.assertGreater(output.stat().st_size, 0)
                    self.assertEqual(item["dimensions"].keys(), {"width", "height"})
                    self.assertTrue(item["caption"])
                    self.assertTrue(item["alt_text"])
                    self.assertEqual(item["output_sha256"], self._sha256(output))
                    self.assertTrue(item["sources"])
                    for source in item["sources"]:
                        source_path = ROOT / source["path"]
                        self.assertTrue(source_path.is_file())
                        self.assertEqual(source["sha256"], self._sha256(source_path))

            written_manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(written_manifest, manifest)

    def test_outputs_and_hashes_are_deterministic(self):
        with TemporaryDirectory() as first_tmp, TemporaryDirectory() as second_tmp:
            first = build_all(ROOT, Path(first_tmp) / "figures")
            second = build_all(ROOT, Path(second_tmp) / "figures")

        self.assertEqual(first, second)

    def test_svg_assets_are_code_native_and_have_no_replacement_glyphs(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)

            for figure_id, item in manifest["figures"].items():
                with self.subTest(figure_id=figure_id):
                    svg = (out_dir / item["file"]).read_text(encoding="utf-8")
                    self.assertIn("<svg", svg)
                    self.assertNotIn("\ufffd", svg)
                    self.assertNotIn("&#xfffd;", svg.lower())
                    self.assertNotIn("<image", svg)
                    self.assertNotIn("linearGradient", svg)
                    self.assertNotIn("<filter", svg)
                    self.assertIn("Arial Unicode MS", svg)
                    self.assertIn("Hiragino Sans GB", svg)

    def test_figure_semantics_do_not_overclaim_transactions_or_future_repair(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            architecture = (out_dir / manifest["figures"]["architecture"]["file"]).read_text(encoding="utf-8")
            protocol = (out_dir / manifest["figures"]["commit_protocol"]["file"]).read_text(encoding="utf-8")
            repair = (out_dir / manifest["figures"]["provenance_repair"]["file"]).read_text(encoding="utf-8")

        self.assertIn("非事务适配", architecture)
        self.assertIn("不提供事务", architecture)
        self.assertIn("最新策略重验证", protocol)
        self.assertIn("完整提交 / 未提交", protocol)
        self.assertNotIn("部分提交", protocol)
        self.assertIn("当前：仅后代失效", repair)
        self.assertIn("未来扩展", repair)
        self.assertIn("stale / 重算 / 新来源", repair)

    def test_architecture_adapter_boxes_start_below_their_panel_header(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            architecture = (out_dir / manifest["figures"]["architecture"]["file"]).read_text(encoding="utf-8")

        self.assertIn('x="72.0" y="135.0"', architecture)
        self.assertIn('x="68.0" y="455.0"', architecture)

    def test_manifest_declares_required_source_dependencies(self):
        with TemporaryDirectory() as tmp:
            manifest = build_all(ROOT, Path(tmp) / "figures")

        controlled_sources = {item["path"] for item in manifest["figures"]["controlled_results"]["sources"]}
        evidence_sources = {item["path"] for item in manifest["figures"]["evidence_layers"]["sources"]}
        self.assertEqual(controlled_sources, {"results/paper_evidence/controlled_suite.json"})
        self.assertTrue(
            {
                "configs/txnmem_ccfa_paper.json",
                "configs/paper_claims.json",
                "results/final_controlled/results/schedule_baseline.json",
                "results/final_controlled/results/minimal_mutant_witnesses.json",
                "results/submission_evidence/toxiproxy_faults_30/aggregate.json",
                "results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json",
            }.issubset(evidence_sources)
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()
