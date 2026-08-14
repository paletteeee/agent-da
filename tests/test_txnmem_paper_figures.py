from __future__ import annotations

import hashlib
import json
import sys
import unittest
import xml.etree.ElementTree as ET
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

    def test_evidence_figure_downgrades_legacy_toxiproxy_state_claim(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            evidence = (
                out_dir / manifest["figures"]["evidence_layers"]["file"]
            ).read_text(encoding="utf-8")
            item = manifest["figures"]["evidence_layers"]

        self.assertNotIn("0 partial", evidence)
        self.assertIn("仅故障/响应路径", evidence)
        self.assertIn("未独立核验双存储状态", evidence)
        self.assertNotIn("partial commit", item["alt_text"])
        self.assertIn("状态未独立核验", item["caption"])

    def test_architecture_svg_encodes_revalidation_before_persist(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            architecture = self._svg_root(
                out_dir / manifest["figures"]["architecture"]["file"]
            )

        manager = self._labelled_box(architecture, "Transaction Manager")
        policy = self._labelled_box(architecture, "Policy Engine")
        store = self._labelled_box(architecture, "Memory Store")
        repair = self._labelled_box(architecture, "Provenance Repair")
        self.assertTrue(self._has_arrow(architecture, manager, policy))
        self.assertTrue(self._has_arrow(architecture, policy, store))
        self.assertFalse(self._has_arrow(architecture, manager, store))
        self.assertFalse(self._has_arrow(architecture, store, policy))
        self.assertTrue(self._has_arrow(architecture, repair, store))
        self.assertFalse(self._has_arrow(architecture, repair, policy))

    def test_motivation_risk_rectangles_do_not_overlap(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            timeline = self._svg_root(
                out_dir / manifest["figures"]["motivation_timeline"]["file"]
            )

        risk_boxes = [
            self._labelled_box(timeline, f"风险 {number}") for number in (1, 2, 3)
        ]
        for left_index, left in enumerate(risk_boxes):
            for right in risk_boxes[left_index + 1 :]:
                self.assertFalse(self._rectangles_overlap(left, right))

    def test_secondary_svg_labels_meet_one_column_size_floor(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)

            for figure_id, item in manifest["figures"].items():
                with self.subTest(figure_id=figure_id):
                    root = self._svg_root(out_dir / item["file"])
                    sizes = [
                        float(text.attrib["font-size"])
                        for text in root.findall("{http://www.w3.org/2000/svg}text")
                    ]
                    self.assertGreaterEqual(min(sizes), 15.0)

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

    @staticmethod
    def _svg_root(path: Path) -> ET.Element:
        return ET.fromstring(path.read_text(encoding="utf-8"))

    @staticmethod
    def _rectangles_overlap(
        left: tuple[float, float, float, float],
        right: tuple[float, float, float, float],
    ) -> bool:
        left_x, left_y, left_width, left_height = left
        right_x, right_y, right_width, right_height = right
        return (
            max(left_x, right_x) < min(left_x + left_width, right_x + right_width)
            and max(left_y, right_y) < min(left_y + left_height, right_y + right_height)
        )

    @staticmethod
    def _point_in_or_on(
        point: tuple[float, float], bounds: tuple[float, float, float, float]
    ) -> bool:
        x, y = point
        left, top, width, height = bounds
        epsilon = 0.01
        return (
            left - epsilon <= x <= left + width + epsilon
            and top - epsilon <= y <= top + height + epsilon
        )

    @classmethod
    def _has_arrow(
        cls,
        root: ET.Element,
        source: tuple[float, float, float, float],
        target: tuple[float, float, float, float],
    ) -> bool:
        for line in root.findall("{http://www.w3.org/2000/svg}line"):
            if line.attrib.get("stroke") != "#1F4E79" or line.attrib.get("stroke-width") != "2":
                continue
            start = (float(line.attrib["x1"]), float(line.attrib["y1"]))
            end = (float(line.attrib["x2"]), float(line.attrib["y2"]))
            if cls._point_in_or_on(start, source) and cls._point_in_or_on(end, target):
                return True
        return False

    @staticmethod
    def _labelled_box(root: ET.Element, label_fragment: str) -> tuple[float, float, float, float]:
        text = next(
            item
            for item in root.findall("{http://www.w3.org/2000/svg}text")
            if label_fragment in (item.text or "")
        )
        x, y = float(text.attrib["x"]), float(text.attrib["y"])
        candidates = []
        for rect in root.findall("{http://www.w3.org/2000/svg}rect"):
            bounds = tuple(float(rect.attrib[field]) for field in ("x", "y", "width", "height"))
            if PaperFigureBuilderTests._point_in_or_on((x, y), bounds):
                candidates.append(bounds)
        if not candidates:
            raise AssertionError(f"no enclosing rectangle for {label_fragment}")
        return min(candidates, key=lambda bounds: bounds[2] * bounds[3])
