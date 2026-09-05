from __future__ import annotations

import hashlib
import json
import copy
import sys
import unittest
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_txnmem_paper_figures import (  # noqa: E402
    REQUIRED_FIGURE_IDS,
    _provenance_performance_scaling,
    build_all,
)


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
                    "provenance_performance_scaling",
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

    def test_evidence_figure_reports_state_verified_toxiproxy_boundary(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            evidence = (
                out_dir / manifest["figures"]["evidence_layers"]["file"]
            ).read_text(encoding="utf-8")
            item = manifest["figures"]["evidence_layers"]

        self.assertIn("90 complete / 60 absent", evidence)
        self.assertIn("0 partial / 0 unknown", evidence)
        self.assertIn("操作后双存储回读", item["caption"])
        self.assertIn("五个单机场景", item["caption"])
        self.assertIn("不代表一般分布式事务", item["alt_text"])
        self.assertNotIn("未独立核验双存储状态", evidence)

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

    def test_unrotated_svg_text_stays_inside_each_viewbox(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)

            for figure_id, item in manifest["figures"].items():
                with self.subTest(figure_id=figure_id):
                    root = self._svg_root(out_dir / item["file"])
                    canvas_width = float(root.attrib["width"])
                    for label in root.findall(
                        "{http://www.w3.org/2000/svg}text"
                    ):
                        if "transform" in label.attrib:
                            continue
                        content = "".join(label.itertext())
                        font_size = float(label.attrib["font-size"])
                        estimated_width = sum(
                            font_size
                            * (
                                1.0
                                if unicodedata.east_asian_width(character) in {"W", "F"}
                                else 0.62
                            )
                            for character in content
                        )
                        x = float(label.attrib["x"])
                        anchor = label.attrib.get("text-anchor", "start")
                        if anchor == "middle":
                            left, right = x - estimated_width / 2, x + estimated_width / 2
                        elif anchor == "end":
                            left, right = x - estimated_width, x
                        else:
                            left, right = x, x + estimated_width
                        self.assertGreaterEqual(left, -1.0, content)
                        self.assertLessEqual(right, canvas_width + 1.0, content)

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
                "results/submission_evidence/toxiproxy_state_verified_30/aggregate.json",
                "results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json",
            }.issubset(evidence_sources)
        )

    def test_v10_scaling_figure_is_a_complete_measurement_results_projection(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            item = manifest["figures"]["provenance_performance_scaling"]
            svg = (out_dir / item["file"]).read_text(encoding="utf-8")

        self.assertEqual(
            {source["path"] for source in item["sources"]},
            {"results/provenance_performance_v10_measurements/aggregate.json"},
        )
        self.assertEqual(svg.count('class="throughput-point"'), 15)
        self.assertEqual(svg.count('class="ci-whisker"'), 15)
        for label in (
            "100 nodes",
            "1,000 nodes",
            "10,000 nodes",
            "并发数",
            "吞吐（ops/s，对数刻度）",
            "峰值 21.899",
            "峰值 2.844",
            "峰值 0.123",
        ):
            self.assertIn(label, svg)
        self.assertIn("whole-repetition bootstrap 95% CI", item["caption"])
        self.assertIn("v10 测量矩阵", item["alt_text"])
        for overclaim in ("终验通过", "正式成功", "promotion", "生产级"):
            self.assertNotIn(overclaim, svg + item["caption"] + item["alt_text"])

    def test_v10_scaling_legend_stays_inside_the_svg_canvas(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "figures"
            manifest = build_all(ROOT, out_dir)
            root = self._svg_root(
                out_dir
                / manifest["figures"]["provenance_performance_scaling"]["file"]
            )

        namespace = {"svg": "http://www.w3.org/2000/svg"}
        legend_groups = root.findall(".//svg:g[@class='series-legend']", namespace)
        self.assertEqual(len(legend_groups), 3)
        canvas_width = float(root.attrib["width"])
        for legend in legend_groups:
            labels = legend.findall("svg:text", namespace)
            self.assertEqual(len(labels), 2)
            self.assertEqual(len({float(label.attrib["y"]) for label in labels}), 2)
            for label in labels:
                # A conservative full-em estimate catches labels whose declared
                # anchor leaves them outside the SVG viewBox.
                estimated_right = float(label.attrib["x"]) + len(label.text or "") * float(
                    label.attrib["font-size"]
                )
                self.assertLessEqual(estimated_right, canvas_width)

    def test_v10_scaling_peak_annotations_follow_a_valid_source_revision(self):
        source = json.loads(
            (
                ROOT
                / "results/provenance_performance_v10_measurements/aggregate.json"
            ).read_text(encoding="utf-8")
        )
        revised = copy.deepcopy(source)
        revised_cell = next(
            cell
            for cell in revised["cells"]
            if cell["graph_node_count"] == 100 and cell["concurrency"] == 4
        )
        revised_cell["successful_throughput_ops_per_second"] = 30.0
        revised_cell["throughput_95ci"] = {"lower": 29.0, "upper": 31.0}

        with TemporaryDirectory() as tmp:
            source_path = (
                Path(tmp)
                / "results/provenance_performance_v10_measurements/aggregate.json"
            )
            source_path.parent.mkdir(parents=True)
            source_path.write_text(json.dumps(revised), encoding="utf-8")
            svg, _, alt_text, _, _ = _provenance_performance_scaling(Path(tmp))

        self.assertIn("峰值 30.000", svg)
        self.assertNotIn("峰值 21.899", svg)
        self.assertIn("100 nodes 在并发 4 达到被测峰值 30.000 ops/s", alt_text)

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
