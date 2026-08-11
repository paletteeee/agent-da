from __future__ import annotations

from pathlib import Path
import sys
import unittest
import zipfile
from xml.etree import ElementTree as ET

from docx import Document
from docx.oxml.ns import qn


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT.parent.parent.parent.parent / "outputs" / "TxnMem_CCF-A中文论文初稿.docx"
sys.path.insert(0, str(ROOT / "scripts"))

from build_txnmem_ccfa_docx import build_document  # noqa: E402


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
WP_NS = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
DOC_PR_NS = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
NS = {"w": W_NS, "wp": WP_NS}
MARKERS = (
    "[[FIG:",
    "[[TABLE:",
    "[[CLAIM:",
    "TXNMEM-AUTHOR-ANNOTATIONS",
    "BIBLIOGRAPHY",
    "TODO",
    "\\(",
    "\\)",
    "configs/txnmem_paper_references.json",
)


class TxnMemCcfaDocxTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.path = build_document(ROOT, OUT)
        cls.document = Document(cls.path)
        with zipfile.ZipFile(cls.path) as archive:
            cls.parts = {name: archive.read(name) for name in archive.namelist()}
        cls.document_xml = cls.parts["word/document.xml"].decode("utf-8")

    def test_generated_docx_has_paper_structure(self) -> None:
        self.assertTrue(self.path.is_file())
        self.assertGreater(self.path.stat().st_size, 100_000)
        headings = [p.text for p in self.document.paragraphs if p.style.name.startswith("Heading")]
        self.assertIn("1 引言", headings)
        self.assertIn("6 评估", headings)
        self.assertGreaterEqual(len(self.document.inline_shapes), 6)
        self.assertGreaterEqual(len(self.document.tables), 8)

    def test_first_page_starts_as_anonymous_paper_not_report_cover(self) -> None:
        opening = [p.text.strip() for p in self.document.paragraphs[:6] if p.text.strip()]
        self.assertEqual(opening[0], "TxnMem：面向多 Agent 共享记忆的策略感知事务运行时")
        self.assertIn("匿名稿", opening)
        self.assertIn("摘要", opening)
        self.assertIn("TxnMem: A Policy-Aware Transactional Runtime for Shared Memory in Multi-Agent Systems", self.document_xml)
        self.assertNotIn("Prepared for", self.document_xml)

    def test_reader_facing_text_contains_no_source_handoff_or_annotation_markers(self) -> None:
        text = "\n".join(p.text for p in self.document.paragraphs)
        for marker in MARKERS:
            self.assertNotIn(marker, text)
        self.assertNotIn("图：", "\n".join(p.text for p in self.document.paragraphs if "图：" in p.text))

    def test_styles_lists_and_captions_are_word_native(self) -> None:
        normal = self.document.styles["Normal"]
        self.assertEqual(normal.font.name, "Calibri")
        self.assertEqual(normal.font.size.pt, 11)
        self.assertEqual(self.document.styles["Heading 1"].font.size.pt, 16)
        self.assertEqual(self.document.styles["Heading 2"].font.size.pt, 13)
        self.assertEqual(self.document.styles["Heading 3"].font.size.pt, 12)
        self.assertTrue(any(p.style.name == "Caption" for p in self.document.paragraphs))
        self.assertTrue(any(p.style.name.startswith("List") for p in self.document.paragraphs))

    def test_figures_have_manifest_alt_text_and_captions(self) -> None:
        document_root = ET.fromstring(self.parts["word/document.xml"])
        drawings = document_root.findall(".//wp:docPr", NS)
        self.assertEqual(len(drawings), 6)
        for drawing in drawings:
            self.assertTrue(drawing.attrib.get("descr"))
        captions = [p.text for p in self.document.paragraphs if p.style.name == "Caption"]
        self.assertEqual(len([caption for caption in captions if caption.startswith("图")]), 6)

    def test_tables_have_fixed_geometry_repeating_headers_and_titles(self) -> None:
        document_root = ET.fromstring(self.parts["word/document.xml"])
        tables = document_root.findall(".//w:tbl", NS)
        self.assertGreaterEqual(len(tables), 8)
        repeated_headers = 0
        for table in tables:
            tbl_width = table.find("./w:tblPr/w:tblW", NS)
            self.assertIsNotNone(tbl_width)
            self.assertEqual(tbl_width.attrib[f"{{{W_NS}}}w"], "9360")
            widths = table.findall("./w:tblGrid/w:gridCol", NS)
            self.assertTrue(widths)
            self.assertTrue(all(int(width.attrib[f"{{{W_NS}}}w"]) > 0 for width in widths))
            self.assertEqual(sum(int(width.attrib[f"{{{W_NS}}}w"]) for width in widths), 9360)
            for cell in table.findall(".//w:tc", NS):
                tc_width = cell.find("./w:tcPr/w:tcW", NS)
                self.assertIsNotNone(tc_width)
                self.assertIn(int(tc_width.attrib[f"{{{W_NS}}}w"]),
                              [int(width.attrib[f"{{{W_NS}}}w"]) for width in widths])
            first_row = table.find("./w:tr", NS)
            self.assertIsNotNone(first_row)
            if first_row.find("./w:trPr/w:tblHeader", NS) is not None:
                repeated_headers += 1
        self.assertEqual(repeated_headers, len(tables))
        captions = [p.text for p in self.document.paragraphs if p.style.name == "Caption"]
        self.assertGreaterEqual(len([caption for caption in captions if caption.startswith("表")]), 8)

    def test_references_are_complete_and_stably_ordered(self) -> None:
        text = "\n".join(p.text for p in self.document.paragraphs)
        self.assertIn("参考文献", text)
        for number in range(1, 33):
            self.assertIn(f"[R{number:02d}]", text)
        self.assertEqual(sum(1 for p in self.document.paragraphs if p.text.startswith("[R")), 32)

    def test_page_field_and_anonymous_metadata_are_present(self) -> None:
        footer_xml = b"".join(content for name, content in self.parts.items() if name.startswith("word/footer"))
        self.assertIn(b"PAGE", footer_xml)
        core = self.parts["docProps/core.xml"].decode("utf-8")
        self.assertNotIn("xiaoyan", core.lower())
        self.assertNotIn("agent-db", core.lower())
        core_root = ET.fromstring(core)
        creator = core_root.find("{http://purl.org/dc/elements/1.1/}creator")
        modified_by = core_root.find(
            "{http://schemas.openxmlformats.org/package/2006/metadata/core-properties}lastModifiedBy"
        )
        self.assertIn(creator.text, (None, ""))
        self.assertIn(modified_by.text, (None, ""))


if __name__ == "__main__":
    unittest.main()
