from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from txnmem_paper_projection import controlled_result_rows  # noqa: E402
from build_txnmem_ccfa_docx import _controlled_rows  # noqa: E402


class ControlledResultProjectionTests(unittest.TestCase):
    def test_all_five_markdown_and_docx_rows_match_controlled_artifact(self):
        evidence = json.loads(
            (ROOT / "results/paper_evidence/controlled_suite.json").read_text(
                encoding="utf-8"
            )
        )
        projected = controlled_result_rows(ROOT)
        expected = [
            {
                "variant": row["variant"],
                "violation": f"{evidence['variants'][row['variant']]['violation_count']}/400",
                "oracle": f"{evidence['variants'][row['variant']]['oracle_match_count']}/400",
            }
            for row in projected
        ]

        source = (ROOT / "docs/paper/txnmem_ccfa_draft_zh.md").read_text(
            encoding="utf-8"
        )
        table = source.split("`[[TABLE:controlled_results]]`", 1)[1].split(
            "<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->", 1
        )[0]
        markdown_rows = []
        for line in table.splitlines():
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) == 4 and cells[0] in evidence["variants"]:
                markdown_rows.append(
                    {"variant": cells[0], "violation": cells[1], "oracle": cells[2]}
                )

        _, docx_rows = _controlled_rows(ROOT)
        normalized_docx = [
            {"variant": row[0], "violation": row[1], "oracle": row[2]}
            for row in docx_rows
        ]
        self.assertEqual(len(expected), 5)
        self.assertEqual(markdown_rows, expected)
        self.assertEqual(normalized_docx, expected)
        self.assertIn(
            {"variant": "TxnMem-NoRepair", "violation": "100/400", "oracle": "300/400"},
            expected,
        )


if __name__ == "__main__":
    unittest.main()
