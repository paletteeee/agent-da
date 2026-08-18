"""Reproducible task-level manifests for the public benchmark batch runner."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_benchmark_manifests import build_native_scale_manifest


class NativeScaleManifestTests(unittest.TestCase):
    def _locomo_source(self, directory: Path) -> Path:
        path = directory / "locomo.json"
        path.write_text(
            json.dumps(
                {
                    "samples": [
                        {"sample_id": f"sample-{index}", "conversation": {}}
                        for index in range(5)
                    ]
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_manifest_is_deterministic_and_contains_hash_and_task_split(self):
        with TemporaryDirectory() as tmp:
            source = self._locomo_source(Path(tmp))
            first = build_native_scale_manifest("locomo", source, limit=5, seed=17, split="test")
            second = build_native_scale_manifest("locomo", source, limit=5, seed=17, split="test")
        self.assertEqual(first, second)
        self.assertEqual(first["task_count"], 5)
        self.assertEqual(len(first["manifest_hash"]), 64)
        split = first["task_level_split"]
        self.assertTrue(set(split["train_task_ids"]).isdisjoint(split["holdout_task_ids"]))
        self.assertEqual(set(split["train_task_ids"]) | set(split["holdout_task_ids"]),
                         {task["task_id"] for task in first["tasks"]})

    def test_manifest_respects_requested_limit(self):
        with TemporaryDirectory() as tmp:
            source = self._locomo_source(Path(tmp))
            manifest = build_native_scale_manifest("locomo", source, limit=3, seed=17)
        self.assertEqual(manifest["task_count"], 3)
        self.assertEqual(len(manifest["tasks"]), 3)

    def test_manifest_rejects_non_positive_limit(self):
        with self.assertRaises(ValueError):
            build_native_scale_manifest("locomo", "missing.json", limit=0)


if __name__ == "__main__":
    unittest.main()
