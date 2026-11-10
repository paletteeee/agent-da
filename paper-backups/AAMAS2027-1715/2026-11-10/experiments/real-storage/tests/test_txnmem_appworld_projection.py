import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_appworld_projection import regenerate_appworld_projection
from txnmem_trace_pipeline import build_trace_instances, load_trace_records


class AppWorldProjectionRegenerationTests(unittest.TestCase):
    def test_regeneration_preserves_method_url_order_and_drops_request_values(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for task_id, calls in {
                "task_a": [
                    {"method": "get", "url": "/spotify/list", "data": {"token": "secret-a"}},
                    {"method": "post", "url": "/spotify/create", "data": {"name": "private"}},
                ],
                "task_b": [
                    {"method": "patch", "url": "/todo/update", "data": {"password": "secret-b"}}
                ],
            }.items():
                directory = root / "data" / "tasks" / task_id / "ground_truth"
                directory.mkdir(parents=True)
                (directory / "api_calls.json").write_text(json.dumps(calls), encoding="utf-8")
            output = root / "projection.jsonl"

            inventory = regenerate_appworld_projection(
                root,
                ["task_a", "task_b"],
                output,
            )
            output_text = output.read_text(encoding="utf-8")
            records = load_trace_records(output)
            instances = build_trace_instances(
                records,
                "appworld",
                source="appworld-official-reference-api-calls-redacted",
                seed=17,
            )

        self.assertEqual(inventory["event_count"], 3)
        self.assertEqual(inventory["task_count"], 2)
        self.assertEqual(inventory["method_counts"], {"get": 1, "patch": 1, "post": 1})
        self.assertFalse(inventory["request_data_values_retained"])
        self.assertNotIn("secret-a", output_text)
        self.assertNotIn("private", output_text)
        self.assertEqual(records[0]["sequence"], 1)
        self.assertEqual(records[0]["url"], "/spotify/list")
        self.assertEqual(len(instances), 2)

    def test_regeneration_rejects_missing_or_malformed_source(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                regenerate_appworld_projection(root, ["missing"], root / "out.jsonl")


if __name__ == "__main__":
    unittest.main()
