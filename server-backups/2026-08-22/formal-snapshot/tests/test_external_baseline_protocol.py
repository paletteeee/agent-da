"""Documentation contract for external baseline capability boundaries."""

import unittest
from pathlib import Path


PROTOCOL = (
    Path(__file__).resolve().parents[1] / "docs" / "external_baseline_protocol.md"
)


class ExternalBaselineProtocolTests(unittest.TestCase):
    def test_markers_and_invalidation_do_not_credit_missing_backend_semantics(self):
        protocol = PROTOCOL.read_text(encoding="utf-8")

        protocol_lower = protocol.lower()

        self.assertIn("scheduling marker only", protocol_lower)
        self.assertNotIn("in-memory trace buffer", protocol_lower)
        self.assertNotIn("flush the buffered calls", protocol_lower)
        self.assertIn("native recursive invalidation", protocol_lower)
        self.assertNotIn("descendants are found and updated by the adapter", protocol_lower)

    def test_runtime_failure_is_distinct_from_capability_absent(self):
        protocol = PROTOCOL.read_text(encoding="utf-8")

        self.assertIn("`runtime_failure`", protocol)
        self.assertIn("`capability_absent`", protocol)
        self.assertIn("PostgreSQL service is unavailable", protocol)


if __name__ == "__main__":
    unittest.main()
