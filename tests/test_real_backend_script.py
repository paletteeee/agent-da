from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RealBackendScriptTests(unittest.TestCase):
    def test_smoke_script_creates_both_proxies_and_runs_fault_cli_through_them(self):
        script = (ROOT / "scripts" / "run_real_backend_smoke.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('"name":"txnmem-qdrant"', script)
        self.assertIn('"upstream":"qdrant:6333"', script)
        self.assertIn('"listen":"0.0.0.0:19000"', script)
        self.assertIn('"name":"txnmem-neo4j"', script)
        self.assertIn('"upstream":"neo4j:7687"', script)
        self.assertIn('"listen":"0.0.0.0:19001"', script)
        self.assertIn("backend-performance", script)
        self.assertIn("--service-url http://127.0.0.1:19000", script)
        self.assertIn("TXNMEM_NEO4J_URI=bolt://127.0.0.1:19001", script)
        self.assertIn("TXNMEM_TOXIPROXY_URL=http://127.0.0.1:8474", script)

    def test_compose_does_not_publish_direct_qdrant_or_neo4j_data_ports(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('"6333:6333"', compose)
        self.assertNotIn('"7687:7687"', compose)
        self.assertIn('"19000:19000"', compose)
        self.assertIn('"19001:19001"', compose)


if __name__ == "__main__":
    unittest.main()
