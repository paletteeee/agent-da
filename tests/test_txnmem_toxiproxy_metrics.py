from __future__ import annotations

import copy
import unittest

from txnmem_toxiproxy_metrics import (
    ToxiproxyMetricsError,
    derive_proxy_counter_deltas,
    parse_toxiproxy_byte_counters,
    proxy_counter_payload_sha256,
    proxy_counter_values,
    validate_proxy_counter_snapshot,
)


ROUTES = [
    {
        "role": "qdrant",
        "proxy_name": "txnmem-qdrant",
        "listen": "0.0.0.0:19000",
        "upstream": "qdrant:6333",
        "enabled": True,
        "toxics_count": 0,
    },
    {
        "role": "neo4j",
        "proxy_name": "txnmem-neo4j",
        "listen": "0.0.0.0:19001",
        "upstream": "neo4j:7687",
        "enabled": True,
        "toxics_count": 0,
    },
]

METRICS = """
# TYPE toxiproxy_proxy_received_bytes_total counter
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 1.1e1
toxiproxy_proxy_sent_bytes_total{proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333",direction="upstream"} 13
toxiproxy_proxy_received_bytes_total{listener="[::]:19000",upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant"} 17
toxiproxy_proxy_sent_bytes_total{upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000"} 19
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 23
toxiproxy_proxy_sent_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 29
toxiproxy_proxy_received_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 31
toxiproxy_proxy_sent_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 37
"""


class ToxiproxyMetricsTests(unittest.TestCase):
    def test_parses_exact_official_proxy_metric_closure(self):
        snapshot = parse_toxiproxy_byte_counters(
            METRICS, phase="baseline_a", proxy_routes=ROUTES
        )

        self.assertEqual(snapshot["schema"], "txnmem-provenance-proxy-counters-v1")
        self.assertEqual([row["total_bytes"] for row in snapshot["routes"]], [60, 120])
        self.assertEqual(snapshot["toxiproxy_total_bytes"], 180)
        self.assertEqual(
            snapshot["routes"][0],
            {
                "role": "qdrant",
                "proxy_name": "txnmem-qdrant",
                "listener": "0.0.0.0:19000",
                "upstream": "qdrant:6333",
                "received_upstream_bytes": 11,
                "sent_upstream_bytes": 13,
                "received_downstream_bytes": 17,
                "sent_downstream_bytes": 19,
                "total_bytes": 60,
            },
        )

    def test_rejects_non_closing_or_invalid_metric_series(self):
        missing = METRICS.replace(
            'toxiproxy_proxy_sent_bytes_total{upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000"} 19\n',
            "",
        )
        duplicate = METRICS + (
            'toxiproxy_proxy_sent_bytes_total{proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333",direction="upstream"} 13\n'
        )
        cases = {
            "transmitted": METRICS.replace("_sent_bytes_total", "_transmitted_bytes_total", 1),
            "unknown_family": METRICS + 'toxiproxy_proxy_connections_total{proxy="txnmem-qdrant"} 1\n',
            "duplicate_series": duplicate,
            "missing_series": missing,
            "duplicate_label": METRICS.replace(
                'direction="upstream",proxy=', 'direction="upstream",direction="upstream",proxy=', 1
            ),
            "extra_label": METRICS.replace('proxy="txnmem-qdrant",', 'proxy="txnmem-qdrant",extra="x",', 1),
            "missing_label": METRICS.replace('listener="[::]:19000",', "", 1),
            "wrong_proxy": METRICS.replace("txnmem-qdrant", "wrong-proxy", 1),
            "wrong_listener": METRICS.replace("[::]:19000", "127.0.0.1:19000", 1),
            "wrong_upstream": METRICS.replace("qdrant:6333", "qdrant:6334", 1),
            "invalid_direction": METRICS.replace('direction="upstream"', 'direction="sideways"', 1),
            "negative": METRICS.replace(" 1.1e1", " -1", 1),
            "fractional": METRICS.replace(" 1.1e1", " 1.5", 1),
            "nan": METRICS.replace(" 1.1e1", " NaN", 1),
            "infinity": METRICS.replace(" 1.1e1", " +Inf", 1),
            "above_exact_limit": METRICS.replace(" 1.1e1", " 9007199254740992", 1),
        }

        for name, metrics in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(ToxiproxyMetricsError):
                    parse_toxiproxy_byte_counters(
                        metrics, phase="baseline_a", proxy_routes=ROUTES
                    )

    def test_validates_hashes_and_derives_role_ordered_counter_values(self):
        baseline = parse_toxiproxy_byte_counters(
            METRICS, phase="baseline_a", proxy_routes=ROUTES
        )
        final = parse_toxiproxy_byte_counters(
            METRICS.replace(" 1.1e1", " 15", 1),
            phase="final",
            proxy_routes=ROUTES,
        )

        self.assertEqual(
            proxy_counter_values(baseline),
            (11, 13, 17, 19, 23, 29, 31, 37),
        )
        self.assertEqual(
            proxy_counter_payload_sha256(baseline),
            proxy_counter_payload_sha256(
                parse_toxiproxy_byte_counters(
                    METRICS, phase="baseline_b", proxy_routes=ROUTES
                )
            ),
        )
        self.assertEqual(
            derive_proxy_counter_deltas(baseline, final),
            {
                "schema": "txnmem-provenance-proxy-counter-deltas-v1",
                "routes": [
                    {
                        "role": "qdrant",
                        "proxy_name": "txnmem-qdrant",
                        "listener": "0.0.0.0:19000",
                        "upstream": "qdrant:6333",
                        "received_upstream_bytes": 4,
                        "sent_upstream_bytes": 0,
                        "received_downstream_bytes": 0,
                        "sent_downstream_bytes": 0,
                        "total_bytes": 4,
                    },
                    {
                        "role": "neo4j",
                        "proxy_name": "txnmem-neo4j",
                        "listener": "0.0.0.0:19001",
                        "upstream": "neo4j:7687",
                        "received_upstream_bytes": 0,
                        "sent_upstream_bytes": 0,
                        "received_downstream_bytes": 0,
                        "sent_downstream_bytes": 0,
                        "total_bytes": 0,
                    },
                ],
                "toxiproxy_total_bytes": 4,
            },
        )

        tampered = copy.deepcopy(baseline)
        tampered["toxiproxy_total_bytes"] = 181
        with self.assertRaises(ToxiproxyMetricsError):
            validate_proxy_counter_snapshot(tampered)
        with self.assertRaises(ToxiproxyMetricsError):
            validate_proxy_counter_snapshot(baseline, expected_phase="final")


if __name__ == "__main__":
    unittest.main()
