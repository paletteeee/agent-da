"""Run a small end-to-end benchmark slice with real vector/graph services."""

from __future__ import annotations

import json
import os
import statistics
import time
from pathlib import Path

from txnmem_benchmark_bridge import TauBenchAdapter, _official_tau_user_strategy
from txnmem_model_protocol import OpenAICompatibleClient
from txnmem_real_experiment import load_task_manifest, run_benchmark_batch
from txnmem_vector_graph_backend import VectorGraphMemoryBackend


def main() -> int:
    root = Path(os.environ.get("TXNMEM_ROOT", ".")).resolve()
    manifest_path = root / "results/native_scale/manifests/tau_bench.json"
    out_root = root / os.environ.get("TXNMEM_E2E_OUT", "results/e2e_real_backend")
    manifest, digest = load_task_manifest(manifest_path)
    limit = int(os.environ.get("TXNMEM_E2E_LIMIT", "5"))
    model_revision = os.environ["TXNMEM_MODEL_REVISION"]
    model_server_build = os.environ["TXNMEM_MODEL_SERVER_BUILD"]
    source_commit = os.environ["TXNMEM_SOURCE_COMMIT"]
    qdrant_url = os.environ.get("TXNMEM_QDRANT_URL", "http://127.0.0.1:6333")
    neo4j_uri = os.environ.get("TXNMEM_NEO4J_URI", "bolt://127.0.0.1:7687")
    neo4j_auth = (
        os.environ.get("TXNMEM_NEO4J_USER", "neo4j"),
        os.environ["TXNMEM_NEO4J_PASSWORD"],
    )
    model = OpenAICompatibleClient(
        os.environ.get("TXNMEM_ENDPOINT", "http://127.0.0.1:8000/v1"),
        os.environ.get("TXNMEM_MODEL", "qwen2.5-7b-instruct"),
        timeout_s=float(os.environ.get("TXNMEM_TIMEOUT", "180")),
    )
    rows: list[dict[str, object]] = []

    health_backend = VectorGraphMemoryBackend(
        "e2e-healthcheck",
        qdrant_url,
        neo4j_uri,
        neo4j_auth,
    )
    try:
        backend_health = health_backend.healthcheck()
    finally:
        health_backend.close()
    if not all(
        bool(backend_health.get(service, {}).get("available"))
        for service in ("qdrant", "neo4j")
    ):
        raise RuntimeError("Qdrant/Neo4j healthcheck failed before E2E run")

    for index, task in enumerate(manifest["tasks"][:limit], start=1):
        task_out = out_root / f"task_{index:02d}"

        def adapter_factory() -> TauBenchAdapter:
            from tau_bench.envs.airline.env import MockAirlineDomainEnv

            env = MockAirlineDomainEnv(
                user_strategy=_official_tau_user_strategy("scripted"),
                task_split="test",
                task_index=None,
            )
            return TauBenchAdapter(lambda: env, task_split="test", user_strategy="scripted")

        def backend_factory(_index: int, _root: Path, task_index: int = index) -> VectorGraphMemoryBackend:
            return VectorGraphMemoryBackend(
                f"e2e-tau-{task_index:04d}",
                qdrant_url,
                neo4j_uri,
                neo4j_auth,
            )

        started = time.perf_counter()
        report = run_benchmark_batch(
            {"manifest_version": 1, "dataset_name": manifest["dataset_name"], "tasks": [task]},
            model,
            task_out,
            backend_factory=backend_factory,
            adapter_factory=adapter_factory,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        summary = report["task_summaries"][0] if report.get("task_summaries") else {}
        row = {
            "task_id": task["task_id"],
            "elapsed_ms": elapsed_ms,
            "status": summary.get("status"),
            "failure_code": summary.get("failure_code"),
            "official": summary.get("official"),
            "native_event_count": summary.get("native_event_count", 0),
        }
        rows.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)

    elapsed_values = [float(row["elapsed_ms"]) for row in rows]
    result = {
        "status": "available",
        "benchmark": "tau-bench-airline",
        "task_count": len(rows),
        "model": os.environ.get("TXNMEM_MODEL", "qwen2.5-7b-instruct"),
        "model_revision": model_revision,
        "model_server_build": model_server_build,
        "source_commit": source_commit,
        "backend": {
            service: backend_health[service].get("version")
            for service in ("qdrant", "neo4j")
        },
        "backend_health": backend_health,
        "manifest_sha256": digest,
        "rows": rows,
        "mean_ms": statistics.mean(elapsed_values) if elapsed_values else None,
        "p50_ms": statistics.median(elapsed_values) if elapsed_values else None,
        "execution_scope": "single_host_model_and_vector_graph_services",
        "production_latency_claim": False,
    }
    output = out_root / "e2e_real_backend_summary.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
