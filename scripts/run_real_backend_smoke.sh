#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_OUT_DIR="${TXNMEM_OUT_DIR:-${TXNMEM_ROOT}/results/real_backend_smoke}"
TXNMEM_COMPOSE_FILE="${TXNMEM_ROOT}/infra/real_backend/docker-compose.yml"
TXNMEM_QDRANT_URL="${TXNMEM_QDRANT_URL:-http://127.0.0.1:6333}"
TXNMEM_NEO4J_URI="${TXNMEM_NEO4J_URI:-bolt://127.0.0.1:7687}"
TXNMEM_NEO4J_USER="${TXNMEM_NEO4J_USER:-neo4j}"
TXNMEM_NEO4J_PASSWORD="${TXNMEM_NEO4J_PASSWORD:-txnmem-local-only}"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"

mkdir -p "$TXNMEM_OUT_DIR"

write_blocked() {
  local reason="$1"
  TXNMEM_BLOCKED_REASON="$reason" TXNMEM_OUT_VALUE="$TXNMEM_OUT_DIR" \
    python3 - <<'PY'
import json
import os
from pathlib import Path

payload = {
    "status": "blocked",
    "reason": os.environ["TXNMEM_BLOCKED_REASON"],
    "production_latency_claim": False,
}
Path(os.environ["TXNMEM_OUT_VALUE"], "blocked_report.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
  echo "real backend smoke blocked: $reason" >&2
  exit 2
}

if [ ! -x "$TXNMEM_PYTHON" ]; then
  TXNMEM_PYTHON="$(command -v python3 || true)"
fi
if [ -z "$TXNMEM_PYTHON" ] || [ ! -x "$TXNMEM_PYTHON" ]; then
  write_blocked "missing_python"
fi
if ! command -v docker >/dev/null 2>&1; then
  write_blocked "docker_not_installed"
fi
if ! docker compose version >/dev/null 2>&1; then
  write_blocked "docker_compose_unavailable"
fi

docker compose -f "$TXNMEM_COMPOSE_FILE" up -d
if ! curl --fail --silent --show-error --max-time 10 "$TXNMEM_QDRANT_URL/readyz" >/dev/null; then
  write_blocked "qdrant_healthcheck_failed"
fi
if ! docker compose -f "$TXNMEM_COMPOSE_FILE" exec -T neo4j cypher-shell -u "$TXNMEM_NEO4J_USER" -p "$TXNMEM_NEO4J_PASSWORD" 'RETURN 1' >/dev/null 2>&1; then
  write_blocked "neo4j_healthcheck_failed"
fi

export PYTHONPATH="$TXNMEM_ROOT/src"
TXNMEM_QDRANT_URL="$TXNMEM_QDRANT_URL" TXNMEM_NEO4J_URI="$TXNMEM_NEO4J_URI" \
TXNMEM_NEO4J_USER="$TXNMEM_NEO4J_USER" TXNMEM_NEO4J_PASSWORD="$TXNMEM_NEO4J_PASSWORD" \
TXNMEM_OUT_DIR="$TXNMEM_OUT_DIR" "$TXNMEM_PYTHON" - <<'PY'
import json
import os
from pathlib import Path

from txnmem_vector_graph_backend import VectorGraphMemoryBackend

out_dir = Path(os.environ["TXNMEM_OUT_DIR"])
backend = VectorGraphMemoryBackend(
    "smoke-episode",
    os.environ["TXNMEM_QDRANT_URL"],
    os.environ["TXNMEM_NEO4J_URI"],
    (os.environ["TXNMEM_NEO4J_USER"], os.environ["TXNMEM_NEO4J_PASSWORD"]),
)
backend.write("smoke-source", value="remote smoke source")
backend.derive("smoke-derived", ["smoke-source"], value="remote smoke derived")
metrics = backend.metrics()
backend.close()
reopened = VectorGraphMemoryBackend(
    "smoke-episode",
    os.environ["TXNMEM_QDRANT_URL"],
    os.environ["TXNMEM_NEO4J_URI"],
    (os.environ["TXNMEM_NEO4J_USER"], os.environ["TXNMEM_NEO4J_PASSWORD"]),
)
try:
    reopened_value = reopened.read("smoke-source")
    report = {
        "status": "completed" if reopened_value is not None else "error",
        "health": reopened.healthcheck(),
        "metrics": metrics,
        "reopen_read_success": reopened_value is not None,
        "partial_commit_count": int(metrics.get("rollback_count", 0) or 0),
        "production_latency_claim": False,
    }
finally:
    reopened.close()
(out_dir / "real_backend_smoke.json").write_text(
    json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
if report["status"] != "completed" or report["partial_commit_count"] != 0:
    raise SystemExit(1)
PY

echo "real backend smoke completed: $TXNMEM_OUT_DIR/real_backend_smoke.json"
