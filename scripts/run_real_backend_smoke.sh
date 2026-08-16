#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_OUT_DIR="${TXNMEM_OUT_DIR:-${TXNMEM_ROOT}/results/real_backend_faults_formal}"
TXNMEM_COMPOSE_FILE="${TXNMEM_ROOT}/infra/real_backend/docker-compose.yml"
TXNMEM_QDRANT_URL="${TXNMEM_QDRANT_URL:-http://127.0.0.1:19000}"
TXNMEM_NEO4J_URI="${TXNMEM_NEO4J_URI:-bolt://127.0.0.1:19001}"
TXNMEM_TOXIPROXY_URL="${TXNMEM_TOXIPROXY_URL:-http://127.0.0.1:8474}"
TXNMEM_NEO4J_USER="${TXNMEM_NEO4J_USER:-neo4j}"
TXNMEM_NEO4J_PASSWORD="${TXNMEM_NEO4J_PASSWORD:-txnmem-local-only}"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"
TXNMEM_REPETITIONS="${TXNMEM_REPETITIONS:-1}"
TXNMEM_EVENTS="${TXNMEM_EVENTS:-2}"

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
  echo "real backend fault run blocked: $reason" >&2
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
if ! command -v curl >/dev/null 2>&1; then
  write_blocked "curl_not_installed"
fi

docker compose -f "$TXNMEM_COMPOSE_FILE" up -d

wait_for_health() {
  local container_name="$1"
  local attempt
  local health
  for attempt in $(seq 1 60); do
    health="$(docker inspect --format '{{.State.Health.Status}}' "$container_name" 2>/dev/null || true)"
    if [ "$health" = "healthy" ]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

if ! wait_for_health txnmem-qdrant; then
  write_blocked "qdrant_healthcheck_failed"
fi
if ! wait_for_health txnmem-neo4j; then
  write_blocked "neo4j_healthcheck_failed"
fi

for attempt in $(seq 1 30); do
  if curl --fail --silent --show-error --max-time 5 "$TXNMEM_TOXIPROXY_URL/proxies" >/dev/null; then
    break
  fi
  if [ "$attempt" = "30" ]; then
    write_blocked "toxiproxy_management_unavailable"
  fi
  sleep 1
done

replace_proxy() {
  local proxy_name="$1"
  local payload="$2"
  if curl --fail --silent --show-error --max-time 5 \
      "$TXNMEM_TOXIPROXY_URL/proxies/$proxy_name" >/dev/null 2>&1; then
    curl --fail --silent --show-error --max-time 5 -X DELETE \
      "$TXNMEM_TOXIPROXY_URL/proxies/$proxy_name" >/dev/null
  fi
  curl --fail --silent --show-error --max-time 5 -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" "$TXNMEM_TOXIPROXY_URL/proxies" >/dev/null
}

replace_proxy txnmem-qdrant \
  '{"name":"txnmem-qdrant","listen":"0.0.0.0:19000","upstream":"qdrant:6333","enabled":true}'
replace_proxy txnmem-neo4j \
  '{"name":"txnmem-neo4j","listen":"0.0.0.0:19001","upstream":"neo4j:7687","enabled":true}'

if ! curl --fail --silent --show-error --max-time 10 "$TXNMEM_QDRANT_URL/readyz" >/dev/null; then
  write_blocked "qdrant_proxy_healthcheck_failed"
fi

read -r -a TXNMEM_EVENT_ARGS <<< "$TXNMEM_EVENTS"
export PYTHONPATH="$TXNMEM_ROOT/src"
TXNMEM_NEO4J_URI=bolt://127.0.0.1:19001 \
TXNMEM_TOXIPROXY_URL=http://127.0.0.1:8474 \
TXNMEM_NEO4J_USER="$TXNMEM_NEO4J_USER" \
TXNMEM_NEO4J_PASSWORD="$TXNMEM_NEO4J_PASSWORD" \
"$TXNMEM_PYTHON" "$TXNMEM_ROOT/src/txnmem_experiment.py" backend-performance \
  --backend vector-graph \
  --service-url http://127.0.0.1:19000 \
  --events "${TXNMEM_EVENT_ARGS[@]}" \
  --repetitions "$TXNMEM_REPETITIONS" \
  --out-dir "$TXNMEM_OUT_DIR"

TXNMEM_REPORT="$TXNMEM_OUT_DIR/results/backend_performance.json"
"$TXNMEM_PYTHON" -c \
  'import json,sys; r=json.load(open(sys.argv[1])); f=r["fault_matrix"]; assert f["all_scenarios_evidence_valid"] and f["all_scenarios_state_verified"] and f["all_observed_states_consistent"]' \
  "$TXNMEM_REPORT"

echo "real backend fault run completed: $TXNMEM_REPORT"
