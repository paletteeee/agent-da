#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_OUT_DIR="${TXNMEM_OUT_DIR:-${TXNMEM_ROOT}/results/multi_agent_real_storage_v1}"
TXNMEM_COMPOSE_FILE="${TXNMEM_ROOT}/infra/real_backend/docker-compose.yml"
TXNMEM_QDRANT_URL="${TXNMEM_QDRANT_URL:-http://127.0.0.1:19000}"
TXNMEM_NEO4J_URI="${TXNMEM_NEO4J_URI:-bolt://127.0.0.1:19001}"
TXNMEM_TOXIPROXY_URL="${TXNMEM_TOXIPROXY_URL:-http://127.0.0.1:8474}"
TXNMEM_NEO4J_USER="${TXNMEM_NEO4J_USER:-neo4j}"
TXNMEM_NEO4J_PASSWORD="${TXNMEM_NEO4J_PASSWORD:-txnmem-local-only}"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"
TXNMEM_REPETITIONS="${TXNMEM_REPETITIONS:-30}"

mkdir -p "$TXNMEM_OUT_DIR"

write_blocked() {
  local reason="$1"
  "$TXNMEM_PYTHON" -c \
    'import json,sys; from pathlib import Path; p=Path(sys.argv[1]); p.mkdir(parents=True,exist_ok=True); (p/"blocked_report.json").write_text(json.dumps({"status":"blocked","reason":sys.argv[2]},indent=2,sort_keys=True)+"\n",encoding="utf-8")' \
    "$TXNMEM_OUT_DIR" "$reason"
  echo "multi-agent real-storage run blocked: $reason" >&2
  exit 2
}

if [ ! -x "$TXNMEM_PYTHON" ]; then
  TXNMEM_PYTHON="$(command -v python3 || true)"
fi
if [ -z "$TXNMEM_PYTHON" ] || [ ! -x "$TXNMEM_PYTHON" ]; then
  echo "multi-agent real-storage run blocked: missing_python" >&2
  exit 2
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

export PYTHONPATH="$TXNMEM_ROOT/src"
export TXNMEM_NEO4J_USER
export TXNMEM_NEO4J_PASSWORD
"$TXNMEM_PYTHON" "$TXNMEM_ROOT/src/txnmem_experiment.py" \
  multi-agent-storage-experiment \
  --backend vector-graph \
  --qdrant-url "$TXNMEM_QDRANT_URL" \
  --neo4j-uri "$TXNMEM_NEO4J_URI" \
  --toxiproxy-url "$TXNMEM_TOXIPROXY_URL" \
  --repetitions "$TXNMEM_REPETITIONS" \
  --out-dir "$TXNMEM_OUT_DIR"

TXNMEM_REPORT="$TXNMEM_OUT_DIR/results/multi_agent_storage_results.json"
"$TXNMEM_PYTHON" -c \
  'import json,sys; r=json.load(open(sys.argv[1],encoding="utf-8")); assert r["observation_count"]==240; assert r["all_state_readbacks_complete"] is True; assert r["real_service_evidence"] is True' \
  "$TXNMEM_REPORT"

echo "multi-agent real-storage run completed: $TXNMEM_REPORT"
