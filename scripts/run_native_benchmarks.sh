#!/usr/bin/env bash
# Run native TxnMem memory trace collection on the three public benchmarks
# against a remote vLLM endpoint.
#
# Usage:
#   bash scripts/run_native_benchmarks.sh \
#     --endpoint http://GPU_HOST:8000/v1 --model qwen2.5-7b-instruct \
#     [--tau-tasks 50] [--appworld-tasks 20] [--locomo-tasks 10] [--out-dir results/native]
set -euo pipefail

ENDPOINT=""
MODEL=""
OUT_DIR="results/native"
TAU_TASKS=50
APPWORLD_TASKS=20
LOCOMO_TASKS=10
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export APPWORLD_ROOT="$ROOT/external_data/deps/appworld-data"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --endpoint) ENDPOINT="$2"; shift 2 ;;
    --model) MODEL="$2"; shift 2 ;;
    --tau-tasks) TAU_TASKS="$2"; shift 2 ;;
    --appworld-tasks) APPWORLD_TASKS="$2"; shift 2 ;;
    --locomo-tasks) LOCOMO_TASKS="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

if [ -z "$ENDPOINT" ] || [ -z "$MODEL" ]; then
  echo "error: --endpoint and --model are required" >&2
  exit 1
fi

PY="$ROOT/.venv/bin/python"
cd "$ROOT"

mkdir -p "$OUT_DIR"

echo "==> generating manifests"
$PY src/txnmem_benchmark_manifests.py --benchmark tau-bench --tau-domain airline \
  --out configs/native_tau_airline.json --max-tasks "$TAU_TASKS" --seed 17
$PY src/txnmem_benchmark_manifests.py --benchmark appworld \
  --out configs/native_appworld.json --max-tasks "$APPWORLD_TASKS" --seed 17
$PY src/txnmem_benchmark_manifests.py --benchmark locomo \
  --out configs/native_locomo.json --max-tasks "$LOCOMO_TASKS" --seed 17

echo "==> tau-bench native run ($TAU_TASKS tasks)"
$PY src/txnmem_experiment.py benchmark-native-smoke \
  --benchmark tau-bench --manifest configs/native_tau_airline.json \
  --endpoint "$ENDPOINT" --model "$MODEL" --out-dir "$OUT_DIR/tau_airline" || true

echo "==> appworld native run ($APPWORLD_TASKS tasks)"
$PY src/txnmem_experiment.py benchmark-native-smoke \
  --benchmark appworld --manifest configs/native_appworld.json \
  --endpoint "$ENDPOINT" --model "$MODEL" --out-dir "$OUT_DIR/appworld" || true

echo "==> locomo native run ($LOCOMO_TASKS tasks)"
$PY src/txnmem_experiment.py benchmark-native-smoke \
  --benchmark locomo --manifest configs/native_locomo.json \
  --endpoint "$ENDPOINT" --model "$MODEL" --out-dir "$OUT_DIR/locomo" || true

echo "==> done; summaries:"
ls -la "$OUT_DIR"/*/results/native_model_summary.json 2>/dev/null || true
