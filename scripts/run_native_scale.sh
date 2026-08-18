#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"
TXNMEM_ENDPOINT=""
TXNMEM_MODEL=""
TXNMEM_TAU_TASKS=50
TXNMEM_APPWORLD_TASKS=20
TXNMEM_LOCOMO_TASKS=10
TXNMEM_OUT_DIR="${TXNMEM_ROOT}/results/native_scale"
TXNMEM_REPETITIONS=1
TXNMEM_APPWORLD_ROOT="${TXNMEM_ROOT}/external_data/deps/appworld-data"
TXNMEM_LOCOMO_SOURCE="${TXNMEM_ROOT}/external_data/raw/locomo10.json"
TXNMEM_LOCOMO_EVALUATOR_COMMAND="${TXNMEM_LOCOMO_EVALUATOR_COMMAND:-}"

usage() {
  echo "usage: $0 --endpoint URL --model MODEL [--tau-tasks N] [--appworld-tasks N] [--locomo-tasks N] [--out-dir DIR] [--repetitions N]"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --endpoint) TXNMEM_ENDPOINT="$2"; shift 2 ;;
    --model) TXNMEM_MODEL="$2"; shift 2 ;;
    --tau-tasks) TXNMEM_TAU_TASKS="$2"; shift 2 ;;
    --appworld-tasks) TXNMEM_APPWORLD_TASKS="$2"; shift 2 ;;
    --locomo-tasks) TXNMEM_LOCOMO_TASKS="$2"; shift 2 ;;
    --out-dir) TXNMEM_OUT_DIR="$2"; shift 2 ;;
    --repetitions) TXNMEM_REPETITIONS="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

TXNMEM_LOCOMO_EVALUATOR_ARGS=()
if [ -n "$TXNMEM_LOCOMO_EVALUATOR_COMMAND" ]; then
  TXNMEM_LOCOMO_EVALUATOR_ARGS=(--locomo-evaluator-command "$TXNMEM_LOCOMO_EVALUATOR_COMMAND")
fi

if [ -z "$TXNMEM_ENDPOINT" ] || [ -z "$TXNMEM_MODEL" ]; then
  echo "error: --endpoint and --model are required" >&2
  exit 2
fi
if [ ! -x "$TXNMEM_PYTHON" ]; then
  echo "error: Python interpreter not found: $TXNMEM_PYTHON" >&2
  exit 2
fi

mkdir -p "$TXNMEM_OUT_DIR/manifests" "$TXNMEM_OUT_DIR/runs"
export PYTHONPATH="$TXNMEM_ROOT/src"

"$TXNMEM_PYTHON" - "$TXNMEM_ROOT" "$TXNMEM_OUT_DIR" "$TXNMEM_TAU_TASKS" "$TXNMEM_APPWORLD_TASKS" "$TXNMEM_LOCOMO_TASKS" "$TXNMEM_APPWORLD_ROOT" "$TXNMEM_LOCOMO_SOURCE" <<'PY'
import json
import sys
from pathlib import Path

from txnmem_benchmark_manifests import build_native_scale_manifest

root, out_dir, tau_limit, appworld_limit, locomo_limit, appworld_root, locomo_source = sys.argv[1:]
out_dir = Path(out_dir)
jobs = [
    ("tau-bench", "airline", int(tau_limit), "test"),
    ("appworld", appworld_root, int(appworld_limit), "test"),
    ("locomo", locomo_source, int(locomo_limit), "test"),
]
summary = {"seed": 17, "manifests": {}}
for benchmark, source, limit, split in jobs:
    manifest = build_native_scale_manifest(benchmark, source, limit, seed=17, split=split)
    path = out_dir / "manifests" / f"{benchmark.replace('-', '_')}.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary["manifests"][benchmark] = {
        "path": str(path),
        "task_count": manifest["task_count"],
        "manifest_hash": manifest["manifest_hash"],
    }
(out_dir / "scale_manifest_summary.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

for TXNMEM_JOB in tau_bench appworld locomo; do
  case "$TXNMEM_JOB" in
    tau_bench) TXNMEM_BENCHMARK="tau-bench" ;;
    appworld) TXNMEM_BENCHMARK="appworld" ;;
    locomo) TXNMEM_BENCHMARK="locomo" ;;
  esac
  "$TXNMEM_PYTHON" "$TXNMEM_ROOT/src/txnmem_experiment.py" benchmark-native-batch \
    --benchmark "$TXNMEM_BENCHMARK" \
    --manifest "$TXNMEM_OUT_DIR/manifests/${TXNMEM_JOB}.json" \
    --out-dir "$TXNMEM_OUT_DIR/runs/${TXNMEM_JOB}" \
    --memory-backend sqlite \
    --repetitions "$TXNMEM_REPETITIONS" \
    --endpoint "$TXNMEM_ENDPOINT" \
    --model "$TXNMEM_MODEL" \
    --appworld-root "$TXNMEM_APPWORLD_ROOT" \
    "${TXNMEM_LOCOMO_EVALUATOR_ARGS[@]}"
done

echo "native scale batch completed: $TXNMEM_OUT_DIR"
