#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"
TXNMEM_ENDPOINT=""
TXNMEM_MODEL=""
TXNMEM_TAU_DOMAIN="retail"
TXNMEM_TAU_SPLIT="test"
TXNMEM_TAU_TASKS=115
TXNMEM_APPWORLD_SPLIT="test_normal"
TXNMEM_APPWORLD_TASKS=168
TXNMEM_LOCOMO_TASKS=10
TXNMEM_OUT_DIR="${TXNMEM_ROOT}/results/native_scale"
TXNMEM_REPETITIONS=1
TXNMEM_TAU_ROOT="${TXNMEM_TAU_ROOT:-${TXNMEM_ROOT}/external_data/deps/tau-bench}"
TXNMEM_APPWORLD_ROOT="${TXNMEM_APPWORLD_ROOT:-${TXNMEM_ROOT}/external_data/deps/appworld-data}"
TXNMEM_LOCOMO_SOURCE="${TXNMEM_LOCOMO_SOURCE:-${TXNMEM_ROOT}/external_data/raw/locomo10.json}"
TXNMEM_LOCOMO_EVALUATOR_COMMAND="${TXNMEM_LOCOMO_EVALUATOR_COMMAND:-}"
TXNMEM_BENCHMARKS="tau-bench,appworld,locomo"
TXNMEM_SHARD_COUNT=1
TXNMEM_GENERATE_ONLY=0
TXNMEM_MERGE_ONLY=0
TXNMEM_RESUME=0

usage() {
  echo "usage: $0 [--endpoint URL --model MODEL] [--benchmarks LIST] [--tau-domain retail --tau-split test --tau-tasks 115] [--appworld-root DIR --appworld-split test_normal --appworld-tasks 168] [--locomo-tasks N] [--out-dir DIR] [--repetitions N] [--shard-count N] [--generate-only|--merge-only] [--resume]"
  echo "formal defaults: tau-bench retail/test=115; AppWorld test_normal=168"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --endpoint) TXNMEM_ENDPOINT="$2"; shift 2 ;;
    --model) TXNMEM_MODEL="$2"; shift 2 ;;
    --benchmarks) TXNMEM_BENCHMARKS="$2"; shift 2 ;;
    --tau-domain) TXNMEM_TAU_DOMAIN="$2"; shift 2 ;;
    --tau-split) TXNMEM_TAU_SPLIT="$2"; shift 2 ;;
    --tau-tasks) TXNMEM_TAU_TASKS="$2"; shift 2 ;;
    --appworld-root) TXNMEM_APPWORLD_ROOT="$2"; shift 2 ;;
    --appworld-split) TXNMEM_APPWORLD_SPLIT="$2"; shift 2 ;;
    --appworld-tasks) TXNMEM_APPWORLD_TASKS="$2"; shift 2 ;;
    --locomo-tasks) TXNMEM_LOCOMO_TASKS="$2"; shift 2 ;;
    --out-dir) TXNMEM_OUT_DIR="$2"; shift 2 ;;
    --repetitions) TXNMEM_REPETITIONS="$2"; shift 2 ;;
    --shard-count) TXNMEM_SHARD_COUNT="$2"; shift 2 ;;
    --generate-only) TXNMEM_GENERATE_ONLY=1; shift ;;
    --merge-only) TXNMEM_MERGE_ONLY=1; shift ;;
    --resume) TXNMEM_RESUME=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ "$TXNMEM_GENERATE_ONLY" -eq 1 ] && [ "$TXNMEM_MERGE_ONLY" -eq 1 ]; then
  echo "error: --generate-only and --merge-only are mutually exclusive" >&2
  exit 2
fi
for TXNMEM_NUMBER in "$TXNMEM_TAU_TASKS" "$TXNMEM_APPWORLD_TASKS" "$TXNMEM_LOCOMO_TASKS" "$TXNMEM_REPETITIONS" "$TXNMEM_SHARD_COUNT"; do
  case "$TXNMEM_NUMBER" in
    ''|*[!0-9]*|0) echo "error: task counts, repetitions, and shard count must be positive integers" >&2; exit 2 ;;
  esac
done

TXNMEM_LOCOMO_EVALUATOR_ARGS=()
if [ -n "$TXNMEM_LOCOMO_EVALUATOR_COMMAND" ]; then
  TXNMEM_LOCOMO_EVALUATOR_ARGS=(--locomo-evaluator-command "$TXNMEM_LOCOMO_EVALUATOR_COMMAND")
fi

if [ ! -x "$TXNMEM_PYTHON" ]; then
  echo "error: Python interpreter not found: $TXNMEM_PYTHON" >&2
  exit 2
fi

export PYTHONPATH="$TXNMEM_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

"$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" <<'PY'
import sys

from txnmem_formal_io import FormalStore

store = FormalStore(sys.argv[1])
store.ensure_directory("manifests")
store.ensure_directory("runs")
PY

if [ "$TXNMEM_MERGE_ONLY" -eq 0 ]; then
"$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_BENCHMARKS" "$TXNMEM_TAU_ROOT" "$TXNMEM_TAU_DOMAIN" "$TXNMEM_TAU_SPLIT" "$TXNMEM_TAU_TASKS" "$TXNMEM_APPWORLD_ROOT" "$TXNMEM_APPWORLD_SPLIT" "$TXNMEM_APPWORLD_TASKS" "$TXNMEM_LOCOMO_SOURCE" "$TXNMEM_LOCOMO_TASKS" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import sys

(
    out_dir,
    requested_benchmarks,
    tau_root,
    tau_domain,
    tau_split,
    tau_limit,
    appworld_root,
    appworld_split,
    appworld_limit,
    locomo_source,
    locomo_limit,
    shard_count,
    resume,
) = sys.argv[1:]
sys.path.insert(0, tau_root)

from txnmem_benchmark_manifests import build_native_scale_manifest, shard_manifest
from txnmem_formal_io import FormalStore, validate_parent_manifest

store = FormalStore(out_dir)
allowed = {
    "tau-bench": (tau_domain, int(tau_limit), tau_split),
    "appworld": (appworld_root, int(appworld_limit), appworld_split),
    "locomo": (locomo_source, int(locomo_limit), "test"),
}
selected = [item.strip() for item in requested_benchmarks.split(",") if item.strip()]
if not selected or len(selected) != len(set(selected)) or any(item not in allowed for item in selected):
    raise SystemExit("--benchmarks must be a unique comma-separated subset of tau-bench,appworld,locomo")
resume = bool(int(resume))

summary = {"seed": 17, "shard_count": int(shard_count), "manifests": {}}
for benchmark in selected:
    source, limit, split = allowed[benchmark]
    manifest = build_native_scale_manifest(benchmark, source, limit, seed=17, split=split)
    formal_expected = None
    if benchmark == "tau-bench" and source == "retail" and split == "test":
        formal_expected = 115
    elif benchmark == "appworld" and split == "test_normal":
        formal_expected = 168
    if formal_expected is not None and (
        manifest.get("source_task_count") != formal_expected
        or manifest.get("task_count") != formal_expected
    ):
        raise SystemExit(
            f"frozen formal source count mismatch for {benchmark}/{split}: "
            f"source={manifest.get('source_task_count')} selected={manifest.get('task_count')} "
            f"expected={formal_expected}"
        )
    validate_parent_manifest(manifest)
    job = benchmark.replace("-", "_")
    store.write_or_verify_json(
        "manifests",
        job,
        "parent.json",
        payload=manifest,
        resume=resume,
        artifact_name="manifest",
        sort_keys=False,
    )
    for shard in shard_manifest(manifest, int(shard_count)):
        store.write_or_verify_json(
            "manifests",
            job,
            f"shard_{shard['shard_index']:03d}.json",
            payload=shard,
            resume=resume,
            artifact_name="manifest",
            sort_keys=False,
        )
    summary["manifests"][benchmark] = {
        "path": f"manifests/{job}/parent.json",
        "task_count": manifest["task_count"],
        "manifest_hash": manifest["manifest_hash"],
    }
store.write_or_verify_json(
    "scale_manifest_summary.json",
    payload=summary,
    resume=resume,
    artifact_name="manifest summary",
    sort_keys=False,
)
PY
fi

IFS=',' read -r -a TXNMEM_SELECTED_BENCHMARKS <<< "$TXNMEM_BENCHMARKS"

if [ "$TXNMEM_GENERATE_ONLY" -eq 0 ]; then
  "$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_BENCHMARKS" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import sys

from txnmem_formal_io import FormalStore, preflight_existing_merge

store = FormalStore(sys.argv[1])
selected = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
shard_count = int(sys.argv[3])
resume = bool(int(sys.argv[4]))
jobs = {"tau-bench": "tau_bench", "appworld": "appworld", "locomo": "locomo"}
for benchmark in selected:
    if benchmark not in jobs:
        raise SystemExit(f"unsupported benchmark: {benchmark}")
    preflight_existing_merge(
        store,
        jobs[benchmark],
        shard_count,
        resume=resume,
    )
PY
fi

bind_shard_report() {
  "$TXNMEM_PYTHON" - "$1" "$2" "$3" "$4" <<'PY'
import sys

from txnmem_formal_io import FormalStore, bind_shard_files

store = FormalStore(sys.argv[1])
bind_shard_files(store, sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
PY
}

if [ "$TXNMEM_GENERATE_ONLY" -eq 0 ] && [ "$TXNMEM_MERGE_ONLY" -eq 0 ]; then
  if [ -z "$TXNMEM_ENDPOINT" ] || [ -z "$TXNMEM_MODEL" ]; then
    echo "error: --endpoint and --model are required for shard execution" >&2
    exit 2
  fi
  TXNMEM_RUN_PLAN_OUTPUT="$("$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_BENCHMARKS" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import sys

from txnmem_formal_io import FormalStore, prepare_shard_runs

store = FormalStore(sys.argv[1])
selected = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
job_names = {"tau-bench": "tau_bench", "appworld": "appworld", "locomo": "locomo"}
try:
    jobs = [job_names[item] for item in selected]
except KeyError as exc:
    raise SystemExit(f"unsupported benchmark: {exc.args[0]}") from exc
for job, shard_index, action in prepare_shard_runs(
    store,
    jobs,
    int(sys.argv[3]),
    resume=bool(int(sys.argv[4])),
):
    print(f"{job}\t{shard_index}\t{action}")
PY
)"
  TXNMEM_RUN_PLAN_LINES=()
  while IFS= read -r TXNMEM_PLAN_LINE; do
    if [ -n "$TXNMEM_PLAN_LINE" ]; then
      TXNMEM_RUN_PLAN_LINES[${#TXNMEM_RUN_PLAN_LINES[@]}]="$TXNMEM_PLAN_LINE"
    fi
  done <<< "$TXNMEM_RUN_PLAN_OUTPUT"
  TXNMEM_RUN_PLAN_CURSOR=0
for TXNMEM_BENCHMARK_ITEM in "${TXNMEM_SELECTED_BENCHMARKS[@]}"; do
  TXNMEM_BENCHMARK="${TXNMEM_BENCHMARK_ITEM//[[:space:]]/}"
  TXNMEM_JOB="${TXNMEM_BENCHMARK//-/_}"
  case "$TXNMEM_JOB" in
    tau_bench|appworld|locomo) ;;
    *) echo "error: unsupported benchmark: $TXNMEM_BENCHMARK" >&2; exit 2 ;;
  esac
  for ((TXNMEM_SHARD_INDEX=0; TXNMEM_SHARD_INDEX<TXNMEM_SHARD_COUNT; TXNMEM_SHARD_INDEX++)); do
    printf -v TXNMEM_SHARD_NAME 'shard_%03d' "$TXNMEM_SHARD_INDEX"
    TXNMEM_SHARD_PATH="$TXNMEM_OUT_DIR/manifests/$TXNMEM_JOB/${TXNMEM_SHARD_NAME}.json"
    TXNMEM_RUN_DIR="$TXNMEM_OUT_DIR/runs/$TXNMEM_JOB/$TXNMEM_SHARD_NAME"
    TXNMEM_RAW_REPORT="$TXNMEM_RUN_DIR/results/native_batch_summary.json"
    TXNMEM_BOUND_REPORT="$TXNMEM_RUN_DIR/shard_report.json"
    TXNMEM_RUN_PLAN_LINE="${TXNMEM_RUN_PLAN_LINES[$TXNMEM_RUN_PLAN_CURSOR]}"
    TXNMEM_RUN_PLAN_CURSOR=$((TXNMEM_RUN_PLAN_CURSOR + 1))
    IFS=$'\t' read -r TXNMEM_PLAN_JOB TXNMEM_PLAN_INDEX TXNMEM_RUN_ACTION <<< "$TXNMEM_RUN_PLAN_LINE"
    if [ "$TXNMEM_PLAN_JOB" != "$TXNMEM_JOB" ] || [ "$TXNMEM_PLAN_INDEX" -ne "$TXNMEM_SHARD_INDEX" ]; then
      echo "error: shard run plan order mismatch" >&2
      exit 2
    fi
    if [ "$TXNMEM_RUN_ACTION" = "reuse" ]; then
      continue
    fi
    if [ "$TXNMEM_RUN_ACTION" != "execute" ]; then
      echo "error: invalid shard run action: $TXNMEM_RUN_ACTION" >&2
      exit 2
    fi
    "$TXNMEM_PYTHON" "$TXNMEM_ROOT/src/txnmem_experiment.py" benchmark-native-batch \
      --benchmark "$TXNMEM_BENCHMARK" \
      --manifest "$TXNMEM_SHARD_PATH" \
      --tau-domain "$TXNMEM_TAU_DOMAIN" \
      --tau-split "$TXNMEM_TAU_SPLIT" \
      --out-dir "$TXNMEM_RUN_DIR" \
      --memory-backend sqlite \
      --repetitions "$TXNMEM_REPETITIONS" \
      --endpoint "$TXNMEM_ENDPOINT" \
      --model "$TXNMEM_MODEL" \
      --appworld-root "$TXNMEM_APPWORLD_ROOT" \
      ${TXNMEM_LOCOMO_EVALUATOR_ARGS[@]+"${TXNMEM_LOCOMO_EVALUATOR_ARGS[@]}"}
    bind_shard_report "$TXNMEM_OUT_DIR" "$TXNMEM_JOB" "$TXNMEM_SHARD_INDEX" "$TXNMEM_SHARD_COUNT"
  done
done
fi

if [ "$TXNMEM_GENERATE_ONLY" -eq 0 ]; then
for TXNMEM_BENCHMARK_ITEM in "${TXNMEM_SELECTED_BENCHMARKS[@]}"; do
  TXNMEM_BENCHMARK="${TXNMEM_BENCHMARK_ITEM//[[:space:]]/}"
  TXNMEM_JOB="${TXNMEM_BENCHMARK//-/_}"
  "$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_JOB" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import sys

from txnmem_formal_io import FormalStore, finalize_native_merge

store = FormalStore(sys.argv[1])
resume = bool(int(sys.argv[4]))
finalize_native_merge(
    store,
    sys.argv[2],
    int(sys.argv[3]),
    resume=resume,
)
PY
done
fi

if [ "$TXNMEM_GENERATE_ONLY" -eq 1 ]; then
  echo "native scale manifests generated: $TXNMEM_OUT_DIR"
else
  echo "native scale shards merged: $TXNMEM_OUT_DIR"
fi
