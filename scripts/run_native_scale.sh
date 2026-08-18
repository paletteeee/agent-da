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

mkdir -p "$TXNMEM_OUT_DIR/manifests" "$TXNMEM_OUT_DIR/runs"
export PYTHONPATH="$TXNMEM_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

if [ "$TXNMEM_MERGE_ONLY" -eq 0 ]; then
"$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_BENCHMARKS" "$TXNMEM_TAU_ROOT" "$TXNMEM_TAU_DOMAIN" "$TXNMEM_TAU_SPLIT" "$TXNMEM_TAU_TASKS" "$TXNMEM_APPWORLD_ROOT" "$TXNMEM_APPWORLD_SPLIT" "$TXNMEM_APPWORLD_TASKS" "$TXNMEM_LOCOMO_SOURCE" "$TXNMEM_LOCOMO_TASKS" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import json
import sys
from pathlib import Path

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

out_dir = Path(out_dir)
allowed = {
    "tau-bench": (tau_domain, int(tau_limit), tau_split),
    "appworld": (appworld_root, int(appworld_limit), appworld_split),
    "locomo": (locomo_source, int(locomo_limit), "test"),
}
selected = [item.strip() for item in requested_benchmarks.split(",") if item.strip()]
if not selected or len(selected) != len(set(selected)) or any(item not in allowed for item in selected):
    raise SystemExit("--benchmarks must be a unique comma-separated subset of tau-bench,appworld,locomo")
resume = bool(int(resume))

def write_or_verify(path: Path, payload: dict) -> None:
    if path.exists():
        if not resume:
            raise SystemExit(f"refusing to overwrite existing manifest without --resume: {path}")
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"existing resume manifest is malformed: {path}") from exc
        if existing != payload:
            raise SystemExit(f"existing resume manifest does not match frozen source: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

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
    job = benchmark.replace("-", "_")
    manifest_dir = out_dir / "manifests" / job
    path = manifest_dir / "parent.json"
    write_or_verify(path, manifest)
    for shard in shard_manifest(manifest, int(shard_count)):
        write_or_verify(manifest_dir / f"shard_{shard['shard_index']:03d}.json", shard)
    summary["manifests"][benchmark] = {
        "path": f"manifests/{job}/parent.json",
        "task_count": manifest["task_count"],
        "manifest_hash": manifest["manifest_hash"],
    }
write_or_verify(out_dir / "scale_manifest_summary.json", summary)
PY
fi

IFS=',' read -r -a TXNMEM_SELECTED_BENCHMARKS <<< "$TXNMEM_BENCHMARKS"

bind_shard_report() {
  "$TXNMEM_PYTHON" - "$1" "$2" "$3" <<'PY'
import json
import sys
from pathlib import Path

shard_path, raw_path, output_path = map(Path, sys.argv[1:])
try:
    shard = json.loads(shard_path.read_text(encoding="utf-8"))
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"cannot bind malformed shard output: {exc}") from exc
if raw.get("manifest_sha256") != shard.get("manifest_hash"):
    raise SystemExit("native report manifest hash does not match executed shard")
execution_condition = raw.get("condition_fingerprint")
if not isinstance(execution_condition, str) or not execution_condition:
    raise SystemExit("native report has no execution condition fingerprint")
repetitions = raw.get("repetitions")
if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions < 1:
    raise SystemExit("native report has malformed repetitions")
tasks = shard.get("tasks")
rows = raw.get("task_summaries")
if not isinstance(tasks, list) or not isinstance(rows, list) or len(rows) != len(tasks) * repetitions:
    raise SystemExit("native report task rows do not cover the shard repetitions")
bound_rows = []
for repetition in range(1, repetitions + 1):
    for task_offset, task in enumerate(tasks):
        row = rows[(repetition - 1) * len(tasks) + task_offset]
        if not isinstance(row, dict) or row.get("task_id") != task.get("task_id"):
            raise SystemExit("native report task order does not match the shard")
        item = dict(row)
        item["source_position"] = task["source_position"]
        item["repetition"] = repetition
        bound_rows.append(item)
report = {
    "parent_manifest_hash": shard["parent_manifest_hash"],
    "shard_index": shard["shard_index"],
    "shard_count": shard["shard_count"],
    "benchmark": shard["benchmark"],
    "split": shard["split"],
    "source_identity": shard["source_identity"],
    "condition_fingerprint": shard["condition_fingerprint"],
    "execution_condition_fingerprint": execution_condition,
    "execution_manifest_hash": raw.get("manifest_sha256"),
    "repetitions": repetitions,
    "task_summaries": bound_rows,
}
if "domain" in shard:
    report["domain"] = shard["domain"]
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

if [ "$TXNMEM_GENERATE_ONLY" -eq 0 ] && [ "$TXNMEM_MERGE_ONLY" -eq 0 ]; then
  if [ -z "$TXNMEM_ENDPOINT" ] || [ -z "$TXNMEM_MODEL" ]; then
    echo "error: --endpoint and --model are required for shard execution" >&2
    exit 2
  fi
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
    if [ "$TXNMEM_RESUME" -eq 1 ] && [ -f "$TXNMEM_BOUND_REPORT" ]; then
      continue
    fi
    if [ "$TXNMEM_RESUME" -eq 1 ] && [ -f "$TXNMEM_RAW_REPORT" ]; then
      bind_shard_report "$TXNMEM_SHARD_PATH" "$TXNMEM_RAW_REPORT" "$TXNMEM_BOUND_REPORT"
      continue
    fi
    if [ -e "$TXNMEM_RUN_DIR" ] && [ "$TXNMEM_RESUME" -eq 0 ]; then
      echo "error: refusing to overwrite shard run without --resume: $TXNMEM_RUN_DIR" >&2
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
      "${TXNMEM_LOCOMO_EVALUATOR_ARGS[@]}"
    bind_shard_report "$TXNMEM_SHARD_PATH" "$TXNMEM_RAW_REPORT" "$TXNMEM_BOUND_REPORT"
  done
done
fi

if [ "$TXNMEM_GENERATE_ONLY" -eq 0 ]; then
mkdir -p "$TXNMEM_OUT_DIR/merged"
for TXNMEM_BENCHMARK_ITEM in "${TXNMEM_SELECTED_BENCHMARKS[@]}"; do
  TXNMEM_BENCHMARK="${TXNMEM_BENCHMARK_ITEM//[[:space:]]/}"
  TXNMEM_JOB="${TXNMEM_BENCHMARK//-/_}"
  "$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_JOB" "$TXNMEM_SHARD_COUNT" "$TXNMEM_RESUME" <<'PY'
import json
import sys
from pathlib import Path

from txnmem_batch_merge import merge_native_shards

out_dir = Path(sys.argv[1])
job = sys.argv[2]
shard_count = int(sys.argv[3])
resume = bool(int(sys.argv[4]))
try:
    parent = json.loads((out_dir / "manifests" / job / "parent.json").read_text(encoding="utf-8"))
    reports = [
        json.loads(
            (out_dir / "runs" / job / f"shard_{index:03d}" / "shard_report.json").read_text(
                encoding="utf-8"
            )
        )
        for index in range(shard_count)
    ]
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"cannot merge incomplete or malformed shard outputs for {job}: {exc}") from exc
merged = merge_native_shards(parent, reports)
path = out_dir / "merged" / f"{job}.json"
if path.exists():
    if not resume:
        raise SystemExit(f"refusing to overwrite existing merge without --resume: {path}")
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing resume merge is malformed: {path}") from exc
    if existing != merged:
        raise SystemExit(f"existing resume merge does not match recomputation: {path}")
else:
    path.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
PY
done
fi

if [ "$TXNMEM_GENERATE_ONLY" -eq 1 ]; then
  echo "native scale manifests generated: $TXNMEM_OUT_DIR"
else
  echo "native scale shards merged: $TXNMEM_OUT_DIR"
fi
