#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TXNMEM_PYTHON="${TXNMEM_PYTHON:-${TXNMEM_ROOT}/.venv/bin/python}"
TXNMEM_ENDPOINT="${TXNMEM_ENDPOINT:-}"
TXNMEM_MODEL="${TXNMEM_MODEL:-}"
TXNMEM_OUT_DIR="${TXNMEM_OUT_DIR:-${TXNMEM_ROOT}/results/remote_evidence}"
TXNMEM_APPWORLD_ROOT="${TXNMEM_APPWORLD_ROOT:-${TXNMEM_ROOT}/external_data/deps/appworld-data}"
TXNMEM_LOCOMO_SOURCE="${TXNMEM_LOCOMO_SOURCE:-${TXNMEM_ROOT}/external_data/raw/locomo10.json}"

mkdir -p "$TXNMEM_OUT_DIR"
if [ -z "$TXNMEM_ENDPOINT" ] || [ -z "$TXNMEM_MODEL" ]; then
  python3 - "$TXNMEM_OUT_DIR" <<'PY'
import json
import sys
from pathlib import Path

Path(sys.argv[1], "blocked_report.json").write_text(
    json.dumps(
        {"status": "blocked", "reason": "missing_model_endpoint_or_model", "production_latency_claim": False},
        indent=2,
    ) + "\n",
    encoding="utf-8",
)
PY
  echo "remote evidence blocked: set TXNMEM_ENDPOINT and TXNMEM_MODEL" >&2
  exit 2
fi
if [ ! -x "$TXNMEM_PYTHON" ]; then
  echo "remote evidence blocked: missing Python interpreter $TXNMEM_PYTHON" >&2
  exit 2
fi
if ! curl --fail --silent --show-error --max-time 10 "$TXNMEM_ENDPOINT/models" >/dev/null; then
  echo "remote evidence blocked: model endpoint health check failed" >&2
  exit 2
fi

export PYTHONPATH="$TXNMEM_ROOT/src"
"$TXNMEM_PYTHON" - "$TXNMEM_OUT_DIR" "$TXNMEM_APPWORLD_ROOT" "$TXNMEM_LOCOMO_SOURCE" <<'PY'
import json
import sys
from pathlib import Path

from txnmem_benchmark_manifests import build_native_scale_manifest

out_dir, appworld_root, locomo_source = sys.argv[1:]
out_dir = Path(out_dir)
checks = {"status": "ready", "production_latency_claim": False, "manifests": {}}
for benchmark, source, limit in (
    ("appworld", appworld_root, 20),
    ("locomo", locomo_source, 10),
):
    manifest = build_native_scale_manifest(benchmark, source, limit, seed=17, split="test")
    checks["manifests"][benchmark] = {"task_count": limit, "manifest_hash": manifest["manifest_hash"]}
(out_dir / "preflight.json").write_text(json.dumps(checks, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

nohup bash "$TXNMEM_ROOT/scripts/run_native_scale.sh" \
  --endpoint "$TXNMEM_ENDPOINT" \
  --model "$TXNMEM_MODEL" \
  --out-dir "$TXNMEM_OUT_DIR/native_scale" \
  > "$TXNMEM_OUT_DIR/native_scale.log" 2>&1 &
echo $! > "$TXNMEM_OUT_DIR/native_scale.pid"
echo "remote evidence started; log=$TXNMEM_OUT_DIR/native_scale.log"
