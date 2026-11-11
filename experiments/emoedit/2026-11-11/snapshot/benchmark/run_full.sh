#!/usr/bin/env bash
set -Eeuo pipefail
cd /data/emoedit-baseline
exec 9>benchmark/full.lock
flock -n 9 || exit 73
PYTHON=.venv/bin/python
RUN=benchmark/official30_seed48000_qformer4000
stage=starting
status() {
 "$PYTHON" - "$stage" "$$" "$1" <<'PY'
import sys,datetime
from benchmark.protocol import RUN,atomic_json
atomic_json(RUN/'pipeline_status.json',{'stage':sys.argv[1],'pid':int(sys.argv[2]),'exit_code':int(sys.argv[3]),'updated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
PY
}
trap 'rc=$?; if [ "$rc" -ne 0 ]; then stage="failed:$stage"; status "$rc"; fi' EXIT
stage=generation;status 0
"$PYTHON" -u benchmark/generate.py > benchmark/logs/full_generation.log 2>&1
stage=metrics;status 0
"$PYTHON" -u benchmark/evaluate.py --batch-size 16 > benchmark/logs/full_metrics.log 2>&1
stage=final_audit;status 0
"$PYTHON" -u benchmark/finalize.py > benchmark/logs/final_audit.log 2>&1
stage=complete;status 0
