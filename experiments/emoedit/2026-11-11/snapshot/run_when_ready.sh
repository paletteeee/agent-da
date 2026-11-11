#!/usr/bin/env bash
set -euo pipefail
cd /data/emoedit-baseline
while [ "$(stat -c %s models/Q-Former_4000.pth 2>/dev/null || echo 0)" -lt 397305617 ]; do sleep 5; done
printf '%s  %s\n' '24b91803c39691c4bd60a88c1df86f857a1e520645fdb9b039da4e7bde5436bc' 'models/Q-Former_4000.pth' | sha256sum -c -
set +e
.venv/bin/python -u run_demo.py > logs/demo.log 2>&1
result=$?
printf '%s\n' "$result" > logs/demo.exit
exit "$result"
