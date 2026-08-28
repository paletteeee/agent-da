#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: read_formal_provenance_progress.sh RUN_ID AUTHORIZATION_NONCE" >&2
  exit 64
fi

exec /usr/bin/env -i \
  LANG=C.UTF-8 \
  LC_ALL=C.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" progress --run-id "$1" --authorization-nonce "$2"
