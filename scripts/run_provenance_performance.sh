#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: $0 CANDIDATE_OUT_DIR LAUNCH_JSON COMPLETION_JSON AUTHORIZATION_NONCE RUN_ID TRANSPORT" >&2
  exit 64
fi

out_dir=$1
launch_json=$2
completion_json=$3
authorization_nonce=$4
run_id=$5
transport=$6

: "${TXNMEM_NEO4J_PASSWORD:?TXNMEM_NEO4J_PASSWORD must be set}"

/usr/bin/env -i \
  LANG=C.UTF-8 \
  LC_ALL=C.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 \
  TXNMEM_NEO4J_PASSWORD="$TXNMEM_NEO4J_PASSWORD" \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" measure \
  --candidate-root "$out_dir" \
  --launch-out "$launch_json" \
  --completion-out "$completion_json" \
  --authorization-nonce "$authorization_nonce" \
  --run-id "$run_id" \
  --transport "$transport"

# The measured output is an immutable diagnostic candidate. Formal status is granted
# only by provenance-promote after independent topology collection and digest
# registration; promotion reuses these exact measured bytes.
