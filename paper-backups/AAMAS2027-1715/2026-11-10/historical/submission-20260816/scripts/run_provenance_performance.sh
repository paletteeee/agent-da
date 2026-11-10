#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 OUT_DIR ENVIRONMENT_ATTESTATION_JSON RUN_ID" >&2
  exit 64
fi

out_dir=$1
environment_attestation=$2
run_id=$3

: "${TXNMEM_NEO4J_PASSWORD:?TXNMEM_NEO4J_PASSWORD must be set}"

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_experiment.py \
  provenance-performance \
  --backend vector-graph \
  --config configs/provenance_performance_matrix.json \
  --run-id "$run_id" \
  --out-dir "$out_dir" \
  --service-url "${TXNMEM_QDRANT_URL:-http://127.0.0.1:6333}" \
  --environment-attestation "$environment_attestation" \
  --formal
