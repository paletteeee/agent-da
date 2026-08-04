#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="${TXNMEM_ROOT:-/data/txnmem}"
LOCOMO_ROOT="${LOCOMO_ROOT:-/data/locomo}"
LOCOMO_VENV="${LOCOMO_VENV:-/data/venvs/locomo-agent}"
LOCOMO_SOURCE="${LOCOMO_SOURCE:-${TXNMEM_ROOT}/external_data/raw/locomo10.json}"
LOCOMO_ENDPOINT="${LOCOMO_ENDPOINT:-http://127.0.0.1:8000/v1}"
LOCOMO_MODEL="${LOCOMO_MODEL:-qwen2.5-7b-instruct}"
LOCOMO_LIMIT="${LOCOMO_LIMIT:-1}"
LOCOMO_CONTEXT_MAX_CHARS="${LOCOMO_CONTEXT_MAX_CHARS:-4000}"
LOCOMO_OUT_DIR="${LOCOMO_OUT_DIR:-${TXNMEM_ROOT}/results/locomo_native_smoke}"

test -x "${LOCOMO_VENV}/bin/python"
test -d "${LOCOMO_ROOT}"
test -f "${LOCOMO_SOURCE}"

export PYTHONPATH="${TXNMEM_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export TXNMEM_LOCOMO_CONTEXT_MAX_CHARS="${LOCOMO_CONTEXT_MAX_CHARS}"
exec "${LOCOMO_VENV}/bin/python" "${TXNMEM_ROOT}/src/txnmem_experiment.py" \
  public-native-smoke \
  --dataset locomo \
  --source "${LOCOMO_SOURCE}" \
  --limit "${LOCOMO_LIMIT}" \
  --out-dir "${LOCOMO_OUT_DIR}" \
  --endpoint "${LOCOMO_ENDPOINT}" \
  --model "${LOCOMO_MODEL}"
