#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="${TXNMEM_ROOT:-/data/txnmem}"
LOCOMO_ROOT="${LOCOMO_ROOT:-/data/locomo}"
LOCOMO_VENV="${LOCOMO_VENV:-/data/venvs/locomo-agent}"

if [[ ! -d "${LOCOMO_ROOT}/.git" ]]; then
  git clone --depth 1 https://github.com/snap-research/locomo.git "${LOCOMO_ROOT}"
fi

if [[ ! -x "${LOCOMO_VENV}/bin/python" ]]; then
  python3 -m venv "${LOCOMO_VENV}"
fi

"${LOCOMO_VENV}/bin/python" -m pip install --upgrade pip

echo "LoCoMo source: ${LOCOMO_ROOT}"
echo "LoCoMo agent Python: ${LOCOMO_VENV}/bin/python"
echo "TxnMem source path: ${TXNMEM_ROOT}/src"
echo "Use: export PYTHONPATH=${TXNMEM_ROOT}/src"
