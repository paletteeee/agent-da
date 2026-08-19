#!/usr/bin/env bash
set -euo pipefail

TXNMEM_ROOT="${TXNMEM_ROOT:-/data/txnmem}"
TXNMEM_RUNTIME_ROOT="${TXNMEM_RUNTIME_ROOT:-${TXNMEM_ROOT}/.scale_runtime}"
TXNMEM_BASE_PYTHON="${TXNMEM_BASE_PYTHON:-python3}"
TXNMEM_LOCOMO_COMMIT="3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376"
TXNMEM_LOCOMO_SHORT_COMMIT="3eb6f2c5"
TXNMEM_LOCOMO_ARCHIVE_SHA256="6b79b8bc2637397c7297ada08a30c6f57aa77cddbe28106ee91db33680a2f6d3"
TXNMEM_LOCOMO_EVALUATOR_SHA256="8e3be5d57ff2ff9ec5cd05939592f468c5f3f1fd95d13e431932bdf6bf0fd6fd"
TXNMEM_LOCOMO_SOURCE="${TXNMEM_RUNTIME_ROOT}/locomo_official_${TXNMEM_LOCOMO_SHORT_COMMIT}"
TXNMEM_LOCOMO_PACKAGES="${TXNMEM_RUNTIME_ROOT}/python_packages/locomo_eval"
TXNMEM_EXISTING_PYTHONPATH="${PYTHONPATH-}"

for TXNMEM_REQUIRED_COMMAND in curl sha256sum tar mktemp; do
  command -v "$TXNMEM_REQUIRED_COMMAND" >/dev/null
done
"$TXNMEM_BASE_PYTHON" -c "import numpy, regex, requests, torch, transformers"

if [[ ! -f "${TXNMEM_LOCOMO_SOURCE}/task_eval/evaluation.py" ]]; then
  TXNMEM_STAGE_DIR="$(mktemp -d /tmp/txnmem-locomo-stage.XXXXXX)"
  TXNMEM_ARCHIVE="$(mktemp /tmp/txnmem-locomo-archive.XXXXXX.tar.gz)"
  trap 'rm -rf "${TXNMEM_STAGE_DIR:-}"; rm -f "${TXNMEM_ARCHIVE:-}"' EXIT
  curl -L --fail --silent --show-error \
    "https://codeload.github.com/snap-research/locomo/tar.gz/${TXNMEM_LOCOMO_COMMIT}" \
    -o "$TXNMEM_ARCHIVE"
  printf '%s  %s\n' "$TXNMEM_LOCOMO_ARCHIVE_SHA256" "$TXNMEM_ARCHIVE" | sha256sum -c -
  tar -xzf "$TXNMEM_ARCHIVE" \
    -C "$TXNMEM_STAGE_DIR" \
    --strip-components=1 \
    --exclude='*/__pycache__/*' \
    "locomo-${TXNMEM_LOCOMO_COMMIT}/task_eval" \
    "locomo-${TXNMEM_LOCOMO_COMMIT}/LICENSE.txt" \
    "locomo-${TXNMEM_LOCOMO_COMMIT}/README.MD"
  printf '%s  %s\n' \
    "$TXNMEM_LOCOMO_EVALUATOR_SHA256" \
    "$TXNMEM_STAGE_DIR/task_eval/evaluation.py" | sha256sum -c -
  cat >"$TXNMEM_STAGE_DIR/SOURCE_IDENTITY.json" <<JSON
{
  "archive_sha256": "${TXNMEM_LOCOMO_ARCHIVE_SHA256}",
  "commit": "${TXNMEM_LOCOMO_COMMIT}",
  "evaluator_sha256": "${TXNMEM_LOCOMO_EVALUATOR_SHA256}",
  "source": "https://github.com/snap-research/locomo"
}
JSON
  mkdir -p "$(dirname "$TXNMEM_LOCOMO_SOURCE")"
  if [[ -e "$TXNMEM_LOCOMO_SOURCE" ]]; then
    echo "refusing to replace existing LoCoMo source: $TXNMEM_LOCOMO_SOURCE" >&2
    exit 2
  fi
  mv "$TXNMEM_STAGE_DIR" "$TXNMEM_LOCOMO_SOURCE"
  TXNMEM_STAGE_DIR=""
  rm -f "$TXNMEM_ARCHIVE"
  TXNMEM_ARCHIVE=""
  trap - EXIT
fi

printf '%s  %s\n' \
  "$TXNMEM_LOCOMO_EVALUATOR_SHA256" \
  "$TXNMEM_LOCOMO_SOURCE/task_eval/evaluation.py" | sha256sum -c -

mkdir -p "$TXNMEM_LOCOMO_PACKAGES"
TXNMEM_EVAL_PYTHONPATH="${TXNMEM_LOCOMO_PACKAGES}:${TXNMEM_LOCOMO_SOURCE}"
if [[ -n "$TXNMEM_EXISTING_PYTHONPATH" ]]; then
  TXNMEM_EVAL_PYTHONPATH="${TXNMEM_EVAL_PYTHONPATH}:${TXNMEM_EXISTING_PYTHONPATH}"
fi

if ! PYTHONPATH="$TXNMEM_EVAL_PYTHONPATH" "$TXNMEM_BASE_PYTHON" -c \
  "import bert_score, joblib, matplotlib, nltk, pandas" 2>/dev/null; then
  TXNMEM_LOCOMO_PACKAGES_PINNED=(
    "nltk==3.9.2"
    "joblib==1.5.3"
    "bert-score==0.3.13"
    "pandas==3.0.5"
    "matplotlib==3.11.1"
    "python-dateutil==2.9.0.post0"
    "contourpy==1.3.3"
    "cycler==0.12.1"
    "fonttools==4.63.0"
    "kiwisolver==1.5.0"
    "pyparsing==3.3.2"
  )
  "$TXNMEM_BASE_PYTHON" -m pip install \
    --target "$TXNMEM_LOCOMO_PACKAGES" \
    --no-deps \
    --upgrade \
    "${TXNMEM_LOCOMO_PACKAGES_PINNED[@]}"
fi

PYTHONPATH="$TXNMEM_EVAL_PYTHONPATH" "$TXNMEM_BASE_PYTHON" - <<'PY'
from task_eval.evaluation import eval_question_answering

qas = [{"question": "Who?", "answer": "Alice", "category": 1, "prediction": "Alice"}]
result = eval_question_answering(qas, "prediction")
assert float(result[0][0]) == 1.0
PY

TXNMEM_LOCK_PATH="${TXNMEM_RUNTIME_ROOT}/locomo_evaluator_environment.json"
TXNMEM_LOCK_PATH="$TXNMEM_LOCK_PATH" \
TXNMEM_EVALUATOR_PATH="$TXNMEM_LOCOMO_SOURCE/task_eval/evaluation.py" \
PYTHONPATH="$TXNMEM_EVAL_PYTHONPATH" \
"$TXNMEM_BASE_PYTHON" - <<'PY'
import hashlib
import json
import os
import platform
from importlib.metadata import version
from pathlib import Path

evaluator = Path(os.environ["TXNMEM_EVALUATOR_PATH"])
payload = {
    "evaluator_sha256": hashlib.sha256(evaluator.read_bytes()).hexdigest(),
    "packages": {
        name: version(name)
        for name in ("bert-score", "matplotlib", "nltk", "pandas", "torch", "transformers")
    },
    "python": platform.python_version(),
}
Path(os.environ["TXNMEM_LOCK_PATH"]).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "LoCoMo official source: $TXNMEM_LOCOMO_SOURCE"
echo "LoCoMo evaluator packages: $TXNMEM_LOCOMO_PACKAGES"
echo "LoCoMo evaluator Python: $TXNMEM_BASE_PYTHON"
echo "TxnMem source path: ${TXNMEM_ROOT}/src"
echo "Use PYTHONPATH=${TXNMEM_ROOT}/src:${TXNMEM_EVAL_PYTHONPATH}"
