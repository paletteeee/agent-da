#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 DATA_ROOT [PYTHON]" >&2
  exit 2
fi

DATA_ROOT=$1
PYTHON_BIN=${2:-python3}
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

REVISION=98d7416c24c778c2fee6e6f3006e7a073259d48f
S_SHA256=d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442
S_SIZE=277383467
ORACLE_SHA256=821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c
ORACLE_SIZE=15388478
S_URL="https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/$REVISION/longmemeval_s_cleaned.json"
ORACLE_URL="https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/$REVISION/longmemeval_oracle.json"
OFFICIAL_COMMIT=9e0b455f4ef0e2ab8f2e582289761153549043fc
OFFICIAL_SHORT_COMMIT=9e0b455f
OFFICIAL_ARCHIVE_SHA256=466228f1df97704bafce005f0a5291973b2cb1f420e2dc600013346b9b82ce63
OFFICIAL_ARCHIVE_SIZE=984710
OFFICIAL_EVALUATOR_SHA256=ecce9c4c79dc89d99534ac17b383a5cbb5b9f0c69ee98adaf0684742e3d95251
OFFICIAL_METRICS_SHA256=e9283933a0cefb7a0ded7365e436ae3d1be5aac41853325e6155d83bf07607f0
OFFICIAL_REQUIREMENTS_SHA256=d9d66e3c70fa859855f0fb47f3b3ee39b881d599e27f9b10ba725c7796a9d14b
OFFICIAL_URL="https://codeload.github.com/xiaowu0162/LongMemEval/tar.gz/$OFFICIAL_COMMIT"
RAW_DIR="$DATA_ROOT/raw"
TARGET="$RAW_DIR/longmemeval_s_cleaned.json"
ORACLE_TARGET="$RAW_DIR/longmemeval_oracle.json"
OFFICIAL_ROOT="$DATA_ROOT/official_$OFFICIAL_SHORT_COMMIT"

file_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

verify_source() {
  local source_path=$1
  local expected_size=$2
  local expected_sha256=$3
  local label=$4
  local actual_size
  local actual_sha256
  actual_size=$(wc -c < "$source_path" | tr -d '[:space:]')
  actual_sha256=$(file_sha256 "$source_path")
  if [[ "$actual_size" != "$expected_size" ]]; then
    echo "$label size mismatch: expected $expected_size, got $actual_size" >&2
    return 1
  fi
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "$label SHA-256 mismatch" >&2
    return 1
  fi
}

download_pinned() {
  local source_url=$1
  local target_path=$2
  local expected_size=$3
  local expected_sha256=$4
  local label=$5
  local partial_path="$target_path.partial.$$"
  if [[ -e "$target_path" ]]; then
    if [[ ! -f "$target_path" || -L "$target_path" ]]; then
      echo "refusing non-regular existing $label target" >&2
      return 1
    fi
    verify_source "$target_path" "$expected_size" "$expected_sha256" "$label"
    return
  fi
  if ! curl --fail --location --silent --show-error \
    "$source_url" --output "$partial_path"; then
    rm -f "$partial_path"
    return 1
  fi
  if ! verify_source \
    "$partial_path" "$expected_size" "$expected_sha256" "$label"; then
    rm -f "$partial_path"
    return 1
  fi
  mv "$partial_path" "$target_path"
}

mkdir -p "$RAW_DIR"
download_pinned "$S_URL" "$TARGET" "$S_SIZE" "$S_SHA256" "LongMemEval-S"
download_pinned \
  "$ORACLE_URL" "$ORACLE_TARGET" "$ORACLE_SIZE" "$ORACLE_SHA256" \
  "LongMemEval oracle"

if [[ ! -f "$OFFICIAL_ROOT/src/evaluation/evaluate_qa.py" ]]; then
  if [[ -e "$OFFICIAL_ROOT" ]]; then
    echo "refusing incomplete existing LongMemEval official source" >&2
    exit 1
  fi
  STAGE_DIR=$(mktemp -d /tmp/txnmem-longmemeval-source.XXXXXX)
  ARCHIVE=$(mktemp /tmp/txnmem-longmemeval-source.XXXXXX.tar.gz)
  trap 'rm -rf "${STAGE_DIR:-}"; rm -f "${ARCHIVE:-}"' EXIT
  curl --fail --location --silent --show-error "$OFFICIAL_URL" --output "$ARCHIVE"
  verify_source \
    "$ARCHIVE" "$OFFICIAL_ARCHIVE_SIZE" "$OFFICIAL_ARCHIVE_SHA256" \
    "LongMemEval official archive"
  tar -xzf "$ARCHIVE" -C "$STAGE_DIR" --strip-components=1 \
    "LongMemEval-$OFFICIAL_COMMIT/src/evaluation/evaluate_qa.py" \
    "LongMemEval-$OFFICIAL_COMMIT/src/evaluation/print_qa_metrics.py" \
    "LongMemEval-$OFFICIAL_COMMIT/requirements-lite.txt" \
    "LongMemEval-$OFFICIAL_COMMIT/README.md" \
    "LongMemEval-$OFFICIAL_COMMIT/LICENSE"
  cat >"$STAGE_DIR/SOURCE_IDENTITY.json" <<JSON
{
  "archive_sha256": "$OFFICIAL_ARCHIVE_SHA256",
  "commit": "$OFFICIAL_COMMIT",
  "evaluate_qa_sha256": "$OFFICIAL_EVALUATOR_SHA256",
  "print_qa_metrics_sha256": "$OFFICIAL_METRICS_SHA256",
  "requirements_lite_sha256": "$OFFICIAL_REQUIREMENTS_SHA256",
  "source": "https://github.com/xiaowu0162/LongMemEval"
}
JSON
  mv "$STAGE_DIR" "$OFFICIAL_ROOT"
  STAGE_DIR=""
  rm -f "$ARCHIVE"
  ARCHIVE=""
  trap - EXIT
fi

[[ "$(file_sha256 "$OFFICIAL_ROOT/src/evaluation/evaluate_qa.py")" == \
  "$OFFICIAL_EVALUATOR_SHA256" ]]
[[ "$(file_sha256 "$OFFICIAL_ROOT/src/evaluation/print_qa_metrics.py")" == \
  "$OFFICIAL_METRICS_SHA256" ]]
[[ "$(file_sha256 "$OFFICIAL_ROOT/requirements-lite.txt")" == \
  "$OFFICIAL_REQUIREMENTS_SHA256" ]]

PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
  "$PYTHON_BIN" "$PROJECT_ROOT/src/longmemeval_eval.py" \
  preflight --data "$TARGET" --oracle "$ORACLE_TARGET"

echo "LongMemEval official evaluator source: $OFFICIAL_ROOT"
