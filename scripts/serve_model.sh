#!/usr/bin/env bash
# Start the vLLM OpenAI-compatible endpoint for TxnMem native benchmark runs.
#
# Usage:
#   MODEL_PATH=/data/models/Qwen/Qwen2___5-7B-Instruct bash scripts/serve_model.sh
set -euo pipefail

MODEL_PATH="${MODEL_PATH:-/data/models/Qwen/Qwen2___5-7B-Instruct}"
MODEL_ID="${MODEL_ID:-qwen2.5-7b-instruct}"
PORT="${PORT:-8000}"
HOST="${HOST:-127.0.0.1}"

uv run --python 3.11 vllm serve "$MODEL_PATH" \
  --host "$HOST" \
  --port "$PORT" \
  --served-model-name "$MODEL_ID" \
  --dtype half \
  --gpu-memory-utilization 0.90 \
  --max-model-len 8192 \
  --enable-auto-tool-choice \
  --tool-call-parser hermes \
  --disable-log-requests
