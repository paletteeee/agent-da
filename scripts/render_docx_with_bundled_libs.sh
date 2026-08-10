#!/usr/bin/env bash
set -euo pipefail

TXNMEM_CODEX_DEPS="${TXNMEM_CODEX_DEPS:-/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies}"
TXNMEM_RENDERER="${TXNMEM_RENDERER:-/Users/xiaoyan_zhu/.codex/plugins/cache/openai-primary-runtime/documents/26.805.11740/skills/documents/render_docx.py}"
TXNMEM_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TXNMEM_FONTCONFIG_FILE="${TXNMEM_FONTCONFIG_FILE:-${TXNMEM_SCRIPT_DIR}/fontconfig-macos.conf}"

if [[ ! -f "$TXNMEM_RENDERER" ]]; then
    echo "render_docx.py not found: $TXNMEM_RENDERER" >&2
    exit 1
fi
if [[ ! -f "$TXNMEM_FONTCONFIG_FILE" ]]; then
    echo "Fontconfig file not found: $TXNMEM_FONTCONFIG_FILE" >&2
    exit 1
fi

# Keep the runtime bin on PATH so render_docx.py does not prepend its own
# soffice wrapper ahead of this repository-local library-aware wrapper.
mkdir -p /private/tmp/txnmem-fontconfig-cache
export FONTCONFIG_FILE="$TXNMEM_FONTCONFIG_FILE"
export FONTCONFIG_PATH="$TXNMEM_SCRIPT_DIR"
export PATH="$TXNMEM_SCRIPT_DIR:${TXNMEM_CODEX_DEPS}/bin:${PATH:-/usr/bin:/bin}"
exec "${TXNMEM_CODEX_DEPS}/python/bin/python3" "$TXNMEM_RENDERER" "$@"
