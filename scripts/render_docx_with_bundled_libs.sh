#!/usr/bin/env bash
set -euo pipefail

TXNMEM_CODEX_DEPS="${TXNMEM_CODEX_DEPS:-/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies}"
TXNMEM_RENDERER="${TXNMEM_RENDERER:-/Users/xiaoyan_zhu/.codex/plugins/cache/openai-primary-runtime/documents/26.630.12135/skills/documents/render_docx.py}"
TXNMEM_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$TXNMEM_RENDERER" ]]; then
    echo "render_docx.py not found: $TXNMEM_RENDERER" >&2
    exit 1
fi

# Keep the runtime bin on PATH so render_docx.py does not prepend its own
# soffice wrapper ahead of this repository-local library-aware wrapper.
export PATH="$TXNMEM_SCRIPT_DIR:${TXNMEM_CODEX_DEPS}/bin:${PATH:-/usr/bin:/bin}"
exec "${TXNMEM_CODEX_DEPS}/python/bin/python3" "$TXNMEM_RENDERER" "$@"
