#!/usr/bin/env bash
set -euo pipefail

TXNMEM_CODEX_DEPS="${TXNMEM_CODEX_DEPS:-}"
TXNMEM_RENDERER="${TXNMEM_RENDERER:-}"
TXNMEM_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TXNMEM_FONTCONFIG_FILE="${TXNMEM_FONTCONFIG_FILE:-${TXNMEM_SCRIPT_DIR}/fontconfig-macos.conf}"
TXNMEM_FONT_CACHE="${TXNMEM_FONT_CACHE:-${TMPDIR:-/tmp}/txnmem-fontconfig-cache}"

if [[ -z "$TXNMEM_CODEX_DEPS" ]] || [[ ! -x "$TXNMEM_CODEX_DEPS/python/bin/python3" ]]; then
    echo "set TXNMEM_CODEX_DEPS to the bundled workspace-dependency root" >&2
    exit 1
fi
if [[ -z "$TXNMEM_RENDERER" ]]; then
    echo "set TXNMEM_RENDERER to the documents render_docx.py path" >&2
    exit 1
fi
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
mkdir -p "$TXNMEM_FONT_CACHE"
export FONTCONFIG_FILE="$TXNMEM_FONTCONFIG_FILE"
export FONTCONFIG_PATH="$TXNMEM_SCRIPT_DIR"
export PATH="$TXNMEM_SCRIPT_DIR:${TXNMEM_CODEX_DEPS}/bin:${PATH:-/usr/bin:/bin}"
exec "${TXNMEM_CODEX_DEPS}/python/bin/python3" "$TXNMEM_RENDERER" "$@"
