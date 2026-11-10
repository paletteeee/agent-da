#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUT_JSON" >&2
  exit 64
fi

smoke_out=$1
if [[ "$smoke_out" != /* ]]; then
  echo "formal smoke output must be an absolute path" >&2
  exit 64
fi
smoke_name=${smoke_out##*/}
smoke_parent=${smoke_out%/*}
if [[ -z "$smoke_name" || "$smoke_name" == "." || "$smoke_name" == ".." ]]; then
  echo "formal smoke output filename is invalid" >&2
  exit 64
fi
if [[ -z "$smoke_parent" ]]; then
  smoke_parent=/
fi
if [[ -L "$smoke_parent" || ! -d "$smoke_parent" ]]; then
  echo "formal smoke output parent is unavailable" >&2
  exit 2
fi

project_root=$(/usr/bin/readlink -f -- "$PWD")
resolved_parent=$(/usr/bin/readlink -f -- "$smoke_parent")
if [[ "$resolved_parent" != "$smoke_parent" ]]; then
  echo "formal smoke output parent must be canonical" >&2
  exit 2
fi
smoke_out="$resolved_parent/$smoke_name"
case "$smoke_out" in
  "$project_root"|"$project_root"/*)
    echo "formal smoke output must be outside the repository" >&2
    exit 2
    ;;
esac
if [[ -e "$smoke_out" || -L "$smoke_out" ]]; then
  echo "formal smoke output already exists" >&2
  exit 2
fi

: "${TXNMEM_NEO4J_PASSWORD:?TXNMEM_NEO4J_PASSWORD must be set}"

exec /usr/bin/env -i \
  LANG=C.UTF-8 \
  LC_ALL=C.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 \
  TXNMEM_NEO4J_PASSWORD="$TXNMEM_NEO4J_PASSWORD" \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" smoke --out "$smoke_out"
