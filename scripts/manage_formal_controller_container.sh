#!/usr/bin/env bash
set -euo pipefail

if [[ "${TXNMEM_FORMAL_CONTAINER_WRAPPER_SANITIZED:-}" != "1" ]]; then
  script_path="$0"
  if [[ "$script_path" != /* ]]; then
    script_path="$PWD/${script_path#./}"
  fi
  exec /usr/bin/env -i \
    TXNMEM_FORMAL_CONTAINER_WRAPPER_SANITIZED=1 \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    /bin/bash --noprofile --norc "$script_path" "$@"
fi

script_directory=$(/usr/bin/dirname -- "$0")
script_root=$(builtin cd -- "$script_directory/.." && /bin/pwd -P)
unset TXNMEM_FORMAL_CONTAINER_WRAPPER_SANITIZED
/usr/bin/python3 -I "$script_root/src/txnmem_formal_controller_container.py" "$@"
