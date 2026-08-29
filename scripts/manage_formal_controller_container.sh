#!/usr/bin/env bash
set -euo pipefail

script_root=$(/usr/bin/cd "$(/usr/bin/dirname "$0")/.." && /bin/pwd -P)
/usr/bin/python3 -I "$script_root/src/txnmem_formal_controller_container.py" "$@"
