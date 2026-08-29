#!/bin/sh
set -eu

repository_root="$(command pwd -P)"
exec /usr/bin/env -i \
  LANG=C.UTF-8 LC_ALL=C.UTF-8 \
  /usr/bin/python3 -I \
  "$repository_root/src/txnmem_formal_controller_container.py" "$@"
