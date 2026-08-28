#!/bin/sh
set -eu

reader_install_path=/opt/txnmem-formal-controller/read_formal_provenance_progress.sh
case "$0" in
  "$reader_install_path") ;;
  *) exit 77 ;;
esac

case "$#" in
  2) ;;
  *) exit 64 ;;
esac

exec /usr/bin/env -i \
  LANG=C.UTF-8 \
  LC_ALL=C.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" progress --run-id "$1" --authorization-nonce "$2"
