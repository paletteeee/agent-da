#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  run_formal_provenance_ablation.sh smoke PROTECTED_SMOKE_REPORT_JSON
  run_formal_provenance_ablation.sh validate RECEIPT_JSON SAMPLES_JSONL REPETITIONS_JSONL VALIDATED_JSON
  run_formal_provenance_ablation.sh promote RECEIPT_JSON SAMPLES_JSONL REPETITIONS_JSONL FORMAL_OUT_DIR

The candidate must be produced under the protected formal host lifecycle.
Validation and promotion re-read exact candidate bytes from a committed,
root-approved source export. Promotion is exclusive and never overwrites.
EOF
  exit 64
}

[[ $# -ge 1 ]] || usage
action=$1
shift

case "$action" in
  smoke)
    [[ $# -eq 1 ]] || usage
    [[ "$1" = /* && -f "$1" && ! -L "$1" ]] || {
      echo "formal ablation smoke report must be an absolute regular file" >&2
      exit 2
    }
    exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" ablation-smoke --report "$1"
    ;;
  validate)
    [[ $# -eq 4 ]] || usage
    controller_action=ablation-validate
    output_flag=--out
    ;;
  promote)
    [[ $# -eq 4 ]] || usage
    controller_action=ablation-promote
    output_flag=--out-dir
    ;;
  *) usage ;;
esac

for input in "$1" "$2" "$3"; do
  [[ "$input" = /* && -f "$input" && ! -L "$input" ]] || {
    echo "formal ablation input must be an absolute regular file" >&2
    exit 2
  }
done
[[ "$4" = /* && ! -e "$4" && ! -L "$4" ]] || {
  echo "formal ablation output must be a new absolute path" >&2
  exit 2
}

exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" "$controller_action" \
  --receipt "$1" --samples "$2" --repetitions "$3" "$output_flag" "$4"
