#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  run_formal_provenance_ablation.sh smoke-run PROTECTED_SMOKE_REPORT_JSON
  run_formal_provenance_ablation.sh smoke PROTECTED_SMOKE_REPORT_JSON
  run_formal_provenance_ablation.sh create FORMAL_OUT_DIR
  run_formal_provenance_ablation.sh run CANDIDATE_ROOT
  run_formal_provenance_ablation.sh resume CANDIDATE_ROOT
  run_formal_provenance_ablation.sh seal CANDIDATE_ROOT
  run_formal_provenance_ablation.sh validate RECEIPT_JSON ATTESTATIONS_JSON SAMPLES_JSONL REPETITIONS_JSONL CANDIDATE_ROOT FORMAL_OUT_DIR VALIDATED_JSON
  run_formal_provenance_ablation.sh promote RECEIPT_JSON ATTESTATIONS_JSON SAMPLES_JSONL REPETITIONS_JSONL CANDIDATE_ROOT FORMAL_OUT_DIR

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
  smoke-run)
    [[ $# -eq 1 && "$1" = /* ]] || usage
    exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 TXNMEM_NEO4J_PASSWORD="${TXNMEM_NEO4J_PASSWORD:-}" \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" ablation-smoke-run --out "$1"
    ;;
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
  create)
    [[ $# -eq 1 && "$1" = /* ]] || usage
    exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" ablation-create --formal-out-dir "$1"
    ;;
  run|resume|seal)
    [[ $# -eq 1 && "$1" = /* && -d "$1" && ! -L "$1" ]] || usage
    exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 TXNMEM_NEO4J_PASSWORD="${TXNMEM_NEO4J_PASSWORD:-}" \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" "ablation-$action" --candidate-root "$1"
    ;;
  validate)
    [[ $# -eq 7 ]] || usage
    controller_action=ablation-validate
    ;;
  promote)
    [[ $# -eq 6 ]] || usage
    controller_action=ablation-promote
    ;;
  *) usage ;;
esac

for input in "$1" "$2" "$3" "$4"; do
  [[ "$input" = /* && -f "$input" && ! -L "$input" ]] || {
    echo "formal ablation input must be an absolute regular file" >&2
    exit 2
  }
done
[[ "$5" = /* && -d "$5" && ! -L "$5" ]] || { echo "candidate root is invalid" >&2; exit 2; }

if [[ "$controller_action" == ablation-validate ]]; then
  exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
    /usr/bin/python3 -I -S -B \
    /opt/txnmem-formal-controller/txnmem_formal_controller.py \
    --project-root "$PWD" ablation-validate \
    --receipt "$1" --attestations "$2" --samples "$3" --repetitions "$4" \
    --candidate-root "$5" --formal-out-dir "$6" --out "$7"
fi

exec /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" ablation-promote \
  --receipt "$1" --attestations "$2" --samples "$3" --repetitions "$4" \
  --candidate-root "$5" --formal-out-dir "$6" --out-dir "$6"
