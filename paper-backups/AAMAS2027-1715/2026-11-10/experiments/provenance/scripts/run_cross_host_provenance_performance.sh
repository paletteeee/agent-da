#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  run_cross_host_provenance_performance.sh measure CANDIDATE_ROOT LAUNCH_JSON COMPLETION_JSON AUTHORIZATION_NONCE RUN_ID TRANSPORT
  run_cross_host_provenance_performance.sh material CANDIDATE_ROOT CANDIDATE_BUNDLE_ID MATERIAL_JSON
  run_cross_host_provenance_performance.sh attest LAUNCH_JSON COMPLETION_JSON AUTHORIZATION_NONCE SANITIZED_TOPOLOGY_JSON
  run_cross_host_provenance_performance.sh promote CANDIDATE_ROOT CANDIDATE_BUNDLE_ID SANITIZED_TOPOLOGY_JSON FORMAL_OUT_DIR
  run_cross_host_provenance_performance.sh smoke OUT_JSON

Required order: generate an out-of-tree 0600 nonce -> pre-register its SHA-256
for the run-id hash in source and commit -> measure -> material -> attest -> independently register the
sanitized attestation digest -> promote. Promotion never reruns measurement.
EOF
  exit 64
}

if [[ $# -lt 1 ]]; then
  usage
fi

action=$1
shift

case "$action" in
  measure)
    [[ $# -eq 6 ]] || usage
    scripts/run_provenance_performance.sh "$1" "$2" "$3" "$4" "$5" "$6"
    ;;
  material)
    [[ $# -eq 3 ]] || usage
    /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" material \
      --candidate-root "$1" \
      --bundle-id "$2" \
      --out "$3"
    ;;
  attest)
    [[ $# -eq 4 ]] || usage
    /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" attest \
      --launch "$1" \
      --completion "$2" \
      --authorization-nonce "$3" \
      --out "$4"
    echo "pause: independently review and register the sanitized attestation digest before promote" >&2
    ;;
  promote)
    [[ $# -eq 4 ]] || usage
    /usr/bin/env -i LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 \
      /usr/bin/python3 -I -S -B \
      /opt/txnmem-formal-controller/txnmem_formal_controller.py \
      --project-root "$PWD" promote \
      --candidate-root "$1" \
      --bundle-id "$2" \
      --topology-attestation "$3" \
      --out-dir "$4"
    ;;
  smoke)
    [[ $# -eq 1 ]] || usage
    scripts/run_formal_provenance_smoke.sh "$1"
    ;;
  *)
    usage
    ;;
esac
