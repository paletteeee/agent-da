#!/usr/bin/env bash
set -euo pipefail

# Re-enter before doing any external work so inherited PATH, Bash functions,
# Python/Git variables and startup-file hooks cannot select installation tools.
if [[ "${TXNMEM_FORMAL_INSTALL_SANITIZED:-}" != "1" ]]; then
  script_path="$0"
  if [[ "$script_path" != /* ]]; then
    script_path="$PWD/${script_path#./}"
  fi
  wheel_source="${TXNMEM_FORMAL_WHEEL_SOURCE:-}"
  exec /usr/bin/env -i \
    TXNMEM_FORMAL_INSTALL_SANITIZED=1 \
    TXNMEM_FORMAL_WHEEL_SOURCE="$wheel_source" \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    /bin/bash --noprofile --norc "$script_path" "$@"
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "formal runtime installation requires root" >&2
  exit 77
fi
if [[ "$(/usr/bin/uname -s)" != "Linux" ]]; then
  echo "formal runtime installation requires Linux" >&2
  exit 78
fi
if [[ "$#" -ne 2 || ! "$2" =~ ^[0-9a-f]{40}([0-9a-f]{24})?$ ]]; then
  echo "usage: install_formal_provenance_runtime.sh PROJECT_ROOT APPROVED_COMMIT" >&2
  exit 64
fi

project_argument="$1"
approved_commit="$2"
controller_dir=/opt/txnmem-formal-controller
controller_target="$controller_dir/txnmem_formal_controller.py"
approval_target="$controller_dir/approved_source_manifest.json"
runtime_root=/opt/txnmem-formal-runtime
wheel_dir="$runtime_root/wheels"
runs_root=/var/lib/txnmem-formal/runs
bootstrap_root=/var/lib/txnmem-formal/bootstrap
runner_name=txnmem-formal
runner_uid=65532
runner_gid=65532

required_executables=(
  /bin/bash
  /usr/bin/chmod
  /usr/bin/docker
  /usr/bin/env
  /usr/bin/find
  /usr/bin/getent
  /usr/bin/git
  /usr/bin/install
  /usr/bin/mktemp
  /usr/bin/mv
  /usr/bin/python3
  /usr/bin/readlink
  /usr/bin/rmdir
  /usr/bin/sort
  /usr/bin/stat
  /usr/bin/uname
  /usr/sbin/groupadd
  /usr/sbin/nft
  /usr/sbin/useradd
)
for executable in "${required_executables[@]}"; do
  if [[ ! -x "$executable" ]]; then
    echo "required protected executable is unavailable: $executable" >&2
    exit 2
  fi
  if [[ -L "$executable" ]]; then
    resolved=$(/usr/bin/readlink -f "$executable")
  else
    resolved=$executable
  fi
  owner_mode=$(/usr/bin/stat -Lc '%u:%a' "$resolved")
  owner=${owner_mode%%:*}
  mode=${owner_mode##*:}
  if [[ "$owner" != 0 ]] || (( (8#$mode & 8#022) != 0 )); then
    echo "required executable is not root protected: $executable" >&2
    exit 2
  fi
done

project_root=$(/usr/bin/readlink -f "$project_argument")
observed_root=$(/usr/bin/git -c "safe.directory=$project_root" -C "$project_root" rev-parse --show-toplevel)
if [[ "$observed_root" != "$project_root" ]]; then
  echo "PROJECT_ROOT is not the exact approved repository root" >&2
  exit 2
fi
observed_commit=$(/usr/bin/git -c "safe.directory=$project_root" -C "$project_root" rev-parse HEAD)
if [[ "$observed_commit" != "$approved_commit" ]]; then
  echo "repository HEAD does not match APPROVED_COMMIT" >&2
  exit 2
fi

staging=$(/usr/bin/mktemp -d /var/tmp/txnmem-formal-install.XXXXXX)
cleanup() {
  if [[ -n "${staging:-}" && -d "$staging" ]]; then
    /usr/bin/find "$staging" -mindepth 1 -delete
    /usr/bin/rmdir "$staging"
  fi
}
trap cleanup EXIT

SCRIPT_PATH=$(/usr/bin/readlink -f "$0") \
PROJECT_ROOT="$project_root" APPROVED_COMMIT="$approved_commit" \
STAGING="$staging" /usr/bin/python3 -I -S -B - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

root = Path(os.environ["PROJECT_ROOT"]).resolve(strict=True)
commit = os.environ["APPROVED_COMMIT"]
staging = Path(os.environ["STAGING"]).resolve(strict=True)
script_path = Path(os.environ["SCRIPT_PATH"]).resolve(strict=True)
git = "/usr/bin/git"
auxiliary = {
    "configs/provenance_performance_matrix.json",
    "configs/provenance_runtime_lock.json",
    "infra/real_backend/docker-compose.yml",
    "scripts/install_formal_provenance_runtime.sh",
    "scripts/run_cross_host_provenance_performance.sh",
    "scripts/run_formal_provenance_smoke.sh",
    "scripts/run_provenance_performance.sh",
}
required = auxiliary | {
    "src/txnmem_formal_controller.py",
    "src/txnmem_formal_smoke.py",
    "src/txnmem_provenance_execution_collector.py",
    "src/txnmem_provenance_runner.py",
}

def run(*args, text=False):
    return subprocess.run(
        [git, "-c", f"safe.directory={root}", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=text,
        env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
    ).stdout

if run("rev-parse", "HEAD", text=True).strip() != commit:
    raise SystemExit("repository HEAD changed during approval")
listing = run("ls-tree", "-r", "--name-only", "-z", commit, "--", "src")
try:
    names = listing.decode("utf-8").split("\0")
except UnicodeError as exc:
    raise SystemExit("formal source path is not UTF-8") from exc
paths = sorted(
    auxiliary
    | {
        name
        for name in names
        if name.startswith("src/") and name.endswith(".py")
    }
)
if not required.issubset(paths) or len(paths) != len(set(paths)):
    raise SystemExit("formal approved source closure is incomplete")

rows = []
payloads = {}
for relative in paths:
    if (
        not relative
        or relative.startswith("/")
        or ".." in Path(relative).parts
        or not re.fullmatch(r"[A-Za-z0-9_./-]+", relative)
    ):
        raise SystemExit("formal source path is unsafe")
    payload = run("show", f"{commit}:{relative}")
    payloads[relative] = payload
    rows.append(
        {
            "path": relative,
            "blob_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
if script_path.read_bytes() != payloads["scripts/install_formal_provenance_runtime.sh"]:
    raise SystemExit("running installer differs from the approved Git blob")

manifest = {
    "schema": "txnmem-formal-approved-source-v1",
    "source_commit": commit,
    "files": rows,
}
(staging / "approved_source_manifest.json").write_bytes(
    json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    + b"\n"
)
(staging / "txnmem_formal_controller.py").write_bytes(
    payloads["src/txnmem_formal_controller.py"]
)
(staging / "provenance_runtime_lock.json").write_bytes(
    payloads["configs/provenance_runtime_lock.json"]
)
for path in (
    staging / "approved_source_manifest.json",
    staging / "txnmem_formal_controller.py",
    staging / "provenance_runtime_lock.json",
):
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
if run("rev-parse", "HEAD", text=True).strip() != commit:
    raise SystemExit("repository HEAD changed during approval")
PY

lock_path="$staging/provenance_runtime_lock.json"
python_version=$(/usr/bin/python3 -I -S -B -c 'import platform; print(platform.python_version())')
LOCK_PATH="$lock_path" PYTHON_VERSION="$python_version" /usr/bin/python3 -I -S -B -c '
import json, os
with open(os.environ["LOCK_PATH"], "r", encoding="utf-8") as stream:
    lock = json.load(stream)
if lock.get("schema") != "txnmem-provenance-runtime-lock-v1":
    raise SystemExit("runtime lock schema mismatch")
if os.environ["PYTHON_VERSION"] not in lock.get("python_versions", []):
    raise SystemExit("/usr/bin/python3 is not source-registered")
'

wheel_staging="$staging/wheels"
/usr/bin/install -d -o root -g root -m 0700 "$wheel_staging"
wheel_source=${TXNMEM_FORMAL_WHEEL_SOURCE:-}
if [[ -n "$wheel_source" ]]; then
  if [[ ! -d "$wheel_source" ]]; then
    echo "TXNMEM_FORMAL_WHEEL_SOURCE is not a directory" >&2
    exit 2
  fi
  while IFS= read -r filename; do
    /usr/bin/install -m 0444 "$wheel_source/$filename" "$wheel_staging/$filename"
  done < <(LOCK_PATH="$lock_path" /usr/bin/python3 -I -S -B -c '
import json, os
with open(os.environ["LOCK_PATH"], "r", encoding="utf-8") as stream:
    lock = json.load(stream)
for row in lock["distributions"]:
    print(row["filename"])
')
else
  /usr/bin/python3 -m pip download --disable-pip-version-check \
    --no-deps --only-binary=:all: --dest "$wheel_staging" \
    neo4j==5.28.1 pytz==2025.2
fi

LOCK_PATH="$lock_path" WHEEL_DIR="$wheel_staging" /usr/bin/python3 -I -S -B -c '
import hashlib, json, os
from pathlib import Path
with open(os.environ["LOCK_PATH"], "r", encoding="utf-8") as stream:
    lock = json.load(stream)
root = Path(os.environ["WHEEL_DIR"])
expected = {row["filename"]: row["sha256"] for row in lock["distributions"]}
observed = {path.name for path in root.iterdir() if path.is_file()}
if observed != set(expected):
    raise SystemExit("formal wheel closure mismatch")
for filename, digest in expected.items():
    if hashlib.sha256((root / filename).read_bytes()).hexdigest() != digest:
        raise SystemExit("formal wheel sha256 mismatch")
'

existing_group=$(/usr/bin/getent group "$runner_gid" || true)
existing_group=${existing_group%%:*}
if [[ -n "$existing_group" && "$existing_group" != "$runner_name" ]]; then
  echo "formal runner GID is owned by another group" >&2
  exit 2
fi
if [[ -z "$existing_group" ]]; then
  /usr/sbin/groupadd --system --gid "$runner_gid" "$runner_name"
fi
existing_user=$(/usr/bin/getent passwd "$runner_uid" || true)
existing_user=${existing_user%%:*}
if [[ -n "$existing_user" && "$existing_user" != "$runner_name" ]]; then
  echo "formal runner UID is owned by another account" >&2
  exit 2
fi
if [[ -z "$existing_user" ]]; then
  /usr/sbin/useradd --system --uid "$runner_uid" --gid "$runner_gid" \
    --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin \
    "$runner_name"
fi

/usr/bin/install -d -o root -g root -m 0755 "$controller_dir"
/usr/bin/install -d -o root -g root -m 0755 "$runtime_root"
/usr/bin/install -d -o root -g root -m 0755 "$wheel_dir"
/usr/bin/install -d -o root -g root -m 0700 "$bootstrap_root"
/usr/bin/install -d -o root -g "$runner_gid" -m 0750 "$runs_root"

generation=${staging##*/}
controller_new="$controller_dir/.txnmem_formal_controller.$approved_commit.$generation.new"
approval_new="$controller_dir/.approved_source_manifest.$approved_commit.$generation.new"
/usr/bin/install -o root -g root -m 0555 \
  "$staging/txnmem_formal_controller.py" "$controller_new"
/usr/bin/install -o root -g root -m 0444 \
  "$staging/approved_source_manifest.json" "$approval_new"
# Each rename is atomic; any interruption between them leaves a hash mismatch
# that makes the controller fail closed rather than trusting a mixed generation.
/usr/bin/mv -f "$approval_new" "$approval_target"
/usr/bin/mv -f "$controller_new" "$controller_target"

/usr/bin/chmod 0755 "$wheel_dir"
/usr/bin/find "$wheel_dir" -mindepth 1 -delete
while IFS= read -r wheel; do
  filename=${wheel##*/}
  /usr/bin/install -o root -g root -m 0444 "$wheel" "$wheel_dir/$filename"
done < <(/usr/bin/find "$wheel_staging" -mindepth 1 -maxdepth 1 -type f -print | /usr/bin/sort)
/usr/bin/chmod 0555 "$wheel_dir"

if ! /usr/sbin/nft -j list tables >/dev/null 2>&1; then
  echo "nftables validation is unavailable" >&2
  exit 2
fi
/usr/bin/docker version >/dev/null
/usr/bin/git --version >/dev/null

echo "formal provenance runtime installed for approved commit $approved_commit"
