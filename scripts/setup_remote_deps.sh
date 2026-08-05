#!/usr/bin/env bash
# TxnMem native benchmark dependencies: install and verify on the GPU host.
#
# Usage:
#   bash scripts/setup_remote_deps.sh [--root /data/txnmem]
#
# Installs into a Python 3.11+ venv, unpacks AppWorld data and apps source,
# and prints a status summary.  Model serving (vLLM) is started separately
# with scripts/serve_model.sh.
set -euo pipefail

ROOT="${ROOT:-/data/txnmem}"
BENCH_ROOT="$ROOT/external_data/deps"
VENV="$ROOT/.venv"
PYTHON_BIN="${PYTHON_BIN:-/data/venvs/locomo-agent/bin/python}"

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3 || command -v python || true)"
fi
if [ -z "$PYTHON_BIN" ] || [ ! -x "$PYTHON_BIN" ]; then
  echo "error: no usable Python interpreter found; set PYTHON_BIN" >&2
  exit 2
fi

mkdir -p "$ROOT"

echo "==> creating venv at $VENV"
if [ ! -x "$VENV/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$VENV"
fi
source "$VENV/bin/activate"

echo "==> installing txnmem code"
pip install -e "$ROOT" 2>/dev/null || true
if [ -f "$ROOT/requirements-remote.txt" ]; then
  python -m pip install -r "$ROOT/requirements-remote.txt" --quiet
else
  python -m pip install cryptography --quiet
fi

echo "==> installing tau-bench (official)"
if [ ! -d "$BENCH_ROOT/tau-bench" ]; then
  git clone --depth 1 https://github.com/sierra-research/tau-bench.git "$BENCH_ROOT/tau-bench"
fi
pip install -e "$BENCH_ROOT/tau-bench" --quiet

echo "==> installing appworld (official repo + apps source)"
if [ ! -d "$BENCH_ROOT/appworld" ]; then
  git clone --depth 1 https://github.com/StonyBrookNLP/appworld.git "$BENCH_ROOT/appworld"
fi
# Apps source ships as an encrypted bundle; unpack it into the repo.
if [ ! -d "$BENCH_ROOT/appworld/src/appworld/apps/venmo" ]; then
  echo "    fetching encrypted apps.bundle from Git LFS"
  curl -sL "https://media.githubusercontent.com/media/StonyBrookNLP/appworld/main/src/appworld/.source/apps.bundle" \
    -o /tmp/apps.bundle
  curl -sL "https://media.githubusercontent.com/media/StonyBrookNLP/appworld/main/src/appworld/.source/tests.bundle" \
    -o /tmp/tests.bundle
  python - "$BENCH_ROOT/appworld/src" "$BENCH_ROOT/appworld/src/appworld" "$BENCH_ROOT/appworld/tests" <<'PYEOF'
import io, os, sys, zipfile

appworld_src, appworld_dest, tests_dest = sys.argv[1:]
sys.path.insert(0, appworld_src)
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

PASSWORD = "WEquKLy##9M@qu"
SALT = b"Nvx#rYcYQ2%btf"

def key(pw, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return kdf.derive(pw.encode())

for bundle, dest in [
    ("/tmp/apps.bundle", appworld_dest),
    ("/tmp/tests.bundle", tests_dest),
]:
    data = open(bundle, "rb").read()
    iv, enc = data[:16], data[16:]
    cipher = Cipher(algorithms.AES(key(PASSWORD, SALT)), modes.CFB(iv), backend=default_backend())
    dec = cipher.decryptor()
    zf = zipfile.ZipFile(io.BytesIO(dec.update(enc) + dec.finalize()))
    os.makedirs(dest, exist_ok=True)
    zf.extractall(dest)
    print(f"    unpacked {bundle} -> {len(zf.namelist())} files")
PYEOF
fi
pip install -e "$BENCH_ROOT/appworld" --quiet

echo "==> downloading + unpacking AppWorld data bundle (v0.2.0)"
if [ ! -d "$BENCH_ROOT/appworld-data/data/tasks" ]; then
  mkdir -p "$BENCH_ROOT/appworld-data"
  DATA_BUNDLE="$BENCH_ROOT/appworld-data/data-0.2.0.bundle"
  if [ ! -s "$DATA_BUNDLE" ]; then
    curl --fail --silent --show-error --location --retry 2 --retry-delay 5 --max-time 900 \
      "https://s3.us-west-2.amazonaws.com/appworld.dev/data-0.2.0.bundle" \
      -o "$DATA_BUNDLE.tmp"
    mv "$DATA_BUNDLE.tmp" "$DATA_BUNDLE"
  fi
  python - "$DATA_BUNDLE" "$BENCH_ROOT/appworld-data" "$BENCH_ROOT/appworld/src" <<'PYEOF'
import io, os, shutil, sys, zipfile
from pathlib import Path

PASSWORD = "WEquKLy##9M@qu"
SALT = b"Nvx#rYcYQ2%btf"
bundle, dest, appworld_src = sys.argv[1:]

sys.path.insert(0, appworld_src)
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

def key(pw, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return kdf.derive(pw.encode())

data = open(bundle, "rb").read()
iv, enc = data[:16], data[16:]
cipher = Cipher(algorithms.AES(key(PASSWORD, SALT)), modes.CFB(iv), backend=default_backend())
dec = cipher.decryptor()
zf = zipfile.ZipFile(io.BytesIO(dec.update(enc) + dec.finalize()))
inner = os.path.join(dest, "data")
os.makedirs(inner, exist_ok=True)
zf.extractall(inner)
# bundle entries carry a leading data/ prefix
if os.path.isdir(os.path.join(inner, "data")):
    nested = os.path.join(inner, "data")
    for child in Path(nested).iterdir():
        shutil.move(str(child), str(inner))
    Path(nested).rmdir()
print(f"    unpacked data bundle -> {len(zf.namelist())} files")
PYEOF
fi

echo "==> verifying imports"
python - <<'PYEOF'
from appworld.api_docs import prepare_api_docs
from tau_bench.envs.airline.env import MockAirlineDomainEnv
print("tau-bench env OK")
env = MockAirlineDomainEnv(user_strategy="human", task_index=0)
print(f"tau-bench tools: {len(env.tools_info)}")
from appworld.apps import get_all_apps
apps = [a for a in get_all_apps() if a != "admin"]
print(f"appworld apps: {len(apps)}")
total = sum(len(prepare_api_docs(a, include_private_apis=False, format="function_calling")) for a in apps)
print(f"appworld APIs: {total}")
PYEOF

echo "==> setup_remote_deps done"
