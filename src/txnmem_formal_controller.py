"""Root-protected bootstrap for TxnMem formal evidence commands.

This file is installed outside the repository.  It imports no project module
until it has verified itself against HEAD and exported committed Python blobs
to a private, detached tree.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


CONTROLLER_INSTALL_PATH = Path(
    "/opt/txnmem-formal-controller/txnmem_formal_controller.py"
)
APPROVAL_MANIFEST_PATH = Path(
    "/opt/txnmem-formal-controller/approved_source_manifest.json"
)
BOOTSTRAP_ROOT = Path("/var/lib/txnmem-formal/bootstrap")
GIT_EXECUTABLE = Path("/usr/bin/git")
_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_APPROVAL_SCHEMA = "txnmem-formal-approved-source-v1"
_CONTEXT_SCHEMA = "txnmem-formal-controller-context-v1"
_FORMAL_AUXILIARY_PATHS = (
    "configs/provenance_performance_matrix.json",
    "configs/provenance_runtime_lock.json",
    "infra/real_backend/docker-compose.yml",
    "scripts/install_formal_provenance_runtime.sh",
    "scripts/run_cross_host_provenance_performance.sh",
    "scripts/run_provenance_performance.sh",
)
_REQUIRED_APPROVED_PATHS = frozenset(
    {
        *_FORMAL_AUXILIARY_PATHS,
        "src/txnmem_formal_controller.py",
        "src/txnmem_provenance_execution_collector.py",
        "src/txnmem_provenance_runner.py",
    }
)


class FormalControllerError(RuntimeError):
    """The independently installed controller boundary is unavailable."""


@dataclass(frozen=True)
class _BootstrapExport:
    path: Path
    device: int
    inode: int
    parent_device: int
    parent_inode: int


@dataclass(frozen=True)
class _ApprovedSource:
    commit: str
    files: tuple[tuple[str, str], ...]
    manifest: dict[str, Any]
    manifest_sha256: str


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FormalControllerError("formal approval manifest is not canonical") from exc


def _normalize_approved_source(value: Any) -> _ApprovedSource:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema", "source_commit", "files"}
        or value.get("schema") != _APPROVAL_SCHEMA
        or not isinstance(value.get("source_commit"), str)
        or not _COMMIT.fullmatch(str(value["source_commit"]))
        or not isinstance(value.get("files"), list)
    ):
        raise FormalControllerError("formal approval manifest schema is invalid")
    rows: list[tuple[str, str]] = []
    normalized_rows: list[dict[str, str]] = []
    for row in value["files"]:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"path", "blob_sha256"}
            or not isinstance(row.get("path"), str)
            or not isinstance(row.get("blob_sha256"), str)
            or not _SHA256.fullmatch(str(row["blob_sha256"]))
        ):
            raise FormalControllerError("formal approval manifest row is invalid")
        relative = str(row["path"])
        path = Path(relative)
        if (
            path.is_absolute()
            or path.as_posix() != relative
            or any(part in {"", ".", ".."} for part in path.parts)
            or not (
                (relative.startswith("src/") and relative.endswith(".py"))
                or relative in _FORMAL_AUXILIARY_PATHS
            )
        ):
            raise FormalControllerError("formal approval path is unsafe")
        rows.append((relative, str(row["blob_sha256"])))
        normalized_rows.append(
            {"path": relative, "blob_sha256": str(row["blob_sha256"])}
        )
    if (
        not rows
        or rows != sorted(set(rows))
        or not _REQUIRED_APPROVED_PATHS.issubset({path for path, _hash in rows})
    ):
        raise FormalControllerError("formal approval source closure is incomplete")
    manifest = {
        "schema": _APPROVAL_SCHEMA,
        "source_commit": str(value["source_commit"]),
        "files": normalized_rows,
    }
    return _ApprovedSource(
        commit=str(value["source_commit"]),
        files=tuple(rows),
        manifest=manifest,
        manifest_sha256=_sha256(_canonical_json_bytes(manifest)),
    )


def _load_approved_source() -> _ApprovedSource:
    path = _require_protected_file(APPROVAL_MANIFEST_PATH)

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        document: dict[str, Any] = {}
        for key, value in pairs:
            if key in document:
                raise FormalControllerError(
                    "formal approval manifest contains duplicate keys"
                )
            document[key] = value
        return document

    try:
        raw = path.read_bytes()
        document = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                FormalControllerError(
                    f"formal approval manifest contains {value}"
                )
            ),
        )
    except FormalControllerError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalControllerError("formal approval manifest is unreadable") from exc
    approved = _normalize_approved_source(document)
    if raw != _canonical_json_bytes(approved.manifest) + b"\n":
        raise FormalControllerError("formal approval manifest bytes are not canonical")
    return approved


def _require_protected_file(path: Path, *, executable: bool = False) -> Path:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise FormalControllerError("formal controller file is unavailable")
    resolved = candidate.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise FormalControllerError("formal controller file is not root protected")
    parent = resolved.parent
    while True:
        parent_metadata = parent.stat()
        if (
            parent_metadata.st_uid != 0
            or stat.S_IMODE(parent_metadata.st_mode) & 0o022
        ):
            raise FormalControllerError(
                "formal controller file parent is not root protected"
            )
        if parent == parent.parent:
            break
        parent = parent.parent
    if executable and not os.access(resolved, os.X_OK):
        raise FormalControllerError("formal controller executable is unavailable")
    return resolved


def _git(project_root: Path, *arguments: str, text: bool = False):
    git = _require_protected_file(GIT_EXECUTABLE, executable=True)
    try:
        return subprocess.run(
            [
                str(git),
                "-c",
                f"safe.directory={project_root}",
                *arguments,
            ],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=text,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise FormalControllerError("formal controller Git operation failed") from exc


def _verify_installed_controller(project_root: Path) -> _ApprovedSource:
    if Path(__file__).resolve() != CONTROLLER_INSTALL_PATH:
        raise FormalControllerError("formal controller is not running from its install path")
    installed = _require_protected_file(CONTROLLER_INSTALL_PATH)
    approved = _load_approved_source()
    commit = str(_git(project_root, "rev-parse", "HEAD", text=True)).strip()
    if commit != approved.commit:
        raise FormalControllerError("repository HEAD is not the approved source commit")
    committed = bytes(
        _git(
            project_root,
            "show",
            f"{approved.commit}:src/txnmem_formal_controller.py",
        )
    )
    approved_hash = dict(approved.files)["src/txnmem_formal_controller.py"]
    if (
        _sha256(committed) != approved_hash
        or _sha256(installed.read_bytes()) != approved_hash
    ):
        raise FormalControllerError("installed controller differs from committed bytes")
    return approved


def _create_committed_export(
    project_root: Path,
    approved: _ApprovedSource,
    *,
    controller_uid: int = 0,
) -> _BootstrapExport:
    if type(controller_uid) is not int or controller_uid < 0:
        raise FormalControllerError("formal controller uid is invalid")
    BOOTSTRAP_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    if BOOTSTRAP_ROOT.is_symlink():
        raise FormalControllerError("formal bootstrap root is linked")
    root_stat = BOOTSTRAP_ROOT.stat()
    if (
        root_stat.st_uid != controller_uid
        or stat.S_IMODE(root_stat.st_mode) != 0o700
    ):
        raise FormalControllerError("formal bootstrap root is not private")
    export = Path(tempfile.mkdtemp(prefix="source-", dir=BOOTSTRAP_ROOT))
    export_metadata = export.stat()
    identity = _BootstrapExport(
        path=export,
        device=int(export_metadata.st_dev),
        inode=int(export_metadata.st_ino),
        parent_device=int(root_stat.st_dev),
        parent_inode=int(root_stat.st_ino),
    )
    try:
        directories = {export}
        for relative, expected_hash in approved.files:
            target = export / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            directories.update(
                directory
                for directory in target.parents
                if directory == export or export in directory.parents
            )
            payload = bytes(
                _git(project_root, "show", f"{approved.commit}:{relative}")
            )
            if _sha256(payload) != expected_hash:
                raise FormalControllerError("formal committed source blob drifted")
            descriptor = os.open(
                target,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                0o400,
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        for directory in sorted(
            directories, key=lambda value: len(value.parts), reverse=True
        ):
            directory.chmod(0o500)
        current_commit = str(
            _git(project_root, "rev-parse", "HEAD", text=True)
        ).strip()
        if current_commit != approved.commit:
            raise FormalControllerError(
                "repository HEAD changed during formal source export"
            )
        return identity
    except BaseException:
        _remove_export(identity)
        raise


def _remove_export_tree(directory_descriptor: int) -> None:
    os.fchmod(directory_descriptor, 0o700)
    try:
        names = sorted(os.listdir(directory_descriptor))
    except OSError as exc:
        raise FormalControllerError("cannot enumerate formal bootstrap export") from exc
    for name in names:
        if not isinstance(name, str) or name in {"", ".", ".."} or "/" in name:
            raise FormalControllerError("formal bootstrap export entry is unsafe")
        try:
            metadata = os.stat(
                name, dir_fd=directory_descriptor, follow_symlinks=False
            )
        except OSError as exc:
            raise FormalControllerError("cannot inspect formal bootstrap export") from exc
        if stat.S_ISDIR(metadata.st_mode):
            child_descriptor = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_descriptor,
            )
            try:
                _remove_export_tree(child_descriptor)
            finally:
                os.close(child_descriptor)
            os.rmdir(name, dir_fd=directory_descriptor)
        elif stat.S_ISREG(metadata.st_mode):
            os.unlink(name, dir_fd=directory_descriptor)
        else:
            raise FormalControllerError(
                "formal bootstrap export contains a link or special file"
            )


def _remove_export(export: _BootstrapExport) -> None:
    if not isinstance(export, _BootstrapExport):
        raise FormalControllerError("formal bootstrap cleanup identity is invalid")
    target = export.path.expanduser().absolute()
    root = BOOTSTRAP_ROOT.expanduser().absolute()
    if target.parent != root or not target.name.startswith("source-"):
        raise FormalControllerError("formal bootstrap cleanup target is unsafe")
    root_descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        root_metadata = os.fstat(root_descriptor)
        if (
            int(root_metadata.st_dev) != export.parent_device
            or int(root_metadata.st_ino) != export.parent_inode
        ):
            raise FormalControllerError("formal bootstrap parent identity changed")
        try:
            export_descriptor = os.open(
                target.name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=root_descriptor,
            )
        except FileNotFoundError:
            return
        try:
            metadata = os.fstat(export_descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or int(metadata.st_dev) != export.device
                or int(metadata.st_ino) != export.inode
            ):
                raise FormalControllerError("formal bootstrap export identity changed")
            _remove_export_tree(export_descriptor)
        finally:
            os.close(export_descriptor)
        os.rmdir(target.name, dir_fd=root_descriptor)
    finally:
        os.close(root_descriptor)


def _dispatch(
    action: str,
    arguments: Sequence[str],
    export: Path,
    approved: _ApprovedSource,
) -> int:
    module_name = {
        "measure": "txnmem_provenance_execution_collector",
        "material": "txnmem_experiment",
        "attest": "txnmem_topology_attestation",
        "promote": "txnmem_experiment",
    }.get(action)
    if module_name is None:
        raise FormalControllerError("formal controller action is unsupported")
    forwarded = list(arguments)
    if action == "material":
        forwarded.insert(0, "provenance-candidate-material")
    elif action == "promote":
        forwarded.insert(0, "provenance-promote")
    source_directory = export / "src"
    if module_name in sys.modules:
        raise FormalControllerError("formal controller target was pre-imported")
    original_modules = set(sys.modules)
    sys.path.insert(0, str(source_directory))
    try:
        module = importlib.import_module(module_name)
        entry = getattr(module, "main", None)
        if not callable(entry):
            raise FormalControllerError("formal controller target has no entry point")
        if action == "measure":
            result = entry(
                forwarded,
                _controller_context={
                    "schema": _CONTEXT_SCHEMA,
                    "source_commit": approved.commit,
                    "source_manifest": {
                        "schema": "txnmem-provenance-source-manifest-v1",
                        "source_commit": approved.commit,
                        "files": [
                            {"path": path, "blob_sha256": digest}
                            for path, digest in approved.files
                        ],
                    },
                    "approval_manifest_sha256": approved.manifest_sha256,
                },
            )
        else:
            result = entry(forwarded)
    finally:
        for name, module in list(sys.modules.items()):
            if name in original_modules:
                continue
            module_file = getattr(module, "__file__", None)
            if module_file is not None:
                try:
                    Path(str(module_file)).resolve().relative_to(export)
                except (OSError, ValueError):
                    continue
                sys.modules.pop(name, None)
        if sys.path and sys.path[0] == str(source_directory):
            sys.path.pop(0)
    if type(result) is not int:
        raise FormalControllerError("formal controller target returned an invalid status")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) < 3 or arguments[0] != "--project-root":
        print("formal controller blocked: invalid invocation", file=sys.stderr)
        return 64
    project_root = Path(arguments[1]).expanduser().absolute()
    action = arguments[2]
    forwarded = arguments[3:]
    try:
        if os.geteuid() != 0:
            raise FormalControllerError("formal controller requires root")
        project_root = project_root.resolve(strict=True)
        approved = _verify_installed_controller(project_root)
        export = _create_committed_export(project_root, approved)
        try:
            result = _dispatch(action, forwarded, export.path, approved)
        except BaseException as primary:
            try:
                _remove_export(export)
            except BaseException as cleanup:
                try:
                    primary.add_note(
                        f"formal bootstrap cleanup also failed: {type(cleanup).__name__}"
                    )
                except AttributeError:
                    pass
            raise
        else:
            _remove_export(export)
            return result
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"formal controller blocked: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
