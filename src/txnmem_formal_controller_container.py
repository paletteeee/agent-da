"""Fail-closed lifecycle commands for the formal controller container."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Callable, Sequence


DOCKER = "/usr/bin/docker"
GIT = "/usr/bin/git"
REPOSITORY_MOUNT = "/workspace/txnmem"
WHEEL_MOUNT = "/opt/txnmem-formal-wheel-source"
STATE_MOUNT = "/var/lib/txnmem-formal"
SOCKET_MOUNT = "/var/run/docker.sock"
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CONTAINER_ID = re.compile(r"^[0-9a-f]{64}$")
_WHEEL_FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}\.whl$")
_IMAGE = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?$"
)
_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_MOUNT_ROOTS = frozenset({Path("/"), Path("/etc"), Path("/opt"), Path("/var/lib/docker")})
_LIFECYCLE_LABEL = "com.txnmem.formal.lifecycle"
_ROLE_LABEL = "com.txnmem.formal.role"
_IMAGE_LABEL = "com.txnmem.formal.image-id"
_CONTAINER_ROLE = "controller"
_STATE_ROLE = "controller-state"


class ControllerContainerError(RuntimeError):
    """The controller container boundary is invalid or could not be created."""


class ControllerContainerCleanupError(ControllerContainerError):
    """An owned lifecycle resource could not be safely removed."""


Runner = Callable[[list[str]], subprocess.CompletedProcess]


def _run(argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(argv, check=False, capture_output=True, text=True)


def _require_name(value: str, label: str) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise ControllerContainerError(f"{label} is unsafe")
    return value


def _require_image(value: str) -> str:
    if not isinstance(value, str) or not _IMAGE.fullmatch(value):
        raise ControllerContainerError("image reference is unsafe")
    return value


def _require_lifecycle_token(value: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ControllerContainerError("lifecycle token is invalid")
    return value


def _require_image_id(value: str) -> str:
    if not isinstance(value, str) or not _IMAGE_ID.fullmatch(value):
        raise ControllerContainerError("image ID is invalid")
    return value


def _require_directory(value: Path, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise ControllerContainerError(f"{label} must be a canonical directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ControllerContainerError(f"{label} must be a canonical directory") from exc
    if path != resolved or resolved in _FORBIDDEN_MOUNT_ROOTS:
        raise ControllerContainerError(f"{label} must be a canonical directory")
    return resolved


def _git(repository: Path, *arguments: str) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            [GIT, "-C", str(repository), *arguments],
            check=False,
            capture_output=True,
            text=True,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        )
    except OSError as exc:
        raise ControllerContainerError("repository identity cannot be verified") from exc


def _require_repository(value: Path) -> Path:
    repository = _require_directory(value, "repository")
    if len(repository.parts) < 4:
        raise ControllerContainerError("repository path is too broad")
    top_level = _git(repository, "rev-parse", "--show-toplevel")
    if top_level.returncode != 0 or Path(top_level.stdout.strip()) != repository:
        raise ControllerContainerError("repository is not the exact Git top-level")
    clean = _git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    if clean.returncode != 0 or clean.stdout:
        raise ControllerContainerError("repository is not clean")
    lock_relative = "configs/provenance_runtime_lock.json"
    tracked = _git(repository, "ls-files", "--error-unmatch", "--", lock_relative)
    if tracked.returncode != 0 or tracked.stdout.strip() != lock_relative:
        raise ControllerContainerError("runtime lock is not tracked")
    return repository


def _load_locked_wheels(repository: Path) -> dict[str, str]:
    lock_path = repository / "configs/provenance_runtime_lock.json"
    if lock_path.is_symlink() or not lock_path.is_file():
        raise ControllerContainerError("runtime lock is not a regular file")
    try:
        document = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ControllerContainerError("runtime lock is invalid") from exc
    if (
        not isinstance(document, dict)
        or document.get("schema") != "txnmem-provenance-runtime-lock-v1"
        or not isinstance(document.get("distributions"), list)
        or not document["distributions"]
    ):
        raise ControllerContainerError("runtime lock is invalid")
    expected: dict[str, str] = {}
    for row in document["distributions"]:
        if not isinstance(row, dict):
            raise ControllerContainerError("runtime lock distribution is invalid")
        filename = row.get("filename")
        digest = row.get("sha256")
        if (
            not isinstance(filename, str)
            or not _WHEEL_FILENAME.fullmatch(filename)
            or not isinstance(digest, str)
            or not _SHA256.fullmatch(digest)
            or filename in expected
        ):
            raise ControllerContainerError("runtime lock distribution is invalid")
        expected[filename] = digest
    return expected


def _require_wheel_source(value: Path, repository: Path) -> Path:
    wheel_source = _require_directory(value, "wheel source")
    if len(wheel_source.parts) < 4:
        raise ControllerContainerError("wheel source path is too broad")
    expected = _load_locked_wheels(repository)
    try:
        entries = list(wheel_source.iterdir())
    except OSError as exc:
        raise ControllerContainerError("wheel source cannot be enumerated") from exc
    observed_names = {entry.name for entry in entries}
    if observed_names != set(expected) or len(entries) != len(expected):
        raise ControllerContainerError("wheel source closure does not match runtime lock")
    for entry in entries:
        if entry.is_symlink() or not entry.is_file():
            raise ControllerContainerError("wheel source entry is not a regular file")
        try:
            digest = hashlib.sha256(entry.read_bytes()).hexdigest()
        except OSError as exc:
            raise ControllerContainerError("wheel source entry cannot be read") from exc
        if digest != expected[entry.name]:
            raise ControllerContainerError("wheel source digest does not match runtime lock")
    return wheel_source


def _result(run: Runner, argv: list[str], operation: str) -> subprocess.CompletedProcess:
    try:
        result = run(argv)
    except OSError as exc:
        raise ControllerContainerError(f"Docker {operation} could not be invoked") from exc
    if not isinstance(result, subprocess.CompletedProcess):
        raise ControllerContainerError(f"Docker {operation} returned an invalid result")
    return result


def _require_absent(run: Runner, argv: list[str], label: str) -> None:
    result = _result(run, argv, "inspect")
    if result.returncode == 0:
        raise ControllerContainerError(f"{label} already exists")
    if result.returncode != 1:
        raise ControllerContainerError(f"Docker inspect failed for {label}")


def _inspect_document(run: Runner, argv: list[str], label: str) -> dict:
    result = _result(run, argv, "inspect")
    if result.returncode != 0:
        raise ControllerContainerError(f"{label} ownership cannot be inspected")
    try:
        document = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ControllerContainerError(f"{label} ownership is invalid") from exc
    if not isinstance(document, list) or len(document) != 1 or not isinstance(document[0], dict):
        raise ControllerContainerError(f"{label} ownership is invalid")
    return document[0]


def _resolve_image_id(run: Runner, image_reference: str) -> str:
    image_reference = _require_image(image_reference)
    document = _inspect_document(
        run,
        [DOCKER, "image", "inspect", image_reference],
        "controller image",
    )
    return _require_image_id(document.get("Id"))


def _volume_identity_matches(
    run: Runner, volume_name: str, lifecycle_token: str
) -> bool:
    try:
        document = _inspect_document(
            run, [DOCKER, "volume", "inspect", volume_name], "state volume"
        )
    except ControllerContainerError:
        return False
    return document.get("Name") == volume_name and document.get("Labels") == {
        _LIFECYCLE_LABEL: lifecycle_token,
        _ROLE_LABEL: _STATE_ROLE,
    }


def _container_identity_matches(
    run: Runner,
    container_id: str,
    container_name: str,
    lifecycle_token: str,
    image_id: str,
) -> bool:
    try:
        document = _inspect_document(
            run, [DOCKER, "container", "inspect", container_id], "controller container"
        )
    except ControllerContainerError:
        return False
    config = document.get("Config")
    labels = config.get("Labels") if isinstance(config, dict) else None
    if not isinstance(labels, dict):
        return False
    formal_labels = {
        key: value
        for key, value in labels.items()
        if isinstance(key, str) and key.startswith("com.txnmem.formal.")
    }
    return (
        document.get("Id") == container_id
        and document.get("Name") == f"/{container_name}"
        and document.get("Image") == image_id
        and isinstance(config, dict)
        and config.get("Image") == image_id
        and formal_labels
        == {
            _LIFECYCLE_LABEL: lifecycle_token,
            _ROLE_LABEL: _CONTAINER_ROLE,
            _IMAGE_LABEL: image_id,
        }
    )


def _cleanup_owned_resources(
    run: Runner,
    *,
    container_id: str | None,
    container_name: str,
    volume_owned: bool,
    volume_name: str,
    lifecycle_token: str,
    image_id: str,
) -> None:
    cleanup_failed = False
    if container_id is not None:
        if _container_identity_matches(
            run, container_id, container_name, lifecycle_token, image_id
        ):
            try:
                result = _result(
                    run,
                    [DOCKER, "rm", "-f", container_id],
                    "container cleanup",
                )
                cleanup_failed = cleanup_failed or result.returncode != 0
            except ControllerContainerError:
                cleanup_failed = True
        else:
            cleanup_failed = True
    if volume_owned:
        if _volume_identity_matches(run, volume_name, lifecycle_token):
            try:
                result = _result(
                    run,
                    [DOCKER, "volume", "rm", volume_name],
                    "volume cleanup",
                )
                cleanup_failed = cleanup_failed or result.returncode != 0
            except ControllerContainerError:
                cleanup_failed = True
        else:
            cleanup_failed = True
    if cleanup_failed:
        raise ControllerContainerCleanupError(
            "controller lifecycle cleanup failed"
        )


def _require_install_mounts(document: dict) -> tuple[Path, Path, str]:
    mounts = document.get("Mounts")
    if not isinstance(mounts, list) or len(mounts) != 4:
        raise ControllerContainerError("controller mount closure is invalid")
    by_destination = {}
    for mount in mounts:
        if (
            not isinstance(mount, dict)
            or not isinstance(mount.get("Destination"), str)
            or mount["Destination"] in by_destination
        ):
            raise ControllerContainerError("controller mount closure is invalid")
        by_destination[mount["Destination"]] = mount
    if set(by_destination) != {
        REPOSITORY_MOUNT,
        WHEEL_MOUNT,
        SOCKET_MOUNT,
        STATE_MOUNT,
    }:
        raise ControllerContainerError("controller mount closure is invalid")

    repository_mount = by_destination[REPOSITORY_MOUNT]
    wheel_mount = by_destination[WHEEL_MOUNT]
    socket_mount = by_destination[SOCKET_MOUNT]
    state_mount = by_destination[STATE_MOUNT]
    for mount in (repository_mount, wheel_mount):
        if (
            mount.get("Type") != "bind"
            or mount.get("Mode") != "ro"
            or mount.get("RW") is not False
            or not isinstance(mount.get("Source"), str)
        ):
            raise ControllerContainerError("controller read-only mount is invalid")
    if (
        socket_mount.get("Type") != "bind"
        or socket_mount.get("Source") != SOCKET_MOUNT
        or socket_mount.get("Mode") != ""
        or socket_mount.get("RW") is not True
    ):
        raise ControllerContainerError("controller Docker socket mount is invalid")
    volume_name = state_mount.get("Name")
    if (
        state_mount.get("Type") != "volume"
        or state_mount.get("Mode") != ""
        or state_mount.get("RW") is not True
        or not isinstance(volume_name, str)
    ):
        raise ControllerContainerError("controller state mount is invalid")
    volume_name = _require_name(volume_name, "volume name")
    repository = _require_repository(Path(repository_mount["Source"]))
    wheel_source = _require_wheel_source(Path(wheel_mount["Source"]), repository)
    return repository, wheel_source, volume_name


def _require_install_identity(
    run: Runner, document: dict, container_name: str, lifecycle_token: str
) -> str:
    container_id = document.get("Id")
    if not isinstance(container_id, str) or not _CONTAINER_ID.fullmatch(container_id):
        raise ControllerContainerError("controller container ID is invalid")
    if document.get("Name") != f"/{container_name}":
        raise ControllerContainerError("controller container name is invalid")
    observed_image_id = _require_image_id(document.get("Image"))
    state = document.get("State")
    config = document.get("Config")
    host_config = document.get("HostConfig")
    if not isinstance(state, dict) or state.get("Running") is not True:
        raise ControllerContainerError("controller container is not running")
    if not isinstance(config, dict) or config.get("User") != "0:0":
        raise ControllerContainerError("controller container user is invalid")
    if config.get("Image") != observed_image_id:
        raise ControllerContainerError("controller image identity is invalid")
    labels = config.get("Labels")
    if not isinstance(labels, dict):
        raise ControllerContainerError("controller lifecycle labels are invalid")
    formal_labels = {
        key: value
        for key, value in labels.items()
        if isinstance(key, str) and key.startswith("com.txnmem.formal.")
    }
    if formal_labels != {
        _LIFECYCLE_LABEL: lifecycle_token,
        _ROLE_LABEL: _CONTAINER_ROLE,
        _IMAGE_LABEL: observed_image_id,
    }:
        raise ControllerContainerError("controller lifecycle labels are invalid")
    if not isinstance(host_config, dict):
        raise ControllerContainerError("controller security identity is invalid")
    cap_add = host_config.get("CapAdd")
    if (
        host_config.get("NetworkMode") != "host"
        or host_config.get("PidMode") != "host"
        or host_config.get("CapDrop") != ["ALL"]
        or not isinstance(cap_add, list)
        or len(cap_add) != 2
        or not all(isinstance(value, str) for value in cap_add)
        or set(cap_add) != {"NET_ADMIN", "SYS_PTRACE"}
        or host_config.get("SecurityOpt") != ["no-new-privileges"]
        or host_config.get("Privileged") is not False
    ):
        raise ControllerContainerError("controller security identity is invalid")
    _repository, _wheels, volume_name = _require_install_mounts(document)
    if not _volume_identity_matches(run, volume_name, lifecycle_token):
        raise ControllerContainerError("controller state ownership is invalid")
    return container_id


def build_create_argv(
    repository: Path,
    wheel_source: Path,
    container_name: str,
    volume_name: str,
    image: str,
    lifecycle_token: str,
) -> list[str]:
    """Return the sole permitted argv for a long-lived controller."""
    repository = _require_repository(repository)
    wheel_source = _require_wheel_source(wheel_source, repository)
    container_name = _require_name(container_name, "container name")
    volume_name = _require_name(volume_name, "volume name")
    image = _require_image_id(image)
    lifecycle_token = _require_lifecycle_token(lifecycle_token)
    return [
        DOCKER, "create", "--name", container_name, "--user", "0:0", "--network", "host",
        "--pid", "host", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--cap-add",
        "SYS_PTRACE", "--security-opt", "no-new-privileges=true", "--label",
        f"{_LIFECYCLE_LABEL}={lifecycle_token}", "--label", f"{_ROLE_LABEL}={_CONTAINER_ROLE}",
        "--label", f"{_IMAGE_LABEL}={image}", "--mount",
        f"type=bind,src={repository},dst={REPOSITORY_MOUNT},readonly", "--mount",
        f"type=bind,src={wheel_source},dst={WHEEL_MOUNT},readonly", "--mount",
        f"type=bind,src={SOCKET_MOUNT},dst={SOCKET_MOUNT}", "--mount",
        f"type=volume,src={volume_name},dst={STATE_MOUNT}", image, "/bin/sh", "-c",
        "exec sleep infinity",
    ]


def create_controller(
    repository: Path, wheel_source: Path, container_name: str, volume_name: str, image: str,
    *, lifecycle_token: str, run: Runner | None = None,
) -> None:
    """Create and start one fresh controller, removing partial state on failure."""
    runner = _run if run is None else run
    lifecycle_token = _require_lifecycle_token(lifecycle_token)
    image = _require_image(image)
    repository = _require_repository(repository)
    wheel_source = _require_wheel_source(wheel_source, repository)
    container_name = _require_name(container_name, "container name")
    volume_name = _require_name(volume_name, "volume name")
    image_id = _resolve_image_id(runner, image)
    argv = build_create_argv(
        repository, wheel_source, container_name, volume_name, image_id, lifecycle_token
    )
    _require_absent(runner, [DOCKER, "container", "inspect", container_name], "controller container")
    _require_absent(runner, [DOCKER, "volume", "inspect", volume_name], "controller state volume")
    owned_volume = False
    owned_container_id: str | None = None
    try:
        volume_argv = [
            DOCKER,
            "volume",
            "create",
            "--label",
            f"{_LIFECYCLE_LABEL}={lifecycle_token}",
            "--label",
            f"{_ROLE_LABEL}={_STATE_ROLE}",
            volume_name,
        ]
        volume_result = _result(runner, volume_argv, "volume create")
        if volume_result.returncode != 0 or volume_result.stdout.strip() != volume_name:
            raise ControllerContainerError("Docker volume create failed")
        if not _volume_identity_matches(runner, volume_name, lifecycle_token):
            raise ControllerContainerError("state volume ownership proof failed")
        owned_volume = True
        create_result = _result(runner, argv, "create")
        container_id = create_result.stdout.strip()
        if create_result.returncode != 0 or not _CONTAINER_ID.fullmatch(container_id):
            raise ControllerContainerError("Docker create failed")
        if not _container_identity_matches(
            runner, container_id, container_name, lifecycle_token, image_id
        ):
            raise ControllerContainerError("controller container ownership proof failed")
        owned_container_id = container_id
        if _result(runner, [DOCKER, "start", container_id], "start").returncode != 0:
            raise ControllerContainerError("Docker start failed")
    except ControllerContainerError as lifecycle_error:
        try:
            _cleanup_owned_resources(
                runner,
                container_id=owned_container_id,
                container_name=container_name,
                volume_owned=owned_volume,
                volume_name=volume_name,
                lifecycle_token=lifecycle_token,
                image_id=image_id,
            )
        except ControllerContainerCleanupError as cleanup_error:
            raise cleanup_error from lifecycle_error
        raise


def install_controller(
    container_name: str,
    approved_commit: str,
    *,
    lifecycle_token: str,
    run: Runner | None = None,
) -> None:
    """Run the one approved installer against the read-only repository mount."""
    container_name = _require_name(container_name, "container name")
    if not isinstance(approved_commit, str) or not _COMMIT.fullmatch(approved_commit):
        raise ControllerContainerError("approved commit is invalid")
    lifecycle_token = _require_lifecycle_token(lifecycle_token)
    runner = _run if run is None else run
    document = _inspect_document(
        runner,
        [DOCKER, "container", "inspect", container_name],
        "controller container",
    )
    container_id = _require_install_identity(
        runner, document, container_name, lifecycle_token
    )
    argv = [
        DOCKER, "exec", "--user", "0:0", "--env", f"TXNMEM_FORMAL_WHEEL_SOURCE={WHEEL_MOUNT}",
        container_id, "/bin/bash", f"{REPOSITORY_MOUNT}/scripts/install_formal_provenance_runtime.sh",
        REPOSITORY_MOUNT, approved_commit,
    ]
    if _result(runner, argv, "install").returncode != 0:
        raise ControllerContainerError("Docker install failed")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("build", "create", "install"))
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--wheel-source", type=Path)
    parser.add_argument("--container-name", default="txnmem-formal-controller")
    parser.add_argument("--volume-name", default="txnmem-formal-state")
    parser.add_argument("--image", default="txnmem-formal-controller:approved")
    parser.add_argument("--approved-commit")
    parser.add_argument("--lifecycle-token")
    values = parser.parse_args(argv)
    if values.action == "build":
        image = _require_image(values.image)
        root = Path(__file__).resolve().parents[1]
        result = _result(_run, [DOCKER, "build", "--tag", image, "--file", str(root / "infra/formal_controller/Dockerfile"), str(root / "infra/formal_controller")], "build")
        if result.returncode != 0:
            raise ControllerContainerError("Docker build failed")
    elif values.action == "create":
        if (
            values.repository is None
            or values.wheel_source is None
            or values.lifecycle_token is None
        ):
            raise ControllerContainerError(
                "create requires repository, wheel source, and lifecycle token"
            )
        create_controller(
            values.repository,
            values.wheel_source,
            values.container_name,
            values.volume_name,
            values.image,
            lifecycle_token=values.lifecycle_token,
        )
    else:
        if values.approved_commit is None or values.lifecycle_token is None:
            raise ControllerContainerError(
                "install requires approved commit and lifecycle token"
            )
        install_controller(
            values.container_name,
            values.approved_commit,
            lifecycle_token=values.lifecycle_token,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
