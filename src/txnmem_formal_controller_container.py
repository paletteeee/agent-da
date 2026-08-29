"""Fail-closed lifecycle commands for the formal controller container."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from typing import Callable, Sequence


DOCKER = "/usr/bin/docker"
REPOSITORY_MOUNT = "/workspace/txnmem"
WHEEL_MOUNT = "/opt/txnmem-formal-wheel-source"
STATE_MOUNT = "/var/lib/txnmem-formal"
SOCKET_MOUNT = "/var/run/docker.sock"
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_IMAGE = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?$"
)
_FORBIDDEN_MOUNT_ROOTS = frozenset({Path("/"), Path("/etc"), Path("/opt"), Path("/var/lib/docker")})


class ControllerContainerError(RuntimeError):
    """The controller container boundary is invalid or could not be created."""


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


def build_create_argv(
    repository: Path,
    wheel_source: Path,
    container_name: str,
    volume_name: str,
    image: str,
) -> list[str]:
    """Return the sole permitted argv for a long-lived controller."""
    repository = _require_directory(repository, "repository")
    wheel_source = _require_directory(wheel_source, "wheel source")
    container_name = _require_name(container_name, "container name")
    volume_name = _require_name(volume_name, "volume name")
    image = _require_image(image)
    return [
        DOCKER, "create", "--name", container_name, "--user", "0:0", "--network", "host",
        "--pid", "host", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--cap-add",
        "SYS_PTRACE", "--security-opt", "no-new-privileges=true", "--mount",
        f"type=bind,src={repository},dst={REPOSITORY_MOUNT},readonly", "--mount",
        f"type=bind,src={wheel_source},dst={WHEEL_MOUNT},readonly", "--mount",
        f"type=bind,src={SOCKET_MOUNT},dst={SOCKET_MOUNT}", "--mount",
        f"type=volume,src={volume_name},dst={STATE_MOUNT}", image, "/bin/sh", "-c",
        "exec sleep infinity",
    ]


def create_controller(
    repository: Path, wheel_source: Path, container_name: str, volume_name: str, image: str,
    *, run: Runner | None = None,
) -> None:
    """Create and start one fresh controller, removing partial state on failure."""
    runner = _run if run is None else run
    argv = build_create_argv(repository, wheel_source, container_name, volume_name, image)
    _require_absent(runner, [DOCKER, "container", "inspect", container_name], "controller container")
    _require_absent(runner, [DOCKER, "volume", "inspect", volume_name], "controller state volume")
    created = False
    try:
        created = True
        if _result(runner, argv, "create").returncode != 0:
            raise ControllerContainerError("Docker create failed")
        if _result(runner, [DOCKER, "start", container_name], "start").returncode != 0:
            raise ControllerContainerError("Docker start failed")
    except ControllerContainerError:
        if created:
            _result(runner, [DOCKER, "rm", "-f", container_name], "partial cleanup")
            _result(runner, [DOCKER, "volume", "rm", volume_name], "partial cleanup")
        raise


def install_controller(container_name: str, approved_commit: str, *, run: Runner | None = None) -> None:
    """Run the one approved installer against the read-only repository mount."""
    container_name = _require_name(container_name, "container name")
    if not isinstance(approved_commit, str) or not _COMMIT.fullmatch(approved_commit):
        raise ControllerContainerError("approved commit is invalid")
    runner = _run if run is None else run
    argv = [
        DOCKER, "exec", "--user", "0:0", "--env", f"TXNMEM_FORMAL_WHEEL_SOURCE={WHEEL_MOUNT}",
        container_name, "/bin/bash", f"{REPOSITORY_MOUNT}/scripts/install_formal_provenance_runtime.sh",
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
    values = parser.parse_args(argv)
    if values.action == "build":
        image = _require_image(values.image)
        root = Path(__file__).resolve().parents[1]
        result = _result(_run, [DOCKER, "build", "--tag", image, "--file", str(root / "infra/formal_controller/Dockerfile"), str(root / "infra/formal_controller")], "build")
        if result.returncode != 0:
            raise ControllerContainerError("Docker build failed")
    elif values.action == "create":
        if values.repository is None or values.wheel_source is None:
            raise ControllerContainerError("create requires repository and wheel source")
        create_controller(values.repository, values.wheel_source, values.container_name, values.volume_name, values.image)
    else:
        if values.approved_commit is None:
            raise ControllerContainerError("install requires approved commit")
        install_controller(values.container_name, values.approved_commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
