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
SOCKET_MOUNT = "/var/run/docker.sock"
STATE_MOUNT = "/var/lib/txnmem-formal"
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
_REQUIRED_CAPABILITIES = (
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "KILL",
    "NET_ADMIN",
    "SETGID",
    "SETUID",
    "SYS_PTRACE",
)
_REQUIRED_MASKED_PATHS = frozenset(
    {
        "/proc/acpi",
        "/proc/asound",
        "/proc/interrupts",
        "/proc/kcore",
        "/proc/keys",
        "/proc/latency_stats",
        "/proc/sched_debug",
        "/proc/scsi",
        "/proc/timer_list",
        "/proc/timer_stats",
        "/sys/devices/virtual/powercap",
        "/sys/firmware",
    }
)
_REQUIRED_READONLY_PATHS = frozenset(
    {
        "/proc/bus",
        "/proc/fs",
        "/proc/irq",
        "/proc/sys",
        "/proc/sysrq-trigger",
    }
)
_DYNAMIC_MASKED_PATH = re.compile(
    r"^/sys/devices/system/cpu/cpu[0-9]+/thermal_throttle$"
)
_SELINUX_ENFORCE_PATH = Path("/sys/fs/selinux/enforce")
_DOCKER_ENV = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "HOME": "/nonexistent",
    "DOCKER_HOST": "unix:///var/run/docker.sock",
    "DOCKER_CONFIG": "/var/empty/txnmem-formal-docker-config",
}


class ControllerContainerError(RuntimeError):
    """The controller container boundary is invalid or could not be created."""


class ControllerContainerCleanupError(ControllerContainerError):
    """An owned lifecycle resource could not be safely removed."""


Runner = Callable[[list[str]], subprocess.CompletedProcess]


def _run(argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        env=dict(_DOCKER_ENV),
    )


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


def _inventory_names(run: Runner, kind: str, label: str) -> set[str]:
    if kind == "container":
        argv = [DOCKER, "container", "ls", "--all", "--format", "{{.Names}}"]
    else:
        raise ControllerContainerError("Docker inventory kind is invalid")
    result = _result(run, argv, "inspect")
    if result.returncode != 0 or not isinstance(result.stdout, str):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    names = result.stdout.splitlines()
    if (
        len(names) != len(set(names))
        or any(
            not name
            or len(name) > 255
            or any(ord(character) < 32 for character in name)
            for name in names
        )
    ):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    return set(names)


def _inventory_container_ids(run: Runner, label: str) -> set[str]:
    argv = [
        DOCKER,
        "container",
        "ls",
        "--all",
        "--no-trunc",
        "--format",
        "{{.ID}}",
    ]
    result = _result(run, argv, "inspect")
    if result.returncode != 0 or not isinstance(result.stdout, str):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    identifiers = result.stdout.splitlines()
    if (
        len(identifiers) != len(set(identifiers))
        or any(not _CONTAINER_ID.fullmatch(identifier) for identifier in identifiers)
    ):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    return set(identifiers)


def _inventory_volume_names(run: Runner, label: str) -> set[str]:
    argv = [DOCKER, "volume", "ls", "--quiet"]
    result = _result(run, argv, "inspect")
    if result.returncode != 0 or not isinstance(result.stdout, str):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    names = result.stdout.splitlines()
    if (
        len(names) != len(set(names))
        or any(
            not name
            or len(name) > 255
            or any(ord(character) < 32 for character in name)
            for name in names
        )
    ):
        raise ControllerContainerError(f"Docker inspect failed for {label}")
    return set(names)


def _require_absent(run: Runner, kind: str, name: str, label: str) -> None:
    if name in _inventory_names(run, kind, label):
        raise ControllerContainerError(f"{label} already exists")


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


def _try_inspect_document(run: Runner, argv: list[str]) -> dict | None:
    result = _result(run, argv, "inspect")
    if result.returncode != 0:
        return None
    try:
        document = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(document, list)
        or len(document) != 1
        or not isinstance(document[0], dict)
    ):
        return None
    return document[0]


def _resolve_image_id(run: Runner, image_reference: str) -> str:
    image_reference = _require_image(image_reference)
    document = _inspect_document(
        run,
        [DOCKER, "image", "inspect", image_reference],
        "controller image",
    )
    return _require_image_id(document.get("Id"))


def _container_identity_state(
    run: Runner,
    reference: str,
    container_name: str,
    lifecycle_token: str,
    image_id: str,
) -> tuple[str, str | None, str | None]:
    document = _try_inspect_document(
        run, [DOCKER, "container", "inspect", reference]
    )
    if document is None:
        return (
            (
                "ambiguous"
                if container_name
                in _inventory_names(run, "container", "controller container")
                else "absent"
            ),
            None,
            None,
        )
    config = document.get("Config")
    labels = config.get("Labels") if isinstance(config, dict) else None
    if not isinstance(labels, dict):
        return "foreign", None, None
    observed_id = document.get("Id")
    if not isinstance(observed_id, str) or not _CONTAINER_ID.fullmatch(observed_id):
        return "foreign", None, None
    try:
        volume_name = _realized_anonymous_state_volume_name(document)
        _require_requested_anonymous_state_volume(document)
    except ControllerContainerError:
        volume_name = None
    formal_labels = {
        key: value
        for key, value in labels.items()
        if isinstance(key, str) and key.startswith("com.txnmem.formal.")
    }
    owned = (
        (not _CONTAINER_ID.fullmatch(reference) or observed_id == reference)
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
        and volume_name is not None
    )
    return (
        ("owned", observed_id, volume_name)
        if owned
        else ("foreign", observed_id, None)
    )


def _cleanup_owned_resources(
    run: Runner,
    *,
    container_id: str | None,
    container_name: str,
    lifecycle_token: str,
    image_id: str,
) -> None:
    cleanup_failed = False
    if container_id is not None:
        try:
            state, observed_id, volume_name = _container_identity_state(
                run, container_id, container_name, lifecycle_token, image_id
            )
            if (
                state == "owned"
                and observed_id == container_id
                and volume_name is not None
            ):
                result = _result(
                    run,
                    [DOCKER, "rm", "-f", "-v", container_id],
                    "container cleanup",
                )
                if result.returncode != 0:
                    cleanup_failed = True
                else:
                    remaining_container_ids = _inventory_container_ids(
                        run, "controller container cleanup"
                    )
                    remaining_volume_names = _inventory_volume_names(
                        run, "controller anonymous volume cleanup"
                    )
                    if (
                        container_id in remaining_container_ids
                        or volume_name in remaining_volume_names
                    ):
                        cleanup_failed = True
            else:
                cleanup_failed = True
        except ControllerContainerError:
            cleanup_failed = True
    if cleanup_failed:
        raise ControllerContainerCleanupError(
            "controller lifecycle cleanup failed"
        )


def _require_safe_volume_request_options(mount: dict) -> None:
    if any(
        mount.get(option) is not None
        for option in (
            "BindOptions",
            "TmpfsOptions",
            "ImageOptions",
            "ClusterOptions",
        )
    ):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    volume_options = mount.get("VolumeOptions")
    if volume_options is None:
        return
    if not isinstance(volume_options, dict) or set(volume_options) - {
        "NoCopy",
        "Labels",
        "Subpath",
        "DriverConfig",
    }:
        raise ControllerContainerError("controller anonymous state mount is invalid")
    if (
        volume_options.get("NoCopy") not in (None, False)
        or volume_options.get("Labels") not in (None, {})
        or volume_options.get("Subpath") not in (None, "")
        or volume_options.get("DriverConfig") is not None
    ):
        raise ControllerContainerError("controller anonymous state mount is invalid")


def _require_requested_anonymous_state_volume(document: dict) -> None:
    host_config = document.get("HostConfig")
    host_mounts = host_config.get("Mounts") if isinstance(host_config, dict) else None
    if not isinstance(host_config, dict) or not isinstance(host_mounts, list):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    volume_driver = host_config.get("VolumeDriver", "")
    if not isinstance(volume_driver, str) or volume_driver:
        raise ControllerContainerError("controller anonymous state mount is invalid")
    if any(not isinstance(mount, dict) for mount in host_mounts):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    state_requests = [
        mount for mount in host_mounts if mount.get("Target") == STATE_MOUNT
    ]
    volume_requests = [
        mount for mount in host_mounts if mount.get("Type") == "volume"
    ]
    if len(state_requests) != 1 or volume_requests != state_requests:
        raise ControllerContainerError("controller anonymous state mount is invalid")
    state_request = state_requests[0]
    source = state_request.get("Source", "")
    read_only = state_request.get("ReadOnly", False)
    if (
        not isinstance(source, str)
        or source
        or type(read_only) is not bool
        or read_only is not False
        or state_request.get("Consistency") not in (None, "")
    ):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    _require_safe_volume_request_options(state_request)


def _realized_anonymous_state_volume_name(document: dict) -> str:
    mounts = document.get("Mounts")
    if not isinstance(mounts, list) or any(
        not isinstance(mount, dict) for mount in mounts
    ):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    state_mounts = [
        mount
        for mount in mounts
        if mount.get("Destination") == STATE_MOUNT
    ]
    volume_mounts = [mount for mount in mounts if mount.get("Type") == "volume"]
    if len(state_mounts) != 1 or volume_mounts != state_mounts:
        raise ControllerContainerError("controller anonymous state mount is invalid")
    state_mount = state_mounts[0]
    volume_name = state_mount.get("Name")
    if (
        state_mount.get("Type") != "volume"
        or not isinstance(volume_name, str)
        or not _SHA256.fullmatch(volume_name)
        or state_mount.get("Driver") != "local"
        or not isinstance(state_mount.get("Source"), str)
        or not state_mount["Source"]
        or state_mount.get("Mode") not in ("", "rw", "z")
        or state_mount.get("RW") is not True
    ):
        raise ControllerContainerError("controller anonymous state mount is invalid")
    return volume_name


def _require_install_mounts(document: dict) -> tuple[Path, Path]:
    mounts = document.get("Mounts")
    host_config = document.get("HostConfig")
    host_mounts = host_config.get("Mounts") if isinstance(host_config, dict) else None
    if (
        not isinstance(mounts, list)
        or len(mounts) != 4
        or not isinstance(host_config, dict)
        or not isinstance(host_mounts, list)
        or len(host_mounts) != 4
    ):
        raise ControllerContainerError("controller mount closure is invalid")
    _require_requested_anonymous_state_volume(document)
    volume_driver = host_config.get("VolumeDriver", "")
    if not isinstance(volume_driver, str) or volume_driver:
        raise ControllerContainerError("controller volume driver is invalid")
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

    by_target = {}
    for mount in host_mounts:
        if (
            not isinstance(mount, dict)
            or not isinstance(mount.get("Target"), str)
            or mount["Target"] in by_target
        ):
            raise ControllerContainerError("controller mount closure is invalid")
        by_target[mount["Target"]] = mount
    if set(by_target) != {
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
            or mount.get("Mode") not in ("", "ro")
            or mount.get("RW") is not False
            or not isinstance(mount.get("Source"), str)
            or mount.get("Propagation", "") not in ("", "rprivate")
        ):
            raise ControllerContainerError("controller read-only mount is invalid")
    if (
        socket_mount.get("Type") != "bind"
        or socket_mount.get("Source") != SOCKET_MOUNT
        or socket_mount.get("Mode") not in ("", "rw")
        or socket_mount.get("RW") is not True
        or socket_mount.get("Propagation", "") not in ("", "rprivate")
    ):
        raise ControllerContainerError("controller Docker socket mount is invalid")
    _realized_anonymous_state_volume_name(document)
    if state_mount.get("Propagation", "") != "":
        raise ControllerContainerError("controller anonymous state mount is invalid")

    expected_host_mounts = {
        REPOSITORY_MOUNT: ("bind", repository_mount.get("Source"), True),
        WHEEL_MOUNT: ("bind", wheel_mount.get("Source"), True),
        SOCKET_MOUNT: ("bind", SOCKET_MOUNT, False),
        STATE_MOUNT: ("volume", "", False),
    }
    for target, (mount_type, source, read_only) in expected_host_mounts.items():
        mount = by_target[target]
        observed_source = mount.get("Source", "")
        observed_read_only = mount.get("ReadOnly", False)
        if (
            mount.get("Type") != mount_type
            or not isinstance(observed_source, str)
            or observed_source != source
            or type(observed_read_only) is not bool
            or observed_read_only is not read_only
            or mount.get("Consistency") not in (None, "")
        ):
            raise ControllerContainerError("controller mount request is invalid")

        bind_options = mount.get("BindOptions")
        if mount_type == "bind":
            if bind_options is not None:
                if not isinstance(bind_options, dict) or set(bind_options) - {
                    "Propagation",
                    "NonRecursive",
                    "CreateMountpoint",
                    "ReadOnlyNonRecursive",
                    "ReadOnlyForceRecursive",
                }:
                    raise ControllerContainerError("controller bind request is invalid")
                propagation = bind_options.get("Propagation", "")
                if (
                    not isinstance(propagation, str)
                    or propagation not in ("", "rprivate")
                ):
                    raise ControllerContainerError("controller bind request is invalid")
                for option in (
                    "NonRecursive",
                    "CreateMountpoint",
                    "ReadOnlyNonRecursive",
                    "ReadOnlyForceRecursive",
                ):
                    observed = bind_options.get(option, False)
                    if type(observed) is not bool or observed is not False:
                        raise ControllerContainerError(
                            "controller bind request is invalid"
                        )
            if any(
                mount.get(option) is not None
                for option in (
                    "VolumeOptions",
                    "TmpfsOptions",
                    "ImageOptions",
                    "ClusterOptions",
                )
            ):
                raise ControllerContainerError("controller bind request is invalid")
        elif bind_options is not None:
            raise ControllerContainerError("controller anonymous state mount is invalid")

    repository = _require_repository(Path(repository_mount["Source"]))
    wheel_source = _require_wheel_source(Path(wheel_mount["Source"]), repository)
    return repository, wheel_source


def _normalize_capabilities(value: object) -> frozenset[str] | None:
    if not isinstance(value, list):
        return None
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            return None
        capability = item[4:] if item.startswith("CAP_") else item
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", capability):
            return None
        normalized.append(capability)
    if len(normalized) != len(set(normalized)):
        return None
    return frozenset(normalized)


def _has_exact_no_new_privileges(value: object) -> bool:
    return isinstance(value, list) and value in (
        ["no-new-privileges"],
        ["no-new-privileges:true"],
        ["no-new-privileges=true"],
    )


def _has_daemon_disabled_selinux_label(
    value: object, process_label: object
) -> bool:
    """Accept only Docker's exact NNP plus disabled-label normalization."""
    if (
        process_label != ""
        or not isinstance(value, list)
        or len(value) != 2
        or any(not isinstance(option, str) for option in value)
        or len(set(value)) != 2
        or "label=disable" not in value
    ):
        return False
    remaining = [option for option in value if option != "label=disable"]
    return _has_exact_no_new_privileges(remaining)


def _host_allows_disabled_selinux_label(
    enforce_path: Path = _SELINUX_ENFORCE_PATH,
) -> bool:
    """Return true only when SELinux is absent or explicitly non-enforcing."""
    try:
        enforce_path.stat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    try:
        return enforce_path.read_text(encoding="utf-8").strip() == "0"
    except OSError:
        return False


def _is_absent_or_empty_list(value: object) -> bool:
    return value is None or (isinstance(value, list) and not value)


def _is_absent_or_empty_dict(value: object) -> bool:
    return value is None or (isinstance(value, dict) and not value)


def _has_required_system_paths(
    value: object, required: frozenset[str], *, allow_dynamic_masked: bool = False
) -> bool:
    if (
        not isinstance(value, list)
        or any(not isinstance(path, str) for path in value)
        or len(value) != len(set(value))
    ):
        return False
    observed = set(value)
    if not required.issubset(observed):
        return False
    extras = observed - required
    return not extras or (
        allow_dynamic_masked
        and all(_DYNAMIC_MASKED_PATH.fullmatch(path) for path in extras)
    )


def _has_closed_privilege_channels(host_config: dict) -> bool:
    if any(
        not _is_absent_or_empty_list(host_config.get(field))
        for field in (
            "Binds",
            "Devices",
            "DeviceRequests",
            "DeviceCgroupRules",
            "GroupAdd",
            "VolumesFrom",
        )
    ):
        return False
    if any(
        not _is_absent_or_empty_dict(host_config.get(field))
        for field in ("Sysctls", "Tmpfs")
    ):
        return False
    namespace_modes = {
        "IpcMode": ("private",),
        "UTSMode": (None, "", "private"),
        "UsernsMode": (None, ""),
        "CgroupnsMode": ("private",),
    }
    return (
        host_config.get("Runtime") == "runc"
        and _has_required_system_paths(
            host_config.get("MaskedPaths"),
            _REQUIRED_MASKED_PATHS,
            allow_dynamic_masked=True,
        )
        and _has_required_system_paths(
            host_config.get("ReadonlyPaths"), _REQUIRED_READONLY_PATHS
        )
        and all(
            host_config.get(field) in allowed
            for field, allowed in namespace_modes.items()
        )
    )


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
    cap_add = _normalize_capabilities(host_config.get("CapAdd"))
    cap_drop = _normalize_capabilities(host_config.get("CapDrop"))
    security_options = host_config.get("SecurityOpt")
    security_options_valid = _has_exact_no_new_privileges(security_options) or (
        _has_daemon_disabled_selinux_label(
            security_options, document.get("ProcessLabel")
        )
        and _host_allows_disabled_selinux_label()
    )
    if (
        host_config.get("NetworkMode") != "host"
        or host_config.get("PidMode") != "host"
        or cap_drop != frozenset({"ALL"})
        or cap_add != frozenset(_REQUIRED_CAPABILITIES)
        or not security_options_valid
        or host_config.get("Privileged") is not False
        or not _has_closed_privilege_channels(host_config)
    ):
        raise ControllerContainerError("controller security identity is invalid")
    _require_install_mounts(document)
    return container_id


def build_create_argv(
    repository: Path,
    wheel_source: Path,
    container_name: str,
    image: str,
    lifecycle_token: str,
) -> list[str]:
    """Return the sole permitted argv for a long-lived controller."""
    repository = _require_repository(repository)
    wheel_source = _require_wheel_source(wheel_source, repository)
    container_name = _require_name(container_name, "container name")
    image = _require_image_id(image)
    lifecycle_token = _require_lifecycle_token(lifecycle_token)
    capability_argv = [
        argument
        for capability in _REQUIRED_CAPABILITIES
        for argument in ("--cap-add", capability)
    ]
    return [
        DOCKER, "create", "--name", container_name, "--user", "0:0", "--network", "host",
        "--pid", "host", "--ipc", "private", "--cgroupns", "private",
        "--runtime", "runc",
        "--cap-drop", "ALL", *capability_argv,
        "--security-opt", "no-new-privileges=true", "--label",
        f"{_LIFECYCLE_LABEL}={lifecycle_token}", "--label", f"{_ROLE_LABEL}={_CONTAINER_ROLE}",
        "--label", f"{_IMAGE_LABEL}={image}", "--mount",
        f"type=bind,src={repository},dst={REPOSITORY_MOUNT},readonly", "--mount",
        f"type=bind,src={wheel_source},dst={WHEEL_MOUNT},readonly", "--mount",
        f"type=volume,dst={STATE_MOUNT}", "--mount",
        f"type=bind,src={SOCKET_MOUNT},dst={SOCKET_MOUNT}",
        image, "/bin/sh", "-c", "exec sleep infinity",
    ]


def create_controller(
    repository: Path, wheel_source: Path, container_name: str, image: str,
    *, lifecycle_token: str, run: Runner | None = None,
) -> None:
    """Create and start one fresh controller, removing partial state on failure."""
    runner = _run if run is None else run
    lifecycle_token = _require_lifecycle_token(lifecycle_token)
    image = _require_image(image)
    repository = _require_repository(repository)
    wheel_source = _require_wheel_source(wheel_source, repository)
    container_name = _require_name(container_name, "container name")
    image_id = _resolve_image_id(runner, image)
    argv = build_create_argv(
        repository, wheel_source, container_name, image_id, lifecycle_token
    )
    _require_absent(
        runner, "container", container_name, "controller container"
    )
    owned_container_id: str | None = None
    try:
        try:
            create_result = _result(runner, argv, "create")
        except ControllerContainerError as exc:
            raise ControllerContainerCleanupError(
                "controller lifecycle cleanup failed"
            ) from exc
        raw_container_id = (
            create_result.stdout.strip()
            if isinstance(create_result.stdout, str)
            else ""
        )
        if not _CONTAINER_ID.fullmatch(raw_container_id):
            raise ControllerContainerCleanupError(
                "controller lifecycle cleanup failed"
            ) from ControllerContainerError("Docker create failed")
        try:
            (
                container_state,
                observed_container_id,
                _observed_volume_name,
            ) = _container_identity_state(
                runner,
                raw_container_id,
                container_name,
                lifecycle_token,
                image_id,
            )
        except ControllerContainerError as exc:
            raise ControllerContainerCleanupError(
                "controller lifecycle cleanup failed"
            ) from exc
        if container_state == "owned":
            owned_container_id = observed_container_id
        elif container_state == "ambiguous":
            raise ControllerContainerCleanupError(
                "controller lifecycle cleanup failed"
            )
        if create_result.returncode != 0:
            raise ControllerContainerError("Docker create failed")
        if (
            container_state != "owned"
            or observed_container_id != raw_container_id
        ):
            raise ControllerContainerError("controller container ownership proof failed")
        if _result(
            runner, [DOCKER, "start", raw_container_id], "start"
        ).returncode != 0:
            raise ControllerContainerError("Docker start failed")
    except ControllerContainerError as lifecycle_error:
        try:
            _cleanup_owned_resources(
                runner,
                container_id=owned_container_id,
                container_name=container_name,
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
    host_config = document.get("HostConfig")
    uses_disabled_selinux_label = isinstance(
        host_config, dict
    ) and _has_daemon_disabled_selinux_label(
        host_config.get("SecurityOpt"), document.get("ProcessLabel")
    )
    if (
        uses_disabled_selinux_label
        and not _host_allows_disabled_selinux_label()
    ):
        raise ControllerContainerError("controller security identity is invalid")
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
