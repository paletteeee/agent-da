"""Strict filesystem and JSON contracts for the native-scale launcher."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from txnmem_benchmark_manifests import _canonical_hash, shard_manifest


class FormalIOError(ValueError):
    """A formal artifact is ambiguous, stale, incomplete, or unsafe."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FormalIOError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> Any:
    raise FormalIOError(f"non-finite JSON number: {value}")


def canonical_json_bytes(value: Any) -> bytes:
    """Return a type-preserving canonical JSON encoding."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FormalIOError("value is not canonical JSON") from exc


def canonical_json_equal(left: Any, right: Any) -> bool:
    """Compare JSON values without Python's bool/int coercion."""

    return canonical_json_bytes(left) == canonical_json_bytes(right)


def _require_exact_int(
    document: Mapping[str, Any],
    field: str,
    *,
    expected: int | None = None,
    minimum: int | None = None,
) -> int:
    value = document.get(field)
    if type(value) is not int:  # Deliberately reject bool and integer subclasses.
        raise FormalIOError(f"{field} must be an integer")
    if expected is not None and value != expected:
        raise FormalIOError(f"{field} mismatch")
    if minimum is not None and value < minimum:
        raise FormalIOError(f"{field} must be at least {minimum}")
    return value


class FormalStore:
    """No-follow access to formal artifacts below one resolved output root."""

    def __init__(self, output_root: str | Path):
        requested = Path(output_root).expanduser().absolute()
        if requested.is_symlink():
            raise FormalIOError(f"formal output root must not be a symlink: {requested}")
        try:
            requested.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise FormalIOError(f"cannot create formal output root: {requested}") from exc
        if requested.is_symlink() or not requested.is_dir():
            raise FormalIOError(f"formal output root is not a real directory: {requested}")
        try:
            self.root = requested.resolve(strict=True)
        except OSError as exc:
            raise FormalIOError(f"cannot resolve formal output root: {requested}") from exc

    @staticmethod
    def _validate_parts(parts: Sequence[str]) -> tuple[str, ...]:
        if not parts:
            raise FormalIOError("formal path must have at least one component")
        normalized: list[str] = []
        for part in parts:
            if (
                not isinstance(part, str)
                or not part
                or part in {".", ".."}
                or Path(part).name != part
            ):
                raise FormalIOError(f"invalid formal path component: {part!r}")
            normalized.append(part)
        return tuple(normalized)

    @property
    def _directory_flags(self) -> int:
        return os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)

    def _open_root(self) -> int:
        try:
            return os.open(self.root, self._directory_flags)
        except OSError as exc:
            raise FormalIOError(f"formal output root is unavailable: {self.root}") from exc

    def _open_parent(self, parts: Sequence[str], *, create: bool) -> tuple[int, str]:
        normalized = self._validate_parts(parts)
        descriptor = self._open_root()
        try:
            for component in normalized[:-1]:
                try:
                    child = os.open(component, self._directory_flags, dir_fd=descriptor)
                except FileNotFoundError:
                    if not create:
                        raise
                    try:
                        os.mkdir(component, mode=0o755, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    child = os.open(component, self._directory_flags, dir_fd=descriptor)
                except OSError as exc:
                    raise FormalIOError(
                        f"formal path component is not a real directory: {component}"
                    ) from exc
                os.close(descriptor)
                descriptor = child
            return descriptor, normalized[-1]
        except BaseException:
            os.close(descriptor)
            raise

    def path(self, *parts: str) -> Path:
        normalized = self._validate_parts(parts)
        return self.root.joinpath(*normalized)

    def ensure_directory(self, *parts: str) -> None:
        normalized = self._validate_parts(parts)
        descriptor = self._open_root()
        try:
            for component in normalized:
                try:
                    child = os.open(component, self._directory_flags, dir_fd=descriptor)
                except FileNotFoundError:
                    try:
                        os.mkdir(component, mode=0o755, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    child = os.open(component, self._directory_flags, dir_fd=descriptor)
                except OSError as exc:
                    raise FormalIOError(
                        f"formal path component is not a real directory: {component}"
                    ) from exc
                os.close(descriptor)
                descriptor = child
        finally:
            os.close(descriptor)

    def entry_kind(self, *parts: str) -> str:
        try:
            parent, name = self._open_parent(parts, create=False)
        except FileNotFoundError:
            return "missing"
        try:
            try:
                metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return "missing"
            if stat.S_ISLNK(metadata.st_mode):
                raise FormalIOError(f"formal path must not be a symlink: {self.path(*parts)}")
            if stat.S_ISREG(metadata.st_mode):
                return "file"
            if stat.S_ISDIR(metadata.st_mode):
                return "directory"
            raise FormalIOError(f"formal path has unsupported type: {self.path(*parts)}")
        finally:
            os.close(parent)

    def create_directory_exclusive(self, *parts: str) -> None:
        parent, name = self._open_parent(parts, create=True)
        try:
            try:
                os.mkdir(name, mode=0o755, dir_fd=parent)
            except FileExistsError as exc:
                raise FormalIOError(
                    f"refusing to reuse existing formal directory: {self.path(*parts)}"
                ) from exc
            except OSError as exc:
                raise FormalIOError(
                    f"cannot create formal directory: {self.path(*parts)}"
                ) from exc
        finally:
            os.close(parent)

    def load_json(self, *parts: str) -> Any:
        parent, name = self._open_parent(parts, create=False)
        descriptor: int | None = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(name, flags, dir_fd=parent)
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise FormalIOError(f"formal JSON is not a regular file: {self.path(*parts)}")
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                descriptor = None
                return json.load(
                    stream,
                    object_pairs_hook=_reject_duplicate_keys,
                    parse_constant=_reject_nonfinite,
                )
        except FormalIOError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise FormalIOError(f"malformed formal JSON: {self.path(*parts)}") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent)

    def write_json_exclusive(
        self,
        *parts: str,
        payload: Any,
        sort_keys: bool = True,
    ) -> None:
        try:
            encoded = (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=sort_keys,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise FormalIOError("formal payload is not valid JSON") from exc
        parent, name = self._open_parent(parts, create=True)
        descriptor: int | None = None
        try:
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            descriptor = os.open(name, flags, 0o644, dir_fd=parent)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = None
                stream.write(encoded)
                stream.flush()
        except FileExistsError as exc:
            raise FormalIOError(
                f"refusing to overwrite existing formal file: {self.path(*parts)}"
            ) from exc
        except OSError as exc:
            raise FormalIOError(f"cannot write formal file: {self.path(*parts)}") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent)

    def write_or_verify_json(
        self,
        *parts: str,
        payload: Any,
        resume: bool,
        artifact_name: str,
        sort_keys: bool = True,
    ) -> None:
        kind = self.entry_kind(*parts)
        if kind == "missing":
            self.write_json_exclusive(
                *parts,
                payload=payload,
                sort_keys=sort_keys,
            )
            return
        if kind != "file":
            raise FormalIOError(f"existing {artifact_name} is not a regular file")
        if not resume:
            raise FormalIOError(
                f"refusing to overwrite existing {artifact_name} without --resume: "
                f"{self.path(*parts)}"
            )
        existing = self.load_json(*parts)
        if not canonical_json_equal(existing, payload):
            raise FormalIOError(
                f"existing resume {artifact_name} does not match recomputation: "
                f"{self.path(*parts)}"
            )


def validate_parent_manifest(manifest: Any) -> Mapping[str, Any]:
    if not isinstance(manifest, Mapping):
        raise FormalIOError("parent manifest must be a mapping")
    _require_exact_int(manifest, "manifest_version", expected=1)
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise FormalIOError("parent manifest tasks must be a non-empty list")
    _require_exact_int(manifest, "task_count", expected=len(tasks))
    for field in ("source_task_count", "seed"):
        if field in manifest:
            _require_exact_int(manifest, field, minimum=0)
    split_identity = manifest.get("public_split_identity")
    if split_identity is not None:
        if not isinstance(split_identity, Mapping):
            raise FormalIOError("public_split_identity must be a mapping")
        for field in ("source_task_count", "selected_task_count"):
            _require_exact_int(split_identity, field, minimum=1)
    task_split = manifest.get("task_level_split")
    if task_split is not None:
        if not isinstance(task_split, Mapping):
            raise FormalIOError("task_level_split must be a mapping")
        _require_exact_int(task_split, "seed", minimum=0)
    for index, task in enumerate(tasks):
        if not isinstance(task, Mapping):
            raise FormalIOError(f"parent manifest task {index} must be a mapping")
        position = _require_exact_int(task, "source_position", expected=index)
        if "source_index" in task:
            _require_exact_int(task, "source_index", expected=position)
    manifest_hash = manifest.get("manifest_hash")
    if not isinstance(manifest_hash, str) or manifest_hash != _canonical_hash(manifest):
        raise FormalIOError("parent manifest_hash does not match canonical content")
    return manifest


def _validate_shard_manifest(shard: Any) -> Mapping[str, Any]:
    if not isinstance(shard, Mapping):
        raise FormalIOError("shard manifest must be a mapping")
    _require_exact_int(shard, "manifest_version", expected=1)
    shard_index = _require_exact_int(shard, "shard_index", minimum=0)
    shard_count = _require_exact_int(shard, "shard_count", minimum=1)
    if shard_index >= shard_count:
        raise FormalIOError("shard_index is outside shard_count")
    tasks = shard.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise FormalIOError("shard manifest tasks must be a non-empty list")
    _require_exact_int(shard, "task_count", expected=len(tasks))
    for task in tasks:
        if not isinstance(task, Mapping):
            raise FormalIOError("shard task must be a mapping")
        _require_exact_int(task, "source_position", minimum=0)
    manifest_hash = shard.get("manifest_hash")
    if not isinstance(manifest_hash, str) or manifest_hash != _canonical_hash(shard):
        raise FormalIOError("shard manifest_hash does not match canonical content")
    return shard


def bind_native_shard_report(shard: Any, raw: Any) -> dict[str, Any]:
    """Strictly bind one raw summary to its frozen shard identity."""

    shard = _validate_shard_manifest(shard)
    if not isinstance(raw, Mapping):
        raise FormalIOError("raw native report must be a mapping")
    if "schema_version" in raw:
        _require_exact_int(raw, "schema_version", expected=1)
    if raw.get("manifest_sha256") != shard.get("manifest_hash"):
        raise FormalIOError("native report manifest hash does not match executed shard")
    execution_condition = raw.get("condition_fingerprint")
    if not isinstance(execution_condition, str) or not execution_condition:
        raise FormalIOError("native report has no execution condition fingerprint")
    repetitions = _require_exact_int(raw, "repetitions", minimum=1)
    tasks = shard["tasks"]
    rows = raw.get("task_summaries")
    if not isinstance(rows, list) or len(rows) != len(tasks) * repetitions:
        raise FormalIOError("native report task rows do not cover shard repetitions")
    if "task_count" in raw:
        _require_exact_int(raw, "task_count", expected=len(rows))
    if "unique_task_count" in raw:
        _require_exact_int(raw, "unique_task_count", expected=len(tasks))
    for field in ("native_event_count", "evaluation_error_count"):
        if field in raw:
            _require_exact_int(raw, field, minimum=0)

    bound_rows: list[dict[str, Any]] = []
    for repetition in range(1, repetitions + 1):
        for task_offset, task in enumerate(tasks):
            row = rows[(repetition - 1) * len(tasks) + task_offset]
            if not isinstance(row, Mapping) or row.get("task_id") != task.get("task_id"):
                raise FormalIOError("native report task order does not match shard")
            status = row.get("status")
            if not isinstance(status, str) or not status:
                raise FormalIOError("native report task has malformed status")
            official = row.get("official")
            if official is not None and not isinstance(official, Mapping):
                raise FormalIOError("native report task has malformed official result")
            if "source_position" in row:
                _require_exact_int(
                    row,
                    "source_position",
                    expected=task["source_position"],
                )
            if "repetition" in row:
                _require_exact_int(row, "repetition", expected=repetition)
            item = dict(row)
            item["source_position"] = task["source_position"]
            item["repetition"] = repetition
            bound_rows.append(item)

    report: dict[str, Any] = {
        "parent_manifest_hash": shard["parent_manifest_hash"],
        "shard_index": shard["shard_index"],
        "shard_count": shard["shard_count"],
        "benchmark": shard["benchmark"],
        "split": shard["split"],
        "source_identity": shard["source_identity"],
        "condition_fingerprint": shard["condition_fingerprint"],
        "execution_condition_fingerprint": execution_condition,
        "execution_manifest_hash": raw["manifest_sha256"],
        "repetitions": repetitions,
        "task_summaries": bound_rows,
    }
    if "domain" in shard:
        report["domain"] = shard["domain"]
    return report


def _load_frozen_job(
    store: FormalStore, job: str, shard_count: int
) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    parent = store.load_json("manifests", job, "parent.json")
    validate_parent_manifest(parent)
    expected_shards = shard_manifest(parent, shard_count)
    shards: list[Mapping[str, Any]] = []
    for index, expected in enumerate(expected_shards):
        shard = store.load_json("manifests", job, f"shard_{index:03d}.json")
        _validate_shard_manifest(shard)
        if not canonical_json_equal(shard, expected):
            raise FormalIOError(f"shard manifest {index} does not match frozen parent")
        shards.append(shard)
    return parent, shards


def recompute_native_merge(
    store: FormalStore,
    job: str,
    shard_count: int,
    *,
    require_raw: bool,
) -> dict[str, Any]:
    """Strictly reload and recompute one merged native-scale report."""

    from txnmem_batch_merge import merge_native_shards

    parent, shards = _load_frozen_job(store, job, shard_count)
    reports: list[Mapping[str, Any]] = []
    for index, shard in enumerate(shards):
        shard_name = f"shard_{index:03d}"
        bound = store.load_json("runs", job, shard_name, "shard_report.json")
        if require_raw:
            raw = store.load_json(
                "runs",
                job,
                shard_name,
                "results",
                "native_batch_summary.json",
            )
            rebound = bind_native_shard_report(shard, raw)
            if not canonical_json_equal(bound, rebound):
                raise FormalIOError(f"bound report does not match raw shard {index}")
        reports.append(bound)
    merged = merge_native_shards(parent, reports)
    validate_merged_report(merged)
    return merged


def validate_merged_report(report: Any) -> Mapping[str, Any]:
    if not isinstance(report, Mapping):
        raise FormalIOError("merged report must be a mapping")
    for field in ("schema_version", "shard_count", "repetitions", "task_count", "row_count"):
        _require_exact_int(report, field, minimum=1)
    if report["schema_version"] != 1:
        raise FormalIOError("unsupported merged schema_version")
    task_aggregate = report.get("task_aggregate")
    official = report.get("official")
    if not isinstance(task_aggregate, Mapping) or not isinstance(official, Mapping):
        raise FormalIOError("merged report aggregates must be mappings")
    for field in ("denominator", "successes", "failures"):
        _require_exact_int(task_aggregate, field, minimum=0)
    for field in ("trials", "successes", "failures"):
        _require_exact_int(official, field, minimum=0)
    for counts_name, counts in (
        ("status_counts", task_aggregate.get("status_counts")),
        ("evaluator_status_counts", official.get("evaluator_status_counts")),
    ):
        if not isinstance(counts, Mapping):
            raise FormalIOError(f"{counts_name} must be a mapping")
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise FormalIOError(f"{counts_name} values must be non-negative integers")
    return report


def preflight_existing_merge(
    store: FormalStore,
    job: str,
    shard_count: int,
    *,
    resume: bool,
) -> bool:
    """Validate an existing protected merge before any shard process."""

    parts = ("merged", f"{job}.json")
    kind = store.entry_kind(*parts)
    if kind == "missing":
        return False
    if kind != "file":
        raise FormalIOError(f"existing merge is not a regular file: {store.path(*parts)}")
    if not resume:
        raise FormalIOError(
            f"refusing to overwrite existing merge without --resume: {store.path(*parts)}"
        )
    existing = store.load_json(*parts)
    validate_merged_report(existing)
    recomputed = recompute_native_merge(
        store,
        job,
        shard_count,
        require_raw=True,
    )
    if not canonical_json_equal(existing, recomputed):
        raise FormalIOError(
            f"existing resume merge does not match recomputation: {store.path(*parts)}"
        )
    return True


def prepare_shard_run(
    store: FormalStore,
    job: str,
    shard_index: int,
    shard_count: int,
    *,
    resume: bool,
) -> str:
    """Exclusively create a new run or strictly verify a completed run."""

    _, shards = _load_frozen_job(store, job, shard_count)
    shard = shards[shard_index]
    shard_name = f"shard_{shard_index:03d}"
    run_parts = ("runs", job, shard_name)
    kind = store.entry_kind(*run_parts)
    if kind == "missing":
        store.create_directory_exclusive(*run_parts)
        return "execute"
    if kind != "directory":
        raise FormalIOError(f"shard run is not a directory: {store.path(*run_parts)}")
    if not resume:
        raise FormalIOError(
            f"refusing to overwrite shard run without --resume: {store.path(*run_parts)}"
        )
    raw = store.load_json(
        *run_parts,
        "results",
        "native_batch_summary.json",
    )
    bound = store.load_json(*run_parts, "shard_report.json")
    rebound = bind_native_shard_report(shard, raw)
    if not canonical_json_equal(bound, rebound):
        raise FormalIOError(f"bound report does not match raw shard {shard_index}")
    return "reuse"


def bind_shard_files(
    store: FormalStore,
    job: str,
    shard_index: int,
    shard_count: int,
) -> None:
    _, shards = _load_frozen_job(store, job, shard_count)
    shard = shards[shard_index]
    shard_name = f"shard_{shard_index:03d}"
    raw = store.load_json(
        "runs",
        job,
        shard_name,
        "results",
        "native_batch_summary.json",
    )
    report = bind_native_shard_report(shard, raw)
    store.write_json_exclusive(
        "runs",
        job,
        shard_name,
        "shard_report.json",
        payload=report,
    )


def finalize_native_merge(
    store: FormalStore,
    job: str,
    shard_count: int,
    *,
    resume: bool,
    require_raw: bool,
) -> None:
    merged = recompute_native_merge(
        store,
        job,
        shard_count,
        require_raw=require_raw,
    )
    parts = ("merged", f"{job}.json")
    kind = store.entry_kind(*parts)
    if kind == "missing":
        store.write_json_exclusive(*parts, payload=merged)
        return
    if kind != "file":
        raise FormalIOError(f"existing merge is not a regular file: {store.path(*parts)}")
    if not resume:
        raise FormalIOError(
            f"refusing to overwrite existing merge without --resume: {store.path(*parts)}"
        )
    existing = store.load_json(*parts)
    validate_merged_report(existing)
    if not canonical_json_equal(existing, merged):
        raise FormalIOError(
            f"existing resume merge does not match recomputation: {store.path(*parts)}"
        )
