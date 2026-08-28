import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import txnmem_formal_controller as controller
import txnmem_provenance_progress as progress_protocol


class FormalControllerCleanupTests(unittest.TestCase):
    @staticmethod
    def _progress_line() -> bytes:
        return progress_protocol.canonical_snapshot_line(
            {
                "schema": "txnmem-provenance-progress-snapshot-v1",
                "run_binding_sha256": "a" * 64,
                "config_sha256": "b" * 64,
                "phase": "measurement",
                "cell_index": 1,
                "cell_count": 15,
                "graph_size": 100,
                "concurrency": 1,
                "repetition_index": 1,
                "repetition_count": 30,
                "completed_repetitions": 1,
                "total_repetitions": 450,
                "completed_samples": 32,
                "total_samples": 14400,
                "update_sequence": 1,
                "status": "running",
                "last_update_age_seconds": 0,
            }
        )

    @staticmethod
    def _identity(path: Path, parent: Path):
        metadata = path.stat()
        parent_metadata = parent.stat()
        return controller._BootstrapExport(
            path=path,
            device=int(metadata.st_dev),
            inode=int(metadata.st_ino),
            parent_device=int(parent_metadata.st_dev),
            parent_inode=int(parent_metadata.st_ino),
        )

    @staticmethod
    def _approved_repository(root: Path):
        repository = root / "repository"
        paths = sorted(controller._REQUIRED_APPROVED_PATHS)
        for relative in paths:
            path = repository / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"# {relative}\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
        subprocess.run(
            ["git", "config", "user.email", "controller@example.invalid"],
            cwd=repository,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Controller Fixture"],
            cwd=repository,
            check=True,
        )
        subprocess.run(["git", "add", "."], cwd=repository, check=True)
        subprocess.run(
            ["git", "commit", "-q", "-m", "approved"],
            cwd=repository,
            check=True,
        )
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        manifest = {
            "schema": controller._APPROVAL_SCHEMA,
            "source_commit": commit,
            "files": [
                {
                    "path": relative,
                    "blob_sha256": hashlib.sha256(
                        (repository / relative).read_bytes()
                    ).hexdigest(),
                }
                for relative in paths
            ],
        }
        return repository, controller._normalize_approved_source(manifest)

    def test_cleanup_is_inode_bound_and_removes_only_the_attested_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir(mode=0o700)
            export = bootstrap / "source-fixture"
            nested = export / "src"
            nested.mkdir(parents=True, mode=0o700)
            (nested / "module.py").write_text("value = 1\n", encoding="utf-8")
            identity = self._identity(export, bootstrap)

            with patch.object(controller, "BOOTSTRAP_ROOT", bootstrap):
                controller._remove_export(identity)

            self.assertFalse(export.exists())

    def test_cleanup_rejects_same_path_replacement_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir(mode=0o700)
            export = bootstrap / "source-fixture"
            export.mkdir(mode=0o700)
            identity = self._identity(export, bootstrap)
            original = bootstrap / "original"
            os.rename(export, original)
            export.mkdir(mode=0o700)
            marker = export / "replacement.txt"
            marker.write_text("preserve", encoding="utf-8")

            with patch.object(controller, "BOOTSTRAP_ROOT", bootstrap):
                with self.assertRaisesRegex(
                    controller.FormalControllerError, "identity changed"
                ):
                    controller._remove_export(identity)

            self.assertEqual(marker.read_text(encoding="utf-8"), "preserve")

    def test_approved_export_contains_configs_scripts_and_compose(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository, approved = self._approved_repository(root)
            bootstrap = root / "bootstrap"
            bootstrap.mkdir(mode=0o700)

            with patch.object(controller, "BOOTSTRAP_ROOT", bootstrap):
                exported = controller._create_committed_export(
                    repository,
                    approved,
                    controller_uid=os.getuid(),
                )
                try:
                    for relative in controller._FORMAL_AUXILIARY_PATHS:
                        self.assertTrue((exported.path / relative).is_file())
                        self.assertEqual(
                            (exported.path / relative).read_bytes(),
                            (repository / relative).read_bytes(),
                        )
                    self.assertEqual(
                        (exported.path / "configs" / "provenance_performance_matrix.json").stat().st_mode
                        & 0o777,
                        0o400,
                    )
                finally:
                    controller._remove_export(exported)

    def test_approved_source_closure_includes_smoke_module_and_wrapper(self):
        self.assertIn(
            "scripts/run_formal_provenance_smoke.sh",
            controller._FORMAL_AUXILIARY_PATHS,
        )
        self.assertIn(
            "src/txnmem_formal_smoke.py",
            controller._REQUIRED_APPROVED_PATHS,
        )
        self.assertIn(
            "src/txnmem_provenance_progress.py",
            controller._REQUIRED_APPROVED_PATHS,
        )
        self.assertIn(
            "scripts/read_formal_provenance_progress.sh",
            controller._FORMAL_AUXILIARY_PATHS,
        )
        self.assertIn(
            "scripts/read_formal_provenance_progress.sh",
            controller._REQUIRED_APPROVED_PATHS,
        )

    def test_export_rejects_head_change_from_root_approved_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository, approved = self._approved_repository(root)
            target = repository / "src" / "txnmem_formal_controller.py"
            target.write_text("# unapproved replacement\n", encoding="utf-8")
            subprocess.run(["git", "add", str(target)], cwd=repository, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "unapproved"],
                cwd=repository,
                check=True,
            )
            bootstrap = root / "bootstrap"
            bootstrap.mkdir(mode=0o700)

            with patch.object(controller, "BOOTSTRAP_ROOT", bootstrap):
                with self.assertRaisesRegex(
                    controller.FormalControllerError, "HEAD changed"
                ):
                    controller._create_committed_export(
                        repository,
                        approved,
                        controller_uid=os.getuid(),
                    )
            self.assertEqual(list(bootstrap.iterdir()), [])

    def test_dispatch_runs_measure_from_export_with_formal_configs_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp).resolve() / "export"
            (export / "src").mkdir(parents=True)
            (export / "configs").mkdir()
            (export / "configs" / "provenance_performance_matrix.json").write_text(
                "{}\n", encoding="utf-8"
            )
            (export / "src" / "txnmem_provenance_execution_collector.py").write_text(
                "\n".join(
                    (
                        "from pathlib import Path",
                        "def main(argv, *, _controller_context=None):",
                        "    config = Path(__file__).resolve().parents[1] / 'configs' / 'provenance_performance_matrix.json'",
                        "    return 0 if config.is_file() and _controller_context.get('source_commit') else 91",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            rows = tuple(
                (path, hashlib.sha256(path.encode()).hexdigest())
                for path in sorted(controller._REQUIRED_APPROVED_PATHS)
            )
            manifest = {
                "schema": controller._APPROVAL_SCHEMA,
                "source_commit": "a" * 40,
                "files": [
                    {"path": path, "blob_sha256": digest}
                    for path, digest in rows
                ],
            }
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=rows,
                manifest=manifest,
                manifest_sha256=hashlib.sha256(
                    controller._canonical_json_bytes(manifest)
                ).hexdigest(),
            )
            module_name = "txnmem_provenance_execution_collector"
            previous = sys.modules.pop(module_name, None)
            try:
                self.assertEqual(
                    controller._dispatch("measure", [], export, approved), 0
                )
            finally:
                sys.modules.pop(module_name, None)
                if previous is not None:
                    sys.modules[module_name] = previous

    def test_dispatch_runs_smoke_with_the_controller_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp).resolve() / "export"
            (export / "src").mkdir(parents=True)
            (export / "src" / "txnmem_formal_smoke.py").write_text(
                "\n".join(
                    (
                        "def main(argv, *, _controller_context=None):",
                        "    return 0 if argv == ['--out', '/external/smoke.json'] and _controller_context.get('source_commit') else 91",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            rows = tuple(
                (path, hashlib.sha256(path.encode()).hexdigest())
                for path in sorted(controller._REQUIRED_APPROVED_PATHS)
            )
            manifest = {
                "schema": controller._APPROVAL_SCHEMA,
                "source_commit": "a" * 40,
                "files": [
                    {"path": path, "blob_sha256": digest}
                    for path, digest in rows
                ],
            }
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=rows,
                manifest=manifest,
                manifest_sha256=hashlib.sha256(
                    controller._canonical_json_bytes(manifest)
                ).hexdigest(),
            )
            module_name = "txnmem_formal_smoke"
            previous = sys.modules.pop(module_name, None)
            try:
                self.assertEqual(
                    controller._dispatch(
                        "smoke",
                        ["--out", "/external/smoke.json"],
                        export,
                        approved,
                    ),
                    0,
                )
            finally:
                sys.modules.pop(module_name, None)
                if previous is not None:
                    sys.modules[module_name] = previous

    def test_dispatch_progress_uses_the_dedicated_reader_and_never_measurement_main(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp).resolve() / "export"
            (export / "src").mkdir(parents=True)
            expected_line = self._progress_line()
            (export / "src" / "txnmem_provenance_execution_collector.py").write_text(
                "\n".join(
                    (
                        "def main(*_args, **_kwargs):",
                        "    raise RuntimeError('measurement entry point used')",
                        "def read_formal_progress_line(argv, *, _controller_context=None, _controller_project_root=None):",
                        f"    return {expected_line!r} if argv == ['--run-id', 'run', '--authorization-nonce', '/private/nonce'] and _controller_context.get('source_commit') and str(_controller_project_root) == '/approved/project' else b''",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            rows = tuple(
                (path, hashlib.sha256(path.encode()).hexdigest())
                for path in sorted(controller._REQUIRED_APPROVED_PATHS)
            )
            manifest = {
                "schema": controller._APPROVAL_SCHEMA,
                "source_commit": "a" * 40,
                "files": [
                    {"path": path, "blob_sha256": digest}
                    for path, digest in rows
                ],
            }
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=rows,
                manifest=manifest,
                manifest_sha256=hashlib.sha256(
                    controller._canonical_json_bytes(manifest)
                ).hexdigest(),
            )
            module_name = "txnmem_provenance_execution_collector"
            previous = sys.modules.pop(module_name, None)
            try:
                try:
                    observed = controller._dispatch(
                        "progress",
                        [
                            "--run-id",
                            "run",
                            "--authorization-nonce",
                            "/private/nonce",
                        ],
                        export,
                        approved,
                        project_root=Path("/approved/project"),
                    )
                except controller.FormalControllerError as exc:
                    self.fail(
                        "dedicated progress dispatch is missing: "
                        + type(exc).__name__
                    )
                self.assertEqual(observed, expected_line)
            finally:
                sys.modules.pop(module_name, None)
                if previous is not None:
                    sys.modules[module_name] = previous

    def test_dispatch_progress_rejects_a_reader_blocked_record_as_non_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp).resolve() / "export"
            (export / "src").mkdir(parents=True)
            blocked_line = (
                b'{"blocked_class":"formal_progress_unavailable",'
                b'"schema":"txnmem-provenance-progress-reader-v1",'
                b'"status":"blocked"}\n'
            )
            (export / "src" / "txnmem_provenance_execution_collector.py").write_text(
                "\n".join(
                    (
                        "def main(*_args, **_kwargs):",
                        "    raise RuntimeError('measurement entry point used')",
                        "def read_formal_progress_line(*_args, **_kwargs):",
                        f"    return {blocked_line!r}",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            module_name = "txnmem_provenance_execution_collector"
            previous = sys.modules.pop(module_name, None)
            try:
                with self.assertRaises(controller.FormalControllerError):
                    controller._dispatch(
                        "progress",
                        [
                            "--run-id",
                            "run",
                            "--authorization-nonce",
                            "/private/nonce",
                        ],
                        export,
                        approved,
                        project_root=Path("/approved/project"),
                    )
            finally:
                sys.modules.pop(module_name, None)
                if previous is not None:
                    sys.modules[module_name] = previous

    def test_main_progress_emits_exactly_one_canonical_line_after_export_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            export = Path(tmp).resolve() / "source-fixture"
            export.mkdir()
            identity = self._identity(export, export.parent)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            expected_line = self._progress_line()
            stdout = io.StringIO()
            stderr = io.StringIO()
            cleanup_observed = []

            def remove_export(_identity):
                cleanup_observed.append(True)

            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", return_value=expected_line
            ), patch.object(
                controller, "_remove_export", side_effect=remove_export
            ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    [
                        "--project-root",
                        str(root),
                        "progress",
                        "--run-id",
                        "private-run",
                        "--authorization-nonce",
                        "/private/nonce",
                    ]
                )

            self.assertEqual(status, 0)
            self.assertEqual(stdout.getvalue().encode("utf-8"), expected_line)
            self.assertEqual(stderr.getvalue(), "")
            self.assertEqual(cleanup_observed, [True])

    def test_main_progress_flush_failure_never_appends_a_second_status_record(self):
        class FlushFailsAfterWrite:
            def __init__(self):
                self.parts: list[str] = []

            def write(self, value: str) -> int:
                self.parts.append(value)
                return len(value)

            def flush(self) -> None:
                raise OSError("seeded private flush failure /private/output")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            export = Path(tmp).resolve() / "source-fixture"
            export.mkdir()
            identity = self._identity(export, export.parent)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            expected_line = self._progress_line()
            stdout = FlushFailsAfterWrite()
            stderr = io.StringIO()

            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", return_value=expected_line
            ), patch.object(
                controller, "_remove_export"
            ), patch.object(
                controller.sys, "stdout", stdout
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    [
                        "--project-root",
                        str(root),
                        "progress",
                        "--run-id",
                        "private-run",
                        "--authorization-nonce",
                        "/private/nonce",
                    ]
                )

            observed = "".join(stdout.parts).encode("utf-8")
            self.assertEqual(status, 2)
            self.assertEqual(observed, expected_line)
            self.assertEqual(observed.count(b"\n"), 1)
            self.assertNotIn(b"formal_progress_unavailable", observed)
            self.assertEqual(stderr.getvalue(), "")

    def test_main_progress_rejects_canonical_json_outside_the_sanitized_closure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            export = Path(tmp).resolve() / "source-fixture"
            export.mkdir()
            identity = self._identity(export, export.parent)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            leaked_value = "/seeded/private/progress.json"
            unsanitized_line = (
                b'{"private_path":"/seeded/private/progress.json"}\n'
            )
            expected_blocked = (
                b'{"blocked_class":"formal_progress_unavailable",'
                b'"schema":"txnmem-provenance-progress-reader-v1",'
                b'"status":"blocked"}\n'
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", return_value=unsanitized_line
            ), patch.object(
                controller, "_remove_export"
            ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    [
                        "--project-root",
                        str(root),
                        "progress",
                        "--run-id",
                        "private-run",
                        "--authorization-nonce",
                        "/private/nonce",
                    ]
                )

            self.assertEqual(status, 2)
            self.assertEqual(stdout.getvalue().encode("utf-8"), expected_blocked)
            self.assertNotIn(leaked_value, stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_progress_output_gate_rejects_raw_values_in_sanitized_fields(self):
        valid = json.loads(self._progress_line())
        raw_run = dict(valid)
        raw_run["run_binding_sha256"] = "seeded-private-run-id"
        raw_exception = dict(valid)
        raw_exception["status"] = "blocked"
        raw_exception["terminal_reason_class"] = (
            "seeded raw exception /private/progress.json"
        )

        for label, document in (
            ("run identity", raw_run),
            ("terminal reason", raw_exception),
        ):
            with self.subTest(case=label):
                payload = (
                    json.dumps(
                        document,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
                with self.assertRaises(controller.FormalControllerError):
                    controller._validate_progress_output(payload)

    def test_progress_output_gate_rejects_semantically_impossible_matrix_states(self):
        valid = json.loads(self._progress_line())
        cases = (
            {**valid, "cell_index": 999},
            {**valid, "completed_repetitions": -1},
            {**valid, "graph_size": 1000},
            {**valid, "concurrency": 2},
            {**valid, "repetition_index": 2},
            {**valid, "completed_repetitions": 2, "completed_samples": 64},
            {**valid, "update_sequence": 2},
            {
                **valid,
                "status": "completed",
                "terminal_reason_class": "completed",
            },
        )
        for document in cases:
            with self.subTest(document=document):
                payload = (
                    json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
                    + b"\n"
                )
                with self.assertRaises(controller.FormalControllerError):
                    controller._validate_progress_output(payload)

    def test_progress_output_gate_requires_exact_zero_state_closure(self):
        starting = json.loads(self._progress_line())
        starting.update(
            {
                "cell_index": 1,
                "graph_size": 100,
                "concurrency": 1,
                "repetition_index": 0,
                "completed_repetitions": 0,
                "completed_samples": 0,
                "update_sequence": 0,
                "status": "starting",
            }
        )
        self.assertEqual(
            controller._validate_progress_output(
                json.dumps(starting, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
            ),
            json.dumps(starting, sort_keys=True, separators=(",", ":")).encode()
            + b"\n",
        )
        blocked_zero = {
            **starting,
            "status": "blocked",
            "terminal_reason_class": "formal_eligibility_failed",
        }
        blocked_zero_payload = (
            json.dumps(
                blocked_zero,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        self.assertEqual(
            controller._validate_progress_output(blocked_zero_payload),
            blocked_zero_payload,
        )
        for document in (
            {**starting, "cell_index": 2},
            {**starting, "status": "running"},
            {
                **starting,
                "status": "completed",
                "terminal_reason_class": "completed",
            },
        ):
            with self.subTest(document=document):
                payload = (
                    json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
                    + b"\n"
                )
                with self.assertRaises(controller.FormalControllerError):
                    controller._validate_progress_output(payload)

    def test_progress_output_gate_accepts_only_the_exact_completed_terminal_state(self):
        completed = json.loads(self._progress_line())
        completed.update(
            {
                "cell_index": 15,
                "graph_size": 10000,
                "concurrency": 16,
                "repetition_index": 30,
                "completed_repetitions": 450,
                "completed_samples": 14400,
                "update_sequence": 450,
                "status": "completed",
                "terminal_reason_class": "completed",
            }
        )
        payload = (
            json.dumps(completed, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        self.assertEqual(controller._validate_progress_output(payload), payload)

    def test_main_progress_sanitizes_project_root_expansion_failure(self):
        raw_failure = "seeded private root /private/project"
        raw_argument = "~txnmem-definitely-missing-user/private-project"
        expected_blocked = controller._blocked_progress_output().decode("utf-8")
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.object(
            controller.Path,
            "expanduser",
            side_effect=RuntimeError(raw_failure),
        ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = controller.main(
                [
                    "--project-root",
                    raw_argument,
                    "progress",
                    "--run-id",
                    "private-run",
                    "--authorization-nonce",
                    "/private/nonce",
                ]
            )

        self.assertEqual(status, 2)
        self.assertEqual(stdout.getvalue(), expected_blocked)
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn(raw_failure, stdout.getvalue())
        self.assertNotIn(raw_argument, stdout.getvalue())

    def test_invalid_progress_invocation_is_sanitized_by_the_outer_boundary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = controller.main(
                ["--wrong-root-flag", "/private/project", "progress"]
            )

        self.assertEqual(status, 64)
        self.assertEqual(stdout.getvalue().encode(), controller._blocked_progress_output())
        self.assertEqual(stderr.getvalue(), "")

    def test_main_progress_failures_emit_only_stable_canonical_blocked_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            export = Path(tmp).resolve() / "source-fixture"
            export.mkdir()
            identity = self._identity(export, export.parent)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            raw_failure = "seeded raw private path /private/nonce and credential"

            for label, dispatch_result, cleanup_failure in (
                ("reader", RuntimeError(raw_failure), None),
                ("cleanup", self._progress_line(), OSError(raw_failure)),
            ):
                with self.subTest(case=label):
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    dispatch_patch = (
                        patch.object(controller, "_dispatch", side_effect=dispatch_result)
                        if isinstance(dispatch_result, BaseException)
                        else patch.object(controller, "_dispatch", return_value=dispatch_result)
                    )
                    cleanup_patch = (
                        patch.object(controller, "_remove_export", side_effect=cleanup_failure)
                        if cleanup_failure is not None
                        else patch.object(controller, "_remove_export")
                    )
                    with patch.object(
                        controller.os, "geteuid", return_value=0
                    ), patch.object(
                        controller, "_verify_installed_controller", return_value=approved
                    ), patch.object(
                        controller, "_create_committed_export", return_value=identity
                    ), dispatch_patch, cleanup_patch, contextlib.redirect_stdout(
                        stdout
                    ), contextlib.redirect_stderr(stderr):
                        status = controller.main(
                            [
                                "--project-root",
                                str(root),
                                "progress",
                                "--run-id",
                                "private-run",
                                "--authorization-nonce",
                                "/private/nonce",
                            ]
                        )

                    blocked = stdout.getvalue().encode("utf-8")
                    self.assertEqual(status, 2)
                    self.assertEqual(blocked.count(b"\n"), 1)
                    document = json.loads(blocked)
                    self.assertEqual(
                        document,
                        {
                            "blocked_class": "formal_progress_unavailable",
                            "schema": "txnmem-provenance-progress-reader-v1",
                            "status": "blocked",
                        },
                    )
                    self.assertEqual(
                        blocked,
                        json.dumps(
                            document,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        + b"\n",
                    )
                    self.assertNotIn(raw_failure, stdout.getvalue())
                    self.assertEqual(stderr.getvalue(), "")

    def test_mixed_installer_generation_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository, approved = self._approved_repository(root)
            installed = root / "txnmem_formal_controller.py"
            installed.write_text("# interrupted replacement\n", encoding="utf-8")
            installed_reader = root / "read_formal_provenance_progress.sh"
            installed_reader.write_bytes(
                (repository / "scripts/read_formal_provenance_progress.sh").read_bytes()
            )
            approval_path = root / "approved_source_manifest.json"
            approval_path.write_bytes(
                controller._canonical_json_bytes(approved.manifest) + b"\n"
            )

            def protected(path, *, executable=False):
                return Path(path).resolve(strict=True)

            with patch.object(
                controller, "CONTROLLER_INSTALL_PATH", installed
            ), patch.object(
                controller, "PROGRESS_READER_INSTALL_PATH", installed_reader
            ), patch.object(
                controller, "APPROVAL_MANIFEST_PATH", approval_path
            ), patch.object(
                controller, "__file__", str(installed)
            ), patch.object(
                controller, "_require_protected_file", side_effect=protected
            ):
                with self.assertRaisesRegex(
                    controller.FormalControllerError, "differs"
                ):
                    controller._verify_installed_controller(repository)

    def test_installed_progress_reader_must_match_the_approved_committed_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository, approved = self._approved_repository(root)
            installed = root / "txnmem_formal_controller.py"
            installed.write_bytes(
                (repository / "src/txnmem_formal_controller.py").read_bytes()
            )
            installed_reader = root / "read_formal_provenance_progress.sh"
            installed_reader.write_text("#!/bin/sh\n# mutable replacement\n", encoding="utf-8")
            approval_path = root / "approved_source_manifest.json"
            approval_path.write_bytes(
                controller._canonical_json_bytes(approved.manifest) + b"\n"
            )

            def protected(path, *, executable=False):
                return Path(path).resolve(strict=True)

            with patch.object(
                controller, "CONTROLLER_INSTALL_PATH", installed
            ), patch.object(
                controller, "PROGRESS_READER_INSTALL_PATH", installed_reader
            ), patch.object(
                controller, "APPROVAL_MANIFEST_PATH", approval_path
            ), patch.object(
                controller, "__file__", str(installed)
            ), patch.object(
                controller, "_require_protected_file", side_effect=protected
            ):
                with self.assertRaisesRegex(
                    controller.FormalControllerError, "progress reader differs"
                ):
                    controller._verify_installed_controller(repository)

    def test_main_preserves_dispatch_failure_when_cleanup_also_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            export = root / "source-fixture"
            export.mkdir()
            identity = self._identity(export, root)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            stderr = io.StringIO()
            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", side_effect=ValueError("primary")
            ), patch.object(
                controller, "_remove_export", side_effect=OSError("cleanup")
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    ["--project-root", str(root), "measure"]
                )

            self.assertEqual(status, 2)
            self.assertIn("ValueError", stderr.getvalue())
            self.assertNotIn("OSError", stderr.getvalue())

    def test_main_removes_successful_smoke_report_when_export_cleanup_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir()
            export = bootstrap / "source-fixture"
            export.mkdir()
            report = Path(tmp).resolve() / "reports" / "smoke.json"
            report.parent.mkdir()
            identity = self._identity(export, bootstrap)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )

            def dispatch(action, arguments, *_args):
                self.assertEqual(action, "smoke")
                self.assertEqual(arguments, ["--out", str(report)])
                report.write_text('{"schema":"success"}\n', encoding="utf-8")
                return 0

            stderr = io.StringIO()
            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", side_effect=dispatch
            ), patch.object(
                controller, "_remove_export", side_effect=OSError("cleanup")
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    ["--project-root", str(root), "smoke", "--out", str(report)]
                )

            self.assertEqual(status, 2)
            self.assertFalse(report.exists())
            self.assertIn("OSError", stderr.getvalue())

    def test_main_removes_successful_smoke_report_with_equals_output_form(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir()
            export = bootstrap / "source-fixture"
            export.mkdir()
            report = Path(tmp).resolve() / "reports" / "smoke.json"
            report.parent.mkdir()
            identity = self._identity(export, bootstrap)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )

            def dispatch(action, arguments, *_args):
                self.assertEqual(action, "smoke")
                self.assertIn(arguments, (["--out", str(report)], [f"--out={report}"]))
                report.write_text('{"schema":"success"}\n', encoding="utf-8")
                return 0

            stderr = io.StringIO()
            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", side_effect=dispatch
            ), patch.object(
                controller, "_remove_export", side_effect=OSError("cleanup")
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    ["--project-root", str(root), "smoke", f"--out={report}"]
                )

            self.assertEqual(status, 2)
            self.assertFalse(report.exists())
            self.assertIn("OSError", stderr.getvalue())

    def test_main_rejects_abbreviated_smoke_output_before_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir()
            export = bootstrap / "source-fixture"
            export.mkdir()
            report = Path(tmp).resolve() / "reports" / "smoke.json"
            report.parent.mkdir()
            identity = self._identity(export, bootstrap)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            dispatch_calls: list[tuple[str, list[str]]] = []

            def dispatch(action, arguments, *_args):
                dispatch_calls.append((action, list(arguments)))
                report.write_text('{"schema":"success"}\n', encoding="utf-8")
                return 0

            stderr = io.StringIO()
            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", side_effect=dispatch
            ), patch.object(
                controller, "_remove_export"
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    ["--project-root", str(root), "smoke", "--ou", str(report)]
                )

            self.assertEqual(status, 2)
            self.assertEqual(dispatch_calls, [])
            self.assertFalse(report.exists())

    def test_main_rejects_repeated_mixed_smoke_output_before_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            bootstrap = Path(tmp).resolve() / "bootstrap"
            bootstrap.mkdir()
            export = bootstrap / "source-fixture"
            export.mkdir()
            first = Path(tmp).resolve() / "reports" / "first.json"
            second = Path(tmp).resolve() / "reports" / "second.json"
            first.parent.mkdir()
            identity = self._identity(export, bootstrap)
            approved = controller._ApprovedSource(
                commit="a" * 40,
                files=(),
                manifest={},
                manifest_sha256="b" * 64,
            )
            dispatch_calls: list[tuple[str, list[str]]] = []

            def dispatch(action, arguments, *_args):
                dispatch_calls.append((action, list(arguments)))
                second.write_text('{"schema":"success"}\n', encoding="utf-8")
                return 0

            stderr = io.StringIO()
            with patch.object(controller.os, "geteuid", return_value=0), patch.object(
                controller, "_verify_installed_controller", return_value=approved
            ), patch.object(
                controller, "_create_committed_export", return_value=identity
            ), patch.object(
                controller, "_dispatch", side_effect=dispatch
            ), patch.object(
                controller, "_remove_export"
            ), contextlib.redirect_stderr(stderr):
                status = controller.main(
                    [
                        "--project-root",
                        str(root),
                        "smoke",
                        "--out",
                        str(first),
                        f"--out={second}",
                    ]
                )

            self.assertEqual(status, 2)
            self.assertEqual(dispatch_calls, [])
            self.assertFalse(first.exists())
            self.assertFalse(second.exists())

    def test_smoke_report_invalidation_rejects_relative_output_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "repository"
            root.mkdir()
            with self.assertRaisesRegex(
                controller.FormalControllerError, "invalid"
            ):
                controller._smoke_output_path(["--out", "smoke.json"], root)

    def test_repository_installer_builds_the_root_protected_formal_boundary(self):
        root = Path(__file__).resolve().parents[1]
        installer = root / "scripts" / "install_formal_provenance_runtime.sh"
        text = installer.read_text(encoding="utf-8")

        self.assertIn('[[ "${EUID}" -ne 0 ]]', text)
        self.assertIn("/opt/txnmem-formal-controller", text)
        self.assertIn("runtime_root=/opt/txnmem-formal-runtime", text)
        self.assertIn('wheel_dir="$runtime_root/wheels"', text)
        self.assertIn("/var/lib/txnmem-formal/runs", text)
        self.assertIn("65532", text)
        self.assertIn("provenance_runtime_lock.json", text)
        self.assertIn("sha256", text)
        self.assertIn("/usr/bin/docker", text)
        self.assertIn("/usr/sbin/nft", text)
        self.assertIn("-m 0555", text)
        self.assertIn("exec /usr/bin/env -i", text)
        self.assertIn("APPROVED_COMMIT", text)
        self.assertIn("approved_source_manifest.json", text)
        self.assertIn("running installer differs from the approved Git blob", text)
        self.assertIn("/usr/bin/mv -f", text)
        self.assertIn('"scripts/run_formal_provenance_smoke.sh"', text)
        self.assertIn('"src/txnmem_formal_smoke.py"', text)
        self.assertIn('"src/txnmem_provenance_progress.py"', text)
        self.assertNotIn("sshpass", text)
        self.assertNotIn("StrictHostKeyChecking=no", text)

        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "hostile-path-used"
            fake_uname = Path(tmp) / "uname"
            fake_uname.write_text(
                f"#!/bin/sh\n/usr/bin/touch {marker}\n", encoding="utf-8"
            )
            fake_uname.chmod(0o755)
            environment = dict(os.environ)
            environment["PATH"] = f"{tmp}:{environment.get('PATH', '')}"
            subprocess.run(
                ["/bin/bash", str(installer)],
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
