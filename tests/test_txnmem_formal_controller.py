import contextlib
import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import txnmem_formal_controller as controller


class FormalControllerCleanupTests(unittest.TestCase):
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

    def test_mixed_installer_generation_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository, approved = self._approved_repository(root)
            installed = root / "txnmem_formal_controller.py"
            installed.write_text("# interrupted replacement\n", encoding="utf-8")
            approval_path = root / "approved_source_manifest.json"
            approval_path.write_bytes(
                controller._canonical_json_bytes(approved.manifest) + b"\n"
            )

            def protected(path, *, executable=False):
                return Path(path).resolve(strict=True)

            with patch.object(
                controller, "CONTROLLER_INSTALL_PATH", installed
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
