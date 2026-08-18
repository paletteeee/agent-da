from pathlib import Path
import re
import unittest


class RemoteSetupScriptTests(unittest.TestCase):
    def setUp(self):
        self.script = (
            Path(__file__).resolve().parents[1] / "scripts" / "setup_remote_deps.sh"
        ).read_text(encoding="utf-8")

    def test_bundle_paths_are_not_left_as_unexpanded_shell_variables(self):
        self.assertNotRegex(self.script, r'Path\("\$BENCH_ROOT')
        self.assertNotRegex(self.script, r'\(\s*"\$BENCH_ROOT/appworld')

    def test_data_bundle_unpacker_imports_sys_before_changing_path(self):
        self.assertRegex(self.script, r"import io, os, sys, zipfile")

    def test_venv_creation_accepts_an_existing_python_runtime(self):
        self.assertRegex(self.script, r'PYTHON_BIN=.*locomo-agent/bin/python')
        self.assertIn('"$PYTHON_BIN" -m venv "$VENV"', self.script)

    def test_appworld_bundle_download_has_a_timeout_and_reuses_partial_file(self):
        self.assertIn('DATA_BUNDLE="$BENCH_ROOT/appworld-data/data-0.2.0.bundle"', self.script)
        self.assertIn('if [ ! -s "$DATA_BUNDLE" ]; then', self.script)
        self.assertIn('--max-time', self.script)


if __name__ == "__main__":
    unittest.main()
