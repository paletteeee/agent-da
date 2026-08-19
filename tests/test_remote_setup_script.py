from pathlib import Path
import re
import unittest


class RemoteSetupScriptTests(unittest.TestCase):
    def setUp(self):
        self.script = (
            Path(__file__).resolve().parents[1] / "scripts" / "setup_remote_deps.sh"
        ).read_text(encoding="utf-8")
        self.locomo_script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "bootstrap_locomo_agent.sh"
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

    def test_locomo_evaluator_source_is_commit_and_hash_pinned(self):
        self.assertIn("3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376", self.locomo_script)
        self.assertIn(
            "8e3be5d57ff2ff9ec5cd05939592f468c5f3f1fd95d13e431932bdf6bf0fd6fd",
            self.locomo_script,
        )
        self.assertIn("codeload.github.com/snap-research/locomo", self.locomo_script)
        self.assertNotIn("git clone --depth 1", self.locomo_script)

    def test_locomo_evaluator_installs_only_lightweight_target_dependencies(self):
        self.assertIn("--target", self.locomo_script)
        self.assertIn("--no-deps", self.locomo_script)
        self.assertIn("bert-score==0.3.13", self.locomo_script)
        self.assertNotRegex(self.locomo_script, r"pip install[^\n]*torch")

    def test_locomo_evaluator_rejects_importable_but_unpinned_packages(self):
        self.assertIn("verify_pinned_packages", self.locomo_script)
        self.assertGreaterEqual(self.locomo_script.count("verify_pinned_packages"), 3)
        self.assertIn('"bert-score": "0.3.13"', self.locomo_script)
        self.assertIn("distribution(name).locate_file", self.locomo_script)

    def test_locomo_evaluator_setup_runs_official_f1_smoke(self):
        self.assertIn("from task_eval.evaluation import eval_question_answering", self.locomo_script)
        self.assertIn("assert float(result[0][0]) == 1.0", self.locomo_script)


if __name__ == "__main__":
    unittest.main()
