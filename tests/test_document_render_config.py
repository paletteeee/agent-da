from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DocumentRenderConfigTests(unittest.TestCase):
    def test_macos_renderer_uses_explicit_cjk_fontconfig(self):
        script = (ROOT / "scripts/render_docx_with_bundled_libs.sh").read_text(
            encoding="utf-8"
        )
        config_path = ROOT / "scripts/fontconfig-macos.conf"

        self.assertIn("FONTCONFIG_FILE", script)
        self.assertTrue(config_path.is_file())
        config = config_path.read_text(encoding="utf-8")
        self.assertIn("/System/Library/Fonts", config)
        self.assertIn("/System/Library/Fonts/Supplemental", config)
        self.assertIn("/private/tmp/txnmem-fontconfig-cache", config)


if __name__ == "__main__":
    unittest.main()
