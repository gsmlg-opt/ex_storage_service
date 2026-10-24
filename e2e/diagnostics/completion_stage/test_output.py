import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from retain_output import retain


class DiagnosticOutputTest(unittest.TestCase):
    def test_mask_command_and_later_value_are_excluded_from_retained_bytes(self):
        source = io.StringIO("setup\n::add-mask::fixture-secret\nresult fixture-secret\n")
        output, commands = io.StringIO(), io.StringIO()
        retain(source, output, commands)
        self.assertEqual(output.getvalue(), "setup\nresult [REDACTED]\n")
        self.assertEqual(commands.getvalue(), "::add-mask::fixture-secret\n")

    def test_invalid_mask_fails_without_retaining_it(self):
        output = io.StringIO()
        with self.assertRaisesRegex(ValueError, "invalid diagnostic mask command"):
            retain(io.StringIO("::add-mask::\n"), output, io.StringIO())
        self.assertEqual(output.getvalue(), "")

    def test_cli_creates_private_log_and_rejects_overwrite(self):
        script = Path(__file__).with_name("retain_output.py")
        with tempfile.TemporaryDirectory(prefix="ess-completion-output-") as root:
            path = Path(root) / "workload.log"
            result = subprocess.run([sys.executable, "-B", str(script), str(path)],
                                    input="normal\n::add-mask::fixture-secret\n",
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(path.read_text(), "normal\n")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            second = subprocess.run([sys.executable, "-B", str(script), str(path)],
                                    input="replacement\n", text=True, capture_output=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual(path.read_text(), "normal\n")


if __name__ == "__main__":
    unittest.main()
