from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from kctl_pkg.process import run_command, run_streaming_command


class ProcessTests(unittest.TestCase):
    def test_run_command_captures_stdout_stderr_and_callbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)
            started: list[int] = []
            finished: list[int] = []

            result = run_command(
                ["python3", "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
                cwd=cwd,
                process_started=started.append,
                process_finished=finished.append,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, "out\n")
            self.assertEqual(result.stderr, "err\n")
            self.assertFalse(result.stopped)
            self.assertEqual(len(started), 1)
            self.assertEqual(started, finished)

    def test_run_command_writes_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)

            result = run_command(
                ["python3", "-c", "print(input().upper())"],
                cwd=cwd,
                stdin_text="hello",
            )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, "HELLO\n")

    def test_run_command_honors_stop_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)

            result = run_command(
                ["python3", "-c", "import time; time.sleep(5)"],
                cwd=cwd,
                stop_requested=lambda: True,
            )

            self.assertTrue(result.stopped)
            self.assertNotEqual(result.exit_code, 0)

    def test_run_streaming_command_captures_streams(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)

            result = run_streaming_command(
                [
                    "python3",
                    "-c",
                    textwrap.dedent(
                        """
                        import sys
                        print("out-1")
                        print("err-1", file=sys.stderr)
                        """
                    ),
                ],
                cwd=cwd,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("out-1\n", result.stdout)
            self.assertIn("err-1\n", result.stderr)
            self.assertFalse(result.stopped)

    def test_run_streaming_command_filters_hidden_and_duplicate_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)
            output_path = cwd / "captured.txt"

            result = run_streaming_command(
                [
                    "python3",
                    "-c",
                    textwrap.dedent(
                        """
                        print("hide-me")
                        print("repeat")
                        print("repeat")
                        """
                    ),
                ],
                cwd=cwd,
                filter_stream=True,
                hidden_lines={"hide-me"},
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("hide-me\n", result.stdout)
            self.assertIn("repeat\nrepeat\n", result.stdout)
            self.assertFalse(result.stopped)

    def test_run_streaming_command_honors_stop_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path(tmpdir)
            started: list[int] = []
            finished: list[int] = []

            result = run_streaming_command(
                ["python3", "-c", "import time; print('start'); time.sleep(5)"],
                cwd=cwd,
                stop_requested=lambda: True,
                process_started=started.append,
                process_finished=finished.append,
            )

            self.assertTrue(result.stopped)
            self.assertNotEqual(result.exit_code, 0)
            self.assertEqual(len(started), 1)
            self.assertEqual(started, finished)


if __name__ == "__main__":
    unittest.main()
