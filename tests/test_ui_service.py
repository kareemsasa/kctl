from __future__ import annotations

import io
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.cli import main
from kctl_pkg.ui_service import render_dashboard_service
from tests.test_ui_index import init_git_repo


class UIServiceTests(unittest.TestCase):
    def test_render_dashboard_service_includes_tailscale_and_announce_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            unit_text = render_dashboard_service(
                repo_path=repo_path,
                host="0.0.0.0",
                port=8421,
                tailscale=True,
                announce_url="http://erebus.tail172bcd.ts.net:8421",
                db_path=None,
                python_executable="/usr/bin/python3",
            )

            self.assertIn("ExecStart=", unit_text)
            self.assertIn("ui dashboard", unit_text)
            self.assertIn("--tailscale", unit_text)
            self.assertIn("--announce-url", unit_text)
            self.assertIn("erebus.tail172bcd.ts.net:8421", unit_text)

    def test_cli_ui_service_print_outputs_unit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                exit_code = main(["ui", "service", "print", str(repo_path), "--announce-url", "http://erebus.tail172bcd.ts.net:8421"])

            self.assertEqual(exit_code, 0)
            output = buffer.getvalue()
            self.assertIn("[Service]", output)
            self.assertIn("--announce-url", output)

    def test_cli_ui_service_install_writes_unit_and_runs_systemctl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            service_path = Path(tmpdir) / "kctl-dashboard.service"
            calls: list[tuple[str, ...]] = []

            def fake_systemctl_user(*args: str) -> subprocess.CompletedProcess[str]:
                calls.append(args)
                return subprocess.CompletedProcess(["systemctl", "--user", *args], 0, "", "")

            with patch("kctl_pkg.cli.default_service_path", return_value=service_path), patch(
                "kctl_pkg.cli.run_systemctl_user", side_effect=fake_systemctl_user
            ):
                exit_code = main(
                    [
                        "ui",
                        "service",
                        "install",
                        str(repo_path),
                        "--announce-url",
                        "http://erebus.tail172bcd.ts.net:8421",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue(service_path.exists())
            unit_text = service_path.read_text()
            self.assertIn("--tailscale", unit_text)
            self.assertIn("erebus.tail172bcd.ts.net:8421", unit_text)
            self.assertEqual(calls, [("daemon-reload",), ("enable", "kctl-dashboard.service"), ("restart", "kctl-dashboard.service")])


if __name__ == "__main__":
    unittest.main()
