from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.cli import main
from tests.test_ui_index import init_git_repo


class CLITests(unittest.TestCase):
    def test_cli_run_passes_provider_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "plan.yaml"
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n"
            )

            with patch("kctl_pkg.cli.run_plan", return_value=0) as run_plan_mock:
                exit_code = main(["run", str(plan_path), "--provider", "claude"])

            self.assertEqual(exit_code, 0)
            run_plan_mock.assert_called_once()
            self.assertEqual(run_plan_mock.call_args.kwargs["provider_override"], "claude")

    def test_cli_batch_passes_provider_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root_path = Path(tmpdir) / "repos"
            repo_path = root_path / "repo-a"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "plan.yaml"
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n"
            )

            with patch("kctl_pkg.cli.resolve_plan_path", return_value=plan_path), patch(
                "kctl_pkg.cli.discover_git_repos",
                return_value=[repo_path],
            ), patch("kctl_pkg.cli.run_plan", return_value=0) as run_plan_mock:
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = main(
                        [
                            "batch",
                            str(plan_path),
                            "--root",
                            str(root_path),
                            "--provider",
                            "claude",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertIn("repo-a: exit 0", buffer.getvalue())
            self.assertEqual(run_plan_mock.call_args.kwargs["provider_override"], "claude")

    def test_cli_plans_run_many_passes_provider_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()

            with patch("kctl_pkg.cli.run_many_plans", return_value=0) as run_many_mock:
                exit_code = main(
                    [
                        "plans",
                        "run-many",
                        str(plans_dir),
                        "--provider",
                        "claude",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run_many_mock.assert_called_once()
            self.assertEqual(run_many_mock.call_args.kwargs["provider_override"], "claude")


if __name__ == "__main__":
    unittest.main()
