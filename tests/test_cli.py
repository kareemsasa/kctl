from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.cli import main
from kctl_pkg.types import PlanError
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

    def test_cli_run_passes_partial_step_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "plan.yaml"
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n"
            )

            with patch("kctl_pkg.cli.run_plan", return_value=0) as run_plan_mock:
                exit_code = main(["run", str(plan_path), "--from-step", "inspect"])

            self.assertEqual(exit_code, 0)
            self.assertEqual(run_plan_mock.call_args.kwargs["from_step"], "inspect")
            self.assertIsNone(run_plan_mock.call_args.kwargs["only_step"])

    def test_cli_init_delegates_to_init_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "plan.yaml"

            with patch("kctl_pkg.cli.init_plan", return_value=0) as init_plan_mock:
                exit_code = main(
                    [
                        "init",
                        "tooling_change",
                        str(output_path),
                        "--repo",
                        "/tmp/repo",
                        "--objective",
                        "Improve tooling",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(init_plan_mock.call_args.kwargs["template_name"], "tooling_change")
            self.assertEqual(init_plan_mock.call_args.kwargs["output_path"], output_path.resolve())

    def test_cli_plans_status_delegates_to_status_printer(self) -> None:
        with patch("kctl_pkg.cli.print_run_status", return_value=0) as print_status_mock:
            exit_code = main(["plans", "status", "run-123"])

        self.assertEqual(exit_code, 0)
        print_status_mock.assert_called_once_with("run-123")

    def test_cli_ui_index_prints_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            with patch(
                "kctl_pkg.cli.index_repository_state",
                return_value={"runs": 2, "plan_executions": 3, "step_executions": 4, "workspaces": 5},
            ):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = main(["ui", "index", str(repo_path)])

            self.assertEqual(exit_code, 0)
            self.assertIn("Indexed: 2 runs  3 plans  4 steps  5 workspaces", buffer.getvalue())

    def test_cli_ui_detail_commands_delegate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            with patch("kctl_pkg.cli.print_ui_runs", return_value=0) as runs_mock:
                self.assertEqual(main(["ui", "runs", str(repo_path)]), 0)
                runs_mock.assert_called_once()

            with patch("kctl_pkg.cli.print_ui_run_detail", return_value=0) as run_mock:
                self.assertEqual(main(["ui", "run", str(repo_path), "run-1"]), 0)
                run_mock.assert_called_once()

            with patch("kctl_pkg.cli.print_ui_workspaces", return_value=0) as workspaces_mock:
                self.assertEqual(main(["ui", "workspaces", str(repo_path)]), 0)
                workspaces_mock.assert_called_once()

    def test_cli_ui_dashboard_delegates_with_tailscale_host_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            with patch("kctl_pkg.cli.serve_dashboard", return_value=0) as serve_dashboard_mock:
                exit_code = main(["ui", "dashboard", str(repo_path), "--tailscale"])

            self.assertEqual(exit_code, 0)
            self.assertEqual(serve_dashboard_mock.call_args.kwargs["host"], "0.0.0.0")
            self.assertTrue(serve_dashboard_mock.call_args.kwargs["tailscale"])

    def test_cli_ui_service_install_honors_no_enable_and_no_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            service_path = Path(tmpdir) / "kctl-dashboard.service"
            init_git_repo(repo_path)
            calls: list[tuple[str, ...]] = []

            def fake_systemctl(*args: str) -> object:
                calls.append(args)
                return object()

            with patch("kctl_pkg.cli.default_service_path", return_value=service_path), patch(
                "kctl_pkg.cli.run_systemctl_user",
                side_effect=fake_systemctl,
            ), patch("kctl_pkg.cli.ensure_systemctl_success"), patch(
                "kctl_pkg.cli.install_dashboard_service"
            ), patch("kctl_pkg.cli.render_dashboard_service", return_value="[Unit]\n"):
                exit_code = main(
                    [
                        "ui",
                        "service",
                        "install",
                        str(repo_path),
                        "--no-enable",
                        "--no-start",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(calls, [("daemon-reload",)])

    def test_cli_batch_grouped_and_quiet_modes(self) -> None:
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
            ), patch("kctl_pkg.cli.run_plan", return_value=0):
                grouped = io.StringIO()
                with redirect_stdout(grouped):
                    self.assertEqual(main(["batch", str(plan_path), "--root", str(root_path), "--output-mode", "grouped"]), 0)
                self.assertIn("== repo-a ==", grouped.getvalue())

                quiet = io.StringIO()
                with redirect_stdout(quiet):
                    self.assertEqual(main(["batch", str(plan_path), "--root", str(root_path), "--output-mode", "quiet"]), 0)
                self.assertIn("repo-a: exit 0", quiet.getvalue())

    def test_cli_error_paths_return_exit_2(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "plan.yaml"
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n"
            )

            stderr = io.StringIO()
            with redirect_stdout(io.StringIO()), patch("sys.stderr", stderr), patch(
                "kctl_pkg.cli.run_plan", side_effect=PlanError("bad run")
            ):
                self.assertEqual(main(["run", str(plan_path)]), 2)
            self.assertIn("Error: bad run", stderr.getvalue())

            stderr = io.StringIO()
            with patch("sys.stderr", stderr):
                self.assertEqual(main(["batch", str(plan_path), "--root", str(repo_path), "--approve-each-step"]), 2)
            self.assertIn("interactive prompts are not supported in batch mode", stderr.getvalue())

            stderr = io.StringIO()
            with patch("sys.stderr", stderr), patch("kctl_pkg.cli.run_many_plans", side_effect=PlanError("bad many")):
                self.assertEqual(main(["plans", "run-many", str(repo_path)]), 2)
            self.assertIn("Error: bad many", stderr.getvalue())

            stderr = io.StringIO()
            with patch("sys.stderr", stderr), patch("kctl_pkg.cli.index_repository_state", side_effect=PlanError("bad ui")):
                self.assertEqual(main(["ui", "index", str(repo_path)]), 2)
            self.assertIn("Error: bad ui", stderr.getvalue())

            stderr = io.StringIO()
            with patch("sys.stderr", stderr), patch("kctl_pkg.cli.init_plan", side_effect=PlanError("bad init")):
                self.assertEqual(
                    main(["init", "tooling_change", str(Path(tmpdir) / "out.yaml"), "--repo", str(repo_path), "--objective", "x"]),
                    2,
                )
            self.assertIn("Error: bad init", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
