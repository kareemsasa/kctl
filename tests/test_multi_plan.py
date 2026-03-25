from __future__ import annotations

import json
import subprocess
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.multi import discover_plan_files, run_many_plans
from kctl_pkg.runner import execute_plan_run
from kctl_pkg.types import CommandResult


def run_checked(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=str(cwd), check=True, capture_output=True, text=True)


def init_git_repo(repo_path: Path) -> None:
    repo_path.mkdir(parents=True, exist_ok=True)
    run_checked(["git", "init"], repo_path)
    run_checked(["git", "config", "user.name", "Test User"], repo_path)
    run_checked(["git", "config", "user.email", "test@example.com"], repo_path)
    (repo_path / "README.md").write_text("hello\n")
    run_checked(["git", "add", "README.md"], repo_path)
    run_checked(["git", "commit", "-m", "init"], repo_path)


class MultiPlanTests(unittest.TestCase):
    def test_discover_plan_files_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir)
            (plans_dir / "b-second.yaml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")
            (plans_dir / "a-first.yml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")
            discovered = discover_plan_files(plans_dir)
            self.assertEqual([path.name for path in discovered], ["a-first.yml", "b-second.yaml"])

    def test_execute_plan_run_verify_step_is_handled_by_kctl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "verify.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: verify only
                    steps:
                      - id: verify
                        kind: verify
                        commands:
                          - printf ok
                    """
                ).strip()
                + "\n"
            )

            with patch("kctl_pkg.runner.run_streaming_command", side_effect=AssertionError("agent should not run")):
                run_data = execute_plan_run(
                    plan_path=plan_path,
                    verbose=False,
                    approve_each_step=False,
                    branch=None,
                    commit=False,
                    commit_message=None,
                    allow_dirty_start=False,
                    review_enabled=False,
                    interactive=False,
                )

            step = run_data["steps"][0]
            self.assertEqual(step["codex"]["command"], [])
            self.assertEqual(step["verify"]["exit_code"], 0)
            self.assertEqual(run_data["status"], "success")

    def test_execute_plan_run_passes_persisted_artifact_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "artifact.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: test artifacts
                    steps:
                      - id: inspect
                        kind: agent
                        prompt: Inspect
                      - id: plan
                        kind: agent
                        prompt: Plan
                    """
                ).strip()
                + "\n"
            )

            prompts: list[str] = []

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                prompt = command[-1]
                prompts.append(prompt)
                if "Current step id: inspect" in prompt:
                    stdout = (
                        "done\n```json\n"
                        + json.dumps(
                            {
                                "project_type": "app",
                                "stack": ["py"],
                                "summary": "sum",
                                "key_directories": [{"path": "src", "purpose": "code"}],
                                "key_files": [{"path": "README.md", "purpose": "docs"}],
                                "relevant_areas": [{"path": "src", "reason": "logic"}],
                                "constraints": [{"path": "src", "note": "keep stable"}],
                                "assumptions": ["a"],
                                "unknowns": ["u"],
                            }
                        )
                        + "\n```\n"
                    )
                else:
                    stdout = (
                        "done\n```json\n"
                        + json.dumps(
                            {
                                "objective": "obj",
                                "approach": "approach",
                                "steps": [{"id": "implement", "name": "impl", "files": ["x"], "intent": "y"}],
                                "verification": {"commands": ["printf ok"], "manual_checks": []},
                                "risks": ["r"],
                                "out_of_scope": ["o"],
                            }
                        )
                        + "\n```\n"
                    )
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout=stdout, stderr="")

            with patch("kctl_pkg.runner.run_streaming_command", side_effect=fake_streaming_command):
                run_data = execute_plan_run(
                    plan_path=plan_path,
                    verbose=False,
                    approve_each_step=False,
                    branch=None,
                    commit=False,
                    commit_message=None,
                    allow_dirty_start=False,
                    review_enabled=False,
                    interactive=False,
                )

            self.assertEqual(run_data["status"], "success")
            self.assertIn("Structured inspect artifact:", prompts[1])

    def test_run_many_plans_respects_concurrency_and_writes_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            for index in range(3):
                (plans_dir / f"{index + 1:03d}-plan.yaml").write_text(
                    textwrap.dedent(
                        f"""
                        repo: {repo_path}
                        objective: plan {index}
                        steps:
                          - id: inspect
                            prompt: Inspect {index}
                        """
                    ).strip()
                    + "\n"
                )

            active = 0
            max_active = 0
            active_lock = threading.Lock()

            def fake_create_workspace(repo_root: Path, workspace_path: Path, branch_name: str) -> Path:
                workspace_path.mkdir(parents=True, exist_ok=True)
                return workspace_path

            def fake_execute_plan_run(**kwargs):
                nonlocal active, max_active
                with active_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.1)
                with active_lock:
                    active -= 1
                run_output_dir = kwargs["run_output_dir_override"]
                run_output_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "status": "success",
                    "steps": [
                        {
                            "id": "inspect",
                            "status": "success",
                            "verify": None,
                            "changed_files_count": 0,
                        }
                    ],
                    "log_path": str(run_output_dir / "run.json"),
                }

            with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=fake_create_workspace), patch(
                "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
            ):
                exit_code = run_many_plans(plans_dir, concurrency=2, verbose=False)

            self.assertEqual(exit_code, 0)
            self.assertLessEqual(max_active, 2)
            run_logs = sorted((repo_path / ".kctl" / "runs").glob("*/run.json"))
            self.assertTrue(run_logs)
            run_state = json.loads(run_logs[-1].read_text())
            self.assertEqual(run_state["status"], "passed")
            self.assertEqual(len(run_state["plans"]), 3)


if __name__ == "__main__":
    unittest.main()
