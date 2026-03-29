from __future__ import annotations

import json
import os
import subprocess
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.artifacts import single_run_dir
from kctl_pkg.multi import discover_plan_files, run_many_plans
from kctl_pkg.plan import normalize_plan
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
            self.assertEqual(step["step_type"]["effective_type"], "verify")
            self.assertEqual(step["step_type"]["source"], "inferred")
            self.assertEqual(run_data["status"], "success")

    def test_execute_plan_run_explicit_verify_type_is_handled_by_kctl(self) -> None:
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
                      - id: validate
                        type: verify
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
            self.assertEqual(step["step_type"]["effective_type"], "verify")
            self.assertEqual(step["step_type"]["source"], "explicit")
            self.assertEqual(run_data["status"], "success")

    def test_normalize_plan_records_explicit_and_inferred_step_types(self) -> None:
        plan = {
            "repo": "/tmp/repo",
            "objective": "x",
            "steps": [
                {"id": "inspect", "prompt": "Inspect"},
                {"id": "validate", "type": "verify", "commands": ["printf ok"]},
                {"id": "implement", "prompt": "Implement"},
            ],
        }

        normalized = normalize_plan(plan)

        self.assertEqual(normalized["steps"][0]["_kctl_step_type"]["effective_type"], "analyze")
        self.assertEqual(normalized["steps"][0]["_kctl_step_type"]["source"], "inferred")
        self.assertEqual(normalized["steps"][1]["_kctl_step_type"]["effective_type"], "verify")
        self.assertEqual(normalized["steps"][1]["_kctl_step_type"]["source"], "explicit")
        self.assertEqual(normalized["steps"][2]["_kctl_step_type"]["effective_type"], "change")
        self.assertEqual(normalized["steps"][2]["_kctl_step_type"]["source"], "inferred")
        self.assertEqual(normalized["steps"][0]["_kctl_output"]["effective_schema"], "inspect_v1")
        self.assertEqual(normalized["steps"][0]["_kctl_output"]["source"], "inferred")
        self.assertIsNone(normalized["steps"][2]["_kctl_output"]["effective_schema"])
        self.assertEqual(normalized["steps"][0]["_kctl_mode"]["effective_mode"], "default")
        self.assertEqual(normalized["steps"][1]["_kctl_verify"]["effective_mode"], "legacy")
        self.assertEqual(normalized["steps"][1]["_kctl_verify"]["source"], "inferred")

    def test_execute_plan_run_explicit_read_only_mode_fails_on_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "readonly.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: read only
                    steps:
                      - id: inspect-state
                        type: analyze
                        mode: read-only
                        prompt: Inspect
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                (repo_path / "notes.txt").write_text("changed\n")
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="done\n", stderr="")

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

            step = run_data["steps"][0]
            self.assertEqual(step["status"], "failure")
            self.assertEqual(step["failure_reason"], "expected_clean_diff")
            self.assertEqual(step["mode"]["effective_mode"], "read-only")
            self.assertEqual(step["mode"]["source"], "explicit")

    def test_execute_plan_run_expect_clean_diff_maps_to_inferred_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "readonly.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: legacy read only
                    steps:
                      - id: inspect
                        prompt: Inspect
                        expect_clean_diff: true
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                stdout = (
                    "done\n```json\n"
                    + json.dumps(
                        {
                            "project_type": "app",
                            "stack": ["py"],
                            "summary": "sum",
                            "key_directories": [{"path": ".", "purpose": "root"}],
                            "key_files": [{"path": "README.md", "purpose": "docs"}],
                            "relevant_areas": [{"path": "README.md", "reason": "docs"}],
                            "constraints": [{"path": "README.md", "note": "keep stable"}],
                            "assumptions": ["a"],
                            "unknowns": ["u"],
                        }
                    )
                    + "\n```\n"
                )
                (repo_path / "notes.txt").write_text("changed\n")
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

            step = run_data["steps"][0]
            self.assertEqual(step["status"], "failure")
            self.assertEqual(step["mode"]["effective_mode"], "read-only")
            self.assertEqual(step["mode"]["source"], "inferred")

    def test_normalize_plan_records_explicit_and_default_verify_modes(self) -> None:
        plan = {
            "repo": "/tmp/repo",
            "objective": "x",
            "defaults": {"verify_mode": "full"},
            "steps": [
                {"id": "verify", "commands": ["printf ok"]},
                {"id": "validate", "type": "verify", "commands": ["printf ok"], "verify_mode": "legacy"},
            ],
        }

        normalized = normalize_plan(plan)

        self.assertEqual(normalized["steps"][0]["_kctl_verify"]["effective_mode"], "full")
        self.assertEqual(normalized["steps"][0]["_kctl_verify"]["source"], "default")
        self.assertEqual(normalized["steps"][1]["_kctl_verify"]["effective_mode"], "legacy")
        self.assertEqual(normalized["steps"][1]["_kctl_verify"]["source"], "explicit")

    def test_execute_plan_run_external_storage_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            kctl_home = Path(tmpdir) / "kctl-home"
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

            with patch.dict(os.environ, {"KCTL_ARTIFACT_STORAGE": "external", "KCTL_HOME": str(kctl_home)}, clear=False):
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

                run_output_dir = Path(run_data["run_output_dir"])
                self.assertEqual(run_data["artifact_storage_mode"], "external")
                self.assertEqual(run_data["artifact_root_path"], str(run_output_dir.parent))
                self.assertEqual(run_output_dir, single_run_dir(repo_path, run_output_dir.name, storage_mode="external"))
                self.assertTrue(run_output_dir.exists())

    def test_run_many_plans_external_storage_writes_under_kctl_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            kctl_home = Path(tmpdir) / "kctl-home"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            for index in range(2):
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

            def fake_create_workspace(repo_root: Path, workspace_path: Path, branch_name: str) -> Path:
                workspace_path.mkdir(parents=True, exist_ok=True)
                return workspace_path

            def fake_execute_plan_run(**kwargs):
                run_output_dir = kwargs["run_output_dir_override"]
                run_output_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "status": "success",
                    "artifact_storage_mode": "external",
                    "artifact_root_path": str(run_output_dir.parent),
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

            with patch.dict(os.environ, {"KCTL_ARTIFACT_STORAGE": "external", "KCTL_HOME": str(kctl_home)}, clear=False):
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=fake_create_workspace), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
                ):
                    exit_code = run_many_plans(plans_dir, concurrency=2, verbose=False)

                self.assertEqual(exit_code, 0)
                run_logs = sorted((kctl_home / "repos").glob("*/runs/*/run.json"))
                self.assertTrue(run_logs)
                run_state = json.loads(run_logs[-1].read_text())
                self.assertEqual(run_state["artifact_storage_mode"], "external")
                self.assertEqual(Path(run_state["artifact_root_path"]).resolve(), Path(run_logs[-1]).parent.parent.resolve())
                self.assertTrue(run_state["plans"])
                self.assertEqual(Path(run_state["plans"][0]["run_output_dir"]).resolve().parent, Path(run_logs[-1]).parent.resolve())
                self.assertEqual(
                    Path(run_state["plans"][0]["worktree_path"]).resolve().parents[3],
                    (kctl_home / "repos").resolve(),
                )

    def test_execute_plan_run_explicit_output_schema_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "schema.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: explicit schema
                    steps:
                      - id: survey
                        type: analyze
                        prompt: Survey
                        output:
                          schema: inspect_v1
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
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

            step = run_data["steps"][0]
            self.assertEqual(step["status"], "success")
            self.assertEqual(step["output"]["effective_schema"], "inspect_v1")
            self.assertEqual(step["output"]["source"], "explicit")
            self.assertIn("inspect_v1", step["structured_artifacts"])

    def test_execute_plan_run_review_policy_advisory_does_not_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            (repo_path / "app.py").write_text("print('x')\n")
            run_checked(["git", "add", "app.py"], repo_path)
            run_checked(["git", "commit", "-m", "add app"], repo_path)
            plan_path = Path(tmpdir) / "review.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: advisory review
                    steps:
                      - id: inspect
                        prompt: Inspect
                      - id: check-review
                        type: review
                        prompt: Review
                        review:
                          policy: advisory
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                prompt = command[-1]
                if "Current step id: inspect" in prompt:
                    stdout = (
                        "done\n```json\n"
                        + json.dumps(
                            {
                                "project_type": "app",
                                "stack": ["py"],
                                "summary": "sum",
                                "key_directories": [{"path": ".", "purpose": "root"}],
                                "key_files": [{"path": "app.py", "purpose": "code"}],
                                "relevant_areas": [{"path": "app.py", "reason": "logic"}],
                                "constraints": [{"path": "app.py", "note": "keep stable"}],
                                "assumptions": ["a"],
                                "unknowns": ["u"],
                            }
                        )
                        + "\n```\n"
                    )
                else:
                    (repo_path / "app.py").write_text("print('changed')\n")
                    stdout = "reviewed\n"
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout=stdout, stderr="")

            review_items = [
                {"reviewer": "scope reviewer", "verdict": "concern", "summary": "scope", "findings": ["f1"]},
                {"reviewer": "test reviewer", "verdict": "pass", "summary": "tests", "findings": []},
            ]

            with patch("kctl_pkg.runner.run_streaming_command", side_effect=fake_streaming_command), patch(
                "kctl_pkg.runner.run_step_reviews", return_value=review_items
            ):
                run_data = execute_plan_run(
                    plan_path=plan_path,
                    verbose=False,
                    approve_each_step=False,
                    branch=None,
                    commit=False,
                    commit_message=None,
                    allow_dirty_start=False,
                    review_enabled=True,
                    interactive=False,
                )

            step = run_data["steps"][1]
            self.assertEqual(step["status"], "success")
            self.assertIsNone(step["failure_reason"])
            self.assertEqual(step["review_policy"]["effective_policy"], "advisory")
            self.assertEqual(step["review_policy"]["source"], "explicit")

    def test_execute_plan_run_review_policy_blocking_fails_on_concern(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            (repo_path / "app.py").write_text("print('x')\n")
            run_checked(["git", "add", "app.py"], repo_path)
            run_checked(["git", "commit", "-m", "add app"], repo_path)
            plan_path = Path(tmpdir) / "review.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: blocking review
                    steps:
                      - id: check-review
                        type: review
                        prompt: Review
                        review:
                          policy: blocking
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                (repo_path / "app.py").write_text("print('changed')\n")
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="reviewed\n", stderr="")

            review_items = [
                {"reviewer": "scope reviewer", "verdict": "concern", "summary": "scope", "findings": ["f1"]},
                {"reviewer": "test reviewer", "verdict": "pass", "summary": "tests", "findings": []},
            ]

            with patch("kctl_pkg.runner.run_streaming_command", side_effect=fake_streaming_command), patch(
                "kctl_pkg.runner.run_step_reviews", return_value=review_items
            ):
                run_data = execute_plan_run(
                    plan_path=plan_path,
                    verbose=False,
                    approve_each_step=False,
                    branch=None,
                    commit=False,
                    commit_message=None,
                    allow_dirty_start=False,
                    review_enabled=True,
                    interactive=False,
                )

            step = run_data["steps"][0]
            self.assertEqual(step["status"], "failure")
            self.assertEqual(step["failure_reason"], "review_concern")
            self.assertEqual(step["review_policy"]["effective_policy"], "blocking")

    def test_execute_plan_run_legacy_review_step_infers_manual_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            (repo_path / "app.py").write_text("print('x')\n")
            run_checked(["git", "add", "app.py"], repo_path)
            run_checked(["git", "commit", "-m", "add app"], repo_path)
            plan_path = Path(tmpdir) / "review.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: legacy review
                    steps:
                      - id: review
                        prompt: Review
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                (repo_path / "app.py").write_text("print('changed')\n")
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="reviewed\n", stderr="")

            review_items = [
                {"reviewer": "scope reviewer", "verdict": "concern", "summary": "scope", "findings": ["f1"]},
                {"reviewer": "test reviewer", "verdict": "pass", "summary": "tests", "findings": []},
            ]

            with patch("kctl_pkg.runner.run_streaming_command", side_effect=fake_streaming_command), patch(
                "kctl_pkg.runner.run_step_reviews", return_value=review_items
            ):
                run_data = execute_plan_run(
                    plan_path=plan_path,
                    verbose=False,
                    approve_each_step=False,
                    branch=None,
                    commit=False,
                    commit_message=None,
                    allow_dirty_start=False,
                    review_enabled=True,
                    interactive=False,
                )

            step = run_data["steps"][0]
            self.assertEqual(step["status"], "paused")
            self.assertEqual(step["failure_reason"], "review_manual")
            self.assertEqual(step["review_policy"]["effective_policy"], "manual")
            self.assertEqual(step["review_policy"]["source"], "inferred")

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
