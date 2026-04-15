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

from kctl_pkg.artifacts import resolve_storage, single_run_dir
from kctl_pkg.git import create_isolated_workspace
from kctl_pkg.multi import (
    _apply_provider_override_to_plan,
    _apply_repo_override_to_plan,
    build_branch_name,
    build_multi_run_id,
    discover_plan_files,
    format_status_line,
    load_normalized_multi_plans,
    load_plan_specs,
    print_run_status,
    resolve_multi_run_log,
    resolve_status_run_path,
    run_many_plans,
    sanitize_plan_id,
    stop_request_path,
    write_blocked_run_state,
    write_run_state,
)
from kctl_pkg.plan import normalize_plan, validate_plan
from kctl_pkg.runner import execute_plan_run
from kctl_pkg.types import CommandResult, PlanError


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
    def test_provider_override_updates_defaults(self) -> None:
        plan = {"defaults": {"provider": "claude"}}

        updated_codex = _apply_provider_override_to_plan(plan, "codex")
        self.assertEqual(updated_codex["defaults"]["provider"], "codex")
        self.assertNotIn("permission_mode", updated_codex["defaults"])
        self.assertEqual(updated_codex["_kctl_provider"], "codex")

        updated_claude = _apply_provider_override_to_plan({"defaults": {}}, "claude")
        self.assertEqual(updated_claude["defaults"]["provider"], "claude")
        self.assertEqual(updated_claude["defaults"]["permission_mode"], "auto")
        self.assertEqual(updated_claude["_kctl_permission_mode"], "auto")

        with self.assertRaisesRegex(PlanError, "provider override must be one of"):
            _apply_provider_override_to_plan({}, "other")

    def test_repo_override_updates_repo_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir)
            updated = _apply_repo_override_to_plan({"repo": "/tmp/old"}, repo_path)
            self.assertEqual(updated["repo"], str(repo_path.resolve()))

        original = {"repo": "/tmp/old"}
        self.assertIs(_apply_repo_override_to_plan(original, None), original)

    def test_basic_multi_helpers(self) -> None:
        self.assertEqual(sanitize_plan_id("  weird / plan  "), "weird-plan")
        self.assertEqual(build_branch_name("run-1", "plan one"), "kctl/run-1/plan-one")
        self.assertTrue(build_multi_run_id().endswith("Z"))
        self.assertEqual(stop_request_path(Path("/tmp/run")), Path("/tmp/run/stop-requested"))
        self.assertEqual(
            format_status_line({"plan_id": "001-plan", "status": "passed", "current_step": "verify", "verify_result": "passed"}),
            "  001-plan: passed  step=verify  verify=passed",
        )

    def test_load_plan_specs_rejects_mixed_repo_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_a = Path(tmpdir) / "repo-a"
            repo_b = Path(tmpdir) / "repo-b"
            init_git_repo(repo_a)
            init_git_repo(repo_b)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-a.yaml").write_text(
                f"repo: {repo_a}\nobjective: a\nsteps:\n  - id: inspect\n    prompt: Inspect\n"
            )
            (plans_dir / "002-b.yaml").write_text(
                f"repo: {repo_b}\nobjective: b\nsteps:\n  - id: inspect\n    prompt: Inspect\n"
            )

            with self.assertRaisesRegex(PlanError, "must target the same git repository"):
                load_plan_specs(plans_dir)

    def test_load_normalized_multi_plans_disambiguates_duplicate_plan_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "same name.yaml").write_text(
                f"repo: {repo_path}\nobjective: one\nsteps:\n  - id: inspect\n    prompt: Inspect\n"
            )
            (plans_dir / "same@name.yml").write_text(
                f"repo: {repo_path}\nobjective: two\nsteps:\n  - id: inspect\n    prompt: Inspect\n"
            )

            specs, plans = load_normalized_multi_plans(plans_dir)

            self.assertEqual([spec.plan_id for spec in specs], ["same-name", "same-name-02"])
            self.assertEqual(sorted(plans), ["same-name", "same-name-02"])

    def test_write_blocked_run_state_persists_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "run-1"
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            plan_path = plans_dir / "001-plan.yaml"
            plan_path.write_text("repo: /tmp\nobjective: x\nsteps: []\n")
            spec = type("Spec", (), {
                "plan_id": "001-plan",
                "filename": "001-plan.yaml",
                "plan_path": plan_path,
                "step_ids": ["inspect"],
            })()
            snapshot = {
                "captured_at": "2026-01-01T00:00:00+00:00",
                "environment": {"storage_mode": "in_repo"},
                "repo_root": str(Path(tmpdir) / "repo"),
                "worktree_root": str(Path(tmpdir) / "worktrees" / "run-1"),
            }

            run_data = write_blocked_run_state(
                plans_dir=plans_dir,
                run_root=run_root,
                run_id="run-1",
                concurrency=2,
                plan_specs=[spec],
                preflight_snapshot=snapshot,
            )

            self.assertEqual(run_data["status"], "blocked")
            self.assertEqual(run_data["plans"][0]["failure_reason"], "preflight_failed")
            self.assertTrue((run_root / "run.json").exists())

    def test_write_run_state_and_resolve_log_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_root = repo_path / ".kctl" / "runs" / "run-1"
            run_data = {
                "run_id": "run-1",
                "plans": [{"plan_id": "001-plan", "status": "passed", "current_step": "inspect", "verify_result": "not-run"}],
            }

            run_path = write_run_state(run_root, run_data)

            self.assertEqual(run_path, run_root / "run.json")
            self.assertEqual(resolve_multi_run_log(repo_path, "run-1"), run_path.resolve())

    def test_resolve_status_run_path_accepts_run_directory_and_plans_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = repo_path / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-plan.yaml").write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: inspect\n    prompt: Inspect\n"
            )
            run_root = repo_path / ".kctl" / "runs" / "run-1"
            write_run_state(
                run_root,
                {
                    "run_id": "run-1",
                    "plans_dir": str(plans_dir.resolve()),
                    "plans": [{"plan_id": "001-plan", "status": "passed", "current_step": "inspect", "verify_result": "not-run"}],
                },
            )

            self.assertEqual(resolve_status_run_path(str(run_root)), (run_root / "run.json").resolve())
            self.assertEqual(resolve_status_run_path(str(plans_dir)), (run_root / "run.json").resolve())

    def test_resolve_status_run_path_rejects_unknown_target(self) -> None:
        with self.assertRaisesRegex(PlanError, "Could not resolve run status target"):
            resolve_status_run_path("missing-run-id")

    def test_print_run_status_renders_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_root = repo_path / ".kctl" / "runs" / "run-1"
            write_run_state(
                run_root,
                {
                    "run_id": "run-1",
                    "plans": [{"plan_id": "001-plan", "status": "passed", "current_step": "inspect", "verify_result": "not-run"}],
                },
            )

            with patch("sys.stdout.write") as mock_write:
                exit_code = print_run_status(str(run_root))

            rendered = "".join(call.args[0] for call in mock_write.call_args_list)
            self.assertEqual(exit_code, 0)
            self.assertIn("Run:", rendered)
            self.assertIn("001-plan: passed", rendered)

    def test_create_isolated_workspace_links_repo_local_venv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            workspace_path = Path(tmpdir) / "workspace"
            init_git_repo(repo_path)
            (repo_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
            (repo_path / ".venv" / "bin" / "python").write_text("#!/usr/bin/env python3\n")

            created_path = create_isolated_workspace(repo_path, workspace_path, "kctl/test-workspace")

            self.assertEqual(created_path, workspace_path)
            self.assertTrue((workspace_path / ".venv").exists())
            self.assertTrue((workspace_path / ".venv").is_symlink())
            self.assertEqual((workspace_path / ".venv").resolve(), (repo_path / ".venv").resolve())

    def test_execute_plan_run_provider_override_applies_to_preflight_binaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "provider.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: provider override
                    defaults:
                      provider: codex
                    steps:
                      - id: implement
                        prompt: Implement
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="ok\n", stderr="")

            def fake_which(binary: str, path: str | None = None) -> str:
                return f"/usr/bin/{binary}"

            with patch("kctl_pkg.preflight.shutil.which", side_effect=fake_which), patch(
                "kctl_pkg.runner.run_streaming_command",
                side_effect=fake_streaming_command,
            ):
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
                    provider_override="claude",
                )

            self.assertIn("claude", run_data["preflight"]["required_binaries"])
            self.assertNotIn("codex", run_data["preflight"]["required_binaries"])

    def test_execute_plan_run_can_start_from_named_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "from-step.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: partial run
                    steps:
                      - id: inspect
                        prompt: Inspect
                      - id: implement
                        prompt: Implement
                      - id: verify
                        type: verify
                        commands:
                          - printf ok
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="ok\n", stderr="")

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
                    from_step="implement",
                )

            self.assertEqual([step["id"] for step in run_data["steps"]], ["implement", "verify"])
            self.assertEqual(run_data["requested_from_step"], "implement")
            self.assertIsNone(run_data["requested_only_step"])

    def test_execute_plan_run_can_run_only_named_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "only-step.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: single step
                    steps:
                      - id: inspect
                        prompt: Inspect
                      - id: implement
                        prompt: Implement
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                return CommandResult(command=command, cwd=str(repo_path), exit_code=0, stdout="ok\n", stderr="")

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
                    only_step="implement",
                )

            self.assertEqual([step["id"] for step in run_data["steps"]], ["implement"])
            self.assertEqual(run_data["requested_only_step"], "implement")
            self.assertIsNone(run_data["requested_from_step"])

    def test_execute_plan_run_classifies_agent_quota_exhaustion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "quota.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: quota
                    steps:
                      - id: implement
                        prompt: Implement
                    """
                ).strip()
                + "\n"
            )

            def fake_streaming_command(*args, **kwargs):
                command = args[0]
                return CommandResult(
                    command=command,
                    cwd=str(repo_path),
                    exit_code=1,
                    stdout="",
                    stderr="You've hit your limit · resets 3pm (America/Chicago)\n",
                )

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
            self.assertEqual(step["failure_reason"], "agent_quota_exhausted")
            self.assertEqual(step["failure_details"]["reset_hint"], "3pm (America/Chicago)")

    def test_resolve_storage_prefers_custom_root_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_root = Path(tmpdir) / "visible-runs"
            env = {
                "KCTL_ARTIFACT_ROOT": str(custom_root),
                "KCTL_ARTIFACT_STORAGE": "external",
                "KCTL_HOME": str(Path(tmpdir) / "ignored-home"),
            }
            with patch.dict(os.environ, env, clear=False):
                storage = resolve_storage()

            self.assertEqual(storage.mode, "custom_root")
            self.assertEqual(storage.root, custom_root.resolve())

    def test_resolve_storage_falls_back_to_in_repo_when_external_roots_are_unwritable(self) -> None:
        env = {
            "KCTL_ARTIFACT_ROOT": "/root/blocked",
            "KCTL_ARTIFACT_STORAGE": "external",
            "KCTL_HOME": "/root/ignored-home",
        }
        with patch.dict(os.environ, env, clear=False):
            with patch("kctl_pkg.artifacts._storage_root_is_writable", return_value=False):
                storage = resolve_storage()

        self.assertEqual(storage.mode, "in_repo")

    def test_discover_plan_files_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir)
            (plans_dir / "b-second.yaml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")
            (plans_dir / "a-first.yml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")
            discovered = discover_plan_files(plans_dir)
            self.assertEqual([path.name for path in discovered], ["a-first.yml", "b-second.yaml"])

    def test_discover_plan_files_can_filter_selected_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir)
            (plans_dir / "a-first.yml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")
            (plans_dir / "b-second.yaml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n")

            discovered = discover_plan_files(plans_dir, selected_filenames={"b-second.yaml"})

            self.assertEqual([path.name for path in discovered], ["b-second.yaml"])

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
            self.assertEqual(step["agent"]["command"], [])
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
            self.assertEqual(step["agent"]["command"], [])
            self.assertEqual(step["verify"]["exit_code"], 0)
            self.assertEqual(step["step_type"]["effective_type"], "verify")
            self.assertEqual(step["step_type"]["source"], "explicit")
            self.assertEqual(run_data["status"], "success")
            summary_text = (Path(run_data["run_output_dir"]) / "summary.md").read_text()
            self.assertIn("# verify.yaml", summary_text)
            self.assertIn("Timestamp:", summary_text)
            self.assertIn("## Steps", summary_text)
            self.assertIn("- validate: success (verification: passed)", summary_text)

    def test_execute_plan_run_blocks_before_launch_when_required_env_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = Path(tmpdir) / "blocked.yaml"
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: blocked
                    required_env:
                      - TEST_REQUIRED_SECRET
                    steps:
                      - id: inspect
                        prompt: Inspect
                    """
                ).strip()
                + "\n"
            )

            with patch.dict(os.environ, {"PATH": os.environ.get("PATH", "")}, clear=True):
                with patch("kctl_pkg.runner.run_streaming_command", side_effect=AssertionError("agent should not run")):
                    with self.assertRaises(PlanError) as context:
                        execute_plan_run(
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

            self.assertIn("Preflight failed before launch", str(context.exception))
            run_logs = sorted((repo_path / ".kctl-runs").glob("*/run.json"))
            self.assertTrue(run_logs)
            run_state = json.loads(run_logs[-1].read_text())
            self.assertEqual(run_state["status"], "blocked")
            self.assertEqual(run_state["preflight"]["issues"][0]["code"], "missing_env")

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

    def test_validate_plan_rejects_unsupported_review_output_schema(self) -> None:
        plan = {
            "repo": "/tmp/repo",
            "objective": "x",
            "steps": [
                {
                    "id": "review",
                    "type": "review",
                    "prompt": "Review",
                    "output": {"schema": "review_v1"},
                }
            ],
        }

        with self.assertRaises(PlanError) as context:
            validate_plan(plan)

        self.assertIn("inspect_v1, plan_v1", str(context.exception))

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

            env = {"KCTL_ARTIFACT_STORAGE": "external", "KCTL_HOME": str(kctl_home)}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
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

    def test_execute_plan_run_custom_root_storage_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            artifact_root = Path(tmpdir) / "visible-runs"
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

            env = {
                "KCTL_ARTIFACT_ROOT": str(artifact_root),
                "KCTL_ARTIFACT_STORAGE": "external",
                "KCTL_HOME": str(Path(tmpdir) / "ignored-home"),
            }
            with patch.dict(os.environ, env, clear=False):
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
                self.assertEqual(run_data["artifact_storage_mode"], "custom_root")
                self.assertEqual(run_data["artifact_root_path"], str(run_output_dir.parent))
                self.assertEqual(run_output_dir, single_run_dir(repo_path, run_output_dir.name, storage_mode="custom_root"))
                self.assertTrue(run_output_dir.exists())
                self.assertEqual(run_output_dir.parents[2], artifact_root / "repos")

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

            env = {"KCTL_ARTIFACT_STORAGE": "external", "KCTL_HOME": str(kctl_home)}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
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

    def test_run_many_plans_custom_root_writes_under_visible_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            artifact_root = Path(tmpdir) / "visible-runs"
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
                    "artifact_storage_mode": "custom_root",
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

            env = {
                "KCTL_ARTIFACT_ROOT": str(artifact_root),
                "KCTL_ARTIFACT_STORAGE": "external",
                "KCTL_HOME": str(Path(tmpdir) / "ignored-home"),
            }
            with patch.dict(os.environ, env, clear=False):
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=fake_create_workspace), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
                ):
                    exit_code = run_many_plans(plans_dir, concurrency=2, verbose=False)

                self.assertEqual(exit_code, 0)
                run_logs = sorted((artifact_root / "repos").glob("*/runs/*/run.json"))
                self.assertTrue(run_logs)
                run_state = json.loads(run_logs[-1].read_text())
                self.assertEqual(run_state["artifact_storage_mode"], "custom_root")
                self.assertEqual(Path(run_state["artifact_root_path"]).resolve(), Path(run_logs[-1]).parent.parent.resolve())
                self.assertTrue(run_state["plans"])
                self.assertEqual(Path(run_state["plans"][0]["run_output_dir"]).resolve().parent, Path(run_logs[-1]).parent.resolve())
                self.assertEqual(
                    Path(run_state["plans"][0]["worktree_path"]).resolve().parents[3],
                    (artifact_root / "repos").resolve(),
                )
                summary_text = (Path(run_logs[-1]).parent / "summary.md").read_text()
                self.assertIn("# plans", summary_text)
                self.assertIn("Timestamp:", summary_text)
                self.assertIn("## Plans", summary_text)
                self.assertIn("- 001-plan: passed (verification: not-run)", summary_text)

    def test_run_many_plans_can_override_target_repo_for_all_selected_plans(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            target_repo = Path(tmpdir) / "target-repo"
            init_git_repo(repo_path)
            init_git_repo(target_repo)
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

            execute_calls: list[dict[str, object]] = []

            def fake_create_workspace(repo_root: Path, workspace_path: Path, branch_name: str) -> Path:
                self.assertEqual(repo_root.resolve(), target_repo.resolve())
                workspace_path.mkdir(parents=True, exist_ok=True)
                return workspace_path

            def fake_execute_plan_run(**kwargs):
                execute_calls.append(kwargs)
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
                exit_code = run_many_plans(
                    plans_dir,
                    concurrency=1,
                    verbose=False,
                    selected_plan_names=["001-plan.yaml", "002-plan.yaml"],
                    repo_override=target_repo,
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(execute_calls), 2)
            self.assertTrue(all("/worktrees/" in str(call["repo_override"]) for call in execute_calls))

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

            env = {"KCTL_ARTIFACT_STORAGE": "in_repo"}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
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
            summary_text = (run_logs[-1].parent / "summary.md").read_text()
            self.assertIn("## Plans", summary_text)
            self.assertIn("- 001-plan: passed (verification: not-run)", summary_text)
            stream_text = (run_logs[-1].parent / "stream.log").read_text()
            self.assertIn("Run:", stream_text)
            self.assertIn("Summary", stream_text)

    def test_run_many_plans_can_reuse_existing_workspace_for_single_plan_partial_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            workspace_path = repo_path / ".kctl" / "worktrees" / "previous-run" / "001-plan"
            workspace_path.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "worktree", "add", "-b", "kctl/previous/001-plan", str(workspace_path), "HEAD"], cwd=str(repo_path), check=True, capture_output=True)
            (plans_dir / "001-plan.yaml").write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: plan 0
                    steps:
                      - id: inspect
                        prompt: Inspect 0
                      - id: verify
                        type: verify
                        commands:
                          - printf ok
                    """
                ).strip()
                + "\n"
            )

            execute_calls: list[dict[str, object]] = []

            def fake_execute_plan_run(**kwargs):
                execute_calls.append(kwargs)
                run_output_dir = kwargs["run_output_dir_override"]
                run_output_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "status": "success",
                    "steps": [
                        {
                            "id": "verify",
                            "status": "success",
                            "verify": {"exit_code": 0},
                            "changed_files_count": 0,
                        }
                    ],
                    "log_path": str(run_output_dir / "run.json"),
                }

            env = {"KCTL_ARTIFACT_STORAGE": "in_repo"}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=AssertionError("workspace should be reused")), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
                ):
                    exit_code = run_many_plans(
                        plans_dir,
                        concurrency=1,
                        verbose=False,
                        selected_plan_names=["001-plan.yaml"],
                        only_step="verify",
                        reuse_workspace_path=workspace_path,
                    )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(execute_calls), 1)
            self.assertEqual(execute_calls[0]["repo_override"], str(workspace_path.resolve()))
            self.assertEqual(execute_calls[0]["only_step"], "verify")

    def test_run_many_plans_marks_run_stopped_when_plan_is_stopped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-plan.yaml").write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: plan 0
                    steps:
                      - id: inspect
                        prompt: Inspect 0
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
                    "status": "stopped",
                    "steps": [
                        {
                            "id": "inspect",
                            "status": "stopped",
                            "verify": None,
                            "changed_files_count": 0,
                            "failure_reason": "run_stopped",
                        }
                    ],
                    "log_path": str(run_output_dir / "run.json"),
                }

            env = {"KCTL_ARTIFACT_STORAGE": "in_repo"}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=fake_create_workspace), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
                ):
                    exit_code = run_many_plans(plans_dir, concurrency=1, verbose=False)

            self.assertEqual(exit_code, 1)
            run_logs = sorted((repo_path / ".kctl" / "runs").glob("*/run.json"))
            self.assertTrue(run_logs)
            run_state = json.loads(run_logs[-1].read_text())
            self.assertEqual(run_state["status"], "stopped")
            self.assertEqual(run_state["plans"][0]["status"], "stopped")
            self.assertEqual(run_state["plans"][0]["failure_reason"], "run_stopped")

    def test_run_many_plans_persists_active_pids_while_process_callbacks_fire(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-plan.yaml").write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: plan 0
                    steps:
                      - id: inspect
                        prompt: Inspect 0
                    """
                ).strip()
                + "\n"
            )

            def fake_create_workspace(repo_root: Path, workspace_path: Path, branch_name: str) -> Path:
                workspace_path.mkdir(parents=True, exist_ok=True)
                return workspace_path

            active_pid_snapshots: list[list[int]] = []

            def fake_execute_plan_run(**kwargs):
                process_started = kwargs["process_started"]
                process_finished = kwargs["process_finished"]
                run_output_dir = kwargs["run_output_dir_override"]
                run_output_dir.mkdir(parents=True, exist_ok=True)
                process_started(42424)
                run_log = run_output_dir.parent / "run.json"
                active_pid_snapshots.append(json.loads(run_log.read_text())["active_pids"])
                process_finished(42424)
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

            env = {"KCTL_ARTIFACT_STORAGE": "in_repo"}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("KCTL_ARTIFACT_ROOT", None)
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=fake_create_workspace), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=fake_execute_plan_run
                ):
                    exit_code = run_many_plans(plans_dir, concurrency=1, verbose=False)

            self.assertEqual(exit_code, 0)
            self.assertEqual(active_pid_snapshots, [[42424]])
            run_logs = sorted((repo_path / ".kctl" / "runs").glob("*/run.json"))
            self.assertTrue(run_logs)
            run_state = json.loads(run_logs[-1].read_text())
            self.assertEqual(run_state["active_pids"], [])

    def test_run_many_plans_blocks_before_launch_when_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-plan.yaml").write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: plan 0
                    required_env:
                      - TEST_REQUIRED_SECRET
                    steps:
                      - id: inspect
                        prompt: Inspect 0
                    """
                ).strip()
                + "\n"
            )

            with patch.dict(os.environ, {"PATH": os.environ.get("PATH", "")}, clear=True):
                with patch("kctl_pkg.multi.create_isolated_workspace", side_effect=AssertionError("workspace should not be created")), patch(
                    "kctl_pkg.multi.execute_plan_run", side_effect=AssertionError("plan should not run")
                ):
                    with self.assertRaises(PlanError) as context:
                        run_many_plans(plans_dir, concurrency=1, verbose=False)

            self.assertIn("Preflight failed before launch", str(context.exception))
            run_logs = sorted((repo_path / ".kctl" / "runs").glob("*/run.json"))
            self.assertTrue(run_logs)
            run_state = json.loads(run_logs[-1].read_text())
            self.assertEqual(run_state["status"], "blocked")
            self.assertEqual(run_state["plans"][0]["status"], "blocked")
            self.assertEqual(run_state["plans"][0]["failure_reason"], "preflight_failed")


if __name__ == "__main__":
    unittest.main()
