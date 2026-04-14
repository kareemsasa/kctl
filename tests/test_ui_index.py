from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.artifacts import single_run_dir
from kctl_pkg.ui_index import (
    build_step_metadata,
    default_db_path,
    derive_step_kind,
    index_repository_state,
    print_ui_run_detail,
    print_ui_runs,
    print_ui_workspaces,
)


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


def write_sample_plan_run(repo_path: Path) -> tuple[str, Path]:
    plans_dir = repo_path / "plans" / "traffic-simulator"
    plans_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plans_dir / "001-add-ui.yaml"
    plan_path.write_text(
        textwrap.dedent(
            f"""
            repo: {repo_path}
            objective: Improve the simulator UI
            steps:
              - id: inspect
                prompt: Inspect
              - id: verify
                kind: verify
                commands:
                  - printf ok
            """
        ).strip()
        + "\n"
    )

    run_id = "20260325T120000000000Z"
    run_root = repo_path / ".kctl" / "runs" / run_id
    plan_id = "001-add-ui"
    plan_run_dir = run_root / plan_id
    plan_run_dir.mkdir(parents=True, exist_ok=True)
    raw_output_path = plan_run_dir / "step-01-raw.md"
    raw_output_path.write_text("# raw\n")
    artifact_path = plan_run_dir / "step-01-inspect.json"
    artifact_path.write_text(json.dumps({"summary": "ok"}) + "\n")

    plan_run_data = {
        "started_at": "2026-03-25T12:00:00+00:00",
        "ended_at": "2026-03-25T12:01:00+00:00",
        "plan_path": str(plan_path),
        "repo": str(repo_path / ".kctl" / "worktrees" / run_id / plan_id),
        "objective": "Improve the simulator UI",
        "defaults": {},
        "review_enabled": False,
        "repo_dirty_at_start": False,
        "branch_before": "main",
        "branch_after": f"kctl/{run_id}/{plan_id}",
        "commit_created": False,
        "commit_sha": None,
        "status": "success",
        "run_output_dir": str(plan_run_dir),
        "steps": [
            {
                "id": "inspect",
                "prompt": "Inspect",
                "codex_prompt": "prompt",
                "started_at": "2026-03-25T12:00:00+00:00",
                "ended_at": "2026-03-25T12:00:10+00:00",
                "expect_clean_diff": False,
                "status": "success",
                "failure_reason": None,
                "before_git_status": {"exit_code": 0, "stdout": "", "stderr": ""},
                "after_git_status": {"exit_code": 0, "stdout": " M src/app.ts\n", "stderr": ""},
                "diff_stat": {"exit_code": 0, "stdout": " src/app.ts | 2 ++", "stderr": ""},
                "baseline_changed_files": [],
                "new_changed_files": ["src/app.ts"],
                "changed_files": ["src/app.ts"],
                "changed_files_count": 1,
                "codex_summary": "done",
                "codex": {"command": ["codex"], "cwd": str(repo_path), "exit_code": 0, "stdout": "ok", "stderr": ""},
                "verify": None,
                "step_type": {"effective_type": "analyze"},
                "output": {"schema": "inspect_v1"},
                "mode": {"effective_mode": "read-only"},
                "provider": "codex",
                "permission_mode": None,
                "verify_environment": None,
                "reviews": [],
                "raw_artifact_path": str(raw_output_path),
                "structured_artifacts": {"inspect": str(artifact_path)},
                "artifact_parse_error": None,
            },
            {
                "id": "verify",
                "prompt": "",
                "codex_prompt": "",
                "started_at": "2026-03-25T12:00:10+00:00",
                "ended_at": "2026-03-25T12:00:20+00:00",
                "expect_clean_diff": False,
                "status": "success",
                "failure_reason": None,
                "before_git_status": {"exit_code": 0, "stdout": " M src/app.ts\n", "stderr": ""},
                "after_git_status": {"exit_code": 0, "stdout": " M src/app.ts\n", "stderr": ""},
                "diff_stat": {"exit_code": 0, "stdout": " src/app.ts | 2 ++", "stderr": ""},
                "baseline_changed_files": ["src/app.ts"],
                "new_changed_files": [],
                "changed_files": ["src/app.ts"],
                "changed_files_count": 1,
                "codex_summary": "Verification handled by kctl.",
                "codex": {"command": [], "cwd": str(repo_path), "exit_code": 0, "stdout": "Verification handled by kctl.\n", "stderr": ""},
                "verify": {
                    "command": ["sh", "-lc", "printf ok"],
                    "cwd": str(repo_path),
                    "exit_code": 0,
                    "stdout": "ok",
                    "stderr": "",
                    "environment": {"cwd": str(repo_path), "shell": "sh -lc"},
                },
                "step_type": {"effective_type": "verify"},
                "verify_mode": {"effective_mode": "legacy"},
                "provider": "codex",
                "permission_mode": None,
                "verify_environment": {"cwd": str(repo_path), "shell": "sh -lc"},
                "reviews": [],
                "raw_artifact_path": str(plan_run_dir / "step-02-raw.md"),
                "structured_artifacts": {"verify": str(plan_run_dir / "step-02-verify.json")},
                "artifact_parse_error": None,
            },
        ],
    }
    (plan_run_dir / "run.json").write_text(json.dumps(plan_run_data, indent=2) + "\n")
    (plan_run_dir / "step-02-raw.md").write_text("# verify\n")
    (plan_run_dir / "step-02-verify.json").write_text(json.dumps({"status": "pass"}) + "\n")

    aggregate_run = {
        "run_id": run_id,
        "plans_dir": str(plans_dir),
        "repo": str(repo_path),
        "status": "passed",
        "started_at": "2026-03-25T12:00:00+00:00",
        "ended_at": "2026-03-25T12:01:00+00:00",
        "concurrency": 2,
        "plans": [
            {
                "plan_id": plan_id,
                "filename": plan_path.name,
                "plan_path": str(plan_path),
                "status": "passed",
                "current_step": "verify",
                "step_statuses": {"inspect": "success", "verify": "success"},
                "worktree_path": str(repo_path / ".kctl" / "worktrees" / run_id / plan_id),
                "branch_name": f"kctl/{run_id}/{plan_id}",
                "run_output_dir": str(plan_run_dir),
                "log_path": str(plan_run_dir / "run.json"),
                "verify_result": "passed",
            }
        ],
    }
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "run.json").write_text(json.dumps(aggregate_run, indent=2) + "\n")
    (repo_path / ".kctl" / "worktrees" / run_id / plan_id).mkdir(parents=True, exist_ok=True)
    return run_id, plan_path


class UIIndexTests(unittest.TestCase):
    def test_build_step_metadata_preserves_execution_context(self) -> None:
        metadata = build_step_metadata(
            {
                "structured_artifacts": {"inspect_v1": "/tmp/inspect.json"},
                "artifact_parse_error": None,
                "verify_environment": {"shell": "sh -lc"},
                "before_git_status": {"stdout": ""},
                "after_git_status": {"stdout": " M app.py"},
                "diff_stat": {"stdout": " app.py | 2 +-"},
                "failure_reason": "agent_rate_limited",
                "failure_details": {"reset_hint": "3pm"},
                "step_type": {"effective_type": "analyze"},
                "output": {"schema": "inspect_v1"},
                "review_policy": {"effective_policy": "advisory"},
                "mode": {"effective_mode": "read-only"},
                "verify_mode": {"effective_mode": "legacy"},
                "provider": "claude",
                "permission_mode": "plan",
                "reviews": [{"reviewer": "scope", "verdict": "pass"}],
            }
        )

        self.assertEqual(metadata["failure_reason"], "agent_rate_limited")
        self.assertEqual(metadata["failure_details"], {"reset_hint": "3pm"})
        self.assertEqual(metadata["step_type"], {"effective_type": "analyze"})
        self.assertEqual(metadata["provider"], "claude")
        self.assertEqual(metadata["permission_mode"], "plan")
        self.assertEqual(metadata["reviews"], [{"reviewer": "scope", "verdict": "pass"}])

    def test_derive_step_kind_prefers_effective_step_type(self) -> None:
        self.assertEqual(derive_step_kind({"id": "inspect", "step_type": {"effective_type": "analyze"}}), "analyze")
        self.assertEqual(derive_step_kind({"id": "implement", "step_type": {"effective_type": "change"}}), "change")
        self.assertEqual(derive_step_kind({"id": "review", "step_type": {"effective_type": "review"}}), "review")
        self.assertEqual(derive_step_kind({"id": "verify", "verify": {"exit_code": 0}}), "verify")

    def test_default_db_path_uses_custom_root_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            artifact_root = Path(tmpdir) / "visible-runs"
            init_git_repo(repo_path)

            with patch.dict(
                os.environ,
                {
                    "KCTL_ARTIFACT_ROOT": str(artifact_root),
                    "KCTL_ARTIFACT_STORAGE": "external",
                    "KCTL_HOME": str(Path(tmpdir) / "ignored-home"),
                },
                clear=False,
            ):
                db_path = default_db_path(repo_path)

            self.assertTrue(str(db_path).startswith(str(artifact_root.resolve())))

    def test_default_db_path_falls_back_to_repo_when_external_root_is_unwritable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            with patch.dict(
                os.environ,
                {
                    "KCTL_ARTIFACT_ROOT": "/root/blocked",
                    "KCTL_ARTIFACT_STORAGE": "external",
                    "KCTL_HOME": "/root/ignored-home",
                },
                clear=False,
            ):
                with patch("kctl_pkg.artifacts._storage_root_is_writable", return_value=False):
                    db_path = default_db_path(repo_path)

            self.assertEqual(db_path, repo_path.resolve() / ".kctl" / "ui-state.db")

    def test_index_repository_state_populates_sqlite_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)

            counts = index_repository_state(repo_path)

            self.assertEqual(counts["runs"], 1)
            self.assertEqual(counts["plan_executions"], 1)
            self.assertEqual(counts["step_executions"], 2)
            self.assertEqual(counts["workspaces"], 1)

            db_path = default_db_path(repo_path)
            connection = sqlite3.connect(str(db_path))
            try:
                run_count = connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                plan_execution_row = connection.execute(
                    "SELECT status, verify_status, current_step_key FROM plan_executions WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                step_rows = connection.execute(
                    "SELECT step_key, kind, verify_status, changed_files_count FROM step_executions ORDER BY sequence_index"
                ).fetchall()
                metadata_rows = connection.execute(
                    "SELECT step_key, metadata_json FROM step_executions ORDER BY sequence_index"
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual(run_count, 1)
            self.assertEqual(plan_execution_row, ("passed", "passed", "verify"))
            self.assertEqual(
                step_rows,
                [
                    ("inspect", "analyze", "not_run", 1),
                    ("verify", "verify", "passed", 1),
                ],
            )
            inspect_metadata = json.loads(metadata_rows[0][1])
            verify_metadata = json.loads(metadata_rows[1][1])
            self.assertEqual(inspect_metadata["step_type"], {"effective_type": "analyze"})
            self.assertEqual(inspect_metadata["output"], {"schema": "inspect_v1"})
            self.assertEqual(inspect_metadata["mode"], {"effective_mode": "read-only"})
            self.assertEqual(inspect_metadata["provider"], "codex")
            self.assertEqual(verify_metadata["verify_mode"], {"effective_mode": "legacy"})

    def test_ui_print_commands_read_indexed_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            runs_buffer = io.StringIO()
            with redirect_stdout(runs_buffer):
                print_ui_runs(repo_path)
            self.assertIn(run_id, runs_buffer.getvalue())

            detail_buffer = io.StringIO()
            with redirect_stdout(detail_buffer):
                print_ui_run_detail(repo_path, run_id)
            output = detail_buffer.getvalue()
            self.assertIn("001-add-ui", output)
            self.assertIn("verify", output)

            workspaces_buffer = io.StringIO()
            with redirect_stdout(workspaces_buffer):
                print_ui_workspaces(repo_path)
            workspace_output = workspaces_buffer.getvalue()
            self.assertIn("001-add-ui", workspace_output)
            self.assertIn("released", workspace_output)

    def test_index_repository_state_reads_external_single_run_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            kctl_home = Path(tmpdir) / "kctl-home"
            init_git_repo(repo_path)
            plan_path = repo_path / "plans" / "sample.yaml"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: External run
                    steps:
                      - id: inspect
                        prompt: Inspect
                    """
                ).strip()
                + "\n"
            )

            run_id = "20260329T000000000000Z"
            with patch.dict(
                os.environ,
                {"KCTL_HOME": str(kctl_home), "KCTL_ARTIFACT_STORAGE": "external"},
                clear=False,
            ):
                run_dir = single_run_dir(repo_path, run_id, storage_mode="external")
                run_dir.mkdir(parents=True, exist_ok=True)
                raw_output_path = run_dir / "step-01-raw.md"
                raw_output_path.write_text("# raw\n")
                artifact_path = run_dir / "step-01-inspect.json"
                artifact_path.write_text(json.dumps({"summary": "ok"}) + "\n")
                run_data = {
                    "started_at": "2026-03-29T00:00:00+00:00",
                    "ended_at": "2026-03-29T00:01:00+00:00",
                    "plan_path": str(plan_path),
                    "repo": str(repo_path),
                    "objective": "External run",
                    "defaults": {},
                    "review_enabled": False,
                    "repo_dirty_at_start": False,
                    "branch_before": "main",
                    "branch_after": "main",
                    "commit_created": False,
                    "commit_sha": None,
                    "status": "success",
                    "artifact_storage_mode": "external",
                    "artifact_root_path": str(run_dir.parent),
                    "run_output_dir": str(run_dir),
                    "steps": [
                        {
                            "id": "inspect",
                            "prompt": "Inspect",
                            "codex_prompt": "prompt",
                            "started_at": "2026-03-29T00:00:00+00:00",
                            "ended_at": "2026-03-29T00:00:10+00:00",
                            "expect_clean_diff": False,
                            "status": "success",
                            "failure_reason": None,
                            "before_git_status": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "after_git_status": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "diff_stat": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "baseline_changed_files": [],
                            "new_changed_files": [],
                            "changed_files": [],
                            "changed_files_count": 0,
                            "codex_summary": "done",
                            "codex": {"command": ["codex"], "cwd": str(repo_path), "exit_code": 0, "stdout": "ok", "stderr": ""},
                            "verify": None,
                            "verify_environment": None,
                            "reviews": [],
                            "raw_artifact_path": str(raw_output_path),
                            "structured_artifacts": {"inspect_v1": str(artifact_path)},
                            "artifact_parse_error": None,
                        }
                    ],
                }
                (run_dir / "run.json").write_text(json.dumps(run_data, indent=2) + "\n")
                counts = index_repository_state(repo_path)
                self.assertEqual(counts["runs"], 1)
                db_path = default_db_path(repo_path)
                connection = sqlite3.connect(str(db_path))
                try:
                    run_row = connection.execute("SELECT id, launch_source FROM runs").fetchone()
                finally:
                    connection.close()
            self.assertEqual(run_row, (f"single:{run_id}", "single_run"))

    def test_index_repository_state_reads_custom_root_single_run_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            artifact_root = Path(tmpdir) / "visible-runs"
            init_git_repo(repo_path)
            plan_path = repo_path / "plans" / "sample.yaml"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                textwrap.dedent(
                    f"""
                    repo: {repo_path}
                    objective: Custom root run
                    steps:
                      - id: inspect
                        prompt: Inspect
                    """
                ).strip()
                + "\n"
            )

            run_id = "20260329T000000000001Z"
            with patch.dict(
                os.environ,
                {
                    "KCTL_ARTIFACT_ROOT": str(artifact_root),
                    "KCTL_ARTIFACT_STORAGE": "external",
                    "KCTL_HOME": str(Path(tmpdir) / "ignored-home"),
                },
                clear=False,
            ):
                run_dir = single_run_dir(repo_path, run_id, storage_mode="custom_root")
                run_dir.mkdir(parents=True, exist_ok=True)
                raw_output_path = run_dir / "step-01-raw.md"
                raw_output_path.write_text("# raw\n")
                artifact_path = run_dir / "step-01-inspect.json"
                artifact_path.write_text(json.dumps({"summary": "ok"}) + "\n")
                run_data = {
                    "started_at": "2026-03-29T00:00:00+00:00",
                    "ended_at": "2026-03-29T00:01:00+00:00",
                    "plan_path": str(plan_path),
                    "repo": str(repo_path),
                    "objective": "Custom root run",
                    "defaults": {},
                    "review_enabled": False,
                    "repo_dirty_at_start": False,
                    "branch_before": "main",
                    "branch_after": "main",
                    "commit_created": False,
                    "commit_sha": None,
                    "status": "success",
                    "artifact_storage_mode": "custom_root",
                    "artifact_root_path": str(run_dir.parent),
                    "run_output_dir": str(run_dir),
                    "steps": [
                        {
                            "id": "inspect",
                            "prompt": "Inspect",
                            "codex_prompt": "prompt",
                            "started_at": "2026-03-29T00:00:00+00:00",
                            "ended_at": "2026-03-29T00:00:10+00:00",
                            "expect_clean_diff": False,
                            "status": "success",
                            "failure_reason": None,
                            "before_git_status": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "after_git_status": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "diff_stat": {"exit_code": 0, "stdout": "", "stderr": ""},
                            "baseline_changed_files": [],
                            "new_changed_files": [],
                            "changed_files": [],
                            "changed_files_count": 0,
                            "codex_summary": "done",
                            "codex": {"command": ["codex"], "cwd": str(repo_path), "exit_code": 0, "stdout": "ok", "stderr": ""},
                            "verify": None,
                            "verify_environment": None,
                            "reviews": [],
                            "raw_artifact_path": str(raw_output_path),
                            "structured_artifacts": {"inspect_v1": str(artifact_path)},
                            "artifact_parse_error": None,
                        }
                    ],
                }
                (run_dir / "run.json").write_text(json.dumps(run_data, indent=2) + "\n")
                counts = index_repository_state(repo_path)
                self.assertEqual(counts["runs"], 1)
                db_path = default_db_path(repo_path)
                self.assertTrue(str(db_path).startswith(str(artifact_root)))
                connection = sqlite3.connect(str(db_path))
                try:
                    run_row = connection.execute("SELECT id, launch_source FROM runs").fetchone()
                finally:
                    connection.close()
            self.assertEqual(run_row, (f"single:{run_id}", "single_run"))


if __name__ == "__main__":
    unittest.main()
