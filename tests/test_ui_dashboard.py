from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from kctl_pkg.ui_dashboard import (
    DashboardApp,
    build_dashboard_access_urls,
    check_repo_path,
    list_plans_in_directory,
    summarize_preflight_for_dashboard,
)
from kctl_pkg.ui_index import default_db_path, index_repository_state
from tests.test_ui_index import init_git_repo, write_sample_plan_run


class UIDashboardTests(unittest.TestCase):
    def test_check_repo_path_reports_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            repo_path.mkdir()
            file_path = Path(tmpdir) / "file.txt"
            file_path.write_text("x")

            self.assertEqual(check_repo_path("")[0], "empty")
            self.assertEqual(check_repo_path(str(repo_path))[0], "ok")
            self.assertEqual(check_repo_path(str(file_path))[0], "not_dir")
            self.assertEqual(check_repo_path(str(Path(tmpdir) / "missing"))[0], "missing")

    def test_list_plans_in_directory_reports_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-first.yaml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n")
            (plans_dir / "002-second.yml").write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n")

            status, message, plans = list_plans_in_directory(str(plans_dir))

            self.assertEqual(status, "ok")
            self.assertIn("Found 2 plan file(s)", message)
            self.assertEqual(plans, ["001-first.yaml", "002-second.yml"])

    def test_summarize_preflight_for_dashboard_surfaces_ready_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            plans_dir = repo_path / ".kctl" / "plans"
            init_git_repo(repo_path)
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-first.yaml").write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n"
            )

            summary = summarize_preflight_for_dashboard(str(repo_path), str(plans_dir))

            self.assertIn(summary["status"], {"pass", "block"})
            self.assertIn("repo", summary["items"])
            self.assertIn("binaries", summary["items"])
            self.assertIn("writable_paths", summary["items"])
            self.assertIn("required_env", summary["items"])

    def test_summarize_preflight_for_dashboard_honors_provider_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = repo_path / ".kctl" / "plans"
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-first.yaml").write_text("placeholder\n")
            plan_specs = [SimpleNamespace(filename="001-first.yaml")]
            normalized_plan = {
                "defaults": {"provider": "codex"},
                "steps": [{"id": "implement", "_kctl_step_type": {"effective_type": "change"}}],
            }

            class FakeIssue:
                code = ""
                message = ""
                fix = ""

            class FakeReport:
                ok = True
                repo_root = repo_path
                run_root = repo_path / ".kctl" / "runs" / "r1"
                worktree_root = repo_path / ".kctl" / "worktrees" / "r1"
                required_binaries = ["claude", "git"]
                required_env: list[str] = []
                issues = [FakeIssue()]

            with patch(
                "kctl_pkg.ui_dashboard.load_normalized_multi_plans",
                return_value=(plan_specs, {"001-first": normalized_plan}),
            ), patch(
                "kctl_pkg.ui_dashboard.preflight_multi_run",
                return_value=FakeReport(),
            ):
                summary = summarize_preflight_for_dashboard(
                    str(repo_path),
                    str(plans_dir),
                    provider_override="claude",
                )

            binaries_details = summary["items"]["binaries"]["details"] or ""
            self.assertIn("claude", binaries_details)
            self.assertNotIn("codex", binaries_details)

    def test_build_dashboard_access_urls_prefers_announce_url(self) -> None:
        urls = build_dashboard_access_urls(
            "0.0.0.0",
            8421,
            announce_url="http://kctl-node.tailnet.ts.net:8421",
            tailscale=True,
            hostname="ignored-host",
        )
        self.assertEqual(
            urls,
            [
                "http://kctl-node.tailnet.ts.net:8421",
                "http://localhost:8421",
                "http://ignored-host:8421",
            ],
        )

    def test_build_dashboard_access_urls_adds_tailscale_hostname_hint(self) -> None:
        urls = build_dashboard_access_urls("0.0.0.0", 8421, tailscale=True, hostname="kctl-node")
        self.assertEqual(urls, ["http://localhost:8421", "http://kctl-node:8421"])

    def test_dashboard_renders_runs_plan_cards_steps_and_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_page(run_id=run_id)

            self.assertIn("kctl", html)
            self.assertIn("overview-bar", html)
            self.assertIn("Attention", html)
            self.assertIn("Workspaces", html)
            self.assertIn("Plans", html)
            self.assertIn("Runs", html)
            self.assertIn("Run Detail", html)
            self.assertIn("Plan Executions", html)
            self.assertIn("Plan Execution Detail", html)
            self.assertIn("Step Timeline", html)
            self.assertIn("Workspace", html)
            self.assertIn(run_id, html)
            self.assertIn("001-add-ui", html)
            self.assertIn("verify", html)
            self.assertIn(".kctl/worktrees/", html)
            self.assertIn("lifecycle=released", html)
            self.assertIn('name="viewport"', html)
            self.assertIn("table-scroll", html)
            self.assertIn("page-header", html)
            self.assertIn("@media (max-width: 860px)", html)
            self.assertIn("Plan File", html)
            self.assertIn("main-nav", html)
            self.assertIn("/actions", html)
            self.assertIn("/projects", html)

            actions_html = app.render_actions_page()

            self.assertIn("Run Plans", actions_html)
            self.assertIn("Refresh Index", actions_html)
            self.assertIn("Create Plan", actions_html)
            self.assertIn("template_name", actions_html)
            self.assertIn(".kctl/plans", actions_html)
            self.assertIn("Plans Dir Override", actions_html)
            self.assertIn("Target Repo", actions_html)
            self.assertIn("target_repo_run_many_status", actions_html)
            self.assertIn("/api/check-repo", actions_html)
            self.assertIn("plans_dir_preview", actions_html)
            self.assertIn("/api/list-plans", actions_html)
            self.assertIn("/api/preflight", actions_html)
            self.assertIn("selected_plans", actions_html)
            self.assertIn("Launch Preflight", actions_html)
            self.assertIn("run_many_preflight", actions_html)
            self.assertIn("preflight-badge", actions_html)
            self.assertIn("run_many_launch_decision", actions_html)
            self.assertIn("PASS", actions_html)
            self.assertIn("run_many_submit_button", actions_html)
            self.assertIn("run_single_across_projects_button", actions_html)
            self.assertIn("Tracked Projects", actions_html)

    def test_dashboard_attention_queue_surfaces_blocked_and_running_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            db_path = default_db_path(repo_path)
            connection = sqlite3.connect(str(db_path))
            try:
                connection.execute(
                    """
                    UPDATE plan_executions
                    SET status = 'blocked', failure_reason = 'review_blocked'
                    WHERE run_id = ?
                    """,
                    (run_id,),
                )
                connection.execute(
                    """
                    UPDATE workspaces
                    SET status = 'active', released_at = NULL
                    WHERE plan_execution_id = (
                        SELECT id FROM plan_executions WHERE run_id = ?
                    )
                    """,
                    (run_id,),
                )
                connection.execute(
                    """
                    INSERT INTO runs (
                        id, repository_id, launch_source, plans_dir, concurrency, status, started_at, ended_at, run_root_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "run-running",
                        str(repo_path.resolve()),
                        "multi_run",
                        str((repo_path / "plans").resolve()),
                        1,
                        "running",
                        "2026-03-25T13:00:00+00:00",
                        None,
                        str((repo_path / ".kctl" / "runs" / "run-running").resolve()),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO plan_definitions (
                        id, repository_id, file_path, slug, title, objective, content_hash, phase_name, group_name, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"{repo_path.resolve()}:running-plan",
                        str(repo_path.resolve()),
                        str((repo_path / "plans" / "running-plan.yaml").resolve()),
                        "running-plan",
                        None,
                        "Running plan",
                        None,
                        None,
                        None,
                        "2026-03-25T13:00:00+00:00",
                        "2026-03-25T13:00:00+00:00",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO plan_executions (
                        id, run_id, plan_definition_id, status, current_step_key, verify_status, started_at, ended_at, worktree_path, branch_name, log_path, changed_files_count, failure_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "run-running:running-plan",
                        "run-running",
                        f"{repo_path.resolve()}:running-plan",
                        "running",
                        "implement",
                        "not_run",
                        "2026-03-25T13:00:00+00:00",
                        None,
                        str((repo_path / ".kctl" / "worktrees" / "run-running" / "running-plan").resolve()),
                        "kctl/run-running/running-plan",
                        str((repo_path / ".kctl" / "runs" / "run-running" / "running-plan" / "run.json").resolve()),
                        0,
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO workspaces (
                        id, repository_id, plan_execution_id, path, branch_name, base_ref, status, created_at, released_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "workspace-running",
                        str(repo_path.resolve()),
                        "run-running:running-plan",
                        str((repo_path / ".kctl" / "worktrees" / "run-running" / "running-plan").resolve()),
                        "kctl/run-running/running-plan",
                        "main",
                        "active",
                        "2026-03-25T13:00:00+00:00",
                        None,
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            app = DashboardApp(repo_path)
            html = app.render_page(run_id=run_id)

            self.assertIn("blocked", html)
            self.assertIn("running", html)
            # operator action labels replace raw kind names in attention cards
            self.assertIn("Review Workspace", html)
            self.assertIn("Stale — Investigate", html)
            self.assertIn("review_blocked", html)
            self.assertIn("running-plan", html)
            self.assertIn("lifecycle=stale", html)
            self.assertIn("lifecycle=active", html)

    def test_dashboard_renders_action_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_page(action_message="Started run-many for /tmp/plans.")

            self.assertIn("Started run-many for /tmp/plans.", html)

    def test_start_run_many_runs_and_reindexes_in_background(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            target_repo = Path(tmpdir) / "target-repo"
            target_repo.mkdir()
            init_git_repo(repo_path)
            calls: list[tuple[str, object]] = []

            def fake_run_many(
                plans_dir_arg: Path,
                concurrency: int,
                verbose: bool,
                selected_plan_names: list[str] | None = None,
                run_id_override: str | None = None,
                provider_override: str | None = None,
            ) -> int:
                calls.append(
                    (
                        "run_many",
                        plans_dir_arg.resolve(),
                        concurrency,
                        verbose,
                        selected_plan_names,
                        run_id_override,
                        provider_override,
                    )
                )
                return 0

            def fake_index(repo_path_arg: Path, db_path: Path | None = None) -> dict[str, int]:
                calls.append(("index", repo_path_arg.resolve(), db_path))
                return {"repositories": 1, "plan_definitions": 0, "runs": 0, "plan_executions": 0, "step_executions": 0, "workspaces": 0}

            app = DashboardApp(repo_path)
            with patch("kctl_pkg.ui_dashboard.run_many_plans", side_effect=fake_run_many), patch(
                "kctl_pkg.ui_dashboard.index_repository_state", side_effect=fake_index
            ), patch("kctl_pkg.ui_dashboard.threading.Thread") as thread_cls:
                def run_immediately(*args, **kwargs):
                    target = kwargs["target"]
                    class ImmediateThread:
                        def start(self_nonlocal) -> None:
                            target()
                    return ImmediateThread()
                thread_cls.side_effect = run_immediately
                run_id = app.start_run_many(app.plans_dir_for_repo(target_repo), concurrency=2, selected_plan_names=["001-one.yaml"])

            self.assertEqual(
                calls,
                [
                    ("run_many", app.plans_dir_for_repo(target_repo).resolve(), 2, False, ["001-one.yaml"], run_id, None),
                    ("index", repo_path.resolve(), None),
                ],
            )
            self.assertTrue(run_id)

    def test_project_tracking_and_cross_project_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_a = Path(tmpdir) / "project-a"
            project_b = Path(tmpdir) / "project-b"
            init_git_repo(repo_path)
            init_git_repo(project_a)
            init_git_repo(project_b)
            plans_dir = repo_path / ".kctl" / "plans"
            plans_dir.mkdir(parents=True, exist_ok=True)
            plan_path = plans_dir / "001-sample.yaml"
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n"
            )
            calls: list[tuple[str, str]] = []

            app = DashboardApp(repo_path)
            app.add_tracked_project(project_a)
            app.add_tracked_project(project_b)
            tracked = app.load_tracked_projects()
            self.assertIn(str(project_a.resolve()), tracked)
            self.assertIn(str(project_b.resolve()), tracked)

            def fake_run_plan(*, plan_path, repo_override, **kwargs):
                calls.append((str(plan_path), str(repo_override)))
                return 0

            with patch("kctl_pkg.ui_dashboard.run_plan", side_effect=fake_run_plan), patch(
                "kctl_pkg.ui_dashboard.index_repository_state", return_value={}
            ), patch("kctl_pkg.ui_dashboard.threading.Thread") as thread_cls:
                def run_immediately(*args, **kwargs):
                    target = kwargs["target"]

                    class ImmediateThread:
                        def start(self_nonlocal) -> None:
                            target()

                    return ImmediateThread()

                thread_cls.side_effect = run_immediately
                app.start_run_plan_across_projects(
                    plan_path=plan_path,
                    project_paths=[project_a, project_b],
                    provider_override="claude",
                )

            self.assertEqual(
                sorted(calls),
                sorted(
                    [
                        (str(plan_path.resolve()), str(project_a.resolve())),
                        (str(plan_path.resolve()), str(project_b.resolve())),
                    ]
                ),
            )

            app.remove_tracked_project(str(project_a))
            tracked_after = app.load_tracked_projects()
            self.assertNotIn(str(project_a.resolve()), tracked_after)

    def test_add_tracked_project_rejects_duplicate(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_a = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project_a)

            app = DashboardApp(repo_path)
            app.add_tracked_project(project_a)
            with self.assertRaises(PlanError) as ctx:
                app.add_tracked_project(project_a)
            self.assertIn("already tracked", str(ctx.exception).lower())

    def test_dashboard_renders_live_output_for_running_unindexed_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            index_repository_state(repo_path)

            run_id = "20260412T045948708353Z"
            run_root = repo_path / ".kctl" / "runs" / run_id
            plan_root = run_root / "001-review"
            plan_root.mkdir(parents=True, exist_ok=True)
            (run_root / "stream.log").write_text("[001-review] starting plan\n[001-review] running inspect\n")
            (run_root / "run.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "plans_dir": str((repo_path / ".kctl" / "plans").resolve()),
                        "repo": str(repo_path.resolve()),
                        "artifact_storage_mode": "in_repo",
                        "artifact_root_path": str((repo_path / ".kctl" / "runs").resolve()),
                        "stream_log_path": str((run_root / "stream.log").resolve()),
                        "status": "running",
                        "started_at": "2026-04-12T04:59:48.708897+00:00",
                        "concurrency": 1,
                        "plans": [
                            {
                                "plan_id": "001-review",
                                "filename": "001-review.yaml",
                                "plan_path": str((repo_path / ".kctl" / "plans" / "001-review.yaml").resolve()),
                                "status": "running",
                                "current_step": "inspect",
                                "step_statuses": {"inspect": "running"},
                                "worktree_path": str((repo_path / ".kctl" / "worktrees" / run_id / "001-review").resolve()),
                                "branch_name": f"kctl/{run_id}/001-review",
                                "run_output_dir": str(plan_root.resolve()),
                                "log_path": None,
                                "verify_result": "not-run",
                            }
                        ],
                    }
                )
                + "\n"
            )

            app = DashboardApp(repo_path)
            html = app.render_page(run_id=run_id)

            self.assertIn("Live Output", html)
            self.assertIn("live_output_stream", html)
            self.assertIn("[001-review] starting plan", html)
            self.assertIn("/api/run-output", html)

    def test_dashboard_renders_saved_run_preflight_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            run_root = repo_path / ".kctl" / "runs" / run_id
            run_data = json.loads((run_root / "run.json").read_text())
            run_data["preflight"] = {
                "captured_at": "2026-04-12T05:07:33.958781+00:00",
                "source": "launch",
                "repo_root": str(repo_path.resolve()),
                "run_root": str(run_root.resolve()),
                "worktree_root": str((repo_path / ".kctl" / "worktrees" / run_id).resolve()),
                "required_binaries": ["codex", "git"],
                "required_env": ["OPENAI_API_KEY"],
                "issues": [
                    {
                        "code": "missing_env",
                        "message": "Required environment variable 'OPENAI_API_KEY' is not defined.",
                        "fix": "Set 'OPENAI_API_KEY' in the shell or service environment before starting the run.",
                    }
                ],
            }
            (run_root / "run.json").write_text(json.dumps(run_data) + "\n")
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_page(run_id=run_id)

            self.assertIn("Launch Snapshot", html)
            self.assertIn("captured_at=2026-04-12T05:07:33.958781+00:00", html)
            self.assertIn("OPENAI_API_KEY", html)
            self.assertIn("Fix:", html)
            self.assertIn("blockers", html)
            self.assertIn("Copy env", html)

    def test_attention_queue_shows_operator_action_labels_and_rerun_button(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            from kctl_pkg.ui_read import list_plan_executions as _list_plan_executions
            from kctl_pkg.ui_store import UIStateStore
            db_path = default_db_path(repo_path)
            plan_exec = _list_plan_executions(repo_path, run_id)[0]
            run_log_path = Path(plan_exec.log_path or "")
            run_log_data = json.loads(run_log_path.read_text())
            run_log_data["status"] = "failure"
            run_log_data["steps"][-1]["status"] = "failure"
            run_log_data["steps"][-1]["failure_reason"] = "agent_quota_exhausted"
            run_log_data["steps"][-1]["failure_details"] = {
                "reset_hint": "3pm (America/Chicago)"
            }
            run_log_path.write_text(json.dumps(run_log_data, indent=2) + "\n")
            store = UIStateStore(db_path)
            try:
                store.initialize()
                # make it a safe-rerun failed plan (no worktree, so workspace_is_dirty=None)
                store.upsert(
                    "plan_executions",
                    {
                        "id": plan_exec.id,
                        "run_id": run_id,
                        "plan_definition_id": plan_exec.plan_definition_id,
                        "status": "failed",
                        "current_step_key": "implement",
                        "verify_status": "not_run",
                        "started_at": "2026-03-25T12:00:00+00:00",
                        "ended_at": "2026-03-25T12:05:00+00:00",
                        "worktree_path": None,
                        "branch_name": None,
                        "log_path": str(run_log_path),
                        "changed_files_count": 0,
                        "failure_reason": "agent_quota_exhausted",
                    },
                    ["id"],
                )
                store.commit()
            finally:
                store.close()

            app = DashboardApp(repo_path)
            html = app.render_page()

            self.assertIn("Safe to Rerun", html)
            self.assertIn("/actions/rerun-plan", html)
            self.assertIn("Rerun", html)
            self.assertNotIn("Fix Config", html)
            self.assertIn("Blocked by agent quota", html)
            self.assertIn("Retry after: 3pm (America/Chicago)", html)

    def test_attention_queue_shows_fix_config_label_for_preflight_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            from kctl_pkg.ui_read import list_plan_executions as _list_plan_executions
            from kctl_pkg.ui_store import UIStateStore
            db_path = default_db_path(repo_path)
            plan_exec = _list_plan_executions(repo_path, run_id)[0]
            store = UIStateStore(db_path)
            try:
                store.initialize()
                store.upsert(
                    "plan_executions",
                    {
                        "id": plan_exec.id,
                        "run_id": run_id,
                        "plan_definition_id": plan_exec.plan_definition_id,
                        "status": "blocked",
                        "current_step_key": "implement",
                        "verify_status": "not_run",
                        "started_at": "2026-03-25T12:00:00+00:00",
                        "ended_at": "2026-03-25T12:00:00+00:00",
                        "worktree_path": None,
                        "branch_name": None,
                        "log_path": None,
                        "changed_files_count": 0,
                        "failure_reason": "preflight_failed",
                    },
                    ["id"],
                )
                store.commit()
            finally:
                store.close()

            app = DashboardApp(repo_path)
            html = app.render_page()

            self.assertIn("Fix Config", html)
            self.assertIn("Blocked at launch", html)
            self.assertNotIn("/actions/rerun-plan", html)

    def test_create_plan_writes_template_based_plan_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            target_repo = Path(tmpdir) / "target-repo"
            init_git_repo(repo_path)
            init_git_repo(target_repo)
            app = DashboardApp(repo_path)
            created_path = app.create_plan(
                target_repo=target_repo,
                template_name="single_step",
                output_name="001-sample.yaml",
                objective="Add a small UI improvement",
                force=False,
            )

            expected_path = app.plans_dir_for_repo(target_repo) / "001-sample.yaml"
            self.assertEqual(created_path, expected_path)
            contents = expected_path.read_text()
            self.assertIn("objective: Add a small UI improvement", contents)
            self.assertIn(f"repo: {target_repo}", contents)
            self.assertIn("id: implement", contents)

    def test_dashboard_renders_plan_file_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_dir = repo_path / ".kctl" / "plans"
            plan_dir.mkdir(parents=True, exist_ok=True)
            plan_path = plan_dir / "001-sample.yaml"
            plan_path.write_text("repo: /tmp\nobjective: review\nsteps:\n  - id: inspect\n    prompt: look\n")
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_page(selected_plan_file=str(plan_path))

            self.assertIn("001-sample.yaml", html)
            self.assertIn("objective: review", html)


if __name__ == "__main__":
    unittest.main()
