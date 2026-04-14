from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from http import HTTPStatus
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
from kctl_pkg.ui_dashboard_http import handle_api_get, handle_page_get, resolve_action_redirect
from kctl_pkg.ui_index import default_db_path, index_repository_state
from tests.test_ui_index import init_git_repo, write_sample_plan_run


class UIDashboardTests(unittest.TestCase):
    def test_handle_api_get_preflight_passes_provider_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)

            calls: list[tuple[str, str, list[str] | None, str | None]] = []

            def fake_summarize(
                target_repo_value: str,
                plans_dir_value: str,
                selected_plan_names: list[str] | None = None,
                provider_override: str | None = None,
            ) -> dict[str, object]:
                calls.append((target_repo_value, plans_dir_value, selected_plan_names, provider_override))
                return {"status": "pass"}

            response = handle_api_get(
                app,
                "/api/preflight",
                {
                    "target_repo": [str(repo_path)],
                    "plans_dir": [str(repo_path / ".kctl" / "plans")],
                    "selected_plans": ["001-a.yaml"],
                    "provider_override": ["claude"],
                },
                summarize_preflight=fake_summarize,
            )

            self.assertIsNotNone(response)
            assert response is not None
            status_code, content_type, body = response
            self.assertEqual(status_code, HTTPStatus.OK)
            self.assertEqual(content_type, "application/json; charset=utf-8")
            self.assertEqual(json.loads(body.decode("utf-8")), {"status": "pass"})
            self.assertEqual(
                calls,
                [(str(repo_path), str(repo_path / ".kctl" / "plans"), ["001-a.yaml"], "claude")],
            )

    def test_handle_api_get_run_output_reads_live_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            with patch.object(app, "load_live_run_data", return_value={"run_id": "run-1"}), patch.object(
                app,
                "read_live_output",
                return_value=("stream text", "/tmp/run-1/stream.log", "running"),
            ):
                response = handle_api_get(
                    app,
                    "/api/run-output",
                    {"run_id": ["run-1"]},
                    summarize_preflight=lambda *args, **kwargs: {},
                )

            self.assertIsNotNone(response)
            assert response is not None
            _, _, body = response
            self.assertEqual(
                json.loads(body.decode("utf-8")),
                {
                    "run_id": "run-1",
                    "status": "running",
                    "output_path": "/tmp/run-1/stream.log",
                    "output": "stream text",
                },
            )

    def test_handle_page_get_rejects_unknown_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)

            from kctl_pkg.types import PlanError

            with self.assertRaises(PlanError) as ctx:
                handle_page_get(app, "/missing", {})

            self.assertIn("Not Found", str(ctx.exception))

    def test_resolve_action_redirect_keeps_project_detail_message_when_present(self) -> None:
        action_result = SimpleNamespace(
            redirect_to="/projects/detail?path=%2Ftmp%2Frepo&message=existing",
            message="ignored",
            run_id=None,
        )

        location = resolve_action_redirect(action_result, "new message")

        self.assertEqual(location, "/projects/detail?path=%2Ftmp%2Frepo&message=existing")

    def test_resolve_action_redirect_builds_run_root_location(self) -> None:
        action_result = SimpleNamespace(redirect_to="/", message="Started.", run_id="run-123")

        location = resolve_action_redirect(action_result, "Started.")

        self.assertIn("run_id=run-123", location)
        self.assertIn("message=Started.", location)

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
            self.assertIn("lifecycle-released", html)
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
            self.assertIn("lifecycle-stale", html)
            self.assertIn("lifecycle-active", html)

    def test_dashboard_renders_action_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_page(action_message="Started run-many for /tmp/plans.")

            self.assertIn("Started run-many for /tmp/plans.", html)

    def test_render_route_dispatches_root_dashboard_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_route("/", {"run_id": [run_id]})

            self.assertIn("Run Detail", html)
            self.assertIn(run_id, html)
            self.assertIn("Plan Executions", html)

    def test_render_route_dispatches_runs_detail_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_route("/runs/detail", {"id": [run_id]})

            self.assertIn("&larr; Dashboard", html)
            self.assertIn("Plan Executions", html)
            self.assertIn("Live Output", html)
            self.assertIn(run_id, html)

    def test_render_route_rejects_runs_detail_without_id(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            with self.assertRaises(PlanError) as ctx:
                app.render_route("/runs/detail", {})

            self.assertIn("Run id is required", str(ctx.exception))

    def test_render_route_dispatches_selected_plan_file_from_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_dir = repo_path / ".kctl" / "plans"
            plan_dir.mkdir(parents=True, exist_ok=True)
            plan_path = plan_dir / "001-sample.yaml"
            plan_path.write_text("repo: /tmp\nobjective: inspect\nsteps:\n  - id: inspect\n    prompt: look\n")
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_route("/", {"selected_plan_file": [str(plan_path)]})

            self.assertIn("Plan File", html)
            self.assertIn("001-sample.yaml", html)
            self.assertIn("objective: inspect", html)

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

    def test_handle_action_run_many_returns_redirect_and_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            target_repo = Path(tmpdir) / "target-repo"
            init_git_repo(repo_path)
            init_git_repo(target_repo)

            app = DashboardApp(repo_path)
            with patch.object(app, "start_run_many", return_value="run-123") as start_run_many_mock:
                result = app.handle_action(
                    "/actions/run-many",
                    {
                        "target_repo": [str(target_repo)],
                        "concurrency": ["2"],
                        "selected_plans": ["001-one.yaml"],
                        "provider_override": ["claude"],
                    },
                )

            self.assertEqual(result.redirect_to, "/")
            self.assertEqual(result.run_id, "run-123")
            self.assertIn("Started plan run for 001-one.yaml", result.message)
            self.assertEqual(start_run_many_mock.call_args.kwargs["provider_override"], "claude")

    def test_handle_action_start_session_uses_default_provider_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)

            app = DashboardApp(repo_path)
            with patch("kctl_pkg.ui_dashboard.available_providers", return_value=[("claude", "Claude")]), patch.object(
                app,
                "start_agent_session",
                return_value="session-123",
            ) as start_session_mock:
                result = app.handle_action(
                    "/actions/start-session",
                    {
                        "project_path": [str(project_path)],
                        "prompt": ["inspect this repo"],
                    },
                )

            self.assertEqual(result.redirect_to, "/sessions/detail?id=session-123")
            self.assertEqual(result.message, "Session started.")
            self.assertEqual(start_session_mock.call_args.kwargs["provider"], "claude")

    def test_handle_action_rerun_plan_requires_existing_plan_file(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            app = DashboardApp(repo_path)
            with self.assertRaises(PlanError) as ctx:
                app.handle_action("/actions/rerun-plan", {"plan_file_path": [str(Path(tmpdir) / "missing.yaml")]})

            self.assertIn("Plan file not found", str(ctx.exception))

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
            self.assertIn("Retry after", html)
            self.assertIn("3pm (America/Chicago)", html)

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


class AgentSessionTests(unittest.TestCase):
    # ------------------------------------------------------------------
    # Storage helpers
    # ------------------------------------------------------------------

    def test_list_sessions_returns_empty_when_no_sessions_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            self.assertEqual(app.list_sessions(), [])

    def test_list_sessions_returns_sessions_sorted_newest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            for session_id, started_at in [
                ("20260101-000001-aabbccdd", "2026-01-01T00:00:01+00:00"),
                ("20260101-000002-aabbccdd", "2026-01-01T00:00:02+00:00"),
            ]:
                meta: dict[str, object] = {
                    "id": session_id,
                    "project_path": tmpdir,
                    "prompt": "do a thing",
                    "provider": "codex",
                    "status": "completed",
                    "started_at": started_at,
                }
                app._write_session_meta(meta)

            sessions = app.list_sessions()
            self.assertEqual(len(sessions), 2)
            self.assertEqual(sessions[0]["id"], "20260101-000002-aabbccdd")
            self.assertEqual(sessions[1]["id"], "20260101-000001-aabbccdd")

    def test_read_session_output_returns_empty_for_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            self.assertEqual(app.read_session_output("nonexistent-id"), "")

    def test_read_session_output_returns_log_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "20260101-000001-aabbccdd"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("agent output line\n")
            self.assertEqual(app.read_session_output(session_id), "agent output line\n")

    def test_get_session_returns_none_for_missing_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            self.assertIsNone(app.get_session("nonexistent"))

    def test_get_session_returns_meta_for_existing_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "20260101-000001-aabbccdd",
                "project_path": tmpdir,
                "prompt": "do a thing",
                "provider": "claude",
                "status": "running",
                "started_at": "2026-01-01T00:00:01+00:00",
            }
            app._write_session_meta(meta)
            result = app.get_session("20260101-000001-aabbccdd")
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["prompt"], "do a thing")

    # ------------------------------------------------------------------
    # start_agent_session
    # ------------------------------------------------------------------

    def test_start_agent_session_rejects_missing_project(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            with self.assertRaises(PlanError):
                app.start_agent_session(
                    project_path=str(Path(tmpdir) / "no-such-dir"),
                    prompt="hello",
                    provider="codex",
                )

    def test_start_agent_session_writes_meta_and_returns_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project = Path(tmpdir) / "project"
            project.mkdir()
            app = DashboardApp(Path(tmpdir) / "repo")
            launched: list[tuple[list[str], str]] = []

            def fake_run(meta: dict, command: list[str], output_path: object) -> None:
                launched.append((command, str(meta["project_path"])))

            with patch.object(app, "_run_session_subprocess", side_effect=fake_run):
                session_id = app.start_agent_session(
                    project_path=str(project),
                    prompt="write a test",
                    provider="codex",
                )

            self.assertTrue(session_id)
            meta = app.get_session(session_id)
            self.assertIsNotNone(meta)
            assert meta is not None
            self.assertEqual(meta["status"], "running")
            self.assertEqual(meta["prompt"], "write a test")
            self.assertEqual(meta["provider"], "codex")
            self.assertEqual(len(launched), 1)
            self.assertIn("codex", launched[0][0])

    def test_start_agent_session_builds_claude_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project = Path(tmpdir) / "project"
            project.mkdir()
            app = DashboardApp(Path(tmpdir) / "repo")
            launched: list[list[str]] = []

            def fake_run(meta: dict, command: list[str], output_path: object) -> None:
                launched.append(command)

            with patch.object(app, "_run_session_subprocess", side_effect=fake_run):
                app.start_agent_session(
                    project_path=str(project),
                    prompt="add logging",
                    provider="claude",
                )

            self.assertEqual(len(launched), 1)
            cmd = launched[0]
            self.assertIn("claude", cmd)
            self.assertIn("--dangerously-skip-permissions", cmd)
            self.assertIn("-p", cmd)
            self.assertIn("add logging", cmd)

    # ------------------------------------------------------------------
    # reply_to_session
    # ------------------------------------------------------------------

    def test_reply_to_session_raises_for_unknown_session(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            with self.assertRaises(PlanError, msg="Session not found"):
                app.reply_to_session("nonexistent", "follow up")

    def test_reply_to_session_raises_when_session_still_running(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "sess-running",
                "project_path": tmpdir,
                "prompt": "first turn",
                "provider": "codex",
                "provider_session_id": "uuid-1",
                "status": "running",
                "started_at": "2026-01-01T00:00:01+00:00",
                "messages": [{"role": "user", "content": "first turn", "timestamp": "2026-01-01T00:00:01+00:00"}],
            }
            app._write_session_meta(meta)
            with self.assertRaises(PlanError):
                app.reply_to_session("sess-running", "follow up")

    def test_reply_to_session_appends_message_and_relaunches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-done"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("first turn output\n")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "prompt": "first turn",
                "provider": "claude",
                "provider_session_id": "uuid-abc",
                "status": "completed",
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:00:10+00:00",
                "exit_code": 0,
                "messages": [{"role": "user", "content": "first turn", "timestamp": "2026-01-01T00:00:01+00:00"}],
            }
            app._write_session_meta(meta)
            launched: list[list[str]] = []

            def fake_run(m: dict, command: list[str], output_path: object) -> None:
                launched.append(command)

            with patch.object(app, "_run_session_subprocess", side_effect=fake_run):
                app.reply_to_session(session_id, "now do more")

            fresh = app.get_session(session_id)
            assert fresh is not None
            self.assertEqual(fresh["status"], "running")
            messages = list(fresh.get("messages") or [])
            self.assertEqual(len(messages), 2)
            self.assertEqual(messages[1]["content"], "now do more")
            self.assertEqual(len(launched), 1)
            cmd = launched[0]
            self.assertIn("--resume", cmd)
            self.assertIn("uuid-abc", cmd)

    # ------------------------------------------------------------------
    # stop_agent_session
    # ------------------------------------------------------------------

    def test_stop_agent_session_returns_false_for_unknown_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            self.assertFalse(app.stop_agent_session("nonexistent"))

    def test_stop_agent_session_returns_false_when_not_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "sess-done",
                "project_path": tmpdir,
                "prompt": "done",
                "provider": "codex",
                "status": "completed",
                "started_at": "2026-01-01T00:00:01+00:00",
                "pid": None,
            }
            app._write_session_meta(meta)
            self.assertFalse(app.stop_agent_session("sess-done"))

    def test_stop_agent_session_sends_sigterm_to_pid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "sess-running",
                "project_path": tmpdir,
                "prompt": "running",
                "provider": "codex",
                "status": "running",
                "started_at": "2026-01-01T00:00:01+00:00",
                "pid": 99999,
            }
            app._write_session_meta(meta)
            killed: list[tuple[int, int]] = []
            import signal as _signal

            with patch("kctl_pkg.ui_dashboard.os.kill", side_effect=lambda pid, sig: killed.append((pid, sig))):
                result = app.stop_agent_session("sess-running")

            self.assertTrue(result)
            self.assertEqual(killed, [(99999, _signal.SIGTERM)])

    # ------------------------------------------------------------------
    # Page renderers
    # ------------------------------------------------------------------

    def test_render_sessions_page_with_no_tracked_projects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            html = app.render_sessions_page()
            self.assertIn("Sessions", html)
            self.assertIn("Add a project", html)
            self.assertIn("No sessions yet", html)

    def test_render_sessions_page_lists_existing_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project = Path(tmpdir) / "my-project"
            init_git_repo(project)
            app = DashboardApp(Path(tmpdir) / "repo")
            app.add_tracked_project(project)
            meta: dict[str, object] = {
                "id": "20260101-000001-aabbccdd",
                "project_path": str(project),
                "project_name": "my-project",
                "prompt": "fix the bug",
                "provider": "claude",
                "status": "completed",
                "started_at": "2026-01-01T00:00:01+00:00",
            }
            app._write_session_meta(meta)
            html = app.render_sessions_page()
            self.assertIn("my-project", html)
            self.assertIn("fix the bug", html)
            self.assertIn("completed", html)
            self.assertIn("claude", html)
            self.assertIn("/sessions/detail?id=20260101-000001-aabbccdd", html)

    def test_render_session_detail_page_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            html = app.render_session_detail_page("no-such-id")
            self.assertIn("Session not found", html)

    def test_render_session_detail_page_shows_meta_and_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "20260101-000001-aabbccdd"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("agent did a lot\n")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": "/some/path",
                "project_name": "my-project",
                "prompt": "add tests",
                "provider": "codex",
                "provider_session_id": "uuid-xyz",
                "status": "completed",
                "exit_code": 0,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:05:00+00:00",
                "pid": None,
                "token_warning": None,
                "messages": [
                    {"role": "user", "content": "add tests", "timestamp": "2026-01-01T00:00:01+00:00"},
                ],
            }
            app._write_session_meta(meta)
            html = app.render_session_detail_page(session_id)
            self.assertIn("my-project", html)
            self.assertIn("add tests", html)
            self.assertIn("codex", html)
            self.assertIn("completed", html)
            self.assertIn("agent did a lot", html)
            self.assertIn("Exit code", html)
            self.assertIn("reply", html.lower())

    def test_render_session_detail_page_shows_stop_button_when_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-running"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "go",
                "provider": "claude",
                "provider_session_id": "uuid-1",
                "status": "running",
                "exit_code": None,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": None,
                "pid": 12345,
                "token_warning": None,
                "messages": [{"role": "user", "content": "go", "timestamp": "2026-01-01T00:00:01+00:00"}],
            }
            app._write_session_meta(meta)
            html = app.render_session_detail_page(session_id)
            self.assertIn("Stop Session", html)
            self.assertIn("/actions/stop-session", html)

    def test_render_session_detail_page_shows_token_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-quota"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("you have hit your limit\n")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "too much",
                "provider": "claude",
                "provider_session_id": "uuid-2",
                "status": "failed",
                "exit_code": 1,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:01:00+00:00",
                "pid": None,
                "token_warning": "you have hit your limit",
                "messages": [],
            }
            app._write_session_meta(meta)
            html = app.render_session_detail_page(session_id)
            self.assertIn("Token/quota warning", html)
            self.assertIn("hit your limit", html)


if __name__ == "__main__":
    unittest.main()
