from __future__ import annotations

import json
import io
import signal
import sqlite3
import tempfile
import unittest
from http import HTTPStatus
from contextlib import redirect_stdout
from io import BytesIO
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
from kctl_pkg.types import PlanError
from kctl_pkg.ui_dashboard_support import (
    _detect_token_warning,
    _display_status_label,
    _failure_reason_label,
    _fmt_ts,
    _lifecycle_badge,
    _link,
    _normalize_token_warning_text,
    _operator_action_label,
    _page_link,
    _provider_select_html,
    _preflight_run_snapshot,
    _render_action_button,
    _render_attention_card,
    _render_collapsible_section,
    _render_preflight_item_html,
    _render_selection_list,
    _run_detail_link,
    _status_badge,
    _status_class,
    available_providers,
    read_plan_file,
)
from kctl_pkg.ui_dashboard_server import serve_dashboard
from kctl_pkg.ui_dashboard_http import handle_api_get, handle_page_get, resolve_action_redirect
from kctl_pkg.ui_index import default_db_path, index_repository_state
from kctl_pkg.ui_dashboard_sessions import (
    _format_session_ts,
    _render_transcript_html,
    _session_status_counts,
    _split_session_output_turns,
    _tail_lines_text,
    list_sessions as list_sessions_direct,
    reply_to_session as reply_to_session_direct,
    run_session_subprocess,
    start_agent_session as start_agent_session_direct,
    stop_agent_session as stop_agent_session_direct,
    write_session_meta as write_session_meta_direct,
)
from kctl_pkg.ui_read import AttentionItem
from tests.test_ui_index import init_git_repo, write_sample_plan_run


class UIDashboardTests(unittest.TestCase):
    def test_render_collapsible_section_helper(self) -> None:
        html = _render_collapsible_section(
            "Launch Snapshot",
            "<div>body</div>",
            heading_tag="h3",
            open_by_default=True,
            details_id="snapshot",
            style="margin-top:12px",
        )

        self.assertIn("<details class='panel actions-details' id='snapshot' style='margin-top:12px' open>", html)
        self.assertIn("<summary><h3 class='inline-heading'>Launch Snapshot</h3></summary>", html)
        self.assertIn("<div>body</div>", html)

    def test_render_selection_list_helper(self) -> None:
        html = _render_selection_list(
            "selected_plans",
            [("001-a.yaml", "001-a.yaml"), ("002-b.yaml", "002-b.yaml")],
            heading="Plans found",
            selected_values={"002-b.yaml"},
            item_class="",
        )

        self.assertIn("<strong>Plans found</strong>", html)
        self.assertIn("name='selected_plans'", html)
        self.assertIn("value='001-a.yaml'", html)
        self.assertIn("value='002-b.yaml' checked", html)
        self.assertIn("class='selection-list-item'", html)
        self.assertIn("class='selection-list-control'", html)
        self.assertIn("class='selection-list-label'>002-b.yaml</span>", html)

    def test_render_action_button_helper(self) -> None:
        html = _render_action_button("Save", action_name="kctlTestAction", button_id="save_btn")

        self.assertIn("id='save_btn'", html)
        self.assertIn("href='#'", html)
        self.assertIn("role='button'", html)
        self.assertIn('window.kctlActionButtonClick(this, "kctlTestAction")', html)
        self.assertIn('window.kctlKeyActionButton(this, "kctlTestAction", event)', html)
        self.assertIn("ontouchend=", html)
        self.assertIn("onpointerup=", html)

    def test_support_helper_primitives(self) -> None:
        self.assertEqual(_status_class("success"), "status-success")
        self.assertEqual(_status_class("blocked"), "status-failure")
        self.assertEqual(_status_class("stopping"), "status-running")
        self.assertEqual(_status_class("mystery"), "status-neutral")
        self.assertEqual(_link({"run_id": "r1"}, plan_execution_id="p1"), "/?run_id=r1&plan_execution_id=p1")
        self.assertEqual(_link({"run_id": "r1"}, run_id=None), "/")
        self.assertEqual(_page_link("/runs/r1", message="done"), "/runs/r1?message=done")
        self.assertEqual(_operator_action_label("active"), "Running")
        self.assertEqual(_operator_action_label("custom"), "custom")
        self.assertEqual(_failure_reason_label("agent_rate_limited"), "Blocked by agent rate limit")
        self.assertEqual(_failure_reason_label("agent_failed"), "Agent failed")
        self.assertEqual(_display_status_label("blocked", "run_stopped"), "stopped")
        self.assertEqual(_display_status_label("running", None), "running")
        self.assertEqual(_fmt_ts(None), "—")
        self.assertEqual(_fmt_ts("2026-01-01T00:00:01+00:00"), "2026-01-01 00:00:01 UTC")
        self.assertIn("released", _lifecycle_badge("released"))
        self.assertIn("stopped", _status_badge("blocked", failure_reason="run_stopped"))

    def test_support_helpers_for_provider_and_preflight_markup(self) -> None:
        with patch("kctl_pkg.ui_dashboard_support.shutil.which", side_effect=lambda name: f"/usr/bin/{name}" if name == "codex" else None):
            self.assertEqual(available_providers(), [("codex", "Codex")])

        provider_html = _provider_select_html("provider_override", [("codex", "Codex")])
        self.assertIn("Default (from plan)", provider_html)
        self.assertIn("Provider Override", provider_html)

        preflight_html = _render_preflight_item_html(
            "Binaries",
            {
                "status": "blocked",
                "summary": "Missing binary",
                "details": "codex",
                "remediation": "Install codex",
                "action_label": "Copy binary",
                "action_value": "codex",
            },
        )
        self.assertIn("preflight-badge-block", preflight_html)
        self.assertIn("Copy binary", preflight_html)

    def test_token_warning_helpers(self) -> None:
        self.assertIsNone(_normalize_token_warning_text(""))
        self.assertIsNone(_normalize_token_warning_text('self.assertIn("hit your limit", html)'))
        self.assertEqual(_normalize_token_warning_text("You have hit your limit"), "You have hit your limit")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.log"
            output_path.write_text("ok\nrate limit reached\n")
            self.assertEqual(_detect_token_warning(output_path), "rate limit reached")
            missing_path = Path(tmpdir) / "missing.log"
            self.assertIsNone(_detect_token_warning(missing_path))

    def test_run_detail_link_helper_builds_real_run_path(self) -> None:
        self.assertEqual(_run_detail_link("run-123"), "/runs/run-123")
        self.assertEqual(
            _run_detail_link("run-123", plan_execution_id="run-123:001-plan"),
            "/runs/run-123?plan_execution_id=run-123%3A001-plan",
        )

    def test_session_helpers(self) -> None:
        self.assertEqual(_tail_lines_text("a\nb\nc\n", line_count=2), "b\nc")
        self.assertEqual(
            _split_session_output_turns(
                "first\n\n"
                + f"{'─' * 60}\n"
                + "[follow-up #2]\n"
                + f"{'─' * 60}\n\n"
                + "second\n"
            ),
            ["first", "second"],
        )
        self.assertEqual(_format_session_ts("2026-01-01T00:00:01+00:00"), "2026-01-01 00:00:01+00:00 UTC")
        self.assertEqual(_session_status_counts([{"status": "running"}, {"status": "completed"}, {"status": "failed"}]), (1, 1, 1))

    def test_render_transcript_html_handles_prompt_only_and_empty_state(self) -> None:
        html = _render_transcript_html([], "", status="completed", provider_label="Codex", prompt="seed prompt")
        self.assertIn("seed prompt", html)
        self.assertIn("Codex", html)

        empty_html = _render_transcript_html([], "", status="completed", provider_label="Codex", prompt="")
        self.assertIn("No transcript yet", empty_html)

    def test_render_transcript_html_handles_string_and_agent_messages(self) -> None:
        html = _render_transcript_html(
            [
                "plain user turn",
                {"role": "assistant", "content": "agent turn", "timestamp": "2026-01-01T00:00:02+00:00"},
            ],
            "",
            status="running",
            provider_label="Codex",
            prompt="",
        )
        self.assertIn("plain user turn", html)
        self.assertIn("agent turn", html)
        self.assertIn("session-chat-row-agent", html)
        self.assertIn("2026-01-01 00:00:02+00:00 UTC", html)

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

    def test_handle_api_get_check_repo_returns_status_payload(self) -> None:
        app = SimpleNamespace()
        response = handle_api_get(
            app,
            "/api/check-repo",
            {"path": ["/tmp"]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        payload = json.loads(body.decode("utf-8"))
        self.assertIn("status", payload)
        self.assertIn("message", payload)

    def test_handle_api_get_resolve_path_returns_resolved_value(self) -> None:
        app = SimpleNamespace()
        response = handle_api_get(
            app,
            "/api/resolve-path",
            {"path": ["."]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        self.assertEqual(json.loads(body.decode("utf-8"))["resolved"], str(Path(".").resolve()))

    def test_handle_api_get_list_plans_returns_plan_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            (plans_dir / "001-a.yaml").write_text("repo: /tmp\nobjective: test\nsteps: []\n")
            app = SimpleNamespace()

            response = handle_api_get(
                app,
                "/api/list-plans",
                {"path": [str(plans_dir)]},
                summarize_preflight=lambda *args, **kwargs: {},
            )

            self.assertIsNotNone(response)
            assert response is not None
            _, _, body = response
            payload = json.loads(body.decode("utf-8"))
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["plans"], ["001-a.yaml"])

    def test_handle_api_get_project_git_status_without_path_reports_error(self) -> None:
        app = SimpleNamespace()
        response = handle_api_get(
            app,
            "/api/project-git-status",
            {"path": [""]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        payload = json.loads(body.decode("utf-8"))
        self.assertFalse(payload["available"])
        self.assertEqual(payload["error"], "no path provided")

    def test_handle_api_get_project_remote_check_without_path_reports_error(self) -> None:
        app = SimpleNamespace()
        response = handle_api_get(
            app,
            "/api/project-remote-check",
            {"path": [""], "remote": ["upstream"]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        payload = json.loads(body.decode("utf-8"))
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["remote"], "upstream")
        self.assertEqual(payload["message"], "no path provided")

    def test_handle_api_get_project_git_diff_without_path_returns_empty_diff(self) -> None:
        app = SimpleNamespace()
        response = handle_api_get(
            app,
            "/api/project-git-diff",
            {"path": [""]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        self.assertEqual(json.loads(body.decode("utf-8")), {"diff": ""})

    def test_handle_api_get_session_output_filters_false_positive_warning(self) -> None:
        app = SimpleNamespace(
            get_session=lambda session_id: {
                "status": "completed",
                "messages": [{"role": "user", "content": "hello"}],
                "token_warning": 'self.assertIn("hit your limit", html)',
            },
            read_session_output=lambda session_id: "output text",
        )
        response = handle_api_get(
            app,
            "/api/session-output",
            {"id": ["sess-1"]},
            summarize_preflight=lambda *args, **kwargs: {},
        )

        self.assertIsNotNone(response)
        assert response is not None
        _, _, body = response
        payload = json.loads(body.decode("utf-8"))
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["output"], "output text")
        self.assertEqual(payload["token_warning"], "")

    def test_handle_api_get_unknown_path_returns_none(self) -> None:
        app = SimpleNamespace()
        self.assertIsNone(
            handle_api_get(
                app,
                "/api/unknown",
                {},
                summarize_preflight=lambda *args, **kwargs: {},
            )
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

    def test_dashboard_recent_runs_is_limited_to_five_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            fake_runs = [
                SimpleNamespace(
                    id=f"run-{index}",
                    status="passed",
                    started_at=f"2026-04-14T00:00:0{index}+00:00",
                    concurrency=1,
                )
                for index in range(6)
            ]
            with patch("kctl_pkg.ui_dashboard_runs.list_runs", return_value=fake_runs), patch(
                "kctl_pkg.ui_dashboard_runs.get_repository_overview",
                return_value=SimpleNamespace(
                    run_count=6,
                    active_run_count=0,
                    failed_run_count=0,
                    running_plan_count=0,
                    blocked_plan_count=0,
                    stale_workspace_count=0,
                ),
            ), patch("kctl_pkg.ui_dashboard_runs.list_attention_items", return_value=[]), patch(
                "kctl_pkg.ui_dashboard_runs.available_providers", return_value=[]
            ):
                html = app.render_page()

            self.assertIn("run-0", html)
            self.assertIn("run-4", html)
            self.assertNotIn("run-5", html)

    def test_dashboard_recent_runs_uses_live_stopping_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            fake_runs = [
                SimpleNamespace(
                    id="run-1",
                    status="running",
                    started_at="2026-04-14T00:00:01+00:00",
                    concurrency=1,
                )
            ]
            with patch("kctl_pkg.ui_dashboard_runs.list_runs", return_value=fake_runs), patch(
                "kctl_pkg.ui_dashboard_runs.get_repository_overview",
                return_value=SimpleNamespace(
                    run_count=1,
                    active_run_count=1,
                    failed_run_count=0,
                    running_plan_count=1,
                    blocked_plan_count=0,
                    stale_workspace_count=0,
                ),
            ), patch("kctl_pkg.ui_dashboard_runs.list_attention_items", return_value=[]), patch(
                "kctl_pkg.ui_dashboard_runs.available_providers", return_value=[]
            ), patch.object(
                app,
                "load_live_run_data",
                return_value={"run_id": "run-1", "status": "running", "stop_requested": True, "active_pids": [12345]},
            ):
                html = app.render_page()

            self.assertIn("stopping", html)

    def test_dashboard_recent_runs_uses_live_stopped_status_for_orphaned_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            fake_runs = [
                SimpleNamespace(
                    id="run-1",
                    status="running",
                    started_at="2026-04-14T00:00:01+00:00",
                    concurrency=1,
                )
            ]
            with patch("kctl_pkg.ui_dashboard_runs.list_runs", return_value=fake_runs), patch(
                "kctl_pkg.ui_dashboard_runs.get_repository_overview",
                return_value=SimpleNamespace(
                    run_count=1,
                    active_run_count=1,
                    failed_run_count=0,
                    running_plan_count=1,
                    blocked_plan_count=0,
                    stale_workspace_count=0,
                ),
            ), patch("kctl_pkg.ui_dashboard_runs.list_attention_items", return_value=[]), patch(
                "kctl_pkg.ui_dashboard_runs.available_providers", return_value=[]
            ), patch.object(
                app,
                "load_live_run_data",
                return_value={"run_id": "run-1", "status": "running", "stop_requested": True},
            ):
                html = app.render_page()

            self.assertIn("stopped", html)

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

        self.assertIn("/runs/run-123", location)
        self.assertIn("message=Started.", location)

    def test_resolve_action_redirect_handles_common_routes(self) -> None:
        self.assertEqual(
            resolve_action_redirect(SimpleNamespace(redirect_to="/sessions", run_id=None), "done"),
            "/sessions?message=done",
        )
        self.assertEqual(
            resolve_action_redirect(SimpleNamespace(redirect_to="/actions", run_id=None), "done"),
            "/actions?message=done",
        )
        self.assertEqual(
            resolve_action_redirect(SimpleNamespace(redirect_to="/projects", run_id=None), "done"),
            "/projects?message=done",
        )
        self.assertEqual(
            resolve_action_redirect(SimpleNamespace(redirect_to="/sessions/detail", run_id=None), "done"),
            "/sessions/detail?message=done",
        )

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

    def test_list_plans_in_directory_handles_empty_missing_and_not_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plans_dir = Path(tmpdir) / "plans"
            plans_dir.mkdir()
            file_path = Path(tmpdir) / "plans.txt"
            file_path.write_text("x\n")

            self.assertEqual(list_plans_in_directory("")[0], "empty")
            self.assertEqual(list_plans_in_directory(str(Path(tmpdir) / "missing"))[0], "missing")
            self.assertEqual(list_plans_in_directory(str(file_path))[0], "not_dir")
            self.assertEqual(list_plans_in_directory(str(plans_dir))[0], "empty")

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

    def test_summarize_preflight_for_dashboard_blocks_on_invalid_inputs_and_plan_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            plans_dir = repo_path / ".kctl" / "plans"
            init_git_repo(repo_path)
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-first.yaml").write_text("placeholder\n")

            blocked = summarize_preflight_for_dashboard("", "")
            self.assertEqual(blocked["status"], "block")
            self.assertEqual(blocked["decision"], "Blocked")

            with patch("kctl_pkg.ui_dashboard.load_normalized_multi_plans", side_effect=PlanError("bad plans")):
                errored = summarize_preflight_for_dashboard(str(repo_path), str(plans_dir))

            self.assertEqual(errored["status"], "block")
            self.assertEqual(errored["items"]["plans_dir"]["status"], "block")
            self.assertIn("bad plans", errored["message"])

    def test_summarize_preflight_for_dashboard_reports_blocking_issues_and_codex_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            plans_dir = repo_path / ".kctl" / "plans"
            init_git_repo(repo_path)
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-first.yaml").write_text("placeholder\n")
            plan_specs = [SimpleNamespace(filename="001-first.yaml")]
            normalized_plan = {
                "defaults": {"provider": "claude", "permission_mode": "manual"},
                "steps": [{"id": "implement"}],
            }

            class FakeIssue:
                def __init__(self, code: str, message: str, fix: str) -> None:
                    self.code = code
                    self.message = message
                    self.fix = fix

            class FakeReport:
                ok = False
                repo_root = repo_path
                run_root = repo_path / ".kctl" / "runs" / "r1"
                worktree_root = repo_path / ".kctl" / "worktrees" / "r1"
                required_binaries = ["codex"]
                required_env = ["OPENAI_API_KEY"]
                issues = [
                    FakeIssue("missing_binary", "Missing codex", "Install codex"),
                    FakeIssue("workspace_dir_unwritable", "Workspace path not writable", "Fix permissions"),
                    FakeIssue("missing_env", "OPENAI_API_KEY missing", "Export OPENAI_API_KEY"),
                ]

            with patch(
                "kctl_pkg.ui_dashboard.load_normalized_multi_plans",
                return_value=(plan_specs, {"001-first": normalized_plan}),
            ) as load_mock, patch(
                "kctl_pkg.ui_dashboard.preflight_multi_run",
                return_value=FakeReport(),
            ) as preflight_mock:
                summary = summarize_preflight_for_dashboard(
                    str(repo_path),
                    str(plans_dir),
                    selected_plan_names=["001-first.yaml"],
                    provider_override="codex",
                )

            load_mock.assert_called_once()
            mutated = preflight_mock.call_args.kwargs["normalized_plans"]["001-first"]
            self.assertEqual(mutated["_kctl_provider"], "codex")
            self.assertEqual(mutated["_kctl_permission_mode"], "auto")
            self.assertNotIn("permission_mode", mutated["defaults"])
            self.assertEqual(summary["status"], "block")
            self.assertEqual(summary["items"]["binaries"]["summary"], "Missing codex")
            self.assertEqual(summary["items"]["required_env"]["summary"], "OPENAI_API_KEY missing")
            self.assertIn("001-first.yaml", summary["items"]["plans_dir"]["details"])

    def test_preflight_run_snapshot_handles_issue_and_non_issue_shapes(self) -> None:
        self.assertIsNone(_preflight_run_snapshot(None))
        self.assertIsNone(_preflight_run_snapshot({"preflight": []}))

        snapshot = _preflight_run_snapshot(
            {
                "started_at": "2026-01-01T00:00:01+00:00",
                "preflight": {
                    "issues": [
                        {"code": "missing_env", "message": "OPENAI_API_KEY missing", "fix": "Export key"},
                        "ignored",
                    ],
                    "required_binaries": ["codex"],
                    "required_env": ["OPENAI_API_KEY"],
                    "run_root": "/tmp/run",
                    "worktree_root": "/tmp/worktree",
                },
            }
        )
        assert snapshot is not None
        self.assertEqual(snapshot["status"], "block")
        self.assertEqual(snapshot["items"]["required_env"]["summary"], "OPENAI_API_KEY missing")
        self.assertEqual(snapshot["items"]["binaries"]["action_value"], "codex")

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

    def test_build_dashboard_access_urls_handles_direct_host_and_hostname_fallback(self) -> None:
        self.assertEqual(build_dashboard_access_urls("127.0.0.1", 8421), ["http://127.0.0.1:8421"])

        with patch("kctl_pkg.ui_dashboard_support.socket.gethostname", return_value="tail-host"):
            urls = build_dashboard_access_urls("0.0.0.0", 8421, announce_url="http://localhost:8421", tailscale=True)

        self.assertEqual(urls, ["http://localhost:8421", "http://tail-host:8421"])

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
            self.assertIn("<summary><h2 class='inline-heading'>Attention</h2></summary>", html)
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
            self.assertIn("dashboard-primary-column", html)
            self.assertIn("dashboard-secondary-column", html)
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
            self.assertIn("Plans source", actions_html)
            self.assertIn("Save Plan Selection", actions_html)
            self.assertIn("method='get' action='/actions'", actions_html)
            self.assertIn("name='stage' value='project'", actions_html)
            self.assertIn("<summary><h2 class='inline-heading'>Create Plan</h2></summary>", actions_html)
            self.assertIn("data-objective='Make one small, low-risk change in this repo.'", actions_html)
            self.assertIn("onchange=\"var o=this.options[this.selectedIndex];var t=document.getElementById('objective');", actions_html)
            self.assertIn(">Make one small, low-risk change in this repo.</textarea>", actions_html)

    def test_actions_page_prerenders_existing_plans_in_run_plans_container(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plans_dir = repo_path / ".kctl" / "plans"
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-sample.yaml").write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n"
            )

            app = DashboardApp(repo_path)
            actions_html = app.render_actions_page()

            self.assertIn("001-sample.yaml", actions_html)
            self.assertIn("Plans", actions_html)

            project_stage_html = app.render_route(
                "/actions",
                {"selected_plans": ["001-sample.yaml"], "stage": ["project"]},
            )
            self.assertIn("Saved Plan:", project_stage_html)
            self.assertIn("Current Repo", project_stage_html)
            self.assertIn("Save Project Selection", project_stage_html)
            self.assertIn("Choose one or more projects to run this plan.", project_stage_html)
            self.assertIn("Edit Previous Step", project_stage_html)

            concurrency_stage_html = app.render_route(
                "/actions",
                {
                    "selected_plans": ["001-sample.yaml"],
                    "project_paths": [str(repo_path.resolve())],
                    "stage": ["concurrency"],
                },
            )
            self.assertIn("Saved Project:", concurrency_stage_html)
            self.assertIn("Save Concurrency", concurrency_stage_html)
            self.assertIn("run_plans_concurrency", concurrency_stage_html)

            plan_stage_html = app.render_route(
                "/actions",
                {"selected_plans": ["001-sample.yaml"], "stage": ["plan"]},
            )
            self.assertIn("Save Plan Selection", plan_stage_html)
            self.assertNotIn("Save Project Selection", plan_stage_html)

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

    def test_render_route_dispatches_real_run_detail_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_route(f"/runs/{run_id}", {})

            self.assertIn("&larr; Dashboard", html)
            self.assertIn("Plan Executions", html)
            self.assertIn(run_id, html)

    def test_render_route_dispatches_run_stop_confirm_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            html = app.render_route(f"/runs/{run_id}/stop", {})

            self.assertIn("Stop Run", html)
            self.assertIn("Confirm Stop Run", html)
            self.assertIn(run_id, html)

    def test_render_route_run_stop_confirm_requests_stop_and_returns_run_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            app = DashboardApp(repo_path)
            with patch.object(app, "stop_run_many", return_value=True) as stop_run_many_mock:
                html = app.render_route(f"/runs/{run_id}/stop", {"message": ["confirm"]})

            stop_run_many_mock.assert_called_once_with(run_id)
            self.assertIn("Stop requested for", html)
            self.assertIn("Plan Executions", html)

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

    def test_render_route_dispatches_actions_projects_and_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)
            plans_dir = repo_path / ".kctl" / "plans"
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-one.yaml").write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n"
            )
            app = DashboardApp(repo_path)
            app.add_tracked_project(project_path)

            actions_html = app.render_route(
                "/actions",
                {
                    "selected_plans": ["001-one.yaml"],
                    "project_paths": [str(project_path)],
                    "concurrency": ["2"],
                    "stage": ["concurrency"],
                },
            )
            projects_html = app.render_route("/projects", {"message": ["done"]})
            project_detail_html = app.render_route("/projects/detail", {"path": [str(project_path)]})
            sessions_html = app.render_route("/sessions", {"project": [str(project_path)]})

            self.assertIn("Run Plans", actions_html)
            self.assertIn("Concurrency", actions_html)
            self.assertIn("Tracked Projects", projects_html)
            self.assertIn(str(project_path), project_detail_html)
            self.assertIn("Launch Session", sessions_html)
            self.assertIn("selected", sessions_html)

    def test_render_route_dispatches_session_detail_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-1"
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "hello",
                "provider": "codex",
                "status": "completed",
                "started_at": "2026-01-01T00:00:01+00:00",
                "messages": [],
            }
            app._write_session_meta(meta)

            html_query = app.render_route("/sessions/detail", {"id": [session_id]})
            html_path = app.render_route(f"/sessions/{session_id}", {})

            self.assertIn("proj", html_query)
            self.assertIn("proj", html_path)

    def test_render_route_rejects_invalid_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)

            with self.assertRaises(PlanError):
                app.render_route("/runs/", {})
            with self.assertRaises(PlanError):
                app.render_route("/runs/run-1/other", {})
            with self.assertRaises(PlanError):
                app.render_route("/sessions/", {})
            with self.assertRaises(PlanError):
                app.render_route("/nope", {})

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
                repo_override: Path | None = None,
                from_step: str | None = None,
                only_step: str | None = None,
                reuse_workspace_path: Path | None = None,
                stop_requested=None,
                process_started=None,
                process_finished=None,
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
                        repo_override,
                        from_step,
                        only_step,
                        reuse_workspace_path,
                        callable(stop_requested),
                        callable(process_started),
                        callable(process_finished),
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
                    ("run_many", app.plans_dir_for_repo(target_repo).resolve(), 2, False, ["001-one.yaml"], run_id, None, None, None, None, None, True, True, True),
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
                        "project_paths": [str(target_repo)],
                        "provider_override": ["claude"],
                    },
                )

            self.assertEqual(result.redirect_to, "/")
            self.assertEqual(result.run_id, "run-123")
            self.assertIn("Started plan run for 001-one.yaml", result.message)
            self.assertEqual(start_run_many_mock.call_args.kwargs["provider_override"], "claude")
            self.assertEqual(start_run_many_mock.call_args.kwargs["repo_override"], target_repo.resolve())

    def test_handle_action_run_many_uses_tracked_projects_for_cross_project_launch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_a = Path(tmpdir) / "project-a"
            project_b = Path(tmpdir) / "project-b"
            init_git_repo(repo_path)
            init_git_repo(project_a)
            init_git_repo(project_b)
            plans_dir = repo_path / ".kctl" / "plans"
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / "001-one.yaml").write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: inspect\n    prompt: x\n"
            )

            app = DashboardApp(repo_path)
            with patch.object(app, "start_run_plan_across_projects", return_value="run-xyz") as start_cross_mock:
                result = app.handle_action(
                    "/actions/run-many",
                    {
                        "target_repo": [str(repo_path)],
                        "concurrency": ["1"],
                        "selected_plans": ["001-one.yaml"],
                        "project_paths": [str(project_a), str(project_b)],
                        "provider_override": ["claude"],
                    },
                )

            self.assertEqual(result.redirect_to, "/actions")
            self.assertIsNone(result.run_id)
            self.assertIn("across 2 project(s)", result.message)
            self.assertEqual(start_cross_mock.call_args.kwargs["provider_override"], "claude")
            self.assertEqual(
                sorted(str(path) for path in start_cross_mock.call_args.kwargs["project_paths"]),
                sorted([str(project_a.resolve()), str(project_b.resolve())]),
            )

    def test_handle_action_run_many_multiple_plans_target_one_project(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_a = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project_a)

            app = DashboardApp(repo_path)
            with patch.object(app, "start_run_many", return_value="run-456") as start_run_many_mock:
                result = app.handle_action(
                    "/actions/run-many",
                    {
                        "target_repo": [str(repo_path)],
                        "concurrency": ["1"],
                        "selected_plans": ["001-one.yaml", "002-two.yaml"],
                        "project_paths": [str(project_a)],
                    },
                )

            self.assertEqual(result.redirect_to, "/")
            self.assertEqual(result.run_id, "run-456")
            self.assertIn("Started 2 plans", result.message)
            self.assertEqual(start_run_many_mock.call_args.kwargs["repo_override"], project_a.resolve())

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

    def test_handle_action_project_tracking_and_index_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)
            app = DashboardApp(repo_path)

            add_result = app.handle_action("/actions/add-project", {"project_path": [str(project_path)]})
            self.assertEqual(add_result.redirect_to, "/projects")
            self.assertIn(str(project_path.resolve()), add_result.message)

            index_result = app.handle_action("/actions/index", {})
            self.assertEqual(index_result.redirect_to, "/actions")
            self.assertEqual(index_result.message, "Index refreshed.")

            remove_result = app.handle_action("/actions/remove-project", {"project_path": [str(project_path)]})
            self.assertEqual(remove_result.redirect_to, "/projects")
            self.assertIn("Removed tracked project", remove_result.message)

    def test_handle_action_create_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            target_repo = Path(tmpdir) / "target"
            init_git_repo(repo_path)
            init_git_repo(target_repo)
            app = DashboardApp(repo_path)

            result = app.handle_action(
                "/actions/create-plan",
                {
                    "target_repo": [str(target_repo)],
                    "template_name": ["tooling_change"],
                    "output_path": ["001-new-plan.yaml"],
                    "objective": ["Do a thing"],
                },
            )

            self.assertEqual(result.redirect_to, "/actions")
            self.assertIn("Created plan at", result.message)
            self.assertTrue((target_repo / ".kctl" / "plans" / "001-new-plan.yaml").exists())

    def test_handle_action_stop_and_reply_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)
            app = DashboardApp(repo_path)

            with patch.object(app, "stop_agent_session", return_value=True) as stop_mock:
                stop_result = app.handle_action("/actions/stop-session", {"session_id": ["sess-1"]})
            self.assertEqual(stop_result.redirect_to, "/sessions/detail?id=sess-1")
            self.assertEqual(stop_result.message, "Session stop signal sent.")
            stop_mock.assert_called_once_with("sess-1")

            with patch.object(app, "reply_to_session") as reply_mock:
                reply_result = app.handle_action(
                    "/actions/session-reply",
                    {"session_id": ["sess-1"], "reply": ["continue"]},
                )
            self.assertEqual(reply_result.redirect_to, "/sessions/detail?id=sess-1")
            self.assertEqual(reply_result.message, "Reply sent.")
            reply_mock.assert_called_once_with("sess-1", "continue")

    def test_handle_action_project_git_branches_and_remote_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)
            app = DashboardApp(repo_path)

            with patch("kctl_pkg.ui_dashboard.create_branch") as create_branch_mock:
                result = app.handle_action(
                    "/actions/project-git-create-branch",
                    {"path": [str(project_path)], "branch": ["feature/test"]},
                )
            self.assertIn("Created and switched to feature/test", result.message)
            create_branch_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.switch_branch") as switch_branch_mock:
                result = app.handle_action(
                    "/actions/project-git-switch",
                    {"path": [str(project_path)], "branch": ["main"]},
                )
            self.assertIn("Switched to main", result.message)
            switch_branch_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.git_pull", return_value="Already up to date.") as pull_mock:
                result = app.handle_action(
                    "/actions/project-git-pull",
                    {"path": [str(project_path)], "remote": ["origin"]},
                )
            self.assertIn("Pulled from origin", result.message)
            pull_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.git_push", side_effect=[PlanError("no upstream"), "done"]) as push_mock:
                result = app.handle_action(
                    "/actions/project-git-push",
                    {"path": [str(project_path)], "remote": ["origin"]},
                )
            self.assertIn("Pushed to origin", result.message)
            self.assertEqual(push_mock.call_count, 2)

    def test_handle_action_project_git_stash_commit_and_discard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            init_git_repo(repo_path)
            init_git_repo(project_path)
            app = DashboardApp(repo_path)

            with patch("kctl_pkg.ui_dashboard.git_stash_save") as stash_mock:
                result = app.handle_action(
                    "/actions/project-git-stash",
                    {"path": [str(project_path)], "stash_message": ["wip"]},
                )
            self.assertEqual(result.message, "Changes stashed")
            stash_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.git_stash_pop") as stash_pop_mock:
                result = app.handle_action("/actions/project-git-stash-pop", {"path": [str(project_path)]})
            self.assertEqual(result.message, "Stash popped")
            stash_pop_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.stage_and_commit", return_value="abc123") as commit_mock:
                result = app.handle_action(
                    "/actions/project-git-commit",
                    {"path": [str(project_path)], "message": ["commit msg"]},
                )
            self.assertIn("Committed abc123", result.message)
            commit_mock.assert_called_once()

            with patch("kctl_pkg.ui_dashboard.discard_all_changes") as discard_mock:
                result = app.handle_action("/actions/project-git-discard", {"path": [str(project_path)]})
            self.assertEqual(result.message, "All changes discarded")
            discard_mock.assert_called_once()

    def test_handle_action_rejects_missing_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)

            with self.assertRaises(PlanError):
                app.handle_action("/actions/add-project", {})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/create-plan", {})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/start-session", {})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/session-reply", {})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/project-git-commit", {"path": [tmpdir]})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/project-git-switch", {"path": [tmpdir]})
            with self.assertRaises(PlanError):
                app.handle_action("/actions/run-many", {"target_repo": [tmpdir], "selected_plans": []})

    def test_handle_action_rerun_plan_requires_existing_plan_file(self) -> None:
        from kctl_pkg.types import PlanError

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)

            app = DashboardApp(repo_path)
            with self.assertRaises(PlanError) as ctx:
                app.handle_action("/actions/rerun-plan", {"plan_file_path": [str(Path(tmpdir) / "missing.yaml")]})

            self.assertIn("Plan file not found", str(ctx.exception))

    def test_handle_action_rerun_plan_passes_partial_step_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = repo_path / ".kctl" / "plans" / "001-sample.yaml"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n  - id: verify\n    type: verify\n    commands:\n      - printf ok\n"
            )

            app = DashboardApp(repo_path)
            with patch.object(app, "start_run_many", return_value="run-123") as start_run_many_mock:
                result = app.handle_action(
                    "/actions/rerun-plan",
                    {
                        "plan_file_path": [str(plan_path)],
                        "from_step": ["verify"],
                        "provider_override": ["claude"],
                    },
                )

            self.assertEqual(result.redirect_to, "/")
            self.assertEqual(result.run_id, "run-123")
            self.assertIn("from step verify", result.message)
            self.assertEqual(start_run_many_mock.call_args.kwargs["from_step"], "verify")
            self.assertIsNone(start_run_many_mock.call_args.kwargs["only_step"])
            self.assertEqual(start_run_many_mock.call_args.kwargs["provider_override"], "claude")

    def test_handle_action_partial_rerun_reuses_indexed_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            plan_path = repo_path / ".kctl" / "plans" / "001-sample.yaml"
            workspace_path = repo_path / ".kctl" / "worktrees" / "run-1" / "001-sample"
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                f"repo: {repo_path}\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n  - id: verify\n    type: verify\n    commands:\n      - printf ok\n"
            )

            app = DashboardApp(repo_path)
            with patch("kctl_pkg.ui_dashboard.get_plan_execution") as get_plan_execution_mock, patch.object(
                app, "start_run_many", return_value="run-123"
            ) as start_run_many_mock:
                get_plan_execution_mock.return_value = SimpleNamespace(
                    id="plan-exec-1",
                    plan_file_path=str(plan_path.resolve()),
                    worktree_path=str(workspace_path.resolve()),
                )
                result = app.handle_action(
                    "/actions/rerun-plan",
                    {
                        "plan_file_path": [str(plan_path)],
                        "plan_execution_id": ["plan-exec-1"],
                        "only_step": ["verify"],
                    },
                )

            self.assertEqual(result.redirect_to, "/")
            self.assertEqual(result.run_id, "run-123")
            self.assertEqual(start_run_many_mock.call_args.kwargs["only_step"], "verify")
            self.assertEqual(
                start_run_many_mock.call_args.kwargs["reuse_workspace_path"],
                workspace_path.resolve(),
            )

    def test_render_run_stop_controls_uses_inline_form_submit(self) -> None:
        from kctl_pkg.ui_dashboard_runs import _render_run_stop_controls

        html = _render_run_stop_controls("run-123", "running")

        self.assertIn("/runs/run-123/stop", html)
        self.assertIn("Stop Run", html)
        self.assertIn("<a", html)

    def test_stop_run_many_persists_stop_request_and_kills_active_pids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id = "20260414T201522347780Z"
            run_root = repo_path / ".kctl" / "runs" / run_id
            run_root.mkdir(parents=True, exist_ok=True)
            (run_root / "run.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "plans_dir": str((repo_path / ".kctl" / "plans").resolve()),
                        "repo": str(repo_path.resolve()),
                        "artifact_storage_mode": "in_repo",
                        "artifact_root_path": str((repo_path / ".kctl" / "runs").resolve()),
                        "stream_log_path": str((run_root / "stream.log").resolve()),
                        "active_pids": [11111, 22222],
                        "stop_requested": False,
                        "status": "running",
                        "started_at": "2026-04-14T20:15:22+00:00",
                        "concurrency": 1,
                        "preflight": {},
                        "plans": [],
                    },
                    indent=2,
                )
                + "\n"
            )

            app = DashboardApp(repo_path)
            killed: list[tuple[int, int]] = []
            with patch("kctl_pkg.ui_dashboard.os.kill", side_effect=lambda pid, sig: killed.append((pid, sig))):
                stopped = app.stop_run_many(run_id)

            self.assertTrue(stopped)
            self.assertEqual(killed, [(11111, signal.SIGTERM), (22222, signal.SIGTERM)])
            self.assertTrue((run_root / "stop-requested").exists())

    def test_live_run_stop_request_surfaces_as_stopping(self) -> None:
        from kctl_pkg.ui_dashboard_state import (
            build_live_steps_from_run_data,
            build_plan_cards_from_live_data,
            build_run_detail_from_live_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            run_data = {
                "run_id": "run-123",
                "plans_dir": str((repo_path / ".kctl" / "plans").resolve()),
                "repo": str(repo_path.resolve()),
                "artifact_root_path": str((repo_path / ".kctl" / "runs").resolve()),
                "status": "running",
                "stop_requested": True,
                "active_pids": [12345],
                "started_at": "2026-04-14T20:15:22+00:00",
                "ended_at": None,
                "concurrency": 1,
                "plans": [
                    {
                        "plan_id": "001-plan",
                        "plan_path": str((repo_path / ".kctl" / "plans" / "001-plan.yaml").resolve()),
                        "status": "running",
                        "current_step": "verify",
                        "step_statuses": {"verify": "running"},
                        "verify_result": "not-run",
                        "worktree_path": None,
                        "branch_name": None,
                        "log_path": None,
                    }
                ],
            }

            run_detail = build_run_detail_from_live_data(app, run_data)
            plan_cards = build_plan_cards_from_live_data(app, run_data)
            live_steps = build_live_steps_from_run_data(app, run_data, "run-123:001-plan")

            self.assertEqual(run_detail.status, "stopping")
            self.assertEqual(plan_cards[0].status, "stopping")
            self.assertEqual(live_steps[0].status, "stopping")

    def test_orphaned_stop_requested_run_surfaces_as_stopped(self) -> None:
        from kctl_pkg.ui_dashboard_state import (
            build_live_steps_from_run_data,
            build_plan_cards_from_live_data,
            build_run_detail_from_live_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            run_data = {
                "run_id": "run-123",
                "plans_dir": str((repo_path / ".kctl" / "plans").resolve()),
                "repo": str(repo_path.resolve()),
                "artifact_root_path": str((repo_path / ".kctl" / "runs").resolve()),
                "status": "running",
                "stop_requested": True,
                "started_at": "2026-04-14T20:15:22+00:00",
                "ended_at": None,
                "concurrency": 1,
                "plans": [
                    {
                        "plan_id": "001-plan",
                        "plan_path": str((repo_path / ".kctl" / "plans" / "001-plan.yaml").resolve()),
                        "status": "running",
                        "current_step": "verify",
                        "step_statuses": {"verify": "running"},
                        "verify_result": "not-run",
                        "worktree_path": None,
                        "branch_name": None,
                        "log_path": None,
                    }
                ],
            }

            run_detail = build_run_detail_from_live_data(app, run_data)
            plan_cards = build_plan_cards_from_live_data(app, run_data)
            live_steps = build_live_steps_from_run_data(app, run_data, "run-123:001-plan")

            self.assertEqual(run_detail.status, "stopped")
            self.assertEqual(plan_cards[0].status, "stopped")
            self.assertEqual(live_steps[0].status, "stopped")

    def test_stopped_plan_uses_operator_facing_labels(self) -> None:
        from kctl_pkg.ui_dashboard_state import build_plan_cards_from_live_data
        from kctl_pkg.ui_dashboard_support import _failure_reason_label, _status_badge

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            app = DashboardApp(repo_path)
            run_data = {
                "run_id": "run-123",
                "plans": [
                    {
                        "plan_id": "001-plan",
                        "plan_path": str((repo_path / ".kctl" / "plans" / "001-plan.yaml").resolve()),
                        "status": "stopped",
                        "failure_reason": "run_stopped",
                        "verify_result": "not-run",
                    }
                ],
            }

            plan_cards = build_plan_cards_from_live_data(app, run_data)

            self.assertEqual(plan_cards[0].failure_reason, "run_stopped")
            self.assertIn("stopped", _status_badge(plan_cards[0].status, failure_reason=plan_cards[0].failure_reason))
            self.assertEqual(_failure_reason_label(plan_cards[0].failure_reason), "Stopped by operator")

    def test_legacy_blocked_run_stopped_status_still_displays_as_stopped(self) -> None:
        from kctl_pkg.ui_dashboard_support import _status_badge

        self.assertIn("stopped", _status_badge("blocked", failure_reason="run_stopped"))

    def test_load_live_run_data_marks_stop_requested_from_marker_file(self) -> None:
        from kctl_pkg.ui_dashboard_state import load_live_run_data

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            run_id = "20260414T201522347780Z"
            run_root = repo_path / ".kctl" / "runs" / run_id
            repo_path.mkdir(parents=True, exist_ok=True)
            run_root.mkdir(parents=True, exist_ok=True)
            (run_root / "run.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "artifact_root_path": str((repo_path / ".kctl" / "runs").resolve()),
                        "status": "running",
                        "plans": [],
                    },
                    indent=2,
                )
                + "\n"
            )
            (run_root / "stop-requested").write_text("2026-04-14T21:00:00Z\n")

            app = DashboardApp(repo_path)
            run_data = load_live_run_data(app, run_id)

            self.assertIsNotNone(run_data)
            assert run_data is not None
            self.assertTrue(run_data["stop_requested"])

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

    def test_load_tracked_projects_ignores_invalid_file_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            repo_path.mkdir(parents=True, exist_ok=True)
            tracked_path = repo_path / ".kctl" / "dashboard-projects.json"
            tracked_path.parent.mkdir(parents=True, exist_ok=True)
            tracked_path.write_text("{not-json}\n")

            app = DashboardApp(repo_path)
            self.assertEqual(app.load_tracked_projects(), [])

            tracked_path.write_text(json.dumps({"project": "/tmp"}) + "\n")
            self.assertEqual(app.load_tracked_projects(), [])

    def test_render_projects_page_shows_unavailable_project_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project)
            app = DashboardApp(repo_path)
            app.add_tracked_project(project)

            with patch("kctl_pkg.ui_dashboard_projects.get_project_git_summary", return_value={"available": False, "error": "broken git"}):
                html = app.render_projects_page()

            self.assertIn("broken git", html)
            self.assertIn("Tracked Projects", html)
            self.assertIn("Refresh", html)

    def test_render_project_detail_page_shows_unavailable_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project)
            app = DashboardApp(repo_path)
            app.add_tracked_project(project)

            with patch("kctl_pkg.ui_dashboard_projects.get_project_git_detail", return_value={"available": False, "name": "project-a", "error": "git detail failed"}):
                html = app.render_project_detail_page(str(project))

            self.assertIn("git detail failed", html)
            self.assertIn("All Projects", html)

    def test_render_project_detail_page_shows_clean_empty_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project)
            app = DashboardApp(repo_path)
            app.add_tracked_project(project)

            detail = {
                "available": True,
                "name": "project-a",
                "branch": "main",
                "dirty": False,
                "changed_count": 0,
                "ahead_behind": (0, 0),
                "remotes": [],
                "status_output": "",
                "diff_stat": "",
                "branches": [],
                "stash_list": [],
                "recent_commits": [],
            }
            with patch("kctl_pkg.ui_dashboard_projects.get_project_git_detail", return_value=detail):
                html = app.render_project_detail_page(str(project), action_message="Saved")

            self.assertIn("action-ok", html)
            self.assertIn("No remotes configured.", html)
            self.assertIn("Clean working tree.", html)
            self.assertIn("No branches found.", html)
            self.assertIn("No stashed changes.", html)
            self.assertIn("Launch Agent", html)

    def test_render_project_detail_page_shows_dirty_repo_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project-a"
            init_git_repo(repo_path)
            init_git_repo(project)
            app = DashboardApp(repo_path)
            app.add_tracked_project(project)

            detail = {
                "available": True,
                "name": "project-a",
                "branch": "feature/test",
                "dirty": True,
                "changed_count": 2,
                "ahead_behind": (1, 0),
                "remotes": [
                    {"name": "origin", "url": "git@example.com:repo.git", "direction": "fetch"},
                    {"name": "origin", "url": "git@example.com:repo.git", "direction": "push"},
                ],
                "status_output": " M app.py",
                "diff_stat": " app.py | 2 +-",
                "branches": [{"name": "feature/test", "current": "true"}, {"name": "main", "current": "false"}],
                "stash_list": ["stash@{0}: WIP on feature/test"],
                "recent_commits": [
                    {"sha": "abc1234", "subject": "Update UI", "author": "Test User", "date": "2026-01-01"},
                ],
            }
            with patch("kctl_pkg.ui_dashboard_projects.get_project_git_detail", return_value=detail):
                html = app.render_project_detail_page(str(project), action_message="Error: failed")

            self.assertIn("action-error", html)
            self.assertIn("Show full diff", html)
            self.assertIn("Commit All", html)
            self.assertIn("Discard All Changes", html)
            self.assertIn("Pull", html)
            self.assertIn("Push", html)
            self.assertIn("Create &amp; Switch", html)
            self.assertIn("stash@{0}: WIP on feature/test", html)
            self.assertIn("Recent Commits (1)", html)

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
            self.assertIn("live_output_tail", html)
            self.assertIn("<strong>1</strong> active", html)
            self.assertIn("<strong>1</strong> running", html)
            self.assertIn("Copy last 50 lines", html)
            self.assertIn("data-copy-target='#live_output_stream'", html)
            self.assertIn("data-copy-last-lines='50'", html)
            self.assertIn("onclick='return window.kctlCopyButtonClick(this)'", html)
            self.assertIn("tap the tail box below", html)
            self.assertIn("Step Timeline", html)
            self.assertIn("<td>inspect</td>", html)
            self.assertIn(">running</span>", html)
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

    def test_dashboard_plan_detail_renders_partial_rerun_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            html = DashboardApp(repo_path).render_page(run_id=run_id)

            self.assertIn("/actions/rerun-plan", html)
            self.assertIn("name='plan_execution_id'", html)
            self.assertIn("Rerun Full Plan", html)
            self.assertIn("Rerun From verify", html)
            self.assertIn("Rerun Verify Only", html)

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

            self.assertIn("<summary><h2 class='inline-heading'>Attention</h2></summary>", html)
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

    def test_render_attention_card_variants_and_read_plan_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plan_path = Path(tmpdir) / "001-plan.yaml"
            plan_path.write_text("objective: test\n")
            workspace_path = str(Path(tmpdir) / "worktree")

            safe_rerun_html = _render_attention_card(
                AttentionItem(
                    kind="plan_execution",
                    run_id="run-1",
                    plan_execution_id="run-1:001-plan",
                    plan_slug="001-plan",
                    status="failed",
                    operator_action="safe_rerun",
                    current_step_key="verify",
                    verify_status="failed",
                    failure_reason="agent_failed",
                    reset_hint=None,
                    plan_file_path=str(plan_path),
                    workspace_path=None,
                    started_at="2026-01-01T00:00:01+00:00",
                ),
                providers=[("codex", "Codex")],
            )
            self.assertIn("/actions/rerun-plan", safe_rerun_html)
            self.assertIn("Provider Override", safe_rerun_html)
            self.assertIn("Agent failed", safe_rerun_html)

            review_html = _render_attention_card(
                AttentionItem(
                    kind="plan_execution",
                    run_id="run-2",
                    plan_execution_id="run-2:001-plan",
                    plan_slug="001-plan",
                    status="failed",
                    operator_action="review_workspace",
                    current_step_key="implement",
                    verify_status="not_run",
                    failure_reason=None,
                    reset_hint=None,
                    plan_file_path=None,
                    workspace_path=workspace_path,
                    started_at="2026-01-01T00:00:01+00:00",
                )
            )
            self.assertIn("Review Workspace", review_html)
            self.assertIn("Copy workspace path", review_html)

            stale_html = _render_attention_card(
                AttentionItem(
                    kind="plan_execution",
                    run_id="run-3",
                    plan_execution_id="run-3:001-plan",
                    plan_slug="001-plan",
                    status="running",
                    operator_action="investigate_stale",
                    current_step_key=None,
                    verify_status="running",
                    failure_reason=None,
                    reset_hint=None,
                    plan_file_path=None,
                    workspace_path=None,
                    started_at="2026-01-01T00:00:01+00:00",
                )
            )
            self.assertIn("Stale", stale_html)
            self.assertIn("no completion after", stale_html)

            self.assertEqual(read_plan_file(plan_path), ("001-plan.yaml", "objective: test\n"))
            with self.assertRaises(PlanError):
                read_plan_file(Path(tmpdir) / "missing.yaml")

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

    def test_serve_dashboard_handler_get_writes_page_response(self) -> None:
        app = SimpleNamespace()
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/"
                        self.headers = {}
                        self.rfile = BytesIO()
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_GET()
                captured["body"] = handler.wfile.getvalue().decode("utf-8")

            def server_close(self) -> None:
                captured["closed"] = True

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.handle_api_get",
            return_value=None,
        ), patch(
            "kctl_pkg.ui_dashboard_server.handle_page_get",
            return_value=(HTTPStatus.OK, "text/html; charset=utf-8", b"<html>ok</html>"),
        ), patch("kctl_pkg.ui_dashboard_server.build_dashboard_access_urls", return_value=[]):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["status"], HTTPStatus.OK)
        self.assertIn(("Content-Type", "text/html; charset=utf-8"), captured["headers"])
        self.assertEqual(captured["body"], "<html>ok</html>")
        self.assertTrue(captured["closed"])

    def test_serve_dashboard_handler_get_404s_not_found(self) -> None:
        app = SimpleNamespace()
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/missing"
                        self.headers = {}
                        self.rfile = BytesIO()
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        return

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_GET()

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.handle_api_get",
            return_value=None,
        ), patch(
            "kctl_pkg.ui_dashboard_server.handle_page_get",
            side_effect=PlanError("Not Found"),
        ), patch("kctl_pkg.ui_dashboard_server.build_dashboard_access_urls", return_value=[]):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["error"], (HTTPStatus.NOT_FOUND, "Not Found"))

    def test_serve_dashboard_handler_get_writes_api_response(self) -> None:
        app = SimpleNamespace()
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/api/check-repo?path=%2Ftmp"
                        self.headers = {}
                        self.rfile = BytesIO()
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_GET()
                captured["body"] = handler.wfile.getvalue().decode("utf-8")

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.handle_api_get",
            return_value=(HTTPStatus.OK, "application/json; charset=utf-8", b'{"ok":true}'),
        ), patch("kctl_pkg.ui_dashboard_server.build_dashboard_access_urls", return_value=[]):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["status"], HTTPStatus.OK)
        self.assertIn(("Content-Type", "application/json; charset=utf-8"), captured["headers"])
        self.assertEqual(captured["body"], '{"ok":true}')

    def test_serve_dashboard_handler_get_renders_generic_plan_error_html(self) -> None:
        app = SimpleNamespace()
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/"
                        self.headers = {}
                        self.rfile = BytesIO()
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_GET()
                captured["body"] = handler.wfile.getvalue().decode("utf-8")

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.handle_api_get",
            return_value=None,
        ), patch(
            "kctl_pkg.ui_dashboard_server.handle_page_get",
            side_effect=PlanError("bad page"),
        ), patch("kctl_pkg.ui_dashboard_server.build_dashboard_access_urls", return_value=[]):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["status"], HTTPStatus.OK)
        self.assertIn("bad page", captured["body"])

    def test_serve_dashboard_handler_post_redirects_action_result(self) -> None:
        action_result = SimpleNamespace(redirect_to="/actions", message="done", run_id=None)
        app = SimpleNamespace(handle_action=lambda path, form: action_result)
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                body = b"project_path=%2Ftmp%2Frepo"

                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/actions/start-session"
                        self.headers = {"Content-Length": str(len(body))}
                        self.rfile = BytesIO(body)
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_POST()

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.build_dashboard_access_urls",
            return_value=[],
        ):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["status"], HTTPStatus.SEE_OTHER)
        self.assertIn(("Location", "/actions?message=done"), captured["headers"])

    def test_serve_dashboard_handler_post_falls_back_on_plan_error(self) -> None:
        app = SimpleNamespace(handle_action=lambda path, form: (_ for _ in ()).throw(PlanError("bad action")))
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                body = b"project_path=%2Ftmp%2Frepo"

                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/actions/start-session"
                        self.headers = {"Content-Length": str(len(body))}
                        self.rfile = BytesIO(body)
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_POST()

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.build_dashboard_access_urls",
            return_value=[],
        ):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["status"], HTTPStatus.SEE_OTHER)
        self.assertIn(("Location", "/actions?message=bad+action"), captured["headers"])

    def test_serve_dashboard_handler_post_404s_unknown_action(self) -> None:
        app = SimpleNamespace(handle_action=lambda path, form: None)
        captured: dict[str, object] = {}

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                body = b""

                class FakeHandler(self.handler_cls):  # type: ignore[misc]
                    def __init__(self) -> None:
                        self.path = "/actions/unknown"
                        self.headers = {"Content-Length": str(len(body))}
                        self.rfile = BytesIO(body)
                        self.wfile = BytesIO()

                    def send_response(self, code: int, message: str | None = None) -> None:
                        captured["status"] = code

                    def send_header(self, name: str, value: str) -> None:
                        captured.setdefault("headers", []).append((name, value))

                    def end_headers(self) -> None:
                        return

                    def send_error(self, code: int, message: str | None = None) -> None:
                        captured["error"] = (code, message)

                handler = FakeHandler()
                handler.do_POST()

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.build_dashboard_access_urls",
            return_value=[],
        ):
            result = serve_dashboard(app=app, host="127.0.0.1", port=8421, summarize_preflight=object())

        self.assertEqual(result, 0)
        self.assertEqual(captured["error"], (HTTPStatus.NOT_FOUND, "Not Found"))

    def test_serve_dashboard_prints_access_urls_and_tailscale_note(self) -> None:
        app = SimpleNamespace()

        class FakeServer:
            def __init__(self, address: tuple[str, int], handler_cls: object) -> None:
                self.handler_cls = handler_cls

            def serve_forever(self) -> None:
                return

            def server_close(self) -> None:
                return

        with patch("kctl_pkg.ui_dashboard_server.ThreadingHTTPServer", FakeServer), patch(
            "kctl_pkg.ui_dashboard_server.build_dashboard_access_urls",
            return_value=["http://127.0.0.1:8421", "http://tailnet:8421"],
        ):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                result = serve_dashboard(
                    app=app,
                    host="0.0.0.0",
                    port=8421,
                    summarize_preflight=object(),
                    tailscale=True,
                )

        self.assertEqual(result, 0)
        text = buffer.getvalue()
        self.assertIn("kctl dashboard listening on 0.0.0.0:8421", text)
        self.assertIn("dashboard url: http://127.0.0.1:8421", text)
        self.assertIn("tailscale note:", text)


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

    def test_list_sessions_ignores_invalid_meta_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            bad_dir = app._session_dir("bad-session")
            bad_dir.mkdir(parents=True, exist_ok=True)
            (bad_dir / "meta.json").write_text("{not json")

            self.assertEqual(app.list_sessions(), [])

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

    def test_read_session_output_returns_empty_on_os_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-oserror"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("agent output line\n")

            with patch.object(Path, "read_text", side_effect=OSError("boom")):
                self.assertEqual(app.read_session_output(session_id), "")

    def test_get_session_returns_none_for_missing_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            self.assertIsNone(app.get_session("nonexistent"))

    def test_get_session_returns_none_for_invalid_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "broken-session"
            session_dir = app._session_dir(session_id)
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "meta.json").write_text("{oops")

            self.assertIsNone(app.get_session(session_id))

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

    def test_reply_to_session_uses_codex_resume_for_codex_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-codex"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("first turn output\n")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "prompt": "first turn",
                "provider": "codex",
                "provider_session_id": "",
                "status": "completed",
                "started_at": "2026-01-01T00:00:01+00:00",
                "messages": [{"role": "user", "content": "first turn", "timestamp": "2026-01-01T00:00:01+00:00"}],
            }
            app._write_session_meta(meta)
            launched: list[list[str]] = []

            with patch.object(app, "_run_session_subprocess", side_effect=lambda m, command, output_path: launched.append(command)):
                app.reply_to_session(session_id, "continue")

            self.assertEqual(launched[0][:5], ["codex", "exec", "resume", "--last", "--full-auto"])

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

    def test_stop_agent_session_tolerates_os_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "sess-running",
                "project_path": tmpdir,
                "prompt": "go",
                "provider": "codex",
                "status": "running",
                "started_at": "2026-01-01T00:00:01+00:00",
                "pid": 12345,
            }
            app._write_session_meta(meta)

            with patch("kctl_pkg.ui_dashboard.os.kill", side_effect=ProcessLookupError):
                self.assertTrue(app.stop_agent_session("sess-running"))

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

    def test_run_session_subprocess_updates_meta_on_success(self) -> None:
        class FakeStdout:
            def __init__(self, lines: list[str]) -> None:
                self._lines = iter(lines)

            def readline(self) -> str:
                return next(self._lines, "")

            def close(self) -> None:
                return None

        class FakeProcess:
            def __init__(self) -> None:
                self.pid = 4321
                self.stdout = FakeStdout(["line 1\n", "line 2\n"])

            def wait(self) -> int:
                return 0

        class ImmediateThread:
            def __init__(self, *, target: object, daemon: bool) -> None:
                self._target = target

            def start(self) -> None:
                assert callable(self._target)
                self._target()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            project_path.mkdir()
            session_id = "sess-success"
            output_path = repo_path / ".kctl" / "sessions" / session_id / "output.log"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": str(project_path),
                "status": "running",
            }

            with patch("kctl_pkg.ui_dashboard_sessions.subprocess.Popen", return_value=FakeProcess()), patch(
                "kctl_pkg.ui_dashboard_sessions.threading.Thread", ImmediateThread
            ), patch("kctl_pkg.ui_dashboard_sessions._detect_token_warning", return_value="quota"):
                run_session_subprocess(repo_path, meta, ["codex", "exec"], output_path)

            written_meta = json.loads((repo_path / ".kctl" / "sessions" / session_id / "meta.json").read_text())
            self.assertEqual(written_meta["status"], "completed")
            self.assertEqual(written_meta["exit_code"], 0)
            self.assertIsNone(written_meta["pid"])
            self.assertEqual(written_meta["token_warning"], "quota")
            self.assertIn("line 1\nline 2\n", output_path.read_text())

    def test_run_session_subprocess_records_exceptions(self) -> None:
        class ImmediateThread:
            def __init__(self, *, target: object, daemon: bool) -> None:
                self._target = target

            def start(self) -> None:
                assert callable(self._target)
                self._target()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project_path = Path(tmpdir) / "project"
            project_path.mkdir()
            session_id = "sess-error"
            output_path = repo_path / ".kctl" / "sessions" / session_id / "output.log"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": str(project_path),
                "status": "running",
            }

            with patch(
                "kctl_pkg.ui_dashboard_sessions.subprocess.Popen",
                side_effect=RuntimeError("spawn failed"),
            ), patch("kctl_pkg.ui_dashboard_sessions.threading.Thread", ImmediateThread), patch(
                "kctl_pkg.ui_dashboard_sessions._detect_token_warning",
                return_value=None,
            ):
                run_session_subprocess(repo_path, meta, ["codex", "exec"], output_path)

            written_meta = json.loads((repo_path / ".kctl" / "sessions" / session_id / "meta.json").read_text())
            self.assertEqual(written_meta["status"], "failed")
            self.assertEqual(written_meta["exit_code"], -1)
            self.assertIn("[kctl] session error: spawn failed", output_path.read_text())

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
            self.assertIn("Launch Session", html)

    def test_render_sessions_page_without_available_providers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            project = Path(tmpdir) / "project"
            init_git_repo(project)
            app.add_tracked_project(project)

            with patch("kctl_pkg.ui_dashboard_sessions.available_providers", return_value=[]):
                html = app.render_sessions_page()

            self.assertIn("No agent providers found", html)

    def test_render_sessions_page_truncates_prompt_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            meta: dict[str, object] = {
                "id": "20260101-000001-aabbccdd",
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "x" * 140,
                "provider": "codex",
                "status": "paused",
                "started_at": "2026-01-01T00:00:01+00:00",
                "messages": "not-a-list",
            }
            app._write_session_meta(meta)

            html = app.render_sessions_page()
            self.assertIn(("x" * 120) + "...", html)
            self.assertIn("0 turns", html)

    def test_list_sessions_direct_ignores_non_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            sessions_root = repo_path / ".kctl" / "sessions"
            sessions_root.mkdir(parents=True, exist_ok=True)
            (sessions_root / "note.txt").write_text("ignore me\n")
            write_session_meta_direct(
                repo_path,
                {
                    "id": "sess-1",
                    "project_path": tmpdir,
                    "status": "completed",
                    "started_at": "2026-01-01T00:00:01+00:00",
                },
            )

            sessions = list_sessions_direct(repo_path)
            self.assertEqual(len(sessions), 1)
            self.assertEqual(sessions[0]["id"], "sess-1")

    def test_start_agent_session_direct_uses_codex_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project"
            project.mkdir()
            launched: list[list[str]] = []

            def fake_run(repo_value: Path, meta: dict[str, object], command: list[str], output_path: Path) -> None:
                self.assertEqual(repo_value, repo_path)
                self.assertEqual(output_path.name, "output.log")
                launched.append(command)

            with patch("kctl_pkg.ui_dashboard_sessions.run_session_subprocess", side_effect=fake_run):
                session_id = start_agent_session_direct(repo_path, str(project), "write a test", "codex")

            self.assertTrue(session_id)
            self.assertEqual(launched[0][:4], ["codex", "exec", "--full-auto", "--cd"])

    def test_reply_to_session_direct_uses_claude_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            project = Path(tmpdir) / "project"
            project.mkdir()
            session_id = "sess-direct"
            output_path = repo_path / ".kctl" / "sessions" / session_id / "output.log"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("first response\n")
            write_session_meta_direct(
                repo_path,
                {
                    "id": session_id,
                    "project_path": str(project),
                    "provider": "claude",
                    "provider_session_id": "uuid-direct",
                    "status": "completed",
                    "started_at": "2026-01-01T00:00:01+00:00",
                    "messages": [{"role": "user", "content": "first", "timestamp": "2026-01-01T00:00:01+00:00"}],
                },
            )
            launched: list[list[str]] = []

            with patch(
                "kctl_pkg.ui_dashboard_sessions.run_session_subprocess",
                side_effect=lambda repo_value, meta, command, output_path: launched.append(command),
            ):
                reply_to_session_direct(repo_path, session_id, "second")

            self.assertIn("--resume", launched[0])
            self.assertIn("uuid-direct", launched[0])

    def test_stop_agent_session_direct_sends_sigterm(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            session_id = "sess-stop"
            write_session_meta_direct(
                repo_path,
                {
                    "id": session_id,
                    "project_path": tmpdir,
                    "status": "running",
                    "pid": 2468,
                },
            )
            killed: list[tuple[int, int]] = []

            with patch("kctl_pkg.ui_dashboard_sessions.os.kill", side_effect=lambda pid, sig: killed.append((pid, sig))):
                result = stop_agent_session_direct(repo_path, session_id)

            self.assertTrue(result)
            self.assertEqual(killed, [(2468, signal.SIGTERM)])

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
                "messages": [{"role": "user", "content": "fix the bug", "timestamp": "2026-01-01T00:00:01+00:00"}],
            }
            app._write_session_meta(meta)
            html = app.render_sessions_page()
            self.assertIn("my-project", html)
            self.assertIn("fix the bug", html)
            self.assertIn("completed", html)
            self.assertIn("claude", html)
            self.assertIn("/sessions/20260101-000001-aabbccdd", html)
            self.assertIn("1 turn", html)

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
            self.assertIn("session-chat-shell", html)
            self.assertIn("session-chat-row-user", html)
            self.assertIn("session-chat-row-agent", html)
            self.assertIn("reply", html.lower())
            self.assertIn("Copy last 50 lines", html)
            self.assertIn("data-copy-target='#session_output_tail'", html)
            self.assertIn("Raw Output", html)
            self.assertIn("Send a follow-up message", html)
            self.assertIn("scrollToBottom(transcriptNode)", html)

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
            self.assertIn("session-chat-form-disabled", html)
            self.assertIn("is responding", html)

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

    def test_render_session_detail_page_ignores_false_positive_token_warning_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-false-warning"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text('self.assertIn("hit your limit", html)\n')
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "run tests",
                "provider": "codex",
                "provider_session_id": "uuid-4",
                "status": "completed",
                "exit_code": 0,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:01:00+00:00",
                "pid": None,
                "token_warning": 'self.assertIn("hit your limit", html)',
                "messages": [],
            }
            app._write_session_meta(meta)
            html = app.render_session_detail_page(session_id)
            self.assertNotIn('id=\'token_warning\'', html)

    def test_render_session_detail_page_interleaves_follow_up_output_as_chat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-follow-up"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                "first response\n"
                "\n"
                f"{'─' * 60}\n"
                "[follow-up #2]\n"
                f"{'─' * 60}\n"
                "\n"
                "second response\n"
            )
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "first request",
                "provider": "codex",
                "provider_session_id": "uuid-3",
                "status": "completed",
                "exit_code": 0,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:02:00+00:00",
                "pid": None,
                "token_warning": None,
                "messages": [
                    {"role": "user", "content": "first request", "timestamp": "2026-01-01T00:00:01+00:00"},
                    {"role": "user", "content": "second request", "timestamp": "2026-01-01T00:01:00+00:00"},
                ],
            }
            app._write_session_meta(meta)
            html = app.render_session_detail_page(session_id)
            self.assertIn("first request", html)
            self.assertIn("second request", html)
            self.assertIn("first response", html)
            self.assertIn("second response", html)

    def test_render_session_detail_page_shows_empty_transcript_when_no_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = DashboardApp(Path(tmpdir) / "repo")
            session_id = "sess-empty"
            output_path = app._session_output_path(session_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("")
            meta: dict[str, object] = {
                "id": session_id,
                "project_path": tmpdir,
                "project_name": "proj",
                "prompt": "",
                "provider": "codex",
                "provider_session_id": "uuid-empty",
                "status": "completed",
                "exit_code": 0,
                "started_at": "2026-01-01T00:00:01+00:00",
                "ended_at": "2026-01-01T00:02:00+00:00",
                "pid": None,
                "token_warning": None,
                "messages": [],
            }
            app._write_session_meta(meta)

            html = app.render_session_detail_page(session_id)
            self.assertIn("No transcript yet. Session activity will appear here.", html)


if __name__ == "__main__":
    unittest.main()
