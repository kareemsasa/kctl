from __future__ import annotations

import os
import signal
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .multi import build_multi_run_id, load_normalized_multi_plans, resolve_multi_run_log, run_many_plans
from .git import (
    create_branch,
    discard_all_changes,
    ensure_git_repo,
    get_project_git_detail,
    get_repo_root,
    git_pull,
    git_push,
    git_stash_pop,
    git_stash_save,
    stage_and_commit,
    switch_branch,
)
from .plan import build_plan_from_template, load_plan_templates
from .paths import project_root
from .preflight import preflight_multi_run
from .runner import run_plan
from .ui_index import index_repository_state

from .types import PlanError
from .ui_dashboard_support import (
    _COMMON_STYLES,
    _SHARED_SCRIPTS,
    _detect_token_warning,
    _escape,
    _failure_reason_label,
    _fmt_ts,
    _lifecycle_badge,
    _link,
    _page_link,
    _preflight_item,
    _preflight_run_snapshot,
    _preflight_status_tone,
    _provider_select_html,
    _render_attention_card,
    _render_nav_html,
    _render_preflight_item_html,
    _status_badge,
    _status_class,
    available_providers,
    build_dashboard_access_urls,
    check_repo_path,
    list_plans_in_directory,
    read_plan_file,
)
from .ui_dashboard_projects import (
    add_tracked_project as dashboard_add_tracked_project,
    load_tracked_projects as dashboard_load_tracked_projects,
    projects_file_path as dashboard_projects_file_path,
    remove_tracked_project as dashboard_remove_tracked_project,
    render_project_detail_page as render_project_detail,
    render_projects_page as render_projects,
    save_tracked_projects as dashboard_save_tracked_projects,
)
from .ui_dashboard_actions import render_actions_page as render_actions
from .ui_dashboard_runs import (
    render_dashboard_detail_page as render_dashboard_detail,
    render_dashboard_page as render_dashboard,
    render_run_page as render_run,
)
from .ui_dashboard_server import serve_dashboard as dashboard_server_serve
from .ui_dashboard_state import (
    build_plan_cards_from_live_data as build_live_plan_cards,
    build_run_detail_from_live_data as build_live_run_detail,
    load_dashboard_state,
    load_live_run_data as load_live_run,
    load_saved_run_data as load_saved_run,
    read_live_output as read_run_live_output,
)
from .ui_dashboard_sessions import (
    get_session as session_get_session,
    list_sessions as session_list_sessions,
    read_session_output as session_read_output,
    render_session_detail_page as render_session_detail,
    render_sessions_page as render_sessions,
    sessions_dir as dashboard_sessions_dir,
    session_dir as dashboard_session_dir,
    session_meta_path as dashboard_session_meta_path,
    session_output_path as dashboard_session_output_path,
    write_session_meta as dashboard_write_session_meta,
    read_session_meta as dashboard_read_session_meta,
    run_session_subprocess as dashboard_run_session_subprocess,
)
from .ui_read import (
    AttentionItem,
    PlanExecutionCard,
    RepositoryOverview,
    RunDetail,
    RunListItem,
    StepTimelineItem,
    WorkspaceDetail,
    WorkspaceSummary,
    get_plan_execution,
    get_repository_overview,
    get_repository,
    get_run,
    get_workspace,
    list_attention_items,
    list_plan_executions,
    list_runs,
    list_step_executions,
    list_workspaces,
)

@dataclass(frozen=True)
class DashboardState:
    repo_name: str
    repo_root: str
    action_message: str | None
    plan_templates: list[tuple[str, str | None]]
    tracked_projects: list[str]
    available_providers: list[tuple[str, str]]
    overview: RepositoryOverview
    attention_items: list[AttentionItem]
    workspaces: list[WorkspaceSummary]
    runs: list[RunListItem]
    selected_run: RunDetail | None
    plan_cards: list[PlanExecutionCard]
    selected_plan: PlanExecutionCard | None
    selected_plan_file_name: str | None
    selected_plan_file_path: str | None
    selected_plan_file_contents: str | None
    launch_preflight: dict[str, object]
    selected_run_preflight: dict[str, object] | None
    live_output: str | None
    live_output_status: str | None
    live_output_path: str | None
    steps: list[StepTimelineItem]
    workspace: WorkspaceDetail | None


@dataclass(frozen=True)
class DashboardActionResult:
    redirect_to: str
    message: str
    run_id: str | None = None


def summarize_preflight_for_dashboard(
    target_repo_value: str,
    plans_dir_value: str,
    selected_plan_names: list[str] | None = None,
    provider_override: str | None = None,
) -> dict[str, object]:
    repo_status, repo_message = check_repo_path(target_repo_value)
    plans_status, plans_message, _plans = list_plans_in_directory(plans_dir_value)
    summary: dict[str, object] = {
        "status": "warn",
        "decision": "Runnable with warnings",
        "message": "Preflight is waiting for valid repo and plans inputs.",
        "items": {
            "repo": _preflight_item(repo_status, repo_message),
            "plans_dir": _preflight_item(plans_status, plans_message),
            "binaries": _preflight_item("warn", "Waiting for plans to resolve required binaries."),
            "writable_paths": _preflight_item("warn", "Waiting for plans to resolve run/workspace paths."),
            "required_env": _preflight_item("warn", "Waiting for plans to resolve required environment variables."),
        },
    }
    if repo_status != "ok" or plans_status != "ok":
        summary["status"] = "block"
        summary["decision"] = "Blocked"
        summary["message"] = "Preflight is blocked until repo and plans inputs resolve."
        return summary

    plans_dir = Path(plans_dir_value).expanduser().resolve()
    selected_filenames = {name.strip() for name in (selected_plan_names or []) if name.strip()}
    try:
        plan_specs, normalized_plans = load_normalized_multi_plans(
            plans_dir,
            selected_filenames=selected_filenames or None,
        )
        if provider_override:
            for plan_id, plan in list(normalized_plans.items()):
                defaults = dict(plan.get("defaults") or {})
                defaults["provider"] = provider_override
                if provider_override == "codex":
                    defaults.pop("permission_mode", None)
                elif "permission_mode" not in defaults:
                    defaults["permission_mode"] = "auto"
                normalized_plan = dict(plan)
                normalized_plan["defaults"] = defaults
                normalized_plan["_kctl_provider"] = provider_override
                normalized_plan["_kctl_permission_mode"] = defaults.get("permission_mode") or "auto"
                normalized_plans[plan_id] = normalized_plan
        report = preflight_multi_run(
            plans_dir=plans_dir,
            run_id=build_multi_run_id(),
            plan_specs=plan_specs,
            normalized_plans=normalized_plans,
        )
    except PlanError as exc:
        return {
            "status": "block",
            "decision": "Blocked",
            "message": str(exc),
            "items": {
                "repo": _preflight_item(repo_status, repo_message),
                "plans_dir": _preflight_item(
                    "block",
                    str(exc),
                    remediation="Fix the plans directory contents so kctl can load the selected plans.",
                ),
                "binaries": _preflight_item("warn", "Could not resolve plan requirements."),
                "writable_paths": _preflight_item("warn", "Could not resolve plan paths."),
                "required_env": _preflight_item("warn", "Could not resolve required environment variables."),
            },
        }

    binary_issues = [issue.message for issue in report.issues if issue.code in {"missing_binary", "missing_path"}]
    binary_fix = next((issue.fix for issue in report.issues if issue.code in {"missing_binary", "missing_path"}), None)
    writable_issues = [issue.message for issue in report.issues if issue.code in {"run_dir_unwritable", "workspace_dir_unwritable"}]
    writable_fix = next((issue.fix for issue in report.issues if issue.code in {"run_dir_unwritable", "workspace_dir_unwritable"}), None)
    env_issues = [issue.message for issue in report.issues if issue.code == "missing_env"]
    env_fix = next((issue.fix for issue in report.issues if issue.code == "missing_env"), None)
    return {
        "status": "pass" if report.ok else "block",
        "message": "Preflight clear. Launch can proceed." if report.ok else f"Preflight blocked by {len(report.issues)} issue(s).",
        "decision": "Ready to run" if report.ok else "Blocked",
        "items": {
            "repo": _preflight_item(
                "pass" if report.repo_root is not None else "block",
                f"Repo root: {report.repo_root}" if report.repo_root is not None else repo_message,
                remediation="Point the form at a valid git repository root." if report.repo_root is None else None,
            ),
            "plans_dir": _preflight_item(
                "pass",
                f"{len(plan_specs)} plan(s) selected from {plans_dir}",
                ", ".join(spec.filename for spec in plan_specs),
            ),
            "binaries": _preflight_item(
                "pass" if not binary_issues else "block",
                "Required binaries available." if not binary_issues else binary_issues[0],
                ", ".join(report.required_binaries) if report.required_binaries else "No external binaries required.",
                remediation=binary_fix,
                action_label="Copy binary" if report.required_binaries else None,
                action_value=report.required_binaries[0] if report.required_binaries else None,
            ),
            "writable_paths": _preflight_item(
                "pass" if not writable_issues else "block",
                "Run and workspace paths writable." if not writable_issues else writable_issues[0],
                "; ".join(
                    value
                    for value in [
                        str(report.run_root) if report.run_root else None,
                        str(report.worktree_root) if report.worktree_root else None,
                    ]
                    if value
                ),
                remediation=writable_fix,
                action_label="Copy path" if report.run_root or report.worktree_root else None,
                action_value=str(report.run_root or report.worktree_root) if report.run_root or report.worktree_root else None,
            ),
            "required_env": _preflight_item(
                "pass" if not env_issues else "block",
                "Required environment present." if not env_issues else env_issues[0],
                ", ".join(report.required_env) if report.required_env else "No required environment variables declared.",
                remediation=env_fix,
                action_label="Copy env" if report.required_env else None,
                action_value=report.required_env[0] if report.required_env else None,
            ),
        },
    }
class DashboardApp:
    def __init__(self, repo_path: Path, db_path: Path | None = None) -> None:
        self.repo_path = repo_path.resolve()
        self.db_path = db_path.resolve() if db_path is not None else None

    def _page_shell(self, *, active_nav: str, body: str, extra_script: str = "") -> str:
        repo_name = _escape(self.repo_path.name)
        repo_root = _escape(str(self.repo_path))
        nav_html = _render_nav_html(active_nav)
        return (
            "<!doctype html>\n<html lang=\"en\">\n<head>\n"
            "  <meta charset=\"utf-8\">\n"
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
            f"  <title>kctl &mdash; {_escape(active_nav)}</title>\n"
            f"  <style>\n{_COMMON_STYLES}  </style>\n"
            f"  <script>\n{_SHARED_SCRIPTS}{extra_script}\n  </script>\n"
            "</head>\n<body>\n"
            "  <div class=\"page\">\n"
            "    <header class=\"page-header\">\n"
            "      <h1>kctl</h1>\n"
            f"      <div>{repo_name} &mdash; <span class=\"header-path\">{repo_root}</span></div>\n"
            f"      {nav_html}\n"
            "    </header>\n"
            f"    {body}\n"
            "  </div>\n"
            "</body>\n</html>"
        )

    @property
    def default_plans_dir(self) -> Path:
        return self.repo_path / ".kctl" / "plans"

    @property
    def projects_file_path(self) -> Path:
        return dashboard_projects_file_path(self.repo_path)

    def load_tracked_projects(self) -> list[str]:
        return dashboard_load_tracked_projects(self.repo_path)

    def save_tracked_projects(self, projects: list[str]) -> None:
        dashboard_save_tracked_projects(self.repo_path, projects)

    def add_tracked_project(self, project_path: Path) -> None:
        dashboard_add_tracked_project(self.repo_path, project_path)

    def remove_tracked_project(self, project_path: str) -> None:
        dashboard_remove_tracked_project(self.repo_path, project_path)

    def plans_dir_for_repo(self, target_repo: Path) -> Path:
        return target_repo.resolve() / ".kctl" / "plans"

    def load_live_run_data(self, run_id: str) -> dict[str, object] | None:
        return load_live_run(self, run_id)

    def load_saved_run_data(self, run_root_path: str | None) -> dict[str, object] | None:
        return load_saved_run(run_root_path)

    def read_live_output(self, run_data: dict[str, object] | None) -> tuple[str | None, str | None, str | None]:
        return read_run_live_output(run_data)

    def load_state(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> DashboardState:
        return load_dashboard_state(
            self,
            state_type=DashboardState,
            summarize_preflight=summarize_preflight_for_dashboard,
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )

    def _build_run_detail_from_live_data(self, run_data: dict[str, object]) -> RunDetail:
        return build_live_run_detail(self, run_data)

    def _build_plan_cards_from_live_data(self, run_data: dict[str, object]) -> list[PlanExecutionCard]:
        return build_live_plan_cards(self, run_data)

    def run_index_now(self) -> None:
        index_repository_state(self.repo_path, db_path=self.db_path)

    def create_plan(
        self,
        *,
        target_repo: Path,
        template_name: str,
        output_name: str,
        objective: str,
        force: bool,
    ) -> Path:
        if not output_name.strip():
            raise PlanError("Plan file name is required.")
        target_repo = target_repo.expanduser().resolve()
        output_path = self.plans_dir_for_repo(target_repo) / output_name
        if output_path.exists() and not force:
            raise PlanError(f"Plan file already exists: {output_path}")
        templates = load_plan_templates(project_root())
        plan = build_plan_from_template(
            templates=templates,
            template_name=template_name,
            repo=str(target_repo),
            objective=objective,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        yaml = __import__("yaml")
        output_path.write_text(yaml.safe_dump(plan, sort_keys=False))
        return output_path

    def start_run_many(
        self,
        plans_dir: Path,
        concurrency: int,
        selected_plan_names: list[str] | None = None,
        provider_override: str | None = None,
    ) -> str:
        run_id = build_multi_run_id()

        def _run() -> None:
            run_many_plans(
                plans_dir.resolve(),
                concurrency=concurrency,
                verbose=False,
                selected_plan_names=selected_plan_names,
                run_id_override=run_id,
                provider_override=provider_override,
            )
            index_repository_state(self.repo_path, db_path=self.db_path)

        threading.Thread(target=_run, daemon=True).start()
        return run_id

    def start_run_plan_across_projects(
        self,
        plan_path: Path,
        project_paths: list[Path],
        provider_override: str | None = None,
    ) -> str:
        run_id = build_multi_run_id()
        normalized_projects = sorted({path.expanduser().resolve() for path in project_paths})

        def _run() -> None:
            for project_path in normalized_projects:
                try:
                    run_plan(
                        plan_path=plan_path.resolve(),
                        verbose=False,
                        approve_each_step=False,
                        branch=None,
                        commit=False,
                        commit_message=None,
                        allow_dirty_start=False,
                        review_enabled=False,
                        repo_override=str(project_path),
                        interactive=False,
                        provider_override=provider_override,
                    )
                except PlanError:
                    continue
                try:
                    index_repository_state(project_path)
                except PlanError:
                    continue

        threading.Thread(target=_run, daemon=True).start()
        return run_id

    # -- Agent sessions -------------------------------------------------------

    @property
    def sessions_dir(self) -> Path:
        return dashboard_sessions_dir(self.repo_path)

    def _session_dir(self, session_id: str) -> Path:
        return dashboard_session_dir(self.repo_path, session_id)

    def _session_meta_path(self, session_id: str) -> Path:
        return dashboard_session_meta_path(self.repo_path, session_id)

    def _session_output_path(self, session_id: str) -> Path:
        return dashboard_session_output_path(self.repo_path, session_id)

    def _write_session_meta(self, meta: dict[str, object]) -> None:
        dashboard_write_session_meta(self.repo_path, meta)

    def _read_session_meta(self, session_id: str) -> dict[str, object] | None:
        return dashboard_read_session_meta(self.repo_path, session_id)

    def list_sessions(self) -> list[dict[str, object]]:
        return session_list_sessions(self.repo_path)

    def get_session(self, session_id: str) -> dict[str, object] | None:
        return session_get_session(self.repo_path, session_id)

    def read_session_output(self, session_id: str) -> str:
        return session_read_output(self.repo_path, session_id)

    def _run_session_subprocess(
        self,
        meta: dict[str, object],
        command: list[str],
        output_path: Path,
    ) -> None:
        dashboard_run_session_subprocess(self.repo_path, meta, command, output_path)

    def start_agent_session(
        self,
        project_path: str,
        prompt: str,
        provider: str,
    ) -> str:
        resolved = str(Path(project_path).expanduser().resolve())
        if not Path(resolved).is_dir():
            raise PlanError(f"Project path is not a directory: {resolved}")

        session_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
        provider_session_id = str(uuid.uuid4())
        output_path = self._session_output_path(session_id)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.touch()

        now = datetime.now(timezone.utc).isoformat()
        meta: dict[str, object] = {
            "id": session_id,
            "project_path": resolved,
            "project_name": Path(resolved).name,
            "prompt": prompt,
            "provider": provider,
            "provider_session_id": provider_session_id,
            "status": "running",
            "started_at": now,
            "ended_at": None,
            "exit_code": None,
            "pid": None,
            "messages": [{"role": "user", "content": prompt, "timestamp": now}],
        }
        self._write_session_meta(meta)

        if provider == "claude":
            command = [
                "claude", "--dangerously-skip-permissions",
                "--session-id", provider_session_id,
                "-p", prompt,
            ]
        else:
            command = ["codex", "exec", "--full-auto", "--cd", resolved, prompt]

        self._run_session_subprocess(meta, command, output_path)
        return session_id

    def reply_to_session(self, session_id: str, reply: str) -> None:
        meta = self._read_session_meta(session_id)
        if not meta:
            raise PlanError(f"Session not found: {session_id}")
        if meta.get("status") == "running":
            raise PlanError("Session is still running. Wait for it to finish before replying.")

        provider = str(meta.get("provider") or "codex")
        provider_session_id = str(meta.get("provider_session_id") or "")
        output_path = self._session_output_path(session_id)

        now = datetime.now(timezone.utc).isoformat()
        messages = list(meta.get("messages") or [])
        messages.append({"role": "user", "content": reply, "timestamp": now})
        meta["messages"] = messages
        meta["status"] = "running"
        meta["ended_at"] = None
        meta["exit_code"] = None
        self._write_session_meta(meta)

        with output_path.open("a", encoding="utf-8") as log:
            log.write(f"\n{'─' * 60}\n")
            log.write(f"[follow-up #{len(messages)}]\n")
            log.write(f"{'─' * 60}\n\n")

        if provider == "claude" and provider_session_id:
            command = [
                "claude", "--dangerously-skip-permissions",
                "--resume", provider_session_id,
                "-p", reply,
            ]
        else:
            command = ["codex", "exec", "resume", "--last", "--full-auto", reply]

        self._run_session_subprocess(meta, command, output_path)

    def stop_agent_session(self, session_id: str) -> bool:
        meta = self._read_session_meta(session_id)
        if not meta:
            return False
        pid = meta.get("pid")
        if not pid or meta.get("status") != "running":
            return False
        try:
            os.kill(int(pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass
        return True

    # -- Page renderers --------------------------------------------------------

    def render_sessions_page(self, *, action_message: str | None = None, prefill_project: str | None = None) -> str:
        return render_sessions(self, action_message=action_message, prefill_project=prefill_project)

    def render_session_detail_page(self, session_id: str) -> str:
        return render_session_detail(self, session_id)

    def render_actions_page(self, *, action_message: str | None = None) -> str:
        return render_actions(
            self,
            action_message=action_message,
            summarize_preflight=summarize_preflight_for_dashboard,
        )

    def render_project_detail_page(self, project_path: str, *, action_message: str | None = None) -> str:
        return render_project_detail(self, project_path, action_message=action_message)

    def render_projects_page(self, *, action_message: str | None = None) -> str:
        return render_projects(self, action_message=action_message)

    def render_page(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> str:
        return render_dashboard(
            self,
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )

    def render_dashboard_detail_page(
        self,
        *,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> str:
        return render_dashboard_detail(
            self,
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )

    def render_run_page(
        self,
        run_id: str,
        plan_execution_id: str | None = None,
    ) -> str:
        return render_run(self, run_id=run_id, plan_execution_id=plan_execution_id)

    def render_route(
        self,
        path: str,
        params: dict[str, list[str]] | None = None,
    ) -> str:
        params = params or {}
        action_message = params.get("message", [None])[0]

        if path == "/actions":
            return self.render_actions_page(action_message=action_message)
        if path == "/projects/detail":
            project_path = params.get("path", [""])[0]
            return self.render_project_detail_page(project_path, action_message=action_message)
        if path == "/projects":
            return self.render_projects_page(action_message=action_message)
        if path == "/sessions/detail":
            session_id = params.get("id", [""])[0]
            return self.render_session_detail_page(session_id)
        if path == "/sessions":
            prefill_project = params.get("project", [None])[0]
            return self.render_sessions_page(action_message=action_message, prefill_project=prefill_project)
        if path == "/runs/detail":
            run_id = params.get("id", [""])[0]
            if not run_id:
                raise PlanError("Run id is required.")
            plan_execution_id = params.get("plan_execution_id", [None])[0]
            return self.render_run_page(run_id=run_id, plan_execution_id=plan_execution_id)
        if path == "/":
            run_id = params.get("run_id", [None])[0]
            plan_execution_id = params.get("plan_execution_id", [None])[0]
            selected_plan_file = params.get("selected_plan_file", [None])[0]
            return self.render_page(
                run_id=run_id,
                plan_execution_id=plan_execution_id,
                action_message=action_message,
                selected_plan_file=selected_plan_file,
            )
        raise PlanError(f"Unsupported route: {path}")

    def handle_action(
        self,
        path: str,
        form_data: dict[str, list[str]] | None = None,
    ) -> DashboardActionResult:
        form_data = form_data or {}
        run_id: str | None = None
        redirect_to = "/actions"

        if path == "/actions/rerun-plan":
            plan_file_path_value = form_data.get("plan_file_path", [""])[0].strip()
            if not plan_file_path_value:
                raise PlanError("Plan file path is required.")
            plan_path = Path(plan_file_path_value)
            if not plan_path.exists():
                raise PlanError(f"Plan file not found: {plan_path}")
            plans_dir = plan_path.parent
            plan_file_name = plan_path.name
            provider_override = form_data.get("provider_override", [""])[0].strip() or None
            run_id = self.start_run_many(
                plans_dir,
                concurrency=1,
                selected_plan_names=[plan_file_name],
                provider_override=provider_override,
            )
            return DashboardActionResult(
                redirect_to="/",
                message=f"Rerun started for {plan_file_name}.",
                run_id=run_id,
            )

        if path == "/actions/add-project":
            project_path_value = form_data.get("project_path", [""])[0].strip()
            if not project_path_value:
                raise PlanError("Project path is required.")
            self.add_tracked_project(Path(project_path_value).expanduser())
            return DashboardActionResult(
                redirect_to="/projects",
                message=f"Tracked project: {Path(project_path_value).expanduser().resolve()}",
            )

        if path == "/actions/remove-project":
            project_path_value = form_data.get("project_path", [""])[0].strip()
            if not project_path_value:
                raise PlanError("Project path is required.")
            self.remove_tracked_project(project_path_value)
            return DashboardActionResult(
                redirect_to="/projects",
                message=f"Removed tracked project: {Path(project_path_value).expanduser().resolve()}",
            )

        if path == "/actions/run-plan-across-projects":
            selected_plan_names = [name.strip() for name in form_data.get("selected_plans", []) if name.strip()]
            if len(selected_plan_names) != 1:
                raise PlanError("Select exactly one plan to run across projects.")
            project_paths = [
                Path(path_value).expanduser().resolve()
                for path_value in form_data.get("project_paths", [])
                if path_value.strip()
            ]
            if not project_paths:
                raise PlanError("Select at least one tracked project.")
            target_repo_value = form_data.get("target_repo", [""])[0].strip()
            if not target_repo_value:
                raise PlanError("Target repo is required.")
            target_repo = Path(target_repo_value).expanduser().resolve()
            plans_dir_value = form_data.get("plans_dir", [""])[0].strip()
            plans_dir = Path(plans_dir_value).expanduser() if plans_dir_value else self.plans_dir_for_repo(target_repo)
            plan_path = plans_dir.resolve() / selected_plan_names[0]
            if not plan_path.exists():
                raise PlanError(f"Plan file not found: {plan_path}")
            provider_override = form_data.get("provider_override", [""])[0].strip() or None
            run_id = self.start_run_plan_across_projects(
                plan_path=plan_path,
                project_paths=project_paths,
                provider_override=provider_override,
            )
            return DashboardActionResult(
                redirect_to="/",
                message=f"Started single-plan run for {selected_plan_names[0]} across {len(project_paths)} project(s).",
                run_id=run_id,
            )

        if path == "/actions/index":
            self.run_index_now()
            return DashboardActionResult(redirect_to="/actions", message="Index refreshed.")

        if path == "/actions/create-plan":
            template_name = form_data.get("template_name", [""])[0].strip()
            target_repo_value = form_data.get("target_repo", [""])[0].strip()
            output_path_value = form_data.get("output_path", [""])[0].strip()
            objective = form_data.get("objective", [""])[0].strip()
            if not template_name:
                raise PlanError("Template name is required.")
            if not target_repo_value:
                raise PlanError("Target repo is required.")
            if not output_path_value:
                raise PlanError("Plan file name is required.")
            if not objective:
                raise PlanError("Objective is required.")
            created_path = self.create_plan(
                target_repo=Path(target_repo_value).expanduser(),
                template_name=template_name,
                output_name=output_path_value,
                objective=objective,
                force=form_data.get("force", [""])[0] == "1",
            )
            return DashboardActionResult(
                redirect_to="/actions",
                message=f"Created plan at {created_path}.",
            )

        if path == "/actions/start-session":
            project_path_value = form_data.get("project_path", [""])[0].strip()
            if not project_path_value:
                raise PlanError("Project is required.")
            prompt_value = form_data.get("prompt", [""])[0].strip()
            if not prompt_value:
                raise PlanError("Prompt is required.")
            provider_value = form_data.get("provider", [""])[0].strip()
            if not provider_value:
                providers = available_providers()
                provider_value = providers[0][0] if providers else "codex"
            session_id = self.start_agent_session(
                project_path=project_path_value,
                prompt=prompt_value,
                provider=provider_value,
            )
            return DashboardActionResult(
                redirect_to=f"/sessions/detail?id={session_id}",
                message="Session started.",
            )

        if path == "/actions/stop-session":
            session_id_value = form_data.get("session_id", [""])[0].strip()
            if not session_id_value:
                raise PlanError("Session ID is required.")
            self.stop_agent_session(session_id_value)
            return DashboardActionResult(
                redirect_to=f"/sessions/detail?id={session_id_value}",
                message="Session stop signal sent.",
            )

        if path == "/actions/session-reply":
            session_id_value = form_data.get("session_id", [""])[0].strip()
            if not session_id_value:
                raise PlanError("Session ID is required.")
            reply_value = form_data.get("reply", [""])[0].strip()
            if not reply_value:
                raise PlanError("Reply message is required.")
            self.reply_to_session(session_id_value, reply_value)
            return DashboardActionResult(
                redirect_to=f"/sessions/detail?id={session_id_value}",
                message="Reply sent.",
            )

        if path.startswith("/actions/project-git-"):
            git_path = form_data.get("path", [""])[0].strip()
            if not git_path:
                raise PlanError("Project path is required.")
            repo = Path(git_path).expanduser().resolve()
            if path == "/actions/project-git-commit":
                commit_msg = form_data.get("message", [""])[0].strip()
                if not commit_msg:
                    raise PlanError("Commit message is required.")
                sha = stage_and_commit(repo, commit_msg)
                message = f"Committed {sha}"
            elif path == "/actions/project-git-switch":
                branch_name = form_data.get("branch", [""])[0].strip()
                if not branch_name:
                    raise PlanError("Branch name is required.")
                switch_branch(repo, branch_name)
                message = f"Switched to {branch_name}"
            elif path == "/actions/project-git-create-branch":
                branch_name = form_data.get("branch", [""])[0].strip()
                if not branch_name:
                    raise PlanError("Branch name is required.")
                create_branch(repo, branch_name)
                message = f"Created and switched to {branch_name}"
            elif path == "/actions/project-git-pull":
                remote_name = form_data.get("remote", ["origin"])[0].strip()
                output = git_pull(repo, remote=remote_name)
                message = f"Pulled from {remote_name}"
                if output:
                    message += f": {output[:120]}"
            elif path == "/actions/project-git-push":
                remote_name = form_data.get("remote", ["origin"])[0].strip()
                try:
                    output = git_push(repo, remote=remote_name)
                except PlanError:
                    output = git_push(repo, remote=remote_name, set_upstream=True)
                message = f"Pushed to {remote_name}"
                if output:
                    message += f": {output[:120]}"
            elif path == "/actions/project-git-stash":
                stash_msg = form_data.get("stash_message", [""])[0].strip() or None
                git_stash_save(repo, message=stash_msg)
                message = "Changes stashed"
            elif path == "/actions/project-git-stash-pop":
                git_stash_pop(repo)
                message = "Stash popped"
            elif path == "/actions/project-git-discard":
                discard_all_changes(repo)
                message = "All changes discarded"
            else:
                raise PlanError("Unknown git action.")
            return DashboardActionResult(
                redirect_to=_page_link("/projects/detail", path=git_path, message=message),
                message=message,
            )

        if path == "/actions/run-many":
            target_repo_value = form_data.get("target_repo", [""])[0].strip()
            if not target_repo_value:
                raise PlanError("Target repo is required.")
            target_repo = Path(target_repo_value).expanduser().resolve()
            plans_dir_value = form_data.get("plans_dir", [""])[0].strip()
            plans_dir = Path(plans_dir_value).expanduser() if plans_dir_value else self.plans_dir_for_repo(target_repo)
            concurrency_value = form_data.get("concurrency", ["1"])[0].strip() or "1"
            concurrency = int(concurrency_value)
            if concurrency < 1:
                raise PlanError("Concurrency must be at least 1.")
            selected_plan_names = [name.strip() for name in form_data.get("selected_plans", []) if name.strip()]
            provider_override = form_data.get("provider_override", [""])[0].strip() or None
            run_id = self.start_run_many(
                plans_dir,
                concurrency,
                selected_plan_names=selected_plan_names or None,
                provider_override=provider_override,
            )
            if len(selected_plan_names) == 1:
                message = f"Started plan run for {selected_plan_names[0]} in {plans_dir}."
            else:
                message = f"Started run-many for {plans_dir}."
            return DashboardActionResult(redirect_to="/", message=message, run_id=run_id)

        raise PlanError(f"Unsupported action: {path}")


def serve_dashboard(
    repo_path: Path,
    host: str,
    port: int,
    db_path: Path | None = None,
    *,
    announce_url: str | None = None,
    tailscale: bool = False,
) -> int:
    app = DashboardApp(repo_path=repo_path, db_path=db_path)
    return dashboard_server_serve(
        app=app,
        host=host,
        port=port,
        summarize_preflight=summarize_preflight_for_dashboard,
        announce_url=announce_url,
        tailscale=tailscale,
    )
