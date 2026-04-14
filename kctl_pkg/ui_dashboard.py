from __future__ import annotations

import json
import os
import signal
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

from .multi import build_multi_run_id, load_normalized_multi_plans, resolve_multi_run_log, run_many_plans
from .git import (
    check_remote_connectivity,
    create_branch,
    discard_all_changes,
    ensure_git_repo,
    get_full_diff,
    get_project_git_detail,
    get_project_git_summary,
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
        return self.repo_path / ".kctl" / "dashboard-projects.json"

    def load_tracked_projects(self) -> list[str]:
        path = self.projects_file_path
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(data, list):
            return []
        projects: list[str] = []
        for item in data:
            if not isinstance(item, str) or not item.strip():
                continue
            projects.append(str(Path(item).expanduser().resolve()))
        return sorted(dict.fromkeys(projects))

    def save_tracked_projects(self, projects: list[str]) -> None:
        path = self.projects_file_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(sorted(dict.fromkeys(projects)), indent=2) + "\n")

    def add_tracked_project(self, project_path: Path) -> None:
        ensure_git_repo(project_path)
        repo_root = get_repo_root(project_path)
        resolved = str(repo_root)
        projects = self.load_tracked_projects()
        if resolved in projects:
            raise PlanError(f"Project already tracked: {resolved}")
        projects.append(resolved)
        self.save_tracked_projects(projects)

    def remove_tracked_project(self, project_path: str) -> None:
        normalized_target = str(Path(project_path).expanduser().resolve())
        projects = [path for path in self.load_tracked_projects() if path != normalized_target]
        self.save_tracked_projects(projects)

    def plans_dir_for_repo(self, target_repo: Path) -> Path:
        return target_repo.resolve() / ".kctl" / "plans"

    def load_live_run_data(self, run_id: str) -> dict[str, object] | None:
        try:
            run_log = resolve_multi_run_log(self.repo_path, run_id)
        except PlanError:
            return None
        try:
            return json.loads(run_log.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def load_saved_run_data(self, run_root_path: str | None) -> dict[str, object] | None:
        if not run_root_path:
            return None
        run_log = Path(run_root_path) / "run.json"
        if not run_log.exists():
            return None
        try:
            return json.loads(run_log.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def read_live_output(self, run_data: dict[str, object] | None) -> tuple[str | None, str | None, str | None]:
        if not run_data:
            return None, None, None
        stream_log_path = run_data.get("stream_log_path")
        if stream_log_path:
            candidate = Path(str(stream_log_path))
        else:
            run_output_dir = run_data.get("run_output_dir")
            if run_output_dir:
                candidate = Path(str(run_output_dir)).parent / "stream.log"
            else:
                candidate = Path(str(run_data.get("artifact_root_path") or "")) / str(run_data.get("run_id") or "") / "stream.log"
        if not candidate.exists():
            return None, str(candidate), str(run_data.get("status") or "unknown")
        try:
            return candidate.read_text(), str(candidate.resolve()), str(run_data.get("status") or "unknown")
        except OSError:
            return None, str(candidate), str(run_data.get("status") or "unknown")

    def load_state(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> DashboardState:
        repository = get_repository(self.repo_path, db_path=self.db_path)
        overview = get_repository_overview(self.repo_path, db_path=self.db_path)
        attention_items = list_attention_items(self.repo_path, db_path=self.db_path)
        workspaces = list_workspaces(self.repo_path, db_path=self.db_path)
        runs = list_runs(self.repo_path, db_path=self.db_path)
        templates = load_plan_templates(project_root())
        plan_templates = [
            (template_name, template.get("description") if isinstance(template, dict) else None)
            for template_name, template in templates.items()
        ]
        tracked_projects = self.load_tracked_projects()
        selected_run: RunDetail | None = None
        plan_cards: list[PlanExecutionCard] = []
        selected_plan: PlanExecutionCard | None = None
        selected_plan_file_name: str | None = None
        selected_plan_file_path: str | None = None
        selected_plan_file_contents: str | None = None
        launch_preflight = summarize_preflight_for_dashboard(
            str(self.repo_path),
            str(self.default_plans_dir),
            selected_plan_names=None,
            provider_override=None,
        )
        selected_run_preflight: dict[str, object] | None = None
        live_output: str | None = None
        live_output_status: str | None = None
        live_output_path: str | None = None
        steps: list[StepTimelineItem] = []
        workspace: WorkspaceDetail | None = None
        live_run_data: dict[str, object] | None = None

        if run_id is None and runs:
            run_id = runs[0].id
        if run_id is not None:
            live_run_data = self.load_live_run_data(run_id)
            try:
                selected_run = get_run(self.repo_path, run_id, db_path=self.db_path)
                plan_cards = list_plan_executions(self.repo_path, run_id, db_path=self.db_path)
            except PlanError:
                if live_run_data is not None:
                    selected_run = self._build_run_detail_from_live_data(live_run_data)
                    plan_cards = self._build_plan_cards_from_live_data(live_run_data)
                else:
                    raise
            if live_run_data is not None and str(live_run_data.get("status") or "") == "running":
                selected_run = self._build_run_detail_from_live_data(live_run_data)
                plan_cards = self._build_plan_cards_from_live_data(live_run_data)
            selected_run_preflight = _preflight_run_snapshot(
                live_run_data or (self.load_saved_run_data(selected_run.run_root_path) if selected_run is not None else None)
            )

        if plan_execution_id is None and plan_cards:
            plan_execution_id = plan_cards[0].id
        if plan_execution_id is not None:
            try:
                selected_plan = get_plan_execution(plan_execution_id, self.repo_path, db_path=self.db_path)
                steps = list_step_executions(plan_execution_id, self.repo_path, db_path=self.db_path)
                workspace = get_workspace(plan_execution_id, self.repo_path, db_path=self.db_path)
            except PlanError:
                if live_run_data is not None:
                    selected_plan = next((plan for plan in plan_cards if plan.id == plan_execution_id), None)
        if selected_plan_file:
            plan_file_path = Path(selected_plan_file).expanduser().resolve()
            selected_plan_file_name, selected_plan_file_contents = read_plan_file(plan_file_path)
            selected_plan_file_path = str(plan_file_path)
        live_output, live_output_path, live_output_status = self.read_live_output(live_run_data)

        return DashboardState(
            repo_name=repository.name,
            repo_root=repository.root_path,
            action_message=action_message,
            plan_templates=plan_templates,
            tracked_projects=tracked_projects,
            available_providers=available_providers(),
            overview=overview,
            attention_items=attention_items,
            workspaces=workspaces,
            runs=runs,
            selected_run=selected_run,
            plan_cards=plan_cards,
            selected_plan=selected_plan,
            selected_plan_file_name=selected_plan_file_name,
            selected_plan_file_path=selected_plan_file_path,
            selected_plan_file_contents=selected_plan_file_contents,
            launch_preflight=launch_preflight,
            selected_run_preflight=selected_run_preflight,
            live_output=live_output,
            live_output_status=live_output_status,
            live_output_path=live_output_path,
            steps=steps,
            workspace=workspace,
        )

    def _build_run_detail_from_live_data(self, run_data: dict[str, object]) -> RunDetail:
        plan_states = list(run_data.get("plans") or [])
        return RunDetail(
            id=str(run_data.get("run_id") or ""),
            repository_id=str(self.repo_path),
            status=str(run_data.get("status") or "unknown"),
            launch_source="plans_run_many",
            plans_dir=str(run_data.get("plans_dir") or ""),
            concurrency=int(run_data.get("concurrency") or 1),
            started_at=str(run_data.get("started_at") or ""),
            ended_at=run_data.get("ended_at"),
            run_root_path=str(Path(str(run_data.get("artifact_root_path") or "")) / str(run_data.get("run_id") or "")),
            plan_execution_count=len(plan_states),
            passed_count=sum(1 for plan in plan_states if plan.get("status") == "passed"),
            failed_count=sum(1 for plan in plan_states if plan.get("status") == "failed"),
            running_count=sum(1 for plan in plan_states if plan.get("status") == "running"),
            blocked_count=sum(1 for plan in plan_states if plan.get("status") == "blocked"),
        )

    def _build_plan_cards_from_live_data(self, run_data: dict[str, object]) -> list[PlanExecutionCard]:
        run_id = str(run_data.get("run_id") or "")
        plan_cards: list[PlanExecutionCard] = []
        for plan_state in run_data.get("plans") or []:
            plan_path = str(plan_state.get("plan_path") or "")
            plan_slug = Path(plan_path).stem if plan_path else str(plan_state.get("plan_id") or "plan")
            plan_id = str(plan_state.get("plan_id") or plan_slug)
            plan_cards.append(
                PlanExecutionCard(
                    id=f"{run_id}:{plan_id}",
                    run_id=run_id,
                    repository_id=str(self.repo_path),
                    plan_definition_id=f"{self.repo_path}:{Path(plan_path).resolve()}" if plan_path else f"{self.repo_path}:{plan_id}",
                    plan_slug=plan_slug,
                    plan_title=None,
                    plan_file_path=plan_path,
                    objective="",
                    phase_name=None,
                    group_name=None,
                    status=str(plan_state.get("status") or "unknown"),
                    current_step_key=plan_state.get("current_step"),
                    verify_status=str(plan_state.get("verify_result") or "not_run"),
                    started_at=str(run_data.get("started_at") or ""),
                    ended_at=run_data.get("ended_at"),
                    worktree_path=plan_state.get("worktree_path"),
                    branch_name=plan_state.get("branch_name"),
                    log_path=plan_state.get("log_path"),
                    changed_files_count=0,
                    failure_reason=None,
                )
            )
        return plan_cards

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
        templates = load_plan_templates(project_root())
        plan_templates = [
            (template_name, template.get("description") if isinstance(template, dict) else None)
            for template_name, template in templates.items()
        ]
        tracked_projects = self.load_tracked_projects()
        providers = available_providers()
        launch_preflight = summarize_preflight_for_dashboard(
            str(self.repo_path),
            str(self.default_plans_dir),
            selected_plan_names=None,
            provider_override=None,
        )
        preflight_items = launch_preflight.get("items", {})
        preflight_html = "".join(
            _render_preflight_item_html(label, item)
            for label, item in (
                ("Repo", preflight_items.get("repo") or {}),
                ("Plans Dir", preflight_items.get("plans_dir") or {}),
                ("Binaries", preflight_items.get("binaries") or {}),
                ("Writable Paths", preflight_items.get("writable_paths") or {}),
                ("Required Env", preflight_items.get("required_env") or {}),
            )
        )
        tracked_projects_html = (
            "".join(
                f"<label class='checkbox'><input type='checkbox' name='project_paths' value='{_escape(p)}'> {_escape(p)}</label>"
                for p in tracked_projects
            )
            if tracked_projects
            else "<div class='help'>No tracked projects yet. <a href='/projects'>Manage projects</a></div>"
        )
        notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
        body = (
            f"<main class='single-column'>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'>"
            f"<h2>Refresh Index</h2>"
            f"<div class='help'>Refreshes the dashboard data from saved runs and workspaces on this machine.</div>"
            f"<form method='post' action='/actions/index'>"
            f"<button type='submit'>Refresh Index</button>"
            f"</form>"
            f"</section>"
            f"<section class='panel'>"
            f"<h2>Run Plans</h2>"
            f"<div class='help'>Runs every plan in the plans folder for the target project.</div>"
            f"<form method='post' action='/actions/run-many'>"
            f"<label for='target_repo_run_many'><strong>Target Repo</strong></label>"
            f"<input id='target_repo_run_many' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
            f"<div id='target_repo_run_many_status' class='repo-check'></div>"
            f"<div><strong>Plans Dir</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
            f"<label for='plans_dir'><strong>Plans Dir Override</strong></label>"
            f"<input id='plans_dir' name='plans_dir' type='text' placeholder='Optional override'>"
            f"<div id='plans_dir_status' class='repo-check'></div>"
            f"<div id='plans_dir_preview' class='plans-preview'></div>"
            f"<div><strong>Tracked Projects</strong></div>"
            f"{tracked_projects_html}"
            f"<div class='preflight-summary'>"
            f"<div class='launch-decision launch-decision-{_escape(_preflight_status_tone(str(launch_preflight.get('status') or 'warn')))}' id='run_many_launch_decision'>{_escape(launch_preflight.get('decision') or 'Runnable with warnings')}</div>"
            f"<div><strong>Launch Preflight</strong></div>"
            f"<div id='run_many_preflight_message' class='repo-check' data-status='{_escape(launch_preflight.get('status'))}'>{_escape(launch_preflight.get('message'))}</div>"
            f"<div id='run_many_preflight' class='preflight-grid'>{preflight_html}</div>"
            f"</div>"
            f"<label for='concurrency'><strong>Concurrency</strong></label>"
            f"<input id='concurrency' name='concurrency' type='number' min='1' value='1'>"
            f"<div class='help'>How many plans can run at the same time. Use 1 for the safest option.</div>"
            + (_provider_select_html("provider_override", providers) if providers else "")
            + "<button type='submit' id='run_many_submit_button'>Run Plans</button>"
            + "</form>"
            + "</section>"
            + "<section class='panel'>"
            + "<h2>Run Plan Across Projects</h2>"
            + "<div class='help'>Run a single plan file against every tracked project. "
            + "Select one plan from the list above, then choose which projects to target.</div>"
            + f"<form method='post' action='/actions/run-plan-across-projects'>"
            + f"<input type='hidden' name='target_repo' value='{_escape(self.repo_path)}'>"
            + f"<input type='hidden' name='plans_dir' value=''>"
            + "<label for='cross_selected_plans'><strong>Plan</strong></label>"
            + "<div class='help'>Use the Plans Dir listing above to identify the plan filename, then enter it here.</div>"
            + "<input id='cross_selected_plans' name='selected_plans' type='text' placeholder='001-sample.yaml' required>"
            + "<label><strong>Target Projects</strong></label>"
            + (tracked_projects_html if tracked_projects else "<div class='help'>No tracked projects. <a href='/projects'>Add projects</a> first.</div>")
            + (_provider_select_html("provider_override", providers) if providers else "")
            + "<button type='submit' id='run_single_across_projects_button'>Run Across Projects</button>"
            + "</form>"
            + "</section>"
            + "<section class='panel'>"
            + "<h2>Create Plan</h2>"
            + "<div class='help'>Creates one new plan file in the target project's plans folder.</div>"
            + "<form method='post' action='/actions/create-plan'>"
            + f"<label for='target_repo_create_plan'><strong>Target Repo</strong></label>"
            + f"<input id='target_repo_create_plan' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
            + "<div id='target_repo_create_plan_status' class='repo-check'></div>"
            + "<label for='template_name'><strong>Template</strong></label>"
            + "<select id='template_name' name='template_name'>"
            + "".join(
                f"<option value='{_escape(name)}'>{_escape(name)}"
                + (f" - {_escape(desc)}" if desc else "")
                + "</option>"
                for name, desc in plan_templates
            )
            + "</select>"
            + f"<div><strong>Plan Root</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
            + "<label for='output_path'><strong>Plan File Name</strong></label>"
            + "<input id='output_path' name='output_path' type='text' placeholder='001-sample.yaml' required>"
            + "<label for='objective'><strong>Objective</strong></label>"
            + "<textarea id='objective' name='objective' rows='5' placeholder='Describe the change' required></textarea>"
            + "<label class='checkbox'><input name='force' type='checkbox' value='1'> Overwrite if the file exists</label>"
            + "<button type='submit'>Create Plan</button>"
            + "</form>"
            + "</section>"
            + "</div>"
            + "</main>"
        )
        actions_script = """\
function renderPreflight(preflight, messageId, containerId) {
  const message = document.getElementById(messageId);
  const container = document.getElementById(containerId);
  const decision = document.getElementById('run_many_launch_decision');
  if (!message || !container || !preflight) return;
  message.dataset.status = preflight.status || 'unknown';
  message.textContent = preflight.message || '';
  const bannerTone = preflight.status === 'pass' || preflight.status === 'ok'
    ? 'pass'
    : (preflight.status === 'block' || preflight.status === 'blocked' || preflight.status === 'error')
      ? 'block'
      : 'warn';
  if (decision) {
    decision.className = `launch-decision launch-decision-${bannerTone}`;
    decision.textContent = preflight.decision || (bannerTone === 'pass' ? 'Ready to run' : bannerTone === 'block' ? 'Blocked' : 'Runnable with warnings');
  }
  const labels = [
    ['repo', 'Repo'],
    ['plans_dir', 'Plans Dir'],
    ['binaries', 'Binaries'],
    ['writable_paths', 'Writable Paths'],
    ['required_env', 'Required Env'],
  ];
  container.innerHTML = labels.map(([key, label]) => {
    const item = (preflight.items || {})[key] || {};
    const details = item.details ? `<div class="help">${item.details}</div>` : '';
    const remediation = item.remediation ? `<div class="help"><strong>Fix:</strong> ${item.remediation}</div>` : '';
    const action = item.action_label && item.action_value
      ? `<button type="button" class="mini-button" data-copy="${item.action_value}">${item.action_label}</button>`
      : '';
    const tone = item.status === 'pass' || item.status === 'ok'
      ? 'pass'
      : (item.status === 'block' || item.status === 'blocked' || item.status === 'error' || item.status === 'missing' || item.status === 'not_dir' || item.status === 'empty')
        ? 'block'
        : 'warn';
    const statusClass = tone === 'pass' ? 'status-success' : tone === 'block' ? 'status-failure' : 'status-neutral';
    return `<div class="preflight-item ${statusClass}"><div><strong>${label}</strong> <span class="preflight-badge preflight-badge-${tone}">${tone.toUpperCase()}</span></div><div>${item.summary || ''}</div>${details}${remediation}${action}</div>`;
  }).join('');
  wireCopyButtons(container);
}
function wirePlansPreview(targetRepoInputId, plansDirInputId, statusId, previewId, preflightMessageId, preflightContainerId) {
  const targetRepoInput = document.getElementById(targetRepoInputId);
  const plansDirInput = document.getElementById(plansDirInputId);
  const runManyForm = plansDirInput ? plansDirInput.closest('form') : null;
  const providerOverrideInput = runManyForm ? runManyForm.querySelector('select[name="provider_override"]') : null;
  const runManyButton = document.getElementById('run_many_submit_button');
  const runAcrossProjectsButton = document.getElementById('run_single_across_projects_button');
  const status = document.getElementById(statusId);
  const preview = document.getElementById(previewId);
  if (!targetRepoInput || !plansDirInput || !status || !preview) return;
  let timer = null;
  function updateRunButtonLabels() {
    const selectedCount = preview.querySelectorAll('input[name="selected_plans"]:checked').length;
    if (runManyButton) {
      runManyButton.textContent = selectedCount === 1 ? 'Run Plan' : 'Run Plans';
    }
    if (runAcrossProjectsButton) {
      runAcrossProjectsButton.disabled = selectedCount !== 1;
      runAcrossProjectsButton.textContent = selectedCount === 1
        ? 'Run Plan Across Projects'
        : 'Select One Plan to Run Across Projects';
    }
  }
  function resolvedPlansDir() {
    const overrideValue = plansDirInput.value.trim();
    if (overrideValue) return overrideValue;
    const repoValue = targetRepoInput.value.trim();
    if (!repoValue) return "";
    return repoValue.replace(/\\/+$/, "") + "/.kctl/plans";
  }
  async function refreshPreflight() {
    const selectedPlans = Array.from(preview.querySelectorAll('input[name="selected_plans"]:checked')).map((node) => node.value);
    const params = new URLSearchParams({
      target_repo: targetRepoInput.value.trim(),
      plans_dir: resolvedPlansDir(),
    });
    if (providerOverrideInput && providerOverrideInput.value.trim()) {
      params.set('provider_override', providerOverrideInput.value.trim());
    }
    selectedPlans.forEach((plan) => params.append('selected_plans', plan));
    const response = await fetch(`/api/preflight?${params.toString()}`);
    const data = await response.json();
    renderPreflight(data, preflightMessageId, preflightContainerId);
  }
  async function refreshPreview() {
    const params = new URLSearchParams({ path: resolvedPlansDir() });
    const response = await fetch(`/api/list-plans?${params.toString()}`);
    const data = await response.json();
    status.dataset.status = data.status;
    status.textContent = data.message;
    if (!data.plans || data.plans.length === 0) {
      preview.innerHTML = "";
      updateRunButtonLabels();
      refreshPreflight();
      return;
    }
    preview.innerHTML =
      "<strong>Plans found</strong>" +
      data.plans.map((plan) => `<label><input type="checkbox" name="selected_plans" value="${plan}"> <span>${plan}</span></label>`).join("");
    preview.querySelectorAll('input[name="selected_plans"]').forEach((node) => {
      node.addEventListener('change', () => {
        updateRunButtonLabels();
        refreshPreflight();
      });
    });
    updateRunButtonLabels();
    refreshPreflight();
  }
  function scheduleRefresh() {
    clearTimeout(timer);
    timer = setTimeout(refreshPreview, 150);
  }
  targetRepoInput.addEventListener('input', scheduleRefresh);
  plansDirInput.addEventListener('input', scheduleRefresh);
  if (providerOverrideInput) {
    providerOverrideInput.addEventListener('change', scheduleRefresh);
  }
  refreshPreview();
}
window.addEventListener('DOMContentLoaded', () => {
  wireCopyButtons(document);
  wireRepoCheck('target_repo_run_many', 'target_repo_run_many_status');
  wireRepoCheck('target_repo_create_plan', 'target_repo_create_plan_status');
  wirePlansPreview(
    'target_repo_run_many',
    'plans_dir',
    'plans_dir_status',
    'plans_dir_preview',
    'run_many_preflight_message',
    'run_many_preflight'
  );
});
"""
        return self._page_shell(active_nav="Actions", body=body, extra_script=actions_script)

    def render_project_detail_page(self, project_path: str, *, action_message: str | None = None) -> str:
        resolved = str(Path(project_path).expanduser().resolve())
        tracked = self.load_tracked_projects()
        if resolved not in tracked:
            return self._page_shell(
                active_nav="Projects",
                body=(
                    "<main class='single-column'><div class='column'>"
                    "<a class='back-link' href='/projects'>&larr; All Projects</a>"
                    f"<section class='panel'><div class='empty'>Project not tracked: {_escape(resolved)}</div></section>"
                    "</div></main>"
                ),
            )
        detail = get_project_git_detail(Path(resolved))
        name = _escape(str(detail.get("name", Path(resolved).name)))

        if not detail.get("available"):
            return self._page_shell(
                active_nav="Projects",
                body=(
                    "<main class='single-column'><div class='column'>"
                    "<a class='back-link' href='/projects'>&larr; All Projects</a>"
                    f"<section class='panel'><h2>{name}</h2>"
                    f"<div class='git-unavailable'>{_escape(str(detail.get('error', 'unavailable')))}</div>"
                    "</section></div></main>"
                ),
            )

        path_hidden = f"<input type='hidden' name='path' value='{_escape(resolved)}'>"

        # -- Feedback banner --
        message_html = ""
        if action_message:
            is_error = action_message.lower().startswith("error:") or action_message.lower().startswith("failed")
            cls = "action-error" if is_error else "action-ok"
            message_html = f"<div class='action-message {cls}'>{_escape(action_message)}</div>"

        # Branch + status badges
        branch = detail.get("branch")
        dirty = detail.get("dirty")
        changed = detail.get("changed_count", 0)
        ahead_behind = detail.get("ahead_behind")

        status_badges: list[str] = []
        if branch:
            status_badges.append(f"<span class='git-badge git-branch'>{_escape(str(branch))}</span>")
        if dirty:
            label = f"{changed} changed file{'s' if changed != 1 else ''}" if changed else "dirty"
            status_badges.append(f"<span class='git-badge git-dirty'>{_escape(label)}</span>")
        elif dirty is not None:
            status_badges.append("<span class='git-badge git-clean'>clean</span>")
        if isinstance(ahead_behind, (list, tuple)) and len(ahead_behind) == 2:
            ahead, behind = ahead_behind
            if ahead and behind:
                status_badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead}</span>")
                status_badges.append(f"<span class='git-badge git-behind'>&darr;{behind}</span>")
            elif ahead:
                status_badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead} ahead</span>")
            elif behind:
                status_badges.append(f"<span class='git-badge git-behind'>&darr;{behind} behind</span>")
            else:
                status_badges.append("<span class='git-badge git-synced'>in sync</span>")

        launch_session_url = _page_link("/sessions", project=resolved)
        header_html = (
            "<a class='back-link' href='/projects'>&larr; All Projects</a>"
            f"{message_html}"
            "<section class='panel'>"
            "<div class='detail-header'>"
            f"<h2 style='margin:0'>{name}</h2>"
            f"<div style='display:flex;gap:8px;align-items:center'>"
            f"<div class='project-git'>{''.join(status_badges)}</div>"
            f"<a href='{_escape(launch_session_url)}' class='btn-primary' "
            f"style='text-decoration:none;font-size:0.85em;padding:6px 12px'>Launch Agent</a>"
            f"</div>"
            "</div>"
            f"<div class='project-path' style='margin-top:4px'><code>{_escape(resolved)}</code></div>"
            "</section>"
        )

        # -- Remotes with pull/push buttons --
        remotes = detail.get("remotes") or []
        unique_remote_names: set[str] = set()
        if remotes:
            remote_names: set[str] = set()
            remote_rows = ""
            for r in remotes:
                url = str(r.get("url", ""))
                rname = str(r.get("name", ""))
                if rname:
                    remote_names.add(rname)
                remote_rows += (
                    "<div class='remote-row'>"
                    f"<span class='remote-name'>{_escape(rname)}</span>"
                    f"<span class='remote-url'>{_escape(url)}</span>"
                    f"<span class='remote-direction'>{_escape(str(r.get('direction', '')))}</span>"
                    "</div>"
                )
            unique_remote_names = remote_names
            connectivity_html = (
                "<div id='remote_status' style='margin-top:8px'>"
                "<span class='ssh-status ssh-pending'>checking remote connectivity&hellip;</span>"
                "</div>"
            ) if remote_names else ""

            pull_push_forms = ""
            for rname in sorted(remote_names):
                pull_push_forms += (
                    f"<div class='git-action-row' style='display:flex;gap:6px;align-items:center;margin-top:8px'>"
                    f"<span style='font-size:0.85em;min-width:60px'>{_escape(rname)}:</span>"
                    f"<form method='post' action='/actions/project-git-pull' style='margin:0'>"
                    f"{path_hidden}<input type='hidden' name='remote' value='{_escape(rname)}'>"
                    f"<button type='submit' class='btn-sm'>Pull</button></form>"
                    f"<form method='post' action='/actions/project-git-push' style='margin:0'>"
                    f"{path_hidden}<input type='hidden' name='remote' value='{_escape(rname)}'>"
                    f"<button type='submit' class='btn-sm'>Push</button></form>"
                    f"</div>"
                )

            remotes_html = (
                "<section class='panel detail-section'>"
                "<h3>Remotes</h3>"
                f"{remote_rows}"
                f"{connectivity_html}"
                f"{pull_push_forms}"
                "</section>"
            )
        else:
            remotes_html = (
                "<section class='panel detail-section'>"
                "<h3>Remotes</h3>"
                "<div class='empty'>No remotes configured.</div>"
                "</section>"
            )

        # -- Working tree status with discard + commit form --
        status_output = str(detail.get("status_output", ""))
        diff_stat = str(detail.get("diff_stat", ""))
        if status_output:
            discard_form = (
                "<form method='post' action='/actions/project-git-discard' style='margin:0;display:inline' "
                f"onsubmit=\"return confirm('This will discard ALL uncommitted changes. Continue?')\">"
                f"{path_hidden}"
                "<button type='submit' class='btn-sm btn-danger'>Discard All Changes</button>"
                "</form>"
            )
            commit_form = (
                "<div style='margin-top:12px'>"
                "<details id='diff_preview'>"
                "<summary style='cursor:pointer;font-size:0.85em;color:var(--muted)'>Show full diff</summary>"
                "<pre class='code-block' id='diff_content' style='max-height:400px;overflow:auto'>Loading&hellip;</pre>"
                "</details>"
                f"<form method='post' action='/actions/project-git-commit' style='margin-top:8px;display:flex;gap:6px;align-items:start'>"
                f"{path_hidden}"
                "<input type='text' name='message' placeholder='Commit message' required "
                "style='flex:1;padding:6px 10px;border:1px solid var(--border);border-radius:6px;"
                "background:var(--surface);color:var(--text);font-size:0.9em'>"
                "<button type='submit' class='btn-primary' style='font-size:0.85em;padding:6px 14px;white-space:nowrap'>Commit All</button>"
                "</form></div>"
            )
            status_html = (
                "<section class='panel detail-section'>"
                "<div style='display:flex;align-items:center;justify-content:space-between'>"
                "<h3 style='margin:0'>Working Tree Status</h3>"
                f"{discard_form}"
                "</div>"
                f"<pre class='code-block'>{_escape(status_output)}</pre>"
                + (f"<pre class='code-block'>{_escape(diff_stat)}</pre>" if diff_stat else "")
                + commit_form
                + "</section>"
            )
        else:
            status_html = (
                "<section class='panel detail-section'>"
                "<h3>Working Tree Status</h3>"
                "<div class='empty'>Clean working tree.</div>"
                "</section>"
            )

        # -- Branches with switch/create --
        branches = detail.get("branches") or []
        if branches:
            branch_items = ""
            for b in branches:
                is_current = str(b.get("current", "false")) == "true"
                bname = _escape(str(b.get("name", "")))
                if is_current:
                    branch_items += f"<li class='branch-current'>* {bname}</li>"
                else:
                    switch_form = (
                        f"<form method='post' action='/actions/project-git-switch' style='margin:0;display:inline'>"
                        f"{path_hidden}<input type='hidden' name='branch' value='{bname}'>"
                        f"<button type='submit' class='btn-sm' style='margin-left:6px'>Switch</button></form>"
                    )
                    branch_items += f"<li>{bname} {switch_form}</li>"
            create_branch_form = (
                "<div style='margin-top:10px;display:flex;gap:6px'>"
                f"<form method='post' action='/actions/project-git-create-branch' style='margin:0;display:flex;gap:6px;flex:1'>"
                f"{path_hidden}"
                "<input type='text' name='branch' placeholder='new-branch-name' required "
                "style='flex:1;padding:5px 8px;border:1px solid var(--border);border-radius:6px;"
                "background:var(--surface);color:var(--text);font-size:0.85em'>"
                "<button type='submit' class='btn-sm'>Create &amp; Switch</button>"
                "</form></div>"
            )
            branches_html = (
                "<section class='panel detail-section'>"
                f"<h3>Local Branches ({len(branches)})</h3>"
                f"<ul class='branch-list'>{branch_items}</ul>"
                f"{create_branch_form}"
                "</section>"
            )
        else:
            branches_html = (
                "<section class='panel detail-section'>"
                "<h3>Local Branches</h3>"
                "<div class='empty'>No branches found.</div>"
                "</section>"
            )

        # -- Stash with save/pop --
        stash_list = detail.get("stash_list") or []
        stash_save_form = (
            "<div style='margin-top:8px;display:flex;gap:6px'>"
            f"<form method='post' action='/actions/project-git-stash' style='margin:0;display:flex;gap:6px;flex:1'>"
            f"{path_hidden}"
            "<input type='text' name='stash_message' placeholder='Stash message (optional)' "
            "style='flex:1;padding:5px 8px;border:1px solid var(--border);border-radius:6px;"
            "background:var(--surface);color:var(--text);font-size:0.85em'>"
            "<button type='submit' class='btn-sm'>Stash</button>"
            "</form></div>"
        )
        if stash_list:
            pop_form = (
                f"<form method='post' action='/actions/project-git-stash-pop' style='margin:0;display:inline'>"
                f"{path_hidden}"
                f"<button type='submit' class='btn-sm' style='margin-left:6px'>Pop</button></form>"
            )
            stash_items = f"<li>{_escape(stash_list[0])} {pop_form}</li>"
            stash_items += "".join(f"<li>{_escape(s)}</li>" for s in stash_list[1:])
            stash_html = (
                "<section class='panel detail-section'>"
                f"<h3>Stash ({len(stash_list)})</h3>"
                f"<ul class='branch-list'>{stash_items}</ul>"
                f"{stash_save_form}"
                "</section>"
            )
        else:
            stash_html = (
                "<section class='panel detail-section'>"
                "<h3>Stash</h3>"
                "<div class='empty' style='margin-bottom:6px'>No stashed changes.</div>"
                f"{stash_save_form}"
                "</section>"
            )

        # Recent commits
        commits = detail.get("recent_commits") or []
        if commits:
            commit_rows = ""
            for c in commits:
                commit_rows += (
                    "<tr>"
                    f"<td class='commit-sha'>{_escape(str(c.get('sha', '')))}</td>"
                    f"<td>{_escape(str(c.get('subject', '')))}</td>"
                    f"<td>{_escape(str(c.get('author', '')))}</td>"
                    f"<td style='white-space:nowrap'>{_escape(str(c.get('date', '')))}</td>"
                    "</tr>"
                )
            commits_html = (
                "<section class='panel'>"
                f"<h3>Recent Commits ({len(commits)})</h3>"
                "<div class='table-scroll'>"
                "<table class='commit-table'>"
                "<thead><tr><th>sha</th><th>message</th><th>author</th><th>when</th></tr></thead>"
                f"<tbody>{commit_rows}</tbody>"
                "</table></div></section>"
            )
        else:
            commits_html = ""

        remote_names_for_detail: set[str] = set()
        for r in remotes:
            rname = str(r.get("name", ""))
            if rname:
                remote_names_for_detail.add(rname)
        remote_names_json = json.dumps(sorted(remote_names_for_detail))
        project_path_json = json.dumps(resolved)

        detail_script = f"""\
window.addEventListener('DOMContentLoaded', async () => {{
  const remoteNames = {remote_names_json};
  const projectPath = {project_path_json};
  const container = document.getElementById('remote_status');
  if (container && remoteNames.length > 0) {{
    const results = [];
    for (const name of remoteNames) {{
      try {{
        const params = new URLSearchParams({{ path: projectPath, remote: name }});
        const resp = await fetch('/api/project-remote-check?' + params.toString());
        const data = await resp.json();
        results.push(data);
      }} catch (e) {{
        results.push({{ remote: name, ok: false, message: 'fetch error' }});
      }}
    }}
    let html = '';
    for (const r of results) {{
      const cls = r.ok ? 'ssh-ok' : 'ssh-fail';
      const label = r.ok ? 'connected' : 'unreachable';
      const proto = r.protocol ? ' (' + esc(r.protocol) + ')' : '';
      const remote = r.remote ? esc(r.remote) : '';
      const msg = (!r.ok && r.message && r.message !== 'connected') ? ' &mdash; ' + esc(r.message) : '';
      html += `<div class="ssh-status ${{cls}}">${{remote}}${{proto}}: ${{label}}${{msg}}</div>`;
      if (r.hint) {{
        html += `<div class="help" style="margin-top:4px;font-size:0.85em">${{esc(r.hint)}}</div>`;
      }}
    }}
    container.innerHTML = html;
  }}
  const diffToggle = document.getElementById('diff_preview');
  if (diffToggle) {{
    let loaded = false;
    diffToggle.addEventListener('toggle', async () => {{
      if (diffToggle.open && !loaded) {{
        loaded = true;
        try {{
          const params = new URLSearchParams({{ path: projectPath }});
          const resp = await fetch('/api/project-git-diff?' + params.toString());
          const data = await resp.json();
          document.getElementById('diff_content').textContent = data.diff || '(no diff)';
        }} catch (e) {{
          document.getElementById('diff_content').textContent = 'Failed to load diff.';
        }}
      }}
    }});
  }}
}});
function esc(s) {{
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}}
"""

        body = (
            "<main class='single-column'><div class='column'>"
            f"{header_html}"
            "<div class='detail-grid'>"
            f"{remotes_html}"
            f"{branches_html}"
            f"{stash_html}"
            "</div>"
            f"{status_html}"
            f"{commits_html}"
            "</div></main>"
        )
        return self._page_shell(active_nav="Projects", body=body, extra_script=detail_script)

    def _render_project_card(self, project_path: str, summary: dict[str, object]) -> str:
        name = Path(project_path).name
        detail_url = "/projects/detail?" + urlencode({"path": project_path})
        header = (
            "<div class='project-header'>"
            f"<a class='project-name' href='{_escape(detail_url)}' style='color:inherit;text-decoration:none'>{_escape(name)}</a>"
            f"<form method='post' action='/actions/remove-project'>"
            f"<input type='hidden' name='project_path' value='{_escape(project_path)}'>"
            f"<button type='submit'>Remove</button>"
            f"</form>"
            "</div>"
            f"<div class='project-path'><code>{_escape(project_path)}</code></div>"
        )
        if not summary.get("available"):
            error = summary.get("error", "unavailable")
            git_html = f"<div class='git-unavailable'>{_escape(str(error))}</div>"
            return f"<div class='project-item' data-project='{_escape(project_path)}'>{header}<div class='project-git'>{git_html}</div></div>"

        badges: list[str] = []
        branch = summary.get("branch")
        if branch:
            badges.append(f"<span class='git-badge git-branch'>{_escape(str(branch))}</span>")

        dirty = summary.get("dirty")
        changed = summary.get("changed_count", 0)
        if dirty:
            label = f"{changed} changed file{'s' if changed != 1 else ''}" if changed else "dirty"
            badges.append(f"<span class='git-badge git-dirty'>{_escape(label)}</span>")
        elif dirty is not None:
            badges.append("<span class='git-badge git-clean'>clean</span>")

        ahead_behind = summary.get("ahead_behind")
        if isinstance(ahead_behind, (list, tuple)) and len(ahead_behind) == 2:
            ahead, behind = ahead_behind
            if ahead and behind:
                badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead}</span>")
                badges.append(f"<span class='git-badge git-behind'>&darr;{behind}</span>")
            elif ahead:
                badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead} ahead</span>")
            elif behind:
                badges.append(f"<span class='git-badge git-behind'>&darr;{behind} behind</span>")
            else:
                badges.append("<span class='git-badge git-synced'>in sync</span>")

        last_commit = summary.get("last_commit")
        if last_commit:
            badges.append(f"<span class='git-commit'>{_escape(str(last_commit))}</span>")

        git_html = "".join(badges)
        return f"<div class='project-item' data-project='{_escape(project_path)}'>{header}<div class='project-git'>{git_html}</div></div>"

    def render_projects_page(self, *, action_message: str | None = None) -> str:
        tracked_projects = self.load_tracked_projects()
        tracked_json = json.dumps(tracked_projects)
        notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
        summaries = {p: get_project_git_summary(Path(p)) for p in tracked_projects}
        project_items_html = "".join(
            self._render_project_card(project_path, summaries[project_path])
            for project_path in tracked_projects
        ) or "<div class='empty'>No tracked projects yet.</div>"
        body = (
            f"<main class='single-column'>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<details class='panel actions-details'>"
            f"<summary><h2 class='inline-heading'>Add Project</h2></summary>"
            f"<div class='help' style='margin-top:8px'>Add a local git repository path to the tracked projects list.</div>"
            f"<form method='post' action='/actions/add-project' id='add_project_form'>"
            f"<label for='project_path'><strong>Project Path</strong></label>"
            f"<input id='project_path' name='project_path' type='text' placeholder='/path/to/project' required>"
            f"<div id='project_path_status' class='repo-check'></div>"
            f"<div id='project_path_duplicate' class='repo-check'></div>"
            f"<button type='submit' id='add_project_button'>Add Project</button>"
            f"</form>"
            f"</details>"
            f"<section class='panel'>"
            f"<div style='display:flex;align-items:center;justify-content:space-between'>"
            f"<h2 style='margin:0'>Tracked Projects</h2>"
            f"<button id='refresh_git_btn' type='button' style='font-size:0.85em'>Refresh</button>"
            f"</div>"
            f"<div class='help'>Local repo paths used for cross-project plan runs.</div>"
            f"<div id='projects_list'>{project_items_html}</div>"
            f"</section>"
            f"</div>"
            f"</main>"
        )
        projects_script = f"""\
window.addEventListener('DOMContentLoaded', () => {{
  wireRepoCheck('project_path', 'project_path_status');
  const trackedProjects = {tracked_json};
  const input = document.getElementById('project_path');
  const dupStatus = document.getElementById('project_path_duplicate');
  const addButton = document.getElementById('add_project_button');
  if (input && dupStatus && addButton) {{
    let timer = null;
    async function checkDuplicate() {{
      const value = input.value.trim();
      if (!value) {{
        dupStatus.textContent = '';
        dupStatus.dataset.status = '';
        addButton.disabled = false;
        return;
      }}
      const params = new URLSearchParams({{ path: value }});
      const response = await fetch('/api/resolve-path?' + params.toString());
      const data = await response.json();
      const resolved = data.resolved || '';
      if (resolved && trackedProjects.includes(resolved)) {{
        dupStatus.dataset.status = 'empty';
        dupStatus.textContent = 'Already tracked: ' + resolved;
        addButton.disabled = true;
      }} else {{
        dupStatus.textContent = '';
        dupStatus.dataset.status = '';
        addButton.disabled = false;
      }}
    }}
    function scheduleCheck() {{
      clearTimeout(timer);
      timer = setTimeout(checkDuplicate, 150);
    }}
    input.addEventListener('input', scheduleCheck);
    checkDuplicate();
  }}

  function renderBadges(d) {{
    let h = '';
    if (d.branch) h += `<span class="git-badge git-branch">${{esc(d.branch)}}</span>`;
    if (d.dirty) {{
      const n = d.changed_count || 0;
      const label = n ? n + ' changed file' + (n !== 1 ? 's' : '') : 'dirty';
      h += `<span class="git-badge git-dirty">${{esc(label)}}</span>`;
    }} else if (d.dirty === false) {{
      h += '<span class="git-badge git-clean">clean</span>';
    }}
    const ab = d.ahead_behind;
    if (Array.isArray(ab) && ab.length === 2) {{
      const [ahead, behind] = ab;
      if (ahead && behind) {{
        h += `<span class="git-badge git-ahead">&uarr;${{ahead}}</span>`;
        h += `<span class="git-badge git-behind">&darr;${{behind}}</span>`;
      }} else if (ahead) {{
        h += `<span class="git-badge git-ahead">&uarr;${{ahead}} ahead</span>`;
      }} else if (behind) {{
        h += `<span class="git-badge git-behind">&darr;${{behind}} behind</span>`;
      }} else {{
        h += '<span class="git-badge git-synced">in sync</span>';
      }}
    }}
    if (d.last_commit) h += `<span class="git-commit">${{esc(d.last_commit)}}</span>`;
    return h;
  }}
  function esc(s) {{
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }}

  const refreshBtn = document.getElementById('refresh_git_btn');
  if (refreshBtn) {{
    refreshBtn.addEventListener('click', async () => {{
      refreshBtn.disabled = true;
      refreshBtn.textContent = 'Refreshing\u2026';
      for (const project of trackedProjects) {{
        const card = document.querySelector(`[data-project="${{CSS.escape(project)}}"]`);
        if (!card) continue;
        const gitDiv = card.querySelector('.project-git');
        if (!gitDiv) continue;
        try {{
          const params = new URLSearchParams({{ path: project }});
          const resp = await fetch('/api/project-git-status?' + params.toString());
          const data = await resp.json();
          if (!data.available) {{
            gitDiv.innerHTML = `<span class="git-unavailable">${{esc(data.error || 'unavailable')}}</span>`;
          }} else {{
            gitDiv.innerHTML = renderBadges(data);
          }}
        }} catch (e) {{
          gitDiv.innerHTML = '<span class="git-unavailable">fetch error</span>';
        }}
      }}
      refreshBtn.disabled = false;
      refreshBtn.textContent = 'Refresh';
    }});
  }}
}});
"""
        return self._page_shell(active_nav="Projects", body=body, extra_script=projects_script)

    def render_page(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> str:
        if run_id is not None or plan_execution_id is not None or selected_plan_file is not None:
            return self.render_dashboard_detail_page(
                run_id=run_id,
                plan_execution_id=plan_execution_id,
                action_message=action_message,
                selected_plan_file=selected_plan_file,
            )

        overview = get_repository_overview(self.repo_path, db_path=self.db_path)
        attention_items = list_attention_items(self.repo_path, db_path=self.db_path)
        runs = list_runs(self.repo_path, db_path=self.db_path)
        providers = available_providers()

        overview_html = (
            "<div class='overview-bar'>"
            f"<span><strong>{_escape(overview.run_count)}</strong> runs</span>"
            f"<span><strong>{_escape(overview.active_run_count)}</strong> active</span>"
            f"<span class='{_status_class('failure') if overview.failed_run_count else ''}'><strong>{_escape(overview.failed_run_count)}</strong> failed</span>"
            f"<span><strong>{_escape(overview.running_plan_count)}</strong> running</span>"
            f"<span><strong>{_escape(overview.blocked_plan_count)}</strong> blocked</span>"
            f"<span><strong>{_escape(overview.stale_workspace_count)}</strong> stale</span>"
            "</div>"
        )

        attention_items_html = "".join(
            _render_attention_card(item, providers=providers) for item in attention_items
        ) or "<div class='empty'>Nothing needs attention right now.</div>"

        run_items_html = "".join(
            (
                f"<a class='list-item {_status_class(run.status)}' href='{_escape(_page_link('/runs/detail', id=run.id))}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
                f"<strong>{_escape(run.id)}</strong>"
                f"{_status_badge(run.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(run.started_at))}</div>"
                f"<div class='kv-row'><span class='kv-label'>Concurrency</span> {_escape(run.concurrency)}</div>"
                "</a>"
            )
            for run in runs
        ) or "<div class='empty'>No runs yet. <a href='/actions'>Go to Actions</a> to launch your first plan.</div>"

        notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
        body = (
            f"{overview_html}"
            f"<main class='single-column'>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'><h2>Attention</h2>{attention_items_html}</section>"
            f"<section class='panel'><h2>Recent Runs</h2>{run_items_html}</section>"
            f"</div>"
            f"</main>"
        )
        return self._page_shell(active_nav="Dashboard", body=body)

    def render_dashboard_detail_page(
        self,
        *,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> str:
        state = self.load_state(
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )
        base_params: dict[str, str] = {}
        if state.selected_run is not None:
            base_params["run_id"] = state.selected_run.id
        if state.selected_plan is not None:
            base_params["plan_execution_id"] = state.selected_plan.id

        overview_html = (
            "<div class='overview-bar'>"
            f"<span><strong>{_escape(state.overview.run_count)}</strong> runs</span>"
            f"<span><strong>{_escape(state.overview.active_run_count)}</strong> active</span>"
            f"<span class='{_status_class('failure') if state.overview.failed_run_count else ''}'><strong>{_escape(state.overview.failed_run_count)}</strong> failed</span>"
            f"<span><strong>{_escape(state.overview.running_plan_count)}</strong> running</span>"
            f"<span><strong>{_escape(state.overview.blocked_plan_count)}</strong> blocked</span>"
            f"<span><strong>{_escape(state.overview.stale_workspace_count)}</strong> stale</span>"
            "</div>"
        )
        notice_html = f"<div class='notice'>{_escape(state.action_message)}</div>" if state.action_message else ""
        attention_items_html = "".join(
            _render_attention_card(item, providers=state.available_providers)
            for item in state.attention_items
        ) or "<div class='empty'>Nothing needs attention right now.</div>"
        workspace_items_html = "".join(
            (
                f"<a class='card {_status_class(workspace.status)}' href='{_escape(_link({}, run_id=workspace.run_id, plan_execution_id=workspace.plan_execution_id))}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
                f"<strong>{_escape(workspace.plan_slug)}</strong>"
                f"{_lifecycle_badge(workspace.lifecycle)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Status</span> {_escape(workspace.status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(workspace.verify_status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
                "</a>"
            )
            for workspace in state.workspaces
        ) or "<div class='empty'>No workspaces indexed.</div>"
        plan_file_items_html = "".join(
            (
                f"<a class='list-item status-neutral' href='{_escape(_link(base_params, selected_plan_file=plan_path))}'>"
                f"<strong>{_escape(plan_name)}</strong>"
                f"<div class='help'>{_escape(plan_path)}</div>"
                "</a>"
            )
            for plan_name, plan_path in (
                (path.name, str(path.resolve()))
                for pattern in ("*.yaml", "*.yml")
                for path in sorted(self.default_plans_dir.glob(pattern))
                if path.is_file()
            )
        ) or "<div class='empty'>No plan files found.</div>"
        run_items_html = "".join(
            (
                f"<a class='list-item {_status_class(run.status)}' href='{_escape(_link({}, run_id=run.id))}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
                f"<strong>{_escape(run.id)}</strong>"
                f"{_status_badge(run.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(run.started_at))}</div>"
                f"<div class='kv-row'><span class='kv-label'>Concurrency</span> {_escape(run.concurrency)}</div>"
                "</a>"
            )
            for run in state.runs
        ) or "<div class='empty'>No runs yet. <a href='/actions'>Go to Actions</a> to launch your first plan.</div>"

        selected_run_html = "<section class='panel'><h2>Run Detail</h2><div class='empty'>No run selected.</div></section>"
        if state.selected_run is not None:
            run_preflight_html = ""
            if state.selected_run_preflight is not None:
                run_preflight_items = state.selected_run_preflight.get("items", {})
                run_preflight_html = (
                    "<details class='panel actions-details' style='margin-top:12px'>"
                    "<summary><h3 class='inline-heading'>Launch Snapshot</h3></summary>"
                    f"<div class='repo-check' style='margin-top:8px' data-status='{_escape(state.selected_run_preflight.get('status'))}'>{_escape(state.selected_run_preflight.get('message'))}</div>"
                    f"<div class='help'>captured_at={_escape(state.selected_run_preflight.get('captured_at'))}</div>"
                    f"<div class='preflight-grid' style='margin-top:8px'>"
                    + "".join(
                        _render_preflight_item_html(label, item)
                        for label, item in (
                            ("Repo", run_preflight_items.get("repo") or {}),
                            ("Plans Dir", run_preflight_items.get("plans_dir") or {}),
                            ("Binaries", run_preflight_items.get("binaries") or {}),
                            ("Writable Paths", run_preflight_items.get("writable_paths") or {}),
                            ("Required Env", run_preflight_items.get("required_env") or {}),
                        )
                    )
                    + "</div></details>"
                )
            selected_run_html = (
                "<section class='panel'>"
                "<h2>Run Detail</h2>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
                f"<strong style='font-family:monospace;font-size:0.9em'>{_escape(state.selected_run.id)}</strong>"
                f"{_status_badge(state.selected_run.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Source</span> {_escape(state.selected_run.launch_source)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(state.selected_run.started_at))}</div>"
                f"<div class='kv-row'><span class='kv-label'>Ended</span> {_escape(_fmt_ts(state.selected_run.ended_at))}</div>"
                f"<div class='kv-row'><span class='kv-label'>Plans</span> {_escape(state.selected_run.plan_execution_count)} total</div>"
                f"{run_preflight_html}"
                "</section>"
            )

        plan_items_html = "".join(
            (
                f"<a class='card {_status_class(plan.status)}' href='{_escape(_link({}, run_id=plan.run_id, plan_execution_id=plan.id))}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
                f"<strong>{_escape(plan.plan_slug)}</strong>"
                f"{_status_badge(plan.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(plan.current_step_key or '\u2014')}</div>"
                f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(plan.verify_status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(plan.changed_files_count)} files</div>"
                "</a>"
            )
            for plan in state.plan_cards
        ) or "<div class='empty'>No plan executions.</div>"

        workspace_detail_html = "<div class='empty'>No workspace recorded.</div>"
        if state.workspace is not None:
            workspace_detail_html = (
                f"<div class='kv-row'><span class='kv-label'>Status</span> {_escape(state.workspace.status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(state.workspace.branch_name or '\u2014')}</code></div>"
                f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(state.workspace.created_at))}</div>"
                + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(state.workspace.released_at))}</div>" if state.workspace.released_at else "")
                + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(state.workspace.path)}</code></div>"
            )

        timeline_rows = "".join(
            (
                "<tr>"
                f"<td>{_escape(step.sequence_index)}</td>"
                f"<td>{_escape(step.step_key)}</td>"
                f"<td>{_escape(step.kind)}</td>"
                f"<td>{_status_badge(step.status)}</td>"
                f"<td>{_escape(step.verify_status)}</td>"
                f"<td>{_escape(step.changed_files_count)}</td>"
                f"<td>{_escape(step.duration_ms)}</td>"
                "</tr>"
            )
            for step in state.steps
        ) or "<tr><td colspan='7' class='empty'>No steps recorded.</td></tr>"

        selected_plan_html = "<section class='panel'><h2>Plan Execution Detail</h2><div class='empty'>No plan selected.</div></section>"
        if state.selected_plan is not None:
            selected_plan_html = (
                "<section class='panel'>"
                "<h2>Plan Execution Detail</h2>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
                f"<strong>{_escape(state.selected_plan.plan_slug)}</strong>"
                f"{_status_badge(state.selected_plan.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(state.selected_plan.current_step_key or '\u2014')}</div>"
                f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(state.selected_plan.verify_status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(state.selected_plan.changed_files_count)} files</div>"
                + (f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(state.selected_plan.branch_name or '\u2014')}</code></div>" if state.selected_plan.branch_name else "")
                + (f"<div class='kv-row'><span class='kv-label'>Log</span> <code style='font-size:0.85em'>{_escape(state.selected_plan.log_path)}</code></div>" if state.selected_plan.log_path else "")
                + (f"<div class='kv-row'><span class='kv-label'>Failure</span> {_escape(_failure_reason_label(state.selected_plan.failure_reason))}</div>" if state.selected_plan.failure_reason else "")
                + "</section>"
            )

        live_output_html = "<div class='empty'>No live output captured for this run yet.</div>"
        if state.live_output_path is not None:
            live_output_html = (
                f"<pre id='live_output_stream' class='code-block live-output'>{_escape(state.live_output or '')}</pre>"
            )
        selected_plan_file_html = "<div class='empty'>No plan file selected.</div>"
        if state.selected_plan_file_name is not None and state.selected_plan_file_contents is not None:
            selected_plan_file_html = (
                f"<div><strong>{_escape(state.selected_plan_file_name)}</strong></div>"
                f"<div>path={_escape(state.selected_plan_file_path)}</div>"
                f"<pre class='code-block'>{_escape(state.selected_plan_file_contents)}</pre>"
            )

        body = (
            f"{overview_html}"
            "<main>"
            "<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'><h2>Attention</h2>{attention_items_html}</section>"
            f"<section class='panel'><h2>Workspaces</h2>{workspace_items_html}</section>"
            f"<section class='panel'><h2>Plans</h2>{plan_file_items_html}</section>"
            f"<section class='panel'><h2>Runs</h2>{run_items_html}</section>"
            "</div>"
            "<div class='column'>"
            f"{selected_run_html}"
            f"<section class='panel'><h2>Plan Executions</h2>{plan_items_html}</section>"
            f"{selected_plan_html}"
            f"<section class='panel'><h2>Live Output</h2>{live_output_html}</section>"
            f"<section class='panel'><h2>Step Timeline</h2><div class='table-scroll'><table><thead><tr><th>#</th><th>Step</th><th>Type</th><th>Status</th><th>Verify</th><th>Files</th><th>ms</th></tr></thead><tbody>{timeline_rows}</tbody></table></div></section>"
            f"<section class='panel'><h2>Workspace</h2>{workspace_detail_html}</section>"
            f"<section class='panel'><h2>Plan File</h2>{selected_plan_file_html}</section>"
            "</div>"
            "</main>"
        )

        dashboard_script = ""
        if state.selected_run is not None:
            dashboard_script = (
                "window.addEventListener('DOMContentLoaded', () => {\n"
                "  wireCopyButtons(document);\n"
                f"  const runId = {json.dumps(state.selected_run.id)};\n"
                f"  const runStatus = {json.dumps(state.live_output_status or state.selected_run.status)};\n"
                "  const outputNode = document.getElementById('live_output_stream');\n"
                "  if (runId && outputNode) {\n"
                "    const refreshOutput = async () => {\n"
                "      const params = new URLSearchParams({ run_id: runId });\n"
                "      const response = await fetch(`/api/run-output?${params.toString()}`);\n"
                "      if (!response.ok) return;\n"
                "      const data = await response.json();\n"
                "      outputNode.textContent = data.output || '';\n"
                "    };\n"
                "    refreshOutput();\n"
                "    if (runStatus === 'running') {\n"
                "      window.setInterval(refreshOutput, 2000);\n"
                "    }\n"
                "  }\n"
                "});\n"
            )

        return self._page_shell(active_nav="Dashboard", body=body, extra_script=dashboard_script)

    def render_run_page(
        self,
        run_id: str,
        plan_execution_id: str | None = None,
    ) -> str:
        live_run_data = self.load_live_run_data(run_id)
        try:
            selected_run = get_run(self.repo_path, run_id, db_path=self.db_path)
            plan_cards = list_plan_executions(self.repo_path, run_id, db_path=self.db_path)
        except PlanError:
            if live_run_data is not None:
                selected_run = self._build_run_detail_from_live_data(live_run_data)
                plan_cards = self._build_plan_cards_from_live_data(live_run_data)
            else:
                raise
        if live_run_data is not None and str(live_run_data.get("status") or "") == "running":
            selected_run = self._build_run_detail_from_live_data(live_run_data)
            plan_cards = self._build_plan_cards_from_live_data(live_run_data)

        selected_run_preflight = _preflight_run_snapshot(
            live_run_data or self.load_saved_run_data(selected_run.run_root_path)
        )

        if plan_execution_id is None and plan_cards:
            plan_execution_id = plan_cards[0].id

        selected_plan: PlanExecutionCard | None = None
        steps: list[StepTimelineItem] = []
        workspace: WorkspaceDetail | None = None
        if plan_execution_id is not None:
            try:
                selected_plan = get_plan_execution(plan_execution_id, self.repo_path, db_path=self.db_path)
                steps = list_step_executions(plan_execution_id, self.repo_path, db_path=self.db_path)
                workspace = get_workspace(plan_execution_id, self.repo_path, db_path=self.db_path)
            except PlanError:
                if live_run_data is not None:
                    selected_plan = next((p for p in plan_cards if p.id == plan_execution_id), None)

        live_output, live_output_path, live_output_status = self.read_live_output(live_run_data)

        # -- Run header --
        run_preflight_html = ""
        if selected_run_preflight is not None:
            run_preflight_items = selected_run_preflight.get("items", {})
            run_preflight_html = (
                "<details class='panel actions-details' style='margin-top:12px'>"
                "<summary><h3 class='inline-heading'>Launch Snapshot</h3></summary>"
                f"<div class='repo-check' style='margin-top:8px' data-status='{_escape(selected_run_preflight.get('status'))}'>{_escape(selected_run_preflight.get('message'))}</div>"
                f"<div class='help'>captured_at={_escape(selected_run_preflight.get('captured_at'))}</div>"
                f"<div class='preflight-grid' style='margin-top:8px'>"
                + "".join(
                    _render_preflight_item_html(label, item)
                    for label, item in (
                        ("Repo", run_preflight_items.get("repo") or {}),
                        ("Plans Dir", run_preflight_items.get("plans_dir") or {}),
                        ("Binaries", run_preflight_items.get("binaries") or {}),
                        ("Writable Paths", run_preflight_items.get("writable_paths") or {}),
                        ("Required Env", run_preflight_items.get("required_env") or {}),
                    )
                )
                + "</div></details>"
            )

        run_header_html = (
            "<section class='panel'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
            f"<strong style='font-family:monospace;font-size:0.9em'>{_escape(selected_run.id)}</strong>"
            f"{_status_badge(selected_run.status)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Source</span> {_escape(selected_run.launch_source)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(selected_run.started_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Ended</span> {_escape(_fmt_ts(selected_run.ended_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Plans</span>"
            f" {_escape(selected_run.plan_execution_count)} total"
            f" &mdash; {_escape(selected_run.passed_count)} passed,"
            f" {_escape(selected_run.failed_count)} failed,"
            f" {_escape(selected_run.running_count)} running,"
            f" {_escape(selected_run.blocked_count)} blocked</div>"
            f"{run_preflight_html}"
            "</section>"
        )

        # -- Plan executions list --
        plan_items_html = "".join(
            (
                f"<a class='card {_status_class(plan.status)}' href='{_escape(_page_link('/runs/detail', id=run_id, plan_execution_id=plan.id))}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
                f"<strong>{_escape(plan.plan_slug)}</strong>"
                f"{_status_badge(plan.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(plan.current_step_key or '\u2014')}</div>"
                f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(plan.verify_status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(plan.changed_files_count)} files</div>"
                "</a>"
            )
            for plan in plan_cards
        ) or "<div class='empty'>No plan executions.</div>"

        # -- Selected plan detail --
        selected_plan_html = ""
        if selected_plan is not None:
            workspace_html = ""
            if workspace is not None:
                workspace_html = (
                    "<div style='margin-top:8px;padding-top:8px;border-top:1px solid #eee'>"
                    f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(workspace.branch_name or '\u2014')}</code></div>"
                    f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(workspace.created_at))}</div>"
                    + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(workspace.released_at))}</div>" if workspace.released_at else "")
                    + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
                    "</div>"
                )
            timeline_rows = "".join(
                (
                    "<tr>"
                    f"<td>{_escape(step.sequence_index)}</td>"
                    f"<td>{_escape(step.step_key)}</td>"
                    f"<td>{_escape(step.kind)}</td>"
                    f"<td>{_status_badge(step.status)}</td>"
                    f"<td>{_escape(step.verify_status)}</td>"
                    f"<td>{_escape(step.changed_files_count)}</td>"
                    f"<td>{_escape(step.duration_ms)}</td>"
                    "</tr>"
                )
                for step in steps
            ) or "<tr><td colspan='7' class='empty'>No steps recorded.</td></tr>"
            selected_plan_html = (
                "<section class='panel'>"
                "<h2>Plan Execution Detail</h2>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
                f"<strong>{_escape(selected_plan.plan_slug)}</strong>"
                f"{_status_badge(selected_plan.status)}"
                f"</div>"
                f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(selected_plan.current_step_key or '\u2014')}</div>"
                f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(selected_plan.verify_status)}</div>"
                f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(selected_plan.changed_files_count)} files</div>"
                + (f"<div class='kv-row'><span class='kv-label'>Log</span> <code style='font-size:0.85em'>{_escape(selected_plan.log_path)}</code></div>" if selected_plan.log_path else "")
                + (f"<div class='kv-row'><span class='kv-label'>Failure</span> {_escape(_failure_reason_label(selected_plan.failure_reason))}</div>" if selected_plan.failure_reason else "")
                + workspace_html
                + "<h3 style='margin:16px 0 8px'>Step Timeline</h3>"
                + "<div class='table-scroll'><table>"
                + "<thead><tr><th>#</th><th>Step</th><th>Type</th><th>Status</th><th>Verify</th><th>Files</th><th>ms</th></tr></thead>"
                + f"<tbody>{timeline_rows}</tbody>"
                + "</table></div>"
                + "</section>"
            )

        # -- Live output --
        live_output_html = "<div class='empty'>No live output captured for this run yet.</div>"
        if live_output_path is not None:
            live_output_html = (
                f"<pre id='live_output_stream' class='code-block live-output'>{_escape(live_output or '')}</pre>"
            )
        live_section_html = f"<section class='panel'><h2>Live Output</h2>{live_output_html}</section>"

        body = (
            "<main class='single-column'>"
            "<div class='column'>"
            f"<a class='back-link' href='/'>&larr; Dashboard</a>"
            f"{run_header_html}"
            f"<section class='panel'><h2>Plan Executions</h2>{plan_items_html}</section>"
            f"{selected_plan_html}"
            f"{live_section_html}"
            "</div>"
            "</main>"
        )
        run_id_json = json.dumps(run_id)
        run_status_json = json.dumps(live_output_status or selected_run.status)
        run_script = (
            "window.addEventListener('DOMContentLoaded', () => {\n"
            "  wireCopyButtons(document);\n"
            f"  const runId = {run_id_json};\n"
            f"  const runStatus = {run_status_json};\n"
            "  const outputNode = document.getElementById('live_output_stream');\n"
            "  if (runId && outputNode && runStatus === 'running') {\n"
            "    const refresh = async () => {\n"
            "      const params = new URLSearchParams({ run_id: runId });\n"
            "      const resp = await fetch(`/api/run-output?${params.toString()}`);\n"
            "      if (!resp.ok) return;\n"
            "      const data = await resp.json();\n"
            "      outputNode.textContent = data.output || '';\n"
            "    };\n"
            "    refresh();\n"
            "    window.setInterval(refresh, 2000);\n"
            "  }\n"
            "});\n"
        )
        return self._page_shell(active_nav="Dashboard", body=body, extra_script=run_script)

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

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/check-repo":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0]
                status, message = check_repo_path(path_value)
                body = json.dumps({"status": status, "message": message}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/resolve-path":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0].strip()
                resolved = str(Path(path_value).expanduser().resolve()) if path_value else ""
                body = json.dumps({"resolved": resolved}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/list-plans":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0]
                status, message, plans = list_plans_in_directory(path_value)
                body = json.dumps({"status": status, "message": message, "plans": plans}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/preflight":
                params = parse_qs(parsed.query)
                target_repo_value = params.get("target_repo", [""])[0]
                plans_dir_value = params.get("plans_dir", [""])[0]
                selected_plan_names = [name.strip() for name in params.get("selected_plans", []) if name.strip()]
                body = json.dumps(
                    summarize_preflight_for_dashboard(
                        target_repo_value,
                        plans_dir_value,
                        selected_plan_names=selected_plan_names or None,
                        provider_override=params.get("provider_override", [""])[0].strip() or None,
                    )
                ).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/run-output":
                params = parse_qs(parsed.query)
                run_id = params.get("run_id", [""])[0]
                run_data = app.load_live_run_data(run_id) if run_id else None
                output, output_path, status = app.read_live_output(run_data)
                body = json.dumps(
                    {
                        "run_id": run_id,
                        "status": status or "unknown",
                        "output_path": output_path,
                        "output": output or "",
                    }
                ).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/project-git-status":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0].strip()
                if path_value:
                    summary = get_project_git_summary(Path(path_value).expanduser().resolve())
                else:
                    summary = {"path": "", "available": False, "error": "no path provided"}
                body = json.dumps(summary).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/project-remote-check":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0].strip()
                remote_name = params.get("remote", ["origin"])[0].strip()
                if path_value:
                    repo = Path(path_value).expanduser().resolve()
                    result = check_remote_connectivity(repo, remote_name)
                else:
                    result = {"ok": False, "remote": remote_name, "message": "no path provided"}
                body = json.dumps(result).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/project-git-diff":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0].strip()
                if path_value:
                    repo = Path(path_value).expanduser().resolve()
                    diff_text = get_full_diff(repo)
                else:
                    diff_text = ""
                body = json.dumps({"diff": diff_text}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/session-output":
                params = parse_qs(parsed.query)
                session_id = params.get("id", [""])[0].strip()
                if session_id:
                    meta = app.get_session(session_id)
                    output = app.read_session_output(session_id)
                    status = str(meta.get("status") or "unknown") if meta else "unknown"
                    messages = list(meta.get("messages") or []) if meta else []
                    token_warning = str(meta.get("token_warning") or "") if meta else ""
                else:
                    output = ""
                    status = "unknown"
                    messages = []
                    token_warning = ""
                body = json.dumps({
                    "id": session_id,
                    "status": status,
                    "output": output,
                    "messages": messages,
                    "token_warning": token_warning,
                }).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path not in {
                "/",
                "/actions",
                "/projects",
                "/projects/detail",
                "/runs/detail",
                "/sessions",
                "/sessions/detail",
            }:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            params = parse_qs(parsed.query)
            try:
                body = app.render_route(parsed.path, params)
            except PlanError as exc:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    (
                        "<!doctype html><html><body><h1>kctl</h1>"
                        f"<p>{_escape(exc)}</p></body></html>"
                    ).encode("utf-8")
                )
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {
                "/actions/index",
                "/actions/run-many",
                "/actions/create-plan",
                "/actions/rerun-plan",
                "/actions/add-project",
                "/actions/remove-project",
                "/actions/run-plan-across-projects",
                "/actions/start-session",
                "/actions/stop-session",
                "/actions/session-reply",
                "/actions/project-git-commit",
                "/actions/project-git-switch",
                "/actions/project-git-create-branch",
                "/actions/project-git-pull",
                "/actions/project-git-push",
                "/actions/project-git-stash",
                "/actions/project-git-stash-pop",
                "/actions/project-git-discard",
            }:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            form_data = parse_qs(self.rfile.read(content_length).decode("utf-8"))
            try:
                action_result = app.handle_action(parsed.path, form_data)
                message = action_result.message
                redirect_to = action_result.redirect_to
                run_id = action_result.run_id
            except (PlanError, ValueError) as exc:
                message = str(exc)
                redirect_to = "/actions"
                run_id = None
            if redirect_to.startswith("/projects/detail?"):
                if "message=" not in redirect_to:
                    location = redirect_to + "&" + urlencode({"message": message})
                else:
                    location = redirect_to
            elif redirect_to.startswith("/sessions/detail"):
                location = redirect_to if "?" in redirect_to else redirect_to + f"?message={message}"
            elif redirect_to == "/sessions":
                location = _page_link("/sessions", message=message)
            elif redirect_to == "/projects":
                location = _page_link("/projects", message=message)
            elif redirect_to == "/actions":
                location = _page_link("/actions", message=message)
            else:
                location = _link({}, run_id=run_id, message=message)
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"kctl dashboard listening on {host}:{port}", flush=True)
    for url in build_dashboard_access_urls(host, port, announce_url=announce_url, tailscale=tailscale):
        print(f"dashboard url: {url}", flush=True)
    if tailscale and announce_url is None:
        print("tailscale note: hostname URL requires MagicDNS or equivalent tailnet DNS.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
