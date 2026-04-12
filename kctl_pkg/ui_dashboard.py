from __future__ import annotations

import html
import json
import socket
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

from .multi import build_multi_run_id, resolve_multi_run_log, run_many_plans
from .plan import build_plan_from_template, load_plan_templates
from .paths import project_root
from .ui_index import index_repository_state

from .types import PlanError
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


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _status_class(status: str | None) -> str:
    if status in {"passed", "success"}:
        return "status-success"
    if status in {"failed", "failure", "blocked"}:
        return "status-failure"
    if status in {"running"}:
        return "status-running"
    return "status-neutral"


def _link(base_params: dict[str, str], **updates: str | None) -> str:
    params = dict(base_params)
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    query = urlencode(params)
    return f"/?{query}" if query else "/"


def check_repo_path(path_value: str) -> tuple[str, str]:
    trimmed = path_value.strip()
    if not trimmed:
        return "empty", "Target repo path is empty."
    candidate = Path(trimmed).expanduser()
    if not candidate.exists():
        return "missing", f"Missing: {candidate}"
    if not candidate.is_dir():
        return "not_dir", f"Not a directory: {candidate}"
    return "ok", f"Exists: {candidate.resolve()}"


def list_plans_in_directory(path_value: str) -> tuple[str, str, list[str]]:
    trimmed = path_value.strip()
    if not trimmed:
        return "empty", "Plans directory is empty.", []
    candidate = Path(trimmed).expanduser()
    if not candidate.exists():
        return "missing", f"Missing: {candidate}", []
    if not candidate.is_dir():
        return "not_dir", f"Not a directory: {candidate}", []
    plan_paths = sorted(
        {path.name for pattern in ("*.yaml", "*.yml") for path in candidate.glob(pattern) if path.is_file()}
    )
    if not plan_paths:
        return "empty", f"No plan files found in {candidate.resolve()}", []
    return "ok", f"Found {len(plan_paths)} plan file(s) in {candidate.resolve()}", plan_paths


def read_plan_file(plan_path: Path) -> tuple[str, str]:
    if not plan_path.exists():
        raise PlanError(f"Plan file does not exist: {plan_path}")
    if not plan_path.is_file():
        raise PlanError(f"Plan path is not a file: {plan_path}")
    return plan_path.name, plan_path.read_text()


@dataclass(frozen=True)
class DashboardState:
    repo_name: str
    repo_root: str
    action_message: str | None
    plan_templates: list[tuple[str, str | None]]
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
    live_output: str | None
    live_output_status: str | None
    live_output_path: str | None
    steps: list[StepTimelineItem]
    workspace: WorkspaceDetail | None


def build_dashboard_access_urls(
    host: str,
    port: int,
    *,
    announce_url: str | None = None,
    tailscale: bool = False,
    hostname: str | None = None,
) -> list[str]:
    urls: list[str] = []
    if announce_url:
        urls.append(announce_url)
    if host in {"0.0.0.0", "::"}:
        urls.append(f"http://localhost:{port}")
        if tailscale:
            resolved_hostname = hostname or socket.gethostname()
            urls.append(f"http://{resolved_hostname}:{port}")
    else:
        urls.append(f"http://{host}:{port}")
    deduped: list[str] = []
    for url in urls:
        if url not in deduped:
            deduped.append(url)
    return deduped


class DashboardApp:
    def __init__(self, repo_path: Path, db_path: Path | None = None) -> None:
        self.repo_path = repo_path.resolve()
        self.db_path = db_path.resolve() if db_path is not None else None

    @property
    def default_plans_dir(self) -> Path:
        return self.repo_path / ".kctl" / "plans"

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
        selected_run: RunDetail | None = None
        plan_cards: list[PlanExecutionCard] = []
        selected_plan: PlanExecutionCard | None = None
        selected_plan_file_name: str | None = None
        selected_plan_file_path: str | None = None
        selected_plan_file_contents: str | None = None
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

    def start_run_many(self, plans_dir: Path, concurrency: int, selected_plan_names: list[str] | None = None) -> str:
        run_id = build_multi_run_id()

        def _run() -> None:
            run_many_plans(
                plans_dir.resolve(),
                concurrency=concurrency,
                verbose=False,
                selected_plan_names=selected_plan_names,
                run_id_override=run_id,
            )
            index_repository_state(self.repo_path, db_path=self.db_path)

        threading.Thread(target=_run, daemon=True).start()
        return run_id

    def render_page(
        self,
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
        base_params = {}
        if state.selected_run is not None:
            base_params["run_id"] = state.selected_run.id
        if state.selected_plan is not None:
            base_params["plan_execution_id"] = state.selected_plan.id
        default_plans_status, default_plans_message, default_plan_files = list_plans_in_directory(str(self.default_plans_dir))

        run_items = "".join(
            (
                f"<a class='list-item {_status_class(run.status)}' href='{_escape(_link({}, run_id=run.id, plan_execution_id=None))}'>"
                f"<div><strong>{_escape(run.id)}</strong></div>"
                f"<div>status={_escape(run.status)} started_at={_escape(run.started_at)}</div>"
                f"<div>concurrency={_escape(run.concurrency)}</div>"
                "</a>"
            )
            for run in state.runs
        ) or "<div class='empty'>No indexed runs.</div>"

        overview_html = (
            "<section class='panel'>"
            "<h2>Overview</h2>"
            f"<div>runs={_escape(state.overview.run_count)} active_runs={_escape(state.overview.active_run_count)} failed_runs={_escape(state.overview.failed_run_count)}</div>"
            f"<div>running_plans={_escape(state.overview.running_plan_count)} blocked_plans={_escape(state.overview.blocked_plan_count)} failed_plans={_escape(state.overview.failed_plan_count)}</div>"
            f"<div>stale_workspaces={_escape(state.overview.stale_workspace_count)} recent_failures={_escape(state.overview.recent_failure_count)}</div>"
            "</section>"
        )

        attention_items_html = "".join(
            (
                f"<a class='card {_status_class(item.status)}' href='{_escape(_link({}, run_id=item.run_id, plan_execution_id=item.plan_execution_id))}'>"
                f"<div><strong>{_escape(item.kind)}</strong> plan={_escape(item.plan_slug)}</div>"
                f"<div>status={_escape(item.status)} step={_escape(item.current_step_key)}</div>"
                f"<div>verify={_escape(item.verify_status)} failure_reason={_escape(item.failure_reason)}</div>"
                f"<div>workspace={_escape(item.workspace_path)}</div>"
                "</a>"
            )
            for item in state.attention_items
        ) or "<div class='empty'>No attention items.</div>"

        action_panel_html = (
            "<section class='panel'>"
            "<h2>Actions</h2>"
            "<div class='help'>Use this panel to create a plan for a project, then run the plans directory when you are ready.</div>"
            + (
                f"<div class='notice'>{_escape(state.action_message)}</div>"
                if state.action_message
                else ""
            )
            + (
                f"<form method='post' action='/actions/index'>"
                f"<input type='hidden' name='run_id' value='{_escape(state.selected_run.id if state.selected_run else '')}'>"
                "<div class='help'>Refreshes the dashboard data from saved runs and workspaces on this machine.</div>"
                "<button type='submit'>Refresh Index</button>"
                "</form>"
                "<form method='post' action='/actions/run-many'>"
                f"<input type='hidden' name='run_id' value='{_escape(state.selected_run.id if state.selected_run else '')}'>"
                "<div class='help'>Runs every plan in the plans folder for the target project.</div>"
                f"<label for='target_repo_run_many'><strong>Target Repo</strong></label>"
                f"<input id='target_repo_run_many' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
                "<div id='target_repo_run_many_status' class='repo-check'></div>"
                f"<div><strong>Plans Dir</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
                "<label for='plans_dir'><strong>Plans Dir Override</strong></label>"
                "<input id='plans_dir' name='plans_dir' type='text' placeholder='Optional override'>"
                "<div id='plans_dir_status' class='repo-check'></div>"
                "<div id='plans_dir_preview' class='plans-preview'></div>"
                "<label for='concurrency'><strong>Concurrency</strong></label>"
                "<input id='concurrency' name='concurrency' type='number' min='1' value='1'>"
                "<div class='help'>How many plans can run at the same time. Use 1 for the safest option.</div>"
                "<button type='submit'>Run Plans</button>"
                "</form>"
                "<form method='post' action='/actions/create-plan'>"
                f"<input type='hidden' name='run_id' value='{_escape(state.selected_run.id if state.selected_run else '')}'>"
                "<div class='help'>Creates one new plan file in the target project's plans folder.</div>"
                f"<label for='target_repo_create_plan'><strong>Target Repo</strong></label>"
                f"<input id='target_repo_create_plan' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
                "<div id='target_repo_create_plan_status' class='repo-check'></div>"
                "<label for='template_name'><strong>Template</strong></label>"
                "<select id='template_name' name='template_name'>"
                + "".join(
                    f"<option value='{_escape(name)}'>{_escape(name)}"
                    + (f" - {_escape(description)}" if description else "")
                    + "</option>"
                    for name, description in state.plan_templates
                )
                + "</select>"
                f"<div><strong>Plan Root</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
                "<label for='output_path'><strong>Plan File Name</strong></label>"
                "<input id='output_path' name='output_path' type='text' placeholder='001-sample.yaml' required>"
                "<label for='objective'><strong>Objective</strong></label>"
                "<textarea id='objective' name='objective' rows='5' placeholder='Describe the change' required></textarea>"
                "<label class='checkbox'><input name='force' type='checkbox' value='1'> Overwrite if the file exists</label>"
                "<button type='submit'>Create Plan</button>"
                "</form>"
            )
            + "</section>"
        )

        workspace_items_html = "".join(
            (
                f"<a class='card {_status_class(workspace.lifecycle)}' href='{_escape(_link({}, run_id=workspace.run_id, plan_execution_id=workspace.plan_execution_id))}'>"
                f"<div><strong>{_escape(workspace.plan_slug)}</strong> lifecycle={_escape(workspace.lifecycle)}</div>"
                f"<div>status={_escape(workspace.status)} step={_escape(workspace.current_step_key)}</div>"
                f"<div>verify={_escape(workspace.verify_status)} failure_reason={_escape(workspace.failure_reason)}</div>"
                f"<div>path={_escape(workspace.path)}</div>"
                "</a>"
            )
            for workspace in state.workspaces
        ) or "<div class='empty'>No workspaces.</div>"

        plan_file_items_html = "".join(
            (
                f"<a class='list-item {_status_class('neutral')}' href='{_escape(_link(base_params, selected_plan_file=str((self.default_plans_dir / plan_file_name).resolve())))}'>"
                f"<div><strong>{_escape(plan_file_name)}</strong></div>"
                "</a>"
            )
            for plan_file_name in default_plan_files
        ) or f"<div class='empty'>{_escape(default_plans_message)}</div>"

        plan_items = "".join(
            (
                f"<a class='card {_status_class(plan.status)}' href='{_escape(_link({'run_id': state.selected_run.id}, run_id=state.selected_run.id, plan_execution_id=plan.id))}'>"
                f"<div><strong>{_escape(plan.plan_slug)}</strong></div>"
                f"<div>status={_escape(plan.status)} current_step={_escape(plan.current_step_key)}</div>"
                f"<div>verify={_escape(plan.verify_status)} changed_files={_escape(plan.changed_files_count)}</div>"
                "</a>"
            )
            for plan in state.plan_cards
        ) or "<div class='empty'>No plan executions for this run.</div>"

        timeline_rows = "".join(
            (
                "<tr>"
                f"<td>{_escape(step.sequence_index)}</td>"
                f"<td>{_escape(step.step_key)}</td>"
                f"<td>{_escape(step.kind)}</td>"
                f"<td class='{_status_class(step.status)}'>{_escape(step.status)}</td>"
                f"<td>{_escape(step.verify_status)}</td>"
                f"<td>{_escape(step.changed_files_count)}</td>"
                f"<td>{_escape(step.duration_ms)}</td>"
                f"<td>{_escape(step.output_path)}</td>"
                f"<td>{_escape(step.artifact_path)}</td>"
                "</tr>"
            )
            for step in state.steps
        ) or "<tr><td colspan='9' class='empty'>No step timeline available.</td></tr>"

        selected_run_html = ""
        if state.selected_run is not None:
            selected_run_html = (
                "<section class='panel'>"
                "<h2>Run Detail</h2>"
                f"<div><strong>{_escape(state.selected_run.id)}</strong></div>"
                f"<div>status={_escape(state.selected_run.status)} launch_source={_escape(state.selected_run.launch_source)}</div>"
                f"<div>started_at={_escape(state.selected_run.started_at)} ended_at={_escape(state.selected_run.ended_at)}</div>"
                f"<div>plans={_escape(state.selected_run.plan_execution_count)} passed={_escape(state.selected_run.passed_count)} "
                f"failed={_escape(state.selected_run.failed_count)} running={_escape(state.selected_run.running_count)} "
                f"blocked={_escape(state.selected_run.blocked_count)}</div>"
                "</section>"
            )

        live_output_html = (
            "<div class='empty'>Start a run to see live plan output here.</div>"
            if state.selected_run is None
            else "<div class='empty'>No live output captured for this run yet.</div>"
        )
        if state.live_output_path is not None:
            live_output_html = (
                f"<div>status={_escape(state.live_output_status)} path={_escape(state.live_output_path)}</div>"
                f"<pre id='live_output_stream' class='code-block live-output'>{_escape(state.live_output or '')}</pre>"
            )

        workspace_html = "<div class='empty'>No workspace details available.</div>"
        if state.workspace is not None:
            workspace_html = (
                f"<div><strong>path</strong>: {_escape(state.workspace.path)}</div>"
                f"<div><strong>branch</strong>: {_escape(state.workspace.branch_name)}</div>"
                f"<div><strong>base_ref</strong>: {_escape(state.workspace.base_ref)}</div>"
                f"<div><strong>status</strong>: {_escape(state.workspace.status)}</div>"
                f"<div><strong>created_at</strong>: {_escape(state.workspace.created_at)}</div>"
                f"<div><strong>released_at</strong>: {_escape(state.workspace.released_at)}</div>"
            )

        selected_plan_html = ""
        if state.selected_plan is not None:
            selected_plan_html = (
                "<section class='panel'>"
                "<h2>Plan Execution Detail</h2>"
                f"<div><strong>{_escape(state.selected_plan.plan_slug)}</strong></div>"
                f"<div>status={_escape(state.selected_plan.status)} current_step={_escape(state.selected_plan.current_step_key)}</div>"
                f"<div>verify={_escape(state.selected_plan.verify_status)} changed_files={_escape(state.selected_plan.changed_files_count)}</div>"
                f"<div>branch={_escape(state.selected_plan.branch_name)}</div>"
                f"<div>log_path={_escape(state.selected_plan.log_path)}</div>"
                f"<div>failure_reason={_escape(state.selected_plan.failure_reason)}</div>"
                "</section>"
            )

        selected_plan_file_html = "<div class='empty'>No plan file selected.</div>"
        if state.selected_plan_file_name is not None and state.selected_plan_file_contents is not None:
            selected_plan_file_html = (
                f"<div><strong>{_escape(state.selected_plan_file_name)}</strong></div>"
                f"<div>path={_escape(state.selected_plan_file_path)}</div>"
                f"<pre class='code-block'>{_escape(state.selected_plan_file_contents)}</pre>"
            )

        return rf"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>kctl Dashboard</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 0;
      padding: 16px;
      background: #f5f5f5;
      color: #111;
      box-sizing: border-box;
      overflow-x: hidden;
    }}
    .page, main {{
      max-width: 1400px;
      margin: 0 auto;
      box-sizing: border-box;
      width: 100%;
    }}
    .page-header {{
      background: #1f2937;
      color: white;
      border-radius: 10px;
      padding: 16px;
    }}
    main {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 16px;
      padding: 16px;
      align-items: start;
      min-width: 0;
    }}
    .column {{
      display: flex;
      flex-direction: column;
      gap: 16px;
      min-width: 0;
    }}
    .panel {{
      background: white;
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 16px;
      min-width: 0;
    }}
    form {{
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }}
    input, button, textarea {{
      font: inherit;
      padding: 8px 10px;
    }}
    select {{
      font: inherit;
      padding: 8px 10px;
    }}
    button {{
      cursor: pointer;
    }}
    textarea {{
      resize: vertical;
      min-height: 120px;
    }}
    .checkbox {{
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .notice {{
      margin-bottom: 12px;
      padding: 10px 12px;
      border-radius: 6px;
      background: #eff6ff;
      border: 1px solid #bfdbfe;
    }}
    .help {{
      color: #4b5563;
      font-size: 0.95em;
      line-height: 1.4;
    }}
    .repo-check {{
      font-size: 0.92em;
      color: #4b5563;
    }}
    .repo-check[data-status='ok'] {{
      color: #15803d;
    }}
    .repo-check[data-status='missing'],
    .repo-check[data-status='not_dir'],
    .repo-check[data-status='empty'] {{
      color: #b91c1c;
    }}
    .plans-preview {{
      color: #374151;
      font-size: 0.92em;
      line-height: 1.4;
    }}
    .plans-preview ul {{
      margin: 6px 0 0;
      padding-left: 18px;
    }}
    .plans-preview label {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 4px 0;
    }}
    .list-item, .card {{
      display: block;
      text-decoration: none;
      color: inherit;
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 8px;
      background: white;
      overflow-wrap: anywhere;
      min-width: 0;
    }}
    .list-item:hover, .card:hover {{
      border-color: #999;
    }}
    .code-block {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      background: #f8fafc;
      border: 1px solid #e5e7eb;
      border-radius: 6px;
      padding: 12px;
      margin-top: 12px;
      font-family: monospace;
      font-size: 0.9em;
    }}
    .live-output {{
      max-height: 420px;
      overflow: auto;
      background: #0f172a;
      color: #e2e8f0;
    }}
    .status-success {{
      border-left: 4px solid #15803d;
    }}
    .status-failure {{
      border-left: 4px solid #b91c1c;
    }}
    .status-running {{
      border-left: 4px solid #1d4ed8;
    }}
    .status-neutral {{
      border-left: 4px solid #6b7280;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      min-width: 760px;
    }}
    th, td {{
      text-align: left;
      padding: 8px;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    .table-scroll {{
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      max-width: 100%;
    }}
    .empty {{
      color: #666;
      font-style: italic;
    }}
    code {{
      font-family: monospace;
      font-size: 0.95em;
    }}
    header div, .panel div {{
      overflow-wrap: anywhere;
    }}
    label {{
      overflow-wrap: anywhere;
    }}
    @media (max-width: 900px) {{
      body {{
        padding: 12px;
      }}
      main {{
        grid-template-columns: 1fr;
        padding: 12px 0 0;
      }}
      .page-header {{
        padding: 14px 12px;
      }}
      .panel {{
        padding: 14px;
      }}
    }}
    @media (max-width: 640px) {{
      body {{
        font-size: 15px;
        padding: 10px;
      }}
      .page-header {{
        padding: 10px;
      }}
      main {{
        gap: 12px;
        padding: 10px 0 0;
      }}
      .column {{
        gap: 12px;
      }}
      .list-item, .card {{
        padding: 10px;
      }}
      input, button, select, textarea {{
        width: 100%;
        box-sizing: border-box;
        min-width: 0;
      }}
      table {{
        min-width: 640px;
      }}
    }}
  </style>
  <script>
    function wireRepoCheck(inputId, statusId) {{
      const input = document.getElementById(inputId);
      const status = document.getElementById(statusId);
      if (!input || !status) return;
      let timer = null;
      async function refreshStatus() {{
        const params = new URLSearchParams({{ path: input.value }});
        const response = await fetch(`/api/check-repo?${{params.toString()}}`);
        const data = await response.json();
        status.dataset.status = data.status;
        status.textContent = data.message;
      }}
      function scheduleRefresh() {{
        clearTimeout(timer);
        timer = setTimeout(refreshStatus, 150);
      }}
      input.addEventListener('input', scheduleRefresh);
      refreshStatus();
    }}
    function wirePlansPreview(targetRepoInputId, plansDirInputId, statusId, previewId) {{
      const targetRepoInput = document.getElementById(targetRepoInputId);
      const plansDirInput = document.getElementById(plansDirInputId);
      const status = document.getElementById(statusId);
      const preview = document.getElementById(previewId);
      if (!targetRepoInput || !plansDirInput || !status || !preview) return;
      let timer = null;
      function resolvedPlansDir() {{
        const overrideValue = plansDirInput.value.trim();
        if (overrideValue) return overrideValue;
        const repoValue = targetRepoInput.value.trim();
        if (!repoValue) return "";
        return repoValue.replace(/\/+$/, "") + "/.kctl/plans";
      }}
      async function refreshPreview() {{
        const params = new URLSearchParams({{ path: resolvedPlansDir() }});
        const response = await fetch(`/api/list-plans?${{params.toString()}}`);
        const data = await response.json();
        status.dataset.status = data.status;
        status.textContent = data.message;
        if (!data.plans || data.plans.length === 0) {{
          preview.innerHTML = "";
          return;
        }}
        preview.innerHTML =
          "<strong>Plans found</strong>" +
          data.plans.map((plan) => `<label><input type="checkbox" name="selected_plans" value="${{plan}}"> <span>${{plan}}</span></label>`).join("");
      }}
      function scheduleRefresh() {{
        clearTimeout(timer);
        timer = setTimeout(refreshPreview, 150);
      }}
      targetRepoInput.addEventListener('input', scheduleRefresh);
      plansDirInput.addEventListener('input', scheduleRefresh);
      refreshPreview();
    }}
    window.addEventListener('DOMContentLoaded', () => {{
      wireRepoCheck('target_repo_run_many', 'target_repo_run_many_status');
      wireRepoCheck('target_repo_create_plan', 'target_repo_create_plan_status');
      wirePlansPreview('target_repo_run_many', 'plans_dir', 'plans_dir_status', 'plans_dir_preview');
      const runId = {json.dumps(state.selected_run.id if state.selected_run else "")};
      const runStatus = {json.dumps(state.live_output_status or (state.selected_run.status if state.selected_run else ""))};
      const outputNode = document.getElementById('live_output_stream');
      if (runId && outputNode) {{
        const refreshOutput = async () => {{
          const params = new URLSearchParams({{ run_id: runId }});
          const response = await fetch(`/api/run-output?${{params.toString()}}`);
          if (!response.ok) return;
          const data = await response.json();
          outputNode.textContent = data.output || "";
        }};
        refreshOutput();
        if (runStatus === "running") {{
          window.setInterval(refreshOutput, 2000);
        }}
      }}
    }});
  </script>
</head>
<body>
  <div class="page">
    <header class="page-header">
      <h1>kctl Dashboard</h1>
      <div>repository={_escape(state.repo_name)} root={_escape(state.repo_root)}</div>
    </header>
    <main>
            <div class="column">
              {overview_html}
              {action_panel_html}
              <section class="panel">
                <h2>Attention Queue</h2>
                {attention_items_html}
              </section>
              <section class="panel">
                <h2>Workspaces</h2>
                {workspace_items_html}
              </section>
              <section class="panel">
                <h2>Plans</h2>
                {plan_file_items_html}
              </section>
              <section class="panel">
                <h2>Runs</h2>
                {run_items}
      </section>
    </div>
    <div class="column">
      {selected_run_html}
      <section class="panel">
        <h2>Plan Executions</h2>
        {plan_items}
      </section>
      {selected_plan_html}
      <section class="panel">
        <h2>Live Output</h2>
        {live_output_html}
      </section>
      <section class="panel">
        <h2>Plan File Detail</h2>
        {selected_plan_file_html}
      </section>
      <section class="panel">
        <h2>Step Timeline</h2>
        <div class="table-scroll">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>step</th>
                <th>kind</th>
                <th>status</th>
                <th>verify</th>
                <th>changed</th>
                <th>duration_ms</th>
                <th>output_path</th>
                <th>artifact_path</th>
              </tr>
            </thead>
            <tbody>
              {timeline_rows}
            </tbody>
          </table>
        </div>
      </section>
      <section class="panel">
        <h2>Workspace</h2>
        {workspace_html}
      </section>
    </div>
    </main>
  </div>
</body>
</html>
"""


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
            if parsed.path != "/":
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            params = parse_qs(parsed.query)
            run_id = params.get("run_id", [None])[0]
            plan_execution_id = params.get("plan_execution_id", [None])[0]
            action_message = params.get("message", [None])[0]
            selected_plan_file = params.get("selected_plan_file", [None])[0]
            try:
                body = app.render_page(
                    run_id=run_id,
                    plan_execution_id=plan_execution_id,
                    action_message=action_message,
                    selected_plan_file=selected_plan_file,
                )
            except PlanError as exc:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    (
                        "<!doctype html><html><body><h1>kctl Dashboard</h1>"
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
            if parsed.path not in {"/actions/index", "/actions/run-many", "/actions/create-plan"}:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            form_data = parse_qs(self.rfile.read(content_length).decode("utf-8"))
            run_id = form_data.get("run_id", [""])[0] or None
            plan_execution_id = form_data.get("plan_execution_id", [""])[0] or None
            try:
                if parsed.path == "/actions/index":
                    app.run_index_now()
                    message = "Index refreshed."
                elif parsed.path == "/actions/create-plan":
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
                    created_path = app.create_plan(
                        target_repo=Path(target_repo_value).expanduser(),
                        template_name=template_name,
                        output_name=output_path_value,
                        objective=objective,
                        force=form_data.get("force", [""])[0] == "1",
                    )
                    message = f"Created plan at {created_path}."
                else:
                    target_repo_value = form_data.get("target_repo", [""])[0].strip()
                    if not target_repo_value:
                        raise PlanError("Target repo is required.")
                    target_repo = Path(target_repo_value).expanduser().resolve()
                    plans_dir_value = form_data.get("plans_dir", [""])[0].strip()
                    plans_dir = Path(plans_dir_value).expanduser() if plans_dir_value else app.plans_dir_for_repo(target_repo)
                    concurrency_value = form_data.get("concurrency", ["1"])[0].strip() or "1"
                    concurrency = int(concurrency_value)
                    if concurrency < 1:
                        raise PlanError("Concurrency must be at least 1.")
                    selected_plan_names = [name.strip() for name in form_data.get("selected_plans", []) if name.strip()]
                    run_id = app.start_run_many(plans_dir, concurrency, selected_plan_names=selected_plan_names or None)
                    message = f"Started run-many for {plans_dir}."
            except (PlanError, ValueError) as exc:
                message = str(exc)
            location = _link(
                {},
                run_id=run_id,
                plan_execution_id=plan_execution_id,
                message=message,
            )
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
