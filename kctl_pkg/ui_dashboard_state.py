from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from .multi import resolve_multi_run_log
from .plan import load_plan_templates
from .paths import project_root
from .types import PlanError
from .ui_dashboard_support import _preflight_run_snapshot, available_providers, read_plan_file
from .ui_read import (
    PlanExecutionCard,
    RunDetail,
    StepTimelineItem,
    WorkspaceDetail,
    get_plan_execution,
    get_repository,
    get_repository_overview,
    get_run,
    get_workspace,
    list_attention_items,
    list_plan_executions,
    list_runs,
    list_step_executions,
    list_workspaces,
)


def load_live_run_data(app: object, run_id: str) -> dict[str, object] | None:
    try:
        run_log = resolve_multi_run_log(app.repo_path, run_id)
    except PlanError:
        return None
    try:
        return json.loads(run_log.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def load_saved_run_data(run_root_path: str | None) -> dict[str, object] | None:
    if not run_root_path:
        return None
    run_log = Path(run_root_path) / "run.json"
    if not run_log.exists():
        return None
    try:
        return json.loads(run_log.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def read_live_output(run_data: dict[str, object] | None) -> tuple[str | None, str | None, str | None]:
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


def build_run_detail_from_live_data(app: object, run_data: dict[str, object]) -> RunDetail:
    plan_states = list(run_data.get("plans") or [])
    return RunDetail(
        id=str(run_data.get("run_id") or ""),
        repository_id=str(app.repo_path),
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


def build_plan_cards_from_live_data(app: object, run_data: dict[str, object]) -> list[PlanExecutionCard]:
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
                repository_id=str(app.repo_path),
                plan_definition_id=f"{app.repo_path}:{Path(plan_path).resolve()}" if plan_path else f"{app.repo_path}:{plan_id}",
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


def load_dashboard_state(
    app: object,
    *,
    state_type: Callable[..., object],
    summarize_preflight: Callable[..., dict[str, object]],
    run_id: str | None = None,
    plan_execution_id: str | None = None,
    action_message: str | None = None,
    selected_plan_file: str | None = None,
) -> object:
    repository = get_repository(app.repo_path, db_path=app.db_path)
    overview = get_repository_overview(app.repo_path, db_path=app.db_path)
    attention_items = list_attention_items(app.repo_path, db_path=app.db_path)
    workspaces = list_workspaces(app.repo_path, db_path=app.db_path)
    runs = list_runs(app.repo_path, db_path=app.db_path)
    templates = load_plan_templates(project_root())
    plan_templates = [
        (template_name, template.get("description") if isinstance(template, dict) else None)
        for template_name, template in templates.items()
    ]
    tracked_projects = app.load_tracked_projects()
    selected_run: RunDetail | None = None
    plan_cards: list[PlanExecutionCard] = []
    selected_plan: PlanExecutionCard | None = None
    selected_plan_file_name: str | None = None
    selected_plan_file_path: str | None = None
    selected_plan_file_contents: str | None = None
    launch_preflight = summarize_preflight(
        str(app.repo_path),
        str(app.default_plans_dir),
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
        live_run_data = app.load_live_run_data(run_id)
        try:
            selected_run = get_run(app.repo_path, run_id, db_path=app.db_path)
            plan_cards = list_plan_executions(app.repo_path, run_id, db_path=app.db_path)
        except PlanError:
            if live_run_data is not None:
                selected_run = app._build_run_detail_from_live_data(live_run_data)
                plan_cards = app._build_plan_cards_from_live_data(live_run_data)
            else:
                raise
        if live_run_data is not None and str(live_run_data.get("status") or "") == "running":
            selected_run = app._build_run_detail_from_live_data(live_run_data)
            plan_cards = app._build_plan_cards_from_live_data(live_run_data)
        selected_run_preflight = _preflight_run_snapshot(
            live_run_data or (app.load_saved_run_data(selected_run.run_root_path) if selected_run is not None else None)
        )

    if plan_execution_id is None and plan_cards:
        plan_execution_id = plan_cards[0].id
    if plan_execution_id is not None:
        try:
            selected_plan = get_plan_execution(plan_execution_id, app.repo_path, db_path=app.db_path)
            steps = list_step_executions(plan_execution_id, app.repo_path, db_path=app.db_path)
            workspace = get_workspace(plan_execution_id, app.repo_path, db_path=app.db_path)
        except PlanError:
            if live_run_data is not None:
                selected_plan = next((plan for plan in plan_cards if plan.id == plan_execution_id), None)
    if selected_plan_file:
        plan_file_path = Path(selected_plan_file).expanduser().resolve()
        selected_plan_file_name, selected_plan_file_contents = read_plan_file(plan_file_path)
        selected_plan_file_path = str(plan_file_path)
    live_output, live_output_path, live_output_status = app.read_live_output(live_run_data)

    return state_type(
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
