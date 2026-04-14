from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .git import ensure_git_repo, get_repo_root, probe_workspace_dirty
from .types import PlanError
from .ui_index import default_db_path
from .ui_store import UIStateStore


STALE_RUNNING_THRESHOLD_SECONDS = 1800


def _effective_plan_status(status: str | None, failure_reason: str | None) -> str:
    if status == "blocked" and failure_reason == "run_stopped":
        return "stopped"
    return str(status or "unknown")


@dataclass(frozen=True)
class RepositorySummary:
    id: str
    name: str
    root_path: str
    default_branch: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class RunListItem:
    id: str
    repository_id: str
    status: str
    launch_source: str
    concurrency: int
    started_at: str
    ended_at: str | None
    run_root_path: str
    plan_execution_count: int


@dataclass(frozen=True)
class RunDetail:
    id: str
    repository_id: str
    status: str
    launch_source: str
    plans_dir: str
    concurrency: int
    started_at: str
    ended_at: str | None
    run_root_path: str
    plan_execution_count: int
    passed_count: int
    failed_count: int
    running_count: int
    blocked_count: int


@dataclass(frozen=True)
class RepositoryOverview:
    run_count: int
    active_run_count: int
    failed_run_count: int
    blocked_plan_count: int
    failed_plan_count: int
    running_plan_count: int
    stale_workspace_count: int
    recent_failure_count: int


@dataclass(frozen=True)
class PlanExecutionCard:
    id: str
    run_id: str
    repository_id: str
    plan_definition_id: str
    plan_slug: str
    plan_title: str | None
    plan_file_path: str
    objective: str
    phase_name: str | None
    group_name: str | None
    status: str
    current_step_key: str | None
    verify_status: str
    started_at: str
    ended_at: str | None
    worktree_path: str | None
    branch_name: str | None
    log_path: str | None
    changed_files_count: int
    failure_reason: str | None


@dataclass(frozen=True)
class StepTimelineItem:
    id: str
    plan_execution_id: str
    step_key: str
    step_name: str | None
    kind: str
    sequence_index: int
    status: str
    verify_status: str
    started_at: str
    ended_at: str | None
    duration_ms: int | None
    output_path: str | None
    artifact_path: str | None
    verify_exit_code: int | None
    changed_files_count: int
    changed_files: list[str]
    metadata: dict[str, object]


@dataclass(frozen=True)
class WorkspaceDetail:
    id: str
    repository_id: str
    plan_execution_id: str
    path: str
    branch_name: str | None
    base_ref: str | None
    status: str
    created_at: str
    released_at: str | None


@dataclass(frozen=True)
class WorkspaceSummary:
    id: str
    repository_id: str
    plan_execution_id: str
    run_id: str
    plan_slug: str
    path: str
    branch_name: str | None
    base_ref: str | None
    status: str
    lifecycle: str
    current_step_key: str | None
    verify_status: str
    failure_reason: str | None
    created_at: str
    released_at: str | None


@dataclass(frozen=True)
class AttentionItem:
    kind: str
    status: str
    operator_action: str  # "active" | "safe_rerun" | "review_workspace" | "fix_config" | "investigate_stale"
    plan_execution_id: str
    run_id: str
    plan_slug: str
    plan_file_path: str | None
    current_step_key: str | None
    failure_reason: str | None
    reset_hint: str | None
    verify_status: str
    workspace_path: str | None
    started_at: str


@dataclass(frozen=True)
class AgentProfileSummary:
    id: str
    display_name: str
    avatar_uri: str | None
    theme_key: str | None
    preset_key: str | None
    status: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AgentAssignmentSummary:
    id: str
    agent_id: str
    plan_execution_id: str
    assigned_at: str
    released_at: str | None
    status: str
    agent_display_name: str
    avatar_uri: str | None
    theme_key: str | None
    preset_key: str | None
    agent_status: str


def _open_store(db_path: Path) -> UIStateStore:
    if not db_path.exists():
        raise PlanError(f"UI state database does not exist: {db_path}. Run `kctl ui index <repo>` first.")
    store = UIStateStore(db_path)
    return store


def resolve_db_path(repo_root_or_id: str | Path, db_path: Path | None = None) -> tuple[Path | None, Path]:
    if db_path is not None:
        candidate = Path(repo_root_or_id).expanduser()
        if candidate.exists():
            ensure_git_repo(candidate)
            repo_root = get_repo_root(candidate)
            return repo_root, db_path
        return None, db_path

    candidate = Path(repo_root_or_id).expanduser()
    if candidate.exists():
        ensure_git_repo(candidate)
        repo_root = get_repo_root(candidate)
        return repo_root, default_db_path(repo_root)

    raise PlanError(f"Could not resolve repository path: {repo_root_or_id}")


def list_repositories(repo_root_or_id: str | Path, db_path: Path | None = None) -> list[RepositorySummary]:
    _, resolved_db_path = resolve_db_path(repo_root_or_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_repositories()
    finally:
        store.close()
    return [
        RepositorySummary(
            id=row["id"],
            name=row["name"],
            root_path=row["root_path"],
            default_branch=row["default_branch"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
    ]


def get_repository(repo_root_or_id: str | Path, db_path: Path | None = None) -> RepositorySummary:
    repo_root, resolved_db_path = resolve_db_path(repo_root_or_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        row = None
        if repo_root is not None:
            row = store.get_repository_by_root_path(str(repo_root))
        if row is None:
            row = store.get_repository_by_id(str(repo_root_or_id))
        if row is None:
            raise PlanError(f"Indexed repository not found: {repo_root_or_id}")
    finally:
        store.close()
    return RepositorySummary(
        id=row["id"],
        name=row["name"],
        root_path=row["root_path"],
        default_branch=row["default_branch"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _resolve_repository_context(
    repo_id: str | Path,
    db_path: Path | None = None,
) -> tuple[RepositorySummary, Path]:
    repository = get_repository(repo_id, db_path=db_path)
    _, resolved_db_path = resolve_db_path(repository.root_path, db_path=db_path)
    return repository, resolved_db_path


def list_runs(repo_id: str | Path, db_path: Path | None = None) -> list[RunListItem]:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_runs(repository.id)
    finally:
        store.close()
    return [
        RunListItem(
            id=row["id"],
            repository_id=repository.id,
            status=row["status"],
            launch_source=row["launch_source"],
            concurrency=row["concurrency"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            run_root_path=row["run_root_path"],
            plan_execution_count=row["plan_execution_count"],
        )
        for row in rows
    ]


def get_run(repo_id: str | Path, run_id: str, db_path: Path | None = None) -> RunDetail:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        row = store.get_run_with_counts(run_id)
        if row is None:
            raise PlanError(f"Indexed run not found: {run_id}")
    finally:
        store.close()
    return RunDetail(
        id=row["id"],
        repository_id=row["repository_id"],
        status=row["status"],
        launch_source=row["launch_source"],
        plans_dir=row["plans_dir"],
        concurrency=row["concurrency"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        run_root_path=row["run_root_path"],
        plan_execution_count=row["plan_execution_count"],
        passed_count=row["passed_count"] or 0,
        failed_count=row["failed_count"] or 0,
        running_count=row["running_count"] or 0,
        blocked_count=row["blocked_count"] or 0,
    )


def list_plan_executions(repo_id: str | Path, run_id: str, db_path: Path | None = None) -> list[PlanExecutionCard]:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_plan_executions_for_run(run_id)
    finally:
        store.close()
    return [
        PlanExecutionCard(
            id=row["id"],
            run_id=row["run_id"],
            repository_id=repository.id,
            plan_definition_id=row["plan_definition_id"],
            plan_slug=row["slug"],
            plan_title=row["title"],
            plan_file_path=row["file_path"],
            objective=row["objective"] if "objective" in row.keys() else "",
            phase_name=row["phase_name"] if "phase_name" in row.keys() else None,
            group_name=row["group_name"] if "group_name" in row.keys() else None,
            status=_effective_plan_status(row["status"], row["failure_reason"]),
            current_step_key=row["current_step_key"],
            verify_status=row["verify_status"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            worktree_path=row["worktree_path"],
            branch_name=row["branch_name"],
            log_path=row["log_path"],
            changed_files_count=row["changed_files_count"],
            failure_reason=row["failure_reason"],
        )
        for row in rows
    ]


def list_repository_plan_executions(repo_id: str | Path, db_path: Path | None = None) -> list[PlanExecutionCard]:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_plan_executions_for_repository(repository.id)
    finally:
        store.close()
    return [
        PlanExecutionCard(
            id=row["id"],
            run_id=row["run_id_value"],
            repository_id=repository.id,
            plan_definition_id=row["plan_definition_id"],
            plan_slug=row["slug"],
            plan_title=row["title"],
            plan_file_path=row["file_path"],
            objective=row["objective"] if "objective" in row.keys() else "",
            phase_name=row["phase_name"] if "phase_name" in row.keys() else None,
            group_name=row["group_name"] if "group_name" in row.keys() else None,
            status=_effective_plan_status(row["status"], row["failure_reason"]),
            current_step_key=row["current_step_key"],
            verify_status=row["verify_status"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            worktree_path=row["worktree_path"],
            branch_name=row["branch_name"],
            log_path=row["log_path"],
            changed_files_count=row["changed_files_count"],
            failure_reason=row["failure_reason"],
        )
        for row in rows
    ]


def get_plan_execution(plan_execution_id: str, repo_id: str | Path, db_path: Path | None = None) -> PlanExecutionCard:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        row = store.get_plan_execution(plan_execution_id)
        if row is None:
            raise PlanError(f"Indexed plan execution not found: {plan_execution_id}")
    finally:
        store.close()
    return PlanExecutionCard(
        id=row["id"],
        run_id=row["run_id_value"],
        repository_id=row["repository_id"],
        plan_definition_id=row["plan_definition_id"],
        plan_slug=row["slug"],
        plan_title=row["title"],
        plan_file_path=row["file_path"],
        objective=row["objective"],
        phase_name=row["phase_name"],
        group_name=row["group_name"],
        status=_effective_plan_status(row["status"], row["failure_reason"]),
        current_step_key=row["current_step_key"],
        verify_status=row["verify_status"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        worktree_path=row["worktree_path"],
        branch_name=row["branch_name"],
        log_path=row["log_path"],
        changed_files_count=row["changed_files_count"],
        failure_reason=row["failure_reason"],
    )


def list_step_executions(plan_execution_id: str, repo_id: str | Path, db_path: Path | None = None) -> list[StepTimelineItem]:
    _, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_step_executions_for_plan_execution(plan_execution_id)
    finally:
        store.close()
    items: list[StepTimelineItem] = []
    for row in rows:
        metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
        changed_files = json.loads(row["changed_files_json"]) if row["changed_files_json"] else []
        items.append(
            StepTimelineItem(
                id=row["id"],
                plan_execution_id=row["plan_execution_id"],
                step_key=row["step_key"],
                step_name=row["step_name"],
                kind=row["kind"],
                sequence_index=row["sequence_index"],
                status=row["status"],
                verify_status=row["verify_status"],
                started_at=row["started_at"],
                ended_at=row["ended_at"],
                duration_ms=row["duration_ms"],
                output_path=row["output_path"],
                artifact_path=row["artifact_path"],
                verify_exit_code=row["verify_exit_code"],
                changed_files_count=row["changed_files_count"],
                changed_files=changed_files,
                metadata=metadata,
            )
        )
    return items


def get_workspace(plan_execution_id: str, repo_id: str | Path, db_path: Path | None = None) -> WorkspaceDetail | None:
    _, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        row = store.get_workspace_for_plan_execution(plan_execution_id)
    finally:
        store.close()
    if row is None:
        return None
    return WorkspaceDetail(
        id=row["id"],
        repository_id=row["repository_id"],
        plan_execution_id=row["plan_execution_id"],
        path=row["path"],
        branch_name=row["branch_name"],
        base_ref=row["base_ref"],
        status=row["status"],
        created_at=row["created_at"],
        released_at=row["released_at"],
    )


def derive_workspace_lifecycle(
    workspace_status: str,
    plan_status: str,
    released_at: str | None,
) -> str:
    if workspace_status == "active" and plan_status == "running" and released_at is None:
        return "active"
    if released_at is not None or workspace_status == "ready":
        return "released"
    if workspace_status == "active" and plan_status != "running":
        return "stale"
    return workspace_status or "unknown"


def list_workspaces(repo_id: str | Path, db_path: Path | None = None) -> list[WorkspaceSummary]:
    repository, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_workspaces_for_repository(repository.id)
    finally:
        store.close()
    return [
        WorkspaceSummary(
            id=row["id"],
            repository_id=row["repository_id"],
            plan_execution_id=row["plan_execution_id"],
            run_id=row["run_id_value"],
            plan_slug=row["slug"],
            path=row["path"],
            branch_name=row["branch_name"],
            base_ref=row["base_ref"],
            status=row["status"],
            lifecycle=derive_workspace_lifecycle(
                workspace_status=row["status"],
                plan_status=row["plan_status"],
                released_at=row["released_at"],
            ),
            current_step_key=row["current_step_key"],
            verify_status=row["verify_status"],
            failure_reason=row["failure_reason"],
            created_at=row["created_at"],
            released_at=row["released_at"],
        )
        for row in rows
    ]


def get_repository_overview(repo_id: str | Path, db_path: Path | None = None) -> RepositoryOverview:
    runs = list_runs(repo_id, db_path=db_path)
    plan_executions = list_repository_plan_executions(repo_id, db_path=db_path)
    workspaces = list_workspaces(repo_id, db_path=db_path)
    stale_workspace_count = sum(1 for workspace in workspaces if workspace.lifecycle == "stale")
    recent_failure_count = sum(1 for plan in plan_executions if plan.status in {"failed", "blocked"})
    return RepositoryOverview(
        run_count=len(runs),
        active_run_count=sum(1 for run in runs if run.status == "running"),
        failed_run_count=sum(1 for run in runs if run.status == "failed"),
        blocked_plan_count=sum(1 for plan in plan_executions if plan.status == "blocked"),
        failed_plan_count=sum(1 for plan in plan_executions if plan.status == "failed"),
        running_plan_count=sum(1 for plan in plan_executions if plan.status == "running"),
        stale_workspace_count=stale_workspace_count,
        recent_failure_count=recent_failure_count,
    )


def classify_operator_action(
    plan_status: str,
    failure_reason: str | None,
    workspace_is_dirty: bool | None,
    started_at: str,
    now_utc: datetime | None = None,
) -> str:
    """Classify what operator action is appropriate for a non-passing plan execution.

    Returns one of:
      "active"            — currently running, no sign of staleness
      "investigate_stale" — running but no progress for >STALE_RUNNING_THRESHOLD_SECONDS
      "fix_config"        — blocked by preflight (env, binary, path)
      "review_workspace"  — workspace has uncommitted changes; human must decide before retry
      "safe_rerun"        — failed cleanly; can be relaunched immediately
    """
    if plan_status == "running":
        if now_utc is not None:
            try:
                started = datetime.fromisoformat(started_at)
                elapsed = (now_utc - started).total_seconds()
                if elapsed > STALE_RUNNING_THRESHOLD_SECONDS:
                    return "investigate_stale"
            except (ValueError, TypeError):
                pass
        return "active"
    if failure_reason == "preflight_failed":
        return "fix_config"
    if workspace_is_dirty is True:
        return "review_workspace"
    return "safe_rerun"


def _extract_reset_hint_from_log(
    log_path: str | None,
    failure_reason: str | None,
) -> str | None:
    if failure_reason not in {"agent_quota_exhausted", "agent_rate_limited"}:
        return None
    if not log_path:
        return None
    try:
        run_data = json.loads(Path(log_path).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    steps = run_data.get("steps")
    if not isinstance(steps, list):
        return None
    for step in reversed(steps):
        if not isinstance(step, dict):
            continue
        if step.get("failure_reason") not in {"agent_quota_exhausted", "agent_rate_limited"}:
            continue
        details = step.get("failure_details")
        if not isinstance(details, dict):
            continue
        hint = details.get("reset_hint")
        if isinstance(hint, str) and hint.strip():
            return hint.strip()
    return None


def list_attention_items(repo_id: str | Path, db_path: Path | None = None) -> list[AttentionItem]:
    plan_executions = list_repository_plan_executions(repo_id, db_path=db_path)
    now_utc = datetime.now(timezone.utc)
    items: list[AttentionItem] = []
    for plan in plan_executions:
        if plan.verify_status == "failed":
            kind = "failed_verify"
        elif plan.status == "blocked":
            kind = "blocked_plan"
        elif plan.status == "failed":
            kind = "failed_plan"
        elif plan.status == "running" and plan.worktree_path:
            kind = "active_workspace"
        else:
            continue
        workspace_is_dirty = probe_workspace_dirty(plan.worktree_path)
        operator_action = classify_operator_action(
            plan_status=plan.status,
            failure_reason=plan.failure_reason,
            workspace_is_dirty=workspace_is_dirty,
            started_at=plan.started_at,
            now_utc=now_utc,
        )
        reset_hint = _extract_reset_hint_from_log(
            plan.log_path,
            plan.failure_reason,
        )
        items.append(
            AttentionItem(
                kind=kind,
                status=plan.status,
                operator_action=operator_action,
                plan_execution_id=plan.id,
                run_id=plan.run_id,
                plan_slug=plan.plan_slug,
                plan_file_path=plan.plan_file_path,
                current_step_key=plan.current_step_key,
                failure_reason=plan.failure_reason,
                reset_hint=reset_hint,
                verify_status=plan.verify_status,
                workspace_path=plan.worktree_path,
                started_at=plan.started_at,
            )
        )
    return items


def list_agent_profiles(repo_id: str | Path, db_path: Path | None = None) -> list[AgentProfileSummary]:
    _, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_agent_profiles()
    finally:
        store.close()
    return [
        AgentProfileSummary(
            id=row["id"],
            display_name=row["display_name"],
            avatar_uri=row["avatar_uri"],
            theme_key=row["theme_key"],
            preset_key=row["preset_key"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
    ]


def list_agent_assignments(
    repo_id: str | Path,
    db_path: Path | None = None,
    plan_execution_id: str | None = None,
    active_only: bool = False,
) -> list[AgentAssignmentSummary]:
    _, resolved_db_path = _resolve_repository_context(repo_id, db_path=db_path)
    store = _open_store(resolved_db_path)
    try:
        rows = store.list_agent_assignments(plan_execution_id=plan_execution_id, active_only=active_only)
    finally:
        store.close()
    return [
        AgentAssignmentSummary(
            id=row["id"],
            agent_id=row["agent_id"],
            plan_execution_id=row["plan_execution_id"],
            assigned_at=row["assigned_at"],
            released_at=row["released_at"],
            status=row["status"],
            agent_display_name=row["display_name"],
            avatar_uri=row["avatar_uri"],
            theme_key=row["theme_key"],
            preset_key=row["preset_key"],
            agent_status=row["agent_status"],
        )
        for row in rows
    ]
