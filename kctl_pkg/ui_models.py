from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class RepositoryRecord:
    id: str
    name: str
    root_path: str
    default_branch: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class PlanDefinitionRecord:
    id: str
    repository_id: str
    file_path: str
    slug: str
    title: str | None
    objective: str
    content_hash: str | None
    phase_name: str | None
    group_name: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class RunRecord:
    id: str
    repository_id: str
    launch_source: str
    plans_dir: str
    concurrency: int
    status: str
    started_at: str
    ended_at: str | None
    run_root_path: str


@dataclass(frozen=True)
class PlanExecutionRecord:
    id: str
    run_id: str
    plan_definition_id: str
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
class StepExecutionRecord:
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
    metadata_json: str
    changed_files_json: str


@dataclass(frozen=True)
class WorkspaceRecord:
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
class AgentProfileRecord:
    id: str
    display_name: str
    avatar_uri: str | None
    theme_key: str | None
    preset_key: str | None
    status: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AgentAssignmentRecord:
    id: str
    agent_id: str
    plan_execution_id: str
    assigned_at: str
    released_at: str | None
    status: str


def record_to_dict(record: Any) -> dict[str, Any]:
    return asdict(record)
