from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import discover_multi_run_logs, discover_single_run_logs, resolve_storage, ui_state_db_path
from .git import ensure_git_repo, get_current_branch, get_repo_root
from .terminal import style_status_text, style_text
from .types import PlanError
from .ui_models import (
    PlanDefinitionRecord,
    PlanExecutionRecord,
    RepositoryRecord,
    RunRecord,
    StepExecutionRecord,
    WorkspaceRecord,
    record_to_dict,
)
from .ui_store import UIStateStore


def sanitize_slug(value: str) -> str:
    lowered = value.strip().lower()
    filtered = "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in lowered)
    compact = filtered.strip("-._")
    return compact or "plan"


def default_db_path(repo_root: Path) -> Path:
    storage = resolve_storage()
    preferred_path = ui_state_db_path(repo_root, storage_mode=storage.mode)
    try:
        preferred_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return repo_root.resolve() / ".kctl" / "ui-state.db"
    if preferred_path.parent.exists() and not os.access(preferred_path.parent, os.W_OK):
        return repo_root.resolve() / ".kctl" / "ui-state.db"
    return preferred_path


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def compute_file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def derive_verify_status(step_result: dict[str, Any]) -> str:
    verify = step_result.get("verify")
    if verify is None:
        return "not_run"
    exit_code = verify.get("exit_code")
    if exit_code == 0:
        return "passed"
    return "failed"


def derive_step_kind(step_result: dict[str, Any]) -> str:
    step_type = step_result.get("step_type")
    if isinstance(step_type, dict):
        effective_type = step_type.get("effective_type")
        if effective_type in {"analyze", "change", "verify", "review"}:
            return str(effective_type)
    return "verify" if step_result["id"] == "verify" or step_result.get("verify") is not None else "agent"


def build_step_metadata(step_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "structured_artifacts": step_result.get("structured_artifacts") or {},
        "artifact_parse_error": step_result.get("artifact_parse_error"),
        "verify_environment": step_result.get("verify_environment"),
        "before_git_status": step_result.get("before_git_status"),
        "after_git_status": step_result.get("after_git_status"),
        "diff_stat": step_result.get("diff_stat"),
        "failure_reason": step_result.get("failure_reason"),
        "failure_details": step_result.get("failure_details"),
        "step_type": step_result.get("step_type"),
        "output": step_result.get("output"),
        "review_policy": step_result.get("review_policy"),
        "mode": step_result.get("mode"),
        "verify_mode": step_result.get("verify_mode"),
        "provider": step_result.get("provider"),
        "permission_mode": step_result.get("permission_mode"),
        "reviews": step_result.get("reviews") or [],
    }


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def index_repository_state(repo_path: Path, db_path: Path | None = None) -> dict[str, int]:
    ensure_git_repo(repo_path)
    repo_root = get_repo_root(repo_path)
    db_path = db_path or default_db_path(repo_root)
    store = UIStateStore(db_path)
    store.initialize()
    try:
        now = iso_now()
        repository_id = str(repo_root)
        existing_repository = RepositoryRecord(
            id=repository_id,
            name=repo_root.name,
            root_path=str(repo_root),
            default_branch=get_current_branch(repo_root),
            created_at=now,
            updated_at=now,
        )
        store.upsert("repositories", record_to_dict(existing_repository), ["id"])
        store.clear_execution_data_for_repository(repository_id)

        counts = {
            "repositories": 1,
            "plan_definitions": 0,
            "runs": 0,
            "plan_executions": 0,
            "step_executions": 0,
            "workspaces": 0,
        }

        for run_root in discover_multi_run_logs(repo_root):
            aggregate = read_json(run_root)
            run_id = str(aggregate["run_id"])
            run_record = RunRecord(
                id=run_id,
                repository_id=repository_id,
                launch_source="plans_run_many",
                plans_dir=str(aggregate.get("plans_dir") or ""),
                concurrency=int(aggregate.get("concurrency") or 1),
                status=str(aggregate.get("status") or "unknown"),
                started_at=str(aggregate.get("started_at") or now),
                ended_at=aggregate.get("ended_at"),
                run_root_path=str(run_root.parent),
            )
            store.upsert("runs", record_to_dict(run_record), ["id"])
            counts["runs"] += 1

            for plan_state in aggregate.get("plans", []):
                plan_id = str(plan_state["plan_id"])
                plan_log_path = Path(plan_state.get("log_path") or Path(plan_state["run_output_dir"]) / "run.json")
                if not plan_log_path.exists():
                    continue
                plan_run_data = read_json(plan_log_path)
                plan_path = Path(plan_run_data["plan_path"])
                plan_definition_id = f"{repository_id}:{plan_path.resolve()}"
                plan_record = PlanDefinitionRecord(
                    id=plan_definition_id,
                    repository_id=repository_id,
                    file_path=str(plan_path.resolve()),
                    slug=sanitize_slug(plan_path.stem),
                    title=None,
                    objective=str(plan_run_data.get("objective") or ""),
                    content_hash=compute_file_hash(plan_path),
                    phase_name=None,
                    group_name=None,
                    created_at=now,
                    updated_at=now,
                )
                store.upsert("plan_definitions", record_to_dict(plan_record), ["id"])
                counts["plan_definitions"] += 1

                plan_execution_id = f"{run_id}:{plan_id}"
                step_results = list(plan_run_data.get("steps") or [])
                plan_execution = PlanExecutionRecord(
                    id=plan_execution_id,
                    run_id=run_id,
                    plan_definition_id=plan_definition_id,
                    status=str(plan_state.get("status") or plan_run_data.get("status") or "unknown"),
                    current_step_key=plan_state.get("current_step"),
                    verify_status=str(plan_state.get("verify_result") or "not_run"),
                    started_at=str(plan_run_data.get("started_at") or now),
                    ended_at=plan_run_data.get("ended_at"),
                    worktree_path=plan_state.get("worktree_path"),
                    branch_name=plan_state.get("branch_name") or plan_run_data.get("branch_after"),
                    log_path=str(plan_log_path),
                    changed_files_count=sum(int(step.get("changed_files_count") or 0) for step in step_results),
                    failure_reason=next(
                        (step.get("failure_reason") for step in reversed(step_results) if step.get("failure_reason")),
                        None,
                    ),
                )
                store.upsert("plan_executions", record_to_dict(plan_execution), ["id"])
                counts["plan_executions"] += 1

                if plan_state.get("worktree_path"):
                    workspace_record = WorkspaceRecord(
                        id=f"{run_id}:{plan_id}",
                        repository_id=repository_id,
                        plan_execution_id=plan_execution_id,
                        path=str(plan_state["worktree_path"]),
                        branch_name=plan_state.get("branch_name"),
                        base_ref="HEAD",
                        status="active" if plan_state.get("status") == "running" else "ready",
                        created_at=str(plan_run_data.get("started_at") or now),
                        released_at=plan_run_data.get("ended_at"),
                    )
                    store.upsert("workspaces", record_to_dict(workspace_record), ["id"])
                    counts["workspaces"] += 1

                for index, step_result in enumerate(step_results, start=1):
                    structured_artifacts = step_result.get("structured_artifacts") or {}
                    artifact_path = None
                    if structured_artifacts:
                        artifact_path = sorted(structured_artifacts.values())[0]
                    metadata = build_step_metadata(step_result)
                    started_at = str(step_result.get("started_at") or now)
                    ended_at = step_result.get("ended_at")
                    duration_ms = None
                    if ended_at:
                        started_dt = datetime.fromisoformat(started_at)
                        ended_dt = datetime.fromisoformat(str(ended_at))
                        duration_ms = int(max(0.0, (ended_dt - started_dt).total_seconds()) * 1000)
                    step_name = step_result["id"].replace("-", " ").title()
                    step_kind = derive_step_kind(step_result)
                    step_record = StepExecutionRecord(
                        id=f"{plan_execution_id}:{index:02d}",
                        plan_execution_id=plan_execution_id,
                        step_key=str(step_result["id"]),
                        step_name=step_name,
                        kind=step_kind,
                        sequence_index=index,
                        status=str(step_result.get("status") or "unknown"),
                        verify_status=derive_verify_status(step_result),
                        started_at=started_at,
                        ended_at=str(ended_at) if ended_at is not None else None,
                        duration_ms=duration_ms,
                        output_path=step_result.get("raw_artifact_path"),
                        artifact_path=artifact_path,
                        verify_exit_code=(step_result.get("verify") or {}).get("exit_code"),
                        changed_files_count=int(step_result.get("changed_files_count") or 0),
                        metadata_json=json.dumps(metadata, sort_keys=True),
                        changed_files_json=json.dumps(step_result.get("changed_files") or []),
                    )
                    store.upsert("step_executions", record_to_dict(step_record), ["id"])
                    counts["step_executions"] += 1

        for legacy_run_log in discover_single_run_logs(repo_root):
            legacy_data = read_json(legacy_run_log)
            run_id = f"single:{legacy_run_log.parent.name}"
            plan_path = Path(legacy_data["plan_path"])
            plan_definition_id = f"{repository_id}:{plan_path.resolve()}"
            plan_record = PlanDefinitionRecord(
                id=plan_definition_id,
                repository_id=repository_id,
                file_path=str(plan_path.resolve()),
                slug=sanitize_slug(plan_path.stem),
                title=None,
                objective=str(legacy_data.get("objective") or ""),
                content_hash=compute_file_hash(plan_path),
                phase_name=None,
                group_name=None,
                created_at=now,
                updated_at=now,
            )
            store.upsert("plan_definitions", record_to_dict(plan_record), ["id"])
            counts["plan_definitions"] += 1

            run_record = RunRecord(
                id=run_id,
                repository_id=repository_id,
                launch_source="single_run",
                plans_dir=str(plan_path.parent),
                concurrency=1,
                status=str(legacy_data.get("status") or "unknown"),
                started_at=str(legacy_data.get("started_at") or now),
                ended_at=legacy_data.get("ended_at"),
                run_root_path=str(legacy_run_log.parent),
            )
            store.upsert("runs", record_to_dict(run_record), ["id"])
            counts["runs"] += 1

            plan_execution_id = f"{run_id}:{sanitize_slug(plan_path.stem)}"
            step_results = list(legacy_data.get("steps") or [])
            plan_execution = PlanExecutionRecord(
                id=plan_execution_id,
                run_id=run_id,
                plan_definition_id=plan_definition_id,
                status=str(legacy_data.get("status") or "unknown"),
                current_step_key=step_results[-1]["id"] if step_results else None,
                verify_status=next((derive_verify_status(step) for step in reversed(step_results) if step.get("verify") is not None), "not_run"),
                started_at=str(legacy_data.get("started_at") or now),
                ended_at=legacy_data.get("ended_at"),
                worktree_path=None,
                branch_name=legacy_data.get("branch_after"),
                log_path=str(legacy_run_log),
                changed_files_count=sum(int(step.get("changed_files_count") or 0) for step in step_results),
                failure_reason=next((step.get("failure_reason") for step in reversed(step_results) if step.get("failure_reason")), None),
            )
            store.upsert("plan_executions", record_to_dict(plan_execution), ["id"])
            counts["plan_executions"] += 1

            for index, step_result in enumerate(step_results, start=1):
                structured_artifacts = step_result.get("structured_artifacts") or {}
                artifact_path = sorted(structured_artifacts.values())[0] if structured_artifacts else None
                metadata = build_step_metadata(step_result)
                started_at = str(step_result.get("started_at") or now)
                ended_at = step_result.get("ended_at")
                duration_ms = None
                if ended_at:
                    started_dt = datetime.fromisoformat(started_at)
                    ended_dt = datetime.fromisoformat(str(ended_at))
                    duration_ms = int(max(0.0, (ended_dt - started_dt).total_seconds()) * 1000)
                step_record = StepExecutionRecord(
                    id=f"{plan_execution_id}:{index:02d}",
                    plan_execution_id=plan_execution_id,
                    step_key=str(step_result["id"]),
                    step_name=str(step_result["id"]).replace("-", " ").title(),
                        kind=derive_step_kind(step_result),
                    sequence_index=index,
                    status=str(step_result.get("status") or "unknown"),
                    verify_status=derive_verify_status(step_result),
                    started_at=started_at,
                    ended_at=str(ended_at) if ended_at is not None else None,
                    duration_ms=duration_ms,
                    output_path=step_result.get("raw_artifact_path"),
                    artifact_path=artifact_path,
                    verify_exit_code=(step_result.get("verify") or {}).get("exit_code"),
                    changed_files_count=int(step_result.get("changed_files_count") or 0),
                    metadata_json=json.dumps(metadata, sort_keys=True),
                    changed_files_json=json.dumps(step_result.get("changed_files") or []),
                )
                store.upsert("step_executions", record_to_dict(step_record), ["id"])
                counts["step_executions"] += 1
        store.commit()
        return counts
    finally:
        store.close()


def print_ui_runs(repo_path: Path, db_path: Path | None = None) -> int:
    repo_root = get_repo_root(repo_path)
    repository_id = str(repo_root)
    db_path = db_path or default_db_path(repo_root)
    if not db_path.exists():
        raise PlanError(f"UI state database does not exist: {db_path}. Run `kctl ui index {repo_root}` first.")
    store = UIStateStore(db_path)
    try:
        rows = store.list_runs(repository_id)
    finally:
        store.close()
    print(style_text(f"Runs ({repo_root.name})", bold=True), flush=True)
    if not rows:
        print("  (none)", flush=True)
        return 0
    for row in rows:
        plans = row["plan_execution_count"]
        status_str = "success" if row["status"] == "passed" else "failure" if row["status"] == "failed" else row["status"]
        print(
            style_status_text(
                f"  {row['id']}  {row['status']}  {plans} plans  {row['started_at']}",
                status_str,
            ),
            flush=True,
        )
    return 0


def print_ui_run_detail(repo_path: Path, run_id: str, db_path: Path | None = None) -> int:
    repo_root = get_repo_root(repo_path)
    db_path = db_path or default_db_path(repo_root)
    if not db_path.exists():
        raise PlanError(f"UI state database does not exist: {db_path}. Run `kctl ui index {repo_root}` first.")
    store = UIStateStore(db_path)
    try:
        run_row = store.get_run(run_id)
        if run_row is None:
            raise PlanError(f"Indexed run not found: {run_id}")
        plan_rows = store.list_plan_executions_for_run(run_id)
        step_rows_by_plan = {
            plan_row["id"]: store.list_step_executions_for_plan_execution(plan_row["id"])
            for plan_row in plan_rows
        }
    finally:
        store.close()
    print(style_text(f"Run {run_id}", bold=True), flush=True)
    print(
        style_text(
            f"  {run_row['status']}  concurrency={run_row['concurrency']}  {run_row['run_root_path']}",
            dim=True,
        ),
        flush=True,
    )
    for plan_row in plan_rows:
        branch = plan_row["branch_name"] or "-"
        status_str = "success" if plan_row["status"] == "passed" else "failure" if plan_row["status"] == "failed" else plan_row["status"]
        print(
            style_status_text(
                f"  {plan_row['slug']}: {plan_row['status']}  "
                f"step={plan_row['current_step_key']}  verify={plan_row['verify_status']}  "
                f"branch={branch}",
                status_str,
            ),
            flush=True,
        )
        for step_row in step_rows_by_plan[plan_row["id"]]:
            files_count = step_row["changed_files_count"]
            files_part = f"  {files_count} files" if files_count else ""
            print(
                style_text(
                    f"    {step_row['sequence_index']}. {step_row['step_key']} ({step_row['kind']}): "
                    f"{step_row['status']}  verify={step_row['verify_status']}{files_part}",
                    dim=True,
                ),
                flush=True,
            )
    return 0


def print_ui_workspaces(repo_path: Path, db_path: Path | None = None) -> int:
    from .ui_read import list_workspaces

    repo_root = get_repo_root(repo_path)
    workspaces = list_workspaces(repo_root, db_path=db_path)
    print(style_text(f"Workspaces ({repo_root.name})", bold=True), flush=True)
    if not workspaces:
        print("  (none)", flush=True)
        return 0
    for workspace in workspaces:
        lifecycle = workspace.lifecycle
        step = workspace.current_step_key or "-"
        lifecycle_status = "success" if lifecycle == "released" else "running" if lifecycle == "active" else "failure"
        print(
            style_status_text(
                f"  {workspace.plan_slug}: {workspace.lifecycle}  "
                f"status={workspace.status}  step={step}  verify={workspace.verify_status}",
                lifecycle_status,
            ),
            flush=True,
        )
        print(style_text(f"    {workspace.path}", dim=True), flush=True)
    return 0
