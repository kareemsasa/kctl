from __future__ import annotations

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .git import create_isolated_workspace, ensure_git_repo, get_repo_root, resolve_repo
from .output import ConsoleOutputSink, OutputSink
from .plan import load_plan, validate_plan
from .runner import execute_plan_run
from .terminal import style_status_text, style_text
from .types import PlanError


PLAN_FILE_PATTERNS = ("*.yaml", "*.yml")


@dataclass(frozen=True)
class PlanSpec:
    plan_id: str
    plan_path: Path
    filename: str
    repo_path: Path
    step_ids: list[str]


def sanitize_plan_id(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return normalized or "plan"


def discover_plan_files(plans_dir: Path) -> list[Path]:
    if not plans_dir.exists():
        raise PlanError(f"Plans directory does not exist: {plans_dir}")
    if not plans_dir.is_dir():
        raise PlanError(f"Plans directory is not a directory: {plans_dir}")
    plan_paths = sorted(
        {path.resolve() for pattern in PLAN_FILE_PATTERNS for path in plans_dir.glob(pattern)}
    )
    if not plan_paths:
        raise PlanError(f"No plan files found under: {plans_dir}")
    return plan_paths


def load_plan_specs(plans_dir: Path) -> list[PlanSpec]:
    plan_specs: list[PlanSpec] = []
    repo_roots: set[Path] = set()
    seen_plan_ids: set[str] = set()
    for index, plan_path in enumerate(discover_plan_files(plans_dir), start=1):
        plan = load_plan(plan_path)
        validate_plan(plan)
        target_repo = resolve_repo(plan_path, plan["repo"])
        ensure_git_repo(target_repo)
        repo_root = get_repo_root(target_repo)
        repo_roots.add(repo_root)
        plan_id = sanitize_plan_id(plan_path.stem)
        if plan_id in seen_plan_ids:
            plan_id = f"{plan_id}-{index:02d}"
        seen_plan_ids.add(plan_id)
        plan_specs.append(
            PlanSpec(
                plan_id=plan_id,
                plan_path=plan_path,
                filename=plan_path.name,
                repo_path=repo_root,
                step_ids=[step["id"] for step in plan["steps"]],
            )
        )
    if len(repo_roots) != 1:
        raise PlanError("All plans in a run-many directory must target the same git repository.")
    return plan_specs


def build_multi_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def build_branch_name(run_id: str, plan_id: str) -> str:
    return f"kctl/{run_id}/{sanitize_plan_id(plan_id)}"


def write_run_state(run_root: Path, run_data: dict[str, Any]) -> Path:
    run_root.mkdir(parents=True, exist_ok=True)
    run_path = run_root / "run.json"
    run_path.write_text(json.dumps(run_data, indent=2) + "\n")
    return run_path


def format_status_line(plan_state: dict[str, Any]) -> str:
    verify_result = plan_state.get("verify_result") or "not-run"
    return (
        f"- {plan_state['plan_id']} ({plan_state['filename']}): "
        f"step={plan_state.get('current_step') or '-'} "
        f"status={plan_state['status']} verify={verify_result}"
    )


def print_run_summary(run_data: dict[str, Any], output_sink: OutputSink) -> None:
    output_sink.write_line(style_text("Plan summary:", bold=True))
    for plan_state in run_data["plans"]:
        status = plan_state["status"]
        rendered_status = "success" if status == "passed" else "failure" if status in {"failed", "blocked"} else status
        output_sink.write_line(style_status_text(format_status_line(plan_state), rendered_status))


def run_many_plans(
    plans_dir: Path,
    concurrency: int,
    verbose: bool = False,
) -> int:
    if concurrency < 1:
        raise PlanError("--concurrency must be at least 1.")
    plan_specs = load_plan_specs(plans_dir)
    repo_root = plan_specs[0].repo_path
    run_id = build_multi_run_id()
    run_root = repo_root / ".kctl" / "runs" / run_id
    worktree_root = repo_root / ".kctl" / "worktrees" / run_id
    output_sink = ConsoleOutputSink()
    run_data: dict[str, Any] = {
        "run_id": run_id,
        "plans_dir": str(plans_dir.resolve()),
        "repo": str(repo_root),
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "concurrency": concurrency,
        "plans": [
            {
                "plan_id": spec.plan_id,
                "filename": spec.filename,
                "plan_path": str(spec.plan_path),
                "status": "pending",
                "current_step": spec.step_ids[0] if spec.step_ids else None,
                "step_statuses": {},
                "worktree_path": None,
                "branch_name": None,
                "run_output_dir": str(run_root / spec.plan_id),
                "log_path": None,
                "verify_result": "not-run",
            }
            for spec in plan_specs
        ],
    }
    state_lock = threading.Lock()
    plan_state_by_id = {plan_state["plan_id"]: plan_state for plan_state in run_data["plans"]}
    write_run_state(run_root, run_data)

    def update_plan_state(plan_id: str, **updates: Any) -> None:
        with state_lock:
            plan_state = plan_state_by_id[plan_id]
            plan_state.update(updates)
            write_run_state(run_root, run_data)

    def run_one_plan(spec: PlanSpec) -> tuple[str, int]:
        plan_output_sink = ConsoleOutputSink(prefix=f"[{spec.plan_id}] ")
        branch_name = build_branch_name(run_id, spec.plan_id)
        worktree_path = worktree_root / spec.plan_id
        update_plan_state(
            spec.plan_id,
            status="running",
            branch_name=branch_name,
            worktree_path=str(worktree_path),
        )
        create_isolated_workspace(repo_root, worktree_path, branch_name)

        def status_callback(event: dict[str, Any]) -> None:
            if event["type"] == "step_started":
                update_plan_state(
                    spec.plan_id,
                    current_step=event["step_id"],
                    step_statuses={
                        **plan_state_by_id[spec.plan_id]["step_statuses"],
                        event["step_id"]: "running",
                    },
                )
                return
            if event["type"] == "step_completed":
                step_statuses = dict(plan_state_by_id[spec.plan_id]["step_statuses"])
                step_statuses[event["step_id"]] = event["status"]
                update_plan_state(
                    spec.plan_id,
                    current_step=event["step_id"],
                    step_statuses=step_statuses,
                )

        run_data_result = execute_plan_run(
            plan_path=spec.plan_path,
            verbose=verbose,
            approve_each_step=False,
            branch=None,
            commit=False,
            commit_message=None,
            allow_dirty_start=False,
            review_enabled=False,
            repo_override=str(worktree_path),
            output_sink=plan_output_sink,
            interactive=False,
            run_output_dir_override=run_root / spec.plan_id,
            status_callback=status_callback,
        )
        final_status = run_data_result["status"]
        verify_result = "not-run"
        for step_result in reversed(run_data_result["steps"]):
            if step_result["verify"] is not None:
                verify_result = "passed" if step_result["verify"]["exit_code"] == 0 else "failed"
                break
        mapped_status = "passed" if final_status == "success" else "blocked" if final_status == "stopped" else "failed"
        update_plan_state(
            spec.plan_id,
            status=mapped_status,
            current_step=run_data_result["steps"][-1]["id"] if run_data_result["steps"] else None,
            log_path=run_data_result["log_path"],
            verify_result=verify_result,
        )
        return spec.plan_id, 0 if mapped_status == "passed" else 1

    failures = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(run_one_plan, spec): spec.plan_id for spec in plan_specs}
        for future in as_completed(futures):
            plan_id = futures[future]
            try:
                _, exit_code = future.result()
            except Exception as exc:
                update_plan_state(plan_id, status="failed")
                failures += 1
                output_sink.write_line(style_status_text(f"[{plan_id}] failed: {exc}", "failure"))
                continue
            if exit_code != 0:
                failures += 1

    run_data["ended_at"] = datetime.now(timezone.utc).isoformat()
    run_data["status"] = "failed" if failures else "passed"
    write_run_state(run_root, run_data)
    output_sink.write_line(style_text(f"Multi-plan run: {run_root}", bold=True))
    print_run_summary(run_data, output_sink)
    return 1 if failures else 0


def resolve_status_run_path(target: str) -> Path:
    target_path = Path(target).expanduser()
    if target_path.exists():
        if target_path.is_dir() and (target_path / "run.json").exists():
            return (target_path / "run.json").resolve()
        plan_specs = load_plan_specs(target_path.resolve())
        repo_root = plan_specs[0].repo_path
        run_logs = sorted((repo_root / ".kctl" / "runs").glob("*/run.json"))
        matching_logs: list[Path] = []
        for run_log in run_logs:
            data = json.loads(run_log.read_text())
            if data.get("plans_dir") == str(target_path.resolve()):
                matching_logs.append(run_log)
        if not matching_logs:
            raise PlanError(f"No saved multi-plan runs found for: {target_path.resolve()}")
        return matching_logs[-1]
    run_log = Path.cwd() / ".kctl" / "runs" / target / "run.json"
    if run_log.exists():
        return run_log.resolve()
    raise PlanError(f"Could not resolve run status target: {target}")


def print_run_status(target: str) -> int:
    run_log = resolve_status_run_path(target)
    data = json.loads(run_log.read_text())
    output_sink = ConsoleOutputSink()
    output_sink.write_line(style_text(f"Run: {run_log.parent}", bold=True))
    print_run_summary(data, output_sink)
    return 0
