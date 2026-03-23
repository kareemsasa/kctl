from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .git import (
    create_commit,
    detect_new_changes,
    ensure_git_repo,
    get_current_branch,
    get_git_diff_stat,
    get_git_status,
    parse_changed_files,
    parse_git_status_entries,
    resolve_repo,
    switch_to_branch,
)
from .paths import project_root
from .plan import build_codex_prompt, load_plan, validate_plan
from .process import run_command, run_streaming_command
from .review import run_step_reviews, should_print_diff_stat
from .terminal import (
    ANSI_CYAN,
    CODEX_STREAM_PREFIX,
    is_meaningful_summary_line,
    style_status_text,
    style_text,
)
from .types import CommandResult, PlanError


def extract_verify_data(verify_result: CommandResult | None) -> dict[str, Any] | None:
    if verify_result is None:
        return None
    return {
        "command": verify_result.command,
        "cwd": verify_result.cwd,
        "exit_code": verify_result.exit_code,
        "stdout": verify_result.stdout,
        "stderr": verify_result.stderr,
    }


def build_synthetic_codex_summary(
    status: str,
    changed_files: list[str],
    verify_result: CommandResult | dict[str, Any] | None,
) -> str:
    changed_files_text = ", ".join(changed_files) if changed_files else "-"
    verify_text = "not-run"
    if verify_result is not None:
        verify_exit_code = (
            verify_result.exit_code
            if isinstance(verify_result, CommandResult)
            else verify_result["exit_code"]
        )
        verify_text = "passed" if verify_exit_code == 0 else "failed"
    return f"status={status}; changed_files={changed_files_text}; verify={verify_text}"


def extract_codex_summary(
    stdout: str,
    status: str,
    changed_files: list[str],
    verify_result: CommandResult | None,
) -> str:
    for line in reversed(
        [line.strip() for line in stdout.splitlines() if line.strip()]
    ):
        if is_meaningful_summary_line(line):
            return line[:200]
    return build_synthetic_codex_summary(status, changed_files, verify_result)


def shorten_summary(text: str, limit: int = 200) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def extract_compact_step_summary(step_result: dict[str, Any]) -> str:
    codex_summary = step_result.get("codex_summary")
    if codex_summary:
        return shorten_summary(codex_summary)
    return build_synthetic_codex_summary(
        status=step_result["status"],
        changed_files=step_result["changed_files"],
        verify_result=step_result["verify"],
    )


def summarize_step_result(step_result: dict[str, Any]) -> str:
    changed_files = (
        ", ".join(step_result["changed_files"]) if step_result["changed_files"] else "-"
    )
    return (
        f"id={step_result['id']} "
        f"status={step_result['status']} "
        f"changed_files={changed_files} "
        f"summary={extract_compact_step_summary(step_result)}"
    )


def format_duration_seconds(started_at: str, ended_at: str) -> str:
    started = datetime.fromisoformat(started_at)
    ended = datetime.fromisoformat(ended_at)
    duration = max(0.0, (ended - started).total_seconds())
    return f"{duration:.1f}s"


def get_verify_label(verify_result: dict[str, Any] | None) -> str:
    if verify_result is None:
        return "skipped"
    return "passed" if verify_result["exit_code"] == 0 else "failed"


def print_step_footer(step_result: dict[str, Any]) -> None:
    footer = (
        f"Step {step_result['id']} | status={step_result['status']} | "
        f"duration={format_duration_seconds(step_result['started_at'], step_result['ended_at'])} | "
        f"verify={get_verify_label(step_result['verify'])} | "
        f"baseline_changed_files={len(step_result['baseline_changed_files'])} | "
        f"new_changed_files={len(step_result['new_changed_files'])}"
    )
    print(style_status_text(footer, step_result["status"], bold=True), flush=True)
    if 0 < len(step_result["new_changed_files"]) <= 5:
        print(f"New: {', '.join(step_result['new_changed_files'])}", flush=True)


def prompt_to_continue() -> bool:
    try:
        response = input(style_text("Continue to next step? [y/N] ", bold=True))
    except EOFError:
        return False
    return response.strip().lower() == "y"


def prompt_to_continue_after_review(
    step_id: str, reviews: list[dict[str, Any]]
) -> bool:
    concern_count = sum(1 for review in reviews if review["verdict"] == "concern")
    try:
        response = input(
            style_text(
                f"Review concerns for step {step_id} ({concern_count} concern). Continue anyway? [y/N] ",
                bold=True,
            )
        )
    except EOFError:
        return False
    return response.strip().lower() == "y"


def print_command_result(label: str, result: CommandResult) -> None:
    status = "success" if result.exit_code == 0 else "failure"
    print(
        style_status_text(f"{label} exit code: {result.exit_code}", status), flush=True
    )
    if result.stdout.strip():
        print(f"{label} stdout:", flush=True)
        print(result.stdout.rstrip(), flush=True)
    if result.stderr.strip():
        print(
            style_status_text(f"{label} stderr:", "failure", stream=sys.stderr),
            file=sys.stderr,
            flush=True,
        )
        print(result.stderr.rstrip(), file=sys.stderr, flush=True)


def print_review_summary(step_id: str, reviews: list[dict[str, Any]]) -> None:
    summary_text = ", ".join(
        f"{review['reviewer']}={review['verdict']}" for review in reviews
    )
    if any(review["verdict"] == "block" for review in reviews):
        review_status = "block"
    elif any(review["verdict"] == "concern" for review in reviews):
        review_status = "concern"
    else:
        review_status = "success"
    print(
        style_status_text(
            f"Review {step_id}: {summary_text}", review_status, bold=True
        ),
        flush=True,
    )
    for review in reviews:
        print(
            style_status_text(
                f"- {review['reviewer']}: {review['summary']}", review["verdict"]
            ),
            flush=True,
        )


def build_step_result(
    step_id: str,
    step_prompt: str,
    codex_prompt: str,
    started_at: str,
    ended_at: str,
    expect_clean_diff: bool,
    status: str,
    failure_reason: str | None,
    before_status: CommandResult,
    after_status: CommandResult,
    diff_stat: CommandResult,
    baseline_changed_files: list[str],
    new_changed_files: list[str],
    changed_files: list[str],
    codex_result: CommandResult,
    verify_result: CommandResult | None,
    reviews: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    verify_data = extract_verify_data(verify_result)
    codex_summary = extract_codex_summary(
        codex_result.stdout, status, changed_files, verify_result
    )
    return {
        "id": step_id,
        "prompt": step_prompt,
        "codex_prompt": codex_prompt,
        "started_at": started_at,
        "ended_at": ended_at,
        "expect_clean_diff": expect_clean_diff,
        "status": status,
        "failure_reason": failure_reason,
        "before_git_status": {
            "exit_code": before_status.exit_code,
            "stdout": before_status.stdout,
            "stderr": before_status.stderr,
        },
        "after_git_status": {
            "exit_code": after_status.exit_code,
            "stdout": after_status.stdout,
            "stderr": after_status.stderr,
        },
        "diff_stat": {
            "exit_code": diff_stat.exit_code,
            "stdout": diff_stat.stdout,
            "stderr": diff_stat.stderr,
        },
        "baseline_changed_files": baseline_changed_files,
        "new_changed_files": new_changed_files,
        "changed_files": changed_files,
        "changed_files_count": len(changed_files),
        "codex_summary": codex_summary,
        "codex": {
            "command": codex_result.command,
            "cwd": codex_result.cwd,
            "exit_code": codex_result.exit_code,
            "stdout": codex_result.stdout,
            "stderr": codex_result.stderr,
        },
        "verify": verify_data,
        "reviews": reviews or [],
    }


def execute_step(
    repo_path: Path,
    objective: str,
    defaults: dict[str, Any],
    step: dict[str, Any],
    prior_summaries: list[str],
    verbose: bool,
    review_enabled: bool,
) -> dict[str, Any]:
    step_id = step["id"]
    print(style_text(f"== Step {step_id} ==", color=ANSI_CYAN, bold=True), flush=True)
    started_at = datetime.now(timezone.utc).isoformat()
    before_status = get_git_status(repo_path)
    codex_prompt = build_codex_prompt(objective, prior_summaries, step)
    prompt_lines_to_hide = {
        line.strip() for line in codex_prompt.splitlines() if line.strip()
    }
    codex_result = run_streaming_command(
        ["codex", "exec", "--full-auto", "--cd", str(repo_path), codex_prompt],
        cwd=repo_path,
        stdout_prefix=CODEX_STREAM_PREFIX,
        stderr_prefix=CODEX_STREAM_PREFIX,
        filter_stream=not verbose,
        hidden_lines=prompt_lines_to_hide if not verbose else None,
    )
    ended_at = datetime.now(timezone.utc).isoformat()
    after_status = get_git_status(repo_path)
    diff_stat = get_git_diff_stat(repo_path)
    baseline_entries = parse_git_status_entries(before_status.stdout)
    after_entries = parse_git_status_entries(after_status.stdout)
    baseline_changed_files = sorted(baseline_entries)
    new_changed_files = detect_new_changes(baseline_entries, after_entries)
    changed_files = parse_changed_files(after_status.stdout)
    expect_clean_diff = bool(step.get("expect_clean_diff", False))
    verify_command = step.get("verify") or defaults.get("verify")
    verify_result: CommandResult | None = None
    reviews: list[dict[str, Any]] = []
    status = "success"
    failure_reason: str | None = None
    if codex_result.exit_code != 0:
        status = "failure"
        failure_reason = "codex_failed"
    if expect_clean_diff and new_changed_files:
        status = "failure"
        failure_reason = "expected_clean_diff"
    if verify_command:
        verify_result = run_command(["sh", "-lc", verify_command], cwd=repo_path)
        print_command_result("verify", verify_result)
        if verify_result.exit_code != 0:
            status = "failure"
            failure_reason = "verify_failed"
    if review_enabled and status == "success" and new_changed_files:
        reviews = run_step_reviews(
            repo_path=repo_path,
            objective=objective,
            step_id=step_id,
            new_changed_files=new_changed_files,
            verify_result=verify_result,
            verbose=verbose,
            print_review_summary=print_review_summary,
        )
        if any(review["verdict"] == "block" for review in reviews):
            status = "failure"
            failure_reason = "review_blocked"
        elif any(review["verdict"] == "concern" for review in reviews):
            status = "paused"
            failure_reason = "review_concern"
    step_result = build_step_result(
        step_id=step_id,
        step_prompt=step["prompt"],
        codex_prompt=codex_prompt,
        started_at=started_at,
        ended_at=ended_at,
        expect_clean_diff=expect_clean_diff,
        status=status,
        failure_reason=failure_reason,
        before_status=before_status,
        after_status=after_status,
        diff_stat=diff_stat,
        baseline_changed_files=baseline_changed_files,
        new_changed_files=new_changed_files,
        changed_files=changed_files,
        codex_result=codex_result,
        verify_result=verify_result,
        reviews=reviews,
    )
    if should_print_diff_stat(diff_stat.stdout, verbose):
        print(style_text("git diff --stat:", bold=True), flush=True)
        print(diff_stat.stdout.rstrip(), flush=True)
    print_step_footer(step_result)
    return step_result


def save_run_log(run_data: dict[str, Any], script_root: Path) -> Path:
    runs_dir = script_root / ".kctl-runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = runs_dir / f"{timestamp}.json"
    log_path.write_text(json.dumps(run_data, indent=2))
    return log_path


def run_plan(
    plan_path: Path,
    verbose: bool,
    approve_each_step: bool,
    branch: str | None,
    commit: bool,
    commit_message: str | None,
    allow_dirty_start: bool,
    review_enabled: bool,
) -> int:
    plan = load_plan(plan_path)
    validate_plan(plan)
    repo_path = resolve_repo(plan_path, plan["repo"])
    ensure_git_repo(repo_path)
    branch_before = get_current_branch(repo_path)
    repo_dirty_at_start = bool(get_git_status(repo_path).stdout.strip())
    if branch:
        switch_to_branch(repo_path, branch)
    branch_after = get_current_branch(repo_path)
    if commit and not commit_message:
        raise PlanError("--commit requires --commit-message.")
    if commit and repo_dirty_at_start and not allow_dirty_start:
        raise PlanError(
            "--commit is not allowed when the repo is already dirty. Use --allow-dirty-start to override."
        )
    if commit and branch_after in {"main", "master"}:
        raise PlanError("--commit is not allowed on main or master.")
    defaults = plan.get("defaults") or {}
    stop_on_failure = bool(defaults.get("stop_on_failure", False))
    prior_summaries: list[str] = []
    step_results: list[dict[str, Any]] = []
    run_status = "success"
    started_at = datetime.now(timezone.utc).isoformat()
    commit_created = False
    commit_sha: str | None = None
    steps = plan["steps"]
    for index, step in enumerate(steps):
        step_result = execute_step(
            repo_path=repo_path,
            objective=plan["objective"],
            defaults=defaults,
            step=step,
            prior_summaries=prior_summaries,
            verbose=verbose,
            review_enabled=review_enabled,
        )
        step_results.append(step_result)
        prior_summaries.append(summarize_step_result(step_result))
        failure_reason = step_result["failure_reason"]
        should_stop = failure_reason in {"expected_clean_diff", "review_blocked"} or (
            stop_on_failure and failure_reason in {"verify_failed", "codex_failed"}
        )
        if step_result["status"] == "paused":
            if prompt_to_continue_after_review(
                step_result["id"], step_result["reviews"]
            ):
                step_result["status"] = "success"
                step_result["failure_reason"] = None
                prior_summaries[-1] = summarize_step_result(step_result)
            else:
                run_status = "stopped"
                break
        elif step_result["status"] != "success":
            run_status = "failure"
        if should_stop:
            run_status = "failure"
            break
        has_next_step = index < len(steps) - 1
        if approve_each_step and has_next_step and not prompt_to_continue():
            run_status = "stopped"
            break
    if run_status == "success" and commit:
        final_status = get_git_status(repo_path)
        if final_status.stdout.strip():
            commit_sha = create_commit(repo_path, commit_message)
            commit_created = True
    branch_after = get_current_branch(repo_path)
    run_data = {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "plan_path": str(plan_path.resolve()),
        "repo": str(repo_path),
        "objective": plan["objective"],
        "defaults": defaults,
        "review_enabled": review_enabled,
        "repo_dirty_at_start": repo_dirty_at_start,
        "branch_before": branch_before,
        "branch_after": branch_after,
        "commit_created": commit_created,
        "commit_sha": commit_sha,
        "status": run_status,
        "steps": step_results,
    }
    log_path = save_run_log(run_data, project_root())
    print(style_text("\nFinal summary:", bold=True), flush=True)
    for step_result in step_results:
        verify_label = "not-run"
        if step_result["verify"] is not None:
            verify_label = (
                "passed" if step_result["verify"]["exit_code"] == 0 else "failed"
            )
        summary_line = (
            f"- {step_result['id']}: {step_result['status']}, "
            f"verify={verify_label}, changed_files={step_result['changed_files_count']}"
        )
        print(style_status_text(summary_line, step_result["status"]), flush=True)
    print(style_text(f"Run log: {log_path}", bold=True), flush=True)
    return 1 if run_status == "failure" else 0
