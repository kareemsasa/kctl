#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


@dataclass
class CommandResult:
    command: list[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str


class PlanError(Exception):
    pass


def run_command(command: list[str], cwd: Path, stdin_text: str | None = None) -> CommandResult:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        input=stdin_text,
        text=True,
        capture_output=True,
    )
    return CommandResult(
        command=command,
        cwd=str(cwd),
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def run_streaming_command(command: list[str], cwd: Path, stdout_prefix: str = "", stderr_prefix: str = "") -> CommandResult:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def forward_stream(stream: Any, sink: Any, prefix: str, captured_chunks: list[str]) -> None:
        for line in iter(stream.readline, ""):
            captured_chunks.append(line)
            sink.write(f"{prefix}{line}" if prefix else line)
            sink.flush()
        stream.close()

    stdout_thread = threading.Thread(
        target=forward_stream,
        args=(process.stdout, sys.stdout, stdout_prefix, stdout_chunks),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=forward_stream,
        args=(process.stderr, sys.stderr, stderr_prefix, stderr_chunks),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    exit_code = process.wait()
    stdout_thread.join()
    stderr_thread.join()

    return CommandResult(
        command=command,
        cwd=str(cwd),
        exit_code=exit_code,
        stdout="".join(stdout_chunks),
        stderr="".join(stderr_chunks),
    )


def load_plan(plan_path: Path) -> dict[str, Any]:
    if not plan_path.exists():
        raise PlanError(f"Plan file does not exist: {plan_path}")

    try:
        data = yaml.safe_load(plan_path.read_text())
    except yaml.YAMLError as exc:
        raise PlanError(f"Failed to parse YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise PlanError("Plan file must contain a top-level mapping.")

    return data


def validate_plan(plan: dict[str, Any]) -> None:
    required_string_fields = ["repo", "objective"]
    for field in required_string_fields:
        value = plan.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PlanError(f"Plan field '{field}' is required and must be a non-empty string.")

    defaults = plan.get("defaults", {})
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise PlanError("Plan field 'defaults' must be a mapping if provided.")

    verify = defaults.get("verify")
    if verify is not None and not isinstance(verify, str):
        raise PlanError("defaults.verify must be a string if provided.")

    stop_on_failure = defaults.get("stop_on_failure")
    if stop_on_failure is not None and not isinstance(stop_on_failure, bool):
        raise PlanError("defaults.stop_on_failure must be a boolean if provided.")

    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        raise PlanError("Plan field 'steps' is required and must be a non-empty list.")

    step_ids: set[str] = set()
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise PlanError(f"Step #{index} must be a mapping.")

        step_id = step.get("id")
        prompt = step.get("prompt")
        if not isinstance(step_id, str) or not step_id.strip():
            raise PlanError(f"Step #{index} field 'id' is required and must be a non-empty string.")
        if step_id in step_ids:
            raise PlanError(f"Duplicate step id: {step_id}")
        step_ids.add(step_id)

        if not isinstance(prompt, str) or not prompt.strip():
            raise PlanError(f"Step '{step_id}' field 'prompt' is required and must be a non-empty string.")

        step_verify = step.get("verify")
        if step_verify is not None and not isinstance(step_verify, str):
            raise PlanError(f"Step '{step_id}' field 'verify' must be a string if provided.")

        expect_clean_diff = step.get("expect_clean_diff")
        if expect_clean_diff is not None and not isinstance(expect_clean_diff, bool):
            raise PlanError(f"Step '{step_id}' field 'expect_clean_diff' must be a boolean if provided.")


def resolve_repo(plan_path: Path, repo_value: str) -> Path:
    repo_path = Path(repo_value)
    if not repo_path.is_absolute():
        repo_path = (plan_path.parent / repo_path).resolve()
    return repo_path


def ensure_git_repo(repo_path: Path) -> None:
    if not repo_path.exists():
        raise PlanError(f"Target repo does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise PlanError(f"Target repo is not a directory: {repo_path}")

    git_check = run_command(["git", "rev-parse", "--show-toplevel"], cwd=repo_path)
    if git_check.exit_code != 0:
        message = git_check.stderr.strip() or git_check.stdout.strip() or "unknown git error"
        raise PlanError(f"Target repo is not a git repo: {repo_path} ({message})")


def get_git_status(repo_path: Path) -> CommandResult:
    return run_command(["git", "status", "--short"], cwd=repo_path)


def get_git_diff_stat(repo_path: Path) -> CommandResult:
    return run_command(["git", "diff", "--stat"], cwd=repo_path)


def parse_changed_files(status_output: str) -> list[str]:
    changed_files: list[str] = []
    for raw_line in status_output.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue

        path_text = line[3:] if len(line) > 3 else ""
        if " -> " in path_text:
            _, path_text = path_text.split(" -> ", 1)

        path_text = path_text.strip()
        if path_text:
            changed_files.append(path_text)

    return changed_files


def build_codex_prompt(objective: str, prior_summaries: list[str], step: dict[str, Any]) -> str:
    sections = [
        "You are executing one step in a larger kctl plan.",
        f"Overall objective:\n{objective.strip()}",
    ]

    if prior_summaries:
        sections.append("Prior step summaries:\n" + "\n".join(f"- {summary}" for summary in prior_summaries))
    else:
        sections.append("Prior step summaries:\n- No prior steps have run.")

    sections.append(f"Current step id: {step['id']}")
    sections.append(f"Current step prompt:\n{step['prompt'].strip()}")
    sections.append(
        "Constraints:\n"
        "- Work only in the current repository.\n"
        "- Keep changes scoped to the current step.\n"
        "- In your final response, summarize what you changed and any verification you ran."
    )
    return "\n\n".join(sections)


def extract_codex_summary(stdout: str) -> str | None:
    ignored_prefixes = (
        "OpenAI Codex ",
        "workdir:",
        "model:",
        "provider:",
        "approval:",
        "sandbox:",
        "reasoning effort:",
        "reasoning summaries:",
        "session id:",
        "mcp startup:",
        "Reconnecting...",
        "WARNING:",
        "note:",
        "thread ",
        "user",
        "assistant",
        "--------",
        "202",
    )
    for line in reversed([line.strip() for line in stdout.splitlines() if line.strip()]):
        if line.startswith(ignored_prefixes):
            continue
        return line[:200]
    return None


def summarize_step_result(step_result: dict[str, Any]) -> str:
    changed_files = ", ".join(step_result["changed_files"]) if step_result["changed_files"] else "-"
    summary = (
        f"id={step_result['id']} "
        f"status={step_result['status']} "
        f"changed_files={changed_files}"
    )
    codex_summary = step_result.get("codex_summary")
    if codex_summary:
        summary += f" summary={codex_summary}"
    return summary


def print_command_result(label: str, result: CommandResult) -> None:
    print(f"{label} exit code: {result.exit_code}", flush=True)
    if result.stdout.strip():
        print(f"{label} stdout:", flush=True)
        print(result.stdout.rstrip(), flush=True)
    if result.stderr.strip():
        print(f"{label} stderr:", file=sys.stderr, flush=True)
        print(result.stderr.rstrip(), file=sys.stderr, flush=True)


def execute_step(
    repo_path: Path,
    objective: str,
    defaults: dict[str, Any],
    step: dict[str, Any],
    prior_summaries: list[str],
) -> dict[str, Any]:
    step_id = step["id"]
    print(f"== Step {step_id} ==", flush=True)

    started_at = datetime.now(timezone.utc).isoformat()
    before_status = get_git_status(repo_path)
    codex_prompt = build_codex_prompt(objective, prior_summaries, step)
    codex_result = run_streaming_command(
        ["codex", "exec", "--full-auto", "--cd", str(repo_path), codex_prompt],
        cwd=repo_path,
        stdout_prefix="codex: ",
        stderr_prefix="codex: ",
    )
    ended_at = datetime.now(timezone.utc).isoformat()
    after_status = get_git_status(repo_path)
    diff_stat = get_git_diff_stat(repo_path)

    changed_files = parse_changed_files(after_status.stdout)
    changed_files_count = len(changed_files)
    expect_clean_diff = bool(step.get("expect_clean_diff", False))
    verify_command = step.get("verify") or defaults.get("verify")
    verify_result: CommandResult | None = None
    codex_summary = extract_codex_summary(codex_result.stdout) if codex_result.exit_code == 0 else None

    status = "success"
    failure_reason: str | None = None

    if codex_result.exit_code != 0:
        status = "failure"
        failure_reason = "codex_failed"

    if expect_clean_diff and changed_files_count > 0:
        status = "failure"
        failure_reason = "expected_clean_diff"

    if verify_command:
        verify_result = run_command(["sh", "-lc", verify_command], cwd=repo_path)
        print_command_result("verify", verify_result)
        if verify_result.exit_code != 0:
            status = "failure"
            failure_reason = "verify_failed"

    step_result = {
        "id": step_id,
        "prompt": step["prompt"],
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
        "changed_files": changed_files,
        "changed_files_count": changed_files_count,
        "codex_summary": codex_summary,
        "codex": {
            "command": codex_result.command,
            "cwd": codex_result.cwd,
            "exit_code": codex_result.exit_code,
            "stdout": codex_result.stdout,
            "stderr": codex_result.stderr,
        },
        "verify": None,
    }

    if verify_result is not None:
        step_result["verify"] = {
            "command": verify_result.command,
            "cwd": verify_result.cwd,
            "exit_code": verify_result.exit_code,
            "stdout": verify_result.stdout,
            "stderr": verify_result.stderr,
        }

    print(f"Changed files: {changed_files_count}", flush=True)
    if diff_stat.stdout.strip():
        print("git diff --stat:", flush=True)
        print(diff_stat.stdout.rstrip(), flush=True)

    return step_result


def save_run_log(run_data: dict[str, Any], script_root: Path) -> Path:
    runs_dir = script_root / ".kctl-runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = runs_dir / f"{timestamp}.json"
    log_path.write_text(json.dumps(run_data, indent=2))
    return log_path


def run_plan(plan_path: Path) -> int:
    plan = load_plan(plan_path)
    validate_plan(plan)

    repo_path = resolve_repo(plan_path, plan["repo"])
    ensure_git_repo(repo_path)

    defaults = plan.get("defaults") or {}
    stop_on_failure = bool(defaults.get("stop_on_failure", False))
    prior_summaries: list[str] = []
    step_results: list[dict[str, Any]] = []
    run_status = "success"
    started_at = datetime.now(timezone.utc).isoformat()

    for step in plan["steps"]:
        step_result = execute_step(
            repo_path=repo_path,
            objective=plan["objective"],
            defaults=defaults,
            step=step,
            prior_summaries=prior_summaries,
        )
        step_results.append(step_result)
        prior_summaries.append(summarize_step_result(step_result))

        should_stop = False
        if step_result["failure_reason"] == "expected_clean_diff":
            should_stop = True
        elif step_result["failure_reason"] == "verify_failed" and stop_on_failure:
            should_stop = True
        elif step_result["failure_reason"] == "codex_failed" and stop_on_failure:
            should_stop = True

        if step_result["status"] != "success":
            run_status = "failure"

        if should_stop:
            break

    run_data = {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "plan_path": str(plan_path.resolve()),
        "repo": str(repo_path),
        "objective": plan["objective"],
        "defaults": defaults,
        "status": run_status,
        "steps": step_results,
    }
    log_path = save_run_log(run_data, Path(__file__).resolve().parent)

    print("\nFinal summary:", flush=True)
    for step_result in step_results:
        verify_label = "not-run"
        if step_result["verify"] is not None:
            verify_label = "passed" if step_result["verify"]["exit_code"] == 0 else "failed"
        print(
            f"- {step_result['id']}: {step_result['status']}, "
            f"verify={verify_label}, changed_files={step_result['changed_files_count']}",
            flush=True,
        )
    print(f"Run log: {log_path}", flush=True)

    return 0 if run_status == "success" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kctl", description="Run Codex plans against git repositories.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a YAML plan.")
    run_parser.add_argument("plan", help="Path to the YAML plan file.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        try:
            return run_plan(Path(args.plan).resolve())
        except PlanError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
