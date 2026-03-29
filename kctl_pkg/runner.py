from __future__ import annotations

import json
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .artifacts import resolve_storage_mode, single_run_dir
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
from .output import ConsoleOutputSink, OutputSink
from .plan import build_codex_prompt, get_step_kind, load_plan, normalize_plan, validate_plan
from .process import run_command, run_streaming_command
from .review import run_step_reviews, should_print_diff_stat
from .terminal import (
    ANSI_CYAN,
    CODEX_STREAM_PREFIX,
    is_meaningful_summary_line,
    style_status_text,
    style_text,
)
from .types import (
    CommandResult,
    PlanError,
    VerifyArtifact,
    VerifyCommandArtifact,
    VerifyIssueArtifact,
    VerifyTestArtifact,
    artifact_to_dict,
    parse_inspect_artifact,
    parse_plan_artifact,
)


FENCED_JSON_PATTERN = re.compile(r"```json\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def extract_verify_data(
    verify_result: CommandResult | None,
    verify_environment: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if verify_result is None:
        return None
    data = {
        "command": verify_result.command,
        "cwd": verify_result.cwd,
        "exit_code": verify_result.exit_code,
        "stdout": verify_result.stdout,
        "stderr": verify_result.stderr,
    }
    if verify_environment is not None:
        data["environment"] = verify_environment
    return data


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


def print_step_footer(step_result: dict[str, Any], output_sink: OutputSink) -> None:
    footer = (
        f"Step {step_result['id']} | status={step_result['status']} | "
        f"duration={format_duration_seconds(step_result['started_at'], step_result['ended_at'])} | "
        f"verify={get_verify_label(step_result['verify'])} | "
        f"baseline_changed_files={len(step_result['baseline_changed_files'])} | "
        f"new_changed_files={len(step_result['new_changed_files'])}"
    )
    output_sink.write_line(style_status_text(footer, step_result["status"], bold=True))
    if 0 < len(step_result["new_changed_files"]) <= 5:
        output_sink.write_line(f"New: {', '.join(step_result['new_changed_files'])}")
    artifact_error = step_result.get("artifact_parse_error")
    if artifact_error:
        output_sink.write_line(style_status_text(f"Artifact error: {artifact_error}", "failure"))


def prompt_to_continue(interactive: bool) -> bool:
    if not interactive:
        return False
    try:
        response = input(style_text("Continue to next step? [y/N] ", bold=True))
    except EOFError:
        return False
    return response.strip().lower() == "y"


def prompt_to_continue_after_review(
    step_id: str, reviews: list[dict[str, Any]], interactive: bool
) -> bool:
    if not interactive:
        return False
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


def print_command_result(label: str, result: CommandResult, output_sink: OutputSink) -> None:
    status = "success" if result.exit_code == 0 else "failure"
    output_sink.write_line(style_status_text(f"{label} exit code: {result.exit_code}", status))
    if result.stdout.strip():
        output_sink.write_line(f"{label} stdout:")
        output_sink.write_line(result.stdout.rstrip())
    if result.stderr.strip():
        output_sink.write_line(
            style_status_text(f"{label} stderr:", "failure", stream=sys.stderr),
            stream="stderr",
        )
        output_sink.write_line(result.stderr.rstrip(), stream="stderr")


def parse_verify_shell(verify_shell: str | None) -> list[str]:
    if verify_shell is None:
        return ["sh", "-lc"]
    shell_parts = shlex.split(verify_shell)
    if not shell_parts:
        raise PlanError("verify_shell must not be empty.")
    return shell_parts


def run_shell_command(
    shell_parts: list[str],
    command_text: str,
    cwd: Path,
) -> CommandResult:
    return run_command([*shell_parts, command_text], cwd=cwd)


def probe_command(shell_parts: list[str], cwd: Path, command_text: str) -> str | None:
    result = run_shell_command(shell_parts, command_text, cwd)
    if result.exit_code != 0:
        return None
    output = result.stdout.strip()
    return output or None


def collect_verify_environment(shell_parts: list[str], repo_path: Path) -> dict[str, Any]:
    return {
        "cwd": str(repo_path),
        "shell": " ".join(shell_parts),
        "which_node": probe_command(shell_parts, repo_path, "command -v node"),
        "node_version": probe_command(shell_parts, repo_path, "node -v"),
        "which_npm": probe_command(shell_parts, repo_path, "command -v npm"),
        "npm_version": probe_command(shell_parts, repo_path, "npm -v"),
    }


def summarize_verify_environment(verify_environment: dict[str, Any]) -> str:
    return (
        f"shell={verify_environment['shell']}; "
        f"node={verify_environment.get('which_node') or 'not-found'}; "
        f"node_version={verify_environment.get('node_version') or 'unknown'}; "
        f"npm={verify_environment.get('which_npm') or 'not-found'}; "
        f"npm_version={verify_environment.get('npm_version') or 'unknown'}"
    )


def run_verify_commands(
    repo_path: Path,
    shell_parts: list[str],
    commands: list[str],
    output_sink: OutputSink,
) -> list[CommandResult]:
    results: list[CommandResult] = []
    for index, command in enumerate(commands, start=1):
        result = run_shell_command(shell_parts, command, repo_path)
        label = "verify" if len(commands) == 1 else f"verify[{index}]"
        print_command_result(label, result, output_sink)
        results.append(result)
        if result.exit_code != 0:
            break
    return results


def combine_verify_results(
    results: list[CommandResult],
    shell_parts: list[str] | None = None,
) -> CommandResult | None:
    if not results:
        return None
    if len(results) == 1:
        return results[0]
    if shell_parts is None:
        shell_parts = ["sh", "-lc"]
    return CommandResult(
        command=[*shell_parts, " && ".join("(%s)" % result.command[-1] for result in results)],
        cwd=results[0].cwd,
        exit_code=next(
            (result.exit_code for result in results if result.exit_code != 0),
            0,
        ),
        stdout="\n\n".join(result.stdout.rstrip() for result in results if result.stdout.strip()),
        stderr="\n\n".join(result.stderr.rstrip() for result in results if result.stderr.strip()),
    )


def print_review_summary(step_id: str, reviews: list[dict[str, Any]], output_sink: OutputSink) -> None:
    summary_text = ", ".join(
        f"{review['reviewer']}={review['verdict']}" for review in reviews
    )
    if any(review["verdict"] == "block" for review in reviews):
        review_status = "block"
    elif any(review["verdict"] == "concern" for review in reviews):
        review_status = "concern"
    else:
        review_status = "success"
    output_sink.write_line(
        style_status_text(
            f"Review {step_id}: {summary_text}", review_status, bold=True
        )
    )
    for review in reviews:
        output_sink.write_line(
            style_status_text(
                f"- {review['reviewer']}: {review['summary']}", review["verdict"]
            )
        )


def ensure_run_output_dir(repo_root: Path, run_id: str, storage_mode: str | None = None) -> Path:
    run_dir = single_run_dir(repo_root, run_id, storage_mode=storage_mode)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def build_step_file_prefix(step_index: int) -> str:
    return f"step-{step_index:02d}"


def write_raw_output_artifact(
    run_output_dir: Path,
    step_index: int,
    step_id: str,
    codex_result: CommandResult,
) -> Path:
    raw_path = run_output_dir / f"{build_step_file_prefix(step_index)}-raw.md"
    content = "\n".join(
        [
            f"# Step {step_index:02d} Raw Output",
            "",
            f"- Step id: `{step_id}`",
            f"- Codex exit code: `{codex_result.exit_code}`",
            "",
            "## stdout",
            "",
            "```text",
            codex_result.stdout.rstrip(),
            "```",
            "",
            "## stderr",
            "",
            "```text",
            codex_result.stderr.rstrip(),
            "```",
            "",
        ]
    )
    raw_path.write_text(content)
    return raw_path


def extract_last_fenced_json_block(output_text: str) -> str:
    matches = FENCED_JSON_PATTERN.findall(output_text)
    if not matches:
        raise PlanError("Expected a final fenced JSON block in Codex output.")
    return matches[-1].strip()


def parse_structured_artifact(schema_name: str, output_text: str) -> dict[str, Any]:
    artifact_text = extract_last_fenced_json_block(output_text)
    try:
        data = json.loads(artifact_text)
    except json.JSONDecodeError as exc:
        raise PlanError(f"Failed to parse {schema_name} artifact JSON: {exc}") from exc
    if schema_name == "inspect_v1":
        return artifact_to_dict(parse_inspect_artifact(data))
    if schema_name == "plan_v1":
        return artifact_to_dict(parse_plan_artifact(data))
    raise PlanError(f"No structured artifact parser is defined for schema '{schema_name}'.")


def write_structured_artifact(
    run_output_dir: Path,
    step_index: int,
    artifact_kind: str,
    artifact_data: dict[str, Any],
) -> Path:
    artifact_path = run_output_dir / f"{build_step_file_prefix(step_index)}-{artifact_kind}.json"
    artifact_path.write_text(json.dumps(artifact_data, indent=2) + "\n")
    return artifact_path


def load_structured_artifact(artifact_path: str) -> dict[str, Any]:
    return json.loads(Path(artifact_path).read_text())


def get_effective_step_type(step: dict[str, Any]) -> str:
    step_type_info = step.get("_kctl_step_type")
    if isinstance(step_type_info, dict):
        effective_type = step_type_info.get("effective_type")
        if isinstance(effective_type, str) and effective_type.strip():
            return effective_type
    return "verify" if get_step_kind(step) == "verify" else "change"


def get_effective_output_info(step: dict[str, Any]) -> dict[str, Any] | None:
    output_info = step.get("_kctl_output")
    if isinstance(output_info, dict):
        return output_info
    return None


def get_effective_review_info(step: dict[str, Any]) -> dict[str, Any] | None:
    review_info = step.get("_kctl_review")
    if isinstance(review_info, dict):
        return review_info
    return None


def get_effective_mode_info(step: dict[str, Any]) -> dict[str, Any] | None:
    mode_info = step.get("_kctl_mode")
    if isinstance(mode_info, dict):
        return mode_info
    return None


def get_effective_verify_info(step: dict[str, Any]) -> dict[str, Any] | None:
    verify_info = step.get("_kctl_verify")
    if isinstance(verify_info, dict):
        return verify_info
    return None


def summarize_command_output(result: CommandResult) -> str:
    chunks: list[str] = [f"exit_code={result.exit_code}"]
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if stdout:
        first_line = next((line.strip() for line in stdout.splitlines() if line.strip()), "")
        if first_line:
            chunks.append(f"stdout={first_line[:160]}")
    if stderr:
        first_line = next((line.strip() for line in stderr.splitlines() if line.strip()), "")
        if first_line:
            chunks.append(f"stderr={first_line[:160]}")
    return "; ".join(chunks)


def build_verify_artifact(
    verify_results: list[CommandResult],
    plan_artifact: dict[str, Any] | None,
    verify_environment: dict[str, Any] | None,
) -> dict[str, Any]:
    commands_run: list[VerifyCommandArtifact] = []
    tests: list[VerifyTestArtifact] = []
    issues: list[VerifyIssueArtifact] = []

    if verify_results:
        for index, verify_result in enumerate(verify_results, start=1):
            commands_run.append(
                VerifyCommandArtifact(
                    command=verify_result.command[-1],
                    exit_code=verify_result.exit_code,
                    summary=summarize_command_output(verify_result),
                )
            )
            tests.append(
                VerifyTestArtifact(
                    name=f"verification command {index}",
                    result="pass" if verify_result.exit_code == 0 else "fail",
                )
            )
        if all(result.exit_code == 0 for result in verify_results):
            issues.append(
                VerifyIssueArtifact(
                    severity="info",
                    summary="Configured verification commands completed successfully.",
                )
            )
            status = "pass"
            recommended_next_action = "stop"
        else:
            issues.append(
                VerifyIssueArtifact(
                    severity="error",
                    summary="At least one configured verification command failed.",
                )
            )
            status = "fail"
            recommended_next_action = "repair"
    else:
        status = "partial"
        recommended_next_action = "manual_review"
        issues.append(
            VerifyIssueArtifact(
                severity="warning",
                summary="No verification command was configured for this verify step.",
            )
        )

    if verify_environment is not None:
        issues.append(
            VerifyIssueArtifact(
                severity="info",
                summary="Verification environment: "
                + summarize_verify_environment(verify_environment),
            )
        )

    if plan_artifact:
        verification = plan_artifact.get("verification", {})
        manual_checks = verification.get("manual_checks", [])
        for check in manual_checks:
            tests.append(VerifyTestArtifact(name=check, result="skipped"))

    verify_artifact = VerifyArtifact(
        status=status,
        commands_run=commands_run,
        tests=tests,
        issues=issues,
        recommended_next_action=recommended_next_action,
    )
    return artifact_to_dict(verify_artifact)


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
    raw_artifact_path: Path | None,
    structured_artifacts: dict[str, str],
    artifact_parse_error: str | None,
    verify_environment: dict[str, Any] | None,
    step_type_info: dict[str, Any] | None,
    output_info: dict[str, Any] | None,
    review_info: dict[str, Any] | None,
    mode_info: dict[str, Any] | None,
    verify_info: dict[str, Any] | None,
) -> dict[str, Any]:
    verify_data = extract_verify_data(verify_result, verify_environment)
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
        "verify_environment": verify_environment,
        "reviews": reviews or [],
        "raw_artifact_path": str(raw_artifact_path) if raw_artifact_path else None,
        "structured_artifacts": structured_artifacts,
        "artifact_parse_error": artifact_parse_error,
        "step_type": step_type_info,
        "output": output_info,
        "review_policy": review_info,
        "mode": mode_info,
        "verify_mode": verify_info,
    }


def execute_agent_step(
    repo_path: Path,
    objective: str,
    prior_summaries: list[str],
    step: dict[str, Any],
    prior_artifacts: dict[str, dict[str, Any]],
    verbose: bool,
    output_sink: OutputSink,
) -> tuple[str, CommandResult]:
    codex_prompt = build_codex_prompt(
        objective,
        prior_summaries,
        step,
        prior_artifacts=prior_artifacts,
    )
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
        output_sink=output_sink,
    )
    return codex_prompt, codex_result


def resolve_verify_commands(
    step: dict[str, Any],
    defaults: dict[str, Any],
    prior_artifacts: dict[str, dict[str, Any]],
    effective_step_type: str,
) -> list[str]:
    explicit_verify_command = step.get("verify") or defaults.get("verify")
    step_commands = step.get("commands")
    step_id = step["id"]

    if effective_step_type == "verify":
        if isinstance(step_commands, list) and all(isinstance(item, str) for item in step_commands):
            return [item for item in step_commands if item.strip()]
        if explicit_verify_command:
            return [explicit_verify_command]
        if step_id == "verify":
            plan_verification = prior_artifacts.get("plan", {}).get("verification", {})
            plan_commands = plan_verification.get("commands", [])
            if isinstance(plan_commands, list) and all(isinstance(item, str) for item in plan_commands):
                return [item for item in plan_commands if item.strip()]
        return []

    if explicit_verify_command:
        return [explicit_verify_command]
    if step_id == "verify":
        plan_verification = prior_artifacts.get("plan", {}).get("verification", {})
        plan_commands = plan_verification.get("commands", [])
        if isinstance(plan_commands, list) and all(isinstance(item, str) for item in plan_commands):
            return [item for item in plan_commands if item.strip()]
    return []


def maybe_collect_phase_artifact(
    step: dict[str, Any],
    effective_step_type: str,
    codex_result: CommandResult,
    run_output_dir: Path,
    step_index: int,
) -> tuple[dict[str, str], dict[str, dict[str, Any]], str | None, str | None]:
    output_info = get_effective_output_info(step)
    structured_artifacts: dict[str, str] = {}
    next_artifacts: dict[str, dict[str, Any]] = {}
    artifact_parse_error: str | None = None
    failure_reason: str | None = None

    if effective_step_type not in {"analyze", "change", "review"}:
        return structured_artifacts, next_artifacts, artifact_parse_error, failure_reason
    if codex_result.exit_code != 0:
        return structured_artifacts, next_artifacts, artifact_parse_error, failure_reason
    if not isinstance(output_info, dict):
        return structured_artifacts, next_artifacts, artifact_parse_error, failure_reason
    schema_name = output_info.get("effective_schema")
    if not isinstance(schema_name, str) or not schema_name.strip():
        return structured_artifacts, next_artifacts, artifact_parse_error, failure_reason

    try:
        artifact_data = parse_structured_artifact(schema_name, codex_result.stdout)
        artifact_path = write_structured_artifact(
            run_output_dir=run_output_dir,
            step_index=step_index,
            artifact_kind=schema_name.removesuffix("_v1"),
            artifact_data=artifact_data,
        )
        structured_artifacts[schema_name] = str(artifact_path)
        next_artifacts[schema_name.removesuffix("_v1")] = load_structured_artifact(str(artifact_path))
    except PlanError as exc:
        artifact_parse_error = str(exc)
        failure_reason = "artifact_parse_failed"
    return structured_artifacts, next_artifacts, artifact_parse_error, failure_reason


def maybe_build_verify_artifact(
    step: dict[str, Any],
    effective_step_type: str,
    run_output_dir: Path,
    step_index: int,
    verify_results: list[CommandResult],
    prior_artifacts: dict[str, dict[str, Any]],
    verify_environment: dict[str, Any] | None,
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    structured_artifacts: dict[str, str] = {}
    next_artifacts: dict[str, dict[str, Any]] = {}

    if effective_step_type != "verify":
        return structured_artifacts, next_artifacts
    if step["id"] != "verify":
        return structured_artifacts, next_artifacts

    verify_artifact_data = build_verify_artifact(
        verify_results=verify_results,
        plan_artifact=prior_artifacts.get("plan"),
        verify_environment=verify_environment,
    )
    verify_artifact_path = write_structured_artifact(
        run_output_dir=run_output_dir,
        step_index=step_index,
        artifact_kind="verify",
        artifact_data=verify_artifact_data,
    )
    structured_artifacts["verify"] = str(verify_artifact_path)
    next_artifacts["verify"] = load_structured_artifact(str(verify_artifact_path))
    return structured_artifacts, next_artifacts


def should_run_reviews(
    review_enabled: bool,
    effective_review_info: dict[str, Any] | None,
) -> bool:
    if not review_enabled:
        return False
    if not isinstance(effective_review_info, dict):
        return True
    effective_policy = effective_review_info.get("effective_policy")
    return isinstance(effective_policy, str) and bool(effective_policy.strip())


def apply_review_policy(
    reviews: list[dict[str, Any]],
    effective_review_info: dict[str, Any] | None,
) -> tuple[str | None, str | None]:
    if not reviews:
        return None, None
    effective_policy = None
    if isinstance(effective_review_info, dict):
        effective_policy = effective_review_info.get("effective_policy")

    if effective_policy == "advisory":
        return "success", None
    if effective_policy == "blocking":
        if any(review["verdict"] == "block" for review in reviews):
            return "failure", "review_blocked"
        if any(review["verdict"] == "concern" for review in reviews):
            return "failure", "review_concern"
        return None, None
    if effective_policy == "manual":
        if any(review["verdict"] in {"block", "concern"} for review in reviews):
            return "paused", "review_manual"
        return None, None

    if any(review["verdict"] == "block" for review in reviews):
        return "failure", "review_blocked"
    if any(review["verdict"] == "concern" for review in reviews):
        return "paused", "review_concern"
    return None, None


def execute_step(
    repo_path: Path,
    objective: str,
    defaults: dict[str, Any],
    step: dict[str, Any],
    step_index: int,
    prior_summaries: list[str],
    prior_artifacts: dict[str, dict[str, Any]],
    run_output_dir: Path,
    verbose: bool,
    review_enabled: bool,
    output_sink: OutputSink,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    step_id = step["id"]
    effective_step_type = get_effective_step_type(step)
    step_type_info = step.get("_kctl_step_type")
    output_info = get_effective_output_info(step)
    review_info = get_effective_review_info(step)
    mode_info = get_effective_mode_info(step)
    verify_info = get_effective_verify_info(step)
    output_sink.write_line(style_text(f"== Step {step_id} ==", color=ANSI_CYAN, bold=True))
    if isinstance(step_type_info, dict):
        declared_type = step_type_info.get("declared_type") or "-"
        output_sink.write_line(
            style_text(
                "step type: "
                f"effective={step_type_info.get('effective_type')} "
                f"source={step_type_info.get('source')} "
                f"declared={declared_type} "
                f"inferred={step_type_info.get('inferred_type')}",
                dim=True,
            )
        )
    if isinstance(output_info, dict) and output_info.get("effective_schema"):
        declared_schema = output_info.get("declared_schema") or "-"
        output_sink.write_line(
            style_text(
                "output schema: "
                f"effective={output_info.get('effective_schema')} "
                f"source={output_info.get('source')} "
                f"declared={declared_schema} "
                f"inferred={output_info.get('inferred_schema') or '-'}",
                dim=True,
            )
        )
    if isinstance(review_info, dict) and review_info.get("effective_policy"):
        declared_policy = review_info.get("declared_policy") or "-"
        output_sink.write_line(
            style_text(
                "review policy: "
                f"effective={review_info.get('effective_policy')} "
                f"source={review_info.get('source')} "
                f"declared={declared_policy} "
                f"inferred={review_info.get('inferred_policy') or '-'}",
                dim=True,
            )
        )
    if isinstance(mode_info, dict):
        declared_mode = mode_info.get("declared_mode") or "-"
        output_sink.write_line(
            style_text(
                "step mode: "
                f"effective={mode_info.get('effective_mode')} "
                f"source={mode_info.get('source')} "
                f"declared={declared_mode} "
                f"inferred={mode_info.get('inferred_mode')}",
                dim=True,
            )
        )
    if isinstance(verify_info, dict):
        declared_verify_mode = verify_info.get("declared_mode") or "-"
        default_verify_mode = verify_info.get("default_mode") or "-"
        output_sink.write_line(
            style_text(
                "verify mode: "
                f"effective={verify_info.get('effective_mode')} "
                f"source={verify_info.get('source')} "
                f"declared={declared_verify_mode} "
                f"default={default_verify_mode} "
                f"inferred={verify_info.get('inferred_mode')}",
                dim=True,
            )
        )
    started_at = datetime.now(timezone.utc).isoformat()
    before_status = get_git_status(repo_path)
    codex_prompt = ""
    if effective_step_type in {"analyze", "change", "review"}:
        codex_prompt, codex_result = execute_agent_step(
            repo_path=repo_path,
            objective=objective,
            prior_summaries=prior_summaries,
            step=step,
            prior_artifacts=prior_artifacts,
            verbose=verbose,
            output_sink=output_sink,
        )
    elif effective_step_type == "verify":
        codex_result = CommandResult(
            command=[],
            cwd=str(repo_path),
            exit_code=0,
            stdout="Verification handled by kctl.\n",
            stderr="",
        )
    else:
        raise PlanError(f"Unsupported step type: {effective_step_type}")
    raw_artifact_path = write_raw_output_artifact(
        run_output_dir=run_output_dir,
        step_index=step_index,
        step_id=step_id,
        codex_result=codex_result,
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
    effective_mode = "default"
    if isinstance(mode_info, dict):
        effective_mode = str(mode_info.get("effective_mode") or "default")
    verify_shell_value = step.get("verify_shell") or defaults.get("verify_shell")
    verify_commands = resolve_verify_commands(
        step=step,
        defaults=defaults,
        prior_artifacts=prior_artifacts,
        effective_step_type=effective_step_type,
    )
    verify_result: CommandResult | None = None
    verify_results: list[CommandResult] = []
    verify_environment: dict[str, Any] | None = None
    reviews: list[dict[str, Any]] = []
    status = "success"
    failure_reason: str | None = None
    structured_artifacts: dict[str, str] = {}
    artifact_parse_error: str | None = None
    next_artifacts: dict[str, dict[str, Any]] = {}

    if codex_result.exit_code != 0:
        status = "failure"
        failure_reason = "codex_failed"
    if effective_mode == "read-only" and new_changed_files:
        status = "failure"
        failure_reason = "expected_clean_diff"

    phase_artifacts, phase_next_artifacts, artifact_parse_error, artifact_failure_reason = maybe_collect_phase_artifact(
        step=step,
        effective_step_type=effective_step_type,
        codex_result=codex_result,
        run_output_dir=run_output_dir,
        step_index=step_index,
    )
    structured_artifacts.update(phase_artifacts)
    next_artifacts.update(phase_next_artifacts)
    if artifact_failure_reason is not None:
        status = "failure"
        failure_reason = artifact_failure_reason

    if verify_commands:
        shell_parts = parse_verify_shell(verify_shell_value)
        verify_environment = collect_verify_environment(shell_parts, repo_path)
        output_sink.write_line(
            style_text(
                "verify environment: "
                + summarize_verify_environment(verify_environment),
                dim=True,
            )
        )
        verify_results = run_verify_commands(repo_path, shell_parts, verify_commands, output_sink)
        verify_result = combine_verify_results(verify_results, shell_parts)
        if verify_result is not None and verify_result.exit_code != 0:
            status = "failure"
            failure_reason = "verify_failed"

    verify_artifacts, verify_next_artifacts = maybe_build_verify_artifact(
        step=step,
        effective_step_type=effective_step_type,
        run_output_dir=run_output_dir,
        step_index=step_index,
        verify_results=verify_results,
        prior_artifacts=prior_artifacts,
        verify_environment=verify_environment,
    )
    structured_artifacts.update(verify_artifacts)
    next_artifacts.update(verify_next_artifacts)

    if should_run_reviews(review_enabled, review_info) and status == "success" and new_changed_files:
        reviews = run_step_reviews(
            repo_path=repo_path,
            objective=objective,
            step_id=step_id,
            new_changed_files=new_changed_files,
            verify_result=verify_result,
            verbose=verbose,
            print_review_summary=lambda review_step_id, review_items: print_review_summary(
                review_step_id, review_items, output_sink
            ),
            output_sink=output_sink,
        )
        review_status, review_failure_reason = apply_review_policy(reviews, review_info)
        if review_status is not None:
            status = review_status
            failure_reason = review_failure_reason

    step_result = build_step_result(
        step_id=step_id,
        step_prompt=step.get("prompt", ""),
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
        raw_artifact_path=raw_artifact_path,
        structured_artifacts=structured_artifacts,
        artifact_parse_error=artifact_parse_error,
        verify_environment=verify_environment,
        step_type_info=step_type_info,
        output_info=output_info,
        review_info=review_info,
        mode_info=mode_info,
        verify_info=verify_info,
    )
    if should_print_diff_stat(diff_stat.stdout, verbose):
        output_sink.write_line(style_text("git diff --stat:", bold=True))
        output_sink.write_line(diff_stat.stdout.rstrip())
    print_step_footer(step_result, output_sink)
    return step_result, next_artifacts


def save_run_log(run_data: dict[str, Any], run_output_dir: Path) -> Path:
    log_path = run_output_dir / "run.json"
    log_path.write_text(json.dumps(run_data, indent=2) + "\n")
    return log_path


def execute_plan_run(
    plan_path: Path,
    verbose: bool,
    approve_each_step: bool,
    branch: str | None,
    commit: bool,
    commit_message: str | None,
    allow_dirty_start: bool,
    review_enabled: bool,
    repo_override: str | None = None,
    output_sink: OutputSink | None = None,
    interactive: bool = True,
    run_output_dir_override: Path | None = None,
    status_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    output_sink = output_sink or ConsoleOutputSink()
    plan = load_plan(plan_path)
    if repo_override is not None:
        plan = dict(plan)
        plan["repo"] = repo_override
    validate_plan(plan)
    plan = normalize_plan(plan)
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
    prior_artifacts: dict[str, dict[str, Any]] = {}
    step_results: list[dict[str, Any]] = []
    run_status = "success"
    started_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    artifact_storage_mode = resolve_storage_mode()
    run_output_dir = run_output_dir_override or ensure_run_output_dir(
        repo_path, run_id, storage_mode=artifact_storage_mode
    )
    if run_output_dir_override is not None:
        run_output_dir.mkdir(parents=True, exist_ok=True)
    commit_created = False
    commit_sha: str | None = None
    steps = plan["steps"]
    if status_callback is not None:
        status_callback(
            {
                "type": "run_started",
                "repo": str(repo_path),
                "run_output_dir": str(run_output_dir),
                "current_step": steps[0]["id"] if steps else None,
                "status": "running",
            }
        )
    for index, step in enumerate(steps, start=1):
        if status_callback is not None:
            status_callback(
                {
                    "type": "step_started",
                    "step_id": step["id"],
                    "status": "running",
                }
            )
        step_result, new_artifacts = execute_step(
            repo_path=repo_path,
            objective=plan["objective"],
            defaults=defaults,
            step=step,
            step_index=index,
            prior_summaries=prior_summaries,
            prior_artifacts=prior_artifacts,
            run_output_dir=run_output_dir,
            verbose=verbose,
            review_enabled=review_enabled,
            output_sink=output_sink,
        )
        step_results.append(step_result)
        prior_summaries.append(summarize_step_result(step_result))
        prior_artifacts.update(new_artifacts)
        if status_callback is not None:
            status_callback(
                {
                    "type": "step_completed",
                    "step_id": step["id"],
                    "status": step_result["status"],
                    "failure_reason": step_result["failure_reason"],
                }
            )
        failure_reason = step_result["failure_reason"]
        should_stop = failure_reason in {
            "artifact_parse_failed",
            "expected_clean_diff",
            "review_blocked",
        } or (stop_on_failure and failure_reason in {"verify_failed", "codex_failed"})
        if step_result["status"] == "paused":
            if prompt_to_continue_after_review(
                step_result["id"], step_result["reviews"], interactive
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
        has_next_step = index < len(steps)
        if approve_each_step and has_next_step and not prompt_to_continue(interactive):
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
        "artifact_storage_mode": artifact_storage_mode,
        "artifact_root_path": str(run_output_dir.parent),
        "run_output_dir": str(run_output_dir),
        "steps": step_results,
    }
    log_path = save_run_log(run_data, run_output_dir)
    run_data["log_path"] = str(log_path)
    output_sink.write_line("")
    output_sink.write_line(style_text("Final summary:", bold=True))
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
        output_sink.write_line(style_status_text(summary_line, step_result["status"]))
    output_sink.write_line(style_text(f"Run log: {log_path}", bold=True))
    if status_callback is not None:
        status_callback(
            {
                "type": "run_completed",
                "status": run_status,
                "current_step": step_results[-1]["id"] if step_results else None,
                "log_path": str(log_path),
            }
        )
    return run_data


def run_plan(
    plan_path: Path,
    verbose: bool,
    approve_each_step: bool,
    branch: str | None,
    commit: bool,
    commit_message: str | None,
    allow_dirty_start: bool,
    review_enabled: bool,
    repo_override: str | None = None,
    output_sink: OutputSink | None = None,
    interactive: bool = True,
) -> int:
    run_data = execute_plan_run(
        plan_path=plan_path,
        verbose=verbose,
        approve_each_step=approve_each_step,
        branch=branch,
        commit=commit,
        commit_message=commit_message,
        allow_dirty_start=allow_dirty_start,
        review_enabled=review_enabled,
        repo_override=repo_override,
        output_sink=output_sink,
        interactive=interactive,
    )
    return 1 if run_data["status"] == "failure" else 0
