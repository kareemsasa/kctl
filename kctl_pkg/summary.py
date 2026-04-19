from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def verify_outcome_label(step_result: dict[str, Any]) -> str:
    verify = step_result.get("verify")
    if verify is None:
        return "not run"
    return "passed" if verify.get("exit_code") == 0 else "failed"


def _load_verify_artifact(step_result: dict[str, Any]) -> dict[str, Any] | None:
    structured_artifacts = step_result.get("structured_artifacts")
    if not isinstance(structured_artifacts, dict):
        return None
    verify_path = structured_artifacts.get("verify")
    if not isinstance(verify_path, str) or not verify_path.strip():
        return None
    path = Path(verify_path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def render_single_run_summary(run_data: dict[str, Any]) -> str:
    plan_name = Path(str(run_data.get("plan_path") or "")).name or "unknown"
    lines = [
        f"# {plan_name}",
        "",
        f"Timestamp: {run_data.get('ended_at') or run_data.get('started_at') or 'unknown'}",
        f"Run status: {run_data.get('status') or 'unknown'}",
        "",
        "## Steps",
    ]
    for step_result in run_data.get("steps") or []:
        lines.append(
            f"- {step_result.get('id') or 'unknown'}: {step_result.get('status') or 'unknown'} "
            f"(verification: {verify_outcome_label(step_result)})"
        )
        verify_artifact = _load_verify_artifact(step_result)
        if verify_artifact is None:
            continue
        commands_run = verify_artifact.get("commands_run")
        if isinstance(commands_run, list) and commands_run:
            lines.append("")
            lines.append("  Verification commands:")
            for command_entry in commands_run:
                if not isinstance(command_entry, dict):
                    continue
                command = command_entry.get("command") or "unknown"
                result = command_entry.get("result") or (
                    "pass" if command_entry.get("exit_code") == 0 else "fail"
                )
                summary = command_entry.get("summary") or ""
                lines.append(f"  - [{result}] `{command}`")
                if isinstance(summary, str) and summary.strip():
                    lines.append(f"    {summary}")
        issues = verify_artifact.get("issues")
        if isinstance(issues, list) and issues:
            lines.append("")
            lines.append("  Verification issues:")
            for issue in issues:
                if not isinstance(issue, dict):
                    continue
                severity = issue.get("severity") or "info"
                summary = issue.get("summary") or ""
                if isinstance(summary, str) and summary.strip():
                    lines.append(f"  - [{severity}] {summary}")
    return "\n".join(lines) + "\n"


def render_multi_run_summary(run_data: dict[str, Any]) -> str:
    plans_dir = Path(str(run_data.get("plans_dir") or "")).name or "unknown"
    lines = [
        f"# {plans_dir}",
        "",
        f"Timestamp: {run_data.get('ended_at') or run_data.get('started_at') or 'unknown'}",
        f"Run status: {run_data.get('status') or 'unknown'}",
        "",
        "## Plans",
    ]
    for plan_state in run_data.get("plans") or []:
        lines.append(
            f"- {plan_state.get('plan_id') or 'unknown'}: {plan_state.get('status') or 'unknown'} "
            f"(verification: {plan_state.get('verify_result') or 'not-run'})"
        )
    return "\n".join(lines) + "\n"


def write_single_run_summary(run_output_dir: Path, run_data: dict[str, Any]) -> Path:
    summary_path = run_output_dir / "summary.md"
    summary_path.write_text(render_single_run_summary(run_data))
    return summary_path


def write_multi_run_summary(run_root: Path, run_data: dict[str, Any]) -> Path:
    summary_path = run_root / "summary.md"
    summary_path.write_text(render_multi_run_summary(run_data))
    return summary_path
