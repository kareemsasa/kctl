from __future__ import annotations

from pathlib import Path
from typing import Any


def verify_outcome_label(step_result: dict[str, Any]) -> str:
    verify = step_result.get("verify")
    if verify is None:
        return "not run"
    return "passed" if verify.get("exit_code") == 0 else "failed"


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
