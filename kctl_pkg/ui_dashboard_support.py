from __future__ import annotations

import shutil
import socket
from pathlib import Path

from .ui_dashboard_markup import (
    _display_status_label,
    _escape,
    _failure_reason_label,
    _fmt_ts,
    _lifecycle_badge,
    _link,
    _operator_action_label,
    _page_link,
    _preflight_item,
    _preflight_status_tone,
    _render_action_button,
    _render_attention_card,
    _render_collapsible_section,
    _render_copy_button,
    _render_nav_html,
    _render_preflight_item_html,
    _render_select_text_link,
    _render_selection_list,
    _run_detail_link,
    _status_badge,
    _status_class,
    read_plan_file,
    _provider_select_html,
)
from .ui_dashboard_scripts import _SHARED_SCRIPTS
from .ui_dashboard_styles import _COMMON_STYLES
from .multi import build_multi_run_id, load_normalized_multi_plans
from .preflight import preflight_multi_run
from .types import PlanError
from .ui_read import STALE_RUNNING_THRESHOLD_SECONDS, AttentionItem


def check_repo_path(path_value: str) -> tuple[str, str]:
    trimmed = path_value.strip()
    if not trimmed:
        return "empty", "Target repo path is empty."
    candidate = Path(trimmed).expanduser()
    if not candidate.exists():
        return "missing", f"Missing: {candidate}"
    if not candidate.is_dir():
        return "not_dir", f"Not a directory: {candidate}"
    return "ok", f"Exists: {candidate.resolve()}"


def available_providers() -> list[tuple[str, str]]:
    providers = []
    if shutil.which("codex") is not None:
        providers.append(("codex", "Codex"))
    if shutil.which("claude") is not None:
        providers.append(("claude", "Claude"))
    return providers


_TOKEN_WARNING_PATTERNS = (
    "hit your limit",
    "usage limit",
    "quota exceeded",
    "quota exhausted",
    "out of tokens",
    "token limit",
    "rate limit",
    "too many requests",
    "max_tokens_exceeded",
    "insufficient_quota",
    "billing",
    "plan limit",
    "context window full",
    "turn limit",
)

_TOKEN_WARNING_FALSE_POSITIVE_PATTERNS = (
    "self.assert",
    "assertionerror",
    "traceback",
    "fail:",
    "expected ",
    " not found in ",
)


def _normalize_token_warning_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    lower = text.lower()
    if not any(pattern in lower for pattern in _TOKEN_WARNING_PATTERNS):
        return None
    if any(pattern in lower for pattern in _TOKEN_WARNING_FALSE_POSITIVE_PATTERNS):
        return None
    return text


def _detect_token_warning(output_path: Path) -> str | None:
    try:
        text = output_path.read_text(errors="replace")
    except OSError:
        return None
    lower = text.lower()
    for pattern in _TOKEN_WARNING_PATTERNS:
        if pattern in lower:
            saw_matching_line = False
            for line in reversed(text.splitlines()):
                if pattern in line.lower():
                    saw_matching_line = True
                    normalized = _normalize_token_warning_text(line)
                    if normalized is not None:
                        return normalized
            if saw_matching_line:
                return None
            normalized = _normalize_token_warning_text(pattern)
            if normalized is not None:
                return normalized
    return None




def list_plans_in_directory(path_value: str) -> tuple[str, str, list[str]]:
    trimmed = path_value.strip()
    if not trimmed:
        return "empty", "Plans directory is empty.", []
    candidate = Path(trimmed).expanduser()
    if not candidate.exists():
        return "missing", f"Missing: {candidate}", []
    if not candidate.is_dir():
        return "not_dir", f"Not a directory: {candidate}", []
    plan_paths = sorted(
        {path.name for pattern in ("*.yaml", "*.yml") for path in candidate.glob(pattern) if path.is_file()}
    )
    if not plan_paths:
        return "empty", f"No plan files found in {candidate.resolve()}", []
    return "ok", f"Found {len(plan_paths)} plan file(s) in {candidate.resolve()}", plan_paths


def _preflight_run_snapshot(run_data: dict[str, object] | None) -> dict[str, object] | None:
    if not run_data:
        return None
    preflight = run_data.get("preflight")
    if not isinstance(preflight, dict):
        return None
    issues = preflight.get("issues") or []
    by_code: dict[str, list[dict[str, object]]] = {}
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        by_code.setdefault(str(issue.get("code") or "unknown"), []).append(issue)
    required_binaries = [str(item) for item in preflight.get("required_binaries") or []]
    required_env = [str(item) for item in preflight.get("required_env") or []]
    run_root = preflight.get("run_root")
    worktree_root = preflight.get("worktree_root")
    return {
        "status": "block" if issues else "pass",
        "message": "Launch preflight snapshot recorded with blockers." if issues else "Launch preflight snapshot recorded as clear.",
        "captured_at": str(preflight.get("captured_at") or run_data.get("started_at") or ""),
        "items": {
            "repo": _preflight_item(
                "block" if by_code.get("missing_repo") or by_code.get("invalid_repo") else "pass",
                str((by_code.get("missing_repo") or by_code.get("invalid_repo") or [{"message": preflight.get("repo_root") or "Repo root recorded."}])[0].get("message") or ""),
                remediation=str((by_code.get("missing_repo") or by_code.get("invalid_repo") or [{}])[0].get("fix") or "") or None,
            ),
            "plans_dir": _preflight_item(
                "block" if by_code.get("missing_plans_dir") or by_code.get("invalid_plans_dir") else "pass",
                str((by_code.get("missing_plans_dir") or by_code.get("invalid_plans_dir") or [{"message": "Plans directory resolved at launch."}])[0].get("message") or ""),
                remediation=str((by_code.get("missing_plans_dir") or by_code.get("invalid_plans_dir") or [{}])[0].get("fix") or "") or None,
            ),
            "binaries": _preflight_item(
                "block" if by_code.get("missing_binary") or by_code.get("missing_path") else "pass",
                "Required binaries available at launch."
                if not (by_code.get("missing_binary") or by_code.get("missing_path"))
                else str((by_code.get("missing_binary") or by_code.get("missing_path") or [{}])[0].get("message") or ""),
                details=", ".join(required_binaries) if required_binaries else "No external binaries required.",
                remediation=str((by_code.get("missing_binary") or by_code.get("missing_path") or [{}])[0].get("fix") or "") or None,
                action_label="Copy binary",
                action_value=required_binaries[0] if required_binaries else None,
            ),
            "writable_paths": _preflight_item(
                "block" if by_code.get("run_dir_unwritable") or by_code.get("workspace_dir_unwritable") else "pass",
                "Run and workspace paths were writable at launch."
                if not (by_code.get("run_dir_unwritable") or by_code.get("workspace_dir_unwritable"))
                else str((by_code.get("run_dir_unwritable") or by_code.get("workspace_dir_unwritable") or [{}])[0].get("message") or ""),
                details="; ".join(value for value in [str(run_root or ""), str(worktree_root or "")] if value),
                remediation=str((by_code.get("run_dir_unwritable") or by_code.get("workspace_dir_unwritable") or [{}])[0].get("fix") or "") or None,
                action_label="Copy path",
                action_value=str(run_root or worktree_root or "") or None,
            ),
            "required_env": _preflight_item(
                "block" if by_code.get("missing_env") else "pass",
                "Required environment variables were present at launch."
                if not by_code.get("missing_env")
                else str((by_code.get("missing_env") or [{}])[0].get("message") or ""),
                details=", ".join(required_env) if required_env else "No required environment variables declared.",
                remediation=str((by_code.get("missing_env") or [{}])[0].get("fix") or "") or None,
                action_label="Copy env",
                action_value=required_env[0] if required_env else None,
            ),
        },
    }


def summarize_preflight_for_dashboard(
    target_repo_value: str,
    plans_dir_value: str,
    selected_plan_names: list[str] | None = None,
    provider_override: str | None = None,
) -> dict[str, object]:
    repo_status, repo_message = check_repo_path(target_repo_value)
    plans_status, plans_message, _plans = list_plans_in_directory(plans_dir_value)
    summary: dict[str, object] = {
        "status": "warn",
        "decision": "Runnable with warnings",
        "message": "Preflight is waiting for valid repo and plans inputs.",
        "items": {
            "repo": _preflight_item(repo_status, repo_message),
            "plans_dir": _preflight_item(plans_status, plans_message),
            "binaries": _preflight_item("warn", "Waiting for plans to resolve required binaries."),
            "writable_paths": _preflight_item("warn", "Waiting for plans to resolve run/workspace paths."),
            "required_env": _preflight_item("warn", "Waiting for plans to resolve required environment variables."),
        },
    }
    if repo_status != "ok" or plans_status != "ok":
        summary["status"] = "block"
        summary["decision"] = "Blocked"
        summary["message"] = "Preflight is blocked until repo and plans inputs resolve."
        return summary

    plans_dir = Path(plans_dir_value).expanduser().resolve()
    selected_filenames = {name.strip() for name in (selected_plan_names or []) if name.strip()}
    try:
        plan_specs, normalized_plans = load_normalized_multi_plans(
            plans_dir,
            selected_filenames=selected_filenames or None,
        )
        if provider_override:
            for plan_id, plan in list(normalized_plans.items()):
                defaults = dict(plan.get("defaults") or {})
                defaults["provider"] = provider_override
                if provider_override == "codex":
                    defaults.pop("permission_mode", None)
                elif "permission_mode" not in defaults:
                    defaults["permission_mode"] = "auto"
                normalized_plan = dict(plan)
                normalized_plan["defaults"] = defaults
                normalized_plan["_kctl_provider"] = provider_override
                normalized_plan["_kctl_permission_mode"] = defaults.get("permission_mode") or "auto"
                normalized_plans[plan_id] = normalized_plan
        report = preflight_multi_run(
            plans_dir=plans_dir,
            run_id=build_multi_run_id(),
            plan_specs=plan_specs,
            normalized_plans=normalized_plans,
        )
    except PlanError as exc:
        return {
            "status": "block",
            "decision": "Blocked",
            "message": str(exc),
            "items": {
                "repo": _preflight_item(repo_status, repo_message),
                "plans_dir": _preflight_item("block", str(exc), remediation="Fix the plans directory contents so kctl can load the selected plans."),
                "binaries": _preflight_item("warn", "Could not resolve plan requirements."),
                "writable_paths": _preflight_item("warn", "Could not resolve plan paths."),
                "required_env": _preflight_item("warn", "Could not resolve required environment variables."),
            },
        }

    binary_issues = [issue.message for issue in report.issues if issue.code in {"missing_binary", "missing_path"}]
    binary_fix = next((issue.fix for issue in report.issues if issue.code in {"missing_binary", "missing_path"}), None)
    writable_issues = [issue.message for issue in report.issues if issue.code in {"run_dir_unwritable", "workspace_dir_unwritable"}]
    writable_fix = next((issue.fix for issue in report.issues if issue.code in {"run_dir_unwritable", "workspace_dir_unwritable"}), None)
    env_issues = [issue.message for issue in report.issues if issue.code == "missing_env"]
    env_fix = next((issue.fix for issue in report.issues if issue.code == "missing_env"), None)
    return {
        "status": "pass" if report.ok else "block",
        "message": "Preflight clear. Launch can proceed." if report.ok else f"Preflight blocked by {len(report.issues)} issue(s).",
        "decision": "Ready to run" if report.ok else "Blocked",
        "items": {
            "repo": _preflight_item(
                "pass" if report.repo_root is not None else "block",
                f"Repo root: {report.repo_root}" if report.repo_root is not None else repo_message,
                remediation="Point the form at a valid git repository root." if report.repo_root is None else None,
            ),
            "plans_dir": _preflight_item(
                "pass",
                f"{len(plan_specs)} plan(s) selected from {plans_dir}",
                ", ".join(spec.filename for spec in plan_specs),
            ),
            "binaries": _preflight_item(
                "pass" if not binary_issues else "block",
                "Required binaries available." if not binary_issues else binary_issues[0],
                ", ".join(report.required_binaries) if report.required_binaries else "No external binaries required.",
                remediation=binary_fix,
                action_label="Copy binary" if report.required_binaries else None,
                action_value=report.required_binaries[0] if report.required_binaries else None,
            ),
            "writable_paths": _preflight_item(
                "pass" if not writable_issues else "block",
                "Run and workspace paths writable." if not writable_issues else writable_issues[0],
                "; ".join(
                    value
                    for value in [str(report.run_root) if report.run_root else None, str(report.worktree_root) if report.worktree_root else None]
                    if value
                ),
                remediation=writable_fix,
                action_label="Copy path" if report.run_root or report.worktree_root else None,
                action_value=str(report.run_root or report.worktree_root) if report.run_root or report.worktree_root else None,
            ),
            "required_env": _preflight_item(
                "pass" if not env_issues else "block",
                "Required environment present." if not env_issues else env_issues[0],
                ", ".join(report.required_env) if report.required_env else "No required environment variables declared.",
                remediation=env_fix,
                action_label="Copy env" if report.required_env else None,
                action_value=report.required_env[0] if report.required_env else None,
            ),
        },
    }


def build_dashboard_access_urls(
    host: str,
    port: int,
    *,
    announce_url: str | None = None,
    tailscale: bool = False,
    hostname: str | None = None,
) -> list[str]:
    urls: list[str] = []
    if announce_url:
        urls.append(announce_url)
    if host in {"0.0.0.0", "::"}:
        urls.append(f"http://localhost:{port}")
        if tailscale:
            resolved_hostname = hostname or socket.gethostname()
            urls.append(f"http://{resolved_hostname}:{port}")
    else:
        urls.append(f"http://{host}:{port}")
    deduped: list[str] = []
    for url in urls:
        if url not in deduped:
            deduped.append(url)
    return deduped
