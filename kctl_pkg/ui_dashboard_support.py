from __future__ import annotations

import html
import shutil
import socket
from pathlib import Path
from urllib.parse import urlencode

from .ui_dashboard_scripts import _SHARED_SCRIPTS
from .ui_dashboard_styles import _COMMON_STYLES
from .multi import build_multi_run_id, load_normalized_multi_plans
from .preflight import preflight_multi_run
from .types import PlanError
from .ui_read import STALE_RUNNING_THRESHOLD_SECONDS, AttentionItem


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _status_class(status: str | None) -> str:
    if status in {"passed", "success"}:
        return "status-success"
    if status in {"failed", "failure", "blocked"}:
        return "status-failure"
    if status in {"running", "stopping"}:
        return "status-running"
    return "status-neutral"


def _link(base_params: dict[str, str], **updates: str | None) -> str:
    params = dict(base_params)
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    query = urlencode(params)
    return f"/?{query}" if query else "/"


def _page_link(route: str, /, **params: str | None) -> str:
    filtered = {k: v for k, v in params.items() if v is not None}
    query = urlencode(filtered)
    return f"{route}?{query}" if query else route


def _run_detail_link(run_id: str, plan_execution_id: str | None = None, message: str | None = None) -> str:
    return _page_link(f"/runs/{run_id}", plan_execution_id=plan_execution_id, message=message)


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


def _provider_select_html(field_name: str, providers: list[tuple[str, str]]) -> str:
    options = "<option value=''>Default (from plan)</option>"
    options += "".join(
        f"<option value='{_escape(value)}'>{_escape(label)}</option>"
        for value, label in providers
    )
    return (
        f"<label for='{field_name}'><strong>Provider Override</strong></label>"
        f"<select id='{field_name}' name='{field_name}'>{options}</select>"
        "<div class='help'>Override the agent provider for this run. "
        "Leave as Default to use whatever each plan specifies.</div>"
    )


def _render_collapsible_section(
    title: str,
    body_html: str,
    *,
    heading_tag: str = "h2",
    open_by_default: bool = False,
    details_id: str | None = None,
    class_name: str = "panel actions-details",
    style: str | None = None,
) -> str:
    details_attrs: list[str] = [f"class='{_escape(class_name)}'"]
    if details_id:
        details_attrs.append(f"id='{_escape(details_id)}'")
    if style:
        details_attrs.append(f"style='{_escape(style)}'")
    if open_by_default:
        details_attrs.append("open")
    return (
        f"<details {' '.join(details_attrs)}>"
        f"<summary><{heading_tag} class='inline-heading'>{_escape(title)}</{heading_tag}></summary>"
        f"{body_html}"
        "</details>"
    )


def _render_selection_list(
    field_name: str,
    items: list[tuple[str, str]],
    *,
    heading: str | None = None,
    selected_values: set[str] | None = None,
    empty_html: str = "",
    input_type: str = "checkbox",
    item_class: str = "checkbox",
    input_attrs: str = "",
) -> str:
    if not items:
        return empty_html
    selected = selected_values or set()
    heading_html = f"<strong>{_escape(heading)}</strong>" if heading else ""
    item_class_name = item_class or "selection-list-item"
    items_html = "".join(
        f"<label class='{_escape(item_class_name)}'>"
        "<span class='selection-list-control'>"
        f"<input type='{_escape(input_type)}' name='{_escape(field_name)}' value='{_escape(value)}'"
        f"{(' ' + input_attrs.strip()) if input_attrs.strip() else ''}"
        f"{' checked' if value in selected else ''}>"
        "</span>"
        f"<span class='selection-list-label'>{_escape(label)}</span>"
        "</label>"
        for value, label in items
    )
    return heading_html + items_html


def _render_action_button(
    label: str,
    *,
    action_name: str,
    button_id: str | None = None,
    class_name: str | None = None,
) -> str:
    id_html = f" id='{_escape(button_id)}'" if button_id else ""
    class_html = f" class='{_escape(class_name or 'btn-primary')}'"
    action_html = _escape(action_name)
    return (
        f"<a href='#' role='button' tabindex='0'{id_html}{class_html} "
        f"onclick='return window.kctlActionButtonClick(this, \"{action_html}\")' "
        f"ontouchend='return window.kctlActionButtonClick(this, \"{action_html}\", event)' "
        f"onpointerup='return window.kctlActionButtonClick(this, \"{action_html}\", event)' "
        f"onkeydown='return window.kctlKeyActionButton(this, \"{action_html}\", event)'>"
        f"{_escape(label)}"
        "</a>"
    )


def _render_copy_button(
    label: str,
    *,
    target_selector: str | None = None,
    copy_value: str | None = None,
    copy_last_lines: int | None = None,
    class_name: str = "mini-button",
) -> str:
    attrs = [f"type='button'", f"class='{_escape(class_name)}'"]
    if target_selector is not None:
        attrs.append(f"data-copy-target='{_escape(target_selector)}'")
    if copy_value is not None:
        attrs.append(f"data-copy='{_escape(copy_value)}'")
    if copy_last_lines is not None:
        attrs.append(f"data-copy-last-lines='{_escape(copy_last_lines)}'")
    attrs.append("onclick='return window.kctlCopyButtonClick(this)'")
    attrs.append("ontouchstart='return window.kctlCopyButtonClick(this)'")
    attrs.append("ontouchend='return window.kctlCopyButtonClick(this)'")
    attrs.append("onmousedown='return window.kctlCopyButtonClick(this)'")
    attrs.append("onpointerdown='return window.kctlCopyButtonClick(this)'")
    attrs.append("onpointerup='return window.kctlCopyButtonClick(this)'")
    return f"<button {' '.join(attrs)}>{_escape(label)}</button>"


def _render_select_text_link(
    label: str,
    *,
    target_id: str,
    class_name: str = "mini-button",
) -> str:
    return (
        f"<a href='#{_escape(target_id)}' class='{_escape(class_name)}' "
        "style='display:inline-block;text-decoration:none' "
        f"onclick=\"var el=document.getElementById('{_escape(target_id)}');"
        "if(el){el.focus();el.select();if(el.setSelectionRange){el.setSelectionRange(0, el.value.length);}} return true;\">"
        f"{_escape(label)}"
        "</a>"
    )


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


def _preflight_item(
    status: str,
    summary: str,
    details: str | None = None,
    remediation: str | None = None,
    action_label: str | None = None,
    action_value: str | None = None,
) -> dict[str, str | None]:
    return {
        "status": status,
        "summary": summary,
        "details": details,
        "remediation": remediation,
        "action_label": action_label,
        "action_value": action_value,
    }


def _preflight_status_tone(status: str) -> str:
    if status in {"ok", "pass"}:
        return "pass"
    if status in {"blocked", "error", "missing", "not_dir", "empty"}:
        return "block"
    return "warn"


def _render_preflight_item_html(label: str, item: dict[str, object]) -> str:
    tone = _preflight_status_tone(str(item.get("status") or "warn"))
    badge = tone.upper()
    details = item.get("details")
    remediation = item.get("remediation")
    action_label = item.get("action_label")
    action_value = item.get("action_value")
    return (
        f"<div class='preflight-item {_status_class(tone)}'>"
        f"<div><strong>{_escape(label)}</strong> <span class='preflight-badge preflight-badge-{_escape(tone)}'>{_escape(badge)}</span></div>"
        f"<div>{_escape(item.get('summary'))}</div>"
        + (f"<div class='help'>{_escape(details)}</div>" if details else "")
        + (f"<div class='help'><strong>Fix:</strong> {_escape(remediation)}</div>" if remediation else "")
        + (
            f"<button type='button' class='mini-button' data-copy='{_escape(action_value)}'>{_escape(action_label)}</button>"
            if action_label and action_value
            else ""
        )
        + "</div>"
    )


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


def _operator_action_label(action: str) -> str:
    return {
        "active": "Running",
        "safe_rerun": "Safe to Rerun",
        "review_workspace": "Review Workspace",
        "fix_config": "Fix Config",
        "investigate_stale": "Stale — Investigate",
    }.get(action, action)


def _failure_reason_label(reason: str | None) -> str | None:
    if reason == "agent_quota_exhausted":
        return "Blocked by agent quota"
    if reason == "agent_rate_limited":
        return "Blocked by agent rate limit"
    if reason == "agent_failed":
        return "Agent failed"
    if reason == "run_stopped":
        return "Stopped by operator"
    return reason


def _display_status_label(status: str | None, failure_reason: str | None = None) -> str:
    # Keep older indexed runs with blocked+run_stopped readable after the
    # write path moved to a first-class stopped status.
    if status == "blocked" and failure_reason == "run_stopped":
        return "stopped"
    return str(status or "unknown")


def _render_attention_card(item: AttentionItem, providers: list[tuple[str, str]] | None = None) -> str:
    link_href = _link({}, run_id=item.run_id, plan_execution_id=item.plan_execution_id)
    action_label = _operator_action_label(item.operator_action)
    current_step_label = item.current_step_key or "—"
    parts = [
        f"<div class='card {_status_class(item.status)}'>",
        f"<a href='{_escape(link_href)}'>",
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>",
        f"<strong>{_escape(action_label)}</strong> <span style='color:#374151'>{_escape(item.plan_slug)}</span>",
        "</div>",
        f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(current_step_label)}</div>",
        f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(item.verify_status)}</div>",
    ]
    if item.failure_reason:
        parts.append(
            f"<div class='kv-row'><span class='kv-label'>Reason</span> {_escape(_failure_reason_label(item.failure_reason))}</div>"
        )
    if item.reset_hint:
        parts.append(f"<div class='kv-row'><span class='kv-label'>Retry after</span> {_escape(item.reset_hint)}</div>")
    parts.append("</a>")
    if item.operator_action == "safe_rerun" and item.plan_file_path:
        provider_html = _provider_select_html("provider_override", providers) if providers else ""
        parts.append(
            f"<form method='post' action='/actions/rerun-plan'>"
            f"<input type='hidden' name='plan_file_path' value='{_escape(item.plan_file_path)}'>"
            f"<input type='hidden' name='run_id' value='{_escape(item.run_id)}'>"
            f"{provider_html}"
            "<button type='submit'>Rerun</button>"
            "</form>"
        )
    elif item.operator_action == "review_workspace" and item.workspace_path:
        parts.append(
            f"<div class='help'>Workspace has uncommitted changes. Review before retrying.</div>"
            f"<button type='button' class='mini-button' data-copy='{_escape(item.workspace_path)}'>Copy workspace path</button>"
        )
    elif item.operator_action == "fix_config":
        parts.append("<div class='help'>Blocked at launch. Fix preflight issues and rerun.</div>")
    elif item.operator_action == "investigate_stale":
        stale_minutes = STALE_RUNNING_THRESHOLD_SECONDS // 60
        parts.append(
            f"<div class='help'>Running since {_escape(item.started_at)} (no completion after {stale_minutes}+ min).</div>"
        )
    parts.append("</div>")
    return "".join(parts)


def read_plan_file(plan_path: Path) -> tuple[str, str]:
    if not plan_path.exists():
        raise PlanError(f"Plan file does not exist: {plan_path}")
    if not plan_path.is_file():
        raise PlanError(f"Plan path is not a file: {plan_path}")
    return plan_path.name, plan_path.read_text()


def _fmt_ts(ts: str | None) -> str:
    if not ts:
        return "\u2014"
    s = str(ts)
    if "T" in s:
        return s.replace("T", " ").split(".")[0].split("+")[0].rstrip("Z") + " UTC"
    return s


def _status_badge(status: str, label: str | None = None, failure_reason: str | None = None) -> str:
    cls = _status_class(status)
    return f"<span class='status-badge {_escape(cls)}'>{_escape(label or _display_status_label(status, failure_reason))}</span>"


def _lifecycle_badge(lifecycle: str) -> str:
    return f"<span class='lifecycle-badge lifecycle-{_escape(lifecycle)}'>{_escape(lifecycle)}</span>"


def _render_nav_html(active: str) -> str:
    items = [("Dashboard", "/"), ("Actions", "/actions"), ("Projects", "/projects"), ("Sessions", "/sessions")]
    parts = []
    for label, href in items:
        cls = "nav-link active" if label == active else "nav-link"
        parts.append(f"<a class='{cls}' href='{href}'>{_escape(label)}</a>")
    return "<nav class='main-nav'>" + "".join(parts) + "</nav>"


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
