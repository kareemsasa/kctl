from __future__ import annotations

import html
import shutil
import socket
from pathlib import Path
from urllib.parse import urlencode

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
    parts = [
        f"<div class='card {_status_class(item.status)}'>",
        f"<a href='{_escape(link_href)}'>",
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>",
        f"<strong>{_escape(action_label)}</strong> <span style='color:#374151'>{_escape(item.plan_slug)}</span>",
        "</div>",
        f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(item.current_step_key or '\u2014')}</div>",
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


_COMMON_STYLES = """\
body {
  font-family: sans-serif;
  margin: 0;
  padding: 16px;
  background: #f5f5f5;
  color: #111;
  box-sizing: border-box;
  overflow-x: hidden;
}
.page, main {
  max-width: 1400px;
  margin: 0 auto;
  box-sizing: border-box;
  width: 100%;
}
.page-header {
  background: #1f2937;
  color: white;
  border-radius: 10px;
  padding: 16px;
}
.header-path {
  opacity: 0.7;
  font-size: 0.9em;
}
.main-nav {
  display: flex;
  gap: 4px;
  margin-top: 12px;
}
.nav-link {
  padding: 8px 16px;
  border-radius: 6px;
  text-decoration: none;
  color: rgba(255,255,255,0.7);
  font-weight: 500;
  font-size: 0.95em;
}
.nav-link:hover {
  background: rgba(255,255,255,0.1);
  color: white;
}
.nav-link.active {
  background: rgba(255,255,255,0.15);
  color: white;
}
.overview-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px 20px;
  padding: 12px 16px;
  margin-top: 12px;
  background: white;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 0.95em;
}
.overview-bar span {
  white-space: nowrap;
}
main {
  display: grid;
  grid-template-columns: 300px 1fr;
  gap: 16px;
  padding: 16px 0;
  align-items: start;
  min-width: 0;
}
main.single-column {
  grid-template-columns: 1fr;
  max-width: 900px;
}
.actions-details summary {
  cursor: pointer;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 8px;
}
.actions-details summary::-webkit-details-marker {
  display: none;
}
.actions-details summary::before {
  content: "\\25B6";
  font-size: 0.7em;
  transition: transform 0.15s;
}
.actions-details[open] summary::before {
  transform: rotate(90deg);
}
.inline-heading {
  display: inline;
  margin: 0;
}
.column {
  display: flex;
  flex-direction: column;
  gap: 16px;
  min-width: 0;
}
.panel {
  background: white;
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 16px;
  min-width: 0;
}
form {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 12px;
}
input, button, textarea {
  font: inherit;
  padding: 8px 10px;
}
select {
  font: inherit;
  padding: 8px 10px;
}
button {
  cursor: pointer;
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}
textarea {
  resize: vertical;
  min-height: 120px;
}
.checkbox {
  display: flex;
  align-items: center;
  gap: 8px;
}
.selection-list-item {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  align-items: start;
  column-gap: 10px;
  margin: 4px 0;
}
.selection-list-control {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 22px;
}
.selection-list-control input {
  margin: 0;
}
.selection-list-label {
  display: block;
  min-width: 0;
  overflow-wrap: anywhere;
}
.notice {
  margin-bottom: 12px;
  padding: 10px 12px;
  border-radius: 6px;
  background: #eff6ff;
  border: 1px solid #bfdbfe;
}
.help {
  color: #4b5563;
  font-size: 0.95em;
  line-height: 1.4;
}
.repo-check {
  font-size: 0.92em;
  color: #4b5563;
}
.repo-check[data-status='ok'] {
  color: #15803d;
}
.repo-check[data-status='missing'],
.repo-check[data-status='not_dir'],
.repo-check[data-status='empty'] {
  color: #b91c1c;
}
.plans-preview {
  color: #374151;
  font-size: 0.92em;
  line-height: 1.4;
}
.plans-preview ul {
  margin: 6px 0 0;
  padding-left: 18px;
}
.plans-preview label {
  margin: 4px 0;
}
.preflight-summary {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.launch-decision {
  border-radius: 8px;
  padding: 10px 12px;
  font-weight: 700;
}
.launch-decision-pass {
  background: #dcfce7;
  color: #166534;
}
.launch-decision-warn {
  background: #fef3c7;
  color: #92400e;
}
.launch-decision-block {
  background: #fee2e2;
  color: #991b1b;
}
.preflight-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 8px;
}
.preflight-item {
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 10px;
  background: white;
}
.preflight-badge {
  display: inline-block;
  margin-left: 6px;
  padding: 1px 6px;
  border-radius: 999px;
  font-size: 0.78em;
  letter-spacing: 0.04em;
}
.preflight-badge-pass {
  background: #dcfce7;
  color: #166534;
}
.preflight-badge-warn {
  background: #fef3c7;
  color: #92400e;
}
.preflight-badge-block {
  background: #fee2e2;
  color: #991b1b;
}
.mini-button {
  margin-top: 8px;
  padding: 6px 8px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #f8fafc;
  font-size: 0.85em;
}
.list-item, .card {
  display: block;
  text-decoration: none;
  color: inherit;
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 12px;
  margin-bottom: 8px;
  background: white;
  overflow-wrap: anywhere;
  min-width: 0;
}
.list-item:hover, .card:hover {
  border-color: #999;
}
.code-block {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 12px;
  margin-top: 12px;
  font-family: monospace;
  font-size: 0.9em;
}
.live-output {
  max-height: 420px;
  overflow: auto;
  background: #0f172a;
  color: #e2e8f0;
}
.status-success {
  border-left: 4px solid #15803d;
}
.status-failure {
  border-left: 4px solid #b91c1c;
}
.status-running {
  border-left: 4px solid #1d4ed8;
}
.status-neutral {
  border-left: 4px solid #6b7280;
}
table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  min-width: 760px;
}
th, td {
  text-align: left;
  padding: 8px;
  border-bottom: 1px solid #e5e7eb;
  vertical-align: top;
  overflow-wrap: anywhere;
}
.table-scroll {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  max-width: 100%;
}
.empty {
  color: #666;
  font-style: italic;
}
code {
  font-family: monospace;
  font-size: 0.95em;
}
header div, .panel div {
  overflow-wrap: anywhere;
}
label {
  overflow-wrap: anywhere;
}
.project-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  margin-bottom: 8px;
  background: white;
}
.project-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.project-name {
  font-weight: 600;
  font-size: 1em;
}
a.project-name:hover {
  text-decoration: underline !important;
}
.project-path {
  font-size: 0.85em;
  color: #666;
  overflow-wrap: anywhere;
}
.project-header form {
  margin: 0;
  flex-shrink: 0;
}
.project-git {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  font-size: 0.85em;
}
.git-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  white-space: nowrap;
}
.git-branch {
  background: #e8f0fe;
  color: #1a56db;
}
.git-clean {
  background: #ecfdf5;
  color: #166534;
}
.git-dirty {
  background: #fef2f2;
  color: #991b1b;
}
.git-ahead {
  background: #f0fdf4;
  color: #166534;
}
.git-behind {
  background: #fffbeb;
  color: #92400e;
}
.git-synced {
  background: #f3f4f6;
  color: #6b7280;
}
.git-commit {
  color: #6b7280;
  font-size: 0.9em;
}
.git-unavailable {
  color: #9ca3af;
  font-style: italic;
  font-size: 0.9em;
}
.back-link {
  display: inline-block;
  margin-bottom: 8px;
  font-size: 0.9em;
  color: #2563eb;
  text-decoration: none;
}
.back-link:hover {
  text-decoration: underline;
}
.detail-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
}
.detail-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}
@media (max-width: 700px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
.detail-section h3 {
  margin: 0 0 8px;
  font-size: 1em;
}
.remote-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 0;
  border-bottom: 1px solid #f0f0f0;
  font-size: 0.9em;
}
.remote-row:last-child {
  border-bottom: none;
}
.remote-name {
  font-weight: 600;
  min-width: 60px;
}
.remote-url {
  font-family: monospace;
  font-size: 0.92em;
  overflow-wrap: anywhere;
  flex: 1;
}
.remote-direction {
  color: #6b7280;
  font-size: 0.85em;
  min-width: 50px;
}
.ssh-status {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.85em;
}
.ssh-ok {
  background: #ecfdf5;
  color: #166534;
}
.ssh-fail {
  background: #fef2f2;
  color: #991b1b;
}
.ssh-pending {
  background: #f3f4f6;
  color: #6b7280;
}
.branch-list {
  list-style: none;
  padding: 0;
  margin: 0;
}
.branch-list li {
  padding: 4px 0;
  font-size: 0.9em;
  font-family: monospace;
}
.branch-current {
  font-weight: 600;
  color: #1a56db;
}
.commit-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.88em;
}
.commit-table th, .commit-table td {
  padding: 5px 8px;
  text-align: left;
  border-bottom: 1px solid #f0f0f0;
}
.commit-table th {
  font-weight: 600;
  color: #374151;
}
.commit-sha {
  font-family: monospace;
  color: #2563eb;
}
.status-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 4px;
  font-size: 0.82em;
  font-weight: 600;
}
.status-badge.status-success { background: #dcfce7; color: #166534; }
.status-badge.status-failure { background: #fee2e2; color: #991b1b; }
.status-badge.status-running { background: #dbeafe; color: #1d4ed8; }
.status-badge.status-neutral { background: #f3f4f6; color: #374151; }
.lifecycle-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 4px;
  font-size: 0.82em;
  font-weight: 600;
}
.lifecycle-released { background: #dcfce7; color: #166534; }
.lifecycle-active { background: #dbeafe; color: #1d4ed8; }
.lifecycle-stale { background: #fef3c7; color: #92400e; }
.kv-row {
  display: flex;
  gap: 8px;
  align-items: baseline;
  font-size: 0.92em;
  margin-top: 3px;
  overflow-wrap: anywhere;
}
.kv-label {
  color: #6b7280;
  min-width: 80px;
  flex-shrink: 0;
  font-size: 0.9em;
}
@media (max-width: 860px) {
  body {
    padding: 12px;
  }
  main {
    grid-template-columns: 1fr;
    padding: 12px 0 0;
  }
  .dashboard-primary-column {
    order: -1;
  }
  .page-header {
    padding: 14px 12px;
  }
  .panel {
    padding: 14px;
  }
  .overview-bar {
    margin-top: 10px;
  }
}
@media (max-width: 640px) {
  body {
    font-size: 15px;
    padding: 10px;
  }
  .page-header {
    padding: 10px;
  }
  main {
    gap: 12px;
    padding: 10px 0 0;
  }
  .column {
    gap: 12px;
  }
  .list-item, .card {
    padding: 10px;
  }
  input, button, select, textarea {
    width: 100%;
    box-sizing: border-box;
    min-width: 0;
  }
  table {
    min-width: 640px;
  }
}
.session-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  margin-bottom: 8px;
  background: white;
  text-decoration: none;
  color: inherit;
}
.session-item:hover {
  border-color: #999;
}
.session-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  font-size: 0.85em;
  color: #6b7280;
}
.session-prompt-preview {
  font-size: 0.92em;
  color: #374151;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
}
.session-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.85em;
  font-weight: 500;
  white-space: nowrap;
}
.session-badge-running {
  background: #dbeafe;
  color: #1d4ed8;
}
.session-badge-completed {
  background: #dcfce7;
  color: #166534;
}
.session-badge-failed {
  background: #fee2e2;
  color: #991b1b;
}
.session-badge-provider {
  background: #f3f4f6;
  color: #374151;
}
.session-output {
  max-height: 600px;
  overflow: auto;
  background: #0f172a;
  color: #e2e8f0;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  border: 1px solid #334155;
  border-radius: 6px;
  padding: 12px;
  margin-top: 12px;
  font-family: monospace;
  font-size: 0.88em;
  line-height: 1.5;
}
.session-prompt-full {
  white-space: pre-wrap;
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 12px;
  font-size: 0.92em;
  line-height: 1.5;
}
.session-actions {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}
.btn-primary {
  background: #1d4ed8;
  color: white;
  border: none;
  border-radius: 6px;
  padding: 10px 16px;
  font-weight: 500;
}
.btn-primary:hover {
  background: #1e40af;
}
.btn-danger {
  background: #dc2626;
  color: white;
  border: none;
  border-radius: 6px;
  padding: 8px 12px;
  font-size: 0.9em;
}
.btn-danger:hover {
  background: #b91c1c;
}
.btn-sm {
  background: var(--surface);
  color: var(--text);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 4px 10px;
  font-size: 0.82em;
  cursor: pointer;
  white-space: nowrap;
}
.btn-sm:hover {
  background: var(--border);
}
.btn-sm.btn-danger {
  background: #dc2626;
  color: white;
  border-color: #dc2626;
  padding: 4px 10px;
  font-size: 0.82em;
}
.btn-sm.btn-danger:hover {
  background: #b91c1c;
  border-color: #b91c1c;
}
.action-message {
  padding: 10px 14px;
  border-radius: 6px;
  margin-bottom: 12px;
  font-size: 0.9em;
}
.action-ok {
  background: rgba(34,197,94,0.12);
  border: 1px solid rgba(34,197,94,0.3);
  color: #22c55e;
}
.action-error {
  background: rgba(239,68,68,0.12);
  border: 1px solid rgba(239,68,68,0.3);
  color: #ef4444;
}
.session-chat-shell {
  padding: 0;
  overflow: hidden;
}
.session-chat-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
  padding: 16px 16px 0;
}
.session-chat-window {
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-height: min(68vh, 760px);
  overflow-y: auto;
  padding: 12px 16px 96px;
  background:
    linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
}
.session-chat-row {
  display: flex;
  width: 100%;
}
.session-chat-row-user {
  justify-content: flex-end;
}
.session-chat-row-agent {
  justify-content: flex-start;
}
.session-chat-bubble {
  max-width: min(85%, 720px);
  border-radius: 18px;
  padding: 10px 12px;
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
  overflow-wrap: anywhere;
}
.session-chat-bubble-user {
  background: #1d4ed8;
  color: #eff6ff;
  border-bottom-right-radius: 6px;
}
.session-chat-bubble-agent {
  background: white;
  color: #0f172a;
  border: 1px solid #dbe4f0;
  border-bottom-left-radius: 6px;
}
.session-chat-meta {
  display: flex;
  gap: 8px;
  justify-content: space-between;
  align-items: center;
  font-size: 0.78em;
  opacity: 0.78;
  margin-bottom: 6px;
}
.session-chat-content {
  white-space: pre-wrap;
  line-height: 1.5;
}
.session-chat-placeholder {
  color: #64748b;
  font-style: italic;
}
.session-chat-empty {
  text-align: center;
  color: #64748b;
  padding: 20px 12px;
}
.session-chat-composer {
  position: sticky;
  bottom: 0;
  padding: 12px 16px 16px;
  background: linear-gradient(180deg, rgba(238,242,255,0) 0%, #eef2ff 22%, #eef2ff 100%);
  border-top: 1px solid #dbe4f0;
}
.session-chat-form {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 10px;
  align-items: end;
  margin: 0;
}
.session-chat-form-disabled {
  opacity: 0.88;
}
.session-chat-input {
  min-height: 52px;
  max-height: 180px;
  resize: vertical;
  border-radius: 16px;
  border: 1px solid #bfdbfe;
  background: white;
  padding: 12px 14px;
}
.session-chat-send {
  min-width: 96px;
  align-self: stretch;
}
@media (max-width: 640px) {
  .session-chat-header {
    padding: 14px 14px 0;
  }
  .session-chat-window {
    max-height: calc(100vh - 280px);
    padding: 10px 14px 104px;
  }
  .session-chat-bubble {
    max-width: 92%;
  }
  .session-chat-composer {
    padding: 10px 14px max(14px, env(safe-area-inset-bottom));
  }
  .session-chat-form {
    grid-template-columns: 1fr;
  }
  .session-chat-send {
    min-width: 0;
  }
}
"""


_SHARED_SCRIPTS = """\
function wireRepoCheck(inputId, statusId) {
  const input = document.getElementById(inputId);
  const status = document.getElementById(statusId);
  if (!input || !status) return;
  let timer = null;
  async function refreshStatus() {
    const params = new URLSearchParams({ path: input.value });
    const response = await fetch(`/api/check-repo?${params.toString()}`);
    const data = await response.json();
    status.dataset.status = data.status;
    status.textContent = data.message;
  }
  function scheduleRefresh() {
    clearTimeout(timer);
    timer = setTimeout(refreshStatus, 150);
  }
  input.addEventListener('input', scheduleRefresh);
  refreshStatus();
}
async function copyTextValue(value) {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement('textarea');
  textarea.value = value;
  textarea.setAttribute('readonly', 'readonly');
  textarea.style.position = 'fixed';
  textarea.style.top = '0';
  textarea.style.left = '0';
  textarea.style.opacity = '0';
  textarea.style.pointerEvents = 'none';
  textarea.style.zIndex = '-1';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  if (typeof textarea.setSelectionRange === 'function') {
    textarea.setSelectionRange(0, textarea.value.length);
  }
  const ok = document.execCommand('copy');
  document.body.removeChild(textarea);
  if (!ok) {
    throw new Error('copy failed');
  }
}
function extractCopyValue(node) {
  let value = node.getAttribute('data-copy') || '';
  if (!value) {
    const targetSelector = node.getAttribute('data-copy-target') || '';
    const targetNode = targetSelector ? document.querySelector(targetSelector) : null;
    if (targetNode) {
      if (
        targetNode instanceof HTMLTextAreaElement
        || targetNode instanceof HTMLInputElement
      ) {
        value = targetNode.value || '';
      } else {
        value = targetNode.textContent || '';
      }
    }
    const copyLastLines = parseInt(node.getAttribute('data-copy-last-lines') || '', 10);
    if (copyLastLines > 0) {
      const lines = value.split(/\r?\n/);
      value = lines.slice(-copyLastLines).join('\n');
    }
  }
  return value;
}
function focusCopyTarget(node) {
  if (!node) return null;
  const targetSelector = node.getAttribute('data-copy-target') || '';
  const targetNode = targetSelector ? document.querySelector(targetSelector) : null;
  if (!targetNode) return null;
  if (
    targetNode instanceof HTMLTextAreaElement
    || targetNode instanceof HTMLInputElement
  ) {
    targetNode.focus();
    targetNode.select();
    if (typeof targetNode.setSelectionRange === 'function') {
      targetNode.setSelectionRange(0, targetNode.value.length);
    }
  }
  return targetNode;
}
async function triggerCopyForNode(node) {
  if (!node) return false;
  let handling = node.dataset.copyHandling === '1';
  if (handling) return false;
  node.dataset.copyHandling = '1';
  node.setAttribute('data-label', node.getAttribute('data-label') || node.textContent || '');
  focusCopyTarget(node);
  const value = extractCopyValue(node);
  if (!value) {
    node.dataset.copyHandling = '0';
    return false;
  }
  try {
    await copyTextValue(value);
    node.textContent = 'Copied';
    window.setTimeout(() => {
      node.textContent = node.getAttribute('data-label') || '';
    }, 1200);
    return true;
  } catch (_error) {
    node.textContent = 'Selected';
    window.setTimeout(() => {
      node.textContent = node.getAttribute('data-label') || '';
    }, 1200);
    return true;
  } finally {
    window.setTimeout(() => {
      node.dataset.copyHandling = '0';
    }, 50);
  }
}
window.kctlCopyButtonClick = function(node, event) {
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (event && typeof event.stopPropagation === 'function') {
    event.stopPropagation();
  }
  triggerCopyForNode(node);
  return false;
};
window.kctlActionButtonClick = function(node, actionName, event) {
  if (!node || !actionName) return false;
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (event && typeof event.stopPropagation === 'function') {
    event.stopPropagation();
  }
  if (node.dataset.actionHandling === '1') return false;
  node.dataset.actionHandling = '1';
  window.setTimeout(() => {
    node.dataset.actionHandling = '0';
  }, 100);
  const action = window[actionName];
  if (typeof action !== 'function') return false;
  return action(node);
};
window.kctlKeyActionButton = function(node, actionName, event) {
  if (!event) return false;
  if (event.key !== 'Enter' && event.key !== ' ') return true;
  return window.kctlActionButtonClick(node, actionName, event);
};
window.kctlSubmitButtonClick = function(node, event) {
  if (!node || !node.form) return false;
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (node.dataset.submitHandling === '1') return false;
  node.dataset.submitHandling = '1';
  node.disabled = true;
  node.setAttribute('data-label', node.getAttribute('data-label') || node.textContent || '');
  node.textContent = 'Stopping...';
  if (typeof node.form.requestSubmit === 'function') {
    node.form.requestSubmit();
  } else {
    node.form.submit();
  }
  return false;
};
function wireCopyButtons(root) {
  (root || document).querySelectorAll('[data-copy], [data-copy-target]').forEach((node) => {
    if (node.dataset.bound === '1') return;
    node.dataset.bound = '1';
    node.setAttribute('data-label', node.textContent || '');
    const handleCopy = async (event) => {
      event.preventDefault();
      if (typeof event.stopPropagation === 'function') {
        event.stopPropagation();
      }
      await triggerCopyForNode(node);
    };
    node.addEventListener('click', handleCopy);
    node.addEventListener('touchstart', handleCopy, { passive: false });
    node.addEventListener('touchend', handleCopy, { passive: false });
    node.addEventListener('mousedown', handleCopy);
    if (window.PointerEvent) {
      node.addEventListener('pointerdown', handleCopy);
      node.addEventListener('pointerup', handleCopy);
    }
  });
}
"""


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
