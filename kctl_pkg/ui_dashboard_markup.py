from __future__ import annotations

import html
from pathlib import Path
from urllib.parse import urlencode

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
