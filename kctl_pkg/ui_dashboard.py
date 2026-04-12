from __future__ import annotations

import html
import json
import shutil
import socket
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

from .multi import build_multi_run_id, load_normalized_multi_plans, resolve_multi_run_log, run_many_plans
from .git import ensure_git_repo, get_repo_root
from .plan import build_plan_from_template, load_plan_templates
from .paths import project_root
from .preflight import preflight_multi_run
from .runner import run_plan
from .ui_index import index_repository_state

from .types import PlanError
from .ui_read import (
    STALE_RUNNING_THRESHOLD_SECONDS,
    AttentionItem,
    PlanExecutionCard,
    RepositoryOverview,
    RunDetail,
    RunListItem,
    StepTimelineItem,
    WorkspaceDetail,
    WorkspaceSummary,
    get_plan_execution,
    get_repository_overview,
    get_repository,
    get_run,
    get_workspace,
    list_attention_items,
    list_plan_executions,
    list_runs,
    list_step_executions,
    list_workspaces,
)


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _status_class(status: str | None) -> str:
    if status in {"passed", "success"}:
        return "status-success"
    if status in {"failed", "failure", "blocked"}:
        return "status-failure"
    if status in {"running"}:
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


def _page_link(path: str, **params: str | None) -> str:
    filtered = {k: v for k, v in params.items() if v is not None}
    query = urlencode(filtered)
    return f"{path}?{query}" if query else path


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
    """Return (value, label) pairs for agent providers found in PATH."""
    providers = []
    if shutil.which("codex") is not None:
        providers.append(("codex", "Codex"))
    if shutil.which("claude") is not None:
        providers.append(("claude", "Claude"))
    return providers


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
    plans_status, plans_message, plans = list_plans_in_directory(plans_dir_value)
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
    return reason


def _render_attention_card(item: AttentionItem, providers: list[tuple[str, str]] | None = None) -> str:
    link_href = _link({}, run_id=item.run_id, plan_execution_id=item.plan_execution_id)
    action_label = _operator_action_label(item.operator_action)
    parts = [
        f"<div class='card {_status_class(item.status)}'>",
        f"<a href='{_escape(link_href)}'>",
        f"<div><strong>{_escape(action_label)}</strong> — {_escape(item.plan_slug)}</div>",
        f"<div>step={_escape(item.current_step_key)} verify={_escape(item.verify_status)}</div>",
    ]
    if item.failure_reason:
        parts.append(f"<div>reason={_escape(_failure_reason_label(item.failure_reason))}</div>")
    if item.reset_hint:
        parts.append(f"<div>Retry after: {_escape(item.reset_hint)}</div>")
    parts.append("</a>")
    if item.operator_action == "safe_rerun" and item.plan_file_path:
        provider_html = _provider_select_html("provider_override", providers) if providers else ""
        parts.append(
            f"<form method='post' action='/actions/rerun-plan'>"
            f"<input type='hidden' name='plan_file_path' value='{_escape(item.plan_file_path)}'>"
            f"<input type='hidden' name='run_id' value='{_escape(item.run_id)}'>"
            f"{provider_html}"
            f"<button type='submit'>Rerun</button>"
            f"</form>"
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
  display: flex;
  align-items: center;
  gap: 8px;
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
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  margin-bottom: 8px;
  background: white;
}
.project-item code {
  flex: 1;
  min-width: 0;
  overflow-wrap: anywhere;
}
.project-item form {
  margin: 0;
  flex-shrink: 0;
}
@media (max-width: 860px) {
  body {
    padding: 12px;
  }
  main {
    grid-template-columns: 1fr;
    padding: 12px 0 0;
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
function wireCopyButtons(root) {
  (root || document).querySelectorAll('[data-copy]').forEach((node) => {
    if (node.dataset.bound === '1') return;
    node.dataset.bound = '1';
    node.setAttribute('data-label', node.textContent || '');
    node.addEventListener('click', async () => {
      const value = node.getAttribute('data-copy') || '';
      if (!value) return;
      try {
        await navigator.clipboard.writeText(value);
        node.textContent = 'Copied';
        window.setTimeout(() => {
          node.textContent = node.getAttribute('data-label') || '';
        }, 1200);
      } catch (_error) {
        node.textContent = 'Copy failed';
      }
    });
  });
}
"""


def _render_nav_html(active: str) -> str:
    items = [("Dashboard", "/"), ("Actions", "/actions"), ("Projects", "/projects")]
    parts = []
    for label, href in items:
        cls = "nav-link active" if label == active else "nav-link"
        parts.append(f"<a class='{cls}' href='{href}'>{_escape(label)}</a>")
    return "<nav class='main-nav'>" + "".join(parts) + "</nav>"


@dataclass(frozen=True)
class DashboardState:
    repo_name: str
    repo_root: str
    action_message: str | None
    plan_templates: list[tuple[str, str | None]]
    tracked_projects: list[str]
    available_providers: list[tuple[str, str]]
    overview: RepositoryOverview
    attention_items: list[AttentionItem]
    workspaces: list[WorkspaceSummary]
    runs: list[RunListItem]
    selected_run: RunDetail | None
    plan_cards: list[PlanExecutionCard]
    selected_plan: PlanExecutionCard | None
    selected_plan_file_name: str | None
    selected_plan_file_path: str | None
    selected_plan_file_contents: str | None
    launch_preflight: dict[str, object]
    selected_run_preflight: dict[str, object] | None
    live_output: str | None
    live_output_status: str | None
    live_output_path: str | None
    steps: list[StepTimelineItem]
    workspace: WorkspaceDetail | None


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


class DashboardApp:
    def __init__(self, repo_path: Path, db_path: Path | None = None) -> None:
        self.repo_path = repo_path.resolve()
        self.db_path = db_path.resolve() if db_path is not None else None

    def _page_shell(self, *, active_nav: str, body: str, extra_script: str = "") -> str:
        repo_name = _escape(self.repo_path.name)
        repo_root = _escape(str(self.repo_path))
        nav_html = _render_nav_html(active_nav)
        return (
            "<!doctype html>\n<html lang=\"en\">\n<head>\n"
            "  <meta charset=\"utf-8\">\n"
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
            f"  <title>kctl &mdash; {_escape(active_nav)}</title>\n"
            f"  <style>\n{_COMMON_STYLES}  </style>\n"
            f"  <script>\n{_SHARED_SCRIPTS}{extra_script}\n  </script>\n"
            "</head>\n<body>\n"
            "  <div class=\"page\">\n"
            "    <header class=\"page-header\">\n"
            "      <h1>kctl</h1>\n"
            f"      <div>{repo_name} &mdash; <span class=\"header-path\">{repo_root}</span></div>\n"
            f"      {nav_html}\n"
            "    </header>\n"
            f"    {body}\n"
            "  </div>\n"
            "</body>\n</html>"
        )

    @property
    def default_plans_dir(self) -> Path:
        return self.repo_path / ".kctl" / "plans"

    @property
    def projects_file_path(self) -> Path:
        return self.repo_path / ".kctl" / "dashboard-projects.json"

    def load_tracked_projects(self) -> list[str]:
        path = self.projects_file_path
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(data, list):
            return []
        projects: list[str] = []
        for item in data:
            if not isinstance(item, str) or not item.strip():
                continue
            projects.append(str(Path(item).expanduser().resolve()))
        return sorted(dict.fromkeys(projects))

    def save_tracked_projects(self, projects: list[str]) -> None:
        path = self.projects_file_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(sorted(dict.fromkeys(projects)), indent=2) + "\n")

    def add_tracked_project(self, project_path: Path) -> None:
        ensure_git_repo(project_path)
        repo_root = get_repo_root(project_path)
        resolved = str(repo_root)
        projects = self.load_tracked_projects()
        if resolved in projects:
            raise PlanError(f"Project already tracked: {resolved}")
        projects.append(resolved)
        self.save_tracked_projects(projects)

    def remove_tracked_project(self, project_path: str) -> None:
        normalized_target = str(Path(project_path).expanduser().resolve())
        projects = [path for path in self.load_tracked_projects() if path != normalized_target]
        self.save_tracked_projects(projects)

    def plans_dir_for_repo(self, target_repo: Path) -> Path:
        return target_repo.resolve() / ".kctl" / "plans"

    def load_live_run_data(self, run_id: str) -> dict[str, object] | None:
        try:
            run_log = resolve_multi_run_log(self.repo_path, run_id)
        except PlanError:
            return None
        try:
            return json.loads(run_log.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def load_saved_run_data(self, run_root_path: str | None) -> dict[str, object] | None:
        if not run_root_path:
            return None
        run_log = Path(run_root_path) / "run.json"
        if not run_log.exists():
            return None
        try:
            return json.loads(run_log.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def read_live_output(self, run_data: dict[str, object] | None) -> tuple[str | None, str | None, str | None]:
        if not run_data:
            return None, None, None
        stream_log_path = run_data.get("stream_log_path")
        if stream_log_path:
            candidate = Path(str(stream_log_path))
        else:
            run_output_dir = run_data.get("run_output_dir")
            if run_output_dir:
                candidate = Path(str(run_output_dir)).parent / "stream.log"
            else:
                candidate = Path(str(run_data.get("artifact_root_path") or "")) / str(run_data.get("run_id") or "") / "stream.log"
        if not candidate.exists():
            return None, str(candidate), str(run_data.get("status") or "unknown")
        try:
            return candidate.read_text(), str(candidate.resolve()), str(run_data.get("status") or "unknown")
        except OSError:
            return None, str(candidate), str(run_data.get("status") or "unknown")

    def load_state(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> DashboardState:
        repository = get_repository(self.repo_path, db_path=self.db_path)
        overview = get_repository_overview(self.repo_path, db_path=self.db_path)
        attention_items = list_attention_items(self.repo_path, db_path=self.db_path)
        workspaces = list_workspaces(self.repo_path, db_path=self.db_path)
        runs = list_runs(self.repo_path, db_path=self.db_path)
        templates = load_plan_templates(project_root())
        plan_templates = [
            (template_name, template.get("description") if isinstance(template, dict) else None)
            for template_name, template in templates.items()
        ]
        tracked_projects = self.load_tracked_projects()
        selected_run: RunDetail | None = None
        plan_cards: list[PlanExecutionCard] = []
        selected_plan: PlanExecutionCard | None = None
        selected_plan_file_name: str | None = None
        selected_plan_file_path: str | None = None
        selected_plan_file_contents: str | None = None
        launch_preflight = summarize_preflight_for_dashboard(
            str(self.repo_path),
            str(self.default_plans_dir),
            selected_plan_names=None,
            provider_override=None,
        )
        selected_run_preflight: dict[str, object] | None = None
        live_output: str | None = None
        live_output_status: str | None = None
        live_output_path: str | None = None
        steps: list[StepTimelineItem] = []
        workspace: WorkspaceDetail | None = None
        live_run_data: dict[str, object] | None = None

        if run_id is None and runs:
            run_id = runs[0].id
        if run_id is not None:
            live_run_data = self.load_live_run_data(run_id)
            try:
                selected_run = get_run(self.repo_path, run_id, db_path=self.db_path)
                plan_cards = list_plan_executions(self.repo_path, run_id, db_path=self.db_path)
            except PlanError:
                if live_run_data is not None:
                    selected_run = self._build_run_detail_from_live_data(live_run_data)
                    plan_cards = self._build_plan_cards_from_live_data(live_run_data)
                else:
                    raise
            if live_run_data is not None and str(live_run_data.get("status") or "") == "running":
                selected_run = self._build_run_detail_from_live_data(live_run_data)
                plan_cards = self._build_plan_cards_from_live_data(live_run_data)
            selected_run_preflight = _preflight_run_snapshot(
                live_run_data or (self.load_saved_run_data(selected_run.run_root_path) if selected_run is not None else None)
            )

        if plan_execution_id is None and plan_cards:
            plan_execution_id = plan_cards[0].id
        if plan_execution_id is not None:
            try:
                selected_plan = get_plan_execution(plan_execution_id, self.repo_path, db_path=self.db_path)
                steps = list_step_executions(plan_execution_id, self.repo_path, db_path=self.db_path)
                workspace = get_workspace(plan_execution_id, self.repo_path, db_path=self.db_path)
            except PlanError:
                if live_run_data is not None:
                    selected_plan = next((plan for plan in plan_cards if plan.id == plan_execution_id), None)
        if selected_plan_file:
            plan_file_path = Path(selected_plan_file).expanduser().resolve()
            selected_plan_file_name, selected_plan_file_contents = read_plan_file(plan_file_path)
            selected_plan_file_path = str(plan_file_path)
        live_output, live_output_path, live_output_status = self.read_live_output(live_run_data)

        return DashboardState(
            repo_name=repository.name,
            repo_root=repository.root_path,
            action_message=action_message,
            plan_templates=plan_templates,
            tracked_projects=tracked_projects,
            available_providers=available_providers(),
            overview=overview,
            attention_items=attention_items,
            workspaces=workspaces,
            runs=runs,
            selected_run=selected_run,
            plan_cards=plan_cards,
            selected_plan=selected_plan,
            selected_plan_file_name=selected_plan_file_name,
            selected_plan_file_path=selected_plan_file_path,
            selected_plan_file_contents=selected_plan_file_contents,
            launch_preflight=launch_preflight,
            selected_run_preflight=selected_run_preflight,
            live_output=live_output,
            live_output_status=live_output_status,
            live_output_path=live_output_path,
            steps=steps,
            workspace=workspace,
        )

    def _build_run_detail_from_live_data(self, run_data: dict[str, object]) -> RunDetail:
        plan_states = list(run_data.get("plans") or [])
        return RunDetail(
            id=str(run_data.get("run_id") or ""),
            repository_id=str(self.repo_path),
            status=str(run_data.get("status") or "unknown"),
            launch_source="plans_run_many",
            plans_dir=str(run_data.get("plans_dir") or ""),
            concurrency=int(run_data.get("concurrency") or 1),
            started_at=str(run_data.get("started_at") or ""),
            ended_at=run_data.get("ended_at"),
            run_root_path=str(Path(str(run_data.get("artifact_root_path") or "")) / str(run_data.get("run_id") or "")),
            plan_execution_count=len(plan_states),
            passed_count=sum(1 for plan in plan_states if plan.get("status") == "passed"),
            failed_count=sum(1 for plan in plan_states if plan.get("status") == "failed"),
            running_count=sum(1 for plan in plan_states if plan.get("status") == "running"),
            blocked_count=sum(1 for plan in plan_states if plan.get("status") == "blocked"),
        )

    def _build_plan_cards_from_live_data(self, run_data: dict[str, object]) -> list[PlanExecutionCard]:
        run_id = str(run_data.get("run_id") or "")
        plan_cards: list[PlanExecutionCard] = []
        for plan_state in run_data.get("plans") or []:
            plan_path = str(plan_state.get("plan_path") or "")
            plan_slug = Path(plan_path).stem if plan_path else str(plan_state.get("plan_id") or "plan")
            plan_id = str(plan_state.get("plan_id") or plan_slug)
            plan_cards.append(
                PlanExecutionCard(
                    id=f"{run_id}:{plan_id}",
                    run_id=run_id,
                    repository_id=str(self.repo_path),
                    plan_definition_id=f"{self.repo_path}:{Path(plan_path).resolve()}" if plan_path else f"{self.repo_path}:{plan_id}",
                    plan_slug=plan_slug,
                    plan_title=None,
                    plan_file_path=plan_path,
                    objective="",
                    phase_name=None,
                    group_name=None,
                    status=str(plan_state.get("status") or "unknown"),
                    current_step_key=plan_state.get("current_step"),
                    verify_status=str(plan_state.get("verify_result") or "not_run"),
                    started_at=str(run_data.get("started_at") or ""),
                    ended_at=run_data.get("ended_at"),
                    worktree_path=plan_state.get("worktree_path"),
                    branch_name=plan_state.get("branch_name"),
                    log_path=plan_state.get("log_path"),
                    changed_files_count=0,
                    failure_reason=None,
                )
            )
        return plan_cards

    def run_index_now(self) -> None:
        index_repository_state(self.repo_path, db_path=self.db_path)

    def create_plan(
        self,
        *,
        target_repo: Path,
        template_name: str,
        output_name: str,
        objective: str,
        force: bool,
    ) -> Path:
        if not output_name.strip():
            raise PlanError("Plan file name is required.")
        target_repo = target_repo.expanduser().resolve()
        output_path = self.plans_dir_for_repo(target_repo) / output_name
        if output_path.exists() and not force:
            raise PlanError(f"Plan file already exists: {output_path}")
        templates = load_plan_templates(project_root())
        plan = build_plan_from_template(
            templates=templates,
            template_name=template_name,
            repo=str(target_repo),
            objective=objective,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        yaml = __import__("yaml")
        output_path.write_text(yaml.safe_dump(plan, sort_keys=False))
        return output_path

    def start_run_many(
        self,
        plans_dir: Path,
        concurrency: int,
        selected_plan_names: list[str] | None = None,
        provider_override: str | None = None,
    ) -> str:
        run_id = build_multi_run_id()

        def _run() -> None:
            run_many_plans(
                plans_dir.resolve(),
                concurrency=concurrency,
                verbose=False,
                selected_plan_names=selected_plan_names,
                run_id_override=run_id,
                provider_override=provider_override,
            )
            index_repository_state(self.repo_path, db_path=self.db_path)

        threading.Thread(target=_run, daemon=True).start()
        return run_id

    def start_run_plan_across_projects(
        self,
        plan_path: Path,
        project_paths: list[Path],
        provider_override: str | None = None,
    ) -> str:
        run_id = build_multi_run_id()
        normalized_projects = sorted({path.expanduser().resolve() for path in project_paths})

        def _run() -> None:
            for project_path in normalized_projects:
                try:
                    run_plan(
                        plan_path=plan_path.resolve(),
                        verbose=False,
                        approve_each_step=False,
                        branch=None,
                        commit=False,
                        commit_message=None,
                        allow_dirty_start=False,
                        review_enabled=False,
                        repo_override=str(project_path),
                        interactive=False,
                        provider_override=provider_override,
                    )
                except PlanError:
                    continue
                try:
                    index_repository_state(project_path)
                except PlanError:
                    continue

        threading.Thread(target=_run, daemon=True).start()
        return run_id

    def render_actions_page(self, *, action_message: str | None = None) -> str:
        templates = load_plan_templates(project_root())
        plan_templates = [
            (template_name, template.get("description") if isinstance(template, dict) else None)
            for template_name, template in templates.items()
        ]
        tracked_projects = self.load_tracked_projects()
        providers = available_providers()
        launch_preflight = summarize_preflight_for_dashboard(
            str(self.repo_path),
            str(self.default_plans_dir),
            selected_plan_names=None,
            provider_override=None,
        )
        preflight_items = launch_preflight.get("items", {})
        preflight_html = "".join(
            _render_preflight_item_html(label, item)
            for label, item in (
                ("Repo", preflight_items.get("repo") or {}),
                ("Plans Dir", preflight_items.get("plans_dir") or {}),
                ("Binaries", preflight_items.get("binaries") or {}),
                ("Writable Paths", preflight_items.get("writable_paths") or {}),
                ("Required Env", preflight_items.get("required_env") or {}),
            )
        )
        tracked_projects_html = (
            "".join(
                f"<label class='checkbox'><input type='checkbox' name='project_paths' value='{_escape(p)}'> {_escape(p)}</label>"
                for p in tracked_projects
            )
            if tracked_projects
            else "<div class='help'>No tracked projects yet. <a href='/projects'>Manage projects</a></div>"
        )
        notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
        body = (
            f"<main class='single-column'>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'>"
            f"<h2>Refresh Index</h2>"
            f"<div class='help'>Refreshes the dashboard data from saved runs and workspaces on this machine.</div>"
            f"<form method='post' action='/actions/index'>"
            f"<button type='submit'>Refresh Index</button>"
            f"</form>"
            f"</section>"
            f"<section class='panel'>"
            f"<h2>Run Plans</h2>"
            f"<div class='help'>Runs every plan in the plans folder for the target project.</div>"
            f"<form method='post' action='/actions/run-many'>"
            f"<label for='target_repo_run_many'><strong>Target Repo</strong></label>"
            f"<input id='target_repo_run_many' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
            f"<div id='target_repo_run_many_status' class='repo-check'></div>"
            f"<div><strong>Plans Dir</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
            f"<label for='plans_dir'><strong>Plans Dir Override</strong></label>"
            f"<input id='plans_dir' name='plans_dir' type='text' placeholder='Optional override'>"
            f"<div id='plans_dir_status' class='repo-check'></div>"
            f"<div id='plans_dir_preview' class='plans-preview'></div>"
            f"<div><strong>Tracked Projects</strong></div>"
            f"{tracked_projects_html}"
            f"<div class='preflight-summary'>"
            f"<div class='launch-decision launch-decision-{_escape(_preflight_status_tone(str(launch_preflight.get('status') or 'warn')))}' id='run_many_launch_decision'>{_escape(launch_preflight.get('decision') or 'Runnable with warnings')}</div>"
            f"<div><strong>Launch Preflight</strong></div>"
            f"<div id='run_many_preflight_message' class='repo-check' data-status='{_escape(launch_preflight.get('status'))}'>{_escape(launch_preflight.get('message'))}</div>"
            f"<div id='run_many_preflight' class='preflight-grid'>{preflight_html}</div>"
            f"</div>"
            f"<label for='concurrency'><strong>Concurrency</strong></label>"
            f"<input id='concurrency' name='concurrency' type='number' min='1' value='1'>"
            f"<div class='help'>How many plans can run at the same time. Use 1 for the safest option.</div>"
            + (_provider_select_html("provider_override", providers) if providers else "")
            + "<button type='submit' id='run_many_submit_button'>Run Plans</button>"
            + "<button type='submit' formaction='/actions/run-plan-across-projects' id='run_single_across_projects_button'>Run Plan Across Projects</button>"
            + "</form>"
            + "</section>"
            + "<section class='panel'>"
            + "<h2>Create Plan</h2>"
            + "<div class='help'>Creates one new plan file in the target project's plans folder.</div>"
            + "<form method='post' action='/actions/create-plan'>"
            + f"<label for='target_repo_create_plan'><strong>Target Repo</strong></label>"
            + f"<input id='target_repo_create_plan' name='target_repo' type='text' value='{_escape(self.repo_path)}' required>"
            + "<div id='target_repo_create_plan_status' class='repo-check'></div>"
            + "<label for='template_name'><strong>Template</strong></label>"
            + "<select id='template_name' name='template_name'>"
            + "".join(
                f"<option value='{_escape(name)}'>{_escape(name)}"
                + (f" - {_escape(desc)}" if desc else "")
                + "</option>"
                for name, desc in plan_templates
            )
            + "</select>"
            + f"<div><strong>Plan Root</strong>: <code>{_escape(self.default_plans_dir)}</code></div>"
            + "<label for='output_path'><strong>Plan File Name</strong></label>"
            + "<input id='output_path' name='output_path' type='text' placeholder='001-sample.yaml' required>"
            + "<label for='objective'><strong>Objective</strong></label>"
            + "<textarea id='objective' name='objective' rows='5' placeholder='Describe the change' required></textarea>"
            + "<label class='checkbox'><input name='force' type='checkbox' value='1'> Overwrite if the file exists</label>"
            + "<button type='submit'>Create Plan</button>"
            + "</form>"
            + "</section>"
            + "</div>"
            + "</main>"
        )
        actions_script = """\
function renderPreflight(preflight, messageId, containerId) {
  const message = document.getElementById(messageId);
  const container = document.getElementById(containerId);
  const decision = document.getElementById('run_many_launch_decision');
  if (!message || !container || !preflight) return;
  message.dataset.status = preflight.status || 'unknown';
  message.textContent = preflight.message || '';
  const bannerTone = preflight.status === 'pass' || preflight.status === 'ok'
    ? 'pass'
    : (preflight.status === 'block' || preflight.status === 'blocked' || preflight.status === 'error')
      ? 'block'
      : 'warn';
  if (decision) {
    decision.className = `launch-decision launch-decision-${bannerTone}`;
    decision.textContent = preflight.decision || (bannerTone === 'pass' ? 'Ready to run' : bannerTone === 'block' ? 'Blocked' : 'Runnable with warnings');
  }
  const labels = [
    ['repo', 'Repo'],
    ['plans_dir', 'Plans Dir'],
    ['binaries', 'Binaries'],
    ['writable_paths', 'Writable Paths'],
    ['required_env', 'Required Env'],
  ];
  container.innerHTML = labels.map(([key, label]) => {
    const item = (preflight.items || {})[key] || {};
    const details = item.details ? `<div class="help">${item.details}</div>` : '';
    const remediation = item.remediation ? `<div class="help"><strong>Fix:</strong> ${item.remediation}</div>` : '';
    const action = item.action_label && item.action_value
      ? `<button type="button" class="mini-button" data-copy="${item.action_value}">${item.action_label}</button>`
      : '';
    const tone = item.status === 'pass' || item.status === 'ok'
      ? 'pass'
      : (item.status === 'block' || item.status === 'blocked' || item.status === 'error' || item.status === 'missing' || item.status === 'not_dir' || item.status === 'empty')
        ? 'block'
        : 'warn';
    const statusClass = tone === 'pass' ? 'status-success' : tone === 'block' ? 'status-failure' : 'status-neutral';
    return `<div class="preflight-item ${statusClass}"><div><strong>${label}</strong> <span class="preflight-badge preflight-badge-${tone}">${tone.toUpperCase()}</span></div><div>${item.summary || ''}</div>${details}${remediation}${action}</div>`;
  }).join('');
  wireCopyButtons(container);
}
function wirePlansPreview(targetRepoInputId, plansDirInputId, statusId, previewId, preflightMessageId, preflightContainerId) {
  const targetRepoInput = document.getElementById(targetRepoInputId);
  const plansDirInput = document.getElementById(plansDirInputId);
  const runManyForm = plansDirInput ? plansDirInput.closest('form') : null;
  const providerOverrideInput = runManyForm ? runManyForm.querySelector('select[name="provider_override"]') : null;
  const runManyButton = document.getElementById('run_many_submit_button');
  const runAcrossProjectsButton = document.getElementById('run_single_across_projects_button');
  const status = document.getElementById(statusId);
  const preview = document.getElementById(previewId);
  if (!targetRepoInput || !plansDirInput || !status || !preview) return;
  let timer = null;
  function updateRunButtonLabels() {
    const selectedCount = preview.querySelectorAll('input[name="selected_plans"]:checked').length;
    if (runManyButton) {
      runManyButton.textContent = selectedCount === 1 ? 'Run Plan' : 'Run Plans';
    }
    if (runAcrossProjectsButton) {
      runAcrossProjectsButton.disabled = selectedCount !== 1;
      runAcrossProjectsButton.textContent = selectedCount === 1
        ? 'Run Plan Across Projects'
        : 'Select One Plan to Run Across Projects';
    }
  }
  function resolvedPlansDir() {
    const overrideValue = plansDirInput.value.trim();
    if (overrideValue) return overrideValue;
    const repoValue = targetRepoInput.value.trim();
    if (!repoValue) return "";
    return repoValue.replace(/\\/+$/, "") + "/.kctl/plans";
  }
  async function refreshPreflight() {
    const selectedPlans = Array.from(preview.querySelectorAll('input[name="selected_plans"]:checked')).map((node) => node.value);
    const params = new URLSearchParams({
      target_repo: targetRepoInput.value.trim(),
      plans_dir: resolvedPlansDir(),
    });
    if (providerOverrideInput && providerOverrideInput.value.trim()) {
      params.set('provider_override', providerOverrideInput.value.trim());
    }
    selectedPlans.forEach((plan) => params.append('selected_plans', plan));
    const response = await fetch(`/api/preflight?${params.toString()}`);
    const data = await response.json();
    renderPreflight(data, preflightMessageId, preflightContainerId);
  }
  async function refreshPreview() {
    const params = new URLSearchParams({ path: resolvedPlansDir() });
    const response = await fetch(`/api/list-plans?${params.toString()}`);
    const data = await response.json();
    status.dataset.status = data.status;
    status.textContent = data.message;
    if (!data.plans || data.plans.length === 0) {
      preview.innerHTML = "";
      updateRunButtonLabels();
      refreshPreflight();
      return;
    }
    preview.innerHTML =
      "<strong>Plans found</strong>" +
      data.plans.map((plan) => `<label><input type="checkbox" name="selected_plans" value="${plan}"> <span>${plan}</span></label>`).join("");
    preview.querySelectorAll('input[name="selected_plans"]').forEach((node) => {
      node.addEventListener('change', () => {
        updateRunButtonLabels();
        refreshPreflight();
      });
    });
    updateRunButtonLabels();
    refreshPreflight();
  }
  function scheduleRefresh() {
    clearTimeout(timer);
    timer = setTimeout(refreshPreview, 150);
  }
  targetRepoInput.addEventListener('input', scheduleRefresh);
  plansDirInput.addEventListener('input', scheduleRefresh);
  if (providerOverrideInput) {
    providerOverrideInput.addEventListener('change', scheduleRefresh);
  }
  refreshPreview();
}
window.addEventListener('DOMContentLoaded', () => {
  wireCopyButtons(document);
  wireRepoCheck('target_repo_run_many', 'target_repo_run_many_status');
  wireRepoCheck('target_repo_create_plan', 'target_repo_create_plan_status');
  wirePlansPreview(
    'target_repo_run_many',
    'plans_dir',
    'plans_dir_status',
    'plans_dir_preview',
    'run_many_preflight_message',
    'run_many_preflight'
  );
});
"""
        return self._page_shell(active_nav="Actions", body=body, extra_script=actions_script)

    def render_projects_page(self, *, action_message: str | None = None) -> str:
        tracked_projects = self.load_tracked_projects()
        tracked_json = json.dumps(tracked_projects)
        notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
        project_items_html = "".join(
            "<div class='project-item'>"
            f"<code>{_escape(project_path)}</code>"
            f"<form method='post' action='/actions/remove-project'>"
            f"<input type='hidden' name='project_path' value='{_escape(project_path)}'>"
            f"<button type='submit'>Remove</button>"
            f"</form>"
            "</div>"
            for project_path in tracked_projects
        ) or "<div class='empty'>No tracked projects yet.</div>"
        body = (
            f"<main class='single-column'>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'>"
            f"<h2>Tracked Projects</h2>"
            f"<div class='help'>Local repo paths used for cross-project plan runs.</div>"
            f"{project_items_html}"
            f"</section>"
            f"<section class='panel'>"
            f"<h2>Add Project</h2>"
            f"<div class='help'>Add a local git repository path to the tracked projects list.</div>"
            f"<form method='post' action='/actions/add-project' id='add_project_form'>"
            f"<label for='project_path'><strong>Project Path</strong></label>"
            f"<input id='project_path' name='project_path' type='text' placeholder='/path/to/project' required>"
            f"<div id='project_path_status' class='repo-check'></div>"
            f"<div id='project_path_duplicate' class='repo-check'></div>"
            f"<button type='submit' id='add_project_button'>Add Project</button>"
            f"</form>"
            f"</section>"
            f"</div>"
            f"</main>"
        )
        projects_script = f"""\
window.addEventListener('DOMContentLoaded', () => {{
  wireRepoCheck('project_path', 'project_path_status');
  const trackedProjects = {tracked_json};
  const input = document.getElementById('project_path');
  const dupStatus = document.getElementById('project_path_duplicate');
  const addButton = document.getElementById('add_project_button');
  if (!input || !dupStatus || !addButton) return;
  let timer = null;
  async function checkDuplicate() {{
    const value = input.value.trim();
    if (!value) {{
      dupStatus.textContent = '';
      dupStatus.dataset.status = '';
      addButton.disabled = false;
      return;
    }}
    const params = new URLSearchParams({{ path: value }});
    const response = await fetch('/api/resolve-path?' + params.toString());
    const data = await response.json();
    const resolved = data.resolved || '';
    if (resolved && trackedProjects.includes(resolved)) {{
      dupStatus.dataset.status = 'empty';
      dupStatus.textContent = 'Already tracked: ' + resolved;
      addButton.disabled = true;
    }} else {{
      dupStatus.textContent = '';
      dupStatus.dataset.status = '';
      addButton.disabled = false;
    }}
  }}
  function scheduleCheck() {{
    clearTimeout(timer);
    timer = setTimeout(checkDuplicate, 150);
  }}
  input.addEventListener('input', scheduleCheck);
  checkDuplicate();
}});
"""
        return self._page_shell(active_nav="Projects", body=body, extra_script=projects_script)

    def render_page(
        self,
        run_id: str | None = None,
        plan_execution_id: str | None = None,
        action_message: str | None = None,
        selected_plan_file: str | None = None,
    ) -> str:
        state = self.load_state(
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )
        base_params = {}
        if state.selected_run is not None:
            base_params["run_id"] = state.selected_run.id
        if state.selected_plan is not None:
            base_params["plan_execution_id"] = state.selected_plan.id
        default_plans_status, default_plans_message, default_plan_files = list_plans_in_directory(str(self.default_plans_dir))
        run_preflight_html = ""
        if state.selected_run_preflight is not None:
            run_preflight_items = state.selected_run_preflight.get("items", {})
            run_preflight_html = (
                "<div class='preflight-summary'>"
                "<div><strong>Launch Snapshot</strong></div>"
                f"<div class='repo-check' data-status='{_escape(state.selected_run_preflight.get('status'))}'>{_escape(state.selected_run_preflight.get('message'))}</div>"
                f"<div class='help'>captured_at={_escape(state.selected_run_preflight.get('captured_at'))}</div>"
                f"<div class='preflight-grid'>"
                + "".join(
                    _render_preflight_item_html(label, item)
                    for label, item in (
                        ("Repo", run_preflight_items.get("repo") or {}),
                        ("Plans Dir", run_preflight_items.get("plans_dir") or {}),
                        ("Binaries", run_preflight_items.get("binaries") or {}),
                        ("Writable Paths", run_preflight_items.get("writable_paths") or {}),
                        ("Required Env", run_preflight_items.get("required_env") or {}),
                    )
                )
                + "</div></div>"
            )

        run_items = "".join(
            (
                f"<a class='list-item {_status_class(run.status)}' href='{_escape(_link({}, run_id=run.id, plan_execution_id=None))}'>"
                f"<div><strong>{_escape(run.id)}</strong></div>"
                f"<div>status={_escape(run.status)} started_at={_escape(run.started_at)}</div>"
                f"<div>concurrency={_escape(run.concurrency)}</div>"
                "</a>"
            )
            for run in state.runs
        ) or "<div class='empty'>No indexed runs.</div>"

        overview_html = (
            "<div class='overview-bar'>"
            f"<span><strong>{_escape(state.overview.run_count)}</strong> runs</span>"
            f"<span><strong>{_escape(state.overview.active_run_count)}</strong> active</span>"
            f"<span class='{_status_class('failure') if state.overview.failed_run_count else ''}'><strong>{_escape(state.overview.failed_run_count)}</strong> failed</span>"
            f"<span><strong>{_escape(state.overview.running_plan_count)}</strong> running</span>"
            f"<span><strong>{_escape(state.overview.blocked_plan_count)}</strong> blocked</span>"
            f"<span><strong>{_escape(state.overview.stale_workspace_count)}</strong> stale</span>"
            "</div>"
        )

        attention_items_html = "".join(
            _render_attention_card(item, providers=state.available_providers) for item in state.attention_items
        ) or "<div class='empty'>No attention items.</div>"

        workspace_items_html = "".join(
            (
                f"<a class='card {_status_class(workspace.lifecycle)}' href='{_escape(_link({}, run_id=workspace.run_id, plan_execution_id=workspace.plan_execution_id))}'>"
                f"<div><strong>{_escape(workspace.plan_slug)}</strong> lifecycle={_escape(workspace.lifecycle)}</div>"
                f"<div>status={_escape(workspace.status)} step={_escape(workspace.current_step_key)}</div>"
                f"<div>verify={_escape(workspace.verify_status)} failure_reason={_escape(workspace.failure_reason)}</div>"
                f"<div>path={_escape(workspace.path)}</div>"
                "</a>"
            )
            for workspace in state.workspaces
        ) or "<div class='empty'>No workspaces.</div>"

        plan_file_items_html = "".join(
            (
                f"<a class='list-item {_status_class('neutral')}' href='{_escape(_link(base_params, selected_plan_file=str((self.default_plans_dir / plan_file_name).resolve())))}'>"
                f"<div><strong>{_escape(plan_file_name)}</strong></div>"
                "</a>"
            )
            for plan_file_name in default_plan_files
        ) or f"<div class='empty'>{_escape(default_plans_message)}</div>"

        plan_items = "".join(
            (
                f"<a class='card {_status_class(plan.status)}' href='{_escape(_link({'run_id': state.selected_run.id}, run_id=state.selected_run.id, plan_execution_id=plan.id))}'>"
                f"<div><strong>{_escape(plan.plan_slug)}</strong></div>"
                f"<div>status={_escape(plan.status)} current_step={_escape(plan.current_step_key)}</div>"
                f"<div>verify={_escape(plan.verify_status)} changed_files={_escape(plan.changed_files_count)}</div>"
                "</a>"
            )
            for plan in state.plan_cards
        ) or "<div class='empty'>No plan executions for this run.</div>"

        timeline_rows = "".join(
            (
                "<tr>"
                f"<td>{_escape(step.sequence_index)}</td>"
                f"<td>{_escape(step.step_key)}</td>"
                f"<td>{_escape(step.kind)}</td>"
                f"<td class='{_status_class(step.status)}'>{_escape(step.status)}</td>"
                f"<td>{_escape(step.verify_status)}</td>"
                f"<td>{_escape(step.changed_files_count)}</td>"
                f"<td>{_escape(step.duration_ms)}</td>"
                f"<td>{_escape(step.output_path)}</td>"
                f"<td>{_escape(step.artifact_path)}</td>"
                "</tr>"
            )
            for step in state.steps
        ) or "<tr><td colspan='9' class='empty'>No step timeline available.</td></tr>"

        selected_run_html = ""
        if state.selected_run is not None:
            selected_run_html = (
                "<section class='panel'>"
                "<h2>Run Detail</h2>"
                f"<div><strong>{_escape(state.selected_run.id)}</strong></div>"
                f"<div>status={_escape(state.selected_run.status)} launch_source={_escape(state.selected_run.launch_source)}</div>"
                f"<div>started_at={_escape(state.selected_run.started_at)} ended_at={_escape(state.selected_run.ended_at)}</div>"
                f"<div>plans={_escape(state.selected_run.plan_execution_count)} passed={_escape(state.selected_run.passed_count)} "
                f"failed={_escape(state.selected_run.failed_count)} running={_escape(state.selected_run.running_count)} "
                f"blocked={_escape(state.selected_run.blocked_count)}</div>"
                f"{run_preflight_html}"
                "</section>"
            )

        live_output_html = (
            "<div class='empty'>Start a run to see live plan output here.</div>"
            if state.selected_run is None
            else "<div class='empty'>No live output captured for this run yet.</div>"
        )
        if state.live_output_path is not None:
            live_output_html = (
                f"<div>status={_escape(state.live_output_status)} path={_escape(state.live_output_path)}</div>"
                f"<pre id='live_output_stream' class='code-block live-output'>{_escape(state.live_output or '')}</pre>"
            )

        workspace_html = "<div class='empty'>No workspace details available.</div>"
        if state.workspace is not None:
            workspace_html = (
                f"<div><strong>path</strong>: {_escape(state.workspace.path)}</div>"
                f"<div><strong>branch</strong>: {_escape(state.workspace.branch_name)}</div>"
                f"<div><strong>base_ref</strong>: {_escape(state.workspace.base_ref)}</div>"
                f"<div><strong>status</strong>: {_escape(state.workspace.status)}</div>"
                f"<div><strong>created_at</strong>: {_escape(state.workspace.created_at)}</div>"
                f"<div><strong>released_at</strong>: {_escape(state.workspace.released_at)}</div>"
            )

        selected_plan_html = ""
        if state.selected_plan is not None:
            selected_plan_html = (
                "<section class='panel'>"
                "<h2>Plan Execution Detail</h2>"
                f"<div><strong>{_escape(state.selected_plan.plan_slug)}</strong></div>"
                f"<div>status={_escape(state.selected_plan.status)} current_step={_escape(state.selected_plan.current_step_key)}</div>"
                f"<div>verify={_escape(state.selected_plan.verify_status)} changed_files={_escape(state.selected_plan.changed_files_count)}</div>"
                f"<div>branch={_escape(state.selected_plan.branch_name)}</div>"
                f"<div>log_path={_escape(state.selected_plan.log_path)}</div>"
                f"<div>failure_reason={_escape(state.selected_plan.failure_reason)}</div>"
                "</section>"
            )

        selected_plan_file_html = "<div class='empty'>No plan file selected.</div>"
        if state.selected_plan_file_name is not None and state.selected_plan_file_contents is not None:
            selected_plan_file_html = (
                f"<div><strong>{_escape(state.selected_plan_file_name)}</strong></div>"
                f"<div>path={_escape(state.selected_plan_file_path)}</div>"
                f"<pre class='code-block'>{_escape(state.selected_plan_file_contents)}</pre>"
            )

        notice_html = f"<div class='notice'>{_escape(state.action_message)}</div>" if state.action_message else ""
        body = (
            f"{overview_html}"
            f"<main>"
            f"<div class='column'>"
            f"{notice_html}"
            f"<section class='panel'><h2>Attention</h2>{attention_items_html}</section>"
            f"<section class='panel'><h2>Runs</h2>{run_items}</section>"
            f"<section class='panel'><h2>Workspaces</h2>{workspace_items_html}</section>"
            f"<section class='panel'><h2>Plans</h2>{plan_file_items_html}</section>"
            f"</div>"
            f"<div class='column'>"
            f"{selected_run_html}"
            f"<section class='panel'><h2>Plan Executions</h2>{plan_items}</section>"
            f"{selected_plan_html}"
            f"<section class='panel'><h2>Live Output</h2>{live_output_html}</section>"
            f"<section class='panel'><h2>Step Timeline</h2>"
            f"<div class='table-scroll'><table>"
            f"<thead><tr><th>#</th><th>step</th><th>kind</th><th>status</th><th>verify</th>"
            f"<th>changed</th><th>duration_ms</th><th>output_path</th><th>artifact_path</th></tr></thead>"
            f"<tbody>{timeline_rows}</tbody>"
            f"</table></div></section>"
            f"<section class='panel'><h2>Workspace</h2>{workspace_html}</section>"
            f"<section class='panel'><h2>Plan File</h2>{selected_plan_file_html}</section>"
            f"</div>"
            f"</main>"
        )
        run_id_json = json.dumps(state.selected_run.id if state.selected_run else "")
        run_status_json = json.dumps(state.live_output_status or (state.selected_run.status if state.selected_run else ""))
        dashboard_script = (
            "window.addEventListener('DOMContentLoaded', () => {\n"
            "  wireCopyButtons(document);\n"
            f"  const runId = {run_id_json};\n"
            f"  const runStatus = {run_status_json};\n"
            "  const outputNode = document.getElementById('live_output_stream');\n"
            "  if (runId && outputNode) {\n"
            "    const refreshOutput = async () => {\n"
            "      const params = new URLSearchParams({ run_id: runId });\n"
            "      const response = await fetch(`/api/run-output?${params.toString()}`);\n"
            "      if (!response.ok) return;\n"
            "      const data = await response.json();\n"
            "      outputNode.textContent = data.output || '';\n"
            "    };\n"
            "    refreshOutput();\n"
            "    if (runStatus === 'running') {\n"
            "      window.setInterval(refreshOutput, 2000);\n"
            "    }\n"
            "  }\n"
            "});\n"
        )
        return self._page_shell(active_nav="Dashboard", body=body, extra_script=dashboard_script)


def serve_dashboard(
    repo_path: Path,
    host: str,
    port: int,
    db_path: Path | None = None,
    *,
    announce_url: str | None = None,
    tailscale: bool = False,
) -> int:
    app = DashboardApp(repo_path=repo_path, db_path=db_path)

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/check-repo":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0]
                status, message = check_repo_path(path_value)
                body = json.dumps({"status": status, "message": message}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/resolve-path":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0].strip()
                resolved = str(Path(path_value).expanduser().resolve()) if path_value else ""
                body = json.dumps({"resolved": resolved}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/list-plans":
                params = parse_qs(parsed.query)
                path_value = params.get("path", [""])[0]
                status, message, plans = list_plans_in_directory(path_value)
                body = json.dumps({"status": status, "message": message, "plans": plans}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/preflight":
                params = parse_qs(parsed.query)
                target_repo_value = params.get("target_repo", [""])[0]
                plans_dir_value = params.get("plans_dir", [""])[0]
                selected_plan_names = [name.strip() for name in params.get("selected_plans", []) if name.strip()]
                body = json.dumps(
                    summarize_preflight_for_dashboard(
                        target_repo_value,
                        plans_dir_value,
                        selected_plan_names=selected_plan_names or None,
                        provider_override=params.get("provider_override", [""])[0].strip() or None,
                    )
                ).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/api/run-output":
                params = parse_qs(parsed.query)
                run_id = params.get("run_id", [""])[0]
                run_data = app.load_live_run_data(run_id) if run_id else None
                output, output_path, status = app.read_live_output(run_data)
                body = json.dumps(
                    {
                        "run_id": run_id,
                        "status": status or "unknown",
                        "output_path": output_path,
                        "output": output or "",
                    }
                ).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path not in {"/", "/actions", "/projects"}:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            params = parse_qs(parsed.query)
            action_message = params.get("message", [None])[0]
            try:
                if parsed.path == "/actions":
                    body = app.render_actions_page(action_message=action_message)
                elif parsed.path == "/projects":
                    body = app.render_projects_page(action_message=action_message)
                else:
                    run_id = params.get("run_id", [None])[0]
                    plan_execution_id = params.get("plan_execution_id", [None])[0]
                    selected_plan_file = params.get("selected_plan_file", [None])[0]
                    body = app.render_page(
                        run_id=run_id,
                        plan_execution_id=plan_execution_id,
                        action_message=action_message,
                        selected_plan_file=selected_plan_file,
                    )
            except PlanError as exc:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    (
                        "<!doctype html><html><body><h1>kctl</h1>"
                        f"<p>{_escape(exc)}</p></body></html>"
                    ).encode("utf-8")
                )
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {
                "/actions/index",
                "/actions/run-many",
                "/actions/create-plan",
                "/actions/rerun-plan",
                "/actions/add-project",
                "/actions/remove-project",
                "/actions/run-plan-across-projects",
            }:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            form_data = parse_qs(self.rfile.read(content_length).decode("utf-8"))
            run_id: str | None = None
            redirect_to = "/actions"
            try:
                if parsed.path == "/actions/rerun-plan":
                    plan_file_path_value = form_data.get("plan_file_path", [""])[0].strip()
                    if not plan_file_path_value:
                        raise PlanError("Plan file path is required.")
                    plan_path = Path(plan_file_path_value)
                    if not plan_path.exists():
                        raise PlanError(f"Plan file not found: {plan_path}")
                    plans_dir = plan_path.parent
                    plan_file_name = plan_path.name
                    provider_override = form_data.get("provider_override", [""])[0].strip() or None
                    run_id = app.start_run_many(plans_dir, concurrency=1, selected_plan_names=[plan_file_name], provider_override=provider_override)
                    message = f"Rerun started for {plan_file_name}."
                    redirect_to = "/"
                elif parsed.path == "/actions/add-project":
                    project_path_value = form_data.get("project_path", [""])[0].strip()
                    if not project_path_value:
                        raise PlanError("Project path is required.")
                    app.add_tracked_project(Path(project_path_value).expanduser())
                    message = f"Tracked project: {Path(project_path_value).expanduser().resolve()}"
                    redirect_to = "/projects"
                elif parsed.path == "/actions/remove-project":
                    project_path_value = form_data.get("project_path", [""])[0].strip()
                    if not project_path_value:
                        raise PlanError("Project path is required.")
                    app.remove_tracked_project(project_path_value)
                    message = f"Removed tracked project: {Path(project_path_value).expanduser().resolve()}"
                    redirect_to = "/projects"
                elif parsed.path == "/actions/run-plan-across-projects":
                    selected_plan_names = [name.strip() for name in form_data.get("selected_plans", []) if name.strip()]
                    if len(selected_plan_names) != 1:
                        raise PlanError("Select exactly one plan to run across projects.")
                    project_paths = [
                        Path(path_value).expanduser().resolve()
                        for path_value in form_data.get("project_paths", [])
                        if path_value.strip()
                    ]
                    if not project_paths:
                        raise PlanError("Select at least one tracked project.")
                    target_repo_value = form_data.get("target_repo", [""])[0].strip()
                    if not target_repo_value:
                        raise PlanError("Target repo is required.")
                    target_repo = Path(target_repo_value).expanduser().resolve()
                    plans_dir_value = form_data.get("plans_dir", [""])[0].strip()
                    plans_dir = (
                        Path(plans_dir_value).expanduser()
                        if plans_dir_value
                        else app.plans_dir_for_repo(target_repo)
                    )
                    plan_path = plans_dir.resolve() / selected_plan_names[0]
                    if not plan_path.exists():
                        raise PlanError(f"Plan file not found: {plan_path}")
                    provider_override = form_data.get("provider_override", [""])[0].strip() or None
                    run_id = app.start_run_plan_across_projects(
                        plan_path=plan_path,
                        project_paths=project_paths,
                        provider_override=provider_override,
                    )
                    message = (
                        f"Started single-plan run for {selected_plan_names[0]} "
                        f"across {len(project_paths)} project(s)."
                    )
                    redirect_to = "/"
                elif parsed.path == "/actions/index":
                    app.run_index_now()
                    message = "Index refreshed."
                    redirect_to = "/actions"
                elif parsed.path == "/actions/create-plan":
                    template_name = form_data.get("template_name", [""])[0].strip()
                    target_repo_value = form_data.get("target_repo", [""])[0].strip()
                    output_path_value = form_data.get("output_path", [""])[0].strip()
                    objective = form_data.get("objective", [""])[0].strip()
                    if not template_name:
                        raise PlanError("Template name is required.")
                    if not target_repo_value:
                        raise PlanError("Target repo is required.")
                    if not output_path_value:
                        raise PlanError("Plan file name is required.")
                    if not objective:
                        raise PlanError("Objective is required.")
                    created_path = app.create_plan(
                        target_repo=Path(target_repo_value).expanduser(),
                        template_name=template_name,
                        output_name=output_path_value,
                        objective=objective,
                        force=form_data.get("force", [""])[0] == "1",
                    )
                    message = f"Created plan at {created_path}."
                    redirect_to = "/actions"
                else:
                    target_repo_value = form_data.get("target_repo", [""])[0].strip()
                    if not target_repo_value:
                        raise PlanError("Target repo is required.")
                    target_repo = Path(target_repo_value).expanduser().resolve()
                    plans_dir_value = form_data.get("plans_dir", [""])[0].strip()
                    plans_dir = Path(plans_dir_value).expanduser() if plans_dir_value else app.plans_dir_for_repo(target_repo)
                    concurrency_value = form_data.get("concurrency", ["1"])[0].strip() or "1"
                    concurrency = int(concurrency_value)
                    if concurrency < 1:
                        raise PlanError("Concurrency must be at least 1.")
                    selected_plan_names = [name.strip() for name in form_data.get("selected_plans", []) if name.strip()]
                    provider_override = form_data.get("provider_override", [""])[0].strip() or None
                    run_id = app.start_run_many(plans_dir, concurrency, selected_plan_names=selected_plan_names or None, provider_override=provider_override)
                    if len(selected_plan_names) == 1:
                        message = f"Started plan run for {selected_plan_names[0]} in {plans_dir}."
                    else:
                        message = f"Started run-many for {plans_dir}."
                    redirect_to = "/"
            except (PlanError, ValueError) as exc:
                message = str(exc)
            if redirect_to == "/projects":
                location = _page_link("/projects", message=message)
            elif redirect_to == "/actions":
                location = _page_link("/actions", message=message)
            else:
                location = _link({}, run_id=run_id, message=message)
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"kctl dashboard listening on {host}:{port}", flush=True)
    for url in build_dashboard_access_urls(host, port, announce_url=announce_url, tailscale=tailscale):
        print(f"dashboard url: {url}", flush=True)
    if tailscale and announce_url is None:
        print("tailscale note: hostname URL requires MagicDNS or equivalent tailnet DNS.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
