from __future__ import annotations

import json

from .types import PlanError
from .ui_dashboard_state import adapt_overview_for_live_runs, merge_runs_with_live_data
from .ui_dashboard_support import (
    _escape,
    _failure_reason_label,
    _fmt_ts,
    _lifecycle_badge,
    _link,
    _page_link,
    _preflight_run_snapshot,
    _render_collapsible_section,
    _render_select_text_link,
    _render_attention_card,
    _render_preflight_item_html,
    _run_detail_link,
    _status_badge,
    _status_class,
    available_providers,
)
from .ui_read import get_plan_execution, get_repository_overview, get_run, get_workspace, list_attention_items, list_plan_executions, list_runs, list_step_executions


def _tail_lines_text(text: str | None, line_count: int = 50) -> str:
    if not text:
        return ""
    return "\n".join(text.splitlines()[-line_count:])


def _select_recent_runs(runs: list[object], limit: int = 5) -> list[object]:
    active_runs = [run for run in runs if str(getattr(run, "status", "")) in {"running", "stopping"}]
    active_ids = {str(getattr(run, "id", "")) for run in active_runs}
    other_runs = [run for run in runs if str(getattr(run, "id", "")) not in active_ids]
    ordered = [*active_runs, *other_runs]
    return ordered[:limit]


def _render_run_items_html(runs: list[object], empty_message: str) -> str:
    return "".join(
        (
            f"<a class='list-item {_status_class(run.status)}' href='{_escape(_run_detail_link(run.id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(run.id)}</strong>"
            f"{_status_badge(run.status)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(run.started_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Concurrency</span> {_escape(run.concurrency)}</div>"
            "</a>"
        )
        for run in runs
    ) or f"<div class='empty'>{empty_message}</div>"


def _render_live_output_section_html(output: str | None, output_path: str | None) -> str:
    if output_path is None:
        return "<section class='panel'><h2>Live Output</h2><div class='empty'>No live output captured for this run yet.</div></section>"
    tail_output = _tail_lines_text(output)
    return (
        "<section class='panel'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
        "<h2>Live Output</h2>"
        + _render_select_text_link(
            "Select tail text",
            target_id="live_output_tail",
        )
        + "</div>"
        "<div class='help'>Mobile: tap Select tail text, then use the browser copy action. You can also tap the tail box below directly.</div>"
        f"<textarea id='live_output_tail' class='code-block' readonly onclick='this.focus();this.select();' style='min-height:120px'>{_escape(tail_output)}</textarea>"
        f"<pre id='live_output_stream' class='code-block live-output'>{_escape(output or '')}</pre>"
        "</section>"
    )


def _render_plan_rerun_controls(plan: object) -> str:
    plan_file_path = getattr(plan, "plan_file_path", None)
    if not plan_file_path:
        return ""
    plan_execution_id = getattr(plan, "id", None)
    plan_execution_id_input = (
        f"<input type='hidden' name='plan_execution_id' value='{_escape(plan_execution_id)}'>"
        if plan_execution_id
        else ""
    )
    current_step_key = getattr(plan, "current_step_key", None)
    verify_status = getattr(plan, "verify_status", None)
    forms = [
        "<form method='post' action='/actions/rerun-plan'>"
        f"<input type='hidden' name='plan_file_path' value='{_escape(plan_file_path)}'>"
        f"{plan_execution_id_input}"
        "<button type='submit'>Rerun Full Plan</button>"
        "</form>"
    ]
    if current_step_key:
        forms.append(
            "<form method='post' action='/actions/rerun-plan'>"
            f"<input type='hidden' name='plan_file_path' value='{_escape(plan_file_path)}'>"
            f"{plan_execution_id_input}"
            f"<input type='hidden' name='from_step' value='{_escape(current_step_key)}'>"
            f"<button type='submit'>Rerun From {_escape(current_step_key)}</button>"
            "</form>"
        )
    if verify_status == "failed" or current_step_key == "verify":
        forms.append(
            "<form method='post' action='/actions/rerun-plan'>"
            f"<input type='hidden' name='plan_file_path' value='{_escape(plan_file_path)}'>"
            f"{plan_execution_id_input}"
            "<input type='hidden' name='only_step' value='verify'>"
            "<button type='submit'>Rerun Verify Only</button>"
            "</form>"
        )
    return "<div style='display:flex;flex-direction:column;gap:8px;margin-top:12px'>" + "".join(forms) + "</div>"


def _render_run_stop_controls(run_id: str, status: str, plan_execution_id: str | None = None) -> str:
    if status != "running":
        return ""
    href = _run_detail_link(f"{run_id}/stop", plan_execution_id=plan_execution_id)
    return (
        f"<div style='margin:12px 0 0'><a class='btn-danger' href='{_escape(href)}' "
        "style='display:inline-block;text-decoration:none'>Stop Run</a></div>"
    )


def _render_run_preflight_html(preflight: dict[str, object] | None) -> str:
    if preflight is None:
        return ""
    preflight_items = preflight.get("items", {})
    return _render_collapsible_section(
        "Launch Snapshot",
        (
            f"<div class='repo-check' style='margin-top:8px' data-status='{_escape(preflight.get('status'))}'>{_escape(preflight.get('message'))}</div>"
            f"<div class='help'>captured_at={_escape(preflight.get('captured_at'))}</div>"
            f"<div class='preflight-grid' style='margin-top:8px'>"
            + "".join(
                _render_preflight_item_html(label, item)
                for label, item in (
                    ("Repo", preflight_items.get("repo") or {}),
                    ("Plans Dir", preflight_items.get("plans_dir") or {}),
                    ("Binaries", preflight_items.get("binaries") or {}),
                    ("Writable Paths", preflight_items.get("writable_paths") or {}),
                    ("Required Env", preflight_items.get("required_env") or {}),
                )
            )
            + "</div>"
        ),
        heading_tag="h3",
        style="margin-top:12px",
    )


def _display_value(value: object, fallback: str = "—") -> object:
    return value if value not in {None, ""} else fallback


def _render_overview_bar(overview: object) -> str:
    return (
        "<div class='overview-bar'>"
        f"<span><strong>{_escape(overview.run_count)}</strong> runs</span>"
        f"<span><strong>{_escape(overview.active_run_count)}</strong> active</span>"
        f"<span class='{_status_class('failure') if overview.failed_run_count else ''}'><strong>{_escape(overview.failed_run_count)}</strong> failed</span>"
        f"<span><strong>{_escape(overview.running_plan_count)}</strong> running</span>"
        f"<span><strong>{_escape(overview.blocked_plan_count)}</strong> blocked</span>"
        f"<span><strong>{_escape(overview.stale_workspace_count)}</strong> stale</span>"
        "</div>"
    )


def _render_plan_cards_html(plans: list[object]) -> str:
    return "".join(
        (
            f"<a class='card {_status_class(plan.status)}' href='{_escape(_run_detail_link(plan.run_id, plan_execution_id=plan.id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(plan.plan_slug)}</strong>"
            f"{_status_badge(plan.status, failure_reason=plan.failure_reason)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(_display_value(plan.current_step_key))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(plan.verify_status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(plan.changed_files_count)} files</div>"
            "</a>"
        )
        for plan in plans
    ) or "<div class='empty'>No plan executions.</div>"


def _render_workspace_items_html(workspaces: list[object]) -> str:
    return "".join(
        (
            f"<a class='card {_status_class(workspace.status)}' href='{_escape(_run_detail_link(workspace.run_id, plan_execution_id=workspace.plan_execution_id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(workspace.plan_slug)}</strong>"
            f"{_lifecycle_badge(workspace.lifecycle)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Status</span> {_escape(workspace.status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(workspace.verify_status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
            "</a>"
        )
        for workspace in workspaces
    ) or "<div class='empty'>No workspaces indexed.</div>"


def _render_plan_file_items_html(app: object, base_params: dict[str, str]) -> str:
    return "".join(
        (
            f"<a class='list-item status-neutral' href='{_escape(_link(base_params, selected_plan_file=plan_path))}'>"
            f"<strong>{_escape(plan_name)}</strong>"
            f"<div class='help'>{_escape(plan_path)}</div>"
            "</a>"
        )
        for plan_name, plan_path in (
            (path.name, str(path.resolve()))
            for pattern in ("*.yaml", "*.yml")
            for path in sorted(app.default_plans_dir.glob(pattern))
            if path.is_file()
        )
    ) or "<div class='empty'>No plan files found.</div>"


def _render_dashboard_selected_run_html(selected_run: object | None, selected_run_preflight: dict[str, object] | None) -> str:
    if selected_run is None:
        return "<section class='panel'><h2>Run Detail</h2><div class='empty'>No run selected.</div></section>"
    run_preflight_html = _render_run_preflight_html(selected_run_preflight)
    return (
        "<section class='panel'>"
        "<h2>Run Detail</h2>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
        f"<strong style='font-family:monospace;font-size:0.9em'>{_escape(selected_run.id)}</strong>"
        f"{_status_badge(selected_run.status)}"
        f"</div>"
        f"<div class='kv-row'><span class='kv-label'>Source</span> {_escape(selected_run.launch_source)}</div>"
        f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(selected_run.started_at))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Ended</span> {_escape(_fmt_ts(selected_run.ended_at))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Plans</span> {_escape(selected_run.plan_execution_count)} total</div>"
        f"{_render_run_stop_controls(selected_run.id, selected_run.status)}"
        f"{run_preflight_html}"
        "</section>"
    )


def _render_workspace_detail_html(workspace: object | None) -> str:
    if workspace is None:
        return "<div class='empty'>No workspace recorded.</div>"
    return (
        f"<div class='kv-row'><span class='kv-label'>Status</span> {_escape(workspace.status)}</div>"
        f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(_display_value(workspace.branch_name))}</code></div>"
        f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(workspace.created_at))}</div>"
        + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(workspace.released_at))}</div>" if workspace.released_at else "")
        + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
    )


def _render_timeline_rows(steps: list[object]) -> str:
    return "".join(
        (
            "<tr>"
            f"<td>{_escape(step.sequence_index)}</td>"
            f"<td>{_escape(step.step_key)}</td>"
            f"<td>{_escape(step.kind)}</td>"
            f"<td>{_status_badge(step.status)}</td>"
            f"<td>{_escape(step.verify_status)}</td>"
            f"<td>{_escape(step.changed_files_count)}</td>"
            f"<td>{_escape(step.duration_ms)}</td>"
            "</tr>"
        )
        for step in steps
    ) or "<tr><td colspan='7' class='empty'>No steps recorded.</td></tr>"


def _render_dashboard_selected_plan_html(selected_plan: object | None) -> str:
    if selected_plan is None:
        return "<section class='panel'><h2>Plan Execution Detail</h2><div class='empty'>No plan selected.</div></section>"
    return (
        "<section class='panel'>"
        "<h2>Plan Execution Detail</h2>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
        f"<strong>{_escape(selected_plan.plan_slug)}</strong>"
        f"{_status_badge(selected_plan.status, failure_reason=selected_plan.failure_reason)}"
        f"</div>"
        f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(_display_value(selected_plan.current_step_key))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(selected_plan.verify_status)}</div>"
        f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(selected_plan.changed_files_count)} files</div>"
        + (f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(_display_value(selected_plan.branch_name))}</code></div>" if selected_plan.branch_name else "")
        + (f"<div class='kv-row'><span class='kv-label'>Log</span> <code style='font-size:0.85em'>{_escape(selected_plan.log_path)}</code></div>" if selected_plan.log_path else "")
        + (f"<div class='kv-row'><span class='kv-label'>Failure</span> {_escape(_failure_reason_label(selected_plan.failure_reason))}</div>" if selected_plan.failure_reason else "")
        + _render_plan_rerun_controls(selected_plan)
        + "</section>"
    )


def _render_selected_plan_file_html(selected_plan_file_name: str | None, selected_plan_file_path: str | None, selected_plan_file_contents: str | None) -> str:
    if selected_plan_file_name is None or selected_plan_file_contents is None:
        return "<div class='empty'>No plan file selected.</div>"
    return (
        f"<div><strong>{_escape(selected_plan_file_name)}</strong></div>"
        f"<div>path={_escape(selected_plan_file_path)}</div>"
        f"<pre class='code-block'>{_escape(selected_plan_file_contents)}</pre>"
    )


def _render_dashboard_live_output_script(run_id: str, run_status: str) -> str:
    return (
        "window.addEventListener('DOMContentLoaded', () => {\n"
        "  wireCopyButtons(document);\n"
        f"  const runId = {json.dumps(run_id)};\n"
        f"  const runStatus = {json.dumps(run_status)};\n"
        "  const outputNode = document.getElementById('live_output_stream');\n"
        "  const tailNode = document.getElementById('live_output_tail');\n"
        "  if (runId && outputNode) {\n"
        "    const renderTail = (text) => {\n"
        "      if (!tailNode) return;\n"
        "      const lines = (text || '').split(/\\r?\\n/);\n"
        "      tailNode.value = lines.slice(-50).join('\\n');\n"
        "    };\n"
        "    renderTail(outputNode.textContent || '');\n"
        "    const refreshOutput = async () => {\n"
        "      const params = new URLSearchParams({ run_id: runId });\n"
        "      const response = await fetch(`/api/run-output?${params.toString()}`);\n"
        "      if (!response.ok) return;\n"
        "      const data = await response.json();\n"
        "      outputNode.textContent = data.output || '';\n"
        "      renderTail(data.output || '');\n"
        "    };\n"
        "    refreshOutput();\n"
        "    if (runStatus === 'running' || runStatus === 'stopping') {\n"
        "      window.setInterval(refreshOutput, 2000);\n"
        "    }\n"
        "  }\n"
        "});\n"
    )


def _render_run_header_html(selected_run: object, selected_run_preflight: dict[str, object] | None, plan_execution_id: str | None = None) -> str:
    run_preflight_html = _render_run_preflight_html(selected_run_preflight)
    return (
        "<section class='panel'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
        f"<strong style='font-family:monospace;font-size:0.9em'>{_escape(selected_run.id)}</strong>"
        f"{_status_badge(selected_run.status)}"
        f"</div>"
        f"<div class='kv-row'><span class='kv-label'>Source</span> {_escape(selected_run.launch_source)}</div>"
        f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(selected_run.started_at))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Ended</span> {_escape(_fmt_ts(selected_run.ended_at))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Plans</span>"
        f" {_escape(selected_run.plan_execution_count)} total"
        f" &mdash; {_escape(selected_run.passed_count)} passed,"
        f" {_escape(selected_run.failed_count)} failed,"
        f" {_escape(selected_run.running_count)} running,"
        f" {_escape(selected_run.blocked_count)} blocked</div>"
        f"{_render_run_stop_controls(selected_run.id, selected_run.status, plan_execution_id=plan_execution_id)}"
        f"{run_preflight_html}"
        "</section>"
    )


def _render_run_selected_plan_html(selected_plan: object | None, workspace: object | None, steps: list[object]) -> str:
    if selected_plan is None:
        return ""
    workspace_html = ""
    if workspace is not None:
        workspace_html = (
            "<div style='margin-top:8px;padding-top:8px;border-top:1px solid #eee'>"
            f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(_display_value(workspace.branch_name))}</code></div>"
            f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(workspace.created_at))}</div>"
            + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(workspace.released_at))}</div>" if workspace.released_at else "")
            + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
            "</div>"
        )
    timeline_rows = _render_timeline_rows(steps)
    return (
        "<section class='panel'>"
        "<h2>Plan Execution Detail</h2>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
        f"<strong>{_escape(selected_plan.plan_slug)}</strong>"
        f"{_status_badge(selected_plan.status, failure_reason=selected_plan.failure_reason)}"
        f"</div>"
        f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(_display_value(selected_plan.current_step_key))}</div>"
        f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(selected_plan.verify_status)}</div>"
        f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(selected_plan.changed_files_count)} files</div>"
        + (f"<div class='kv-row'><span class='kv-label'>Log</span> <code style='font-size:0.85em'>{_escape(selected_plan.log_path)}</code></div>" if selected_plan.log_path else "")
        + (f"<div class='kv-row'><span class='kv-label'>Failure</span> {_escape(_failure_reason_label(selected_plan.failure_reason))}</div>" if selected_plan.failure_reason else "")
        + _render_plan_rerun_controls(selected_plan)
        + workspace_html
        + "<h3 style='margin:16px 0 8px'>Step Timeline</h3>"
        + "<div class='table-scroll'><table>"
        + "<thead><tr><th>#</th><th>Step</th><th>Type</th><th>Status</th><th>Verify</th><th>Files</th><th>ms</th></tr></thead>"
        + f"<tbody>{timeline_rows}</tbody>"
        + "</table></div>"
        + "</section>"
    )


def _render_run_live_output_script(run_id: str, run_status: str) -> str:
    return (
        "window.addEventListener('DOMContentLoaded', () => {\n"
        "  wireCopyButtons(document);\n"
        f"  const runId = {json.dumps(run_id)};\n"
        f"  const runStatus = {json.dumps(run_status)};\n"
        "  const outputNode = document.getElementById('live_output_stream');\n"
        "  const tailNode = document.getElementById('live_output_tail');\n"
        "  if (runId && outputNode && (runStatus === 'running' || runStatus === 'stopping')) {\n"
        "    const renderTail = (text) => {\n"
        "      if (!tailNode) return;\n"
        "      const lines = (text || '').split(/\\r?\\n/);\n"
        "      tailNode.value = lines.slice(-50).join('\\n');\n"
        "    };\n"
        "    renderTail(outputNode.textContent || '');\n"
        "    const refresh = async () => {\n"
        "      const params = new URLSearchParams({ run_id: runId });\n"
        "      const resp = await fetch(`/api/run-output?${params.toString()}`);\n"
        "      if (!resp.ok) return;\n"
        "      const data = await resp.json();\n"
        "      outputNode.textContent = data.output || '';\n"
        "      renderTail(data.output || '');\n"
        "    };\n"
        "    refresh();\n"
        "    window.setInterval(refresh, 2000);\n"
        "  } else if (tailNode && outputNode) {\n"
        "    const lines = (outputNode.textContent || '').split(/\\r?\\n/);\n"
        "    tailNode.value = lines.slice(-50).join('\\n');\n"
        "  }\n"
        "});\n"
    )


def render_run_stop_confirm_page(
    app: object,
    run_id: str,
    plan_execution_id: str | None = None,
) -> str:
    confirm_href = _run_detail_link(f"{run_id}/stop", plan_execution_id=plan_execution_id, message="confirm")
    cancel_href = _run_detail_link(run_id, plan_execution_id=plan_execution_id)
    body = (
        "<main class='single-column'>"
        "<div class='column'>"
        f"<a class='back-link' href='{_escape(cancel_href)}'>&larr; Back To Run</a>"
        "<section class='panel'>"
        "<h2>Stop Run</h2>"
        f"<div class='help'>Request a safe stop for running run <code>{_escape(run_id)}</code>.</div>"
        "<div class='help' style='margin-top:8px'>This asks the runner to terminate the active child process and mark the run as stopped.</div>"
        "<div style='display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:16px'>"
        f"<a class='btn-danger' href='{_escape(confirm_href)}' style='display:inline-block;text-decoration:none'>Confirm Stop Run</a>"
        f"<a href='{_escape(cancel_href)}'>Cancel</a>"
        "</div>"
        "</section>"
        "</div>"
        "</main>"
    )
    return app._page_shell(active_nav="Dashboard", body=body)


def render_dashboard_page(
    app: object,
    run_id: str | None = None,
    plan_execution_id: str | None = None,
    action_message: str | None = None,
    selected_plan_file: str | None = None,
) -> str:
    if run_id is not None or plan_execution_id is not None or selected_plan_file is not None:
        return render_dashboard_detail_page(
            app,
            run_id=run_id,
            plan_execution_id=plan_execution_id,
            action_message=action_message,
            selected_plan_file=selected_plan_file,
        )

    overview = get_repository_overview(app.repo_path, db_path=app.db_path)
    attention_items = list_attention_items(app.repo_path, db_path=app.db_path)
    runs, live_unindexed_runs = merge_runs_with_live_data(
        app,
        list_runs(app.repo_path, db_path=app.db_path),
    )
    overview = adapt_overview_for_live_runs(overview, runs, live_unindexed_runs)
    active_runs = [run for run in runs if str(getattr(run, "status", "")) in {"running", "stopping"}]
    runs = _select_recent_runs(runs, limit=5)
    providers = available_providers()
    overview_html = _render_overview_bar(overview)

    attention_items_html = "".join(
        _render_attention_card(item, providers=providers) for item in attention_items
    ) or "<div class='empty'>Nothing needs attention right now.</div>"

    active_run_items_html = _render_run_items_html(
        active_runs,
        "No active runs right now.",
    )
    run_items_html = _render_run_items_html(
        runs,
        "No runs yet. <a href='/actions'>Go to Actions</a> to launch your first plan.",
    )

    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
    attention_section_html = _render_collapsible_section(
        "Attention",
        attention_items_html,
    )
    body = (
        f"{overview_html}"
        f"<main class='single-column'>"
        f"<div class='column'>"
        f"{notice_html}"
        f"{attention_section_html}"
        f"<section class='panel'><h2>Active Runs</h2>{active_run_items_html}</section>"
        f"<section class='panel'><h2>Recent Runs</h2>{run_items_html}</section>"
        f"</div>"
        f"</main>"
    )
    return app._page_shell(active_nav="Dashboard", body=body)


def render_dashboard_detail_page(
    app: object,
    *,
    run_id: str | None = None,
    plan_execution_id: str | None = None,
    action_message: str | None = None,
    selected_plan_file: str | None = None,
) -> str:
    state = app.load_state(
        run_id=run_id,
        plan_execution_id=plan_execution_id,
        action_message=action_message,
        selected_plan_file=selected_plan_file,
    )
    base_params: dict[str, str] = {}
    if state.selected_run is not None:
        base_params["run_id"] = state.selected_run.id
    if state.selected_plan is not None:
        base_params["plan_execution_id"] = state.selected_plan.id

    overview_html = _render_overview_bar(state.overview)
    notice_html = f"<div class='notice'>{_escape(state.action_message)}</div>" if state.action_message else ""
    attention_items_html = "".join(
        _render_attention_card(item, providers=state.available_providers)
        for item in state.attention_items
    ) or "<div class='empty'>Nothing needs attention right now.</div>"
    workspace_items_html = _render_workspace_items_html(state.workspaces)
    plan_file_items_html = _render_plan_file_items_html(app, base_params)
    run_items_html = "".join(
        (
            f"<a class='list-item {_status_class(run.status)}' href='{_escape(_run_detail_link(run.id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(run.id)}</strong>"
            f"{_status_badge(run.status)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(run.started_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Concurrency</span> {_escape(run.concurrency)}</div>"
            "</a>"
        )
        for run in state.runs
    ) or "<div class='empty'>No runs yet. <a href='/actions'>Go to Actions</a> to launch your first plan.</div>"
    selected_run_html = _render_dashboard_selected_run_html(state.selected_run, state.selected_run_preflight)
    plan_items_html = _render_plan_cards_html(state.plan_cards)
    workspace_detail_html = _render_workspace_detail_html(state.workspace)
    timeline_rows = _render_timeline_rows(state.steps)
    selected_plan_html = _render_dashboard_selected_plan_html(state.selected_plan)
    selected_plan_file_html = _render_selected_plan_file_html(
        state.selected_plan_file_name,
        state.selected_plan_file_path,
        state.selected_plan_file_contents,
    )

    attention_section_html = _render_collapsible_section(
        "Attention",
        attention_items_html,
    )

    body = (
        f"{overview_html}"
        "<main>"
        "<div class='column dashboard-secondary-column'>"
        f"{notice_html}"
        f"{attention_section_html}"
        f"<section class='panel'><h2>Workspaces</h2>{workspace_items_html}</section>"
        f"<section class='panel'><h2>Plans</h2>{plan_file_items_html}</section>"
        f"<section class='panel'><h2>Runs</h2>{run_items_html}</section>"
        "</div>"
        "<div class='column dashboard-primary-column'>"
        f"{selected_run_html}"
        f"<section class='panel'><h2>Plan Executions</h2>{plan_items_html}</section>"
        f"{selected_plan_html}"
        f"{_render_live_output_section_html(state.live_output, state.live_output_path)}"
        f"<section class='panel'><h2>Step Timeline</h2><div class='table-scroll'><table><thead><tr><th>#</th><th>Step</th><th>Type</th><th>Status</th><th>Verify</th><th>Files</th><th>ms</th></tr></thead><tbody>{timeline_rows}</tbody></table></div></section>"
        f"<section class='panel'><h2>Workspace</h2>{workspace_detail_html}</section>"
        f"<section class='panel'><h2>Plan File</h2>{selected_plan_file_html}</section>"
        "</div>"
        "</main>"
    )

    dashboard_script = ""
    if state.selected_run is not None:
        dashboard_script = _render_dashboard_live_output_script(
            state.selected_run.id,
            state.live_output_status or state.selected_run.status,
        )

    return app._page_shell(active_nav="Dashboard", body=body, extra_script=dashboard_script)


def render_run_page(
    app: object,
    run_id: str,
    plan_execution_id: str | None = None,
    action_message: str | None = None,
) -> str:
    live_run_data = app.load_live_run_data(run_id)
    try:
        selected_run = get_run(app.repo_path, run_id, db_path=app.db_path)
        plan_cards = list_plan_executions(app.repo_path, run_id, db_path=app.db_path)
    except PlanError:
        if live_run_data is not None:
            selected_run = app._build_run_detail_from_live_data(live_run_data)
            plan_cards = app._build_plan_cards_from_live_data(live_run_data)
        else:
            raise
    if live_run_data is not None and (
        str(live_run_data.get("status") or "") == "running" or bool(live_run_data.get("stop_requested"))
    ):
        selected_run = app._build_run_detail_from_live_data(live_run_data)
        plan_cards = app._build_plan_cards_from_live_data(live_run_data)

    selected_run_preflight = _preflight_run_snapshot(
        live_run_data or app.load_saved_run_data(selected_run.run_root_path)
    )

    if plan_execution_id is None and plan_cards:
        plan_execution_id = plan_cards[0].id

    selected_plan = None
    steps = []
    workspace = None
    if plan_execution_id is not None:
        try:
            selected_plan = get_plan_execution(plan_execution_id, app.repo_path, db_path=app.db_path)
            steps = list_step_executions(plan_execution_id, app.repo_path, db_path=app.db_path)
            workspace = get_workspace(plan_execution_id, app.repo_path, db_path=app.db_path)
        except PlanError:
            if live_run_data is not None:
                selected_plan = next((p for p in plan_cards if p.id == plan_execution_id), None)
                steps = app._build_live_steps_from_run_data(live_run_data, plan_execution_id)

    live_output, live_output_path, live_output_status = app.read_live_output(live_run_data)
    run_header_html = _render_run_header_html(selected_run, selected_run_preflight, plan_execution_id=plan_execution_id)
    plan_items_html = _render_plan_cards_html(plan_cards)
    selected_plan_html = _render_run_selected_plan_html(selected_plan, workspace, steps)
    live_section_html = _render_live_output_section_html(live_output, live_output_path)
    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""

    body = (
        "<main class='single-column'>"
        "<div class='column'>"
        f"<a class='back-link' href='/'>&larr; Dashboard</a>"
        f"{notice_html}"
        f"{run_header_html}"
        f"<section class='panel'><h2>Plan Executions</h2>{plan_items_html}</section>"
        f"{selected_plan_html}"
        f"{live_section_html}"
        "</div>"
        "</main>"
    )
    run_script = _render_run_live_output_script(
        run_id,
        live_output_status or selected_run.status,
    )
    return app._page_shell(active_nav="Dashboard", body=body, extra_script=run_script)
