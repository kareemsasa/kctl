from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

from .types import PlanError
from .ui_dashboard_support import (
    _escape,
    _failure_reason_label,
    _fmt_ts,
    _lifecycle_badge,
    _link,
    _page_link,
    _preflight_run_snapshot,
    _render_collapsible_section,
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


def _adapt_recent_runs_with_live_status(app: object, runs: list[object]) -> list[object]:
    adapted_runs: list[object] = []
    for run in runs:
        live_run_data = app.load_live_run_data(run.id)
        if live_run_data is not None and str(live_run_data.get("status") or "") == "running" and bool(
            live_run_data.get("stop_requested")
        ):
            active_pids = live_run_data.get("active_pids")
            effective_status = (
                "stopped"
                if not (isinstance(active_pids, list) and any(str(value).strip() for value in active_pids))
                else "stopping"
            )
            if hasattr(run, "__dataclass_fields__"):
                adapted_runs.append(replace(run, status=effective_status))
            else:
                run_data = dict(vars(run))
                run_data["status"] = effective_status
                adapted_runs.append(SimpleNamespace(**run_data))
            continue
        adapted_runs.append(run)
    return adapted_runs


def _render_live_output_section_html(output: str | None, output_path: str | None) -> str:
    if output_path is None:
        return "<section class='panel'><h2>Live Output</h2><div class='empty'>No live output captured for this run yet.</div></section>"
    tail_output = _tail_lines_text(output)
    return (
        "<section class='panel'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
        "<h2>Live Output</h2>"
        "<button type='button' class='mini-button' data-copy-target='#live_output_stream' data-copy-last-lines='50' onclick='return window.kctlCopyButtonClick(this)'>Copy last 50 lines</button>"
        "</div>"
        "<div class='help'>Mobile fallback: tap the tail box below to select text with the browser copy UI.</div>"
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
    runs = _adapt_recent_runs_with_live_status(app, list_runs(app.repo_path, db_path=app.db_path)[:5])
    providers = available_providers()

    overview_html = (
        "<div class='overview-bar'>"
        f"<span><strong>{_escape(overview.run_count)}</strong> runs</span>"
        f"<span><strong>{_escape(overview.active_run_count)}</strong> active</span>"
        f"<span class='{_status_class('failure') if overview.failed_run_count else ''}'><strong>{_escape(overview.failed_run_count)}</strong> failed</span>"
        f"<span><strong>{_escape(overview.running_plan_count)}</strong> running</span>"
        f"<span><strong>{_escape(overview.blocked_plan_count)}</strong> blocked</span>"
        f"<span><strong>{_escape(overview.stale_workspace_count)}</strong> stale</span>"
        "</div>"
    )

    attention_items_html = "".join(
        _render_attention_card(item, providers=providers) for item in attention_items
    ) or "<div class='empty'>Nothing needs attention right now.</div>"

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
        for run in runs
    ) or "<div class='empty'>No runs yet. <a href='/actions'>Go to Actions</a> to launch your first plan.</div>"

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
    notice_html = f"<div class='notice'>{_escape(state.action_message)}</div>" if state.action_message else ""
    attention_items_html = "".join(
        _render_attention_card(item, providers=state.available_providers)
        for item in state.attention_items
    ) or "<div class='empty'>Nothing needs attention right now.</div>"
    workspace_items_html = "".join(
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
        for workspace in state.workspaces
    ) or "<div class='empty'>No workspaces indexed.</div>"
    plan_file_items_html = "".join(
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

    selected_run_html = "<section class='panel'><h2>Run Detail</h2><div class='empty'>No run selected.</div></section>"
    if state.selected_run is not None:
        run_preflight_html = ""
        if state.selected_run_preflight is not None:
            run_preflight_items = state.selected_run_preflight.get("items", {})
            run_preflight_html = _render_collapsible_section(
                "Launch Snapshot",
                (
                f"<div class='repo-check' style='margin-top:8px' data-status='{_escape(state.selected_run_preflight.get('status'))}'>{_escape(state.selected_run_preflight.get('message'))}</div>"
                f"<div class='help'>captured_at={_escape(state.selected_run_preflight.get('captured_at'))}</div>"
                f"<div class='preflight-grid' style='margin-top:8px'>"
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
                + "</div>"
                ),
                heading_tag="h3",
                style="margin-top:12px",
            )
        selected_run_html = (
            "<section class='panel'>"
            "<h2>Run Detail</h2>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
            f"<strong style='font-family:monospace;font-size:0.9em'>{_escape(state.selected_run.id)}</strong>"
            f"{_status_badge(state.selected_run.status)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Source</span> {_escape(state.selected_run.launch_source)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Started</span> {_escape(_fmt_ts(state.selected_run.started_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Ended</span> {_escape(_fmt_ts(state.selected_run.ended_at))}</div>"
            f"<div class='kv-row'><span class='kv-label'>Plans</span> {_escape(state.selected_run.plan_execution_count)} total</div>"
            f"{_render_run_stop_controls(state.selected_run.id, state.selected_run.status)}"
            f"{run_preflight_html}"
            "</section>"
        )

    plan_items_html = "".join(
        (
            f"<a class='card {_status_class(plan.status)}' href='{_escape(_run_detail_link(plan.run_id, plan_execution_id=plan.id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(plan.plan_slug)}</strong>"
            f"{_status_badge(plan.status, failure_reason=plan.failure_reason)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(plan.current_step_key or '\u2014')}</div>"
            f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(plan.verify_status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(plan.changed_files_count)} files</div>"
            "</a>"
        )
        for plan in state.plan_cards
    ) or "<div class='empty'>No plan executions.</div>"

    workspace_detail_html = "<div class='empty'>No workspace recorded.</div>"
    if state.workspace is not None:
        workspace_detail_html = (
            f"<div class='kv-row'><span class='kv-label'>Status</span> {_escape(state.workspace.status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(state.workspace.branch_name or '\u2014')}</code></div>"
            f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(state.workspace.created_at))}</div>"
            + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(state.workspace.released_at))}</div>" if state.workspace.released_at else "")
            + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(state.workspace.path)}</code></div>"
        )

    timeline_rows = "".join(
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
        for step in state.steps
    ) or "<tr><td colspan='7' class='empty'>No steps recorded.</td></tr>"

    selected_plan_html = "<section class='panel'><h2>Plan Execution Detail</h2><div class='empty'>No plan selected.</div></section>"
    if state.selected_plan is not None:
        selected_plan_html = (
            "<section class='panel'>"
            "<h2>Plan Execution Detail</h2>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
            f"<strong>{_escape(state.selected_plan.plan_slug)}</strong>"
            f"{_status_badge(state.selected_plan.status, failure_reason=state.selected_plan.failure_reason)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(state.selected_plan.current_step_key or '\u2014')}</div>"
            f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(state.selected_plan.verify_status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(state.selected_plan.changed_files_count)} files</div>"
            + (f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(state.selected_plan.branch_name or '\u2014')}</code></div>" if state.selected_plan.branch_name else "")
            + (f"<div class='kv-row'><span class='kv-label'>Log</span> <code style='font-size:0.85em'>{_escape(state.selected_plan.log_path)}</code></div>" if state.selected_plan.log_path else "")
            + (f"<div class='kv-row'><span class='kv-label'>Failure</span> {_escape(_failure_reason_label(state.selected_plan.failure_reason))}</div>" if state.selected_plan.failure_reason else "")
            + _render_plan_rerun_controls(state.selected_plan)
            + "</section>"
        )

    selected_plan_file_html = "<div class='empty'>No plan file selected.</div>"
    if state.selected_plan_file_name is not None and state.selected_plan_file_contents is not None:
        selected_plan_file_html = (
            f"<div><strong>{_escape(state.selected_plan_file_name)}</strong></div>"
            f"<div>path={_escape(state.selected_plan_file_path)}</div>"
            f"<pre class='code-block'>{_escape(state.selected_plan_file_contents)}</pre>"
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
        dashboard_script = (
            "window.addEventListener('DOMContentLoaded', () => {\n"
            "  wireCopyButtons(document);\n"
            f"  const runId = {json.dumps(state.selected_run.id)};\n"
            f"  const runStatus = {json.dumps(state.live_output_status or state.selected_run.status)};\n"
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

    run_preflight_html = ""
    if selected_run_preflight is not None:
        run_preflight_items = selected_run_preflight.get("items", {})
        run_preflight_html = _render_collapsible_section(
            "Launch Snapshot",
            (
            f"<div class='repo-check' style='margin-top:8px' data-status='{_escape(selected_run_preflight.get('status'))}'>{_escape(selected_run_preflight.get('message'))}</div>"
            f"<div class='help'>captured_at={_escape(selected_run_preflight.get('captured_at'))}</div>"
            f"<div class='preflight-grid' style='margin-top:8px'>"
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
            + "</div>"
            ),
            heading_tag="h3",
            style="margin-top:12px",
        )

    run_header_html = (
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

    plan_items_html = "".join(
        (
            f"<a class='card {_status_class(plan.status)}' href='{_escape(_run_detail_link(run_id, plan_execution_id=plan.id))}'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
            f"<strong>{_escape(plan.plan_slug)}</strong>"
            f"{_status_badge(plan.status, failure_reason=plan.failure_reason)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(plan.current_step_key or '\u2014')}</div>"
            f"<div class='kv-row'><span class='kv-label'>Verify</span> {_escape(plan.verify_status)}</div>"
            f"<div class='kv-row'><span class='kv-label'>Changed</span> {_escape(plan.changed_files_count)} files</div>"
            "</a>"
        )
        for plan in plan_cards
    ) or "<div class='empty'>No plan executions.</div>"

    selected_plan_html = ""
    if selected_plan is not None:
        workspace_html = ""
        if workspace is not None:
            workspace_html = (
                "<div style='margin-top:8px;padding-top:8px;border-top:1px solid #eee'>"
                f"<div class='kv-row'><span class='kv-label'>Branch</span> <code style='font-size:0.9em'>{_escape(workspace.branch_name or '\u2014')}</code></div>"
                f"<div class='kv-row'><span class='kv-label'>Created</span> {_escape(_fmt_ts(workspace.created_at))}</div>"
                + (f"<div class='kv-row'><span class='kv-label'>Released</span> {_escape(_fmt_ts(workspace.released_at))}</div>" if workspace.released_at else "")
                + f"<div class='kv-row'><span class='kv-label'>Path</span> <code style='font-size:0.85em'>{_escape(workspace.path)}</code></div>"
                "</div>"
            )
        timeline_rows = "".join(
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
        selected_plan_html = (
            "<section class='panel'>"
            "<h2>Plan Execution Detail</h2>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px'>"
            f"<strong>{_escape(selected_plan.plan_slug)}</strong>"
            f"{_status_badge(selected_plan.status, failure_reason=selected_plan.failure_reason)}"
            f"</div>"
            f"<div class='kv-row'><span class='kv-label'>Step</span> {_escape(selected_plan.current_step_key or '\u2014')}</div>"
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
    run_id_json = json.dumps(run_id)
    run_status_json = json.dumps(live_output_status or selected_run.status)
    run_script = (
        "window.addEventListener('DOMContentLoaded', () => {\n"
        "  wireCopyButtons(document);\n"
        f"  const runId = {run_id_json};\n"
        f"  const runStatus = {run_status_json};\n"
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
    return app._page_shell(active_nav="Dashboard", body=body, extra_script=run_script)
