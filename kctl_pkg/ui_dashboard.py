from __future__ import annotations

import html
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

from .types import PlanError
from .ui_read import (
    PlanExecutionCard,
    RunDetail,
    RunListItem,
    StepTimelineItem,
    WorkspaceDetail,
    get_plan_execution,
    get_repository,
    get_run,
    get_workspace,
    list_plan_executions,
    list_runs,
    list_step_executions,
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


@dataclass(frozen=True)
class DashboardState:
    repo_name: str
    repo_root: str
    runs: list[RunListItem]
    selected_run: RunDetail | None
    plan_cards: list[PlanExecutionCard]
    selected_plan: PlanExecutionCard | None
    steps: list[StepTimelineItem]
    workspace: WorkspaceDetail | None


class DashboardApp:
    def __init__(self, repo_path: Path, db_path: Path | None = None) -> None:
        self.repo_path = repo_path.resolve()
        self.db_path = db_path.resolve() if db_path is not None else None

    def load_state(self, run_id: str | None = None, plan_execution_id: str | None = None) -> DashboardState:
        repository = get_repository(self.repo_path, db_path=self.db_path)
        runs = list_runs(self.repo_path, db_path=self.db_path)
        selected_run: RunDetail | None = None
        plan_cards: list[PlanExecutionCard] = []
        selected_plan: PlanExecutionCard | None = None
        steps: list[StepTimelineItem] = []
        workspace: WorkspaceDetail | None = None

        if run_id is None and runs:
            run_id = runs[0].id
        if run_id is not None:
            selected_run = get_run(self.repo_path, run_id, db_path=self.db_path)
            plan_cards = list_plan_executions(self.repo_path, run_id, db_path=self.db_path)

        if plan_execution_id is None and plan_cards:
            plan_execution_id = plan_cards[0].id
        if plan_execution_id is not None:
            selected_plan = get_plan_execution(plan_execution_id, self.repo_path, db_path=self.db_path)
            steps = list_step_executions(plan_execution_id, self.repo_path, db_path=self.db_path)
            workspace = get_workspace(plan_execution_id, self.repo_path, db_path=self.db_path)

        return DashboardState(
            repo_name=repository.name,
            repo_root=repository.root_path,
            runs=runs,
            selected_run=selected_run,
            plan_cards=plan_cards,
            selected_plan=selected_plan,
            steps=steps,
            workspace=workspace,
        )

    def render_page(self, run_id: str | None = None, plan_execution_id: str | None = None) -> str:
        state = self.load_state(run_id=run_id, plan_execution_id=plan_execution_id)
        base_params = {}
        if state.selected_run is not None:
            base_params["run_id"] = state.selected_run.id
        if state.selected_plan is not None:
            base_params["plan_execution_id"] = state.selected_plan.id

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
                "</section>"
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

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>kctl Dashboard</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 0;
      padding: 0;
      background: #f5f5f5;
      color: #111;
    }}
    header {{
      background: #1f2937;
      color: white;
      padding: 16px 20px;
    }}
    main {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 16px;
      padding: 16px;
    }}
    .column {{
      display: flex;
      flex-direction: column;
      gap: 16px;
    }}
    .panel {{
      background: white;
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 16px;
    }}
    .list-item, .card {{
      display: block;
      text-decoration: none;
      color: inherit;
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 8px;
      background: white;
    }}
    .list-item:hover, .card:hover {{
      border-color: #999;
    }}
    .status-success {{
      border-left: 4px solid #15803d;
    }}
    .status-failure {{
      border-left: 4px solid #b91c1c;
    }}
    .status-running {{
      border-left: 4px solid #1d4ed8;
    }}
    .status-neutral {{
      border-left: 4px solid #6b7280;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
    }}
    th, td {{
      text-align: left;
      padding: 8px;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: top;
    }}
    .empty {{
      color: #666;
      font-style: italic;
    }}
    code {{
      font-family: monospace;
      font-size: 0.95em;
    }}
  </style>
</head>
<body>
  <header>
    <h1>kctl Dashboard</h1>
    <div>repository={_escape(state.repo_name)} root={_escape(state.repo_root)}</div>
  </header>
  <main>
    <div class="column">
      <section class="panel">
        <h2>Runs</h2>
        {run_items}
      </section>
    </div>
    <div class="column">
      {selected_run_html}
      <section class="panel">
        <h2>Plan Executions</h2>
        {plan_items}
      </section>
      {selected_plan_html}
      <section class="panel">
        <h2>Step Timeline</h2>
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>step</th>
              <th>kind</th>
              <th>status</th>
              <th>verify</th>
              <th>changed</th>
              <th>duration_ms</th>
              <th>output_path</th>
              <th>artifact_path</th>
            </tr>
          </thead>
          <tbody>
            {timeline_rows}
          </tbody>
        </table>
      </section>
      <section class="panel">
        <h2>Workspace</h2>
        {workspace_html}
      </section>
    </div>
  </main>
</body>
</html>
"""


def serve_dashboard(repo_path: Path, host: str, port: int, db_path: Path | None = None) -> int:
    app = DashboardApp(repo_path=repo_path, db_path=db_path)

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path != "/":
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            params = parse_qs(parsed.query)
            run_id = params.get("run_id", [None])[0]
            plan_execution_id = params.get("plan_execution_id", [None])[0]
            try:
                body = app.render_page(run_id=run_id, plan_execution_id=plan_execution_id)
            except PlanError as exc:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    (
                        "<!doctype html><html><body><h1>kctl Dashboard</h1>"
                        f"<p>{_escape(exc)}</p></body></html>"
                    ).encode("utf-8")
                )
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"kctl dashboard listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
