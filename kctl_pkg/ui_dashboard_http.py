from __future__ import annotations

import json
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlencode

from .git import check_remote_connectivity, get_full_diff, get_project_git_summary
from .types import PlanError
from .ui_dashboard_support import _escape, _link, _page_link, check_repo_path, list_plans_in_directory


def handle_api_get(
    app: object,
    path: str,
    params: dict[str, list[str]],
    *,
    summarize_preflight: object,
) -> tuple[int, str, bytes] | None:
    if path == "/api/check-repo":
        path_value = params.get("path", [""])[0]
        status, message = check_repo_path(path_value)
        body = json.dumps({"status": status, "message": message}).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/resolve-path":
        path_value = params.get("path", [""])[0].strip()
        resolved = str(Path(path_value).expanduser().resolve()) if path_value else ""
        body = json.dumps({"resolved": resolved}).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/list-plans":
        path_value = params.get("path", [""])[0]
        status, message, plans = list_plans_in_directory(path_value)
        body = json.dumps({"status": status, "message": message, "plans": plans}).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/preflight":
        target_repo_value = params.get("target_repo", [""])[0]
        plans_dir_value = params.get("plans_dir", [""])[0]
        selected_plan_names = [name.strip() for name in params.get("selected_plans", []) if name.strip()]
        body = json.dumps(
            summarize_preflight(
                target_repo_value,
                plans_dir_value,
                selected_plan_names=selected_plan_names or None,
                provider_override=params.get("provider_override", [""])[0].strip() or None,
            )
        ).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/run-output":
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
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/project-git-status":
        path_value = params.get("path", [""])[0].strip()
        if path_value:
            summary = get_project_git_summary(Path(path_value).expanduser().resolve())
        else:
            summary = {"path": "", "available": False, "error": "no path provided"}
        body = json.dumps(summary).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/project-remote-check":
        path_value = params.get("path", [""])[0].strip()
        remote_name = params.get("remote", ["origin"])[0].strip()
        if path_value:
            repo = Path(path_value).expanduser().resolve()
            result = check_remote_connectivity(repo, remote_name)
        else:
            result = {"ok": False, "remote": remote_name, "message": "no path provided"}
        body = json.dumps(result).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/project-git-diff":
        path_value = params.get("path", [""])[0].strip()
        if path_value:
            repo = Path(path_value).expanduser().resolve()
            diff_text = get_full_diff(repo)
        else:
            diff_text = ""
        body = json.dumps({"diff": diff_text}).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    if path == "/api/session-output":
        session_id = params.get("id", [""])[0].strip()
        if session_id:
            meta = app.get_session(session_id)
            output = app.read_session_output(session_id)
            status = str(meta.get("status") or "unknown") if meta else "unknown"
            messages = list(meta.get("messages") or []) if meta else []
            token_warning = str(meta.get("token_warning") or "") if meta else ""
        else:
            output = ""
            status = "unknown"
            messages = []
            token_warning = ""
        body = json.dumps(
            {
                "id": session_id,
                "status": status,
                "output": output,
                "messages": messages,
                "token_warning": token_warning,
            }
        ).encode("utf-8")
        return HTTPStatus.OK, "application/json; charset=utf-8", body
    return None


def handle_page_get(app: object, path: str, params: dict[str, list[str]]) -> tuple[int, str, bytes]:
    if path not in {
        "/",
        "/actions",
        "/projects",
        "/projects/detail",
        "/runs/detail",
        "/sessions",
        "/sessions/detail",
    }:
        raise PlanError("Not Found")
    try:
        body = app.render_route(path, params)
    except PlanError as exc:
        body = "<!doctype html><html><body><h1>kctl</h1><p>%s</p></body></html>" % _escape(exc)
    return HTTPStatus.OK, "text/html; charset=utf-8", body.encode("utf-8")


def resolve_action_redirect(action_result: object, message: str) -> str:
    redirect_to = action_result.redirect_to
    run_id = action_result.run_id
    if redirect_to.startswith("/projects/detail?"):
        if "message=" not in redirect_to:
            return redirect_to + "&" + urlencode({"message": message})
        return redirect_to
    if redirect_to.startswith("/sessions/detail"):
        return redirect_to if "?" in redirect_to else redirect_to + f"?message={message}"
    if redirect_to == "/sessions":
        return _page_link("/sessions", message=message)
    if redirect_to == "/projects":
        return _page_link("/projects", message=message)
    if redirect_to == "/actions":
        return _page_link("/actions", message=message)
    return _link({}, run_id=run_id, message=message)
