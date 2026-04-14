from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .types import PlanError
from .ui_dashboard_http import handle_api_get, handle_page_get, resolve_action_redirect
from .ui_dashboard_support import _escape, build_dashboard_access_urls


_POST_ACTION_PATHS = {
    "/actions/index",
    "/actions/run-many",
    "/actions/create-plan",
    "/actions/rerun-plan",
    "/actions/add-project",
    "/actions/remove-project",
    "/actions/run-plan-across-projects",
    "/actions/start-session",
    "/actions/stop-session",
    "/actions/session-reply",
    "/actions/project-git-commit",
    "/actions/project-git-switch",
    "/actions/project-git-create-branch",
    "/actions/project-git-pull",
    "/actions/project-git-push",
    "/actions/project-git-stash",
    "/actions/project-git-stash-pop",
    "/actions/project-git-discard",
}


def serve_dashboard(
    *,
    app: object,
    host: str,
    port: int,
    summarize_preflight: object,
    announce_url: str | None = None,
    tailscale: bool = False,
) -> int:
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            try:
                api_response = handle_api_get(
                    app,
                    parsed.path,
                    params,
                    summarize_preflight=summarize_preflight,
                )
                if api_response is not None:
                    status_code, content_type, body = api_response
                    self.send_response(status_code)
                    self.send_header("Content-Type", content_type)
                    self.end_headers()
                    self.wfile.write(body)
                    return
                status_code, content_type, body = handle_page_get(app, parsed.path, params)
            except PlanError as exc:
                if str(exc) == "Not Found":
                    self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                    return
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    (f"<!doctype html><html><body><h1>kctl</h1><p>{_escape(exc)}</p></body></html>").encode("utf-8")
                )
                return
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in _POST_ACTION_PATHS:
                self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            form_data = parse_qs(self.rfile.read(content_length).decode("utf-8"))
            try:
                action_result = app.handle_action(parsed.path, form_data)
                message = action_result.message
            except (PlanError, ValueError) as exc:
                message = str(exc)
                action_result = type("DashboardActionFallback", (), {"redirect_to": "/actions", "run_id": None})()
            location = resolve_action_redirect(action_result, message)
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
