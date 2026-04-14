# AGENTS Notes

## Current Refactor State

- `kctl_pkg/ui_dashboard.py` has been partially decomposed.
- Shared stateless helpers, styles, shared scripts, and small HTML builders live in `kctl_pkg/ui_dashboard_support.py`.
- Actions page rendering and its preflight/plan-preview browser script now live in `kctl_pkg/ui_dashboard_actions.py`.
- Dashboard overview/detail rendering and run detail rendering now live in `kctl_pkg/ui_dashboard_runs.py`.
- Dashboard state loading and live-run adaptation now live in `kctl_pkg/ui_dashboard_state.py`, but `DashboardApp` still exposes wrapper methods for compatibility.
- Dashboard HTTP/API dispatch helpers now live in `kctl_pkg/ui_dashboard_http.py`; `serve_dashboard(...)` remains the integration point.
- Server bootstrap and the request handler now live in `kctl_pkg/ui_dashboard_server.py`; `kctl_pkg.ui_dashboard.serve_dashboard(...)` is a wrapper for compatibility.
- Project tracking and project page rendering now live in `kctl_pkg/ui_dashboard_projects.py`.
- Session runtime and session page rendering now live in `kctl_pkg/ui_dashboard_sessions.py`.
- GET route dispatch now goes through `DashboardApp.render_route(...)`.
- POST action dispatch now goes through `DashboardApp.handle_action(...)`.
- `kctl_pkg/ui_service.py` now forwards `PATH`, present `KCTL_*` variables, and common provider credential env vars into the generated systemd user service so dashboard-launched runs keep the same runtime context as the installing shell.
- `kctl_pkg/ui_index.py` now preserves explicit effective step types (`analyze`, `change`, `verify`, `review`) when indexing step timelines instead of collapsing non-verify work into `agent`.
- Tests intentionally patch some seams through `kctl_pkg.ui_dashboard`, so preserve those patch points when extracting code.

## Practical Guidance

- Before changing dashboard behavior, run `bash scripts/test`.
- Prefer extracting cohesive subsystems into helper modules while keeping `DashboardApp` as the public integration surface.
- If you move logic out of `ui_dashboard.py`, keep wrapper methods or compatibility imports when tests patch module-level names there.
- The next clean extractions after the current work are:
  - import/cleanup consolidation inside `kctl_pkg/ui_dashboard.py` as behavior-neutral follow-up only

## Known Constraints

- The repo may contain user work in progress. Do not revert unrelated changes.
- `tests/test_ui_dashboard.py` is the main regression net for dashboard refactors.
- `provider_override` is supported in runner, multi-plan, dashboard actions, and now CLI entrypoints.
