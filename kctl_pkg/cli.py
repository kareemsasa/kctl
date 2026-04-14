from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .multi import print_run_status, run_many_plans
from .output import BufferedOutputSink, ConsoleOutputSink, NullOutputSink
from .plan import init_plan, resolve_plan_path
from .runner import run_plan
from .terminal import set_color_enabled, style_status_text
from .types import PlanError
from .ui_dashboard import serve_dashboard
from .ui_index import index_repository_state, print_ui_run_detail, print_ui_runs, print_ui_workspaces
from .ui_service import (
    default_service_name,
    default_service_path,
    ensure_systemctl_success,
    install_dashboard_service,
    render_dashboard_service,
    run_systemctl_user,
)


def add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the raw Codex stream instead of the filtered terminal view.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color output.",
    )
    parser.add_argument(
        "--approve-each-step",
        action="store_true",
        help="Prompt before starting the next step.",
    )
    parser.add_argument(
        "--branch",
        help="Create or switch to this branch before running the plan.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Create a local commit at the end if the run succeeds.",
    )
    parser.add_argument(
        "--commit-message",
        help="Commit message to use with --commit.",
    )
    parser.add_argument(
        "--allow-dirty-start",
        action="store_true",
        help="Allow --commit even if the repo is already dirty before the run starts.",
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help="Run scope and test review passes after successful steps that leave new changes.",
    )
    parser.add_argument(
        "--provider",
        choices=("codex", "claude"),
        dest="provider_override",
        help="Override the agent provider for this run.",
    )
    parser.add_argument(
        "--from-step",
        help="Start the run at this step id and continue through the remaining steps.",
    )
    parser.add_argument(
        "--only-step",
        help="Run only this single step id from the plan.",
    )


def discover_git_repos(root_path: Path) -> list[Path]:
    if not root_path.exists():
        raise PlanError(f"Batch root does not exist: {root_path}")
    if not root_path.is_dir():
        raise PlanError(f"Batch root is not a directory: {root_path}")
    repos = sorted({git_dir.parent.resolve() for git_dir in root_path.rglob(".git") if git_dir.is_dir()})
    if not repos:
        raise PlanError(f"No git repositories found under: {root_path}")
    return repos


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kctl", description="Run Codex plans against git repositories.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a YAML plan.")
    run_parser.add_argument("plan", help="Path to the YAML plan file.")
    add_run_options(run_parser)

    batch_parser = subparsers.add_parser("batch", help="Run the same YAML plan against multiple git repositories.")
    batch_parser.add_argument("plan", help="Path to the YAML plan file.")
    batch_parser.add_argument(
        "--root",
        required=True,
        help="Root directory to scan recursively for git repositories.",
    )
    batch_parser.add_argument(
        "--output-mode",
        choices=("stream", "grouped", "quiet"),
        default="stream",
        help="Batch console output mode.",
    )
    add_run_options(batch_parser)

    plans_parser = subparsers.add_parser("plans", help="Run or inspect multiple plans for one repository.")
    plans_subparsers = plans_parser.add_subparsers(dest="plans_command", required=True)

    run_many_parser = plans_subparsers.add_parser("run-many", help="Run all plans in a directory.")
    run_many_parser.add_argument("plans_dir", help="Directory containing plan YAML files.")
    run_many_parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Maximum number of plans to run concurrently.",
    )
    run_many_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the raw Codex stream instead of the filtered terminal view.",
    )
    run_many_parser.add_argument(
        "--provider",
        choices=("codex", "claude"),
        dest="provider_override",
        help="Override the agent provider for this run.",
    )

    status_parser = plans_subparsers.add_parser("status", help="Show the latest status for a multi-plan run.")
    status_parser.add_argument("target", help="Plans directory, run directory, or run id.")

    ui_parser = subparsers.add_parser("ui", help="Index and inspect execution state for a future UI.")
    ui_subparsers = ui_parser.add_subparsers(dest="ui_command", required=True)

    ui_index_parser = ui_subparsers.add_parser("index", help="Index .kctl runs and worktrees into SQLite.")
    ui_index_parser.add_argument("repo", help="Repository root to index.")
    ui_index_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )

    ui_runs_parser = ui_subparsers.add_parser("runs", help="List indexed runs for a repository.")
    ui_runs_parser.add_argument("repo", help="Repository root to inspect.")
    ui_runs_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )

    ui_run_parser = ui_subparsers.add_parser("run", help="Show indexed detail for one run.")
    ui_run_parser.add_argument("repo", help="Repository root to inspect.")
    ui_run_parser.add_argument("run_id", help="Run id to inspect.")
    ui_run_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )

    ui_workspaces_parser = ui_subparsers.add_parser("workspaces", help="List indexed workspaces for a repository.")
    ui_workspaces_parser.add_argument("repo", help="Repository root to inspect.")
    ui_workspaces_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )

    ui_dashboard_parser = ui_subparsers.add_parser("dashboard", help="Launch a minimal local run dashboard.")
    ui_dashboard_parser.add_argument("repo", help="Repository root to inspect.")
    ui_dashboard_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )
    ui_dashboard_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind.",
    )
    ui_dashboard_parser.add_argument(
        "--port",
        type=int,
        default=8421,
        help="Port to listen on.",
    )
    ui_dashboard_parser.add_argument(
        "--tailscale",
        action="store_true",
        help="Bind on all interfaces and print a Tailscale-friendly hostname URL hint.",
    )
    ui_dashboard_parser.add_argument(
        "--announce-url",
        help="Optional public URL to print alongside the local bind address.",
    )

    ui_service_parser = ui_subparsers.add_parser("service", help="Print or install a user systemd service for the dashboard.")
    ui_service_subparsers = ui_service_parser.add_subparsers(dest="ui_service_command", required=True)

    ui_service_print_parser = ui_service_subparsers.add_parser("print", help="Print the dashboard service unit.")
    ui_service_print_parser.add_argument("repo", help="Repository root to inspect.")
    ui_service_print_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )
    ui_service_print_parser.add_argument(
        "--name",
        default=default_service_name(),
        help="Systemd unit base name.",
    )
    ui_service_print_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind.",
    )
    ui_service_print_parser.add_argument(
        "--port",
        type=int,
        default=8421,
        help="Port to listen on.",
    )
    ui_service_print_parser.add_argument(
        "--no-tailscale",
        dest="tailscale",
        action="store_false",
        default=True,
        help="Do not include --tailscale in the service command.",
    )
    ui_service_print_parser.add_argument(
        "--announce-url",
        help="Optional public URL to print alongside the local bind address.",
    )
    ui_service_print_parser.add_argument(
        "--python",
        dest="python_executable",
        help="Python executable to run inside the service.",
    )

    ui_service_install_parser = ui_service_subparsers.add_parser("install", help="Install and optionally start the dashboard service.")
    ui_service_install_parser.add_argument("repo", help="Repository root to inspect.")
    ui_service_install_parser.add_argument(
        "--db-path",
        help="Optional path to the SQLite state database.",
    )
    ui_service_install_parser.add_argument(
        "--name",
        default=default_service_name(),
        help="Systemd unit base name.",
    )
    ui_service_install_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind.",
    )
    ui_service_install_parser.add_argument(
        "--port",
        type=int,
        default=8421,
        help="Port to listen on.",
    )
    ui_service_install_parser.add_argument(
        "--no-tailscale",
        dest="tailscale",
        action="store_false",
        default=True,
        help="Do not include --tailscale in the service command.",
    )
    ui_service_install_parser.add_argument(
        "--announce-url",
        help="Optional public URL to print alongside the local bind address.",
    )
    ui_service_install_parser.add_argument(
        "--python",
        dest="python_executable",
        help="Python executable to run inside the service.",
    )
    ui_service_install_parser.add_argument(
        "--no-enable",
        action="store_true",
        help="Install the service file without enabling it.",
    )
    ui_service_install_parser.add_argument(
        "--no-start",
        action="store_true",
        help="Install the service file without starting it.",
    )

    init_parser = subparsers.add_parser("init", help="Materialize a YAML plan from a named template.")
    init_parser.add_argument("template_name", help="Template name from kctl-plan-templates.yaml.")
    init_parser.add_argument("output_path", help="Path where the generated plan YAML will be written.")
    init_parser.add_argument(
        "--repo",
        required=True,
        help="Repository path to write into the generated plan.",
    )
    init_parser.add_argument(
        "--objective",
        required=True,
        help="Objective text to write into the generated plan.",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    init_parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    set_color_enabled(not getattr(args, "no_color", False))

    if args.command == "run":
        try:
            return run_plan(
                resolve_plan_path(args.plan),
                verbose=args.verbose,
                approve_each_step=args.approve_each_step,
                branch=args.branch,
                commit=args.commit,
                commit_message=args.commit_message,
                allow_dirty_start=args.allow_dirty_start,
                review_enabled=args.review,
                provider_override=args.provider_override,
                from_step=args.from_step,
                only_step=args.only_step,
            )
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "batch":
        try:
            if args.approve_each_step:
                raise PlanError("interactive prompts are not supported in batch mode")
            plan_path = resolve_plan_path(args.plan)
            repo_paths = discover_git_repos(Path(args.root).expanduser().resolve())
            overall_exit_code = 0
            console_sink = ConsoleOutputSink()
            for repo_path in repo_paths:
                repo_name = repo_path.name
                repo_prefix = f"[{repo_name}] "
                if args.output_mode == "stream":
                    console_sink.write_line(
                        style_status_text(f"== {repo_name} ==", "success", bold=True)
                    )
                    repo_sink = ConsoleOutputSink(prefix=repo_prefix)
                elif args.output_mode == "grouped":
                    repo_sink = BufferedOutputSink(prefix=repo_prefix)
                else:
                    console_sink.write_line(
                        style_status_text(f"== {repo_name} ==", "success", bold=True)
                    )
                    repo_sink = NullOutputSink()
                repo_exit_code = run_plan(
                    plan_path,
                    verbose=args.verbose,
                    approve_each_step=args.approve_each_step,
                    branch=args.branch,
                    commit=args.commit,
                    commit_message=args.commit_message,
                    allow_dirty_start=args.allow_dirty_start,
                    review_enabled=args.review,
                    repo_override=str(repo_path),
                    output_sink=repo_sink,
                    interactive=False,
                    provider_override=args.provider_override,
                )
                if args.output_mode == "grouped":
                    console_sink.write_line(
                        style_status_text(f"== {repo_name} ==", "success", bold=True)
                    )
                    repo_sink.flush_to(console_sink)
                    console_sink.write_line("")
                repo_status = "success" if repo_exit_code == 0 else "failure"
                console_sink.write_line(
                    style_status_text(
                        f"  {repo_name}: exit {repo_exit_code}",
                        repo_status,
                        bold=True,
                    )
                )
                if repo_exit_code != 0:
                    overall_exit_code = 1
            return overall_exit_code
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "init":
        try:
            return init_plan(
                template_name=args.template_name,
                output_path=Path(args.output_path).resolve(),
                repo=args.repo,
                objective=args.objective,
                force=args.force,
            )
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "plans":
        try:
            if args.plans_command == "run-many":
                return run_many_plans(
                    Path(args.plans_dir).expanduser().resolve(),
                    concurrency=args.concurrency,
                    verbose=args.verbose,
                    provider_override=args.provider_override,
                )
            if args.plans_command == "status":
                return print_run_status(args.target)
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "ui":
        try:
            repo_path = Path(args.repo).expanduser().resolve()
            db_path = Path(args.db_path).expanduser().resolve() if getattr(args, "db_path", None) else None
            if args.ui_command == "index":
                counts = index_repository_state(repo_path, db_path=db_path)
                print(
                    style_status_text(
                        f"Indexed: {counts['runs']} runs  "
                        f"{counts['plan_executions']} plans  "
                        f"{counts['step_executions']} steps  "
                        f"{counts['workspaces']} workspaces",
                        "success",
                        bold=True,
                    ),
                    flush=True,
                )
                return 0
            if args.ui_command == "runs":
                return print_ui_runs(repo_path, db_path=db_path)
            if args.ui_command == "run":
                return print_ui_run_detail(repo_path, args.run_id, db_path=db_path)
            if args.ui_command == "workspaces":
                return print_ui_workspaces(repo_path, db_path=db_path)
            if args.ui_command == "dashboard":
                host = "0.0.0.0" if args.tailscale and args.host == "127.0.0.1" else args.host
                return serve_dashboard(
                    repo_path,
                    host=host,
                    port=args.port,
                    db_path=db_path,
                    announce_url=args.announce_url,
                    tailscale=args.tailscale,
                )
            if args.ui_command == "service":
                service_path = default_service_path(args.name)
                service_text = render_dashboard_service(
                    repo_path=repo_path,
                    host=args.host,
                    port=args.port,
                    tailscale=args.tailscale,
                    announce_url=args.announce_url,
                    db_path=db_path,
                    python_executable=args.python_executable,
                )
                if args.ui_service_command == "print":
                    print(service_text, end="", flush=True)
                    return 0
                if args.ui_service_command == "install":
                    install_dashboard_service(service_path, service_text)
                    ensure_systemctl_success(run_systemctl_user("daemon-reload"), "daemon-reload")
                    unit_name = service_path.name
                    if not args.no_enable:
                        ensure_systemctl_success(run_systemctl_user("enable", unit_name), "enable")
                    if not args.no_start:
                        ensure_systemctl_success(run_systemctl_user("restart", unit_name), "restart")
                    print(
                        style_status_text(
                            f"Installed dashboard service at {service_path}",
                            "success",
                            bold=True,
                        ),
                        flush=True,
                    )
                    print(f"Manage with: systemctl --user status {service_path.name}", flush=True)
                    return 0
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    parser.error(f"Unknown command: {args.command}")
    return 2
