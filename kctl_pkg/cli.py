from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .plan import init_plan, resolve_plan_path
from .runner import run_plan
from .terminal import set_color_enabled, style_status_text
from .types import PlanError


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
    add_run_options(batch_parser)

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
            )
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "batch":
        try:
            plan_path = resolve_plan_path(args.plan)
            repo_paths = discover_git_repos(Path(args.root).expanduser().resolve())
            overall_exit_code = 0
            for repo_path in repo_paths:
                print(style_status_text(f"== Batch Repo: {repo_path} ==", "success", bold=True), flush=True)
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
                )
                repo_status = "success" if repo_exit_code == 0 else "failure"
                print(
                    style_status_text(
                        f"Batch summary: repo={repo_path} exit_code={repo_exit_code}",
                        repo_status,
                        bold=True,
                    ),
                    flush=True,
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

    parser.error(f"Unknown command: {args.command}")
    return 2
