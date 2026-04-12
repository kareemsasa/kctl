from __future__ import annotations

import os
import shlex
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifacts import multi_run_dir, resolve_storage, single_run_dir, worktree_run_root
from .git import ensure_git_repo, get_repo_root, resolve_repo
from .types import PlanError


DEFAULT_VERIFY_SHELL = "sh -lc"


@dataclass(frozen=True)
class PreflightIssue:
    code: str
    message: str
    fix: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "fix": self.fix}


@dataclass(frozen=True)
class PreflightReport:
    ok: bool
    scope: str
    repo_root: Path | None
    run_root: Path | None
    worktree_root: Path | None
    required_binaries: list[str]
    required_env: list[str]
    issues: list[PreflightIssue]
    environment: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "scope": self.scope,
            "repo_root": str(self.repo_root) if self.repo_root is not None else None,
            "run_root": str(self.run_root) if self.run_root is not None else None,
            "worktree_root": str(self.worktree_root) if self.worktree_root is not None else None,
            "required_binaries": self.required_binaries,
            "required_env": self.required_env,
            "issues": [issue.to_dict() for issue in self.issues],
            "environment": self.environment,
        }

    def format_blockers(self) -> list[str]:
        return [
            f"- [{issue.code}] {issue.message} Fix: {issue.fix}"
            for issue in self.issues
        ]


def collect_required_env(plan: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("required_env",):
        raw_value = plan.get(key)
        if isinstance(raw_value, list):
            values.extend(str(item).strip() for item in raw_value if str(item).strip())
    defaults = plan.get("defaults")
    if isinstance(defaults, dict):
        raw_defaults_required = defaults.get("required_env")
        if isinstance(raw_defaults_required, list):
            values.extend(str(item).strip() for item in raw_defaults_required if str(item).strip())
    return sorted(dict.fromkeys(values))


def parse_verify_shell_binary(shell_value: str | None) -> str:
    shell_text = shell_value or DEFAULT_VERIFY_SHELL
    shell_parts = shlex.split(shell_text)
    if not shell_parts:
        raise PlanError("verify_shell must not be empty.")
    return shell_parts[0]


def _looks_like_env_assignment(value: str) -> bool:
    if "=" not in value:
        return False
    name, _, _ = value.partition("=")
    return bool(name) and name.replace("_", "").isalnum() and not name[0].isdigit()


def extract_command_binary(command_text: str) -> str | None:
    try:
        parts = shlex.split(command_text)
    except ValueError:
        return None
    for part in parts:
        if _looks_like_env_assignment(part):
            continue
        return part
    return None


def collect_required_binaries(plan: dict[str, Any]) -> list[str]:
    binaries = {"git"}
    defaults = plan.get("defaults") or {}
    steps = plan.get("steps") or []
    default_verify = defaults.get("verify")
    default_verify_shell = defaults.get("verify_shell")
    if isinstance(default_verify, str) and default_verify.strip():
        binaries.add(parse_verify_shell_binary(default_verify_shell))
        default_verify_binary = extract_command_binary(default_verify)
        if default_verify_binary:
            binaries.add(default_verify_binary)
    for step in steps:
        effective_type = ((step.get("_kctl_step_type") or {}).get("effective_type")) or ""
        if effective_type in {"analyze", "change", "review"}:
            binaries.add("codex")
        step_verify = step.get("verify")
        step_verify_shell = step.get("verify_shell")
        if isinstance(step_verify, str) and step_verify.strip():
            binaries.add(parse_verify_shell_binary(step_verify_shell or default_verify_shell))
            step_verify_binary = extract_command_binary(step_verify)
            if step_verify_binary:
                binaries.add(step_verify_binary)
        commands = step.get("commands")
        if isinstance(commands, list) and any(isinstance(item, str) and item.strip() for item in commands):
            binaries.add(parse_verify_shell_binary(step_verify_shell or default_verify_shell))
            for command in commands:
                if not isinstance(command, str) or not command.strip():
                    continue
                command_binary = extract_command_binary(command)
                if command_binary:
                    binaries.add(command_binary)
    return sorted(binaries)


def _record_issue(issues: list[PreflightIssue], code: str, message: str, fix: str) -> None:
    issues.append(PreflightIssue(code=code, message=message, fix=fix))


def _check_writable_dir(path: Path, issues: list[PreflightIssue], *, code: str, label: str) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _record_issue(
            issues,
            code,
            f"{label} could not be created: {path} ({exc})",
            f"Create {path} with write access for the service user.",
        )
        return
    try:
        with tempfile.NamedTemporaryFile(dir=path, prefix=".kctl-preflight-", delete=True):
            pass
    except OSError as exc:
        _record_issue(
            issues,
            code,
            f"{label} is not writable: {path} ({exc})",
            f"Grant write access to {path} for the service user or change the kctl storage location.",
        )


def _check_binaries(required_binaries: list[str], path_value: str, issues: list[PreflightIssue]) -> None:
    if not path_value.strip():
        _record_issue(
            issues,
            "missing_path",
            "PATH is empty.",
            "Set PATH for the launching shell or systemd user service so kctl can resolve codex, git, and verification tools.",
        )
        return
    for binary in required_binaries:
        if shutil.which(binary, path=path_value) is not None:
            continue
        _record_issue(
            issues,
            "missing_binary",
            f"Required binary '{binary}' is not available on PATH.",
            f"Install '{binary}' or add its directory to PATH. Current PATH: {path_value}",
        )


def _check_required_env(required_env: list[str], issues: list[PreflightIssue]) -> None:
    for name in required_env:
        if os.environ.get(name):
            continue
        _record_issue(
            issues,
            "missing_env",
            f"Required environment variable '{name}' is not defined.",
            f"Set '{name}' in the shell or service environment before starting the run.",
        )


def _resolve_repo_root(plan_path: Path, plan: dict[str, Any], issues: list[PreflightIssue]) -> Path | None:
    repo_path = resolve_repo(plan_path, plan["repo"])
    if not repo_path.exists():
        _record_issue(
            issues,
            "missing_repo",
            f"Repository path does not exist: {repo_path}",
            f"Create the repository at {repo_path} or fix the plan's repo field.",
        )
        return None
    if not repo_path.is_dir():
        _record_issue(
            issues,
            "invalid_repo",
            f"Repository path is not a directory: {repo_path}",
            f"Point the plan's repo field at a git working tree directory.",
        )
        return None
    try:
        ensure_git_repo(repo_path)
        return get_repo_root(repo_path)
    except PlanError as exc:
        _record_issue(
            issues,
            "invalid_repo",
            str(exc),
            f"Initialize a git repository at {repo_path} or fix the plan's repo field.",
        )
        return None


def preflight_single_run(
    plan_path: Path,
    plan: dict[str, Any],
    run_id: str,
    run_output_dir_override: Path | None = None,
) -> PreflightReport:
    issues: list[PreflightIssue] = []
    repo_root = _resolve_repo_root(plan_path, plan, issues)
    required_env = collect_required_env(plan)
    required_binaries = collect_required_binaries(plan)
    path_value = os.environ.get("PATH", "")
    _check_binaries(required_binaries, path_value, issues)
    _check_required_env(required_env, issues)
    storage = resolve_storage()
    run_root: Path | None = None
    if repo_root is not None:
        run_root = run_output_dir_override or single_run_dir(repo_root, run_id, storage_mode=storage.mode)
        _check_writable_dir(run_root, issues, code="run_dir_unwritable", label="Run output directory")
    return PreflightReport(
        ok=not issues,
        scope="single_run",
        repo_root=repo_root,
        run_root=run_root,
        worktree_root=None,
        required_binaries=required_binaries,
        required_env=required_env,
        issues=issues,
        environment={"path": path_value, "storage_mode": storage.mode},
    )


def preflight_multi_run(
    *,
    plans_dir: Path,
    run_id: str,
    plan_specs: list[Any],
    normalized_plans: dict[str, dict[str, Any]],
) -> PreflightReport:
    issues: list[PreflightIssue] = []
    if not plans_dir.exists():
        _record_issue(
            issues,
            "missing_plans_dir",
            f"Plans directory does not exist: {plans_dir}",
            "Create the plans directory or fix the path passed to `kctl plans run-many`.",
        )
    elif not plans_dir.is_dir():
        _record_issue(
            issues,
            "invalid_plans_dir",
            f"Plans directory is not a directory: {plans_dir}",
            "Pass a directory that contains YAML plan files.",
        )

    repo_roots: set[Path] = set()
    for spec in plan_specs:
        repo_root = _resolve_repo_root(spec.plan_path, normalized_plans[spec.plan_id], issues)
        if repo_root is not None:
            repo_roots.add(repo_root)
    repo_root = next(iter(repo_roots)) if len(repo_roots) == 1 else None
    if len(repo_roots) > 1:
        _record_issue(
            issues,
            "multiple_repos",
            "All plans in a run-many directory must target the same git repository.",
            "Split the plans into separate directories or point them at one repository root.",
        )

    required_env = sorted(
        {
            env_name
            for plan in normalized_plans.values()
            for env_name in collect_required_env(plan)
        }
    )
    required_binaries = sorted(
        {
            binary
            for plan in normalized_plans.values()
            for binary in collect_required_binaries(plan)
        }
    )
    path_value = os.environ.get("PATH", "")
    _check_binaries(required_binaries, path_value, issues)
    _check_required_env(required_env, issues)

    storage = resolve_storage()
    run_root: Path | None = None
    worktree_root: Path | None = None
    if repo_root is not None:
        run_root = multi_run_dir(repo_root, run_id, storage_mode=storage.mode)
        worktree_root = worktree_run_root(repo_root, run_id, storage_mode=storage.mode)
        _check_writable_dir(run_root, issues, code="run_dir_unwritable", label="Run directory")
        _check_writable_dir(worktree_root, issues, code="workspace_dir_unwritable", label="Workspace directory")

    return PreflightReport(
        ok=not issues,
        scope="multi_run",
        repo_root=repo_root,
        run_root=run_root,
        worktree_root=worktree_root,
        required_binaries=required_binaries,
        required_env=required_env,
        issues=issues,
        environment={"path": path_value, "plans_dir": str(plans_dir.resolve()), "storage_mode": storage.mode},
    )
