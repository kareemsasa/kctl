from __future__ import annotations

import shutil
from pathlib import Path

from .process import run_command
from .types import CommandResult, PlanError


UNKNOWN_GIT_ERROR = "unknown git error"


def get_git_error_message(result: CommandResult) -> str:
    return result.stderr.strip() or result.stdout.strip() or UNKNOWN_GIT_ERROR


def resolve_repo(plan_path: Path, repo_value: str) -> Path:
    repo_path = Path(repo_value).expanduser()
    if not repo_path.is_absolute():
        repo_path = plan_path.parent / repo_path
    return repo_path.resolve()


def ensure_git_repo(repo_path: Path) -> None:
    if not repo_path.exists():
        raise PlanError(f"Target repo does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise PlanError(f"Target repo is not a directory: {repo_path}")

    git_check = run_command(["git", "rev-parse", "--show-toplevel"], cwd=repo_path)
    if git_check.exit_code != 0:
        message = get_git_error_message(git_check)
        raise PlanError(f"Target repo is not a git repo: {repo_path} ({message})")


def get_repo_root(repo_path: Path) -> Path:
    result = run_command(["git", "rev-parse", "--show-toplevel"], cwd=repo_path)
    if result.exit_code != 0:
        message = get_git_error_message(result)
        raise PlanError(f"Failed to determine git repo root: {message}")
    return Path(result.stdout.strip()).resolve()


def get_current_branch(repo_path: Path) -> str:
    result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_path)
    if result.exit_code != 0:
        message = get_git_error_message(result)
        raise PlanError(f"Failed to determine current branch: {message}")
    return result.stdout.strip()


def switch_to_branch(repo_path: Path, branch_name: str) -> None:
    exists_result = run_command(["git", "rev-parse", "--verify", f"refs/heads/{branch_name}"], cwd=repo_path)
    if exists_result.exit_code == 0:
        switch_result = run_command(["git", "switch", branch_name], cwd=repo_path)
    else:
        switch_result = run_command(["git", "switch", "-c", branch_name], cwd=repo_path)
    if switch_result.exit_code != 0:
        message = get_git_error_message(switch_result)
        raise PlanError(f"Failed to switch to branch '{branch_name}': {message}")


def get_git_status(repo_path: Path) -> CommandResult:
    return run_command(["git", "status", "--short"], cwd=repo_path)


def get_git_diff_stat(repo_path: Path) -> CommandResult:
    return run_command(["git", "diff", "--stat"], cwd=repo_path)


def get_git_diff(repo_path: Path) -> CommandResult:
    return run_command(["git", "diff", "--"], cwd=repo_path)


def read_text_file_with_limit(path: Path, byte_limit: int) -> tuple[str, bool]:
    data = path.read_bytes()
    truncated = len(data) > byte_limit
    if truncated:
        data = data[:byte_limit]
    return data.decode("utf-8", errors="replace"), truncated


def parse_changed_files(status_output: str) -> list[str]:
    changed_files: list[str] = []
    for raw_line in status_output.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        path_text = line[3:] if len(line) > 3 else ""
        if " -> " in path_text:
            _, path_text = path_text.split(" -> ", 1)
        path_text = path_text.strip()
        if path_text:
            changed_files.append(path_text)
    return changed_files


def parse_git_status_entries(status_output: str) -> dict[str, str]:
    entries: dict[str, str] = {}
    for raw_line in status_output.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        status_code = line[:2]
        path_text = line[3:] if len(line) > 3 else ""
        if " -> " in path_text:
            _, path_text = path_text.split(" -> ", 1)
        path_text = path_text.strip()
        if path_text:
            entries[path_text] = status_code
    return entries


def detect_new_changes(
    baseline_entries: dict[str, str],
    current_entries: dict[str, str],
) -> list[str]:
    new_changed_files: list[str] = []
    for path in sorted(current_entries):
        baseline_status = baseline_entries.get(path)
        current_status = current_entries[path]
        if baseline_status is None or baseline_status != current_status:
            new_changed_files.append(path)
    return new_changed_files


def create_commit(repo_path: Path, commit_message: str) -> str:
    add_result = run_command(["git", "add", "-A"], cwd=repo_path)
    if add_result.exit_code != 0:
        message = get_git_error_message(add_result)
        raise PlanError(f"Failed to stage changes for commit: {message}")

    commit_result = run_command(["git", "commit", "-m", commit_message], cwd=repo_path)
    if commit_result.exit_code != 0:
        message = get_git_error_message(commit_result)
        raise PlanError(f"Failed to create commit: {message}")

    sha_result = run_command(["git", "rev-parse", "HEAD"], cwd=repo_path)
    if sha_result.exit_code != 0:
        message = get_git_error_message(sha_result)
        raise PlanError(f"Failed to read commit sha: {message}")
    return sha_result.stdout.strip()


def create_isolated_workspace(
    repo_path: Path,
    workspace_path: Path,
    branch_name: str,
) -> Path:
    workspace_path.parent.mkdir(parents=True, exist_ok=True)
    if workspace_path.exists():
        shutil.rmtree(workspace_path)

    worktree_result = run_command(
        ["git", "worktree", "add", "-b", branch_name, str(workspace_path), "HEAD"],
        cwd=repo_path,
    )
    if worktree_result.exit_code == 0:
        return workspace_path

    clone_result = run_command(["git", "clone", str(repo_path), str(workspace_path)], cwd=repo_path)
    if clone_result.exit_code != 0:
        message = get_git_error_message(clone_result)
        raise PlanError(f"Failed to create isolated workspace: {message}")

    switch_result = run_command(["git", "switch", "-c", branch_name], cwd=workspace_path)
    if switch_result.exit_code != 0:
        message = get_git_error_message(switch_result)
        raise PlanError(f"Failed to create isolated workspace branch '{branch_name}': {message}")
    return workspace_path
