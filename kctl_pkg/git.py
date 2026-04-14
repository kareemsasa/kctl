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


def get_ahead_behind(repo_path: Path) -> tuple[int, int] | None:
    """Return (ahead, behind) counts relative to the upstream tracking branch, or None if unavailable."""
    result = run_command(["git", "rev-list", "--left-right", "--count", "@{u}...HEAD"], cwd=repo_path)
    if result.exit_code != 0:
        return None
    parts = result.stdout.strip().split()
    if len(parts) != 2:
        return None
    try:
        return int(parts[1]), int(parts[0])
    except ValueError:
        return None


def get_last_commit_summary(repo_path: Path) -> str | None:
    """Return a short one-line summary of the most recent commit, or None on failure."""
    result = run_command(["git", "log", "-1", "--format=%h %s (%ar)"], cwd=repo_path)
    if result.exit_code != 0:
        return None
    return result.stdout.strip() or None


def get_remotes(repo_path: Path) -> list[dict[str, str]]:
    """Return parsed output of ``git remote -v`` as a list of dicts with name, url, and direction."""
    result = run_command(["git", "remote", "-v"], cwd=repo_path)
    if result.exit_code != 0:
        return []
    remotes: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        name, url = parts[0], parts[1]
        direction = parts[2].strip("()")
        key = (name, url, direction)
        if key not in seen:
            seen.add(key)
            remotes.append({"name": name, "url": url, "direction": direction})
    return remotes


def get_recent_commits(repo_path: Path, count: int = 15) -> list[dict[str, str]]:
    """Return the most recent commits as dicts with sha, subject, author, and relative date."""
    fmt = "%h%x00%s%x00%an%x00%ar"
    result = run_command(["git", "log", f"-{count}", f"--format={fmt}"], cwd=repo_path)
    if result.exit_code != 0:
        return []
    commits: list[dict[str, str]] = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\x00")
        if len(parts) < 4:
            continue
        commits.append({"sha": parts[0], "subject": parts[1], "author": parts[2], "date": parts[3]})
    return commits


def get_local_branches(repo_path: Path) -> list[dict[str, str]]:
    """Return local branches with name and whether each is the current branch."""
    result = run_command(["git", "branch", "--format=%(HEAD) %(refname:short)"], cwd=repo_path)
    if result.exit_code != 0:
        return []
    branches: list[dict[str, str]] = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        current = line.startswith("* ")
        name = line[2:].strip() if current else line.strip()
        branches.append({"name": name, "current": "true" if current else "false"})
    return branches


def get_stash_list(repo_path: Path) -> list[str]:
    result = run_command(["git", "stash", "list"], cwd=repo_path)
    if result.exit_code != 0:
        return []
    return [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]


def _summarize_remote_error(raw_output: str, exit_code: int) -> str:
    """Extract a concise error from verbose git ls-remote output."""
    lines = [line.strip() for line in raw_output.splitlines() if line.strip()]
    for line in lines:
        lower = line.lower()
        if "permission denied" in lower:
            return line
        if "could not resolve" in lower:
            return line
        if "connection refused" in lower:
            return line
        if "no such device or address" in lower:
            return line
        if "repository not found" in lower:
            return line
    if lines:
        return lines[0]
    return f"exit code {exit_code}"


def check_remote_connectivity(repo_path: Path, remote_name: str = "origin") -> dict[str, object]:
    """Test connectivity to a git remote using ``git ls-remote``.

    This goes through git's own transport layer so it correctly handles SSH host
    aliases, key selection via ``~/.ssh/config``, and credential helpers for HTTPS.
    """
    import os
    import subprocess

    url_result = run_command(["git", "remote", "get-url", remote_name], cwd=repo_path)
    if url_result.exit_code != 0:
        return {"remote": remote_name, "ok": False, "message": f"remote '{remote_name}' not found"}
    url = url_result.stdout.strip()
    is_ssh = url.startswith("git@") or "ssh://" in url
    protocol = "ssh" if is_ssh else "https" if url.startswith("https://") else "other"

    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "SSH_ASKPASS": "", "SSH_ASKPASS_REQUIRE": "never"}
    env.pop("DISPLAY", None)
    if is_ssh:
        env["GIT_SSH_COMMAND"] = "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5"

    try:
        completed = subprocess.run(
            ["git", "ls-remote", "--exit-code", "--heads", remote_name],
            cwd=str(repo_path), capture_output=True, text=True, timeout=15, env=env,
        )
        if completed.returncode == 0 or completed.returncode == 2:
            return {"remote": remote_name, "url": url, "protocol": protocol, "ok": True, "message": "connected"}
        output = (completed.stderr + completed.stdout).strip()
        short_message = _summarize_remote_error(output, completed.returncode)
        result: dict[str, object] = {"remote": remote_name, "url": url, "protocol": protocol, "ok": False, "message": short_message}
        if is_ssh and "permission denied" in output.lower():
            host = url.split("@", 1)[1].split(":", 1)[0] if "@" in url else url
            result["hint"] = f"SSH key for {host} may not be loaded in the agent or configured in ~/.ssh/config"
        return result
    except subprocess.TimeoutExpired:
        return {"remote": remote_name, "url": url, "protocol": protocol, "ok": False, "message": "connection timed out"}
    except FileNotFoundError:
        return {"remote": remote_name, "url": url, "protocol": protocol, "ok": False, "message": "git binary not found"}
    except Exception as exc:
        return {"remote": remote_name, "url": url, "protocol": protocol, "ok": False, "message": str(exc)}


def get_project_git_detail(repo_path: Path) -> dict[str, object]:
    """Collect comprehensive git state for a project detail page."""
    path = Path(repo_path)
    detail: dict[str, object] = {"path": str(path), "available": False}
    if not path.exists() or not path.is_dir():
        detail["error"] = "path not found"
        return detail

    root_result = run_command(["git", "rev-parse", "--show-toplevel"], cwd=path)
    if root_result.exit_code != 0:
        detail["error"] = "not a git repo"
        return detail

    detail["available"] = True
    detail["name"] = path.resolve().name

    branch_result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=path)
    detail["branch"] = branch_result.stdout.strip() if branch_result.exit_code == 0 else None

    status_result = get_git_status(path)
    if status_result.exit_code == 0:
        detail["status_output"] = status_result.stdout.strip("\n")
        files = parse_changed_files(status_result.stdout)
        detail["dirty"] = len(files) > 0
        detail["changed_count"] = len(files)
    else:
        detail["status_output"] = ""
        detail["dirty"] = None
        detail["changed_count"] = 0

    detail["ahead_behind"] = get_ahead_behind(path)
    detail["remotes"] = get_remotes(path)
    detail["recent_commits"] = get_recent_commits(path)
    detail["branches"] = get_local_branches(path)
    detail["stash_list"] = get_stash_list(path)

    diff_stat_result = get_git_diff_stat(path)
    detail["diff_stat"] = diff_stat_result.stdout.strip("\n") if diff_stat_result.exit_code == 0 else ""

    return detail


def get_project_git_summary(repo_path: Path) -> dict[str, object]:
    """Collect git state for a tracked project. All fields degrade gracefully."""
    path = Path(repo_path)
    summary: dict[str, object] = {"path": str(path), "available": False}
    if not path.exists() or not path.is_dir():
        summary["error"] = "path not found"
        return summary

    root_result = run_command(["git", "rev-parse", "--show-toplevel"], cwd=path)
    if root_result.exit_code != 0:
        summary["error"] = "not a git repo"
        return summary

    summary["available"] = True

    branch_result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=path)
    summary["branch"] = branch_result.stdout.strip() if branch_result.exit_code == 0 else None

    status_result = get_git_status(path)
    if status_result.exit_code == 0:
        files = parse_changed_files(status_result.stdout)
        summary["dirty"] = len(files) > 0
        summary["changed_count"] = len(files)
    else:
        summary["dirty"] = None
        summary["changed_count"] = 0

    summary["ahead_behind"] = get_ahead_behind(path)
    summary["last_commit"] = get_last_commit_summary(path)
    return summary


def probe_workspace_dirty(workspace_path: str | None) -> bool | None:
    """Return True if the workspace has uncommitted changes, False if clean, None if the path is unavailable."""
    if not workspace_path:
        return None
    path = Path(workspace_path)
    if not path.exists() or not path.is_dir():
        return None
    result = get_git_status(path)
    if result.exit_code != 0:
        return None
    return bool(result.stdout.strip())


def git_pull(repo_path: Path, remote: str = "origin", branch: str | None = None) -> str:
    """Pull from a remote. Returns the git output on success."""
    cmd = ["git", "pull", "--rebase", remote]
    if branch:
        cmd.append(branch)
    result = run_command(cmd, cwd=repo_path)
    if result.exit_code != 0:
        message = get_git_error_message(result)
        raise PlanError(f"Failed to pull from {remote}: {message}")
    return result.stdout.strip()


def git_push(
    repo_path: Path,
    remote: str = "origin",
    branch: str | None = None,
    set_upstream: bool = False,
) -> str:
    """Push to a remote. Returns the git output on success."""
    cmd = ["git", "push"]
    if set_upstream:
        cmd.append("-u")
    cmd.append(remote)
    if branch:
        cmd.append(branch)
    result = run_command(cmd, cwd=repo_path)
    if result.exit_code != 0:
        message = get_git_error_message(result)
        raise PlanError(f"Failed to push to {remote}: {message}")
    return (result.stdout + result.stderr).strip()


def git_stash_save(repo_path: Path, message: str | None = None) -> str:
    """Stash working-tree changes. Returns git output."""
    cmd = ["git", "stash", "push"]
    if message:
        cmd.extend(["-m", message])
    result = run_command(cmd, cwd=repo_path)
    if result.exit_code != 0:
        msg = get_git_error_message(result)
        raise PlanError(f"Failed to stash changes: {msg}")
    return result.stdout.strip()


def git_stash_pop(repo_path: Path) -> str:
    """Pop the top stash entry. Returns git output."""
    result = run_command(["git", "stash", "pop"], cwd=repo_path)
    if result.exit_code != 0:
        msg = get_git_error_message(result)
        raise PlanError(f"Failed to pop stash: {msg}")
    return result.stdout.strip()


def discard_all_changes(repo_path: Path) -> str:
    """Discard all uncommitted changes (tracked and untracked files)."""
    checkout_result = run_command(["git", "checkout", "--", "."], cwd=repo_path)
    if checkout_result.exit_code != 0:
        msg = get_git_error_message(checkout_result)
        raise PlanError(f"Failed to discard tracked changes: {msg}")
    clean_result = run_command(["git", "clean", "-fd"], cwd=repo_path)
    if clean_result.exit_code != 0:
        msg = get_git_error_message(clean_result)
        raise PlanError(f"Failed to clean untracked files: {msg}")
    return "All changes discarded."


def stage_and_commit(repo_path: Path, message: str, paths: list[str] | None = None) -> str:
    """Stage files and commit. Returns the new commit SHA.

    If *paths* is None, stages everything (``git add -A``).
    Otherwise stages only the given paths.
    """
    if paths:
        add_result = run_command(["git", "add", "--"] + paths, cwd=repo_path)
    else:
        add_result = run_command(["git", "add", "-A"], cwd=repo_path)
    if add_result.exit_code != 0:
        msg = get_git_error_message(add_result)
        raise PlanError(f"Failed to stage changes: {msg}")

    commit_result = run_command(["git", "commit", "-m", message], cwd=repo_path)
    if commit_result.exit_code != 0:
        msg = get_git_error_message(commit_result)
        raise PlanError(f"Failed to commit: {msg}")

    sha_result = run_command(["git", "rev-parse", "--short", "HEAD"], cwd=repo_path)
    if sha_result.exit_code != 0:
        return "unknown"
    return sha_result.stdout.strip()


def create_branch(repo_path: Path, branch_name: str) -> None:
    """Create a new branch and switch to it."""
    result = run_command(["git", "switch", "-c", branch_name], cwd=repo_path)
    if result.exit_code != 0:
        msg = get_git_error_message(result)
        raise PlanError(f"Failed to create branch '{branch_name}': {msg}")


def switch_branch(repo_path: Path, branch_name: str) -> None:
    """Switch to an existing local branch."""
    result = run_command(["git", "switch", branch_name], cwd=repo_path)
    if result.exit_code != 0:
        msg = get_git_error_message(result)
        raise PlanError(f"Failed to switch to branch '{branch_name}': {msg}")


def get_full_diff(repo_path: Path) -> str:
    """Return the full working-tree diff (unstaged + untracked shown as new)."""
    result = run_command(["git", "diff", "HEAD"], cwd=repo_path)
    if result.exit_code != 0:
        return ""
    return result.stdout


def create_isolated_workspace(
    repo_path: Path,
    workspace_path: Path,
    branch_name: str,
) -> Path:
    def ensure_runtime_links() -> None:
        source_venv = repo_path / ".venv"
        target_venv = workspace_path / ".venv"
        if not source_venv.exists() or target_venv.exists():
            return
        try:
            target_venv.symlink_to(source_venv, target_is_directory=True)
        except OSError:
            return

    workspace_path.parent.mkdir(parents=True, exist_ok=True)
    if workspace_path.exists():
        shutil.rmtree(workspace_path)

    worktree_result = run_command(
        ["git", "worktree", "add", "-b", branch_name, str(workspace_path), "HEAD"],
        cwd=repo_path,
    )
    if worktree_result.exit_code == 0:
        ensure_runtime_links()
        return workspace_path

    clone_result = run_command(["git", "clone", str(repo_path), str(workspace_path)], cwd=repo_path)
    if clone_result.exit_code != 0:
        message = get_git_error_message(clone_result)
        raise PlanError(f"Failed to create isolated workspace: {message}")

    switch_result = run_command(["git", "switch", "-c", branch_name], cwd=workspace_path)
    if switch_result.exit_code != 0:
        message = get_git_error_message(switch_result)
        raise PlanError(f"Failed to create isolated workspace branch '{branch_name}': {message}")
    ensure_runtime_links()
    return workspace_path
