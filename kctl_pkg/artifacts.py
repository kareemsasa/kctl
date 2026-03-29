from __future__ import annotations

import hashlib
import os
from pathlib import Path


STORAGE_MODE_IN_REPO = "in_repo"
STORAGE_MODE_EXTERNAL = "external"
STORAGE_MODES = {STORAGE_MODE_IN_REPO, STORAGE_MODE_EXTERNAL}


def resolve_storage_mode() -> str:
    value = os.environ.get("KCTL_ARTIFACT_STORAGE", STORAGE_MODE_IN_REPO).strip().lower()
    if value in STORAGE_MODES:
        return value
    return STORAGE_MODE_IN_REPO


def kctl_home() -> Path:
    configured_home = os.environ.get("KCTL_HOME")
    if configured_home:
        return Path(configured_home).expanduser().resolve()
    return (Path.home() / ".kctl").resolve()


def repository_key(repo_root: Path) -> str:
    return hashlib.sha256(str(repo_root.resolve()).encode("utf-8")).hexdigest()[:16]


def single_runs_base(repo_root: Path, storage_mode: str | None = None) -> Path:
    storage_mode = storage_mode or resolve_storage_mode()
    repo_root = repo_root.resolve()
    if storage_mode == STORAGE_MODE_EXTERNAL:
        return kctl_home() / "repos" / repository_key(repo_root) / "single-runs"
    return repo_root / ".kctl-runs"


def single_run_dir(repo_root: Path, run_id: str, storage_mode: str | None = None) -> Path:
    return single_runs_base(repo_root, storage_mode=storage_mode) / run_id


def kctl_state_root(repo_root: Path, storage_mode: str | None = None) -> Path:
    storage_mode = storage_mode or resolve_storage_mode()
    repo_root = repo_root.resolve()
    if storage_mode == STORAGE_MODE_EXTERNAL:
        return kctl_home() / "repos" / repository_key(repo_root)
    return repo_root / ".kctl"


def multi_runs_base(repo_root: Path, storage_mode: str | None = None) -> Path:
    return kctl_state_root(repo_root, storage_mode=storage_mode) / "runs"


def multi_run_dir(repo_root: Path, run_id: str, storage_mode: str | None = None) -> Path:
    return multi_runs_base(repo_root, storage_mode=storage_mode) / run_id


def worktrees_base(repo_root: Path, storage_mode: str | None = None) -> Path:
    return kctl_state_root(repo_root, storage_mode=storage_mode) / "worktrees"


def worktree_run_root(repo_root: Path, run_id: str, storage_mode: str | None = None) -> Path:
    return worktrees_base(repo_root, storage_mode=storage_mode) / run_id


def ui_state_db_path(repo_root: Path, storage_mode: str | None = None) -> Path:
    return kctl_state_root(repo_root, storage_mode=storage_mode) / "ui-state.db"


def discover_single_run_logs(repo_root: Path) -> list[Path]:
    repo_root = repo_root.resolve()
    locations = [
        single_runs_base(repo_root, storage_mode=STORAGE_MODE_IN_REPO),
        single_runs_base(repo_root, storage_mode=STORAGE_MODE_EXTERNAL),
    ]
    discovered: list[Path] = []
    seen: set[Path] = set()
    for location in locations:
        if not location.exists():
            continue
        for path in sorted(location.glob("*/run.json")):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                discovered.append(resolved)
    return discovered


def discover_multi_run_logs(repo_root: Path) -> list[Path]:
    repo_root = repo_root.resolve()
    locations = [
        multi_runs_base(repo_root, storage_mode=STORAGE_MODE_IN_REPO),
        multi_runs_base(repo_root, storage_mode=STORAGE_MODE_EXTERNAL),
    ]
    discovered: list[Path] = []
    seen: set[Path] = set()
    for location in locations:
        if not location.exists():
            continue
        for path in sorted(location.glob("*/run.json")):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                discovered.append(resolved)
    return discovered
