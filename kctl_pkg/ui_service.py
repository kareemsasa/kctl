from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

from .paths import project_root
from .types import PlanError


def _systemd_environment_line(name: str, value: str) -> str:
    escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'Environment="{name}={escaped_value}"'


def default_service_name() -> str:
    return "kctl-dashboard"


def default_service_path(service_name: str) -> Path:
    return Path("~/.config/systemd/user").expanduser().resolve() / f"{service_name}.service"


def render_dashboard_service(
    *,
    repo_path: Path,
    host: str,
    port: int,
    tailscale: bool,
    announce_url: str | None,
    db_path: Path | None,
    python_executable: str | None = None,
) -> str:
    python_cmd = python_executable or sys.executable
    entrypoint = (project_root() / "kctl.py").resolve()
    command = [
        python_cmd,
        str(entrypoint),
        "ui",
        "dashboard",
        str(repo_path.resolve()),
        "--host",
        host,
        "--port",
        str(port),
    ]
    if db_path is not None:
        command.extend(["--db-path", str(db_path.resolve())])
    if tailscale:
        command.append("--tailscale")
    if announce_url:
        command.extend(["--announce-url", announce_url])
    quoted_command = shlex.join(command)
    working_directory = project_root().resolve()
    path_value = os.environ.get("PATH", "")
    npm_global_bin = Path("~/.npm-global/bin").expanduser().resolve()
    if npm_global_bin.is_dir():
        npm_global_str = str(npm_global_bin)
        existing = path_value.split(":") if path_value else []
        if npm_global_str not in existing:
            path_value = npm_global_str + (":" + path_value if path_value else "")
    return "\n".join(
        [
            "[Unit]",
            "Description=kctl dashboard",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={working_directory}",
            _systemd_environment_line("PATH", path_value),
            f"ExecStart={quoted_command}",
            "Restart=on-failure",
            "RestartSec=3",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def install_dashboard_service(service_path: Path, service_contents: str) -> Path:
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(service_contents)
    return service_path


def run_systemctl_user(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", "--user", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def ensure_systemctl_success(result: subprocess.CompletedProcess[str], action: str) -> None:
    if result.returncode == 0:
        return
    message = result.stderr.strip() or result.stdout.strip() or f"systemctl {action} failed"
    raise PlanError(message)
