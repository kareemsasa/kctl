from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CommandResult:
    command: list[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str


class PlanError(Exception):
    pass
