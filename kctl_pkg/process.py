from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from .terminal import CODEX_STREAM_PREFIX, should_display_codex_line, style_text, supports_color
from .types import CommandResult


def run_command(command: list[str], cwd: Path, stdin_text: str | None = None) -> CommandResult:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        input=stdin_text,
        text=True,
        capture_output=True,
    )
    return CommandResult(
        command=command,
        cwd=str(cwd),
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def run_streaming_command(
    command: list[str],
    cwd: Path,
    stdout_prefix: str = "",
    stderr_prefix: str = "",
    filter_stream: bool = False,
    hidden_lines: set[str] | None = None,
) -> CommandResult:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def forward_stream(stream: Any, sink: Any, prefix: str, captured_chunks: list[str]) -> None:
        last_displayed_line: str | None = None
        for line in iter(stream.readline, ""):
            captured_chunks.append(line)
            rendered_line = f"{prefix}{line}" if prefix else line
            if hidden_lines is not None and line.strip() in hidden_lines:
                continue
            if not filter_stream or should_display_codex_line(line):
                if filter_stream and rendered_line == last_displayed_line:
                    continue
                display_line = rendered_line
                if prefix == CODEX_STREAM_PREFIX and supports_color(sink):
                    display_line = style_text(prefix, stream=sink, dim=True) + line
                sink.write(display_line)
                sink.flush()
                last_displayed_line = rendered_line
        stream.close()

    stdout_thread = threading.Thread(
        target=forward_stream,
        args=(process.stdout, sys.stdout, stdout_prefix, stdout_chunks),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=forward_stream,
        args=(process.stderr, sys.stderr, stderr_prefix, stderr_chunks),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    exit_code = process.wait()
    stdout_thread.join()
    stderr_thread.join()

    return CommandResult(
        command=command,
        cwd=str(cwd),
        exit_code=exit_code,
        stdout="".join(stdout_chunks),
        stderr="".join(stderr_chunks),
    )
