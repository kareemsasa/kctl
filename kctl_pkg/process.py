from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .output import ConsoleOutputSink, OutputSink
from .terminal import CODEX_STREAM_PREFIX, should_display_codex_line, style_text, supports_color
from .types import CommandResult


def _terminate_process(process: subprocess.Popen[str]) -> None:
    try:
        process.terminate()
        process.wait(timeout=5)
    except (subprocess.TimeoutExpired, ProcessLookupError):
        try:
            process.kill()
        except ProcessLookupError:
            return


def run_command(
    command: list[str],
    cwd: Path,
    stdin_text: str | None = None,
    env: dict[str, str] | None = None,
    stop_requested: Callable[[], bool] | None = None,
    process_started: Callable[[int], None] | None = None,
    process_finished: Callable[[int], None] | None = None,
) -> CommandResult:
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=process_env,
    )
    stopped = False
    if process_started is not None:
        process_started(process.pid)
    try:
        while True:
            if stop_requested is not None and stop_requested() and process.poll() is None:
                stopped = True
                _terminate_process(process)
            try:
                stdout, stderr = process.communicate(input=stdin_text, timeout=0.2)
                break
            except subprocess.TimeoutExpired:
                stdin_text = None
                continue
        return CommandResult(
            command=command,
            cwd=str(cwd),
            exit_code=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
            stopped=stopped,
        )
    finally:
        if process_finished is not None:
            process_finished(process.pid)


def run_streaming_command(
    command: list[str],
    cwd: Path,
    stdout_prefix: str = "",
    stderr_prefix: str = "",
    filter_stream: bool = False,
    hidden_lines: set[str] | None = None,
    output_sink: OutputSink | None = None,
    display_filter: Callable[[str], bool] | None = None,
    env: dict[str, str] | None = None,
    stop_requested: Callable[[], bool] | None = None,
    process_started: Callable[[int], None] | None = None,
    process_finished: Callable[[int], None] | None = None,
) -> CommandResult:
    output_sink = output_sink or ConsoleOutputSink()
    _display_filter = display_filter if display_filter is not None else should_display_codex_line
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        env=process_env,
    )
    stopped = False
    if process_started is not None:
        process_started(process.pid)

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def forward_stream(stream: Any, prefix: str, captured_chunks: list[str], sink_name: str) -> None:
        last_displayed_line: str | None = None
        for line in iter(stream.readline, ""):
            captured_chunks.append(line)
            rendered_line = f"{prefix}{line}" if prefix else line
            if hidden_lines is not None and line.strip() in hidden_lines:
                continue
            if not filter_stream or _display_filter(line):
                if filter_stream and rendered_line == last_displayed_line:
                    continue
                display_line = rendered_line
                if prefix:
                    terminal_stream = sys.stderr if sink_name == "stderr" else sys.stdout
                    if supports_color(terminal_stream):
                        display_line = style_text(prefix, stream=terminal_stream, dim=True) + line
                output_sink.write(display_line, stream=sink_name)
                last_displayed_line = rendered_line
        stream.close()

    stdout_thread = threading.Thread(
        target=forward_stream,
        args=(process.stdout, stdout_prefix, stdout_chunks, "stdout"),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=forward_stream,
        args=(process.stderr, stderr_prefix, stderr_chunks, "stderr"),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    try:
        while True:
            if stop_requested is not None and stop_requested() and process.poll() is None:
                stopped = True
                _terminate_process(process)
            exit_code = process.poll()
            if exit_code is not None:
                break
            time.sleep(0.1)
        stdout_thread.join()
        stderr_thread.join()
        return CommandResult(
            command=command,
            cwd=str(cwd),
            exit_code=exit_code,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
            stopped=stopped,
        )
    finally:
        if process_finished is not None:
            process_finished(process.pid)
