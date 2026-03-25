from __future__ import annotations

import sys
import threading


class OutputSink:
    def write(self, text: str, *, stream: str = "stdout") -> None:
        raise NotImplementedError

    def write_line(self, text: str = "", *, stream: str = "stdout") -> None:
        self.write(text + "\n", stream=stream)


class ConsoleOutputSink(OutputSink):
    def __init__(self, prefix: str = "") -> None:
        self.prefix = prefix
        self._lock = threading.Lock()

    def write(self, text: str, *, stream: str = "stdout") -> None:
        if not text:
            return
        rendered = self._prefix_text(text)
        sink = sys.stderr if stream == "stderr" else sys.stdout
        with self._lock:
            sink.write(rendered)
            sink.flush()

    def _prefix_text(self, text: str) -> str:
        if not self.prefix:
            return text
        return "".join(
            f"{self.prefix}{line}" if line else self.prefix
            for line in text.splitlines(keepends=True)
        )


class BufferedOutputSink(OutputSink):
    def __init__(self, prefix: str = "") -> None:
        self.prefix = prefix
        self._entries: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def write(self, text: str, *, stream: str = "stdout") -> None:
        if not text:
            return
        rendered = self._prefix_text(text)
        with self._lock:
            self._entries.append((stream, rendered))

    def flush_to(self, sink: OutputSink) -> None:
        with self._lock:
            entries = list(self._entries)
            self._entries.clear()
        for stream, text in entries:
            sink.write(text, stream=stream)

    def _prefix_text(self, text: str) -> str:
        if not self.prefix:
            return text
        return "".join(
            f"{self.prefix}{line}" if line else self.prefix
            for line in text.splitlines(keepends=True)
        )


class NullOutputSink(OutputSink):
    def write(self, text: str, *, stream: str = "stdout") -> None:
        return
