from __future__ import annotations

import re
import sys
from typing import Any


COLOR_ENABLED = False
CODEX_STREAM_PREFIX = "codex: "
ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_DIM = "\033[2m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_CYAN = "\033[36m"
IMPORTANT_OUTPUT_PATTERN = re.compile(
    r"\b(error|errors|exception|traceback|failed|failure|warning|warnings|fatal|timeout|timed out|denied|invalid)\b"
)
CODE_DECLARATION_PATTERN = re.compile(
    r"^(?:"
    r"from\s+\S+\s+import\b|"
    r"import\b|"
    r"export\b|"
    r"const\b|"
    r"let\b|"
    r"var\b|"
    r"type\b|"
    r"interface\b|"
    r"enum\b|"
    r"class\b|"
    r"(?:async\s+)?def\b|"
    r"(?:async\s+)?function\b|"
    r"(?:public|private|protected|static|readonly)\b"
    r")"
)
JSX_TAG_PATTERN = re.compile(r"^</?[A-Za-z][A-Za-z0-9._:-]*(?:\s+[^>]*)?>$")
NUMBERED_DUMP_PATTERN = re.compile(r"^\s*\d+\s+")
PATH_DUMP_PATTERN = re.compile(r"^(?:\.{0,2}/|/|[A-Za-z0-9_.-]+/).+:\d+(?::\d+)?:")
PATH_MATCH_WITH_CONTENT_PATTERN = re.compile(
    r"^(?:\.{0,2}/|/)?(?:[A-Za-z0-9_.-]+/)+[^:\s]+:\d+(?::\d+)?:.+$"
)
OBJECT_FRAGMENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\s*:\s*[^.]+,?$")
TYPE_FIELD_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_?]*\s*:\s*[^=][^,;{}()]*[;,]?$")
TYPE_LITERAL_FRAGMENT_PATTERN = re.compile(r"^(?:\||&)\s*[\"'A-Za-z0-9_.-]+$")
JSX_PROPERTY_FRAGMENT_PATTERN = re.compile(
    r"^(?:className|id|key|name|value|type|variant|size|color|href|src|alt|title|role|on[A-Z][A-Za-z0-9_]*)\s*=\s*.+$"
)
PROSE_PREFIX_PATTERN = re.compile(
    r"^(?:"
    r"i['’]m\b|"
    r"i\b|"
    r"we\b|"
    r"found\b|"
    r"checking\b|"
    r"inspecting\b|"
    r"reviewing\b|"
    r"running\b|"
    r"testing\b|"
    r"verifying\b|"
    r"updated\b|"
    r"changed\b|"
    r"added\b|"
    r"removed\b|"
    r"fixed\b|"
    r"kept\b|"
    r"showing\b|"
    r"hiding\b|"
    r"status\b|"
    r"result\b|"
    r"summary\b|"
    r"verification\b|"
    r"final\b|"
    r"done\b|"
    r"no files were modified\b|"
    r"no changes were made\b"
    r")",
    re.IGNORECASE,
)
RESULT_PREFIX_PATTERN = re.compile(
    r"^(?:"
    r"step\s+\S+\s+\||"
    r"new:\s+|"
    r"review\s+\S+\s*:|"
    r"verify(?:\s+\w+)?:|"
    r"verification\b|"
    r"tests?\b|"
    r"no files were modified\.?|"
    r"no changes were made\.?"
    r")",
    re.IGNORECASE,
)


def set_color_enabled(enabled: bool) -> None:
    global COLOR_ENABLED
    COLOR_ENABLED = enabled


def supports_color(stream: Any) -> bool:
    return bool(COLOR_ENABLED and hasattr(stream, "isatty") and stream.isatty())


def style_text(
    text: str,
    *,
    stream: Any = sys.stdout,
    color: str | None = None,
    bold: bool = False,
    dim: bool = False,
) -> str:
    if not supports_color(stream):
        return text
    codes: list[str] = []
    if bold:
        codes.append(ANSI_BOLD)
    if dim:
        codes.append(ANSI_DIM)
    if color:
        codes.append(color)
    if not codes:
        return text
    return "".join(codes) + text + ANSI_RESET


def style_status_text(text: str, status: str, *, stream: Any = sys.stdout, bold: bool = False) -> str:
    if status == "success":
        color = ANSI_GREEN
    elif status in {"paused", "warning", "concern"}:
        color = ANSI_YELLOW
    elif status in {"failure", "failed", "block", "blocked", "error"}:
        color = ANSI_RED
    else:
        color = None
    return style_text(text, stream=stream, color=color, bold=bold)


def is_command_like_line(line: str) -> bool:
    stripped = line.strip().strip("`")
    command_prefixes = (
        "git ",
        "python ",
        "python3 ",
        "pytest",
        "npm ",
        "pnpm ",
        "yarn ",
        "cargo ",
        "go ",
        "make ",
        "sh ",
        "bash ",
        "./",
        "cd ",
        "ls ",
        "cat ",
        "sed ",
        "rg ",
        "grep ",
        "uv ",
    )
    return stripped.startswith(command_prefixes)


def is_important_output_line(line: str) -> bool:
    lower = line.lower()
    if lower.startswith(("error:", "warning:", "warn:", "fatal:", "usage:")):
        return True
    if "no such file" in lower or "not found" in lower or "permission denied" in lower:
        return True
    return bool(IMPORTANT_OUTPUT_PATTERN.search(lower))


def looks_like_code_or_file_dump(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if PATH_MATCH_WITH_CONTENT_PATTERN.match(stripped):
        return True
    if CODE_DECLARATION_PATTERN.match(stripped):
        return True
    if JSX_TAG_PATTERN.match(stripped):
        return True
    if JSX_PROPERTY_FRAGMENT_PATTERN.match(stripped):
        return True
    if re.fullmatch(r"[{}\[\]();,]+", stripped):
        return True
    if re.fullmatch(r"[{}\[\](),.:;<>=\"'`-]+", stripped):
        return True
    if re.match(r"^(?:if|else|for|while|switch|try|catch|finally|return)\b", stripped):
        return True
    if "=>" in stripped and (stripped.endswith("{") or stripped.endswith(");") or stripped.endswith(",")):
        return True
    if stripped.endswith("{") and re.search(r"\([^)]*\)", stripped):
        return True
    if OBJECT_FRAGMENT_PATTERN.match(stripped) and len(stripped.split()) <= 6:
        return True
    if TYPE_FIELD_PATTERN.match(stripped) and len(stripped.split()) <= 6:
        return True
    if TYPE_LITERAL_FRAGMENT_PATTERN.match(stripped):
        return True
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\??:\s*(?:string|number|boolean|unknown|any|never|void|React\.\w+|\{.*\}|\[.*\]|<.*>)$", stripped):
        return True
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=\{.*\},?$", stripped):
        return True
    if NUMBERED_DUMP_PATTERN.match(line):
        body = NUMBERED_DUMP_PATTERN.sub("", line, count=1).strip()
        if body and (looks_like_code_or_file_dump(body) or "/" in body or body.endswith(("{", "}", ";"))):
            return True
    if PATH_DUMP_PATTERN.match(stripped):
        return True
    if len(stripped) > 120 and (stripped.count("/") >= 2 or stripped.count("\\") >= 2):
        return True
    return False


def looks_like_natural_language_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped or looks_like_code_or_file_dump(stripped):
        return False
    if PATH_DUMP_PATTERN.match(stripped) or PATH_MATCH_WITH_CONTENT_PATTERN.match(stripped) or NUMBERED_DUMP_PATTERN.match(stripped):
        return False
    if stripped.startswith(("/", "./")) or is_command_like_line(stripped):
        return False
    if " | " in stripped and any(token in stripped.lower() for token in ("file changed", "insertion", "deletion")):
        return False
    word_count = len(stripped.split())
    if RESULT_PREFIX_PATTERN.match(stripped):
        return True
    if PROSE_PREFIX_PATTERN.match(stripped):
        return word_count >= 2
    if stripped.startswith(("- ", "* ")):
        bullet_body = stripped[2:].strip()
        if not bullet_body or looks_like_code_or_file_dump(bullet_body) or is_command_like_line(bullet_body):
            return False
        return len(bullet_body.split()) >= 4 and any(char in bullet_body for char in ".:")
    if word_count < 4:
        return False
    if len(stripped) > 160 and ("/" in stripped or "\\" in stripped):
        return False
    punctuation_count = sum(1 for char in stripped if char in ".,:;!?")
    alpha_ratio = sum(1 for char in stripped if char.isalpha()) / max(len(stripped), 1)
    return punctuation_count >= 1 and alpha_ratio >= 0.55


def should_display_codex_line(line: str) -> bool:
    stripped = line.strip()
    lower = stripped.lower()
    hidden_prefixes = (
        "OpenAI Codex ",
        "workdir:",
        "model:",
        "provider:",
        "approval:",
        "sandbox:",
        "reasoning effort:",
        "reasoning summaries:",
        "session id:",
        "mcp startup:",
        "user",
        "202",
    )
    if not stripped:
        return False
    if stripped == "--------":
        return False
    if stripped.startswith(hidden_prefixes):
        return False
    if stripped.startswith(CODEX_STREAM_PREFIX):
        return should_display_codex_line(stripped[len(CODEX_STREAM_PREFIX) :])
    if is_important_output_line(stripped):
        return True
    if "token" in lower and ("input" in lower or "output" in lower or "total" in lower):
        return False
    if stripped.startswith("Reconnecting..."):
        return False
    if stripped in {"Constraints:", "Overall objective:", "Prior step summaries:"}:
        return False
    if stripped.startswith(("Current step id:", "Current step prompt:")):
        return False
    if stripped.startswith("- Work only in the current repository."):
        return False
    if stripped.startswith("- Keep changes scoped to the current step."):
        return False
    if stripped.startswith("- In your final response, summarize what you changed and any verification you ran."):
        return False
    return looks_like_natural_language_line(stripped)


def is_meaningful_summary_line(line: str) -> bool:
    stripped = line.strip()
    lower = stripped.lower()
    ignored_prefixes = (
        "OpenAI Codex ",
        "workdir:",
        "model:",
        "provider:",
        "approval:",
        "sandbox:",
        "reasoning effort:",
        "reasoning summaries:",
        "session id:",
        "mcp startup:",
        "Reconnecting...",
        "WARNING:",
        "note:",
        "thread ",
        "user",
        "assistant",
        "--------",
        "202",
    )
    if stripped.startswith(ignored_prefixes):
        return False
    if looks_like_code_or_file_dump(stripped):
        return False
    if lower in {"verification:", "verify:", "validation:", "tests:"}:
        return False
    if "token" in lower and ("input" in lower or "output" in lower or "total" in lower):
        return False
    if is_command_like_line(stripped):
        return False
    if stripped.startswith(("- ", "* ")) and is_command_like_line(stripped[2:]):
        return False
    return looks_like_natural_language_line(stripped)
