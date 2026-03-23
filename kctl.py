#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import Any

import yaml


@dataclass
class CommandResult:
    command: list[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str


class PlanError(Exception):
    pass


COLOR_ENABLED = False
REVIEWER_NAMES = ("scope reviewer", "test reviewer")
MAX_REVIEW_UNTRACKED_FILE_BYTES = 16_000
CODEX_STREAM_PREFIX = "codex: "
UNKNOWN_GIT_ERROR = "unknown git error"
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


def get_git_error_message(result: CommandResult) -> str:
    return result.stderr.strip() or result.stdout.strip() or UNKNOWN_GIT_ERROR


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


def load_plan(plan_path: Path) -> dict[str, Any]:
    if not plan_path.exists():
        raise PlanError(f"Plan file does not exist: {plan_path}")

    try:
        data = yaml.safe_load(plan_path.read_text())
    except yaml.YAMLError as exc:
        raise PlanError(f"Failed to parse YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise PlanError("Plan file must contain a top-level mapping.")

    return data


def load_plan_templates(script_root: Path) -> dict[str, Any]:
    templates_path = script_root / "kctl-plan-templates.yaml"
    if not templates_path.exists():
        raise PlanError(f"Templates file does not exist: {templates_path}")

    try:
        data = yaml.safe_load(templates_path.read_text())
    except yaml.YAMLError as exc:
        raise PlanError(f"Failed to parse templates YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise PlanError("Templates file must contain a top-level mapping.")

    templates = data.get("templates")
    if not isinstance(templates, dict) or not templates:
        raise PlanError("Templates file must contain a non-empty top-level 'templates' mapping.")

    return templates


def validate_plan(plan: dict[str, Any]) -> None:
    required_string_fields = ["repo", "objective"]
    for field in required_string_fields:
        value = plan.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PlanError(f"Plan field '{field}' is required and must be a non-empty string.")

    defaults = plan.get("defaults", {})
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise PlanError("Plan field 'defaults' must be a mapping if provided.")

    verify = defaults.get("verify")
    if verify is not None and not isinstance(verify, str):
        raise PlanError("defaults.verify must be a string if provided.")

    stop_on_failure = defaults.get("stop_on_failure")
    if stop_on_failure is not None and not isinstance(stop_on_failure, bool):
        raise PlanError("defaults.stop_on_failure must be a boolean if provided.")

    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        raise PlanError("Plan field 'steps' is required and must be a non-empty list.")

    step_ids: set[str] = set()
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise PlanError(f"Step #{index} must be a mapping.")

        step_id = step.get("id")
        prompt = step.get("prompt")
        if not isinstance(step_id, str) or not step_id.strip():
            raise PlanError(f"Step #{index} field 'id' is required and must be a non-empty string.")
        if step_id in step_ids:
            raise PlanError(f"Duplicate step id: {step_id}")
        step_ids.add(step_id)

        if not isinstance(prompt, str) or not prompt.strip():
            raise PlanError(f"Step '{step_id}' field 'prompt' is required and must be a non-empty string.")

        step_verify = step.get("verify")
        if step_verify is not None and not isinstance(step_verify, str):
            raise PlanError(f"Step '{step_id}' field 'verify' must be a string if provided.")

        expect_clean_diff = step.get("expect_clean_diff")
        if expect_clean_diff is not None and not isinstance(expect_clean_diff, bool):
            raise PlanError(f"Step '{step_id}' field 'expect_clean_diff' must be a boolean if provided.")


def build_plan_from_template(
    templates: dict[str, Any],
    template_name: str,
    repo: str,
    objective: str,
) -> dict[str, Any]:
    template = templates.get(template_name)
    if template is None:
        raise PlanError(f"Template does not exist: {template_name}")
    if not isinstance(template, dict):
        raise PlanError(f"Template '{template_name}' must be a mapping.")

    executable_shape = template.get("shape") if "shape" in template else template
    if not isinstance(executable_shape, dict):
        raise PlanError(f"Template '{template_name}' field 'shape' must be a mapping if provided.")

    steps = executable_shape.get("steps")
    if not isinstance(steps, list) or not steps:
        raise PlanError(f"Template '{template_name}' must define a non-empty 'steps' list.")

    defaults = executable_shape.get("defaults")
    if defaults is None:
        defaults = {"stop_on_failure": True}
    elif not isinstance(defaults, dict):
        raise PlanError(f"Template '{template_name}' field 'defaults' must be a mapping if provided.")

    plan = {
        "repo": repo,
        "objective": objective,
        "defaults": defaults,
        "steps": steps,
    }
    validate_plan(plan)
    return plan


def init_plan(
    template_name: str,
    output_path: Path,
    repo: str,
    objective: str,
    force: bool,
) -> int:
    if output_path.exists() and not force:
        raise PlanError(f"Output file already exists: {output_path}. Use --force to overwrite.")

    script_root = Path(__file__).resolve().parent
    templates = load_plan_templates(script_root)
    plan = build_plan_from_template(
        templates=templates,
        template_name=template_name,
        repo=repo,
        objective=objective,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(plan, sort_keys=False))
    print(style_text(f"Created plan {output_path} from template {template_name}", bold=True), flush=True)
    return 0


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


def build_codex_prompt(objective: str, prior_summaries: list[str], step: dict[str, Any]) -> str:
    sections = [
        "You are executing one step in a larger kctl plan.",
        f"Overall objective:\n{objective.strip()}",
    ]

    if prior_summaries:
        sections.append("Prior step summaries:\n" + "\n".join(f"- {summary}" for summary in prior_summaries))
    else:
        sections.append("Prior step summaries:\n- No prior steps have run.")

    sections.append(f"Current step id: {step['id']}")
    sections.append(f"Current step prompt:\n{step['prompt'].strip()}")
    sections.append(
        "Constraints:\n"
        "- Work only in the current repository.\n"
        "- Keep changes scoped to the current step.\n"
        "- In your final response, summarize what you changed and any verification you ran."
    )
    return "\n\n".join(sections)


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


def build_synthetic_codex_summary(
    status: str,
    changed_files: list[str],
    verify_result: CommandResult | dict[str, Any] | None,
) -> str:
    changed_files_text = ", ".join(changed_files) if changed_files else "-"
    verify_text = "not-run"
    if verify_result is not None:
        verify_exit_code = (
            verify_result.exit_code
            if isinstance(verify_result, CommandResult)
            else verify_result["exit_code"]
        )
        verify_text = "passed" if verify_exit_code == 0 else "failed"
    return f"status={status}; changed_files={changed_files_text}; verify={verify_text}"


def extract_codex_summary(stdout: str, status: str, changed_files: list[str], verify_result: CommandResult | None) -> str:
    for line in reversed([line.strip() for line in stdout.splitlines() if line.strip()]):
        if is_meaningful_summary_line(line):
            return line[:200]
    return build_synthetic_codex_summary(status, changed_files, verify_result)


def shorten_summary(text: str, limit: int = 200) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def extract_compact_step_summary(step_result: dict[str, Any]) -> str:
    codex_summary = step_result.get("codex_summary")
    if codex_summary:
        return shorten_summary(codex_summary)
    return build_synthetic_codex_summary(
        status=step_result["status"],
        changed_files=step_result["changed_files"],
        verify_result=step_result["verify"],
    )


def summarize_step_result(step_result: dict[str, Any]) -> str:
    changed_files = ", ".join(step_result["changed_files"]) if step_result["changed_files"] else "-"
    return (
        f"id={step_result['id']} "
        f"status={step_result['status']} "
        f"changed_files={changed_files} "
        f"summary={extract_compact_step_summary(step_result)}"
    )


def extract_verify_data(verify_result: CommandResult | None) -> dict[str, Any] | None:
    if verify_result is None:
        return None
    return {
        "command": verify_result.command,
        "cwd": verify_result.cwd,
        "exit_code": verify_result.exit_code,
        "stdout": verify_result.stdout,
        "stderr": verify_result.stderr,
    }


def format_duration_seconds(started_at: str, ended_at: str) -> str:
    started = datetime.fromisoformat(started_at)
    ended = datetime.fromisoformat(ended_at)
    duration = max(0.0, (ended - started).total_seconds())
    return f"{duration:.1f}s"


def get_verify_label(verify_result: dict[str, Any] | None) -> str:
    if verify_result is None:
        return "skipped"
    return "passed" if verify_result["exit_code"] == 0 else "failed"


def print_step_footer(step_result: dict[str, Any]) -> None:
    footer = (
        f"Step {step_result['id']} | status={step_result['status']} | "
        f"duration={format_duration_seconds(step_result['started_at'], step_result['ended_at'])} | "
        f"verify={get_verify_label(step_result['verify'])} | "
        f"baseline_changed_files={len(step_result['baseline_changed_files'])} | "
        f"new_changed_files={len(step_result['new_changed_files'])}"
    )
    print(style_status_text(footer, step_result["status"], bold=True), flush=True)
    if 0 < len(step_result["new_changed_files"]) <= 5:
        print(f"New: {', '.join(step_result['new_changed_files'])}", flush=True)


def prompt_to_continue() -> bool:
    try:
        response = input(style_text("Continue to next step? [y/N] ", bold=True))
    except EOFError:
        return False
    return response.strip().lower() == "y"


def prompt_to_continue_after_review(step_id: str, reviews: list[dict[str, Any]]) -> bool:
    concern_count = sum(1 for review in reviews if review["verdict"] == "concern")
    try:
        response = input(
            style_text(
                f"Review concerns for step {step_id} ({concern_count} concern). Continue anyway? [y/N] ",
                bold=True,
            )
        )
    except EOFError:
        return False
    return response.strip().lower() == "y"


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


def print_command_result(label: str, result: CommandResult) -> None:
    status = "success" if result.exit_code == 0 else "failure"
    print(style_status_text(f"{label} exit code: {result.exit_code}", status), flush=True)
    if result.stdout.strip():
        print(f"{label} stdout:", flush=True)
        print(result.stdout.rstrip(), flush=True)
    if result.stderr.strip():
        print(style_status_text(f"{label} stderr:", "failure", stream=sys.stderr), file=sys.stderr, flush=True)
        print(result.stderr.rstrip(), file=sys.stderr, flush=True)


def build_verify_summary(verify_result: CommandResult | None) -> str:
    if verify_result is None:
        return "Verification not run."
    status = "passed" if verify_result.exit_code == 0 else "failed"
    summary = f"Verification {status} with exit code {verify_result.exit_code}."
    stdout = verify_result.stdout.strip()
    stderr = verify_result.stderr.strip()
    details: list[str] = []
    if stdout:
        details.append(f"stdout: {stdout[:400]}")
    if stderr:
        details.append(f"stderr: {stderr[:400]}")
    if details:
        summary += " " + " ".join(details)
    return summary


def build_review_prompt(
    reviewer: str,
    objective: str,
    step_id: str,
    changed_files: list[str],
    review_content: str,
    verify_summary: str,
) -> str:
    changed_files_text = "\n".join(f"- {path}" for path in changed_files) if changed_files else "- None"
    return "\n\n".join(
        [
            "You are running a review pass inside kctl.",
            f"Reviewer:\n{reviewer}",
            f"Overall objective:\n{objective.strip()}",
            f"Step id:\n{step_id}",
            f"Changed files:\n{changed_files_text}",
            f"Review content:\n{review_content.strip() if review_content.strip() else '(no review content)'}",
            f"Verification result summary:\n{verify_summary}",
            (
                "Instructions:\n"
                "- Review only; do not modify code.\n"
                "- Focus strictly on this reviewer's remit.\n"
                "- Return JSON only.\n"
                '- Use this exact schema: {"reviewer":"string","verdict":"pass|concern|block","summary":"string","findings":["string"]}.'
            ),
        ]
    )


def extract_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, end_index = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            trailing = text[index + end_index :].strip()
            if trailing.startswith("```"):
                trailing = trailing[3:].strip()
            if trailing and not trailing.startswith(("```",)):
                return candidate
            return candidate
    raise PlanError("Reviewer returned no JSON object.")


def parse_review_result(output_text: str, expected_reviewer: str) -> dict[str, Any]:
    data = extract_json_object(output_text)

    if not isinstance(data, dict):
        raise PlanError(f"{expected_reviewer} returned a non-object review result.")

    reviewer = data.get("reviewer")
    verdict = data.get("verdict")
    summary = data.get("summary")
    findings = data.get("findings")

    if reviewer != expected_reviewer:
        raise PlanError(f"{expected_reviewer} returned mismatched reviewer name: {reviewer!r}")
    if verdict not in {"pass", "concern", "block"}:
        raise PlanError(f"{expected_reviewer} returned invalid verdict: {verdict!r}")
    if not isinstance(summary, str) or not summary.strip():
        raise PlanError(f"{expected_reviewer} returned an empty summary.")
    if not isinstance(findings, list) or not all(isinstance(item, str) for item in findings):
        raise PlanError(f"{expected_reviewer} returned invalid findings.")

    return {
        "reviewer": reviewer,
        "verdict": verdict,
        "summary": summary.strip(),
        "findings": [item.strip() for item in findings if item.strip()],
    }


def print_review_summary(step_id: str, reviews: list[dict[str, Any]]) -> None:
    summary_text = ", ".join(
        f"{review['reviewer']}={review['verdict']}"
        for review in reviews
    )
    if any(review["verdict"] == "block" for review in reviews):
        review_status = "block"
    elif any(review["verdict"] == "concern" for review in reviews):
        review_status = "concern"
    else:
        review_status = "success"
    print(style_status_text(f"Review {step_id}: {summary_text}", review_status, bold=True), flush=True)
    for review in reviews:
        print(style_status_text(f"- {review['reviewer']}: {review['summary']}", review["verdict"]), flush=True)


def build_review_content(repo_path: Path, new_changed_files: list[str]) -> str:
    tracked_files: list[str] = []
    untracked_sections: list[str] = []

    for path_text in new_changed_files:
        status_result = run_command(["git", "status", "--short", "--", path_text], cwd=repo_path)
        if status_result.exit_code != 0:
            message = get_git_error_message(status_result)
            raise PlanError(f"Failed to inspect review file status for {path_text}: {message}")

        status_line = next((line for line in status_result.stdout.splitlines() if line.strip()), "")
        if status_line.startswith("??"):
            file_path = repo_path / path_text
            if not file_path.exists() or not file_path.is_file():
                continue
            content, truncated = read_text_file_with_limit(file_path, MAX_REVIEW_UNTRACKED_FILE_BYTES)
            suffix = " (truncated)" if truncated else ""
            untracked_sections.append(
                f"=== Untracked file: {path_text}{suffix} ===\n{content}"
            )
        else:
            tracked_files.append(path_text)

    tracked_diff_text = "(no tracked diff)"
    if tracked_files:
        diff_result = run_command(["git", "diff", "--", *tracked_files], cwd=repo_path)
        if diff_result.exit_code != 0:
            message = get_git_error_message(diff_result)
            raise PlanError(f"Failed to collect git diff for reviews: {message}")
        tracked_diff_text = diff_result.stdout.strip() or "(no tracked diff)"

    sections = [f"=== Tracked diff for current step files ===\n{tracked_diff_text}"]
    if untracked_sections:
        sections.extend(untracked_sections)
    return "\n\n".join(sections)


def run_step_reviews(
    repo_path: Path,
    objective: str,
    step_id: str,
    new_changed_files: list[str],
    verify_result: CommandResult | None,
    verbose: bool,
) -> list[dict[str, Any]]:
    review_content = build_review_content(repo_path, new_changed_files)
    verify_summary = build_verify_summary(verify_result)
    reviews: list[dict[str, Any]] = []

    for reviewer in REVIEWER_NAMES:
        review_prompt = build_review_prompt(
            reviewer=reviewer,
            objective=objective,
            step_id=step_id,
            changed_files=new_changed_files,
            review_content=review_content,
            verify_summary=verify_summary,
        )
        with tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False) as output_file:
            output_path = Path(output_file.name)
        try:
            review_result = run_streaming_command(
                [
                    "codex",
                    "exec",
                    "review",
                    "--uncommitted",
                    "--full-auto",
                    "-o",
                    str(output_path),
                    review_prompt,
                ],
                cwd=repo_path,
                stdout_prefix=f"{reviewer}: ",
                stderr_prefix=f"{reviewer}: ",
                filter_stream=not verbose,
            )
            review_output = output_path.read_text().strip()
        finally:
            output_path.unlink(missing_ok=True)

        if review_result.exit_code != 0:
            message = review_result.stderr.strip() or review_result.stdout.strip() or "unknown reviewer error"
            raise PlanError(f"{reviewer} failed: {message}")

        parsed_review = parse_review_result(review_output, reviewer)
        parsed_review["codex"] = {
            "command": review_result.command,
            "cwd": review_result.cwd,
            "exit_code": review_result.exit_code,
            "stdout": review_result.stdout,
            "stderr": review_result.stderr,
        }
        reviews.append(parsed_review)

    print_review_summary(step_id, reviews)
    return reviews


def should_print_diff_stat(diff_stat_output: str, verbose: bool) -> bool:
    if verbose:
        return bool(diff_stat_output.strip())
    lines = [line for line in diff_stat_output.splitlines() if line.strip()]
    if not lines:
        return False
    if len(lines) > 3:
        return False
    if any(len(line) > 120 for line in lines):
        return False
    return True


def build_step_result(
    step_id: str,
    step_prompt: str,
    codex_prompt: str,
    started_at: str,
    ended_at: str,
    expect_clean_diff: bool,
    status: str,
    failure_reason: str | None,
    before_status: CommandResult,
    after_status: CommandResult,
    diff_stat: CommandResult,
    baseline_changed_files: list[str],
    new_changed_files: list[str],
    changed_files: list[str],
    codex_result: CommandResult,
    verify_result: CommandResult | None,
    reviews: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    verify_data = extract_verify_data(verify_result)
    codex_summary = extract_codex_summary(codex_result.stdout, status, changed_files, verify_result)

    return {
        "id": step_id,
        "prompt": step_prompt,
        "codex_prompt": codex_prompt,
        "started_at": started_at,
        "ended_at": ended_at,
        "expect_clean_diff": expect_clean_diff,
        "status": status,
        "failure_reason": failure_reason,
        "before_git_status": {
            "exit_code": before_status.exit_code,
            "stdout": before_status.stdout,
            "stderr": before_status.stderr,
        },
        "after_git_status": {
            "exit_code": after_status.exit_code,
            "stdout": after_status.stdout,
            "stderr": after_status.stderr,
        },
        "diff_stat": {
            "exit_code": diff_stat.exit_code,
            "stdout": diff_stat.stdout,
            "stderr": diff_stat.stderr,
        },
        "baseline_changed_files": baseline_changed_files,
        "new_changed_files": new_changed_files,
        "changed_files": changed_files,
        "changed_files_count": len(changed_files),
        "codex_summary": codex_summary,
        "codex": {
            "command": codex_result.command,
            "cwd": codex_result.cwd,
            "exit_code": codex_result.exit_code,
            "stdout": codex_result.stdout,
            "stderr": codex_result.stderr,
        },
        "verify": verify_data,
        "reviews": reviews or [],
    }


def execute_step(
    repo_path: Path,
    objective: str,
    defaults: dict[str, Any],
    step: dict[str, Any],
    prior_summaries: list[str],
    verbose: bool,
    review_enabled: bool,
) -> dict[str, Any]:
    step_id = step["id"]
    print(style_text(f"== Step {step_id} ==", color=ANSI_CYAN, bold=True), flush=True)

    started_at = datetime.now(timezone.utc).isoformat()
    before_status = get_git_status(repo_path)
    codex_prompt = build_codex_prompt(objective, prior_summaries, step)
    prompt_lines_to_hide = {line.strip() for line in codex_prompt.splitlines() if line.strip()}
    codex_result = run_streaming_command(
        ["codex", "exec", "--full-auto", "--cd", str(repo_path), codex_prompt],
        cwd=repo_path,
        stdout_prefix=CODEX_STREAM_PREFIX,
        stderr_prefix=CODEX_STREAM_PREFIX,
        filter_stream=not verbose,
        hidden_lines=prompt_lines_to_hide if not verbose else None,
    )
    ended_at = datetime.now(timezone.utc).isoformat()
    after_status = get_git_status(repo_path)
    diff_stat = get_git_diff_stat(repo_path)

    baseline_entries = parse_git_status_entries(before_status.stdout)
    after_entries = parse_git_status_entries(after_status.stdout)
    baseline_changed_files = sorted(baseline_entries)
    new_changed_files = detect_new_changes(baseline_entries, after_entries)
    changed_files = parse_changed_files(after_status.stdout)
    expect_clean_diff = bool(step.get("expect_clean_diff", False))
    verify_command = step.get("verify") or defaults.get("verify")
    verify_result: CommandResult | None = None
    reviews: list[dict[str, Any]] = []

    status = "success"
    failure_reason: str | None = None

    if codex_result.exit_code != 0:
        status = "failure"
        failure_reason = "codex_failed"

    if expect_clean_diff and new_changed_files:
        status = "failure"
        failure_reason = "expected_clean_diff"

    if verify_command:
        verify_result = run_command(["sh", "-lc", verify_command], cwd=repo_path)
        print_command_result("verify", verify_result)
        if verify_result.exit_code != 0:
            status = "failure"
            failure_reason = "verify_failed"

    if review_enabled and status == "success" and new_changed_files:
        reviews = run_step_reviews(
            repo_path=repo_path,
            objective=objective,
            step_id=step_id,
            new_changed_files=new_changed_files,
            verify_result=verify_result,
            verbose=verbose,
        )
        if any(review["verdict"] == "block" for review in reviews):
            status = "failure"
            failure_reason = "review_blocked"
        elif any(review["verdict"] == "concern" for review in reviews):
            status = "paused"
            failure_reason = "review_concern"

    step_result = build_step_result(
        step_id=step_id,
        step_prompt=step["prompt"],
        codex_prompt=codex_prompt,
        started_at=started_at,
        ended_at=ended_at,
        expect_clean_diff=expect_clean_diff,
        status=status,
        failure_reason=failure_reason,
        before_status=before_status,
        after_status=after_status,
        diff_stat=diff_stat,
        baseline_changed_files=baseline_changed_files,
        new_changed_files=new_changed_files,
        changed_files=changed_files,
        codex_result=codex_result,
        verify_result=verify_result,
        reviews=reviews,
    )

    if should_print_diff_stat(diff_stat.stdout, verbose):
        print(style_text("git diff --stat:", bold=True), flush=True)
        print(diff_stat.stdout.rstrip(), flush=True)
    print_step_footer(step_result)

    return step_result


def save_run_log(run_data: dict[str, Any], script_root: Path) -> Path:
    runs_dir = script_root / ".kctl-runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = runs_dir / f"{timestamp}.json"
    log_path.write_text(json.dumps(run_data, indent=2))
    return log_path


def run_plan(
    plan_path: Path,
    verbose: bool,
    approve_each_step: bool,
    branch: str | None,
    commit: bool,
    commit_message: str | None,
    allow_dirty_start: bool,
    review_enabled: bool,
) -> int:
    plan = load_plan(plan_path)
    validate_plan(plan)

    repo_path = resolve_repo(plan_path, plan["repo"])
    ensure_git_repo(repo_path)
    branch_before = get_current_branch(repo_path)
    repo_dirty_at_start = bool(get_git_status(repo_path).stdout.strip())

    if branch:
        switch_to_branch(repo_path, branch)

    branch_after = get_current_branch(repo_path)
    if commit and not commit_message:
        raise PlanError("--commit requires --commit-message.")
    if commit and repo_dirty_at_start and not allow_dirty_start:
        raise PlanError("--commit is not allowed when the repo is already dirty. Use --allow-dirty-start to override.")
    if commit and branch_after in {"main", "master"}:
        raise PlanError("--commit is not allowed on main or master.")

    defaults = plan.get("defaults") or {}
    stop_on_failure = bool(defaults.get("stop_on_failure", False))
    prior_summaries: list[str] = []
    step_results: list[dict[str, Any]] = []
    run_status = "success"
    started_at = datetime.now(timezone.utc).isoformat()
    commit_created = False
    commit_sha: str | None = None

    steps = plan["steps"]
    for index, step in enumerate(steps):
        step_result = execute_step(
            repo_path=repo_path,
            objective=plan["objective"],
            defaults=defaults,
            step=step,
            prior_summaries=prior_summaries,
            verbose=verbose,
            review_enabled=review_enabled,
        )
        step_results.append(step_result)
        prior_summaries.append(summarize_step_result(step_result))

        failure_reason = step_result["failure_reason"]
        should_stop = failure_reason in {"expected_clean_diff", "review_blocked"} or (
            stop_on_failure and failure_reason in {"verify_failed", "codex_failed"}
        )

        if step_result["status"] == "paused":
            if prompt_to_continue_after_review(step_result["id"], step_result["reviews"]):
                step_result["status"] = "success"
                step_result["failure_reason"] = None
                prior_summaries[-1] = summarize_step_result(step_result)
            else:
                run_status = "stopped"
                break
        elif step_result["status"] != "success":
            run_status = "failure"

        if should_stop:
            run_status = "failure"
            break

        has_next_step = index < len(steps) - 1
        if approve_each_step and has_next_step and not prompt_to_continue():
            run_status = "stopped"
            break

    if run_status == "success" and commit:
        final_status = get_git_status(repo_path)
        if final_status.stdout.strip():
            commit_sha = create_commit(repo_path, commit_message)
            commit_created = True

    branch_after = get_current_branch(repo_path)

    run_data = {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "plan_path": str(plan_path.resolve()),
        "repo": str(repo_path),
        "objective": plan["objective"],
        "defaults": defaults,
        "review_enabled": review_enabled,
        "repo_dirty_at_start": repo_dirty_at_start,
        "branch_before": branch_before,
        "branch_after": branch_after,
        "commit_created": commit_created,
        "commit_sha": commit_sha,
        "status": run_status,
        "steps": step_results,
    }
    log_path = save_run_log(run_data, Path(__file__).resolve().parent)

    print(style_text("\nFinal summary:", bold=True), flush=True)
    for step_result in step_results:
        verify_label = "not-run"
        if step_result["verify"] is not None:
            verify_label = "passed" if step_result["verify"]["exit_code"] == 0 else "failed"
        summary_line = (
            f"- {step_result['id']}: {step_result['status']}, "
            f"verify={verify_label}, changed_files={step_result['changed_files_count']}"
        )
        print(style_status_text(summary_line, step_result["status"]), flush=True)
    print(style_text(f"Run log: {log_path}", bold=True), flush=True)

    return 1 if run_status == "failure" else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kctl", description="Run Codex plans against git repositories.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a YAML plan.")
    run_parser.add_argument("plan", help="Path to the YAML plan file.")
    run_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the raw Codex stream instead of the filtered terminal view.",
    )
    run_parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color output.",
    )
    run_parser.add_argument(
        "--approve-each-step",
        action="store_true",
        help="Prompt before starting the next step.",
    )
    run_parser.add_argument(
        "--branch",
        help="Create or switch to this branch before running the plan.",
    )
    run_parser.add_argument(
        "--commit",
        action="store_true",
        help="Create a local commit at the end if the run succeeds.",
    )
    run_parser.add_argument(
        "--commit-message",
        help="Commit message to use with --commit.",
    )
    run_parser.add_argument(
        "--allow-dirty-start",
        action="store_true",
        help="Allow --commit even if the repo is already dirty before the run starts.",
    )
    run_parser.add_argument(
        "--review",
        action="store_true",
        help="Run scope and test review passes after successful steps that leave new changes.",
    )

    init_parser = subparsers.add_parser("init", help="Materialize a YAML plan from a named template.")
    init_parser.add_argument("template_name", help="Template name from kctl-plan-templates.yaml.")
    init_parser.add_argument("output_path", help="Path where the generated plan YAML will be written.")
    init_parser.add_argument(
        "--repo",
        required=True,
        help="Repository path to write into the generated plan.",
    )
    init_parser.add_argument(
        "--objective",
        required=True,
        help="Objective text to write into the generated plan.",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    init_parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color output.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    global COLOR_ENABLED
    parser = build_parser()
    args = parser.parse_args(argv)
    COLOR_ENABLED = not getattr(args, "no_color", False)

    if args.command == "run":
        try:
            return run_plan(
                Path(args.plan).resolve(),
                verbose=args.verbose,
                approve_each_step=args.approve_each_step,
                branch=args.branch,
                commit=args.commit,
                commit_message=args.commit_message,
                allow_dirty_start=args.allow_dirty_start,
                review_enabled=args.review,
            )
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    if args.command == "init":
        try:
            return init_plan(
                template_name=args.template_name,
                output_path=Path(args.output_path).resolve(),
                repo=args.repo,
                objective=args.objective,
                force=args.force,
            )
        except PlanError as exc:
            print(style_status_text(f"Error: {exc}", "error", stream=sys.stderr, bold=True), file=sys.stderr)
            return 2

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
