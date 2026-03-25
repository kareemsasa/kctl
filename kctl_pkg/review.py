from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from .git import get_git_error_message, read_text_file_with_limit
from .output import OutputSink
from .process import run_command, run_streaming_command
from .types import CommandResult, PlanError


REVIEWER_NAMES = ("scope reviewer", "test reviewer")
MAX_REVIEW_UNTRACKED_FILE_BYTES = 16_000
UNKNOWN_REVIEWER_ERROR = "unknown reviewer error"


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
            untracked_sections.append(f"=== Untracked file: {path_text}{suffix} ===\n{content}")
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
    print_review_summary: Any,
    output_sink: OutputSink,
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
                output_sink=output_sink,
            )
            review_output = output_path.read_text().strip()
        finally:
            output_path.unlink(missing_ok=True)
        if review_result.exit_code != 0:
            message = review_result.stderr.strip() or review_result.stdout.strip() or UNKNOWN_REVIEWER_ERROR
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
