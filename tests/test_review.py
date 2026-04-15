from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.output import NullOutputSink
from kctl_pkg.review import (
    MAX_REVIEW_UNTRACKED_FILE_BYTES,
    UNKNOWN_REVIEWER_ERROR,
    build_review_content,
    build_review_prompt,
    build_verify_summary,
    extract_json_object,
    parse_review_result,
    run_step_reviews,
    should_print_diff_stat,
)
from kctl_pkg.types import CommandResult, PlanError
from tests.test_ui_index import init_git_repo


class ReviewTests(unittest.TestCase):
    def test_build_verify_summary_handles_missing_result(self) -> None:
        self.assertEqual(build_verify_summary(None), "Verification not run.")

    def test_build_verify_summary_includes_status_and_output(self) -> None:
        summary = build_verify_summary(
            CommandResult(
                command=["bash", "scripts/test"],
                cwd="/tmp/repo",
                exit_code=1,
                stdout="out line\n",
                stderr="err line\n",
            )
        )

        self.assertIn("Verification failed with exit code 1.", summary)
        self.assertIn("stdout: out line", summary)
        self.assertIn("stderr: err line", summary)

    def test_build_review_prompt_includes_expected_sections(self) -> None:
        prompt = build_review_prompt(
            reviewer="scope reviewer",
            objective="Improve the dashboard",
            step_id="inspect",
            changed_files=["a.py", "b.py"],
            review_content="diff text",
            verify_summary="Verification passed.",
        )

        self.assertIn("Reviewer:\nscope reviewer", prompt)
        self.assertIn("Overall objective:\nImprove the dashboard", prompt)
        self.assertIn("Changed files:\n- a.py\n- b.py", prompt)
        self.assertIn("Review content:\ndiff text", prompt)
        self.assertIn("Verification result summary:\nVerification passed.", prompt)
        self.assertIn('"verdict":"pass|concern|block"', prompt)

    def test_extract_json_object_accepts_wrapped_json(self) -> None:
        data = extract_json_object("preface\n{\"ok\": true}\n```")

        self.assertEqual(data, {"ok": True})

    def test_extract_json_object_rejects_missing_json(self) -> None:
        with self.assertRaises(PlanError):
            extract_json_object("no object here")

    def test_parse_review_result_trims_findings(self) -> None:
        result = parse_review_result(
            json.dumps(
                {
                    "reviewer": "scope reviewer",
                    "verdict": "concern",
                    "summary": " needs work ",
                    "findings": [" first ", "", "second"],
                }
            ),
            "scope reviewer",
        )

        self.assertEqual(result["summary"], "needs work")
        self.assertEqual(result["findings"], ["first", "second"])

    def test_parse_review_result_rejects_mismatched_reviewer(self) -> None:
        with self.assertRaisesRegex(PlanError, "mismatched reviewer name"):
            parse_review_result(
                json.dumps(
                    {
                        "reviewer": "test reviewer",
                        "verdict": "pass",
                        "summary": "ok",
                        "findings": [],
                    }
                ),
                "scope reviewer",
            )

    def test_parse_review_result_rejects_invalid_findings(self) -> None:
        with self.assertRaisesRegex(PlanError, "invalid findings"):
            parse_review_result(
                json.dumps(
                    {
                        "reviewer": "scope reviewer",
                        "verdict": "pass",
                        "summary": "ok",
                        "findings": "not-a-list",
                    }
                ),
                "scope reviewer",
            )

    def test_should_print_diff_stat_respects_non_verbose_thresholds(self) -> None:
        self.assertFalse(should_print_diff_stat("", verbose=False))
        self.assertTrue(should_print_diff_stat(" a.py | 1 +", verbose=False))
        self.assertFalse(should_print_diff_stat("1\n2\n3\n4", verbose=False))
        self.assertFalse(should_print_diff_stat("x" * 121, verbose=False))
        self.assertTrue(should_print_diff_stat("1\n2\n3", verbose=True))

    def test_build_review_content_combines_tracked_and_untracked_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            (repo_path / "tracked.txt").write_text("tracked\n")
            (repo_path / "untracked.txt").write_text("untracked\n")

            from tests.test_ui_index import run_checked

            run_checked(["git", "add", "tracked.txt"], repo_path)
            run_checked(["git", "commit", "-m", "add tracked"], repo_path)
            (repo_path / "tracked.txt").write_text("tracked updated\n")

            content = build_review_content(repo_path, ["tracked.txt", "untracked.txt"])

            self.assertIn("=== Tracked diff for current step files ===", content)
            self.assertIn("tracked updated", content)
            self.assertIn("=== Untracked file: untracked.txt ===", content)
            self.assertIn("untracked", content)

    def test_build_review_content_marks_truncated_untracked_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            oversized = "x" * (MAX_REVIEW_UNTRACKED_FILE_BYTES + 100)
            (repo_path / "large.txt").write_text(oversized)

            content = build_review_content(repo_path, ["large.txt"])

            self.assertIn("=== Untracked file: large.txt (truncated) ===", content)

    def test_build_review_content_raises_on_git_status_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir)

            with patch("kctl_pkg.review.run_command") as mock_run_command:
                mock_run_command.return_value = CommandResult(
                    command=["git", "status"],
                    cwd=str(repo_path),
                    exit_code=1,
                    stdout="",
                    stderr="fatal: bad status",
                )

                with self.assertRaisesRegex(PlanError, "Failed to inspect review file status"):
                    build_review_content(repo_path, ["bad.txt"])

    def test_build_review_content_raises_on_git_diff_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir)

            with patch("kctl_pkg.review.run_command") as mock_run_command:
                mock_run_command.side_effect = [
                    CommandResult(
                        command=["git", "status"],
                        cwd=str(repo_path),
                        exit_code=0,
                        stdout=" M tracked.txt\n",
                        stderr="",
                    ),
                    CommandResult(
                        command=["git", "diff"],
                        cwd=str(repo_path),
                        exit_code=1,
                        stdout="",
                        stderr="fatal: bad diff",
                    ),
                ]

                with self.assertRaisesRegex(PlanError, "Failed to collect git diff for reviews"):
                    build_review_content(repo_path, ["tracked.txt"])

    def test_run_step_reviews_uses_claude_provider(self) -> None:
        review_payloads = [
            {
                "reviewer": "scope reviewer",
                "verdict": "pass",
                "summary": "scope ok",
                "findings": [],
            },
            {
                "reviewer": "test reviewer",
                "verdict": "concern",
                "summary": "tests thin",
                "findings": ["add regression"],
            },
        ]
        streaming_results = [
            CommandResult(
                command=["claude"],
                cwd="/tmp/repo",
                exit_code=0,
                stdout=json.dumps(review_payloads[0]),
                stderr="",
            ),
            CommandResult(
                command=["claude"],
                cwd="/tmp/repo",
                exit_code=0,
                stdout=json.dumps(review_payloads[1]),
                stderr="",
            ),
        ]
        summary_calls: list[tuple[str, list[dict[str, object]]]] = []

        with patch("kctl_pkg.review.build_review_content", return_value="diff text"), patch(
            "kctl_pkg.review.run_streaming_command", side_effect=streaming_results
        ) as mock_run_streaming:
            reviews = run_step_reviews(
                repo_path=Path("/tmp/repo"),
                objective="Improve coverage",
                step_id="review",
                new_changed_files=["a.py"],
                verify_result=None,
                verbose=False,
                print_review_summary=lambda step_id, reviews: summary_calls.append((step_id, reviews)),
                output_sink=NullOutputSink(),
                provider="claude",
            )

        self.assertEqual([item["reviewer"] for item in reviews], ["scope reviewer", "test reviewer"])
        self.assertEqual(summary_calls[0][0], "review")
        self.assertEqual(mock_run_streaming.call_count, 2)
        first_call = mock_run_streaming.call_args_list[0]
        self.assertEqual(first_call.args[0][:3], ["claude", "--permission-mode", "plan"])

    def test_run_step_reviews_uses_codex_provider_and_reads_artifact_file(self) -> None:
        payloads = [
            {
                "reviewer": "scope reviewer",
                "verdict": "pass",
                "summary": "scope ok",
                "findings": [],
            },
            {
                "reviewer": "test reviewer",
                "verdict": "block",
                "summary": "tests broken",
                "findings": ["fix tests"],
            },
        ]
        written_paths: list[Path] = []

        def fake_run_streaming(command: list[str], cwd: Path, **kwargs: object) -> CommandResult:
            output_index = len(written_paths)
            output_path = Path(command[6])
            output_path.write_text(json.dumps(payloads[output_index]))
            written_paths.append(output_path)
            return CommandResult(command=command, cwd=str(cwd), exit_code=0, stdout="ok\n", stderr="")

        summary_calls: list[list[dict[str, object]]] = []
        with patch("kctl_pkg.review.build_review_content", return_value="diff text"), patch(
            "kctl_pkg.review.run_streaming_command", side_effect=fake_run_streaming
        ) as mock_run_streaming:
            reviews = run_step_reviews(
                repo_path=Path("/tmp/repo"),
                objective="Improve coverage",
                step_id="review",
                new_changed_files=["a.py"],
                verify_result=CommandResult(["bash"], "/tmp/repo", 0, "ok", ""),
                verbose=True,
                print_review_summary=lambda _step_id, reviews: summary_calls.append(reviews),
                output_sink=NullOutputSink(),
                provider="codex",
            )

        self.assertEqual([item["verdict"] for item in reviews], ["pass", "block"])
        self.assertEqual(len(summary_calls[0]), 2)
        self.assertEqual(mock_run_streaming.call_count, 2)
        self.assertTrue(all(not path.exists() for path in written_paths))
        first_call = mock_run_streaming.call_args_list[0]
        self.assertEqual(first_call.args[0][:4], ["codex", "exec", "review", "--uncommitted"])

    def test_run_step_reviews_raises_on_reviewer_failure(self) -> None:
        with patch("kctl_pkg.review.build_review_content", return_value="diff text"), patch(
            "kctl_pkg.review.run_streaming_command",
            return_value=CommandResult(
                command=["codex"],
                cwd="/tmp/repo",
                exit_code=1,
                stdout="",
                stderr="boom",
            ),
        ):
            with self.assertRaisesRegex(PlanError, "scope reviewer failed: boom"):
                run_step_reviews(
                    repo_path=Path("/tmp/repo"),
                    objective="Improve coverage",
                    step_id="review",
                    new_changed_files=["a.py"],
                    verify_result=None,
                    verbose=False,
                    print_review_summary=lambda *args: None,
                    output_sink=NullOutputSink(),
                    provider="codex",
                )

    def test_run_step_reviews_uses_unknown_reviewer_error_fallback(self) -> None:
        with patch("kctl_pkg.review.build_review_content", return_value="diff text"), patch(
            "kctl_pkg.review.run_streaming_command",
            return_value=CommandResult(
                command=["claude"],
                cwd="/tmp/repo",
                exit_code=1,
                stdout="",
                stderr="",
            ),
        ):
            with self.assertRaisesRegex(PlanError, UNKNOWN_REVIEWER_ERROR):
                run_step_reviews(
                    repo_path=Path("/tmp/repo"),
                    objective="Improve coverage",
                    step_id="review",
                    new_changed_files=["a.py"],
                    verify_result=None,
                    verbose=False,
                    print_review_summary=lambda *args: None,
                    output_sink=NullOutputSink(),
                    provider="claude",
                )


if __name__ == "__main__":
    unittest.main()
