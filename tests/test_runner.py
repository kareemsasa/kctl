from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.output import BufferedOutputSink, NullOutputSink
from kctl_pkg.runner import (
    apply_provider_override,
    build_step_file_prefix,
    build_synthetic_agent_summary,
    build_verify_artifact,
    classify_agent_failure,
    collect_verify_environment,
    combine_verify_results,
    execute_agent_step,
    extract_agent_summary,
    extract_last_fenced_json_block,
    extract_verify_data,
    get_effective_mode_info,
    get_effective_output_info,
    get_effective_review_info,
    get_effective_step_type,
    get_effective_verify_info,
    get_verify_label,
    load_structured_artifact,
    maybe_build_verify_artifact,
    maybe_collect_phase_artifact,
    parse_structured_artifact,
    parse_verify_shell,
    print_command_result,
    print_review_summary,
    prompt_to_continue,
    prompt_to_continue_after_review,
    resolve_verify_commands,
    run_verify_commands,
    save_run_log,
    select_steps_to_run,
    shorten_summary,
    summarize_command_output,
    summarize_step_result,
    summarize_verify_environment,
)
from kctl_pkg.types import CommandResult, PlanError


class RunnerHelperTests(unittest.TestCase):
    def test_extract_verify_data_and_label(self) -> None:
        result = CommandResult(["bash", "-lc", "printf ok"], "/tmp/repo", 0, "ok\n", "")
        data = extract_verify_data(result, {"shell": "sh -lc"})

        self.assertEqual(data["exit_code"], 0)
        self.assertEqual(data["environment"]["shell"], "sh -lc")
        self.assertEqual(get_verify_label(None), "skipped")
        self.assertEqual(get_verify_label(data), "passed")

    def test_summary_helpers(self) -> None:
        verify_result = CommandResult(["bash"], "/tmp", 1, "failed\n", "")
        self.assertEqual(
            build_synthetic_agent_summary("success", ["a.py"], verify_result),
            "status=success; changed_files=a.py; verify=failed",
        )
        self.assertEqual(shorten_summary("abc", limit=5), "abc")
        self.assertEqual(shorten_summary("abcdef", limit=5), "ab...")
        self.assertEqual(
            summarize_command_output(CommandResult(["bash"], "/tmp", 1, "out line\n", "err line\n")),
            "exit_code=1; stdout=out line; stderr=err line",
        )

    def test_extract_agent_summary_prefers_meaningful_line(self) -> None:
        stdout = "noise\nUpdated the plan and reran tests.\n"
        self.assertEqual(
            extract_agent_summary(stdout, "success", ["a.py"], None),
            "Updated the plan and reran tests.",
        )

    def test_parse_verify_shell_and_collect_environment(self) -> None:
        self.assertEqual(parse_verify_shell(None), ["sh", "-lc"])
        self.assertEqual(parse_verify_shell("bash -lc"), ["bash", "-lc"])
        with self.assertRaisesRegex(PlanError, "must not be empty"):
            parse_verify_shell("   ")

        with patch("kctl_pkg.runner.probe_command", side_effect=["/usr/bin/node", "v1", "/usr/bin/npm", "10"]):
            env = collect_verify_environment(["sh", "-lc"], Path("/tmp/repo"))
        self.assertIn("shell=sh -lc", summarize_verify_environment(env))

    def test_run_verify_commands_and_combine_results(self) -> None:
        results = [
            CommandResult(["sh", "-lc", "printf ok"], "/tmp", 0, "ok\n", ""),
            CommandResult(["sh", "-lc", "printf bad"], "/tmp", 1, "", "bad\n"),
        ]
        sink = BufferedOutputSink()
        with patch("kctl_pkg.runner.run_shell_command", side_effect=results):
            verify_results = run_verify_commands(Path("/tmp"), ["sh", "-lc"], ["printf ok", "printf bad"], sink)

        self.assertEqual(len(verify_results), 2)
        combined = combine_verify_results(verify_results, ["sh", "-lc"])
        self.assertEqual(combined.exit_code, 1)
        self.assertIn("(printf ok) && (printf bad)", combined.command[-1])
        self.assertEqual(combine_verify_results([], ["sh", "-lc"]), None)

    def test_print_helpers_render_expected_text(self) -> None:
        sink = BufferedOutputSink()
        print_command_result("verify", CommandResult(["bash"], "/tmp", 1, "out\n", "err\n"), sink)
        print_review_summary(
            "review",
            [
                {"reviewer": "scope reviewer", "verdict": "concern", "summary": "scope issue"},
                {"reviewer": "test reviewer", "verdict": "pass", "summary": "ok"},
            ],
            sink,
        )
        rendered = "".join(text for _stream, text in sink._entries)
        self.assertIn("verify: exit 1", rendered)
        self.assertIn("scope reviewer=concern", rendered)

    def test_prompt_helpers(self) -> None:
        with patch("builtins.input", return_value="y"):
            self.assertTrue(prompt_to_continue(True))
        with patch("builtins.input", return_value="n"):
            self.assertFalse(prompt_to_continue_after_review("step", [{"verdict": "concern"}], True))
        self.assertFalse(prompt_to_continue(False))

    def test_structured_artifact_helpers(self) -> None:
        inspect_payload = {
            "project_type": "app",
            "stack": ["py"],
            "summary": "sum",
            "key_directories": [{"path": "src", "purpose": "code"}],
            "key_files": [{"path": "README.md", "purpose": "docs"}],
            "relevant_areas": [{"path": "src", "reason": "logic"}],
            "constraints": [{"path": "src", "note": "keep stable"}],
            "assumptions": ["a"],
            "unknowns": ["u"],
        }
        artifact = parse_structured_artifact("inspect_v1", f"done\n```json\n{json.dumps(inspect_payload)}\n```\n")
        self.assertEqual(artifact["project_type"], "app")
        self.assertEqual(extract_last_fenced_json_block("x\n```json\n{\"a\":1}\n```"), '{"a":1}')
        with self.assertRaisesRegex(PlanError, "Expected a final fenced JSON block"):
            extract_last_fenced_json_block("no json")
        with self.assertRaisesRegex(PlanError, "No structured artifact parser"):
            parse_structured_artifact("other_v1", "```json\n{}\n```")

    def test_step_metadata_helpers(self) -> None:
        step = {
            "id": "verify",
            "_kctl_step_type": {"effective_type": "verify"},
            "_kctl_output": {"effective_schema": "inspect_v1"},
            "_kctl_review": {"effective_policy": "blocking"},
            "_kctl_mode": {"effective_mode": "read-only"},
            "_kctl_verify": {"effective_mode": "plan"},
        }
        self.assertEqual(get_effective_step_type(step), "verify")
        self.assertEqual(get_effective_output_info(step)["effective_schema"], "inspect_v1")
        self.assertEqual(get_effective_review_info(step)["effective_policy"], "blocking")
        self.assertEqual(get_effective_mode_info(step)["effective_mode"], "read-only")
        self.assertEqual(get_effective_verify_info(step)["effective_mode"], "plan")

    def test_apply_provider_override_and_classify_failure(self) -> None:
        self.assertEqual(apply_provider_override({}, "claude")["permission_mode"], "auto")
        self.assertNotIn("permission_mode", apply_provider_override({"permission_mode": "auto"}, "codex"))
        with self.assertRaisesRegex(PlanError, "provider override must be one of"):
            apply_provider_override({}, "other")

        quota_reason, quota_details = classify_agent_failure(
            CommandResult(["codex"], "/tmp", 1, "", "You've hit your limit, resets tomorrow\n")
        )
        self.assertEqual(quota_reason, "agent_quota_exhausted")
        self.assertIn("reset_hint", quota_details)
        rate_reason, _ = classify_agent_failure(
            CommandResult(["codex"], "/tmp", 1, "", "429 too many requests\n")
        )
        self.assertEqual(rate_reason, "agent_rate_limited")

    def test_verify_and_artifact_builders(self) -> None:
        verify_results = [CommandResult(["sh", "-lc", "printf ok"], "/tmp", 0, "ok\n", "")]
        artifact = build_verify_artifact(
            verify_results,
            {"verification": {"manual_checks": ["click button"]}},
            {"shell": "sh -lc"},
        )
        self.assertEqual(artifact["status"], "pass")
        self.assertEqual(artifact["tests"][-1]["name"], "click button")

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            step = {"id": "verify"}
            structured_artifacts, next_artifacts = maybe_build_verify_artifact(
                step,
                "verify",
                run_dir,
                1,
                verify_results,
                {"plan": {"verification": {"manual_checks": []}}},
                {"shell": "sh -lc"},
            )
            self.assertIn("verify", structured_artifacts)
            self.assertIn("verify", next_artifacts)
            self.assertEqual(load_structured_artifact(structured_artifacts["verify"])["status"], "pass")

    def test_maybe_collect_phase_artifact_and_verify_command_resolution(self) -> None:
        inspect_step = {"id": "inspect", "_kctl_output": {"effective_schema": "inspect_v1"}}
        inspect_stdout = (
            "done\n```json\n"
            + json.dumps(
                {
                    "project_type": "app",
                    "stack": ["py"],
                    "summary": "sum",
                    "key_directories": [{"path": "src", "purpose": "code"}],
                    "key_files": [{"path": "README.md", "purpose": "docs"}],
                    "relevant_areas": [{"path": "src", "reason": "logic"}],
                    "constraints": [{"path": "src", "note": "keep stable"}],
                    "assumptions": ["a"],
                    "unknowns": ["u"],
                }
            )
            + "\n```\n"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts, next_artifacts, parse_error, failure_reason = maybe_collect_phase_artifact(
                inspect_step,
                "analyze",
                CommandResult(["codex"], "/tmp", 0, inspect_stdout, ""),
                run_dir,
                1,
            )
            self.assertIn("inspect_v1", artifacts)
            self.assertIn("inspect", next_artifacts)
            self.assertIsNone(parse_error)
            self.assertIsNone(failure_reason)

            artifacts, _next, parse_error, failure_reason = maybe_collect_phase_artifact(
                inspect_step,
                "analyze",
                CommandResult(["codex"], "/tmp", 0, "no json", ""),
                run_dir,
                2,
            )
            self.assertEqual(artifacts, {})
            self.assertEqual(failure_reason, "artifact_parse_failed")
            self.assertIn("Expected a final fenced JSON block", parse_error)

        self.assertEqual(
            resolve_verify_commands({"id": "verify"}, {}, {"plan": {"verification": {"commands": ["pytest"]}}}, "verify"),
            ["pytest"],
        )
        self.assertEqual(
            resolve_verify_commands({"id": "step", "commands": ["npm test"]}, {}, {}, "verify"),
            ["npm test"],
        )
        self.assertEqual(resolve_verify_commands({"id": "step", "verify": "pytest"}, {}, {}, "change"), ["pytest"])

    def test_execute_agent_step_and_save_run_log(self) -> None:
        step = {"id": "inspect", "prompt": "Inspect", "_kctl_step_type": {"effective_type": "analyze"}}
        with patch(
            "kctl_pkg.runner.run_streaming_command",
            return_value=CommandResult(["claude"], "/tmp", 0, "ok\n", ""),
        ) as mock_run_streaming:
            prompt, result = execute_agent_step(
                Path("/tmp"),
                "Objective",
                [],
                step,
                {},
                False,
                NullOutputSink(),
                provider="claude",
                permission_mode="auto",
            )
        self.assertIn("Current step id: inspect", prompt)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(mock_run_streaming.call_args.args[0][:3], ["claude", "--permission-mode", "plan"])

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            run_data = {
                "started_at": "2026-01-01T00:00:00+00:00",
                "ended_at": "2026-01-01T00:00:01+00:00",
                "objective": "obj",
                "steps": [],
            }
            log_path = save_run_log(run_data, run_dir)
            self.assertTrue(log_path.exists())
            self.assertTrue((run_dir / "summary.md").exists())
            self.assertEqual(build_step_file_prefix(3), "step-03")

    def test_step_summary_and_selection_helpers(self) -> None:
        step_result = {
            "id": "inspect",
            "status": "success",
            "changed_files": ["a.py"],
            "verify": None,
            "agent_summary": "Did the thing",
        }
        self.assertIn("summary=Did the thing", summarize_step_result(step_result))

        steps = [{"id": "inspect"}, {"id": "verify"}]
        self.assertEqual(select_steps_to_run(steps, from_step="verify"), [{"id": "verify"}])
        self.assertEqual(select_steps_to_run(steps, only_step="inspect"), [{"id": "inspect"}])
        with self.assertRaisesRegex(PlanError, "cannot be used together"):
            select_steps_to_run(steps, from_step="inspect", only_step="verify")
        with self.assertRaisesRegex(PlanError, "Plan step not found"):
            select_steps_to_run(steps, from_step="missing")


if __name__ == "__main__":
    unittest.main()
