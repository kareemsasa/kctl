from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.plan import (
    build_artifact_context,
    build_artifact_instruction,
    build_agent_prompt,
    build_plan_from_template,
    build_verify_instruction,
    get_step_kind,
    infer_output_schema,
    infer_review_policy,
    infer_step_mode,
    infer_step_type,
    infer_verify_mode,
    init_plan,
    load_plan,
    load_plan_templates,
    normalize_plan,
    normalize_step,
    resolve_plan_path,
    resolve_provider_config,
    resolve_step_mode,
    resolve_step_output,
    resolve_step_review,
    resolve_step_type,
    resolve_step_verify,
    validate_plan,
)
from kctl_pkg.types import PlanError


class PlanTests(unittest.TestCase):
    def test_resolve_plan_path_direct_and_env_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            direct = tmp / "direct.yaml"
            direct.write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n")
            rooted_dir = tmp / "plans"
            rooted_dir.mkdir()
            rooted = rooted_dir / "rooted.yaml"
            rooted.write_text("repo: /tmp\nobjective: y\nsteps:\n  - id: implement\n    prompt: y\n")

            self.assertEqual(resolve_plan_path(str(direct)), direct.resolve())
            with patch.dict("os.environ", {"KCTL_PLAN_ROOT": str(rooted_dir)}, clear=False):
                self.assertEqual(resolve_plan_path("rooted.yaml"), rooted.resolve())

            with patch.dict("os.environ", {"KCTL_PLAN_ROOT": str(rooted_dir)}, clear=False):
                with self.assertRaises(PlanError):
                    resolve_plan_path("missing.yaml")

            with patch.dict("os.environ", {}, clear=True):
                with self.assertRaises(PlanError):
                    resolve_plan_path(str(tmp / "missing.yaml"))

    def test_load_plan_and_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            plan_path = tmp / "plan.yaml"
            plan_path.write_text("repo: /tmp\nobjective: x\nsteps:\n  - id: implement\n    prompt: x\n")
            loaded = load_plan(plan_path)
            self.assertEqual(loaded["objective"], "x")

            bad_path = tmp / "bad.yaml"
            bad_path.write_text("- just\n- a\n- list\n")
            with self.assertRaises(PlanError):
                load_plan(bad_path)

            templates_path = tmp / "kctl-plan-templates.yaml"
            templates_path.write_text(
                "templates:\n"
                "  single:\n"
                "    shape:\n"
                "      defaults:\n"
                "        stop_on_failure: true\n"
                "      steps:\n"
                "        - id: implement\n"
                "          prompt: Do work\n"
            )
            templates = load_plan_templates(tmp)
            self.assertIn("single", templates)

            templates_path.write_text("templates: {}\n")
            with self.assertRaises(PlanError):
                load_plan_templates(tmp)

    def test_validate_plan_rejects_invalid_fields(self) -> None:
        base = {
            "repo": "/tmp/repo",
            "objective": "obj",
            "defaults": {},
            "steps": [{"id": "implement", "prompt": "do it"}],
        }
        cases = [
            ({"repo": ""}, "repo"),
            ({"objective": ""}, "objective"),
            ({"defaults": []}, "defaults"),
            ({"defaults": {"verify": 1}}, "defaults.verify"),
            ({"defaults": {"verify_shell": 1}}, "defaults.verify_shell"),
            ({"defaults": {"verify_mode": "weird"}}, "defaults.verify_mode"),
            ({"defaults": {"provider": "weird"}}, "defaults.provider"),
            ({"defaults": {"provider": "codex", "permission_mode": "auto"}}, "permission_mode is only applicable"),
            ({"defaults": {"permission_mode": "weird", "provider": "claude"}}, "defaults.permission_mode"),
            ({"defaults": {"stop_on_failure": "yes"}}, "defaults.stop_on_failure"),
            ({"defaults": {"required_env": ["", 2]}}, "defaults.required_env"),
            ({"required_env": ["", 2]}, "required_env"),
            ({"steps": []}, "steps"),
            ({"steps": ["bad"]}, "Step #1"),
            ({"steps": [{"id": "", "prompt": "x"}]}, "field 'id'"),
            ({"steps": [{"id": "inspect"}]}, "field 'prompt'"),
            ({"steps": [{"id": "verify", "type": "verify", "commands": [], "verify": 1}]}, "field 'verify'"),
            ({"steps": [{"id": "verify", "type": "verify", "commands": [], "verify_shell": 1}]}, "field 'verify_shell'"),
            ({"steps": [{"id": "verify", "type": "weird", "prompt": "x"}]}, "field 'type'"),
            ({"steps": [{"id": "verify", "kind": "weird", "prompt": "x"}]}, "field 'kind'"),
            ({"steps": [{"id": "verify", "name": "", "prompt": "x"}]}, "field 'name'"),
            ({"steps": [{"id": "verify", "type": "verify", "commands": "bad"}]}, "field 'commands'"),
            ({"steps": [{"id": "inspect", "prompt": "x", "mode": "bad"}]}, "field 'mode'"),
            ({"steps": [{"id": "inspect", "prompt": "x", "output": "bad"}]}, "field 'output'"),
            ({"steps": [{"id": "inspect", "prompt": "x", "output": {"schema": ""}}]}, "output.schema"),
            ({"steps": [{"id": "evaluate", "prompt": "x", "output": {"schema": "review_v1"}}]}, "output.schema"),
            ({"steps": [{"id": "review", "type": "review", "prompt": "x", "review": "bad"}]}, "field 'review'"),
            ({"steps": [{"id": "review", "type": "review", "prompt": "x", "review": {"policy": "bad"}}]}, "review.policy"),
            ({"steps": [{"id": "verify", "type": "verify", "commands": [], "verify_mode": "bad"}]}, "verify_mode"),
            ({"steps": [{"id": "inspect", "prompt": "x", "expect_clean_diff": "yes"}]}, "expect_clean_diff"),
        ]

        for overrides, expected in cases:
            with self.subTest(expected=expected):
                plan = dict(base)
                plan.update(overrides)
                with self.assertRaises(PlanError) as context:
                    validate_plan(plan)
                self.assertIn(expected, str(context.exception))

        with self.assertRaises(PlanError):
            validate_plan(
                {
                    "repo": "/tmp/repo",
                    "objective": "obj",
                    "steps": [
                        {"id": "implement", "prompt": "one"},
                        {"id": "implement", "prompt": "two"},
                    ],
                }
            )

    def test_step_inference_and_resolution_helpers(self) -> None:
        self.assertEqual(infer_step_type({"id": "verify"}), "verify")
        self.assertEqual(infer_step_type({"id": "review"}), "review")
        self.assertEqual(infer_step_type({"id": "plan"}), "analyze")
        self.assertEqual(infer_step_type({"id": "implement"}), "change")
        self.assertEqual(resolve_step_type({"id": "implement", "type": "analyze"})["source"], "explicit")

        self.assertEqual(infer_output_schema({"id": "inspect"}), "inspect_v1")
        self.assertEqual(infer_output_schema({"id": "plan"}), "plan_v1")
        self.assertIsNone(infer_output_schema({"id": "implement"}))
        self.assertEqual(resolve_step_output({"id": "inspect"})["source"], "inferred")
        self.assertEqual(resolve_step_output({"id": "implement", "output": {"schema": "plan_v1"}})["source"], "explicit")
        self.assertEqual(
            resolve_step_output({"id": "evaluate", "output": {"schema": "evaluation_v1"}})["effective_schema"],
            "evaluation_v1",
        )

        self.assertEqual(infer_review_policy({"id": "review"}), "manual")
        self.assertIsNone(infer_review_policy({"id": "implement"}))
        self.assertEqual(resolve_step_review({"id": "review"})["source"], "inferred")
        self.assertEqual(
            resolve_step_review({"id": "review", "review": {"policy": "blocking"}})["effective_policy"],
            "blocking",
        )

        self.assertEqual(infer_step_mode({"expect_clean_diff": True}), "read-only")
        self.assertEqual(resolve_step_mode({"id": "inspect"})["source"], "default")
        self.assertEqual(resolve_step_mode({"id": "inspect", "mode": "read-only"})["source"], "explicit")
        self.assertEqual(resolve_step_mode({"id": "inspect", "expect_clean_diff": True})["source"], "inferred")

        self.assertEqual(infer_verify_mode({"id": "verify"}), "legacy")
        self.assertEqual(resolve_step_verify({"id": "verify"}, {"verify_mode": "full"})["source"], "default")
        self.assertEqual(
            resolve_step_verify({"id": "verify", "verify_mode": "legacy"}, {"verify_mode": "full"})["source"],
            "explicit",
        )

    def test_normalization_and_step_kind_helpers(self) -> None:
        step = normalize_step({"id": "inspect", "prompt": "look"}, {"verify_mode": "full"})
        self.assertEqual(step["_kctl_step_type"]["effective_type"], "analyze")
        self.assertEqual(step["_kctl_output"]["effective_schema"], "inspect_v1")
        self.assertEqual(step["_kctl_verify"]["effective_mode"], "full")

        normalized = normalize_plan(
            {
                "repo": "/tmp/repo",
                "objective": "obj",
                "defaults": {"provider": "claude", "permission_mode": "plan"},
                "steps": [
                    {"id": "inspect", "prompt": "look"},
                    {"id": "verify", "type": "verify", "commands": ["printf ok"]},
                ],
            }
        )
        self.assertEqual(normalized["_kctl_provider"], "claude")
        self.assertEqual(normalized["_kctl_permission_mode"], "plan")
        self.assertEqual(get_step_kind({"type": "verify"}), "verify")
        self.assertEqual(get_step_kind({"type": "review"}), "agent")
        self.assertEqual(get_step_kind({"_kctl_step_type": {"effective_type": "verify"}}), "verify")
        self.assertEqual(get_step_kind({"kind": "verify"}), "verify")
        self.assertEqual(get_step_kind({"commands": ["printf ok"]}), "verify")
        self.assertEqual(get_step_kind({"id": "implement"}), "agent")

    def test_provider_config_and_template_building(self) -> None:
        self.assertEqual(resolve_provider_config({}), ("codex", "auto"))
        self.assertEqual(resolve_provider_config({"provider": "claude", "permission_mode": "plan"}), ("claude", "plan"))

        templates = {
            "single": {
                "shape": {
                    "defaults": {"provider": "codex"},
                    "steps": [{"id": "implement", "prompt": "Do work"}],
                }
            },
            "legacy": {"steps": [{"id": "implement", "prompt": "Do work"}]},
        }
        built = build_plan_from_template(templates, "single", "/tmp/repo", "obj")
        self.assertEqual(built["repo"], "/tmp/repo")
        self.assertEqual(built["defaults"]["provider"], "codex")
        legacy = build_plan_from_template(templates, "legacy", "/tmp/repo", "obj")
        self.assertTrue(legacy["defaults"]["stop_on_failure"])

        with self.assertRaises(PlanError):
            build_plan_from_template(templates, "missing", "/tmp/repo", "obj")
        with self.assertRaises(PlanError):
            build_plan_from_template({"bad": []}, "bad", "/tmp/repo", "obj")
        with self.assertRaises(PlanError):
            build_plan_from_template({"bad": {"shape": []}}, "bad", "/tmp/repo", "obj")
        with self.assertRaises(PlanError):
            build_plan_from_template({"bad": {"shape": {"steps": []}}}, "bad", "/tmp/repo", "obj")
        with self.assertRaises(PlanError):
            build_plan_from_template({"bad": {"shape": {"steps": [{"id": "implement", "prompt": "x"}], "defaults": []}}}, "bad", "/tmp/repo", "obj")

    def test_init_plan_writes_file_and_honors_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            templates_path = tmp / "kctl-plan-templates.yaml"
            templates_path.write_text(
                "templates:\n"
                "  single:\n"
                "    shape:\n"
                "      steps:\n"
                "        - id: implement\n"
                "          prompt: Do work\n"
            )
            output_path = tmp / "plans" / "001-plan.yaml"
            buffer = io.StringIO()
            with patch("kctl_pkg.plan.project_root", return_value=tmp), redirect_stdout(buffer):
                result = init_plan("single", output_path, "/tmp/repo", "Ship it", force=False)

            self.assertEqual(result, 0)
            self.assertIn("Created plan", buffer.getvalue())
            self.assertTrue(output_path.exists())
            self.assertIn("objective: Ship it", output_path.read_text())

            with self.assertRaises(PlanError):
                init_plan("single", output_path, "/tmp/repo", "Ship it", force=False)

    def test_artifact_and_prompt_helpers(self) -> None:
        self.assertIn("inspect artifact", build_artifact_instruction("inspect_v1") or "")
        self.assertIn("plan artifact", build_artifact_instruction("plan_v1") or "")
        self.assertIn("evaluation artifact", build_artifact_instruction("evaluation_v1") or "")
        self.assertIsNone(build_artifact_instruction("other"))

        prior = {"inspect": {"summary": "ok"}, "plan": {"objective": "obj"}}
        self.assertIn("Structured inspect artifact", build_artifact_context("plan", prior) or "")
        self.assertIn("Structured inspect artifact", build_artifact_context("evaluate", prior) or "")
        self.assertIn("Structured plan artifact", build_artifact_context("implement", prior) or "")
        self.assertIn("Structured plan artifact", build_artifact_context("verify", prior) or "")
        self.assertIsNone(build_artifact_context("inspect", {}))
        self.assertIn("Verification execution model", build_verify_instruction())

        prompt = build_agent_prompt(
            "Improve tests",
            ["inspect complete"],
            {"id": "plan", "prompt": "Draft a plan", "_kctl_output": {"effective_schema": "plan_v1"}},
            prior_artifacts=prior,
        )
        self.assertIn("Overall objective", prompt)
        self.assertIn("Prior step summaries", prompt)
        self.assertIn("Structured inspect artifact", prompt)
        self.assertIn("Structured artifact requirement", prompt)

        verify_prompt = build_agent_prompt(
            "Improve tests",
            [],
            {"id": "verify", "prompt": "Assess verification"},
            prior_artifacts={"plan": {"objective": "obj"}},
        )
        self.assertIn("No prior steps have run", verify_prompt)
        self.assertIn("Verification execution model", verify_prompt)


if __name__ == "__main__":
    unittest.main()
