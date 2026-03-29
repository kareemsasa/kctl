from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml

from .paths import project_root
from .terminal import style_text
from .types import PlanError

STEP_TYPES = {"analyze", "change", "verify", "review"}
STRUCTURED_OUTPUT_SCHEMAS = {"inspect_v1", "plan_v1", "review_v1"}
REVIEW_POLICIES = {"advisory", "blocking", "manual"}
STEP_MODES = {"default", "read-only"}
VERIFY_MODES = {"legacy", "full"}


def resolve_plan_path(plan_value: str) -> Path:
    direct_path = Path(plan_value).expanduser()
    if direct_path.exists() and direct_path.is_file():
        return direct_path.resolve()

    plan_root = os.environ.get("KCTL_PLAN_ROOT")
    if plan_root:
        rooted_path = (Path(plan_root).expanduser() / direct_path).resolve()
        if rooted_path.exists() and rooted_path.is_file():
            return rooted_path
        raise PlanError(
            "Plan file was not found. "
            f"Checked direct path: {direct_path.resolve()} and "
            f"KCTL_PLAN_ROOT path: {rooted_path}."
        )

    raise PlanError(
        "Plan file was not found. "
        f"Checked direct path: {direct_path.resolve()}. "
        "KCTL_PLAN_ROOT was not set."
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
    verify_shell = defaults.get("verify_shell")
    if verify_shell is not None and not isinstance(verify_shell, str):
        raise PlanError("defaults.verify_shell must be a string if provided.")
    verify_mode = defaults.get("verify_mode")
    if verify_mode is not None and verify_mode not in VERIFY_MODES:
        raise PlanError("defaults.verify_mode must be one of: legacy, full.")

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
        step_kind = get_step_kind(step)
        if not isinstance(step_id, str) or not step_id.strip():
            raise PlanError(f"Step #{index} field 'id' is required and must be a non-empty string.")
        if step_id in step_ids:
            raise PlanError(f"Duplicate step id: {step_id}")
        step_ids.add(step_id)
        if step_kind == "agent" and (not isinstance(prompt, str) or not prompt.strip()):
            raise PlanError(f"Step '{step_id}' field 'prompt' is required and must be a non-empty string.")
        step_verify = step.get("verify")
        if step_verify is not None and not isinstance(step_verify, str):
            raise PlanError(f"Step '{step_id}' field 'verify' must be a string if provided.")
        step_verify_shell = step.get("verify_shell")
        if step_verify_shell is not None and not isinstance(step_verify_shell, str):
            raise PlanError(f"Step '{step_id}' field 'verify_shell' must be a string if provided.")
        declared_step_type = step.get("type")
        if declared_step_type is not None and declared_step_type not in STEP_TYPES:
            raise PlanError(
                f"Step '{step_id}' field 'type' must be one of: analyze, change, verify, review."
            )
        explicit_step_kind = step.get("kind")
        if explicit_step_kind is not None and explicit_step_kind not in {"agent", "verify"}:
            raise PlanError(f"Step '{step_id}' field 'kind' must be 'agent' or 'verify' if provided.")
        step_name = step.get("name")
        if step_name is not None and (not isinstance(step_name, str) or not step_name.strip()):
            raise PlanError(f"Step '{step_id}' field 'name' must be a non-empty string if provided.")
        commands = step.get("commands")
        if commands is not None:
            if not isinstance(commands, list) or not all(isinstance(item, str) for item in commands):
                raise PlanError(f"Step '{step_id}' field 'commands' must be a list of strings if provided.")
        mode = step.get("mode")
        if mode is not None and mode not in STEP_MODES:
            raise PlanError(f"Step '{step_id}' field 'mode' must be one of: default, read-only.")
        output = step.get("output")
        if output is not None:
            if not isinstance(output, dict):
                raise PlanError(f"Step '{step_id}' field 'output' must be a mapping if provided.")
            output_schema = output.get("schema")
            if output_schema is not None and (not isinstance(output_schema, str) or not output_schema.strip()):
                raise PlanError(f"Step '{step_id}' field 'output.schema' must be a non-empty string if provided.")
        review = step.get("review")
        if review is not None:
            if not isinstance(review, dict):
                raise PlanError(f"Step '{step_id}' field 'review' must be a mapping if provided.")
            review_policy = review.get("policy")
            if review_policy is not None and review_policy not in REVIEW_POLICIES:
                raise PlanError(
                    f"Step '{step_id}' field 'review.policy' must be one of: advisory, blocking, manual."
                )
        verify_mode = step.get("verify_mode")
        if verify_mode is not None and verify_mode not in VERIFY_MODES:
            raise PlanError(f"Step '{step_id}' field 'verify_mode' must be one of: legacy, full.")
        expect_clean_diff = step.get("expect_clean_diff")
        if expect_clean_diff is not None and not isinstance(expect_clean_diff, bool):
            raise PlanError(f"Step '{step_id}' field 'expect_clean_diff' must be a boolean if provided.")


def infer_step_type(step: dict[str, Any]) -> str:
    declared_kind = step.get("kind")
    commands = step.get("commands")
    step_id = str(step.get("id") or "").strip()

    if declared_kind == "verify":
        return "verify"
    if isinstance(commands, list) and commands:
        return "verify"
    if step_id == "verify":
        return "verify"
    if step_id == "review":
        return "review"
    if step_id in {"inspect", "plan"}:
        return "analyze"
    return "change"


def resolve_step_type(step: dict[str, Any]) -> dict[str, Any]:
    declared_type = step.get("type")
    inferred_type = infer_step_type(step)
    if declared_type is not None:
        effective_type = declared_type
        source = "explicit"
    else:
        effective_type = inferred_type
        source = "inferred"
    return {
        "declared_type": declared_type,
        "inferred_type": inferred_type,
        "effective_type": effective_type,
        "source": source,
    }


def infer_output_schema(step: dict[str, Any]) -> str | None:
    step_id = str(step.get("id") or "").strip()
    if step_id == "inspect":
        return "inspect_v1"
    if step_id == "plan":
        return "plan_v1"
    return None


def resolve_step_output(step: dict[str, Any]) -> dict[str, Any]:
    output = step.get("output")
    explicit_schema = None
    if isinstance(output, dict):
        explicit_schema = output.get("schema")
    inferred_schema = infer_output_schema(step)
    if explicit_schema is not None:
        effective_schema = explicit_schema
        source = "explicit"
    elif inferred_schema is not None:
        effective_schema = inferred_schema
        source = "inferred"
    else:
        effective_schema = None
        source = "none"
    return {
        "declared_schema": explicit_schema,
        "inferred_schema": inferred_schema,
        "effective_schema": effective_schema,
        "source": source,
    }


def infer_review_policy(step: dict[str, Any]) -> str | None:
    step_id = str(step.get("id") or "").strip()
    if step_id == "review":
        return "manual"
    return None


def resolve_step_review(step: dict[str, Any]) -> dict[str, Any]:
    review = step.get("review")
    explicit_policy = None
    if isinstance(review, dict):
        explicit_policy = review.get("policy")
    inferred_policy = infer_review_policy(step)
    if explicit_policy is not None:
        effective_policy = explicit_policy
        source = "explicit"
    elif inferred_policy is not None:
        effective_policy = inferred_policy
        source = "inferred"
    else:
        effective_policy = None
        source = "none"
    return {
        "declared_policy": explicit_policy,
        "inferred_policy": inferred_policy,
        "effective_policy": effective_policy,
        "source": source,
    }


def infer_step_mode(step: dict[str, Any]) -> str:
    if bool(step.get("expect_clean_diff", False)):
        return "read-only"
    return "default"


def resolve_step_mode(step: dict[str, Any]) -> dict[str, Any]:
    declared_mode = step.get("mode")
    inferred_mode = infer_step_mode(step)
    if declared_mode is not None:
        effective_mode = declared_mode
        source = "explicit"
    elif inferred_mode != "default":
        effective_mode = inferred_mode
        source = "inferred"
    else:
        effective_mode = "default"
        source = "default"
    return {
        "declared_mode": declared_mode,
        "inferred_mode": inferred_mode,
        "effective_mode": effective_mode,
        "source": source,
    }


def infer_verify_mode(step: dict[str, Any], defaults: dict[str, Any] | None = None) -> str:
    defaults = defaults or {}
    if step.get("verify") or step.get("commands") or step.get("id") == "verify":
        return "legacy"
    if defaults.get("verify") is not None:
        return "legacy"
    return "legacy"


def resolve_step_verify(step: dict[str, Any], defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    defaults = defaults or {}
    declared_mode = step.get("verify_mode")
    default_mode = defaults.get("verify_mode")
    inferred_mode = infer_verify_mode(step, defaults)
    if declared_mode is not None:
        effective_mode = declared_mode
        source = "explicit"
    elif default_mode is not None:
        effective_mode = default_mode
        source = "default"
    else:
        effective_mode = inferred_mode
        source = "inferred"
    return {
        "declared_mode": declared_mode,
        "default_mode": default_mode,
        "inferred_mode": inferred_mode,
        "effective_mode": effective_mode,
        "source": source,
    }


def normalize_step(step: dict[str, Any], defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized_step = dict(step)
    normalized_step["_kctl_step_type"] = resolve_step_type(step)
    normalized_step["_kctl_output"] = resolve_step_output(step)
    normalized_step["_kctl_review"] = resolve_step_review(step)
    normalized_step["_kctl_mode"] = resolve_step_mode(step)
    normalized_step["_kctl_verify"] = resolve_step_verify(step, defaults)
    return normalized_step


def normalize_plan(plan: dict[str, Any]) -> dict[str, Any]:
    normalized_plan = dict(plan)
    defaults = normalized_plan.get("defaults") or {}
    normalized_plan["steps"] = [normalize_step(step, defaults) for step in plan["steps"]]
    return normalized_plan


def get_step_kind(step: dict[str, Any]) -> str:
    step_type = step.get("type")
    if step_type == "verify":
        return "verify"
    if step_type in {"analyze", "change", "review"}:
        return "agent"
    resolved_step_type = step.get("_kctl_step_type")
    if isinstance(resolved_step_type, dict):
        effective_type = resolved_step_type.get("effective_type")
        if effective_type == "verify":
            return "verify"
        if effective_type in {"analyze", "change", "review"}:
            return "agent"
    explicit_kind = step.get("kind")
    if explicit_kind in {"agent", "verify"}:
        return explicit_kind
    commands = step.get("commands")
    if isinstance(commands, list) and commands:
        return "verify"
    return "agent"


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
    templates = load_plan_templates(project_root())
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


def build_artifact_instruction(schema_name: str | None) -> str | None:
    if schema_name == "inspect_v1":
        return (
            "Structured artifact requirement:\n"
            "- End your response with a fenced ```json block only for the inspect artifact.\n"
            '- Use this exact shape: {"project_type":"string","stack":["string"],"summary":"string",'
            '"key_directories":[{"path":"string","purpose":"string"}],'
            '"key_files":[{"path":"string","purpose":"string"}],'
            '"relevant_areas":[{"path":"string","reason":"string"}],'
            '"constraints":[{"path":"string","note":"string"}],'
            '"assumptions":["string"],"unknowns":["string"]}.\n'
            "- Keep values concise and repository-specific."
        )
    if schema_name == "plan_v1":
        return (
            "Structured artifact requirement:\n"
            "- End your response with a fenced ```json block only for the plan artifact.\n"
            '- Use this exact shape: {"objective":"string","approach":"string","steps":[{"id":"string",'
            '"name":"string","files":["string"],"intent":"string"}],"verification":{"commands":["string"],'
            '"manual_checks":["string"]},"risks":["string"],"out_of_scope":["string"]}.\n'
            "- Base the plan on the provided inspect artifact rather than hidden session context."
        )
    return None


def build_artifact_context(step_id: str, prior_artifacts: dict[str, dict[str, Any]]) -> str | None:
    if step_id == "plan" and "inspect" in prior_artifacts:
        return "Structured inspect artifact:\n```json\n" + json.dumps(
            prior_artifacts["inspect"], indent=2, sort_keys=True
        ) + "\n```"
    if step_id not in {"inspect", "plan", "verify"} and "plan" in prior_artifacts:
        return "Structured plan artifact:\n```json\n" + json.dumps(
            prior_artifacts["plan"], indent=2, sort_keys=True
        ) + "\n```"
    if step_id == "verify" and "plan" in prior_artifacts:
        return "Structured plan artifact:\n```json\n" + json.dumps(
            prior_artifacts["plan"], indent=2, sort_keys=True
        ) + "\n```"
    return None


def build_verify_instruction() -> str:
    return (
        "Verification execution model:\n"
        "- kctl will run configured verification commands itself and persist verify.json.\n"
        "- Do not invent command exit codes.\n"
        "- Use the plan artifact as the source of intended verification scope.\n"
        "- Focus your response on validation findings, likely issues, and what should be checked manually."
    )


def build_codex_prompt(
    objective: str,
    prior_summaries: list[str],
    step: dict[str, Any],
    prior_artifacts: dict[str, dict[str, Any]] | None = None,
) -> str:
    prior_artifacts = prior_artifacts or {}
    step_id = step["id"]
    sections = [
        "You are executing one step in a larger kctl plan.",
        f"Overall objective:\n{objective.strip()}",
    ]
    if prior_summaries:
        sections.append("Prior step summaries:\n" + "\n".join(f"- {summary}" for summary in prior_summaries))
    else:
        sections.append("Prior step summaries:\n- No prior steps have run.")
    artifact_context = build_artifact_context(step_id, prior_artifacts)
    if artifact_context:
        sections.append(artifact_context)
    else:
        sections.append("Structured artifacts available:\n- None")
    sections.append(f"Current step id: {step_id}")
    sections.append(f"Current step prompt:\n{step['prompt'].strip()}")
    output_info = step.get("_kctl_output")
    schema_name = None
    if isinstance(output_info, dict):
        schema_name = output_info.get("effective_schema")
    artifact_instruction = build_artifact_instruction(schema_name)
    if artifact_instruction:
        sections.append(artifact_instruction)
    elif step_id == "verify":
        sections.append(build_verify_instruction())
    sections.append(
        "Constraints:\n"
        "- Work only in the current repository.\n"
        "- Keep changes scoped to the current step.\n"
        "- Assume each step is a fresh Codex invocation with no hidden memory from prior steps.\n"
        "- In your final response, summarize what you changed and any verification you ran."
    )
    return "\n\n".join(sections)
