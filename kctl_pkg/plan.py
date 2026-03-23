from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .paths import project_root
from .terminal import style_text
from .types import PlanError


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
