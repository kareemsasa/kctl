from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class CommandResult:
    command: list[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str


class PlanError(Exception):
    pass


@dataclass
class PathPurposeEntry:
    path: str
    purpose: str


@dataclass
class PathReasonEntry:
    path: str
    reason: str


@dataclass
class PathNoteEntry:
    path: str
    note: str


@dataclass
class InspectArtifact:
    project_type: str
    stack: list[str]
    summary: str
    key_directories: list[PathPurposeEntry]
    key_files: list[PathPurposeEntry]
    relevant_areas: list[PathReasonEntry]
    constraints: list[PathNoteEntry]
    assumptions: list[str]
    unknowns: list[str]


@dataclass
class PlanStepArtifact:
    id: str
    name: str
    files: list[str]
    intent: str


@dataclass
class VerificationPlanArtifact:
    commands: list[str]
    manual_checks: list[str]


@dataclass
class PlanArtifact:
    objective: str
    approach: str
    steps: list[PlanStepArtifact]
    verification: VerificationPlanArtifact
    risks: list[str]
    out_of_scope: list[str]


@dataclass
class VerifyCommandArtifact:
    command: str
    exit_code: int
    summary: str


@dataclass
class VerifyTestArtifact:
    name: str
    result: str


@dataclass
class VerifyIssueArtifact:
    severity: str
    summary: str


@dataclass
class VerifyArtifact:
    status: str
    commands_run: list[VerifyCommandArtifact]
    tests: list[VerifyTestArtifact]
    issues: list[VerifyIssueArtifact]
    recommended_next_action: str


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanError(f"{label} must be an object.")
    return value


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanError(f"{label} must be a non-empty string.")
    return value.strip()


def _require_string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise PlanError(f"{label} must be a list of strings.")
    return [item.strip() for item in value]


def _parse_path_purpose_list(value: Any, label: str) -> list[PathPurposeEntry]:
    if not isinstance(value, list):
        raise PlanError(f"{label} must be a list.")
    entries: list[PathPurposeEntry] = []
    for index, item in enumerate(value, start=1):
        data = _require_mapping(item, f"{label}[{index}]")
        entries.append(
            PathPurposeEntry(
                path=_require_string(data.get("path"), f"{label}[{index}].path"),
                purpose=_require_string(data.get("purpose"), f"{label}[{index}].purpose"),
            )
        )
    return entries


def _parse_path_reason_list(value: Any, label: str) -> list[PathReasonEntry]:
    if not isinstance(value, list):
        raise PlanError(f"{label} must be a list.")
    entries: list[PathReasonEntry] = []
    for index, item in enumerate(value, start=1):
        data = _require_mapping(item, f"{label}[{index}]")
        entries.append(
            PathReasonEntry(
                path=_require_string(data.get("path"), f"{label}[{index}].path"),
                reason=_require_string(data.get("reason"), f"{label}[{index}].reason"),
            )
        )
    return entries


def _parse_path_note_list(value: Any, label: str) -> list[PathNoteEntry]:
    if not isinstance(value, list):
        raise PlanError(f"{label} must be a list.")
    entries: list[PathNoteEntry] = []
    for index, item in enumerate(value, start=1):
        data = _require_mapping(item, f"{label}[{index}]")
        entries.append(
            PathNoteEntry(
                path=_require_string(data.get("path"), f"{label}[{index}].path"),
                note=_require_string(data.get("note"), f"{label}[{index}].note"),
            )
        )
    return entries


def parse_inspect_artifact(value: Any) -> InspectArtifact:
    data = _require_mapping(value, "inspect artifact")
    return InspectArtifact(
        project_type=_require_string(data.get("project_type"), "inspect.project_type"),
        stack=_require_string_list(data.get("stack"), "inspect.stack"),
        summary=_require_string(data.get("summary"), "inspect.summary"),
        key_directories=_parse_path_purpose_list(
            data.get("key_directories"), "inspect.key_directories"
        ),
        key_files=_parse_path_purpose_list(data.get("key_files"), "inspect.key_files"),
        relevant_areas=_parse_path_reason_list(
            data.get("relevant_areas"), "inspect.relevant_areas"
        ),
        constraints=_parse_path_note_list(data.get("constraints"), "inspect.constraints"),
        assumptions=_require_string_list(data.get("assumptions"), "inspect.assumptions"),
        unknowns=_require_string_list(data.get("unknowns"), "inspect.unknowns"),
    )


def parse_plan_artifact(value: Any) -> PlanArtifact:
    data = _require_mapping(value, "plan artifact")
    steps_value = data.get("steps")
    if not isinstance(steps_value, list):
        raise PlanError("plan.steps must be a list.")
    steps: list[PlanStepArtifact] = []
    for index, item in enumerate(steps_value, start=1):
        entry = _require_mapping(item, f"plan.steps[{index}]")
        steps.append(
            PlanStepArtifact(
                id=_require_string(entry.get("id"), f"plan.steps[{index}].id"),
                name=_require_string(entry.get("name"), f"plan.steps[{index}].name"),
                files=_require_string_list(entry.get("files"), f"plan.steps[{index}].files"),
                intent=_require_string(entry.get("intent"), f"plan.steps[{index}].intent"),
            )
        )
    verification_data = _require_mapping(data.get("verification"), "plan.verification")
    verification = VerificationPlanArtifact(
        commands=_require_string_list(
            verification_data.get("commands"), "plan.verification.commands"
        ),
        manual_checks=_require_string_list(
            verification_data.get("manual_checks"), "plan.verification.manual_checks"
        ),
    )
    return PlanArtifact(
        objective=_require_string(data.get("objective"), "plan.objective"),
        approach=_require_string(data.get("approach"), "plan.approach"),
        steps=steps,
        verification=verification,
        risks=_require_string_list(data.get("risks"), "plan.risks"),
        out_of_scope=_require_string_list(data.get("out_of_scope"), "plan.out_of_scope"),
    )


def parse_verify_artifact(value: Any) -> VerifyArtifact:
    data = _require_mapping(value, "verify artifact")
    status = _require_string(data.get("status"), "verify.status")
    if status not in {"pass", "fail", "partial"}:
        raise PlanError("verify.status must be one of: pass, fail, partial.")
    commands_value = data.get("commands_run")
    if not isinstance(commands_value, list):
        raise PlanError("verify.commands_run must be a list.")
    commands_run: list[VerifyCommandArtifact] = []
    for index, item in enumerate(commands_value, start=1):
        entry = _require_mapping(item, f"verify.commands_run[{index}]")
        exit_code = entry.get("exit_code")
        if not isinstance(exit_code, int):
            raise PlanError(f"verify.commands_run[{index}].exit_code must be an integer.")
        commands_run.append(
            VerifyCommandArtifact(
                command=_require_string(
                    entry.get("command"), f"verify.commands_run[{index}].command"
                ),
                exit_code=exit_code,
                summary=_require_string(
                    entry.get("summary"), f"verify.commands_run[{index}].summary"
                ),
            )
        )
    tests_value = data.get("tests")
    if not isinstance(tests_value, list):
        raise PlanError("verify.tests must be a list.")
    tests: list[VerifyTestArtifact] = []
    for index, item in enumerate(tests_value, start=1):
        entry = _require_mapping(item, f"verify.tests[{index}]")
        result = _require_string(entry.get("result"), f"verify.tests[{index}].result")
        if result not in {"pass", "fail", "skipped"}:
            raise PlanError(f"verify.tests[{index}].result must be pass, fail, or skipped.")
        tests.append(
            VerifyTestArtifact(
                name=_require_string(entry.get("name"), f"verify.tests[{index}].name"),
                result=result,
            )
        )
    issues_value = data.get("issues")
    if not isinstance(issues_value, list):
        raise PlanError("verify.issues must be a list.")
    issues: list[VerifyIssueArtifact] = []
    for index, item in enumerate(issues_value, start=1):
        entry = _require_mapping(item, f"verify.issues[{index}]")
        severity = _require_string(entry.get("severity"), f"verify.issues[{index}].severity")
        if severity not in {"info", "warning", "error"}:
            raise PlanError(
                f"verify.issues[{index}].severity must be info, warning, or error."
            )
        issues.append(
            VerifyIssueArtifact(
                severity=severity,
                summary=_require_string(
                    entry.get("summary"), f"verify.issues[{index}].summary"
                ),
            )
        )
    next_action = _require_string(
        data.get("recommended_next_action"), "verify.recommended_next_action"
    )
    if next_action not in {"stop", "repair", "manual_review"}:
        raise PlanError(
            "verify.recommended_next_action must be stop, repair, or manual_review."
        )
    return VerifyArtifact(
        status=status,
        commands_run=commands_run,
        tests=tests,
        issues=issues,
        recommended_next_action=next_action,
    )


def artifact_to_dict(value: InspectArtifact | PlanArtifact | VerifyArtifact) -> dict[str, Any]:
    return asdict(value)
