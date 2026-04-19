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
    stopped: bool = False


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
    result: str
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
class EvaluationRepositoryArtifact:
    name: str
    path: str


@dataclass
class EvaluationCategoryArtifact:
    id: str
    name: str
    weight: int
    score: int
    max_score: int
    summary: str
    evidence: list[str]
    risks: list[str]


@dataclass
class EvaluationArtifact:
    repository: EvaluationRepositoryArtifact
    rubric_id: str
    rubric_version: str
    evaluated_at: str
    repo_name: str
    repo_path: str
    commit_sha: str | None
    max_score: int
    confidence: str | None
    profile: str | None
    normalization_basis: str
    summary: str
    categories: list[EvaluationCategoryArtifact]
    overall_findings: list[str]
    blocking_issues: list[str]
    recommended_next_actions: list[str]


@dataclass
class IssueReportIssueArtifact:
    rank: int
    title: str
    severity: str
    why_it_matters: str
    evidence: str
    next_action: str


@dataclass
class IssueReportArtifact:
    summary: str
    top_issues: list[IssueReportIssueArtifact]
    watch_items: list[str]
    best_next_tasks: list[str]


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


def _require_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PlanError(f"{label} must be an integer.")
    return value


def _optional_string(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, label)


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
        result = _require_string(entry.get("result"), f"verify.commands_run[{index}].result")
        if result not in {"pass", "fail", "skipped"}:
            raise PlanError(
                f"verify.commands_run[{index}].result must be pass, fail, or skipped."
            )
        commands_run.append(
            VerifyCommandArtifact(
                command=_require_string(
                    entry.get("command"), f"verify.commands_run[{index}].command"
                ),
                result=result,
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


def parse_evaluation_artifact(value: Any) -> EvaluationArtifact:
    data = _require_mapping(value, "evaluation artifact")
    repository_data = _require_mapping(data.get("repository"), "evaluation.repository")
    repository = EvaluationRepositoryArtifact(
        name=_require_string(repository_data.get("name"), "evaluation.repository.name"),
        path=_require_string(repository_data.get("path"), "evaluation.repository.path"),
    )
    rubric_id = _require_string(data.get("rubric_id"), "evaluation.rubric_id")
    max_score = _require_int(data.get("max_score"), "evaluation.max_score")
    if max_score <= 0:
        raise PlanError("evaluation.max_score must be greater than zero.")
    repo_name = _require_string(data.get("repo_name"), "evaluation.repo_name")
    repo_path = _require_string(data.get("repo_path"), "evaluation.repo_path")
    categories_value = data.get("categories")
    if not isinstance(categories_value, list) or not categories_value:
        raise PlanError("evaluation.categories must be a non-empty list.")
    categories: list[EvaluationCategoryArtifact] = []
    category_ids: set[str] = set()
    total_weight = 0
    for index, item in enumerate(categories_value, start=1):
        entry = _require_mapping(item, f"evaluation.categories[{index}]")
        category_id = _require_string(entry.get("id"), f"evaluation.categories[{index}].id")
        if category_id in category_ids:
            raise PlanError(f"evaluation.categories[{index}].id must be unique.")
        category_ids.add(category_id)
        weight = _require_int(entry.get("weight"), f"evaluation.categories[{index}].weight")
        score = _require_int(entry.get("score"), f"evaluation.categories[{index}].score")
        category_max_score = _require_int(
            entry.get("max_score"), f"evaluation.categories[{index}].max_score"
        )
        if weight <= 0:
            raise PlanError(f"evaluation.categories[{index}].weight must be greater than zero.")
        if category_max_score <= 0:
            raise PlanError(f"evaluation.categories[{index}].max_score must be greater than zero.")
        if category_max_score != max_score:
            raise PlanError(
                f"evaluation.categories[{index}].max_score must match evaluation.max_score."
            )
        if score < 0 or score > category_max_score:
            raise PlanError(
                f"evaluation.categories[{index}].score must be between zero and max_score."
            )
        total_weight += weight
        categories.append(
            EvaluationCategoryArtifact(
                id=category_id,
                name=_require_string(entry.get("name"), f"evaluation.categories[{index}].name"),
                weight=weight,
                score=score,
                max_score=category_max_score,
                summary=_require_string(entry.get("summary"), f"evaluation.categories[{index}].summary"),
                evidence=_require_string_list(
                    entry.get("evidence"), f"evaluation.categories[{index}].evidence"
                ),
                risks=_require_string_list(
                    entry.get("risks"), f"evaluation.categories[{index}].risks"
                ),
            )
        )
    if total_weight != 100:
        raise PlanError("evaluation.categories weights must sum to 100.")
    return EvaluationArtifact(
        repository=repository,
        rubric_id=rubric_id,
        rubric_version=_require_string(data.get("rubric_version"), "evaluation.rubric_version"),
        evaluated_at=_require_string(data.get("evaluated_at"), "evaluation.evaluated_at"),
        repo_name=repo_name,
        repo_path=repo_path,
        commit_sha=_optional_string(data.get("commit_sha"), "evaluation.commit_sha"),
        max_score=max_score,
        confidence=_optional_string(data.get("confidence"), "evaluation.confidence"),
        profile=_optional_string(data.get("profile"), "evaluation.profile"),
        normalization_basis=_require_string(
            data.get("normalization_basis"), "evaluation.normalization_basis"
        ),
        summary=_require_string(data.get("summary"), "evaluation.summary"),
        categories=categories,
        overall_findings=_require_string_list(
            data.get("overall_findings"), "evaluation.overall_findings"
        ),
        blocking_issues=_require_string_list(
            data.get("blocking_issues"), "evaluation.blocking_issues"
        ),
        recommended_next_actions=_require_string_list(
            data.get("recommended_next_actions"), "evaluation.recommended_next_actions"
        ),
    )


def parse_issue_report_artifact(value: Any) -> IssueReportArtifact:
    data = _require_mapping(value, "issue report artifact")
    issues_value = data.get("top_issues")
    if not isinstance(issues_value, list):
        raise PlanError("issue_report.top_issues must be a list.")
    issues: list[IssueReportIssueArtifact] = []
    seen_ranks: set[int] = set()
    for index, item in enumerate(issues_value, start=1):
        entry = _require_mapping(item, f"issue_report.top_issues[{index}]")
        rank = _require_int(entry.get("rank"), f"issue_report.top_issues[{index}].rank")
        if rank <= 0:
            raise PlanError(f"issue_report.top_issues[{index}].rank must be greater than zero.")
        if rank in seen_ranks:
            raise PlanError(f"issue_report.top_issues[{index}].rank must be unique.")
        seen_ranks.add(rank)
        severity = _require_string(
            entry.get("severity"), f"issue_report.top_issues[{index}].severity"
        )
        if severity not in {"critical", "high", "medium", "low"}:
            raise PlanError(
                f"issue_report.top_issues[{index}].severity must be one of: "
                "critical, high, medium, low."
            )
        issues.append(
            IssueReportIssueArtifact(
                rank=rank,
                title=_require_string(
                    entry.get("title"), f"issue_report.top_issues[{index}].title"
                ),
                severity=severity,
                why_it_matters=_require_string(
                    entry.get("why_it_matters"),
                    f"issue_report.top_issues[{index}].why_it_matters",
                ),
                evidence=_require_string(
                    entry.get("evidence"), f"issue_report.top_issues[{index}].evidence"
                ),
                next_action=_require_string(
                    entry.get("next_action"), f"issue_report.top_issues[{index}].next_action"
                ),
            )
        )
    best_next_tasks = _require_string_list(
        data.get("best_next_tasks"), "issue_report.best_next_tasks"
    )
    if len(best_next_tasks) != 3:
        raise PlanError("issue_report.best_next_tasks must contain exactly 3 items.")
    return IssueReportArtifact(
        summary=_require_string(data.get("summary"), "issue_report.summary"),
        top_issues=issues,
        watch_items=_require_string_list(data.get("watch_items"), "issue_report.watch_items"),
        best_next_tasks=best_next_tasks,
    )


def artifact_to_dict(
    value: InspectArtifact | PlanArtifact | EvaluationArtifact | IssueReportArtifact | VerifyArtifact,
) -> dict[str, Any]:
    return asdict(value)
