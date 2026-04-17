from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .types import PlanError, parse_evaluation_artifact


def _yaml_module() -> Any:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise PlanError(
            "PyYAML is required for evaluation manifest operations. Install dependencies with "
            "`python3 -m pip install -e .[dev]` or `python3 -m pip install -r requirements.txt`."
        ) from exc
    return yaml


def load_evaluation_manifest(manifest_path: Path) -> dict[str, Any]:
    yaml = _yaml_module()
    try:
        data = yaml.safe_load(manifest_path.read_text())
    except yaml.YAMLError as exc:
        raise PlanError(f"Failed to parse evaluation manifest YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise PlanError("Evaluation manifest must contain a top-level mapping.")
    evaluations = data.get("evaluations")
    if not isinstance(evaluations, list) or not evaluations:
        raise PlanError("Evaluation manifest must contain a non-empty 'evaluations' list.")
    for index, item in enumerate(evaluations, start=1):
        if not isinstance(item, dict):
            raise PlanError(f"evaluations[{index}] must be a mapping.")
        path_value = item.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            raise PlanError(f"evaluations[{index}].path must be a non-empty string.")
    output_dir = data.get("output_dir")
    if output_dir is not None and (not isinstance(output_dir, str) or not output_dir.strip()):
        raise PlanError("Evaluation manifest field 'output_dir' must be a non-empty string if provided.")
    return data


def resolve_manifest_output_dir(manifest_path: Path, out_override: Path | None = None) -> Path:
    if out_override is not None:
        return out_override.expanduser().resolve()
    manifest = load_evaluation_manifest(manifest_path)
    output_dir = manifest.get("output_dir")
    if not isinstance(output_dir, str) or not output_dir.strip():
        raise PlanError("Output directory is required via --out or manifest output_dir.")
    return (manifest_path.parent / output_dir).resolve()


def _resolve_manifest_evaluation_paths(manifest: dict[str, Any], manifest_path: Path) -> list[Path]:
    resolved_paths: list[Path] = []
    seen: set[Path] = set()
    for index, item in enumerate(manifest["evaluations"], start=1):
        raw_path = Path(str(item["path"])).expanduser()
        path = (manifest_path.parent / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
        if not path.exists():
            raise PlanError(f"Evaluation artifact does not exist: {path}")
        if not path.is_file():
            raise PlanError(f"Evaluation artifact is not a file: {path}")
        if path in seen:
            raise PlanError(f"Duplicate evaluation artifact path in manifest: {path}")
        seen.add(path)
        resolved_paths.append(path)
    return resolved_paths


def _load_evaluation_artifact(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise PlanError(f"Failed to parse evaluation artifact JSON {path}: {exc}") from exc
    return asdict(parse_evaluation_artifact(data))


def _compute_category_contributions(categories: list[dict[str, Any]], max_score: int) -> dict[str, float]:
    return {
        str(category["id"]): round(float(category["weight"]) * float(category["score"]) / float(max_score), 2)
        for category in categories
    }


def _pick_category_extremes(
    categories: list[dict[str, Any]],
    *,
    strongest: bool,
    limit: int = 2,
) -> list[str]:
    ordered = sorted(
        categories,
        key=lambda category: (
            -int(category["score"]) if strongest else int(category["score"]),
            str(category["id"]),
        ),
    )
    return [str(category["id"]) for category in ordered[:limit]]


def _build_repo_summary(evaluation: dict[str, Any], evaluation_path: Path) -> dict[str, Any]:
    categories = list(evaluation["categories"])
    max_score = int(evaluation["max_score"])
    contributions = _compute_category_contributions(categories, max_score)
    weighted_total = round(sum(contributions.values()), 2)
    raw_average = round(sum(int(category["score"]) for category in categories) / len(categories), 2)
    scores = [int(category["score"]) for category in categories]
    risk_note_count = sum(len(category.get("risks") or []) for category in categories)
    blocking_issue_count = len(evaluation.get("blocking_issues") or [])
    return {
        "repo": evaluation["repo_name"],
        "repo_path": evaluation["repo_path"],
        "evaluation_path": str(evaluation_path),
        "commit_sha": evaluation.get("commit_sha"),
        "evaluated_at": evaluation["evaluated_at"],
        "profile": evaluation.get("profile"),
        "confidence": evaluation.get("confidence"),
        "category_scores": {str(category["id"]): int(category["score"]) for category in categories},
        "category_contributions": contributions,
        "weighted_total": weighted_total,
        "raw_average": raw_average,
        "score_spread": max(scores) - min(scores),
        "risk_note_count": risk_note_count,
        "blocking_issue_count": blocking_issue_count,
        "strongest_categories": _pick_category_extremes(categories, strongest=True),
        "weakest_categories": _pick_category_extremes(categories, strongest=False),
    }


def _validate_evaluation_compatibility(evaluations: list[tuple[Path, dict[str, Any]]]) -> dict[str, Any]:
    first_path, first = evaluations[0]
    expected_rubric_id = str(first["rubric_id"])
    expected_rubric_version = str(first["rubric_version"])
    expected_max_score = int(first["max_score"])
    expected_category_ids = [str(category["id"]) for category in first["categories"]]
    expected_weights = {str(category["id"]): int(category["weight"]) for category in first["categories"]}
    expected_normalization_basis = str(first["normalization_basis"])

    for path, evaluation in evaluations[1:]:
        if str(evaluation["rubric_id"]) != expected_rubric_id:
            raise PlanError(
                "Mixed rubric_id values are not supported in one aggregation run: "
                f"{first_path}={expected_rubric_id}, {path}={evaluation['rubric_id']}"
            )
        if str(evaluation["rubric_version"]) != expected_rubric_version:
            raise PlanError(
                "Mixed rubric_version values are not supported in one aggregation run: "
                f"{first_path}={expected_rubric_version}, {path}={evaluation['rubric_version']}"
            )
        if int(evaluation["max_score"]) != expected_max_score:
            raise PlanError(
                f"Mixed max_score values are not supported in one aggregation run: {path}"
            )
        if str(evaluation["normalization_basis"]) != expected_normalization_basis:
            raise PlanError(
                f"Mixed normalization_basis values are not supported in one aggregation run: {path}"
            )
        category_ids = [str(category["id"]) for category in evaluation["categories"]]
        if category_ids != expected_category_ids:
            raise PlanError(
                "Evaluation artifacts must contain exactly the same ordered category ids. "
                f"Expected {expected_category_ids}, got {category_ids} in {path}"
            )
        weights = {str(category["id"]): int(category["weight"]) for category in evaluation["categories"]}
        if weights != expected_weights:
            raise PlanError(
                "Evaluation artifacts must contain exactly the same category weights. "
                f"Expected {expected_weights}, got {weights} in {path}"
            )
    return {
        "rubric_id": expected_rubric_id,
        "rubric_version": expected_rubric_version,
        "max_score": expected_max_score,
        "category_ids": expected_category_ids,
        "category_weights": expected_weights,
        "normalization_basis": expected_normalization_basis,
    }


def build_aggregation_summary(manifest_path: Path) -> dict[str, Any]:
    manifest = load_evaluation_manifest(manifest_path)
    evaluation_paths = _resolve_manifest_evaluation_paths(manifest, manifest_path)
    loaded = [(path, _load_evaluation_artifact(path)) for path in evaluation_paths]
    compatibility = _validate_evaluation_compatibility(loaded)
    repos = [_build_repo_summary(evaluation, path) for path, evaluation in loaded]
    repos.sort(
        key=lambda repo: (
            -float(repo["weighted_total"]),
            int(repo["blocking_issue_count"]),
            str(repo["repo"]).lower(),
        )
    )
    return {
        "rubric_id": compatibility["rubric_id"],
        "rubric_version": compatibility["rubric_version"],
        "max_score": compatibility["max_score"],
        "normalization_basis": compatibility["normalization_basis"],
        "category_ids": compatibility["category_ids"],
        "category_weights": compatibility["category_weights"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repos": repos,
    }

def _render_markdown_summary(summary: dict[str, Any]) -> str:
    lines = [
        "# Evaluation Summary",
        "",
        f"- Rubric: `{summary['rubric_id']}`",
        f"- Max score: `{summary['max_score']}`",
        f"- Generated at: `{summary['generated_at']}`",
        "",
    ]
    category_weights = summary.get("category_weights") or {}
    for repo in summary["repos"]:
        lines.extend(
            [
                f"## {repo['repo']}",
                "",
                f"- Total: `{repo['weighted_total']:.2f}` / 100",
                f"- Raw average: `{repo['raw_average']:.2f}` / {summary['max_score']}",
                f"- Spread: `{repo['score_spread']}`",
                f"- Strongest categories: {', '.join(repo['strongest_categories'])}",
                f"- Weakest categories: {', '.join(repo['weakest_categories'])}",
                f"- Blocking issues: `{repo['blocking_issue_count']}`",
                f"- Risk notes: `{repo['risk_note_count']}`",
                f"- Evaluation artifact: `{repo['evaluation_path']}`",
                "",
                "| Category | Score | Weight | Contribution |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for category_id in summary["category_ids"]:
            lines.append(
                f"| {category_id} | {repo['category_scores'][category_id]} | "
                f"{category_weights[category_id]} | {repo['category_contributions'][category_id]:.2f} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _write_csv(summary: dict[str, Any], output_path: Path) -> None:
    category_ids = list(summary["category_ids"])
    fieldnames = [
        "repo",
        "repo_path",
        "evaluation_path",
        "commit_sha",
        "evaluated_at",
        "profile",
        "confidence",
        "weighted_total",
        "raw_average",
        "score_spread",
        "blocking_issue_count",
        "risk_note_count",
        "strongest_categories",
        "weakest_categories",
    ]
    for category_id in category_ids:
        fieldnames.append(f"{category_id}_score")
        fieldnames.append(f"{category_id}_contribution")
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for repo in summary["repos"]:
            row = {
                "repo": repo["repo"],
                "repo_path": repo["repo_path"],
                "evaluation_path": repo["evaluation_path"],
                "commit_sha": repo["commit_sha"] or "",
                "evaluated_at": repo["evaluated_at"],
                "profile": repo["profile"] or "",
                "confidence": repo["confidence"] or "",
                "weighted_total": f"{repo['weighted_total']:.2f}",
                "raw_average": f"{repo['raw_average']:.2f}",
                "score_spread": repo["score_spread"],
                "blocking_issue_count": repo["blocking_issue_count"],
                "risk_note_count": repo["risk_note_count"],
                "strongest_categories": ",".join(repo["strongest_categories"]),
                "weakest_categories": ",".join(repo["weakest_categories"]),
            }
            for category_id in category_ids:
                row[f"{category_id}_score"] = repo["category_scores"][category_id]
                row[f"{category_id}_contribution"] = f"{repo['category_contributions'][category_id]:.2f}"
            writer.writerow(row)


def write_aggregation_outputs(summary: dict[str, Any], output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = output_dir / "summary.json"
    summary_md_path = output_dir / "summary.md"
    table_csv_path = output_dir / "table.csv"
    summary_json_path.write_text(json.dumps(summary, indent=2) + "\n")
    summary_md_path.write_text(_render_markdown_summary(summary))
    _write_csv(summary, table_csv_path)
    return {
        "summary_json": summary_json_path,
        "summary_md": summary_md_path,
        "table_csv": table_csv_path,
    }


def aggregate_evaluations(manifest_path: Path, output_dir: Path | None = None) -> dict[str, Any]:
    manifest_path = manifest_path.expanduser().resolve()
    if not manifest_path.exists():
        raise PlanError(f"Evaluation manifest does not exist: {manifest_path}")
    if not manifest_path.is_file():
        raise PlanError(f"Evaluation manifest is not a file: {manifest_path}")
    summary = build_aggregation_summary(manifest_path)
    resolved_output_dir = resolve_manifest_output_dir(manifest_path, output_dir)
    outputs = write_aggregation_outputs(summary, resolved_output_dir)
    return {
        "output_dir": resolved_output_dir,
        "summary": summary,
        "outputs": outputs,
    }
