from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from kctl_pkg.evaluation_aggregate import aggregate_evaluations, build_aggregation_summary
from kctl_pkg.types import PlanError


def write_evaluation(
    path: Path,
    *,
    repo_name: str,
    repo_path: str,
    scores: dict[str, int],
    blocking_issues: list[str] | None = None,
    rubric_id: str = "repo-rubric-v1",
    rubric_version: str = "repo_eval_v1",
) -> None:
    categories = [
        ("build_test_health", "Build and test health", 25),
        ("documentation_quality", "Documentation quality", 15),
        ("security_secret_hygiene", "Security and secret hygiene", 15),
        ("operational_readiness", "Operational readiness", 15),
        ("architecture_maintainability", "Architecture and maintainability", 20),
        ("release_readiness", "Release readiness", 10),
    ]
    payload = {
        "repository": {"name": repo_name, "path": repo_path},
        "rubric_id": rubric_id,
        "rubric_version": rubric_version,
        "evaluated_at": "2026-04-17T22:00:00+00:00",
        "repo_name": repo_name,
        "repo_path": repo_path,
        "commit_sha": "deadbeef",
        "max_score": 5,
        "confidence": "medium",
        "profile": "backend_service",
        "normalization_basis": "sum(weight * score / max_score)",
        "summary": f"{repo_name} summary",
        "categories": [
            {
                "id": category_id,
                "name": name,
                "weight": weight,
                "score": scores[category_id],
                "max_score": 5,
                "summary": f"{category_id} summary",
                "evidence": [f"{repo_name}:{category_id}:evidence"],
                "risks": [f"{repo_name}:{category_id}:risk"] if scores[category_id] < 4 else [],
            }
            for category_id, name, weight in categories
        ],
        "overall_findings": [f"{repo_name} finding"],
        "blocking_issues": blocking_issues or [],
        "recommended_next_actions": [f"{repo_name} next"],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


class EvaluationAggregateTests(unittest.TestCase):
    def test_aggregate_evaluations_writes_expected_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            eval_a = tmp / "repo-a.json"
            eval_b = tmp / "repo-b.json"
            write_evaluation(
                eval_a,
                repo_name="repo-a",
                repo_path="/repos/repo-a",
                scores={
                    "build_test_health": 5,
                    "documentation_quality": 4,
                    "security_secret_hygiene": 4,
                    "operational_readiness": 4,
                    "architecture_maintainability": 4,
                    "release_readiness": 3,
                },
            )
            write_evaluation(
                eval_b,
                repo_name="repo-b",
                repo_path="/repos/repo-b",
                scores={
                    "build_test_health": 4,
                    "documentation_quality": 4,
                    "security_secret_hygiene": 3,
                    "operational_readiness": 4,
                    "architecture_maintainability": 4,
                    "release_readiness": 3,
                },
                blocking_issues=["needs release gate"],
            )
            manifest = tmp / "evaluations.yaml"
            manifest.write_text(
                "evaluations:\n"
                f"  - path: {eval_a.name}\n"
                f"  - path: {eval_b.name}\n"
                "output_dir: out\n"
            )

            result = aggregate_evaluations(manifest)

            self.assertEqual(result["summary"]["rubric_id"], "repo-rubric-v1")
            self.assertEqual([repo["repo"] for repo in result["summary"]["repos"]], ["repo-a", "repo-b"])
            self.assertEqual(result["summary"]["repos"][0]["weighted_total"], 83.0)
            self.assertEqual(result["summary"]["repos"][1]["weighted_total"], 75.0)
            self.assertEqual(
                result["summary"]["repos"][0]["category_contributions"]["build_test_health"],
                25.0,
            )
            self.assertTrue((tmp / "out" / "summary.json").exists())
            self.assertTrue((tmp / "out" / "summary.md").exists())
            self.assertTrue((tmp / "out" / "table.csv").exists())

            summary_json = json.loads((tmp / "out" / "summary.json").read_text())
            self.assertEqual(summary_json["repos"][0]["strongest_categories"], ["build_test_health", "architecture_maintainability"])

            summary_md = (tmp / "out" / "summary.md").read_text()
            self.assertIn("## repo-a", summary_md)
            self.assertIn("| build_test_health | 5 | 25 | 25.00 |", summary_md)

            with (tmp / "out" / "table.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["repo"], "repo-a")
            self.assertEqual(rows[0]["weighted_total"], "83.00")
            self.assertEqual(rows[0]["build_test_health_contribution"], "25.00")

    def test_build_aggregation_summary_rejects_mixed_rubrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            eval_a = tmp / "repo-a.json"
            eval_b = tmp / "repo-b.json"
            write_evaluation(
                eval_a,
                repo_name="repo-a",
                repo_path="/repos/repo-a",
                scores={
                    "build_test_health": 5,
                    "documentation_quality": 4,
                    "security_secret_hygiene": 4,
                    "operational_readiness": 4,
                    "architecture_maintainability": 4,
                    "release_readiness": 3,
                },
            )
            write_evaluation(
                eval_b,
                repo_name="repo-b",
                repo_path="/repos/repo-b",
                scores={
                    "build_test_health": 5,
                    "documentation_quality": 4,
                    "security_secret_hygiene": 4,
                    "operational_readiness": 4,
                    "architecture_maintainability": 4,
                    "release_readiness": 3,
                },
                rubric_id="repo-rubric-v2",
            )
            manifest = tmp / "evaluations.yaml"
            manifest.write_text(
                "evaluations:\n"
                f"  - path: {eval_a}\n"
                f"  - path: {eval_b}\n"
            )

            with self.assertRaisesRegex(PlanError, "Mixed rubric_id values"):
                build_aggregation_summary(manifest)

    def test_build_aggregation_summary_rejects_category_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            eval_a = tmp / "repo-a.json"
            eval_b = tmp / "repo-b.json"
            write_evaluation(
                eval_a,
                repo_name="repo-a",
                repo_path="/repos/repo-a",
                scores={
                    "build_test_health": 5,
                    "documentation_quality": 4,
                    "security_secret_hygiene": 4,
                    "operational_readiness": 4,
                    "architecture_maintainability": 4,
                    "release_readiness": 3,
                },
            )
            payload = json.loads(eval_a.read_text())
            payload["repo_name"] = "repo-b"
            payload["repository"]["name"] = "repo-b"
            payload["repo_path"] = "/repos/repo-b"
            payload["repository"]["path"] = "/repos/repo-b"
            payload["categories"][-1]["id"] = "unexpected_release_readiness"
            eval_b.write_text(json.dumps(payload, indent=2) + "\n")
            manifest = tmp / "evaluations.yaml"
            manifest.write_text(
                "evaluations:\n"
                f"  - path: {eval_a}\n"
                f"  - path: {eval_b}\n"
            )

            with self.assertRaisesRegex(PlanError, "same ordered category ids"):
                build_aggregation_summary(manifest)


if __name__ == "__main__":
    unittest.main()
