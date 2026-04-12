from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kctl_pkg.ui_index import default_db_path, index_repository_state
from kctl_pkg.ui_read import (
    derive_workspace_lifecycle,
    get_plan_execution,
    get_repository_overview,
    get_repository,
    get_run,
    get_workspace,
    list_attention_items,
    list_agent_assignments,
    list_agent_profiles,
    list_plan_executions,
    list_repository_plan_executions,
    list_repositories,
    list_runs,
    list_step_executions,
    list_workspaces,
)
from kctl_pkg.ui_store import UIStateStore
from tests.test_ui_index import init_git_repo, write_sample_plan_run


class UIReadTests(unittest.TestCase):
    def test_read_api_returns_typed_run_and_plan_models(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            repository = get_repository(repo_path)
            self.assertEqual(Path(repository.root_path).resolve(), repo_path.resolve())

            repositories = list_repositories(repo_path)
            self.assertEqual(len(repositories), 1)
            self.assertEqual(repositories[0].id, repository.id)

            runs = list_runs(repo_path)
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].id, run_id)

            run_detail = get_run(repo_path, run_id)
            self.assertEqual(run_detail.plan_execution_count, 1)
            self.assertEqual(run_detail.status, "passed")

            plan_cards = list_plan_executions(repo_path, run_id)
            self.assertEqual(len(plan_cards), 1)
            self.assertEqual(plan_cards[0].plan_slug, "001-add-ui")
            self.assertEqual(plan_cards[0].current_step_key, "verify")

            plan_card = get_plan_execution(plan_cards[0].id, repo_path)
            self.assertEqual(plan_card.id, plan_cards[0].id)
            self.assertEqual(plan_card.verify_status, "passed")

    def test_step_timeline_is_ordered_and_workspace_lookup_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            plan_execution_id = list_plan_executions(repo_path, run_id)[0].id
            timeline = list_step_executions(plan_execution_id, repo_path)
            self.assertEqual([item.sequence_index for item in timeline], [1, 2])
            self.assertEqual([item.step_key for item in timeline], ["inspect", "verify"])
            self.assertEqual(timeline[1].verify_status, "passed")
            self.assertEqual(timeline[0].changed_files, ["src/app.ts"])

            workspace = get_workspace(plan_execution_id, repo_path)
            self.assertIsNotNone(workspace)
            assert workspace is not None
            self.assertIn(".kctl/worktrees/", workspace.path)
            self.assertTrue(workspace.branch_name)
            workspaces = list_workspaces(repo_path)
            self.assertEqual(len(workspaces), 1)
            self.assertEqual(workspaces[0].lifecycle, "released")

    def test_agent_queries_work_when_tables_are_empty_or_populated(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            self.assertEqual(list_agent_profiles(repo_path), [])
            self.assertEqual(list_agent_assignments(repo_path), [])

            db_path = default_db_path(repo_path)
            store = UIStateStore(db_path)
            try:
                store.initialize()
                store.upsert(
                    "agent_profiles",
                    {
                        "id": "agent-1",
                        "display_name": "Annie",
                        "avatar_uri": "/avatars/annie.png",
                        "theme_key": "desk-sunrise",
                        "preset_key": "careful",
                        "status": "enabled",
                        "created_at": "2026-03-25T12:00:00+00:00",
                        "updated_at": "2026-03-25T12:00:00+00:00",
                    },
                    ["id"],
                )
                plan_execution_id = list_plan_executions(repo_path, run_id)[0].id
                store.upsert(
                    "agent_assignments",
                    {
                        "id": "assign-1",
                        "agent_id": "agent-1",
                        "plan_execution_id": plan_execution_id,
                        "assigned_at": "2026-03-25T12:00:00+00:00",
                        "released_at": None,
                        "status": "active",
                    },
                    ["id"],
                )
                store.commit()
            finally:
                store.close()

            agents = list_agent_profiles(repo_path)
            self.assertEqual(len(agents), 1)
            self.assertEqual(agents[0].display_name, "Annie")

            assignments = list_agent_assignments(repo_path)
            self.assertEqual(len(assignments), 1)
            self.assertEqual(assignments[0].agent_display_name, "Annie")

            active_assignments = list_agent_assignments(repo_path, plan_execution_id=plan_execution_id, active_only=True)
            self.assertEqual(len(active_assignments), 1)
            self.assertEqual(active_assignments[0].status, "active")

    def test_repository_overview_and_attention_items_reflect_plan_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_id, _ = write_sample_plan_run(repo_path)
            index_repository_state(repo_path)

            db_path = default_db_path(repo_path)
            store = UIStateStore(db_path)
            try:
                store.initialize()
                store.upsert(
                    "runs",
                    {
                        "id": "run-running",
                        "repository_id": str(repo_path.resolve()),
                        "launch_source": "multi_run",
                        "plans_dir": str((repo_path / "plans").resolve()),
                        "concurrency": 1,
                        "status": "running",
                        "started_at": "2026-03-25T13:00:00+00:00",
                        "ended_at": None,
                        "run_root_path": str((repo_path / ".kctl" / "runs" / "run-running").resolve()),
                    },
                    ["id"],
                )
                store.upsert(
                    "plan_definitions",
                    {
                        "id": f"{repo_path.resolve()}:running-plan",
                        "repository_id": str(repo_path.resolve()),
                        "file_path": str((repo_path / "plans" / "running-plan.yaml").resolve()),
                        "slug": "running-plan",
                        "title": None,
                        "objective": "Running plan",
                        "content_hash": None,
                        "phase_name": None,
                        "group_name": None,
                        "created_at": "2026-03-25T13:00:00+00:00",
                        "updated_at": "2026-03-25T13:00:00+00:00",
                    },
                    ["id"],
                )
                existing_plan_execution_id = list_plan_executions(repo_path, run_id)[0].id
                store.upsert(
                    "plan_executions",
                    {
                        "id": existing_plan_execution_id,
                        "run_id": run_id,
                        "plan_definition_id": list_plan_executions(repo_path, run_id)[0].plan_definition_id,
                        "status": "blocked",
                        "current_step_key": "review",
                        "verify_status": "failed",
                        "started_at": "2026-03-25T12:00:00+00:00",
                        "ended_at": "2026-03-25T12:01:00+00:00",
                        "worktree_path": str((repo_path / ".kctl" / "worktrees" / run_id / "001-add-ui").resolve()),
                        "branch_name": "kctl/test",
                        "log_path": str((repo_path / ".kctl" / "runs" / run_id / "001-add-ui" / "run.json").resolve()),
                        "changed_files_count": 1,
                        "failure_reason": "review_blocked",
                    },
                    ["id"],
                )
                store.upsert(
                    "workspaces",
                    {
                        "id": f"{run_id}:001-add-ui",
                        "repository_id": str(repo_path.resolve()),
                        "plan_execution_id": existing_plan_execution_id,
                        "path": str((repo_path / ".kctl" / "worktrees" / run_id / "001-add-ui").resolve()),
                        "branch_name": "kctl/test",
                        "base_ref": "main",
                        "status": "active",
                        "created_at": "2026-03-25T12:00:00+00:00",
                        "released_at": None,
                    },
                    ["id"],
                )
                store.upsert(
                    "plan_executions",
                    {
                        "id": "run-running:running-plan",
                        "run_id": "run-running",
                        "plan_definition_id": f"{repo_path.resolve()}:running-plan",
                        "status": "running",
                        "current_step_key": "implement",
                        "verify_status": "not_run",
                        "started_at": "2026-03-25T13:00:00+00:00",
                        "ended_at": None,
                        "worktree_path": str((repo_path / ".kctl" / "worktrees" / "run-running" / "running-plan").resolve()),
                        "branch_name": "kctl/run-running/running-plan",
                        "log_path": str((repo_path / ".kctl" / "runs" / "run-running" / "running-plan" / "run.json").resolve()),
                        "changed_files_count": 0,
                        "failure_reason": None,
                    },
                    ["id"],
                )
                store.upsert(
                    "workspaces",
                    {
                        "id": "workspace-running",
                        "repository_id": str(repo_path.resolve()),
                        "plan_execution_id": "run-running:running-plan",
                        "path": str((repo_path / ".kctl" / "worktrees" / "run-running" / "running-plan").resolve()),
                        "branch_name": "kctl/run-running/running-plan",
                        "base_ref": "main",
                        "status": "active",
                        "created_at": "2026-03-25T13:00:00+00:00",
                        "released_at": None,
                    },
                    ["id"],
                )
                store.commit()
            finally:
                store.close()

            all_plans = list_repository_plan_executions(repo_path)
            self.assertEqual(len(all_plans), 2)

            overview = get_repository_overview(repo_path)
            self.assertEqual(overview.run_count, 2)
            self.assertEqual(overview.active_run_count, 1)
            self.assertEqual(overview.blocked_plan_count, 1)
            self.assertEqual(overview.running_plan_count, 1)
            self.assertEqual(overview.recent_failure_count, 1)
            self.assertEqual(overview.stale_workspace_count, 1)

            attention_items = list_attention_items(repo_path)
            kinds = {item.kind for item in attention_items}
            self.assertIn("failed_verify", kinds)
            self.assertIn("active_workspace", kinds)

            workspaces = list_workspaces(repo_path)
            lifecycle_by_plan = {workspace.plan_slug: workspace.lifecycle for workspace in workspaces}
            self.assertEqual(lifecycle_by_plan["001-add-ui"], "stale")
            self.assertEqual(lifecycle_by_plan["running-plan"], "active")

    def test_workspace_lifecycle_derivation_is_conservative(self) -> None:
        self.assertEqual(derive_workspace_lifecycle("active", "running", None), "active")
        self.assertEqual(derive_workspace_lifecycle("ready", "passed", "2026-03-25T12:00:00+00:00"), "released")
        self.assertEqual(derive_workspace_lifecycle("active", "blocked", None), "stale")


if __name__ == "__main__":
    unittest.main()
