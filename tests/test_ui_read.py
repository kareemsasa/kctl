from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kctl_pkg.ui_index import default_db_path, index_repository_state
from kctl_pkg.ui_read import (
    get_plan_execution,
    get_repository,
    get_run,
    get_workspace,
    list_agent_assignments,
    list_agent_profiles,
    list_plan_executions,
    list_repositories,
    list_runs,
    list_step_executions,
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


if __name__ == "__main__":
    unittest.main()
