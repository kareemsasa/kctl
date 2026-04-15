from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kctl_pkg.git import (
    _summarize_remote_error,
    check_remote_connectivity,
    create_branch,
    create_commit,
    detect_new_changes,
    discard_all_changes,
    ensure_git_repo,
    get_ahead_behind,
    get_current_branch,
    get_full_diff,
    get_git_diff,
    get_git_diff_stat,
    get_git_error_message,
    get_git_status,
    get_last_commit_summary,
    get_local_branches,
    get_project_git_detail,
    get_project_git_summary,
    get_recent_commits,
    get_remotes,
    get_repo_root,
    get_stash_list,
    git_pull,
    git_push,
    git_stash_pop,
    git_stash_save,
    parse_changed_files,
    parse_git_status_entries,
    probe_workspace_dirty,
    read_text_file_with_limit,
    resolve_repo,
    stage_and_commit,
    switch_branch,
    switch_to_branch,
)
from kctl_pkg.types import CommandResult, PlanError
from tests.test_ui_index import init_git_repo, run_checked


class GitTests(unittest.TestCase):
    def test_get_git_error_message_prefers_stderr_then_stdout(self) -> None:
        self.assertEqual(
            get_git_error_message(CommandResult(["git"], "/tmp", 1, "", "fatal")),
            "fatal",
        )
        self.assertEqual(
            get_git_error_message(CommandResult(["git"], "/tmp", 1, "oops", "")),
            "oops",
        )

    def test_resolve_repo_handles_relative_and_absolute_paths(self) -> None:
        plan_path = Path("/tmp/project/plans/001.yaml")
        self.assertEqual(resolve_repo(plan_path, "../repo"), Path("/tmp/project/repo").resolve())
        self.assertEqual(resolve_repo(plan_path, "/tmp/other"), Path("/tmp/other").resolve())

    def test_ensure_git_repo_rejects_missing_and_non_repo_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "missing"
            with self.assertRaisesRegex(PlanError, "does not exist"):
                ensure_git_repo(missing)

            plain_dir = Path(tmpdir) / "plain"
            plain_dir.mkdir()
            with self.assertRaisesRegex(PlanError, "not a git repo"):
                ensure_git_repo(plain_dir)

    def test_repo_queries_work_against_real_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            ensure_git_repo(repo_path)
            self.assertEqual(get_repo_root(repo_path), repo_path.resolve())
            self.assertTrue(get_current_branch(repo_path))
            self.assertEqual(get_git_status(repo_path).exit_code, 0)
            self.assertEqual(get_git_diff_stat(repo_path).exit_code, 0)
            self.assertEqual(get_git_diff(repo_path).exit_code, 0)

    def test_read_text_file_with_limit_truncates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "file.txt"
            path.write_text("abcdef")
            content, truncated = read_text_file_with_limit(path, 3)
            self.assertEqual(content, "abc")
            self.assertTrue(truncated)

    def test_parse_git_status_helpers(self) -> None:
        status_output = " M tracked.py\nR  old.py -> new.py\n?? new.txt\n"
        self.assertEqual(parse_changed_files(status_output), ["tracked.py", "new.py", "new.txt"])
        self.assertEqual(
            parse_git_status_entries(status_output),
            {"tracked.py": " M", "new.py": "R ", "new.txt": "??"},
        )
        self.assertEqual(
            detect_new_changes({"tracked.py": " M"}, {"tracked.py": " M", "new.txt": "??"}),
            ["new.txt"],
        )

    def test_switch_to_branch_and_create_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            switch_to_branch(repo_path, "feature/test")
            self.assertEqual(get_current_branch(repo_path), "feature/test")

            (repo_path / "new.txt").write_text("hello\n")
            sha = create_commit(repo_path, "add file")
            self.assertTrue(sha)

    def test_switch_to_branch_raises_on_failure(self) -> None:
        with patch("kctl_pkg.git.run_command") as mock_run_command:
            mock_run_command.side_effect = [
                CommandResult(["git"], "/tmp", 1, "", "missing"),
                CommandResult(["git"], "/tmp", 1, "", "boom"),
            ]
            with self.assertRaisesRegex(PlanError, "Failed to switch to branch 'feature'"):
                switch_to_branch(Path("/tmp"), "feature")

    def test_create_commit_raises_on_stage_failure(self) -> None:
        with patch(
            "kctl_pkg.git.run_command",
            return_value=CommandResult(["git"], "/tmp", 1, "", "stage failed"),
        ):
            with self.assertRaisesRegex(PlanError, "Failed to stage changes for commit"):
                create_commit(Path("/tmp"), "msg")

    def test_repo_metadata_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            (repo_path / "tracked.txt").write_text("hello\n")
            run_checked(["git", "add", "tracked.txt"], repo_path)
            run_checked(["git", "commit", "-m", "tracked"], repo_path)
            run_checked(["git", "remote", "add", "origin", "https://example.com/repo.git"], repo_path)

            ahead_behind = get_ahead_behind(repo_path)
            last_commit = get_last_commit_summary(repo_path)
            remotes = get_remotes(repo_path)
            commits = get_recent_commits(repo_path, count=2)
            branches = get_local_branches(repo_path)
            stash_list = get_stash_list(repo_path)

            self.assertIn(ahead_behind, (None, (0, 0)))
            self.assertIsNotNone(last_commit)
            self.assertEqual(remotes[0]["name"], "origin")
            self.assertLessEqual(len(commits), 2)
            self.assertTrue(any(branch["current"] == "true" for branch in branches))
            self.assertEqual(stash_list, [])

    def test_summarize_remote_error_and_connectivity_paths(self) -> None:
        self.assertEqual(
            _summarize_remote_error("fatal: repository not found", 128),
            "fatal: repository not found",
        )
        self.assertEqual(_summarize_remote_error("", 9), "exit code 9")

        with patch(
            "kctl_pkg.git.run_command",
            return_value=CommandResult(["git"], "/tmp", 1, "", ""),
        ):
            result = check_remote_connectivity(Path("/tmp"))
            self.assertFalse(result["ok"])
            self.assertIn("not found", str(result["message"]))

        ssh_url_result = CommandResult(["git"], "/tmp", 0, "git@github.com:org/repo.git\n", "")
        with patch("kctl_pkg.git.run_command", return_value=ssh_url_result), patch(
            "subprocess.run",
            return_value=subprocess.CompletedProcess(["git"], 128, "", "Permission denied (publickey)"),
        ):
            result = check_remote_connectivity(Path("/tmp"))
            self.assertFalse(result["ok"])
            self.assertEqual(result["protocol"], "ssh")
            self.assertIn("hint", result)

        https_url_result = CommandResult(["git"], "/tmp", 0, "https://example.com/repo.git\n", "")
        with patch("kctl_pkg.git.run_command", return_value=https_url_result), patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(["git"], timeout=15),
        ):
            result = check_remote_connectivity(Path("/tmp"))
            self.assertEqual(result["message"], "connection timed out")

    def test_project_git_detail_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = Path(tmpdir) / "missing"
            self.assertEqual(get_project_git_detail(missing_path)["error"], "path not found")
            self.assertEqual(get_project_git_summary(missing_path)["error"], "path not found")

            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            run_checked(["git", "remote", "add", "origin", "https://example.com/repo.git"], repo_path)
            (repo_path / "dirty.txt").write_text("dirty\n")
            run_checked(["git", "stash", "push", "-m", "test stash"], repo_path)
            (repo_path / "dirty.txt").write_text("dirty again\n")

            detail = get_project_git_detail(repo_path)
            summary = get_project_git_summary(repo_path)

            self.assertTrue(detail["available"])
            self.assertTrue(summary["available"])
            self.assertEqual(detail["changed_count"], 1)
            self.assertEqual(summary["changed_count"], 1)
            self.assertIn("remotes", detail)
            self.assertIn("recent_commits", detail)
            self.assertIn("branches", detail)
            self.assertIn("stash_list", detail)
            self.assertIn("last_commit", summary)

    def test_probe_workspace_dirty_and_git_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "repo"
            init_git_repo(repo_path)
            self.assertIsNone(probe_workspace_dirty(None))
            self.assertFalse(probe_workspace_dirty(str(repo_path)))

            (repo_path / "README.md").write_text("changed\n")
            self.assertTrue(probe_workspace_dirty(str(repo_path)))
            self.assertIn("diff --git", get_full_diff(repo_path))

    def test_pull_push_stash_and_branch_wrappers(self) -> None:
        with patch("kctl_pkg.git.run_command") as mock_run_command:
            mock_run_command.return_value = CommandResult(["git"], "/tmp", 0, "ok", "warn")
            self.assertEqual(git_pull(Path("/tmp")), "ok")
            self.assertEqual(git_push(Path("/tmp"), set_upstream=True), "okwarn")
            self.assertEqual(git_stash_save(Path("/tmp"), "msg"), "ok")
            self.assertEqual(git_stash_pop(Path("/tmp")), "ok")

        with patch(
            "kctl_pkg.git.run_command",
            return_value=CommandResult(["git"], "/tmp", 1, "", "boom"),
        ):
            with self.assertRaises(PlanError):
                git_pull(Path("/tmp"))
            with self.assertRaises(PlanError):
                git_push(Path("/tmp"))
            with self.assertRaises(PlanError):
                git_stash_save(Path("/tmp"))
            with self.assertRaises(PlanError):
                git_stash_pop(Path("/tmp"))
            with self.assertRaises(PlanError):
                create_branch(Path("/tmp"), "feature")
            with self.assertRaises(PlanError):
                switch_branch(Path("/tmp"), "feature")

    def test_discard_all_changes_and_stage_and_commit(self) -> None:
        with patch("kctl_pkg.git.run_command") as mock_run_command:
            mock_run_command.side_effect = [
                CommandResult(["git"], "/tmp", 0, "", ""),
                CommandResult(["git"], "/tmp", 0, "", ""),
            ]
            self.assertEqual(discard_all_changes(Path("/tmp")), "All changes discarded.")

        with patch("kctl_pkg.git.run_command") as mock_run_command:
            mock_run_command.side_effect = [
                CommandResult(["git"], "/tmp", 0, "", ""),
                CommandResult(["git"], "/tmp", 0, "", ""),
                CommandResult(["git"], "/tmp", 0, "abc123\n", ""),
            ]
            self.assertEqual(stage_and_commit(Path("/tmp"), "msg", paths=["a.py"]), "abc123")

        with patch("kctl_pkg.git.run_command") as mock_run_command:
            mock_run_command.side_effect = [
                CommandResult(["git"], "/tmp", 0, "", ""),
                CommandResult(["git"], "/tmp", 0, "", ""),
                CommandResult(["git"], "/tmp", 1, "", ""),
            ]
            self.assertEqual(stage_and_commit(Path("/tmp"), "msg"), "unknown")


if __name__ == "__main__":
    unittest.main()
