from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS repositories (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        root_path TEXT NOT NULL UNIQUE,
        default_branch TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS plan_definitions (
        id TEXT PRIMARY KEY,
        repository_id TEXT NOT NULL,
        file_path TEXT NOT NULL,
        slug TEXT NOT NULL,
        title TEXT,
        objective TEXT NOT NULL,
        content_hash TEXT,
        phase_name TEXT,
        group_name TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(repository_id, file_path),
        FOREIGN KEY(repository_id) REFERENCES repositories(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        repository_id TEXT NOT NULL,
        launch_source TEXT NOT NULL,
        plans_dir TEXT NOT NULL,
        concurrency INTEGER NOT NULL,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        run_root_path TEXT NOT NULL,
        FOREIGN KEY(repository_id) REFERENCES repositories(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS plan_executions (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        plan_definition_id TEXT NOT NULL,
        status TEXT NOT NULL,
        current_step_key TEXT,
        verify_status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        worktree_path TEXT,
        branch_name TEXT,
        log_path TEXT,
        changed_files_count INTEGER NOT NULL,
        failure_reason TEXT,
        FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
        FOREIGN KEY(plan_definition_id) REFERENCES plan_definitions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS step_executions (
        id TEXT PRIMARY KEY,
        plan_execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_name TEXT,
        kind TEXT NOT NULL,
        sequence_index INTEGER NOT NULL,
        status TEXT NOT NULL,
        verify_status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        duration_ms INTEGER,
        output_path TEXT,
        artifact_path TEXT,
        verify_exit_code INTEGER,
        changed_files_count INTEGER NOT NULL,
        metadata_json TEXT NOT NULL,
        changed_files_json TEXT NOT NULL,
        FOREIGN KEY(plan_execution_id) REFERENCES plan_executions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workspaces (
        id TEXT PRIMARY KEY,
        repository_id TEXT NOT NULL,
        plan_execution_id TEXT NOT NULL UNIQUE,
        path TEXT NOT NULL,
        branch_name TEXT,
        base_ref TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        released_at TEXT,
        FOREIGN KEY(repository_id) REFERENCES repositories(id) ON DELETE CASCADE,
        FOREIGN KEY(plan_execution_id) REFERENCES plan_executions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_profiles (
        id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        avatar_uri TEXT,
        theme_key TEXT,
        preset_key TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_assignments (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        plan_execution_id TEXT NOT NULL,
        assigned_at TEXT NOT NULL,
        released_at TEXT,
        status TEXT NOT NULL,
        FOREIGN KEY(agent_id) REFERENCES agent_profiles(id) ON DELETE CASCADE,
        FOREIGN KEY(plan_execution_id) REFERENCES plan_executions(id) ON DELETE CASCADE
    )
    """,
)


class UIStateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(db_path))
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        for statement in SCHEMA_STATEMENTS:
            self.connection.execute(statement)
        self.connection.commit()

    def upsert(self, table_name: str, values: dict[str, Any], conflict_columns: list[str]) -> None:
        columns = list(values.keys())
        placeholders = ", ".join("?" for _ in columns)
        assignments = ", ".join(f"{column}=excluded.{column}" for column in columns if column not in conflict_columns)
        sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {assignments}"
        )
        self.connection.execute(sql, [values[column] for column in columns])

    def clear_execution_data_for_repository(self, repository_id: str) -> None:
        self.connection.execute(
            "DELETE FROM agent_assignments WHERE plan_execution_id IN "
            "(SELECT id FROM plan_executions WHERE run_id IN (SELECT id FROM runs WHERE repository_id = ?))",
            (repository_id,),
        )
        self.connection.execute(
            "DELETE FROM workspaces WHERE repository_id = ?",
            (repository_id,),
        )
        self.connection.execute(
            "DELETE FROM step_executions WHERE plan_execution_id IN "
            "(SELECT id FROM plan_executions WHERE run_id IN (SELECT id FROM runs WHERE repository_id = ?))",
            (repository_id,),
        )
        self.connection.execute(
            "DELETE FROM plan_executions WHERE run_id IN (SELECT id FROM runs WHERE repository_id = ?)",
            (repository_id,),
        )
        self.connection.execute("DELETE FROM runs WHERE repository_id = ?", (repository_id,))
        self.connection.execute("DELETE FROM plan_definitions WHERE repository_id = ?", (repository_id,))
        self.connection.commit()

    def list_runs(self, repository_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                runs.id,
                runs.status,
                runs.launch_source,
                runs.concurrency,
                runs.started_at,
                runs.ended_at,
                runs.run_root_path,
                COUNT(plan_executions.id) AS plan_execution_count
            FROM runs
            LEFT JOIN plan_executions ON plan_executions.run_id = runs.id
            WHERE runs.repository_id = ?
            GROUP BY runs.id
            ORDER BY runs.started_at DESC
            """,
            (repository_id,),
        )
        return list(cursor.fetchall())

    def list_repositories(self) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM repositories
            ORDER BY name ASC
            """
        )
        return list(cursor.fetchall())

    def get_repository_by_id(self, repository_id: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            "SELECT * FROM repositories WHERE id = ?",
            (repository_id,),
        )
        return cursor.fetchone()

    def get_repository_by_root_path(self, root_path: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            "SELECT * FROM repositories WHERE root_path = ?",
            (root_path,),
        )
        return cursor.fetchone()

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            "SELECT * FROM runs WHERE id = ?",
            (run_id,),
        )
        return cursor.fetchone()

    def get_run_with_counts(self, run_id: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            """
            SELECT
                runs.*,
                COUNT(plan_executions.id) AS plan_execution_count,
                SUM(CASE WHEN plan_executions.status = 'passed' THEN 1 ELSE 0 END) AS passed_count,
                SUM(CASE WHEN plan_executions.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                SUM(CASE WHEN plan_executions.status = 'running' THEN 1 ELSE 0 END) AS running_count,
                SUM(CASE WHEN plan_executions.status = 'blocked' THEN 1 ELSE 0 END) AS blocked_count
            FROM runs
            LEFT JOIN plan_executions ON plan_executions.run_id = runs.id
            WHERE runs.id = ?
            GROUP BY runs.id
            """,
            (run_id,),
        )
        return cursor.fetchone()

    def list_plan_executions_for_run(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                plan_executions.*,
                plan_definitions.slug,
                plan_definitions.title,
                plan_definitions.file_path,
                plan_definitions.objective,
                plan_definitions.phase_name,
                plan_definitions.group_name
            FROM plan_executions
            JOIN plan_definitions ON plan_definitions.id = plan_executions.plan_definition_id
            WHERE plan_executions.run_id = ?
            ORDER BY plan_definitions.slug
            """,
            (run_id,),
        )
        return list(cursor.fetchall())

    def get_plan_execution(self, plan_execution_id: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            """
            SELECT
                plan_executions.*,
                plan_definitions.slug,
                plan_definitions.title,
                plan_definitions.file_path,
                plan_definitions.objective,
                plan_definitions.group_name,
                plan_definitions.phase_name,
                runs.repository_id,
                runs.id AS run_id_value
            FROM plan_executions
            JOIN plan_definitions ON plan_definitions.id = plan_executions.plan_definition_id
            JOIN runs ON runs.id = plan_executions.run_id
            WHERE plan_executions.id = ?
            """,
            (plan_execution_id,),
        )
        return cursor.fetchone()

    def list_step_executions_for_plan_execution(self, plan_execution_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM step_executions
            WHERE plan_execution_id = ?
            ORDER BY sequence_index ASC
            """,
            (plan_execution_id,),
        )
        return list(cursor.fetchall())

    def get_workspace_for_plan_execution(self, plan_execution_id: str) -> sqlite3.Row | None:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM workspaces
            WHERE plan_execution_id = ?
            """,
            (plan_execution_id,),
        )
        return cursor.fetchone()

    def list_agent_profiles(self) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM agent_profiles
            ORDER BY display_name ASC
            """
        )
        return list(cursor.fetchall())

    def list_agent_assignments(
        self,
        plan_execution_id: str | None = None,
        active_only: bool = False,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if plan_execution_id is not None:
            clauses.append("agent_assignments.plan_execution_id = ?")
            params.append(plan_execution_id)
        if active_only:
            clauses.append("agent_assignments.released_at IS NULL")
        where_clause = ""
        if clauses:
            where_clause = "WHERE " + " AND ".join(clauses)
        cursor = self.connection.execute(
            f"""
            SELECT
                agent_assignments.*,
                agent_profiles.display_name,
                agent_profiles.avatar_uri,
                agent_profiles.theme_key,
                agent_profiles.preset_key,
                agent_profiles.status AS agent_status
            FROM agent_assignments
            JOIN agent_profiles ON agent_profiles.id = agent_assignments.agent_id
            {where_clause}
            ORDER BY agent_assignments.assigned_at ASC
            """,
            params,
        )
        return list(cursor.fetchall())

    def commit(self) -> None:
        self.connection.commit()
