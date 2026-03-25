# kctl UI Data Model v1

## Intent

This document defines the minimum durable data model for a first `kctl` UI that can:

- launch multi-plan runs
- monitor active plan execution
- inspect step timelines
- locate isolated worktrees
- show basic agent assignment state

This is intentionally narrower than a full product model. It is designed to fit the current `kctl` architecture without rewriting the runner.

## Current Ground Truth

Today, `kctl` already persists the key execution facts needed by a UI:

- plan YAML files define repository, objective, defaults, and ordered steps
- single-plan execution writes per-run artifacts and `run.json`
- multi-plan execution writes aggregate state under `.kctl/runs/<run-id>/run.json`
- each plan in a multi-plan run gets its own run subdirectory
- isolated worktrees are created under `.kctl/worktrees/<run-id>/<plan-id>/`
- step artifacts are written to disk and passed forward explicitly rather than relying on hidden agent memory

That means v1 does not need to redesign the runner. It needs a queryable local model over filesystem-backed execution state.

## v1 Modeling Rules

1. Filesystem artifacts stay canonical for logs and detailed step outputs.
2. A local queryable model is added for UI lookup and joins.
3. Only entities required for run history, plan status, step timelines, worktree lookup, and basic agent assignment are persisted.
4. Everything else is either:
   - derived at query time
   - stored as JSON/text on an existing entity
   - deferred to a later phase

## Persistence Recommendation

Use:

- filesystem for raw execution artifacts and worktrees
- SQLite for the UI/query index

Why:

- the runner already writes durable filesystem state
- the UI needs fast filtering, joining, sorting, and history lookup
- SQLite adds structure without forcing a backend service
- ingestion can start from existing `.kctl/runs` and `.kctl/worktrees` paths

Recommended boundary:

- filesystem is the source of raw logs, markdown output, structured artifacts, and worktree contents
- SQLite is the source of queryable UI entities and relationships

## Persisted Entities

### 1. Repository

Purpose: stable identity for a target code repository.

Fields:

- `id`
- `name`
- `root_path`
- `default_branch`
- `created_at`
- `updated_at`

Notes:

- do not persist `plan_dirs` yet
- a repo can discover plan directories on demand, or that can be added later if it becomes real product state

### 2. PlanDefinition

Purpose: durable metadata for a plan file independent of a specific run.

Fields:

- `id`
- `repository_id`
- `file_path`
- `slug`
- `title`
- `objective`
- `content_hash`
- `phase_name` nullable
- `group_name` nullable
- `created_at`
- `updated_at`

Notes:

- `title` can default to filename for v1
- do not persist dependency or conflict metadata yet unless execution starts using it

### 3. Run

Purpose: one multi-plan run launched from the UI or CLI.

Fields:

- `id`
- `repository_id`
- `launch_source`
- `plans_dir`
- `concurrency`
- `status`
- `started_at`
- `ended_at` nullable
- `run_root_path`

Notes:

- no `created_by` or `notes` in v1
- `status` should support at least: `pending`, `running`, `passed`, `failed`, `blocked`

### 4. PlanExecution

Purpose: one execution instance of one plan inside one run.

Fields:

- `id`
- `run_id`
- `plan_definition_id`
- `status`
- `current_step_key`
- `verify_status`
- `started_at`
- `ended_at` nullable
- `worktree_path`
- `branch_name`
- `log_path`
- `changed_files_count`
- `failure_reason` nullable

Notes:

- this is the main card/row entity for the UI
- `verify_status` can be `not_run`, `passed`, `failed`, or `partial`

### 5. StepExecution

Purpose: one executed step within a plan execution.

Fields:

- `id`
- `plan_execution_id`
- `step_key`
- `step_name`
- `kind`
- `sequence_index`
- `status`
- `verify_status`
- `started_at`
- `ended_at` nullable
- `duration_ms` nullable
- `output_path`
- `artifact_path` nullable
- `verify_exit_code` nullable
- `changed_files_count`
- `metadata_json`
- `changed_files_json`

Notes:

- `output_path` points to the raw markdown or equivalent main step output
- `artifact_path` is a single primary structured artifact path when present
- `metadata_json` is the escape hatch for details already present in runner JSON:
  - verify environment
  - structured artifact paths
  - diff-stat summaries
  - parse errors
  - before/after git status excerpts

### 6. Workspace

Purpose: isolated worktree or equivalent workspace for a plan execution.

Fields:

- `id`
- `repository_id`
- `plan_execution_id`
- `path`
- `branch_name`
- `base_ref`
- `status`
- `created_at`
- `released_at` nullable

Notes:

- `base_ref` should usually be the branch or ref the workspace was created from
- no separate workspace snapshot entity in v1
- cleanliness can be derived live from the workspace path when needed

### 7. AgentProfile

Purpose: user-visible worker/persona for the UI.

Fields:

- `id`
- `display_name`
- `avatar_uri`
- `theme_key`
- `preset_key`
- `status`
- `created_at`
- `updated_at`

Notes:

- `preset_key` is only a lightweight string label in v1
- do not persist model/provider defaults yet unless agent launch behavior actually depends on them

### 8. AgentAssignment

Purpose: assignment of an agent to a plan execution.

Fields:

- `id`
- `agent_id`
- `plan_execution_id`
- `assigned_at`
- `released_at` nullable
- `status`

Notes:

- this is enough to support “agent at desk working on plan X”
- current workload can be derived from active assignments

## Relationships

- `Repository` 1 -> many `PlanDefinition`
- `Repository` 1 -> many `Run`
- `Run` 1 -> many `PlanExecution`
- `PlanDefinition` 1 -> many `PlanExecution`
- `PlanExecution` 1 -> many `StepExecution`
- `PlanExecution` 1 -> 1 `Workspace`
- `AgentProfile` 1 -> many `AgentAssignment`
- `PlanExecution` 1 -> many `AgentAssignment` historically, but typically one active assignment at a time

## What Stays Out Of The Schema In v1

Do not persist these as first-class entities yet:

- artifact catalog tables
- verification result tables
- workspace snapshot tables
- behavior preset tables
- run event tables
- UI layout/state tables
- normalized step-definition tables

Reason:

- artifacts and verification details already exist in runner JSON and on disk
- workspace cleanliness can be derived when needed
- behavior presets and layouts are product-layer concerns, not execution-layer requirements
- normalized step definitions are not necessary to build timelines or status views

## Derived Data

These should be computed from the eight canonical entities plus filesystem artifacts:

- active runs
- current agent workload
- board columns by status
- verify badge state
- changed file summaries
- duration strings
- repo run history summaries
- “agent is busy/idle” state
- desk occupancy

## Filesystem Mapping

Recommended mapping from current runner output into the v1 model:

- `.kctl/runs/<run-id>/run.json`
  - source for `Run`
- `.kctl/runs/<run-id>/<plan-id>/run.json`
  - source for `PlanExecution` and `StepExecution`
- `.kctl/runs/<run-id>/<plan-id>/step-XX-raw.md`
  - maps to `StepExecution.output_path`
- `.kctl/runs/<run-id>/<plan-id>/step-XX-*.json`
  - primary structured artifact can map to `StepExecution.artifact_path`
  - remaining detail can go into `metadata_json`
- `.kctl/worktrees/<run-id>/<plan-id>/`
  - source for `Workspace.path`

## API Shape For The Future UI

The UI should not query raw files directly in most cases. Add a thin local service layer that exposes:

- `listRepositories()`
- `listPlans(repositoryId)`
- `listRuns(repositoryId, filters)`
- `getRun(runId)`
- `listPlanExecutions(runId)`
- `getPlanExecution(planExecutionId)`
- `listStepExecutions(planExecutionId)`
- `getWorkspace(planExecutionId)`
- `listAgents()`
- `listAssignments(activeOnly)`
- `assignAgent(planExecutionId, agentId)`

The service can join SQLite rows and only read artifact files when the user opens detailed views.

## Why This Supports The First UI

List/table view:

- rows come from `PlanExecution`
- joins to `PlanDefinition`, `Run`, and `AgentAssignment`

Board view:

- columns come from `PlanExecution.status`
- detail badges come from `current_step_key` and `verify_status`

Desk/agent view:

- characters come from `AgentProfile`
- current work comes from active `AgentAssignment`
- desk state comes from `PlanExecution.status` and `current_step_key`

Step timeline view:

- ordered directly from `StepExecution.sequence_index`

Workspace inspection:

- driven directly from `Workspace.path`

## Migration Plan

Phase 1:

- build a one-way indexer from existing `.kctl/runs` and `.kctl/worktrees` into SQLite
- do not change runner output

Phase 2:

- have the UI read from SQLite and dereference artifact paths on demand

Phase 3:

- optionally teach the runner to update SQLite directly in addition to writing filesystem artifacts

## Explicit Non-Goals

- distributed orchestration
- remote sync
- backend service requirements
- full dependency/conflict planning
- event streaming infrastructure
- UI layout persistence
- fine-grained artifact normalization
- provider/model execution policy on agents

## Recommendation

Build v1 on exactly these eight persisted entities:

- `Repository`
- `PlanDefinition`
- `Run`
- `PlanExecution`
- `StepExecution`
- `Workspace`
- `AgentProfile`
- `AgentAssignment`

That is the smallest durable model that can support a real monitoring UI without forcing premature product decisions.
