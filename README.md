# kctl

`kctl` is a small CLI that runs a YAML execution plan and uses `codex` as the coding agent inside another git repository. `kctl` does not edit code itself in the target repo; it coordinates sequential Codex steps, captures outputs, runs verification commands, and writes a machine-readable run log.

## Requirements

- Python 3
- `codex` CLI installed and available on `PATH`
- `PyYAML` installed

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

## Usage

Run a plan:

```bash
python3 kctl.py run examples/sample-plan.yaml
```

Opt in to external artifact storage for a single run:

```bash
KCTL_ARTIFACT_STORAGE=external python3 kctl.py run examples/sample-plan.yaml
KCTL_ARTIFACT_STORAGE=external KCTL_HOME=/tmp/kctl-home python3 kctl.py run examples/sample-plan.yaml
```

Run the same plan across multiple repositories under a root:

```bash
python3 kctl.py batch examples/sample-plan.yaml --root /Users/sasa/Projects
python3 kctl.py batch examples/sample-plan.yaml --root /Users/sasa/Projects --output-mode grouped
```

Run multiple plans for one repository with isolated worktrees:

```bash
python3 kctl.py plans run-many plans/traffic-simulator --concurrency 3
python3 kctl.py plans status plans/traffic-simulator
```

Opt in to external artifact storage for multi-plan runs and UI/index state:

```bash
KCTL_ARTIFACT_STORAGE=external python3 kctl.py plans run-many plans/traffic-simulator --concurrency 3
KCTL_ARTIFACT_STORAGE=external python3 kctl.py ui index /path/to/repo
KCTL_ARTIFACT_STORAGE=external python3 kctl.py ui dashboard /path/to/repo
```

Index execution state for future UI work and inspect it locally:

```bash
python3 kctl.py ui index /path/to/repo
python3 kctl.py ui runs /path/to/repo
python3 kctl.py ui run /path/to/repo 20260325T120000000000Z
python3 kctl.py ui dashboard /path/to/repo
```

`kctl` can be run from any shell directory. Plan lookup checks the provided path first, then falls back to `KCTL_PLAN_ROOT` if the direct path does not exist.

When `KCTL_ARTIFACT_STORAGE=external` is set, `kctl` stores run metadata outside the target repository. `KCTL_HOME` controls the external root and defaults to `~/.kctl`.

Example from outside this repository:

```bash
KCTL_PLAN_ROOT=/Users/sasa/Projects/kctl/plans python3 /Users/sasa/Projects/kctl/kctl.py run traffic-simulator/001-initialize-simulator.yaml
```

## Plan Format

Top-level fields:

- `repo`: path to the target git repository; relative paths are resolved from the plan file location
- `objective`: overall objective shared with every Codex step
- `defaults.verify`: optional shell command to run after each step
- `defaults.verify_shell`: optional shell prefix for verification commands, for example `zsh -lc`
- `defaults.stop_on_failure`: whether to stop immediately when Codex or verification fails
- `steps`: ordered list of step objects

Step fields:

- `id`: unique step identifier
- `kind`: optional `agent` or `verify`; defaults to `agent` unless `commands` are present
- `name`: optional human-readable step name
- `prompt`: instructions for Codex for that step; required for `agent` steps
- `verify`: optional shell command that overrides `defaults.verify`
- `verify_shell`: optional shell prefix that overrides `defaults.verify_shell`
- `commands`: optional list of deterministic shell commands for `verify` steps
- `expect_clean_diff`: when `true`, the run fails if the step leaves any file changes

Legacy example:

```yaml
repo: ../path/to/target-repo
objective: |
  Make a small, well-scoped change in the target repository with clear separation
  between inspection, implementation, validation, and cleanup.

defaults:
  verify: python3 -m pytest -q
  verify_shell: zsh -lc
  stop_on_failure: true

steps:
  - id: inspect
    prompt: |
      Inspect the repository and identify the smallest useful change to satisfy the
      objective. Do not modify any files. Summarize the intended implementation plan.
    expect_clean_diff: true

  - id: implement
    prompt: |
      Implement the planned change with the smallest practical diff. Add or update
      code and tests only where needed.

  - id: verify
    prompt: |
      Focus on validation. Inspect the current changes, fix obvious test or validation
      issues if needed, and leave the repository in a verifiable state.
    verify: python3 -m pytest -q

  - id: review
    prompt: |
      Review the current diff and reduce incidental edits only. Avoid broad refactors
      or unrelated cleanup.
    expect_clean_diff: false
```

Explicit equivalent:

```yaml
repo: ../path/to/target-repo
objective: |
  Make a small, well-scoped change in the target repository with clear separation
  between inspection, implementation, validation, and cleanup.

defaults:
  verify:
    commands:
      - python3 -m pytest -q
    shell: zsh -lc
    mode: full
  stop_on_failure: true

steps:
  - id: inspect
    type: analyze
    mode: read-only
    prompt: |
      Inspect the repository and identify the smallest useful change to satisfy the
      objective. Do not modify any files. Summarize the intended implementation plan.
    output:
      schema: inspect_v1

  - id: implement
    type: change
    prompt: |
      Implement the planned change with the smallest practical diff. Add or update
      code and tests only where needed.

  - id: verify
    type: verify
    prompt: |
      Focus on validation. Inspect the current changes, fix obvious test or validation
      issues if needed, and leave the repository in a verifiable state.
    verify:
      commands:
        - python3 -m pytest -q
      shell: zsh -lc
      mode: full

  - id: review
    type: review
    review:
      policy: advisory
    prompt: |
      Review the current diff and reduce incidental edits only. Avoid broad refactors
      or unrelated cleanup.
    output:
      schema: review_v1
```

Explicit fields override legacy inference. When omitted, `kctl` resolves behavior from legacy conventions for compatibility.

## Effective Behavior Resolution

During the migration from implicit step behavior to an explicit execution contract, `kctl` resolves step behavior in this order:

1. Explicit step fields
2. Legacy `kind`
3. Step-id conventions
4. Defaults

## Compatibility Mapping

- `expect_clean_diff: true` on legacy analysis-style steps maps to `mode: read-only`.
- Legacy `defaults.verify` string maps to `verify.commands`.
- Legacy verify-step conventions map to `type: verify`.
- Legacy review-step conventions map to `type: review`, with policy inferred when not declared.
- Legacy hard-coded structured parsing for `inspect` and `plan` maps to `output.schema`.
- During migration, explicit contract fields take precedence over `kind`, step-id conventions, and defaults.

## Migration Note

Legacy plans remain valid. New plans can declare intent directly with explicit contract fields such as `type`, `mode`, `output.schema`, and `review.policy`.

## Artifact Storage Migration

### Overview

`kctl` supports two artifact storage modes:

- `in_repo`: keeps current behavior by writing run metadata inside the target repository
- `external`: writes run metadata outside the target repository to keep harness state separate from product code

### Compatibility

Existing in-repo runs remain readable. During migration, `kctl` should support reading both storage layouts. No immediate backfill or conversion is required.

### Recorded Per Run

Each run should record:

- the effective storage mode
- the resolved artifact root path

This keeps inspection and debugging straightforward while both layouts are supported.

### Rollout

`in_repo` remains the default initially. `external` is opt-in first. The default should change only after the external path has been validated across normal runs, multi-plan runs, and UI/index reads.

## Post-Migration Status

### Explicit Fields

- `type`: `analyze` | `change` | `verify` | `review`
- `output.schema`: per-step structured artifact contract
- `review.policy`: `advisory` | `blocking` | `manual`
- `mode`: `default` | `read-only`
- `verify_mode`: `legacy` | `full`

### Legacy Conventions

- `id: inspect` and `id: plan` map to inferred step type and inferred `output.schema`
- `expect_clean_diff: true` maps to inferred `mode: read-only`
- legacy verify-step behavior maps to inferred `type: verify`
- implicit review steps map to inferred `review.policy: manual`
- string `defaults.verify` remains accepted as a legacy verification command form

### Resolution Order

1. Explicit field on the step
2. `defaults`
3. Legacy inference
4. Runner fallback

### First-Class Vs Compatibility

- First-class: `type`, `output.schema`, `review.policy`, `mode`
- Compatibility-shaped: `verify_mode` semantics, legacy id-based inference, partial baseline awareness in verification

### Storage Status

- Modes: `in_repo` (default), `external` (opt-in via `KCTL_ARTIFACT_STORAGE`)
- Dual-read is supported; no migration or conversion is required
- Run metadata records `artifact_storage_mode` and `artifact_root_path`

### Recommended Usage

- Prefer explicit `type`, `output.schema`, and `review.policy`
- Use `mode: read-only` instead of `expect_clean_diff`
- Prefer structured verification commands over legacy string forms

### Not Yet Guaranteed

- verification is not fully baseline-scoped
- not all steps are required to declare schemas
- legacy inference remains supported

That sample plan is intentionally staged:

- `inspect` is analysis only and must leave a clean diff.
- `implement` is where the actual code change happens.
- `verify` focuses on tests and validation.
- `review` trims incidental edits without broadening scope.

## What `kctl run` Does

For each step, `kctl`:

1. Loads and validates the YAML plan.
2. Confirms the target repo exists and is a git repo.
3. Captures `git status --short` before the step.
4. Builds a Codex prompt from the overall objective, prior step summaries, and the current step prompt.
5. Runs `codex exec` in the target repo and captures stdout, stderr, and exit code.
6. Captures `git status --short` and `git diff --stat` after the step.
7. Enforces `expect_clean_diff` if configured.
8. Runs step-level or default verification if configured. Verification may run under a different shell/environment than your interactive terminal unless `verify_shell` is set explicitly.
9. Writes run artifacts under `.kctl-runs/` inside the target repository.

`kctl batch <plan> --root <path>` scans recursively for directories containing `.git`, treats each as a target repository, overrides the plan `repo:` field for that execution only, and runs the normal per-repo flow sequentially.

Batch output modes:

- `stream` (default): stream each repo's logs live with repo-prefixed lines.
- `grouped`: buffer each repo's logs and print them as a single section after that repo completes.
- `quiet`: suppress per-step logs and print only batch repo boundaries and summaries.

Interactive prompts are not supported in batch mode. `--approve-each-step` will fail fast for `kctl batch`.

`kctl plans run-many <plans-dir> --concurrency <n>` loads all `*.yaml` and `*.yml` files in a directory, validates that they target the same repository, creates one isolated workspace per plan under `.kctl/worktrees/<run-id>/`, and runs plans concurrently up to the requested limit while keeping each plan's own steps sequential.

Multi-plan run state is written under `.kctl/runs/<run-id>/run.json`, with one per-plan subdirectory containing that plan's run log and step artifacts.

`kctl ui index <repo>` builds a local SQLite index at `.kctl/ui-state.db` from existing `.kctl/runs`, `.kctl/worktrees`, and legacy `.kctl-runs` data. `kctl ui runs` and `kctl ui run` are read-only inspection commands over that index. `kctl ui dashboard` launches a minimal local web dashboard for browsing runs, plan executions, step timelines, and workspace details.

## Run Logs

Each target repository gets its own `.kctl-runs/` directory. A run writes a directory like:

```text
.kctl-runs/20260323T000000Z/
```

Each run directory contains `run.json` plus per-step artifacts such as:

```text
step-01-raw.md
step-01-inspect.json
step-02-raw.md
step-02-plan.json
step-03-raw.md
step-03-verify.json
```

The run log includes plan metadata, per-step Codex output, git status snapshots, diff stats, verification results, artifact paths, and the final run status.
Each step log also includes `started_at`, `ended_at`, the full `codex_prompt`, `changed_files`, `changed_files_count`, and any structured artifact parse errors.
Verification logs now also include basic environment diagnostics such as the working directory, shell used, `which node`, `node -v`, `which npm`, and `npm -v` when available.
