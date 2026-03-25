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

## Plan Format

Top-level fields:

- `repo`: path to the target git repository; relative paths are resolved from the plan file location
- `objective`: overall objective shared with every Codex step
- `defaults.verify`: optional shell command to run after each step
- `defaults.stop_on_failure`: whether to stop immediately when Codex or verification fails
- `steps`: ordered list of step objects

Step fields:

- `id`: unique step identifier
- `prompt`: instructions for Codex for that step
- `verify`: optional shell command that overrides `defaults.verify`
- `expect_clean_diff`: when `true`, the run fails if the step leaves any file changes

Example:

```yaml
repo: ../path/to/target-repo
objective: |
  Make a small, well-scoped change in the target repository with clear separation
  between inspection, implementation, validation, and cleanup.

defaults:
  verify: python3 -m pytest -q
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
8. Runs step-level or default verification if configured.
9. Writes a JSON log under `.kctl-runs/` in this repository.

## Run Logs

Each run writes a directory like:

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
