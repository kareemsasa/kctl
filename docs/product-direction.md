# Product Direction

## Core Idea

`kctl` is a local control plane for Codex-driven code change workflows.

The core problem is not "how do I run `codex` once?" The core problem is how to make agent-driven work more reliable than a one-off terminal session.

`kctl` should make Codex work:

- controllable
- repeatable
- verifiable
- inspectable

That means the product should optimize for runs, plan executions, steps, workspaces, verification state, and review state rather than only individual interactive threads.

## Why This Exists

Running `codex` directly is useful for one task in one session.

`kctl` exists to provide the operational layer around that session:

- explicit staged plans
- deterministic verification
- durable run logs and structured artifacts
- isolated workspaces for concurrent work
- indexed history for later inspection
- a UI that can answer "what is happening right now?"

In short: `kctl` turns Codex from an interactive coding agent into a managed workflow.

## Product Identity

When deciding what `kctl` is, prefer this framing:

`kctl` is a supervisor for agentic coding work, with the web UI as the eventual primary operator surface.

The CLI remains important, but mainly as:

- the execution backend
- the scripting interface
- the lowest-friction way to launch work

The web UI should become the place where an operator gets an overview of:

- active and recent runs
- plan execution status
- step timelines and artifacts
- failed verification and blocked work
- active or stale workspaces
- eventually, agent sessions and thread history

## Current Priorities

### Keep Building

- Execution state and indexing
- Multi-plan orchestration with isolated workspaces
- Read models that answer operator questions quickly

### Defer

- UI polish that does not improve operator decisions
- additional plan-language complexity before the execution contract is stable
- automation features that increase autonomy without improving observability

### Contain

- long-lived legacy inference in plan semantics
- ambiguity about whether this is only a CLI wrapper
- features that do not map to controllability, verifiability, or inspectability

## Control-Plane MVP

The near-term UI does not need to be visually ambitious. It does need to answer the right questions.

Recommended MVP views:

- Overview: active runs, failed runs, blocked runs, stale workspaces, recent failures
- Runs: sortable list of runs with repo, status, duration, current step, verify result
- Run Detail: plan executions within a run, branch/worktree, summaries, artifact links
- Plan Execution Detail: step timeline, raw outputs, structured artifacts, verify results, failure reason
- Workspaces: active worktrees/branches with status and associated run/plan
- Attention Queue: failed verify, blocked review, stale running work, parse errors, dirty workspaces

## Data Model Direction

The current repo already models much of the right shape. Over time the canonical operational entities should be:

- repository
- run
- plan execution
- step execution
- workspace
- verify result
- review result
- agent session
- thread

The "agent session" and "thread" pieces should only become prominent in the UI once they are persisted explicitly rather than inferred indirectly from step logs.

## Realignment Test

For any proposed feature, ask:

Does this make Codex work more controllable, more verifiable, or more inspectable?

If the answer is no, it is probably not core.

## Next Work Session

If picking up `kctl` after time away, start here:

1. Treat the control-plane direction as the product default.
2. Tighten the normalized execution contract before expanding the plan language further.
3. Improve the UI and indexed reads around operator workflows, not visual polish.
4. Add first-class agent/thread persistence only when the execution model is stable enough to support it cleanly.
