# Signals Agent Harness

This directory contains the headless **Signals agent** — a scheduled scout that explores
a project, writes durable memory across runs, and emits findings into the Signals inbox
via `emit_signal()` using the `signals_agent` source variant.

It is the second agentic surface in Signals. The other one — `report_generation/` — runs
on demand when a `SignalReport` is promoted to `candidate` and produces a single research
output for one report. The harness here is the inverse: it runs on a schedule, decides
_what_ to investigate from scratch, and pushes new signals into the same pipeline rather
than acting on existing ones.

In production it is driven by `SignalsAgentCoordinatorWorkflow` (hourly tick → fan out
per-(team, skill) child workflows). Locally it is exercised via the `run_signals_agent`
management command (see `../management/AGENTS.md`).

## What lives here

- `runner.py`
  Per-run entrypoint (`arun_signals_agent` / `run_signals_agent`). Inserts the
  `SignalAgentRun` row, builds the prompt + toolset, spawns the sandbox session,
  pumps the agent loop until budget exhaustion or natural completion, finalizes the run,
  and returns a `RunResult`. The activity wrapper in `temporal/agentic/agent_scheduler.py`
  delegates straight to this.
- `prompt.py`
  Assembles the system prompt: persona + skill body + relevant memory + project profile
  inventory + recent run summaries. Memory and run history are filtered by skill so a
  specialist only sees its own past work.
- `skill_loader.py`
  Resolves `signals-agent-*` skills from the team's `LLMSkill` rows. Defines
  `SIGNALS_AGENT_SKILL_PREFIX` and `LoadedSkill` (body + version + allowed_tools).
- `lazy_seed.py`
  Canonical skill sync. Reads `products/signals/skills/signals-agent-*/` from disk and
  reconciles them against the team's `LLMSkill` rows: creates missing rows, updates
  ones the team hasn't edited, leaves diverged rows alone, tombstones rows whose
  canonical skill was deleted, backfills metadata. Called both lazily (coordinator tick,
  runner cold-start) and explicitly via the `sync_signals_agent_skills` management command.
- `tool_registry.py`
  Declares `HARNESS_INTERNAL_TOOLS` (the harness-owned tools: emit, memory, profile,
  runs) and resolves the effective toolset for a run by intersecting the skill's
  `allowed_tools` with what the harness actually exposes. Validates tool names so a
  typo in a SKILL.md fails loudly.
- `tools/`
  Implementations of the four harness-internal tools the agent calls during a run:
  - `emit.py` — `emit_signal_*` tools that push findings as `cross_source_issue`
    signals into the standard ingestion pipeline.
  - `memory.py` — `memory_*` tools (read/write/delete) backed by the `SignalMemory` model.
  - `profile.py` — `project_profile_*` tools that read the deterministic
    `SignalProjectProfile` snapshot.
  - `runs.py` — `runs_*` tools that read past `SignalAgentRun` rows for dedupe and
    cross-skill awareness.
- `profile/`
  - `builders.py` — deterministic builders that compute the inventory payload for
    `SignalProjectProfile`. Sections fall into three layers: capability / configured
    (sticky — `products_in_use`, `integrations`, `external_data_sources`,
    `signal_source_configs`, …), aggregated recency (`recent_activity` — per-scope
    counts off the activity log, cross-cutting orientation across every entity type),
    and per-entity recent inventory (`recent_surveys`, `recent_feature_flags`,
    `recent_experiments`, `recent_alerts`, `recent_hog_functions`, `recent_hog_flows`,
    `recent_notebooks`, `recent_cohorts`, `recent_actions`, `recent_dashboards`).
    Per-entity sections are deliberately light (counts + 5 most-recent items with
    name, status, timestamp); deep drilldowns go via the per-entity MCP list tools.
    See the module docstring at `profile/builders.py` for the authoritative section
    list — when adding or renaming a section, bump `INVENTORY_SOURCE_VERSION` so
    the cache invalidates cleanly.
- `limits.py`
  `RunLimits` dataclass (`max_runtime_s`, `max_findings`, …), `DEFAULT_LIMITS`,
  `WORKFLOW_HARD_CEILING_S` (the activity-level ceiling that gates the workflow's
  `start_to_close_timeout`), and `resolve_limits()` which folds per-team
  `SignalAgentConfig.limit_overrides` over the harness defaults.
- `serializers.py`
  DRF serializers for the harness HTTP surface (runs, memory, project profile).
  Annotated for drf-spectacular so the generated MCP tools have informative schemas.
- `views.py`
  `SignalAgentRunViewSet`, `SignalMemoryViewSet`, `SignalProjectProfileViewSet`.
  Routed under `environment_signals_agent_*` basenames in `posthog/api/__init__.py`
  and exposed as `signals-agent-*` MCP tools via `products/signals/mcp/tools.yaml`.

## Mental model

`arun_signals_agent()` is the main entrypoint. One call → one `SignalAgentRun` row →
one sandbox session → zero or more emitted signals.

- The harness owns the run-row lifecycle (insert at start, finalize on completion or
  failure). A partial unique constraint on `(team, skill_name) WHERE status='running'`
  is the single-flight guard against tick-over-tick collisions; an `IntegrityError`
  there becomes a clean skip with `skip_reason="already_running"`.
- The sandbox is opened with the team's MCP token plus the harness-internal tools.
  The skill body is loaded into the system prompt; `enabled_skill_names` on the
  team's `SignalAgentConfig` narrows the candidate pool the coordinator samples from.
- `MultiTurnSession.start()` creates a Tasks `(Task, TaskRun)` pair to drive the
  sandbox. Both UUIDs are captured into `SignalAgentRun.metadata` (`task_id`,
  `task_run_id`) by `_record_task_linkage` immediately after the session returns
  — this powers the `task_url` deep-link surfaced on the run serializers
  (`/project/{team_id}/tasks/{task_id}?runId={task_run_id}`) and is the join key
  for the future LLM-analytics token / cost roll-up. `_finalize_failed` reads-modifies-writes
  metadata so the linkage survives to the failure row a debugger needs to land on.
- Emit happens via the harness's `emit_signal_*` tools, which call `emit_signal()`
  with `source_product="signals_agent"` and `source_type="cross_source_issue"`.
  From there the signal flows through the same emitter → buffer → grouping v2 path
  as any other source.
- Memory and run history are read at prompt assembly time. The agent can also write
  memory mid-run via the `memory_*` tools — that's how a specialist with no anomalies
  to chase records "no LLM activity here, close out fast" so future runs of the
  same skill short-circuit cold.

## Where the rest of the system meets this directory

- **Coordinator** — `temporal/agentic/agent_coordinator.py` and `agent_scheduler.py`.
  Hourly tick (`COORDINATOR_INTERVAL_MINUTES = 60`), `runs_per_tick` sample per team,
  hard cap `MAX_RUNS_PER_TICK = 50` per tick, `ScheduleOverlapPolicy.SKIP` to drop
  ticks rather than queue them.
- **Models** — `SignalAgentConfig`, `SignalAgentRun`, `SignalMemory`,
  `SignalProjectProfile` in `../models.py`.
- **Source variant** — `SignalSourceConfig.SourceProduct.SIGNALS_AGENT` paired with
  `SourceType.CROSS_SOURCE_ISSUE`.
- **Scout fleet** — the `signals-agent-*` skills live at
  `../../skills/signals-agent-*/` (generalist + 5 specialists). See
  `../../skills/AGENTS.md` for the fleet convention.
- **Local commands** — `run_signals_agent` (one-shot run) and
  `sync_signals_agent_skills` (force a canonical-skill sync). Both documented in
  `../management/AGENTS.md`.

## When editing this flow

- Keep the harness loop generic. Skill-specific logic belongs in the SKILL.md of the
  scout, not in `runner.py` or `prompt.py`.
- New harness-internal tools: register in `tool_registry.HARNESS_INTERNAL_TOOLS` and
  add a corresponding scope check on the viewset in `views.py` so the MCP surface
  and the sandbox surface stay aligned.
- If you change the canonical SKILL.md format or directory layout, update
  `lazy_seed.discover_canonical_skills()` and the parser tests — the coordinator
  call to `sync_canonical_skills()` runs on every tick and silently swallows parser
  errors (logs only), so a quiet schema break can leave canonical content stale on
  every team.
- Run-row lifecycle invariants: `SCHEDULED → RUNNING → {COMPLETED, FAILED, ABANDONED}`.
  Anything that can leave a row stuck in `RUNNING` past the workflow deadline must be
  rescued by `runner._drain_final_log` so the partial unique constraint doesn't
  permanently block future runs of the same `(team, skill)` pair.
- Emit path goes through `emit_signal()` and only `emit_signal()`. Do not write to
  the embeddings pipeline or `SignalReport` directly from harness code.
- **If you add or rename a workflow/activity in `temporal/agentic/`, update
  `posthog/temporal/tests/ai/test_module_integrity.py` (`TestSignalsProductModuleIntegrity`)
  to match.**
- **If you change the harness layout or tool surface, update this file to match.**
