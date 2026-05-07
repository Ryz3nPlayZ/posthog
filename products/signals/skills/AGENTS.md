# Signals Skills

Two distinct skill families live in this directory:

1. **Official PostHog skills** — `signals/`, `inbox-exploration/`. First-party PostHog
   skills published via `products/posthog_ai/dist/skills/` and loaded by users through
   the PostHog MCP. They teach a caller how to query, browse, and reason about signals
   data. They are not part of the automated agent path — humans (and human-driven
   agents) reach for them on demand.
2. **Scout fleet** — `signals-agent-*/`. Canonical default skills that the headless
   Signals agent loads into its system prompt at runtime. These are also the first
   example of PostHog shipping templated skills _into a user's PostHog Skills Store_:
   `lazy_seed` mirrors them onto each agent-enabled team's `LLMSkill` rows on the first
   coordinator tick, where users can then edit or override them per-team. They are not
   designed to be invoked by humans directly; the prompt and tool affordances assume
   they are running inside the harness.

## Scout fleet convention (`signals-agent-*`)

The harness discovers scouts by globbing `signals-agent-*` over the team's `LLMSkill`
table. The canonical content on disk in this directory is mirrored to each
agent-enabled team's `LLMSkill` rows by `agent_harness/lazy_seed.py` — see
`../backend/agent_harness/AGENTS.md` for the sync mechanics, and the
`sync_signals_agent_skills` management command for the manual fan-out path.

### Generalist + specialists

- `signals-agent-general/` — cross-product generalist. Reads everything via 12 product
  lenses + four mid-skill references (calibration, dedupe, finding-schema,
  investigation-patterns). This is the entry point if you want to understand how a
  scout decides what to investigate end-to-end. Specialists follow the same shape
  with tighter focus.
- `signals-agent-llm-analytics/` — anomaly watcher for LLM analytics
  (cost / latency / error / token-share regressions).
- `signals-agent-logs/` — anomaly watcher for logs (rate / level / pattern shifts).
- `signals-agent-error-tracking/` — anomaly watcher for error tracking
  (issue spikes, regressions, suppression-rule churn).
- `signals-agent-revenue-analytics/` — anomaly watcher for revenue
  (MRR / churn / segment shifts).
- `signals-agent-observability-gaps/` — the odd one out. Watches for _structural
  gaps_ between events being captured and existing insight / dashboard / alert
  coverage, and emits P3 _recommendations_ rather than P0–P2 _anomalies_.

### How the coordinator picks one

For each team with an enabled `SignalAgentConfig`:

1. The coordinator builds the candidate set: `signals-agent-*` skills present on the
   team's `LLMSkill` rows, intersected with `enabled_skill_names` if the config
   pins a list.
2. It samples `min(runs_per_tick, len(candidates))` skills uniformly at random
   without replacement.
3. Each sampled skill becomes one `RunSignalsAgentWorkflow` child run.

Default `runs_per_tick=1` gives every candidate an equal share of run slots over
time. Setting `runs_per_tick=0` is a soft pause — the team stays `enabled=True`
but contributes no runs that tick. See `agent_coordinator._resolve_skill_names_for_config`
for the exact edge cases.

### Authoring a new scout

Creating a new `signals-agent-foo/SKILL.md` directory and merging it is enough.
The next coordinator tick (or an explicit `sync_signals_agent_skills --all-enabled` run)
will:

- discover it via `lazy_seed.discover_canonical_skills()`,
- create matching `LLMSkill` rows on each agent-enabled team,
- add it to the candidate pool so the per-team sampler will pick it up.

No coordinator-side code change is needed. Use `signals-agent-general` as the
template if your scout is broad; pick a specialist as the template if it is
domain-tight.

### What lives inside a scout SKILL.md

Each scout's body is an instruction set the harness loads verbatim into the system
prompt. References (siblings of `SKILL.md`) are progressively disclosed via
`Skill.read_file()` from inside the run. The fleet shares four conventions worth
knowing if you author a new scout:

- **Calibration** — what counts as a real anomaly vs. noise for the scout's domain.
- **Dedupe rules** — how to check past runs and memory before emitting a finding.
- **Finding schema** — the structured payload shape the scout passes to
  `emit_signal_*`.
- **Investigation patterns** — repeated query shapes the scout will reuse.

The generalist carries all four as references; specialists either include their own
versions or link back to the generalist's.

## When editing skills in this directory

- **Official skills (`signals/`, `inbox-exploration/`).** Disk in this directory is
  the source of truth. Changes get published to `products/posthog_ai/dist/skills/`
  for distribution as part of the official PostHog skill set; they are not
  auto-synced onto teams' `LLMSkill` rows.
- **Scout skills (`signals-agent-*/`).** Disk in this directory is the source of
  truth, and `lazy_seed` mirrors changes onto each agent-enabled team's `LLMSkill`
  rows on the next coordinator tick (or immediately via
  `python manage.py sync_signals_agent_skills --all-enabled`). Teams that have
  manually edited a row are treated as "diverged" and left alone — the sync logs
  them so you can decide whether to nudge those teams to reset.
- **If you change the scout fleet shape (add a new specialist, rename, or change
  the SKILL.md schema), update this file.**

## Reference

- Harness layout and run lifecycle — `../backend/agent_harness/AGENTS.md`
- Coordinator + sampling rules — `../backend/temporal/agentic/agent_coordinator.py`
- Canonical sync mechanics + manual command —
  `../backend/management/AGENTS.md` (Canonical skill sync section)
