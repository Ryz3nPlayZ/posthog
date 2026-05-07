"""Rollout-gating feature flag for the Signals agent.

A single PostHog feature flag (`signals-agent`) covers the full rollout surface of the
headless agent. Two evaluation contexts share the key:

- **Runtime (per-team).** `team_passes_rollout_flag(team)` here. Used by the Temporal
  coordinator activity (gate fan-out per tick) and the `sync_signals_agent_skills`
  management command (gate canonical-skill push). Group context is
  `{"organization": ..., "project": ...}` so the flag dashboard can target on
  project-id allowlist or organization-property conditions.
- **MCP tool surface (per-user).** Evaluated automatically by the MCP server's
  `resolveToolFeatureFlags` (`services/mcp/src/lib/analytics.ts`) on the requesting
  user's distinct_id. Targeting is user-property based (e.g. internal-user property)
  via the `feature_flag: signals-agent` annotation in `products/signals/mcp/tools.yaml`.

Both gates **fail closed** — a flag-eval exception returns `False` (no run, no skill
push, tool absent from the MCP surface). A momentary PostHog-flags outage shouldn't
quietly let through teams the operator hasn't enrolled.

Why one flag, not three: the dashboard rollout strategy composes naturally on a single
flag (project-group condition OR user-property condition). Splitting into per-gate
flags adds dashboards without buying real flexibility — if a single gate needs to lag
behind during rollout, that's a flag-condition tweak, not a separate flag. Revisit
if/when staging genuinely diverges per gate.

Layering: this flag sits *above* the existing static gates on `SignalAgentConfig`
(`enabled`, `shadow_mode`). A team must be both flagged and configured-enabled to
contribute runs; the flag is the dynamic dial, the config is the per-team master
switch + emit posture.
"""

from __future__ import annotations

import posthoganalytics

from posthog.exceptions_capture import capture_exception
from posthog.models.team.team import Team

# Single source of truth for the flag key. Match this exactly in
# `products/signals/mcp/tools.yaml` (`feature_flag: signals-agent`) so the runtime
# and MCP-surface gates stay aligned across rollout flips.
SIGNALS_AGENT_ROLLOUT_FLAG = "signals-agent"


def team_passes_rollout_flag(team: Team) -> bool:
    """Per-team check for `SIGNALS_AGENT_ROLLOUT_FLAG`.

    Targeting is group-evaluated against organization + project. We pass
    `only_evaluate_locally=True` because the conditions we expect to use
    (project-id allowlist, organization is_internal) are local-evaluable from
    the group properties surfaced here — this avoids a flag-eval HTTP roundtrip on
    every coordinator tick and every `sync_signals_agent_skills` invocation. If a
    future rollout condition needs a non-local property (e.g. cohort membership),
    flip this to `False` deliberately rather than by default.

    Fails closed: any exception (eval failure, missing flag definition,
    posthoganalytics misconfig) returns `False` so the gate stays an explicit
    enroll-list rather than a soft default-on.
    """
    try:
        return bool(
            posthoganalytics.feature_enabled(
                SIGNALS_AGENT_ROLLOUT_FLAG,
                str(team.uuid),
                groups={
                    "organization": str(team.organization_id),
                    "project": str(team.id),
                },
                group_properties={
                    "organization": {"id": str(team.organization_id)},
                    "project": {"id": str(team.id)},
                },
                only_evaluate_locally=True,
                send_feature_flag_events=False,
            )
        )
    except Exception as error:
        capture_exception(error)
        return False
