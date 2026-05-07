from __future__ import annotations

import random
from typing import Any

import pytest
from unittest.mock import AsyncMock, patch

import pytest_asyncio
from asgiref.sync import sync_to_async
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import ActivityEnvironment

from posthog.models import Organization, Team
from posthog.models.scoping import team_scope
from posthog.sync import database_sync_to_async

from products.llm_analytics.backend.models.skills import LLMSkill
from products.signals.backend.models import SignalAgentConfig
from products.signals.backend.temporal.agentic.agent_coordinator import (
    CoordinatorWorkflowInput,
    CoordinatorWorkflowOutput,
    FetchEnabledRunsInput,
    PlannedRun,
    SignalsAgentCoordinatorWorkflow,
    fetch_enabled_signals_agent_runs_activity,
)


@pytest_asyncio.fixture
async def aorganization():
    organization = await sync_to_async(Organization.objects.create)(
        name=f"SignalsCoordinatorTestOrg-{random.randint(1, 99999)}",
        is_ai_data_processing_approved=True,
    )
    yield organization
    await sync_to_async(organization.delete)()


@pytest_asyncio.fixture
async def ateam(aorganization):
    team = await sync_to_async(Team.objects.create)(
        organization=aorganization,
        name=f"SignalsCoordinatorTestTeam-{random.randint(1, 99999)}",
    )
    # Scout models use TeamScopedRootMixin (fail-closed); yield inside team_scope
    # so test bodies that touch `Model.objects.X()` find a context.
    # `canonical=True` skips the sync DB resolution lookup (illegal from async).
    with team_scope(team.id, canonical=True):
        yield team
    await sync_to_async(team.delete)()


@pytest_asyncio.fixture
async def aother_team(aorganization):
    # Sibling team used by cross-team tests; do NOT enter `team_scope` for it
    # (only one scope can be active at a time, and `ateam` is the primary one).
    # Cross-team writes should use `team_scope(aother_team.id, canonical=True)` explicitly.
    team = await sync_to_async(Team.objects.create)(
        organization=aorganization,
        name=f"SignalsCoordinatorOtherTeam-{random.randint(1, 99999)}",
    )
    yield team
    await sync_to_async(team.delete)()


def _create_skill(team: Team, name: str) -> LLMSkill:
    return LLMSkill.objects.create(team=team, name=name, description="d", body="b")


@pytest.fixture(autouse=True)
def _stub_canonical_sync(request):
    """Stub `sync_canonical_skills` to a no-op for every test in this module.

    These tests assert on coordinator sampling logic using hand-authored skills as fixtures.
    The real sync would write the canonical fleet onto every team on first encounter, which
    pollutes the candidate pool with skills the test didn't set up. We rely on dedicated
    coverage in `test_agent_harness_lazy_seed.py` for the sync semantics; here we only
    care that the coordinator calls it (and tolerates failures).

    Tests that exercise the real sync (e.g. asserting brand-new teams get seeded) opt out
    by marking themselves `@pytest.mark.real_canonical_sync`.
    """
    if request.node.get_closest_marker("real_canonical_sync"):
        yield
        return
    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.sync_canonical_skills",
        return_value=None,
    ):
        yield


@pytest.fixture(autouse=True)
def _stub_rollout_flag(request):
    """Default the per-team rollout flag to ON for every test in this module.

    The flag gate is exercised explicitly in `test_rollout_flag_off_skips_team` and
    `test_rollout_flag_decides_per_team`. Every other test in this module asserts on
    sampling / fan-out / dispatch logic that pre-dates the flag gate — defaulting the
    flag to OFF would invalidate every assertion below. Tests that need flag-OFF
    semantics opt in via `@pytest.mark.rollout_flag_off` or patch
    `team_passes_rollout_flag` themselves.
    """
    if request.node.get_closest_marker("rollout_flag_off") or request.node.get_closest_marker("rollout_flag_explicit"):
        yield
        return
    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.team_passes_rollout_flag",
        return_value=True,
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_disabled_config_is_skipped(ateam):
    # enabled defaults to False — get_or_create gives a disabled row.
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=False)
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-errors")

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert output.planned_runs == []


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.rollout_flag_explicit
async def test_rollout_flag_off_skips_team(ateam):
    """`enabled=True` alone is not enough: a team that fails the rollout flag gate is
    skipped without contributing planned runs. This is the dynamic dial that lets us
    narrow the rollout via the flag dashboard rather than DB writes."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True, enabled_skill_names=None)
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-errors")

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.team_passes_rollout_flag",
        return_value=False,
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert output.planned_runs == []


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.rollout_flag_explicit
async def test_rollout_flag_decides_per_team(ateam, aother_team):
    """The flag is evaluated per-team, not per-tick. A flag-on team contributes runs
    in the same tick where a flag-off team contributes none."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True, enabled_skill_names=None)
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=aother_team, enabled=True, enabled_skill_names=None
    )
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-errors")
    await database_sync_to_async(_create_skill)(aother_team, "signals-agent-errors")

    # Flag passes for `ateam`, fails for `aother_team`.
    def _per_team_flag(team):
        return team.id == ateam.id

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.team_passes_rollout_flag",
        side_effect=_per_team_flag,
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert [p.team_id for p in output.planned_runs] == [ateam.id]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_null_skill_list_globs_signals_agent_prefix_then_samples_one(ateam):
    """`enabled_skill_names=None` widens the candidate pool to all `signals-agent-*`
    skills on the team; the coordinator then samples one uniformly per tick."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True, enabled_skill_names=None)
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-errors")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-llm")
    # Non-matching prefix is ignored.
    await database_sync_to_async(_create_skill)(ateam, "custom-helper")

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    # Exactly one planned run, drawn from the two matching candidates.
    assert len(output.planned_runs) == 1
    assert output.planned_runs[0].skill_name in {"signals-agent-errors", "signals-agent-llm"}
    assert output.planned_runs[0].team_id == ateam.id


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_explicit_skill_list_filters_to_existing_only(ateam):
    """`enabled_skill_names = [...]` narrows the candidate pool to that intersection
    with what's on the team. With one valid candidate, sampling returns it deterministically."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam,
        enabled=True,
        enabled_skill_names=["signals-agent-errors", "signals-agent-typo"],
    )
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-errors")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-llm")  # not in list

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert [p.skill_name for p in output.planned_runs] == ["signals-agent-errors"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_sampling_picks_one_uniformly_from_candidates(ateam):
    """With multiple candidates on a single team and `runs_per_tick=1` (default),
    the coordinator picks exactly one via `random.sample`. Patching gives us
    deterministic assertions."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True, enabled_skill_names=None)
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-beta")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-gamma")

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.random.sample",
        side_effect=lambda population, k: [population[1]],  # always pick the middle, k must be 1
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    # Candidates are sorted before sampling, so index 1 == "signals-agent-beta".
    assert [p.skill_name for p in output.planned_runs] == ["signals-agent-beta"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_sampling_pool_respects_enabled_skill_names_constraint(ateam):
    """When `enabled_skill_names` is set, the sampling pool is the intersection of
    that list with skills actually present on the team — not the full glob."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam,
        enabled=True,
        enabled_skill_names=["signals-agent-alpha", "signals-agent-beta"],
    )
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-beta")
    # Off-list skill exists on the team but is excluded from sampling.
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-gamma")

    captured: dict[str, Any] = {}

    def _capture_and_sample(population, k):
        captured["population"] = list(population)
        captured["k"] = k
        return [population[0]]

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.random.sample",
        side_effect=_capture_and_sample,
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert captured["population"] == ["signals-agent-alpha", "signals-agent-beta"]
    assert captured["k"] == 1  # default runs_per_tick
    assert "signals-agent-gamma" not in captured["population"]
    assert [p.skill_name for p in output.planned_runs] == ["signals-agent-alpha"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_runs_per_tick_fans_out_n_of_m_skills(ateam):
    """`runs_per_tick=3` with 5 candidates returns 3 distinct skills per tick."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam, enabled=True, enabled_skill_names=None, runs_per_tick=3
    )
    for name in [
        "signals-agent-alpha",
        "signals-agent-beta",
        "signals-agent-gamma",
        "signals-agent-delta",
        "signals-agent-epsilon",
    ]:
        await database_sync_to_async(_create_skill)(ateam, name)

    captured: dict[str, Any] = {}

    def _capture_and_sample(population, k):
        captured["population"] = list(population)
        captured["k"] = k
        # Return the first k — sampling logic itself is `random`'s responsibility, not ours.
        return list(population[:k])

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.random.sample",
        side_effect=_capture_and_sample,
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert captured["k"] == 3  # min(runs_per_tick=3, len(candidates)=5)
    assert len(captured["population"]) == 5  # all candidates fed to the sampler
    assert len(output.planned_runs) == 3
    # Result is sorted before being added to planned runs — defense against duplicates
    # within one tick is the no-replacement property of `random.sample`, not ours.
    skill_names = [p.skill_name for p in output.planned_runs]
    assert skill_names == sorted(skill_names)
    assert len(set(skill_names)) == 3  # all distinct


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_runs_per_tick_clamps_when_above_candidate_count(ateam):
    """`runs_per_tick=10` with only 2 candidates clamps to 2 — runs all of them, no error."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam, enabled=True, enabled_skill_names=None, runs_per_tick=10
    )
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-beta")

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    skill_names = sorted(p.skill_name for p in output.planned_runs)
    assert skill_names == ["signals-agent-alpha", "signals-agent-beta"]


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_runs_per_tick_zero_soft_pauses_team(ateam):
    """`runs_per_tick=0` is a soft pause: the team stays `enabled=True` but contributes
    no runs this tick. Useful during incident windows without flipping the boolean."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam, enabled=True, enabled_skill_names=None, runs_per_tick=0
    )
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-beta")

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert output.planned_runs == []


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_runs_per_tick_no_duplicates_invariant(ateam):
    """Sanity check on the no-replacement invariant. With `runs_per_tick=5` and
    5 candidates, every candidate appears exactly once in the planned runs."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(
        team=ateam, enabled=True, enabled_skill_names=None, runs_per_tick=5
    )
    candidates_seeded = [
        "signals-agent-alpha",
        "signals-agent-beta",
        "signals-agent-gamma",
        "signals-agent-delta",
        "signals-agent-epsilon",
    ]
    for name in candidates_seeded:
        await database_sync_to_async(_create_skill)(ateam, name)

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    skill_names = [p.skill_name for p in output.planned_runs]
    assert sorted(skill_names) == sorted(candidates_seeded)
    assert len(set(skill_names)) == len(candidates_seeded)  # no duplicates


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_planned_runs_one_per_team_sorted_by_team_id(ateam, aother_team):
    """One PlannedRun per enabled team (sampling-of-one), sorted by team_id so the
    stagger assignment is stable across ticks."""
    # Insert in the "wrong" order to verify sort behavior.
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=aother_team, enabled=True)
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True)
    await database_sync_to_async(_create_skill)(aother_team, "signals-agent-errors")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-zeta")
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    # One PlannedRun per team; ateam's run is one of {alpha, zeta} via sampling.
    assert len(output.planned_runs) == 2
    team_ids = [p.team_id for p in output.planned_runs]
    assert team_ids == sorted(team_ids)
    by_team = {p.team_id: p.skill_name for p in output.planned_runs}
    assert by_team[ateam.id] in {"signals-agent-alpha", "signals-agent-zeta"}
    assert by_team[aother_team.id] == "signals-agent-errors"


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.real_canonical_sync
async def test_lazy_seeds_canonical_skills_for_brand_new_team(ateam):
    # An enabled config on a brand-new team (no signals-agent-* skills yet) should
    # still produce planned runs: the coordinator lazy-seeds the canonical set on
    # first encounter so the cadence path doesn't depend on a manual seed step.
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True, enabled_skill_names=None)

    pre = await database_sync_to_async(
        lambda: list(
            LLMSkill.objects.filter(team=ateam, name__startswith="signals-agent-").values_list("name", flat=True)
        )
    )()
    assert pre == []

    env = ActivityEnvironment()
    output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    seeded = await database_sync_to_async(
        lambda: list(
            LLMSkill.objects.filter(team=ateam, name__startswith="signals-agent-").values_list("name", flat=True)
        )
    )()
    # The canonical fleet ships `signals-agent-general` (cross-product generalist) plus
    # specialists; assert at least one canonical skill was seeded.
    assert any(name.startswith("signals-agent-") for name in seeded)
    # Sampling-of-one means exactly one PlannedRun per team, drawn at random from the
    # seeded set. Assert the planned run names a real seeded skill rather than asserting
    # which one — the random pick is deliberate behavior.
    assert len(output.planned_runs) == 1
    assert output.planned_runs[0].skill_name in seeded


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_lazy_seed_failure_does_not_abort_tick(ateam, aother_team):
    # If lazy seed fails for one team, the coordinator should still plan runs for
    # other teams and for skills that already exist on the failing team.
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True)
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=aother_team, enabled=True)
    # ateam already has a hand-authored skill — the seed call shouldn't even fire
    # for them (existing-rows short-circuit) but if it did and somehow raised,
    # we still want planning to succeed.
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-existing")

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.sync_canonical_skills",
        side_effect=RuntimeError("simulated seed failure"),
    ):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    # ateam's existing skill is still plannable; aother_team has no skills and
    # the failed seed left it empty, so it contributes nothing — but the tick
    # didn't crash.
    assert any(p.team_id == ateam.id and p.skill_name == "signals-agent-existing" for p in output.planned_runs)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_truncates_above_hard_cap(ateam, aother_team):
    """The hard cap defends against a config explosion across many teams. Sampling-of-one
    means the cap is now effectively per-team rather than per-skill, so we test it by
    enabling more teams than the (lowered) cap and verifying truncation kicks in."""
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=ateam, enabled=True)
    await database_sync_to_async(SignalAgentConfig.objects.create)(team=aother_team, enabled=True)
    await database_sync_to_async(_create_skill)(ateam, "signals-agent-alpha")
    await database_sync_to_async(_create_skill)(aother_team, "signals-agent-beta")

    with patch("products.signals.backend.temporal.agentic.agent_coordinator.MAX_RUNS_PER_TICK", 1):
        env = ActivityEnvironment()
        output = await env.run(fetch_enabled_signals_agent_runs_activity, FetchEnabledRunsInput())

    assert len(output.planned_runs) == 1


# ── Workflow-level tests ────────────────────────────────────────────────────────
#
# The coordinator dispatches child workflows fire-and-forget via `start_child_workflow`
# with `ParentClosePolicy.ABANDON`, so it returns as soon as the last dispatch resolves.
# We patch the activity + `start_child_workflow` and assert dispatch counts (started vs
# already-running skip) rather than completion outcomes — child runtime success is the
# child workflow's contract, not the coordinator's.


@pytest.mark.asyncio
async def test_workflow_returns_zero_counts_when_no_planned_runs():
    coordinator = SignalsAgentCoordinatorWorkflow()
    fake_fetch_result = type("R", (), {"planned_runs": []})()

    with patch(
        "products.signals.backend.temporal.agentic.agent_coordinator.workflow.execute_activity",
        new_callable=AsyncMock,
        return_value=fake_fetch_result,
    ):
        output = await coordinator.run(CoordinatorWorkflowInput())

    assert output == CoordinatorWorkflowOutput(0, 0, 0)


@pytest.mark.asyncio
async def test_workflow_dispatches_children_fire_and_forget():
    planned = [
        PlannedRun(team_id=1, skill_name="signals-agent-a"),
        PlannedRun(team_id=1, skill_name="signals-agent-b"),
        PlannedRun(team_id=2, skill_name="signals-agent-c"),
    ]
    fake_fetch_result = type("R", (), {"planned_runs": planned})()

    # Second dispatch raises WorkflowAlreadyStartedError → counted as skipped, others as started.
    dispatch_outcomes: list[BaseException | None] = [
        None,
        WorkflowAlreadyStartedError("dup", "signals-agent-run-1-signals-agent-b-tick-1-1"),
        None,
    ]
    dispatch_calls: list[tuple[int, str]] = []

    async def fake_start_child(_workflow_run, run_input, **kwargs):
        idx = len(dispatch_calls)
        dispatch_calls.append((run_input.team_id, run_input.skill_name))
        outcome = dispatch_outcomes[idx]
        if isinstance(outcome, BaseException):
            raise outcome
        return AsyncMock()

    coordinator = SignalsAgentCoordinatorWorkflow()
    with (
        patch(
            "products.signals.backend.temporal.agentic.agent_coordinator.workflow.execute_activity",
            new_callable=AsyncMock,
            return_value=fake_fetch_result,
        ),
        patch(
            "products.signals.backend.temporal.agentic.agent_coordinator.workflow.info",
            return_value=type("Info", (), {"workflow_id": "tick-1"})(),
        ),
        patch(
            "products.signals.backend.temporal.agentic.agent_coordinator.workflow.logger",
        ),
        patch(
            "products.signals.backend.temporal.agentic.agent_coordinator.workflow.start_child_workflow",
            side_effect=fake_start_child,
        ),
    ):
        output = await coordinator.run(CoordinatorWorkflowInput())

    assert output.planned_count == 3
    assert output.started_count == 2
    assert output.skipped_count == 1
    # All three planned runs were dispatched in order, even though one was a dedupe-skip.
    assert dispatch_calls == [
        (1, "signals-agent-a"),
        (1, "signals-agent-b"),
        (2, "signals-agent-c"),
    ]
