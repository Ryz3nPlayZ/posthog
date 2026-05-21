from __future__ import annotations

import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from django.utils import timezone

from posthog.models.team.team import Team
from posthog.models.utils import uuid7
from posthog.sync import database_sync_to_async

from products.signals.backend.models import SignalScoutConfig, SignalScoutRun
from products.signals.backend.scout_harness.lazy_seed import sync_canonical_skills
from products.signals.backend.scout_harness.limits import WORKFLOW_HARD_CEILING_S
from products.signals.backend.scout_harness.prompt import SignalScoutRunSummary, build_run_prompt
from products.signals.backend.scout_harness.skill_loader import LoadedSkill, load_skill_for_run
from products.signals.backend.temporal.agentic import (
    SIGNALS_REPORT_RESEARCH_ENV_NAME,
    get_or_create_signals_sandbox_env,
    resolve_user_id_for_team,
)
from products.tasks.backend.models import SandboxEnvironment, Task, TaskRun
from products.tasks.backend.services.custom_prompt_internals import CustomPromptSandboxContext
from products.tasks.backend.services.custom_prompt_multi_turn_runner import MultiTurnSession

logger = logging.getLogger(__name__)

# Reuse the report-research sandbox env. Same posture: full repo on disk, restricted
# network, MCP read scopes injected. Split out later if the agent needs different policy.
SIGNALS_SCOUT_SANDBOX_ENV_NAME = SIGNALS_REPORT_RESEARCH_ENV_NAME


@dataclass(frozen=True)
class RunResult:
    """Outcome of a run-trigger.

    `run_id` / `task_run_id` are None when the trigger was skipped without
    persisting a row (e.g. another run for the same team/skill is still in
    flight). `status` mirrors `TaskRun.Status` values as strings so callers
    don't need to import the tasks model.
    """

    run_id: str | None
    task_run_id: str | None
    status: str | None
    last_message: str | None
    runtime_s: float
    skill_name: str
    skill_version: int
    skip_reason: str | None = None


def run_signals_scout(
    *,
    team_id: int,
    skill_name: str,
    skill_version: int | None = None,
    repository: str | None = None,
    verbose: bool = False,
) -> RunResult:
    """Synchronous entrypoint: resolves config, spawns sandbox, persists the run row.

    Wraps the async core for callers that aren't inside an event loop (management
    command, direct script). Temporal activities call `arun_signals_scout` directly.
    """
    return asyncio.run(
        arun_signals_scout(
            team_id=team_id,
            skill_name=skill_name,
            skill_version=skill_version,
            repository=repository,
            verbose=verbose,
        )
    )


async def arun_signals_scout(
    *,
    team_id: int,
    skill_name: str,
    skill_version: int | None = None,
    repository: str | None = None,
    verbose: bool = False,
) -> RunResult:
    """Async core. Safe to call from inside a running event loop (Temporal activity)."""
    team = await database_sync_to_async(_get_team, thread_sensitive=False)(team_id)
    config = await database_sync_to_async(_resolve_config, thread_sensitive=False)(team)
    # Sync canonical signals-scout-* skills before we resolve the skill the run asked for.
    # Creates rows for newly-shipped specialists, updates harness-seeded rows the team
    # hasn't edited, and leaves forked / tombstoned rows alone. Failures here should not
    # crash the run — we log and continue with whatever skills the team already has.
    try:
        await database_sync_to_async(sync_canonical_skills, thread_sensitive=False)(team)
    except Exception:
        logger.exception(
            "signals_scout: canonical skill sync failed; continuing with existing team skills",
            extra={"team_id": team_id},
        )
    skill = await database_sync_to_async(load_skill_for_run, thread_sensitive=False)(
        team, skill_name, version=skill_version
    )

    # Self-heal stale RUNNING rows whose age exceeds 2x their max_runtime_s. Catches
    # rows left behind when a worker / sandbox died before the cleanup path could run
    # (e.g. SIGTERM during file-watcher restart, kernel OOM, asyncio cancellation that
    # escaped the harness). Without this, a single stale row blocks every subsequent
    # coordinator tick from spawning a fresh run. Keyed on `(team, skill_name)` to match
    # the partial unique index `signal_scout_run_one_running_per_team_skill` — keying on
    # `config_id` would leave orphaned rows with a stale or null `scout_config_id` (e.g.
    # after a config delete + recreate, since the FK is `SET_NULL`) un-healable while the
    # DB constraint keeps blocking new inserts for the same (team, skill).
    await database_sync_to_async(_self_heal_stale_runs, thread_sensitive=False)(team_id, skill_name)

    # Skip-if-running guard, keyed on (team, skill_name). Different skills for the
    # same team are allowed to run concurrently — `runs_per_tick > 1` relies on this.
    # Best-effort — there is a race window between this check and the bridge-row
    # insert inside _spawn_and_run (a second trigger could land in between), which
    # we accept until a claim/lease primitive lands.
    if await database_sync_to_async(_has_running_run, thread_sensitive=False)(
        team_id=team_id, config_id=str(config.id), skill_name=skill.name
    ):
        logger.info(
            "signals_scout: skipping trigger, prior run still in progress",
            extra={"team_id": team_id, "skill_name": skill.name},
        )
        return RunResult(
            run_id=None,
            task_run_id=None,
            status=None,
            last_message=None,
            runtime_s=0.0,
            skill_name=skill.name,
            skill_version=skill.version,
            skip_reason="prior run still in progress",
        )

    started = time.monotonic()
    # Pre-mint the bridge row's UUID so the prompt can reference it before the row
    # exists. The TaskRun is created inside `MultiTurnSession.start`; the bridge row is
    # inserted via its `on_task_run_created` hook — after the TaskRun exists but before
    # the agent's first turn — so first-turn finding emits can resolve the run by id.
    run_id = uuid7()
    started_at = timezone.now()
    try:
        last_message, task_run_id = await _spawn_and_run(
            team=team,
            config=config,
            run_id=run_id,
            started_at=started_at,
            skill=skill,
            repository=repository,
            verbose=verbose,
        )
        runtime_s = time.monotonic() - started
        return RunResult(
            run_id=str(run_id),
            task_run_id=task_run_id,
            status=TaskRun.Status.COMPLETED.value,
            last_message=last_message,
            runtime_s=runtime_s,
            skill_name=skill.name,
            skill_version=skill.version,
        )
    except Exception:
        runtime_s = time.monotonic() - started
        # Fail safe and silent: the TaskRun MultiTurnSession spans carries the error
        # context (status=FAILED, error_message, full chat log via LLMA). Nothing
        # additional to persist on the bridge row.
        logger.exception(
            "signals_scout: run failed",
            extra={"team_id": team_id, "run_id": str(run_id), "skill_name": skill.name},
        )
        return RunResult(
            run_id=str(run_id),
            task_run_id=None,
            status=TaskRun.Status.FAILED.value,
            last_message=None,
            runtime_s=runtime_s,
            skill_name=skill.name,
            skill_version=skill.version,
        )
    except BaseException as exc:
        # Cancellation / worker-shutdown / system-exit: re-raise so Temporal sees the
        # activity as failed. Post-collapse the bridge row's status flows from its
        # linked TaskRun (managed by MultiTurnSession), so we don't update anything
        # here directly. The self-heal path on the next coordinator tick reconciles
        # any bridge row whose TaskRun got stranded in IN_PROGRESS.
        runtime_s = time.monotonic() - started
        logger.warning(
            "signals_scout: run cancelled mid-flight",
            extra={
                "team_id": team_id,
                "run_id": str(run_id),
                "skill_name": skill.name,
                "exception_type": type(exc).__name__,
                "runtime_s": runtime_s,
            },
        )
        raise


async def _spawn_and_run(
    *,
    team: Team,
    config: SignalScoutConfig,
    run_id: Any,
    started_at: Any,
    skill: LoadedSkill,
    repository: str | None,
    verbose: bool,
) -> tuple[str, str]:
    """Spawn the sandbox, create the bridge row before the first turn, run the agent.

    Returns `(last_message, task_run_id)`.
    """
    user_id = await database_sync_to_async(resolve_user_id_for_team, thread_sensitive=False)(team.id)
    sandbox_env_id = await database_sync_to_async(get_or_create_signals_sandbox_env, thread_sensitive=False)(
        team.id,
        SIGNALS_SCOUT_SANDBOX_ENV_NAME,
        SandboxEnvironment.NetworkAccessLevel.TRUSTED,
    )
    # `repository` is None on the cadence path — v1 doesn't clone a repo into the
    # sandbox. The kwarg stays wired so the management command can still pass
    # `--repository` for ad-hoc local investigations; productionised repo access
    # is deferred (see implementation plan).
    context = CustomPromptSandboxContext(
        team_id=team.id,
        user_id=user_id,
        repository=repository,
        sandbox_environment_id=sandbox_env_id,
        # `signals_scout` is the harness's own scope posture: same scope content as
        # `read_only` (project reads + INTERNAL_SCOPES, including
        # `signal_scout_internal:write`) but reports `has_write_scopes=True` so the
        # MCP server doesn't enable read-only-mode tool filtering. Without that
        # opt-out, the MCP layer would categorically strip every tool annotated
        # `readOnlyHint: false` — including the agent's own `remember`, `forget`,
        # and `emit_finding` tools — even though the OAuth token does carry the
        # right scope to call them.
        posthog_mcp_scopes="signals_scout",
    )
    prompt = build_run_prompt(skill, run_id=str(run_id), team_id=team.id, started_at=started_at)
    logger.info(
        "signals_scout: spawning sandbox",
        extra={
            "team_id": team.id,
            "skill_name": skill.name,
            "skill_version": skill.version,
            "skill_id": skill.skill_id,
            "allowed_tools": skill.allowed_tools,
        },
    )

    async def _create_bridge_row(task_run: TaskRun) -> None:
        # Create the bridge row after the TaskRun exists but BEFORE the agent's first
        # turn runs (via MultiTurnSession's on_task_run_created hook). The scout is
        # single-turn and may call `signals-scout-emit-signal` during that first turn;
        # the emit endpoint resolves the run by id, so the row must already exist or
        # first-turn emits 404. Creating it here (not after `start()` returns) also keeps
        # the cross-link queryable mid-run and surviving both success and failure exits.
        await database_sync_to_async(_create_run_row, thread_sensitive=False)(
            run_id=run_id,
            task_run=task_run,
            team=team,
            config=config,
            skill=skill,
        )

    session, result = await MultiTurnSession.start(
        prompt=prompt,
        context=context,
        model=SignalScoutRunSummary,
        step_name=_step_name(skill),
        verbose=verbose,
        origin_product=Task.OriginProduct.SIGNALS_SCOUT,
        on_task_run_created=_create_bridge_row,
    )
    try:
        # Persist the agent's end-of-turn close-out so non-emitting runs leave a
        # discoverable trace for future-run dedupe. Failure paths skip this on
        # purpose — the bridge row keeps its empty default and the linked TaskRun
        # carries the error context.
        await database_sync_to_async(_finalize_run_summary, thread_sensitive=False)(
            run_id=run_id,
            summary=result.summary,
        )
        return result.summary, str(session.task_run.id)
    finally:
        await session.end()


def _get_team(team_id: int) -> Team:
    return Team.objects.select_related("organization").get(id=team_id)


def _resolve_config(team: Team) -> SignalScoutConfig:
    """Get-or-create the config row. Default is safe (enabled=False)."""
    config, _ = SignalScoutConfig.objects.unscoped().get_or_create(team=team)
    return config


def _has_running_run(*, team_id: int, config_id: str, skill_name: str) -> bool:
    # Locked on (team, skill_name) — different skills for the same team are allowed
    # to fan out, which is the whole point of `runs_per_tick > 1`. `scout_config_id`
    # is included to keep the filter selective on the per-team index. Status flows
    # from the linked TaskRun now that SignalScoutRun is just a bridge.
    return (
        SignalScoutRun.objects.unscoped()
        .filter(
            team_id=team_id,
            scout_config_id=config_id,
            skill_name=skill_name,
            task_run__status=TaskRun.Status.IN_PROGRESS,
        )
        .exists()
    )


# Stale rows past this multiple of `WORKFLOW_HARD_CEILING_S` are reconciled to FAILED.
# 2x is conservative: the workflow's `start_to_close_timeout` is exactly
# `WORKFLOW_HARD_CEILING_S`, so an activity that completed normally — success, failure,
# or Temporal-side timeout — would never be running longer than that. 2x is the slack
# we leave for the row's own update path racing the timeout signal; anything older is
# orphaned beyond reasonable doubt.
_STALE_RUN_MULTIPLIER = 2


def _self_heal_stale_runs(team_id: int, skill_name: str) -> None:
    """Reconcile RUNNING rows older than `_STALE_RUN_MULTIPLIER * WORKFLOW_HARD_CEILING_S`
    to FAILED.

    Catches rows orphaned by worker shutdown, sandbox crash, or async cancellation that
    bypassed the activity's cleanup path. The threshold is anchored to the workflow's
    actual hard timeout (`DEFAULT_MAX_RUNTIME_S + ACTIVITY_SLACK_S`), not the per-run
    `metadata.limits.max_runtime_s` — that override only governs the harness's in-activity
    poll loop, while the Temporal `start_to_close_timeout` is fixed. Anchoring to the
    workflow ceiling keeps the self-heal time uniform across teams; a team setting
    `max_runtime_s = 7200s` doesn't get hours of false-blocking from an orphan that
    Temporal would have killed at ~31 minutes anyway.

    Keyed on `(team_id, skill_name)` to mirror the partial unique index — keying on
    `scout_config_id` would leave orphaned rows with a stale or null FK un-healable while
    the same constraint kept blocking new inserts for the same (team, skill).

    Idempotent: safe to call from any number of concurrent coordinator activities.
    """
    candidates = SignalScoutRun.objects.filter(
        team_id=team_id,
        skill_name=skill_name,
        status=SignalScoutRun.Status.RUNNING,
    ).only("id", "started_at", "metadata")
    now = timezone.now()
    threshold_s = _STALE_RUN_MULTIPLIER * WORKFLOW_HARD_CEILING_S
    for run in candidates:
        age_s = (now - run.started_at).total_seconds()
        if age_s <= threshold_s:
            continue
        SignalScoutRun.objects.filter(id=run.id, status=SignalScoutRun.Status.RUNNING).update(
            status=SignalScoutRun.Status.FAILED,
            completed_at=now,
            summary=(
                f"Run row auto-healed: status=RUNNING for {age_s:.0f}s "
                f"(threshold {threshold_s}s = {_STALE_RUN_MULTIPLIER}x WORKFLOW_HARD_CEILING_S). "
                f"Worker / sandbox likely died without the cleanup path running."
            ),
        )
        logger.warning(
            "signals_scout: self-healed stale running run",
            extra={
                "team_id": team_id,
                "run_id": str(run.id),
                "age_s": age_s,
                "threshold_s": threshold_s,
            },
        )


def _create_run_row(
    *,
    run_id: Any,
    task_run: TaskRun,
    team: Team,
    config: SignalScoutConfig,
    skill: LoadedSkill,
) -> SignalScoutRun:
    return SignalScoutRun.objects.unscoped().create(
        id=run_id,
        task_run=task_run,
        team=team,
        scout_config=config,
        skill_name=skill.name,
        skill_version=skill.version,
    )


def _finalize_run_summary(*, run_id: Any, summary: str) -> None:
    # Targeted UPDATE rather than `.save()` — the row's other fields are untouched
    # by the agent's close-out, and `update()` skips the full model refresh.
    SignalScoutRun.objects.unscoped().filter(id=run_id).update(summary=summary)


def _step_name(skill: LoadedSkill) -> str:
    # Surfaces in the Task title and S3 log prefix. Keep terse — the sandbox truncates.
    safe = skill.name.replace(" ", "_")[:40]
    return f"signals_scout:{safe}"
