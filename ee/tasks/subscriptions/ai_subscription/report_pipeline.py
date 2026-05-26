"""AI-subscription report pipeline: turn a user's prompt into a markdown report.

Three stages run in order — *plan* (an LLM picks up to three HogQL queries), *execute* (run them,
with a bounded per-step fix-retry), and *synthesize* (an LLM writes the markdown). The pipeline owns
none of its side effects: persistence and delivery are the caller's job.

This is purpose-built for AI subscriptions, not a general primitive — every input and prompt is
subscription-shaped. Generalising it is a future exercise, not a present claim.
"""

import re
import uuid
import asyncio
from datetime import UTC, datetime
from typing import Optional, Union

import structlog

from posthog.schema import AssistantHogQLQuery

from posthog.hogql.errors import ExposedHogQLError, InternalHogQLError

from posthog.exceptions_capture import capture_exception
from posthog.models import Team, User
from posthog.ph_client import ph_scoped_capture
from posthog.sync import database_sync_to_async
from posthog.text_sanitization import strip_llm_framing_markers

from ee.hogai.context.insight.query_executor import AssistantQueryExecutor
from ee.hogai.llm import MaxChatOpenAI
from ee.hogai.tool_errors import MaxToolRetryableError
from ee.tasks.subscriptions.ai_subscription.prompts import AI_SUBSCRIPTION_SYNTHESIS_PROMPT, HOGQL_FIX_PROMPT
from ee.tasks.subscriptions.ai_subscription.schemas import EnrichedPromptSpec, HogQLFix, QueryPlanStep
from ee.tasks.subscriptions.ai_subscription.spec_generator import (
    DEFAULT_PLANNER_MODEL,
    DEFAULT_SYNTHESIS_MODEL,
    PromptRejectedError,
    build_enriched_prompt,
)

logger = structlog.get_logger(__name__)

# Wall-clock bounds for the in-band LLM + HogQL pipeline. The caller's outer deadline (Temporal
# activity timeout for scheduled, request timeout for ad-hoc) is the ultimate cap; these prevent a
# single slow upstream from soaking it.
_SYNTHESIS_LLM_TIMEOUT_SECONDS = 90.0
_HOGQL_STEP_TIMEOUT_SECONDS = 60.0
# Backstop length cap on a single step's formatted results before they enter the synthesis prompt.
# The executor already truncates; this is defense-in-depth against a giant value.
_QUERY_RESULT_MAX_CHARS = 50_000

# Per-step query-fix budget: the planner occasionally emits HogQL that fails to parse, so we feed the
# error back and ask for a rewrite rather than dropping the step. Worst case per step is one original
# run plus _MAX_QUERY_FIX_RETRIES × (fix LLM + rerun); steps run concurrently via asyncio.gather.
_MAX_QUERY_FIX_RETRIES = 2
_FIX_LLM_TIMEOUT_SECONDS = 30.0

# Errors signalling "the query itself is wrong" — rewriting may help. Everything else (timeouts, infra
# failures, generic exceptions) falls through to the "_Query failed_" placeholder without retrying,
# since a different SELECT won't fix a ClickHouse outage or a heartbeat timeout.
_RETRYABLE_QUERY_ERRORS: tuple[type[BaseException], ...] = (
    MaxToolRetryableError,
    ExposedHogQLError,
    InternalHogQLError,
)


class AiReportStageError(Exception):
    """Wraps a transient pipeline failure with the stage that produced it, so the delivery error
    record distinguishes a planner timeout from a synthesis timeout. ``PromptRejectedError`` is
    deliberately not wrapped — callers catch it by type to auto-disable / return a 400."""

    def __init__(self, stage: str, original: BaseException) -> None:
        self.stage = stage
        self.original = original
        super().__init__(f"AI report failed at {stage} stage: {original}")


async def generate_ai_report(
    *,
    team: Team,
    user: User,
    prompt: Optional[str],
    window_days: int,
    trace_correlation_id: Optional[Union[int, str]] = None,
) -> str:
    """Run plan → execute → synthesize and return the markdown report.

    Async so each caller (a Temporal activity, an API view) owns its own dispatch — no assumption
    about which thread or event loop we're on. ``PromptRejectedError`` marks a permanent input
    failure; transient LLM/HogQL failures surface as ``AiReportStageError`` carrying the stage.
    """
    if user is None:
        raise PromptRejectedError("AI report must have a user to run.")

    spec = await _plan(team=team, user=user, prompt=prompt, window_days=window_days, trace_id=trace_correlation_id)
    rendered_results, failed_count = await _execute_plan(spec, team, user, trace_correlation_id)
    report = await _synthesize(spec, rendered_results, team, user, trace_correlation_id)
    # Emit the coverage signal only once the report actually exists, so the "generated" event isn't
    # recorded for a run that failed in synthesis.
    await _capture_report_quality(spec, failed_count, team, user, trace_correlation_id)
    return report


async def _plan(
    *, team: Team, user: User, prompt: Optional[str], window_days: int, trace_id: Optional[Union[int, str]]
) -> EnrichedPromptSpec:
    """Stage 1 — sanitize the prompt, gather project context, and ask the planner for a query plan.

    ``build_enriched_prompt`` is sync (ORM + ClickHouse + a blocking LLM call), so it runs off the event
    loop via ``database_sync_to_async`` — which (unlike a bare thread) closes stale Django connections
    around the call, matching how the rest of the Temporal subscription code dispatches ORM work.
    """
    try:
        return await database_sync_to_async(build_enriched_prompt, thread_sensitive=False)(
            team=team,
            user=user,
            prompt=prompt,
            window_days=window_days,
            trace_correlation_id=trace_id,
        )
    except PromptRejectedError:
        raise
    except Exception as exc:
        raise AiReportStageError("planner", exc) from exc


async def _execute_plan(
    spec: EnrichedPromptSpec,
    team: Team,
    user: User,
    trace_correlation_id: Optional[Union[int, str]],
) -> tuple[list[str], int]:
    """Stage 2 — run every planned HogQL query concurrently, returning rendered blocks and a failure
    count. A failed step degrades to a placeholder rather than failing the whole report."""
    try:
        return await _run_steps(spec, team, user, trace_correlation_id)
    except Exception as exc:
        # Per-step failures degrade to placeholders inside `run_step`, so this only catches a failure
        # in the orchestration itself (e.g. the executor failing to construct) — surface it as a
        # "query"-stage error rather than a bare exception in the delivery record.
        raise AiReportStageError("query", exc) from exc


async def _synthesize(
    spec: EnrichedPromptSpec,
    rendered_results: list[str],
    team: Team,
    user: User,
    trace_correlation_id: Optional[Union[int, str]],
) -> str:
    """Stage 3 — ask the synthesis LLM to write the markdown report from the prompt + query results."""
    posthog_properties: dict[str, Union[str, int]] = {
        "feature": "ai_subscription",
        "stage": "synthesis",
        "trace_id": str(uuid.uuid4()),
    }
    if trace_correlation_id is not None:
        posthog_properties["subscription_id"] = trace_correlation_id

    chat = MaxChatOpenAI(
        model=DEFAULT_SYNTHESIS_MODEL,
        temperature=0.2,
        timeout=_SYNTHESIS_LLM_TIMEOUT_SECONDS,
        user=user,
        team=team,
        # AI report LLM spend is billable — usage counts against the team's AI credits.
        billable=True,
        posthog_properties=posthog_properties,
    )

    try:
        # `database_sync_to_async` (not bare `to_thread`): MaxChatOpenAI reads billing/quota from the
        # ORM, so we want Django's connection lifecycle around the call.
        result = await database_sync_to_async(chat.invoke, thread_sensitive=False)(
            [
                ("system", AI_SUBSCRIPTION_SYNTHESIS_PROMPT),
                ("human", _compose_synthesis_human_message(spec, rendered_results)),
            ],
        )
    except Exception as exc:
        raise AiReportStageError("synthesis", exc) from exc
    content = result.content if hasattr(result, "content") else str(result)
    return content if isinstance(content, str) else str(content)


def _compose_synthesis_human_message(spec: EnrichedPromptSpec, rendered_results: list[str]) -> str:
    results_block = "\n".join(rendered_results) if rendered_results else "_No query results were available._"
    # `overall_intent` is planner LLM output derived from user-controlled context, so it gets the same
    # framing-marker treatment as the query results and lives inside its own envelope — it must not be
    # able to inject instruction-shaped text into the synthesis prompt.
    safe_intent = strip_llm_framing_markers(spec.plan.overall_intent, max_len=500)
    return (
        f"<user_prompt>\n{spec.cleaned_prompt}\n</user_prompt>\n\n"
        f"<project_context>\n{spec.context_blob}\n</project_context>\n\n"
        f"<plan_intent>\n{safe_intent}\n</plan_intent>\n\n"
        f"<query_results>\n{results_block}\n</query_results>"
    )


async def _run_steps(
    spec: EnrichedPromptSpec,
    team: Team,
    user: User,
    trace_correlation_id: Optional[Union[int, str]],
) -> tuple[list[str], int]:
    # Pass `user` so executor-internal permission checks and tracing match other call sites
    # (see `ee/hogai/context/insight/query_executor.py` callers in master).
    executor = AssistantQueryExecutor(team, datetime.now(tz=UTC), user=user)

    async def run_step(step: QueryPlanStep) -> tuple[str, bool]:
        current_hogql = step.hogql
        last_exc: Optional[BaseException] = None
        # `step.description` is planner LLM output (derived from user-controlled context), so it gets the
        # same framing-marker stripping as the results it heads — it must not break the <query_results>
        # envelope from inside its own `###` heading.
        safe_description = strip_llm_framing_markers(step.description, max_len=500)

        # attempt 0 = original query; subsequent attempts = LLM-fixed rewrites.
        for attempt in range(_MAX_QUERY_FIX_RETRIES + 1):
            try:
                query = AssistantHogQLQuery(query=current_hogql)
                formatted, _ = await asyncio.wait_for(
                    executor.arun_and_format_query(query),
                    timeout=_HOGQL_STEP_TIMEOUT_SECONDS,
                )
                # Result VALUES are attacker-influenceable: anyone with a public project token can
                # ingest events with crafted property values. Strip LLM framing markers so a poisoned
                # value can't break out of the <query_results> envelope into instruction-shaped text.
                safe_formatted = strip_llm_framing_markers(formatted, _QUERY_RESULT_MAX_CHARS)
                return (f"### {safe_description}\n\n{safe_formatted}", True)
            except Exception as exc:
                last_exc = exc
                if attempt >= _MAX_QUERY_FIX_RETRIES or not _is_retryable_query_error(exc):
                    break
                logger.info(
                    "ai_report.query_fix_attempt",
                    trace_correlation_id=trace_correlation_id,
                    step_description=safe_description,
                    attempt=attempt + 1,
                    max_retries=_MAX_QUERY_FIX_RETRIES,
                    error_type=type(exc).__name__,
                )
                fixed = await _arequest_hogql_fix(
                    original_hogql=current_hogql,
                    # Only forward the message for errors built for user exposure; an InternalHogQLError
                    # can echo cluster URLs / internal table names, which must not reach the LLM provider.
                    error_message=str(exc) if isinstance(exc, ExposedHogQLError) else type(exc).__name__,
                    step_description=safe_description,
                    team=team,
                    user=user,
                    trace_correlation_id=trace_correlation_id,
                )
                if not fixed or fixed.strip() == current_hogql.strip():
                    # LLM returned nothing useful or the same query — no point looping further.
                    break
                current_hogql = fixed

        # Exhausted retries (or non-retryable error). Fall through to the placeholder.
        logger.warning(
            "ai_report.query_failed",
            trace_correlation_id=trace_correlation_id,
            step_description=safe_description,
            exc_info=last_exc,
        )
        if last_exc is not None:
            capture_exception(last_exc, {"trace_correlation_id": trace_correlation_id, "stage": "query"})
        # Pass only the type, not the message — ClickHouse errors can echo team-scoped identifiers
        # (cluster URL, table names) that shouldn't ship into the synthesis prompt (and thus the report).
        type_name = type(last_exc).__name__ if last_exc is not None else "UnknownError"
        return (f"### {safe_description}\n\n_Query failed: {type_name}_", False)

    step_results: list[tuple[str, bool]] = await asyncio.gather(*(run_step(step) for step in spec.plan.steps))
    rendered = [text for text, _ in step_results]
    failed_count = sum(1 for _, ok in step_results if not ok)
    return rendered, failed_count


def _is_retryable_query_error(exc: BaseException) -> bool:
    return isinstance(exc, _RETRYABLE_QUERY_ERRORS)


async def _arequest_hogql_fix(
    *,
    original_hogql: str,
    error_message: str,
    step_description: str,
    team: Team,
    user: User,
    trace_correlation_id: Optional[Union[int, str]],
) -> Optional[str]:
    """Ask the planner-class LLM to rewrite a failing HogQL query. Returns None on failure."""
    posthog_properties: dict[str, Union[str, int]] = {"feature": "ai_subscription", "stage": "query_fix"}
    if trace_correlation_id is not None:
        posthog_properties["subscription_id"] = trace_correlation_id

    llm = MaxChatOpenAI(
        model=DEFAULT_PLANNER_MODEL,
        temperature=0,
        timeout=_FIX_LLM_TIMEOUT_SECONDS,
        user=user,
        team=team,
        # Billable, matching the planner/synthesis calls — the whole pipeline's LLM usage is charged
        # to the team's AI credits.
        billable=True,
        posthog_properties=posthog_properties,
    ).with_structured_output(HogQLFix, method="json_schema", include_raw=False)

    # Single-pass substitution — see spec_generator.generate_query_plan for the rationale behind not
    # using chained .replace().
    substitutions = {
        "description": step_description,
        "error": error_message,
        "original_hogql": original_hogql,
    }
    rendered = re.sub(
        r"\{\{\{(\w+)\}\}\}",
        lambda m: substitutions.get(m.group(1), m.group(0)),
        HOGQL_FIX_PROMPT,
    )

    try:
        result = await database_sync_to_async(llm.invoke, thread_sensitive=False)([("system", rendered)])
    except Exception as exc:
        logger.warning(
            "ai_report.query_fix_llm_failed",
            trace_correlation_id=trace_correlation_id,
            step_description=step_description,
            error_type=type(exc).__name__,
        )
        return None

    if not isinstance(result, HogQLFix):
        return None
    fixed = result.fixed_hogql.strip()
    return fixed or None


async def _capture_report_quality(
    spec: EnrichedPromptSpec,
    failed_count: int,
    team: Team,
    user: User,
    trace_correlation_id: Optional[Union[int, str]],
) -> None:
    """Emit a proactive quality signal per report so we can track query coverage (how many planned
    steps actually returned data) without waiting for a user to report a bad report. Analytics must
    never break delivery, so failures here are swallowed."""
    total_steps = len(spec.plan.steps)
    if failed_count:
        logger.warning(
            "ai_report.delivered_degraded",
            trace_correlation_id=trace_correlation_id,
            failed_steps=failed_count,
            total_steps=total_steps,
        )

    def _emit() -> None:
        with ph_scoped_capture() as capture:
            capture(
                distinct_id=user.distinct_id,
                event="ai_subscription_report_generated",
                properties={
                    "feature": "ai_subscription",
                    "subscription_id": trace_correlation_id,
                    "team_id": team.id,
                    "total_steps": total_steps,
                    "failed_steps": failed_count,
                    # 1.0 = every planned query returned data; lower means the plan/queries under-served
                    # the prompt. A drifting coverage rate is the proactive signal to tune the prompts.
                    "query_coverage": (total_steps - failed_count) / total_steps if total_steps else 0.0,
                    "degraded": bool(failed_count),
                },
            )

    try:
        await asyncio.to_thread(_emit)
    except Exception:
        logger.warning("ai_report.quality_capture_failed", trace_correlation_id=trace_correlation_id, exc_info=True)


__all__ = ["generate_ai_report", "AiReportStageError"]
