import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from posthog.hogql.errors import ExposedHogQLError

from ee.tasks.subscriptions.ai_subscription.report_pipeline import (
    AiReportStageError,
    _arequest_hogql_fix,
    _run_steps,
    generate_ai_report,
)
from ee.tasks.subscriptions.ai_subscription.schemas import EnrichedPromptSpec, HogQLFix, QueryPlan, QueryPlanStep
from ee.tasks.subscriptions.ai_subscription.spec_generator import PromptRejectedError

_RP = "ee.tasks.subscriptions.ai_subscription.report_pipeline"


def _spec(steps: int = 1) -> EnrichedPromptSpec:
    return EnrichedPromptSpec(
        cleaned_prompt="p",
        context_blob="c",
        plan=QueryPlan(
            overall_intent="i",
            steps=[QueryPlanStep(description=f"s{n}", hogql="SELECT 1") for n in range(steps)],
        ),
    )


async def test_user_none_raises_prompt_rejected() -> None:
    with pytest.raises(PromptRejectedError):
        await generate_ai_report(team=MagicMock(), user=None, prompt="x", window_days=7)


@patch(f"{_RP}.build_enriched_prompt", side_effect=PromptRejectedError("empty"))
async def test_prompt_rejected_propagates_unwrapped(_mock_bep: object) -> None:
    # PromptRejectedError must NOT be wrapped as AiReportStageError — callers catch it by type.
    with pytest.raises(PromptRejectedError):
        await generate_ai_report(team=MagicMock(), user=MagicMock(), prompt="", window_days=7)


@patch(f"{_RP}.build_enriched_prompt", side_effect=RuntimeError("planner boom"))
async def test_planner_failure_wrapped_with_stage(_mock_bep: object) -> None:
    with pytest.raises(AiReportStageError) as exc_info:
        await generate_ai_report(team=MagicMock(), user=MagicMock(), prompt="x", window_days=7)
    assert exc_info.value.stage == "planner"


@patch(f"{_RP}._capture_report_quality", new_callable=AsyncMock)
@patch(f"{_RP}.MaxChatOpenAI")
@patch(f"{_RP}._run_steps", new_callable=AsyncMock)
@patch(f"{_RP}.build_enriched_prompt")
async def test_degraded_report_still_synthesizes(
    mock_bep: MagicMock, mock_run: AsyncMock, mock_chat: MagicMock, _mock_capture: AsyncMock
) -> None:
    # One step failed (failed_count=1) but the report still ships — graceful degradation.
    mock_bep.return_value = _spec(steps=1)
    mock_run.return_value = (["### s0\n\n_Query failed: ExposedHogQLError_"], 1)
    mock_chat.return_value.invoke.return_value = MagicMock(content="# Weekly report")

    result = await generate_ai_report(team=MagicMock(), user=MagicMock(), prompt="x", window_days=7)

    assert result == "# Weekly report"


@patch(f"{_RP}._capture_report_quality", new_callable=AsyncMock)
@patch(f"{_RP}.MaxChatOpenAI")
@patch(f"{_RP}._run_steps", new_callable=AsyncMock)
@patch(f"{_RP}.build_enriched_prompt")
async def test_synthesis_failure_wrapped_with_stage(
    mock_bep: MagicMock, mock_run: AsyncMock, mock_chat: MagicMock, _mock_capture: AsyncMock
) -> None:
    mock_bep.return_value = _spec(steps=1)
    mock_run.return_value = (["### s0\n\nok"], 0)
    mock_chat.return_value.invoke.side_effect = RuntimeError("synth boom")

    with pytest.raises(AiReportStageError) as exc_info:
        await generate_ai_report(team=MagicMock(), user=MagicMock(), prompt="x", window_days=7)
    assert exc_info.value.stage == "synthesis"


@patch(f"{_RP}.MaxChatOpenAI")
async def test_request_hogql_fix_returns_fixed_query(mock_chat: MagicMock) -> None:
    structured = mock_chat.return_value.with_structured_output.return_value
    structured.invoke.return_value = HogQLFix(fixed_hogql="SELECT 2")
    result = await _arequest_hogql_fix(
        original_hogql="SELECT 1",
        error_message="boom",
        step_description="d",
        team=MagicMock(),
        user=MagicMock(),
        trace_correlation_id=None,
    )
    assert result == "SELECT 2"


@patch(f"{_RP}.MaxChatOpenAI")
async def test_request_hogql_fix_returns_none_on_wrong_type(mock_chat: MagicMock) -> None:
    structured = mock_chat.return_value.with_structured_output.return_value
    structured.invoke.return_value = "not a HogQLFix"
    result = await _arequest_hogql_fix(
        original_hogql="SELECT 1",
        error_message="boom",
        step_description="d",
        team=MagicMock(),
        user=MagicMock(),
        trace_correlation_id=None,
    )
    assert result is None


@patch(f"{_RP}.AssistantQueryExecutor")
async def test_run_steps_non_retryable_error_degrades_to_placeholder(mock_executor_cls: MagicMock) -> None:
    mock_executor_cls.return_value.arun_and_format_query = AsyncMock(side_effect=RuntimeError("boom"))
    rendered, failed = await _run_steps(_spec(steps=1), MagicMock(), MagicMock(), None)
    assert failed == 1
    assert "_Query failed:" in rendered[0]


@patch(f"{_RP}._arequest_hogql_fix", new_callable=AsyncMock)
@patch(f"{_RP}.AssistantQueryExecutor")
async def test_run_steps_retries_then_succeeds(mock_executor_cls: MagicMock, mock_fix: AsyncMock) -> None:
    # First attempt raises a retryable HogQL error, the LLM fix yields a new query, the rerun succeeds.
    mock_executor_cls.return_value.arun_and_format_query = AsyncMock(
        side_effect=[ExposedHogQLError("bad query"), ("formatted table", None)]
    )
    mock_fix.return_value = "SELECT fixed"
    rendered, failed = await _run_steps(_spec(steps=1), MagicMock(), MagicMock(), None)
    assert failed == 0
    assert "formatted table" in rendered[0]
    mock_fix.assert_awaited_once()


@patch(f"{_RP}._arequest_hogql_fix", new_callable=AsyncMock)
@patch(f"{_RP}.AssistantQueryExecutor")
async def test_run_steps_breaks_early_when_fix_returns_same_query(
    mock_executor_cls: MagicMock, mock_fix: AsyncMock
) -> None:
    # The fix LLM echoes the original query back — re-running it is pointless, so we must stop and
    # degrade rather than burn the retry budget on an identical query.
    mock_executor_cls.return_value.arun_and_format_query = AsyncMock(side_effect=ExposedHogQLError("bad query"))
    mock_fix.return_value = "SELECT 1"  # identical to QueryPlanStep.hogql in _spec()
    rendered, failed = await _run_steps(_spec(steps=1), MagicMock(), MagicMock(), None)
    assert failed == 1
    assert "_Query failed:" in rendered[0]
    # Executor ran exactly once (no rerun of the identical fixed query); the fix was requested once.
    assert mock_executor_cls.return_value.arun_and_format_query.await_count == 1
    mock_fix.assert_awaited_once()
