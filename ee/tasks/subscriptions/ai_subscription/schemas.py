from typing import Literal

from pydantic import BaseModel, Field


class QueryPlanStep(BaseModel):
    """One step in a query plan — a single HogQL query to run.

    MVP supports only HogQL; typed Trends/Funnels/Retention queries are a follow-up.
    """

    description: str = Field(..., max_length=500, description="One-sentence rationale for running this query.")
    query_type: Literal["hogql"] = Field("hogql", description="MVP: always 'hogql'.")
    # The query carries its own timeframe in the HogQL `WHERE timestamp >= …` clause; there is no
    # separate per-step window field, which would just be a second source of truth the executor ignores.
    hogql: str = Field(..., max_length=5000, description="A HogQL SELECT statement scoped to the team's events.")


class QueryPlan(BaseModel):
    """A short, bounded plan of queries that answer the user's prompt."""

    overall_intent: str = Field(
        ...,
        max_length=500,
        description="Plain-English summary of what the report will tell the user.",
    )
    # Three well-chosen queries cover almost any report while keeping worst-case wall-clock
    # (each step can retry) inside the caller's delivery budget.
    steps: list[QueryPlanStep] = Field(..., min_length=1, max_length=3)


class EnrichedPromptSpec(BaseModel):
    """Everything the synthesis step needs to write the final markdown report.

    Built and consumed entirely within ``generate_ai_report`` — it never crosses a Temporal activity
    boundary, so it isn't subject to the ~2 MiB payload limit. Only the final (short, ~400-word)
    markdown report and the subscription id flow back through the workflow. The field caps above keep
    the in-process spec bounded regardless.
    """

    cleaned_prompt: str
    context_blob: str
    plan: QueryPlan


class HogQLFix(BaseModel):
    """LLM response when asked to rewrite a HogQL query that failed to parse/execute."""

    fixed_hogql: str = Field(
        ...,
        description="A single, flat HogQL SELECT statement that addresses the original step intent.",
    )
