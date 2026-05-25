"""Eager web analytics precompute — team selection + baseline pre-warming.

Two layered concerns live here:

1. **Team selection** (`web_analytics_eager_precompute_team_selection`) — a
   daily Dagster asset that mines `PreaggregationJob` for distinct team IDs
   with recent activity and writes the enrolled set back to the
   `WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS` Constance setting.
   Two callers read that setting:

   - `can_use_eager_precompute` (in `web_analytics_lazy_precompute.py`) — the
     query-path gate that lets enrolled teams bypass the lazy rollout (org-FF
     + per-query opt-in).
   - `get_teams_enabled_for_web_analytics_cache_warming` (in `cache_warming.py`)
     — the cache warmer unions this set with its static allowlist and replays
     enrolled teams' actual queries hourly. That replay hits the eager gate
     and populates the lazy precompute cache without any synthesized fan-out.

2. **Baseline warming** (`web_analytics_eager_baseline_warming_job`) — a daily
   Dagster job that runs a small fixed matrix of high-impact queries
   (`last 30d` overview + top-3 breakdowns) for every enrolled team. This is
   belt-and-suspenders for two cases the cache-warmer replay misses:

   - Newly enrolled teams (via FORCED_TEAM_IDS) whose query history hasn't
     yet hit `WEB_ANALYTICS_WARMING_MIN_QUERY_COUNT`.
   - The head of the distribution — `last 30d` + InitialReferringDomain / Page
     / DeviceType — which empirical query-log analysis showed covers ~91% of
     user-facing web analytics queries. Guaranteeing those are always warm is
     the difference between "fast on a fresh load" and "fast only after the
     user runs the query once."

   Baselines run via `runner.run()`, so the same eager / lazy / v2 / raw
   chain users hit also serves the pre-warming. Successful runs populate both
   the lazy precompute cache (for overview) and the Django response cache.
"""

from datetime import timedelta
from typing import Any

from django.utils import timezone as django_timezone

import dagster
import structlog
from prometheus_client import Counter, Gauge

from posthog.hogql.constants import LimitContext

from posthog.clickhouse.query_tagging import Feature, Product, tag_queries
from posthog.dags.common import JobOwners
from posthog.event_usage import EventSource
from posthog.hogql_queries.query_runner import get_query_runner
from posthog.models import Team
from posthog.models.instance_setting import get_instance_setting, set_instance_setting

from products.analytics_platform.backend.models.preaggregation_job import PreaggregationJob

logger = structlog.get_logger(__name__)


EAGER_PRECOMPUTE_AUTO_SELECTED_TEAMS = Gauge(
    "web_analytics_eager_precompute_auto_selected_teams",
    "Distinct teams with PreaggregationJob activity in the last LOOKBACK_DAYS.",
)
EAGER_PRECOMPUTE_FORCED_TEAMS = Gauge(
    "web_analytics_eager_precompute_forced_teams",
    "Teams added by FORCED_TEAM_IDS override beyond the auto-selected set.",
)
EAGER_PRECOMPUTE_FINAL_TEAMS = Gauge(
    "web_analytics_eager_precompute_final_teams",
    "Final enrolled team count post (auto ∪ forced) − blocked.",
)

EAGER_PRECOMPUTE_BASELINE_WARMED = Counter(
    "web_analytics_eager_precompute_baseline_warmed_total",
    "Baseline queries successfully warmed per cycle.",
    ["query_kind"],
)
EAGER_PRECOMPUTE_BASELINE_FAILED = Counter(
    "web_analytics_eager_precompute_baseline_failed_total",
    "Baseline queries that raised during warming.",
    ["query_kind", "error_type"],
)


_AUTO_SELECTED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS"
_FORCED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS"
_BLOCKED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS"


# Baseline matrix: a fixed `last 30d` window over the highest-impact query
# shapes. From `evaluating-web-analytics-performance` analysis, `last 30d` +
# these three breakdowns covers ~91% of user-facing web analytics queries.
# Tuned to a small constant rather than a per-team mined set — the cache
# warmer covers the long tail of team-specific queries; this op covers the
# head every enrolled team is likely to hit.
_BASELINE_DAYS = 30
_BASELINE_BREAKDOWNS = ("InitialReferringDomain", "Page", "DeviceType")


def _select_eager_teams() -> list[int]:
    """Auto-select eager precompute teams: distinct team_ids in PreaggregationJob.

    Any team that has at least one PreaggregationJob row over the last
    `WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS` is auto-selected. Forced
    teams join unconditionally; blocked teams are removed.

    Note: PreaggregationJob does not track which target table a job is for, so
    this auto-selects teams that have ANY preagg activity — including the
    `experiment_*` and `preaggregation_results` tables. In practice this is
    fine: an experiment-only team with no web analytics queries simply has
    nothing for the cache warmer to replay, so eager enrollment is a no-op
    for them. If table-level filtering becomes desirable, add a `target_table`
    column to PreaggregationJob in a follow-up.
    """
    lookback_days = get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS") or 7
    forced = set(get_instance_setting(_FORCED_SETTING) or [])
    blocked = set(get_instance_setting(_BLOCKED_SETTING) or [])

    cutoff = django_timezone.now() - timedelta(days=lookback_days)
    auto_team_ids = set(
        PreaggregationJob.objects.filter(created_at__gte=cutoff).values_list("team_id", flat=True).distinct()
    )

    final = sorted((auto_team_ids | forced) - blocked)

    EAGER_PRECOMPUTE_AUTO_SELECTED_TEAMS.set(len(auto_team_ids))
    EAGER_PRECOMPUTE_FORCED_TEAMS.set(len(forced - auto_team_ids))
    EAGER_PRECOMPUTE_FINAL_TEAMS.set(len(final))

    logger.info(
        "web_analytics_eager_precompute_team_selection",
        auto_selected=len(auto_team_ids),
        forced_added=len(forced - auto_team_ids),
        blocked_removed=len(blocked & auto_team_ids),
        final_count=len(final),
        lookback_days=lookback_days,
    )

    return final


def _baseline_queries() -> list[dict[str, Any]]:
    """Return the fixed baseline matrix used by the warming op.

    Same `filterTestAccounts=True` default the Web Analytics dashboard sends.
    `limit=10` matches the dashboard's per-tile pagination, so the warmed
    response cache exactly matches what users request.
    """
    date_range = {"date_from": f"-{_BASELINE_DAYS}d"}
    queries: list[dict[str, Any]] = [
        {
            "kind": "WebOverviewQuery",
            "dateRange": date_range,
            "properties": [],
            "filterTestAccounts": True,
        },
    ]
    for breakdown in _BASELINE_BREAKDOWNS:
        queries.append(
            {
                "kind": "WebStatsTableQuery",
                "breakdownBy": breakdown,
                "dateRange": date_range,
                "properties": [],
                "filterTestAccounts": True,
                "limit": 10,
            }
        )
    return queries


def _warm_baseline_for_team(context: dagster.OpExecutionContext, team: Team) -> tuple[int, int]:
    """Run the baseline matrix for one team. Returns (warmed, failed)."""
    warmed = 0
    failed = 0
    for query in _baseline_queries():
        kind = str(query.get("kind"))
        breakdown = query.get("breakdownBy")
        label = f"{kind}:{breakdown}" if breakdown else kind
        try:
            runner = get_query_runner(query=query, team=team, limit_context=LimitContext.QUERY_ASYNC)
            tag_queries(
                team_id=team.pk,
                trigger="webAnalyticsEagerBaselineWarming",
                feature=Feature.CACHE_WARMUP,
                product=Product.WEB_ANALYTICS,
            )
            runner.run(analytics_props={"source": EventSource.CACHE_WARMING})
            EAGER_PRECOMPUTE_BASELINE_WARMED.labels(query_kind=label).inc()
            warmed += 1
        except Exception as exc:
            EAGER_PRECOMPUTE_BASELINE_FAILED.labels(query_kind=label, error_type=type(exc).__name__).inc()
            context.log.exception(f"baseline warming failed for team={team.pk} query={label}")
            failed += 1
    return warmed, failed


@dagster.asset(
    name="web_analytics_eager_precompute_team_selection",
    group_name="web_analytics_v2",
    tags={"owner": JobOwners.TEAM_WEB_ANALYTICS.value},
)
def web_analytics_eager_precompute_team_selection(
    context: dagster.AssetExecutionContext,
) -> dagster.MaterializeResult:
    """Daily asset: refresh the eager-enrolled team set.

    Writes the computed list to the `AUTO_SELECTED_TEAM_IDS` Constance setting.
    Both the query-path eager gate and the cache warmer read that setting.
    """
    team_ids = _select_eager_teams()
    set_instance_setting(_AUTO_SELECTED_SETTING, team_ids)

    context.log.info(f"Eager precompute team selection: {len(team_ids)} teams enrolled")
    return dagster.MaterializeResult(
        metadata={
            "team_count": len(team_ids),
            "team_ids": str(team_ids),
        }
    )


@dagster.schedule(
    cron_schedule="0 6 * * *",  # daily at 06:00 UTC, before peak dashboard traffic
    target=web_analytics_eager_precompute_team_selection,
    execution_timezone="UTC",
    tags={"owner": JobOwners.TEAM_WEB_ANALYTICS.value},
)
def web_analytics_eager_precompute_team_selection_schedule(
    context: dagster.ScheduleEvaluationContext,
) -> "dagster.RunRequest | dagster.SkipReason":
    return dagster.RunRequest()


@dagster.op
def warm_eager_baseline_op(context: dagster.OpExecutionContext) -> dict[str, int]:
    """Run the baseline matrix against every eager-enrolled team.

    Reads the same enrolled set the query-path gate sees, so a team blocked or
    de-enrolled between the team-selection asset and this op is correctly
    skipped. Failures per (team, query) are caught and logged so one bad
    team doesn't poison the rest of the run.
    """
    from products.web_analytics.backend.hogql_queries.web_analytics_lazy_precompute import get_eager_enrolled_team_ids

    team_ids = sorted(get_eager_enrolled_team_ids())
    context.log.info(f"Eager baseline warming: {len(team_ids)} teams enrolled")

    warmed = 0
    failed = 0
    for team_id in team_ids:
        try:
            team = Team.objects.get(pk=team_id)
        except Team.DoesNotExist:
            context.log.warning(f"team_id={team_id} not found, skipping")
            continue

        team_warmed, team_failed = _warm_baseline_for_team(context, team)
        warmed += team_warmed
        failed += team_failed

    context.log.info(f"Eager baseline warming complete: warmed={warmed} failed={failed}")
    context.add_output_metadata({"teams": len(team_ids), "warmed": warmed, "failed": failed})
    return {"teams": len(team_ids), "warmed": warmed, "failed": failed}


@dagster.job(
    description="Pre-warms last-30d web analytics queries for eager-enrolled teams",
    tags={"owner": JobOwners.TEAM_WEB_ANALYTICS.value},
)
def web_analytics_eager_baseline_warming_job():
    warm_eager_baseline_op()


@dagster.schedule(
    # 06:30 UTC daily — 30 min after team-selection so a freshly enrolled team
    # is warmed on the same day it's added. Off the top of the hour to avoid
    # collision with the hourly cache-warming schedule.
    cron_schedule="30 6 * * *",
    job=web_analytics_eager_baseline_warming_job,
    execution_timezone="UTC",
    tags={"owner": JobOwners.TEAM_WEB_ANALYTICS.value},
)
def web_analytics_eager_baseline_warming_schedule(
    context: dagster.ScheduleEvaluationContext,
) -> "dagster.RunRequest | dagster.SkipReason":
    return dagster.RunRequest()
