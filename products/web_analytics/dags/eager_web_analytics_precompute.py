"""Eagerly pre-warm web analytics precompute tables.

Runs every 15 minutes. Each cycle:

1. **Team selection** (`select_eager_teams_op`): mines `PreaggregationJob` for
   teams that already produce many distinct lazy precompute jobs over the last
   `WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS`. Top `MAX_TEAMS` by distinct
   `query_hash` count are auto-selected, unioned with `FORCED_TEAM_IDS` and
   stripped of `BLOCKED_TEAM_IDS`. Result is written back to the
   `WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS` Constance setting so
   the query path's `can_use_eager_precompute` can read it.

2. **Warm** (`warm_eager_precompute_op`): for each enrolled team, fans out over
   standard date ranges × top-N host filters × test-account-filter variants.
   For each combo, calls `read_precomputed_jobs_if_ready` first — if a fresh
   READY job exists, skip; otherwise call `ensure_precomputed` to do the work.
   Respects `MAX_JOBS_PER_TEAM` cap so a single team can't monopolise a cycle.

Scope: overview only in this PR. Paths preagg warming + query-side eager gate
will be added in a follow-up once `web_lazy_precompute_common.py` is unified
into the shared `web_analytics_lazy_precompute.py` module.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Optional

from django.db.models import Count
from django.utils import timezone as django_timezone

import dagster
import structlog
from dagster import Backoff, Jitter, RetryPolicy
from prometheus_client import Counter, Gauge, Histogram

from posthog.hogql import ast
from posthog.hogql.parser import parse_expr
from posthog.hogql.property import property_to_expr

from posthog.clickhouse.client import sync_execute
from posthog.clickhouse.query_tagging import Feature, tags_context
from posthog.dags.common import JobOwners
from posthog.exceptions_capture import capture_exception
from posthog.models.instance_setting import get_instance_setting, set_instance_setting
from posthog.models.team import Team

from products.analytics_platform.backend.lazy_computation.lazy_computation_executor import (
    LazyComputationTable,
    ensure_precomputed,
    read_precomputed_jobs_if_ready,
)
from products.analytics_platform.backend.models.preaggregation_job import PreaggregationJob
from products.web_analytics.backend.hogql_queries.web_analytics_lazy_precompute import (
    SESSION_FORWARD_PAD_MINUTES,
    floor_utc_day,
)
from products.web_analytics.backend.hogql_queries.web_overview_lazy_precompute import (
    INSERT_QUERY_TEMPLATE,
    INSERT_TTL_SECONDS,
)
from products.web_analytics.dags.web_preaggregated_utils import check_for_concurrent_runs

logger = structlog.get_logger(__name__)


# Team-selection metrics — emitted once per DAG cycle by the team-selection op.
EAGER_PRECOMPUTE_AUTO_SELECTED_TEAMS = Gauge(
    "web_analytics_eager_precompute_auto_selected_teams",
    "Teams above MIN_JOBS_THRESHOLD on the most recent team-selection scan.",
)
EAGER_PRECOMPUTE_FORCED_TEAMS = Gauge(
    "web_analytics_eager_precompute_forced_teams",
    "Teams added by FORCED_TEAM_IDS override beyond the auto-selected set.",
)
EAGER_PRECOMPUTE_FINAL_TEAMS = Gauge(
    "web_analytics_eager_precompute_final_teams",
    "Final enrolled team count post (auto ∪ forced) − blocked.",
)
EAGER_PRECOMPUTE_TEAM_DIVERSITY = Gauge(
    "web_analytics_eager_precompute_team_diversity",
    "Per-team distinct query_hash count over the lookback window.",
    ["team_id"],
)

# Per-cycle DAG metrics — labelled by family so future paths warming can join.
EAGER_PRECOMPUTE_JOBS_TRIGGERED = Counter(
    "web_analytics_eager_precompute_jobs_triggered_total",
    "Precompute jobs the eager DAG actually triggered (cache miss → ensure_precomputed ran).",
    ["family", "team_id"],
)
EAGER_PRECOMPUTE_SKIPPED_FRESH = Counter(
    "web_analytics_eager_precompute_skipped_fresh_total",
    "Eager DAG combinations skipped because a fresh READY job already exists.",
    ["family", "team_id"],
)
EAGER_PRECOMPUTE_ERRORS = Counter(
    "web_analytics_eager_precompute_errors_total",
    "Errors during eager precompute fan-out, by family and error class.",
    ["family", "team_id", "error_type"],
)
EAGER_PRECOMPUTE_TRUNCATED = Counter(
    "web_analytics_eager_precompute_truncated_total",
    "Teams that hit the MAX_JOBS_PER_TEAM cap in a cycle.",
    ["team_id"],
)
EAGER_PRECOMPUTE_TOP_HOSTS_MINED = Gauge(
    "web_analytics_eager_precompute_top_hosts_mined",
    "Distinct $host filter values discovered for the team this cycle (≤ MAX_HOSTS).",
    ["team_id"],
)
EAGER_PRECOMPUTE_DAG_DURATION_SECONDS = Histogram(
    "web_analytics_eager_precompute_dag_duration_seconds",
    "Wall-clock time for a full eager precompute cycle (selection + warming), by family.",
    ["family"],
    buckets=(10, 30, 60, 120, 300, 600, 900, 1800, float("inf")),
)


_AUTO_SELECTED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS"
_FORCED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS"
_BLOCKED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS"
_FAMILY = "web_overview"
_MAX_HOSTS = 5  # capped here; team count is the main scale dimension


eager_precompute_retry_policy = RetryPolicy(
    max_retries=2,
    delay=10,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL,
)


def _top_host_values(team_id: int, days: int = 7, limit: int = _MAX_HOSTS) -> list[str]:
    """Mine `metrics_query_log_mv` for the most-queried `$host` filter values.

    Best-effort: returns an empty list on any failure so the caller falls back
    to the unfiltered variant. Failures increment an error counter but do not
    block the rest of the team's combinations.
    """
    try:
        with tags_context(
            team_id=team_id,
            feature=Feature.CACHE_WARMUP,
            query_type="web_overview_eager_host_mining",
        ):
            results = sync_execute(
                """
                SELECT
                    JSONExtractRaw(log_comment, 'query') AS query_json_raw,
                    COUNT(*) AS query_count
                FROM metrics_query_log_mv
                WHERE
                    timestamp >= now() - INTERVAL %(days)s DAY
                    AND team_id = %(team_id)s
                    AND query_type IN ('web_overview_query', 'web_overview_preaggregated_query')
                    AND exception_code = 0
                    AND query_json_raw != ''
                GROUP BY query_json_raw
                ORDER BY query_count DESC
                LIMIT %(limit)s
                """,
                {"team_id": team_id, "days": days, "limit": limit * 10},
            )

        seen: dict[str, int] = {}
        for query_json_raw, query_count in results:
            try:
                query_data = json.loads(query_json_raw)
                properties = query_data.get("properties") or []
                for prop in properties:
                    if prop.get("key") == "$host" and prop.get("operator") == "exact":
                        value = prop.get("value")
                        if isinstance(value, str) and value:
                            seen[value] = seen.get(value, 0) + query_count
            except Exception:
                continue

        return sorted(seen, key=lambda h: seen[h], reverse=True)[:limit]
    except Exception:
        logger.exception("web_overview_eager_host_mining_failed", team_id=team_id)
        EAGER_PRECOMPUTE_ERRORS.labels(family=_FAMILY, team_id=str(team_id), error_type="host_mining_failed").inc()
        return []


def _standard_date_ranges(now_utc: datetime) -> list[tuple[datetime, datetime]]:
    """Standard set of (start, end) UTC ranges to pre-warm.

    Mirrors the date ranges most common in the web analytics dashboard: today,
    yesterday, last 7 / 14 / 30 days.
    """
    today_start = floor_utc_day(now_utc)
    today_end = today_start + timedelta(days=1)
    yesterday_start = today_start - timedelta(days=1)

    return [
        (today_start, today_end),
        (yesterday_start, today_start),
        (floor_utc_day(now_utc - timedelta(days=7)), today_end),
        (floor_utc_day(now_utc - timedelta(days=14)), today_end),
        (floor_utc_day(now_utc - timedelta(days=30)), today_end),
    ]


def _build_dag_placeholders(
    team: Team,
    host_filter: Optional[str] = None,
    test_account_filter: Optional[ast.Expr] = None,
) -> dict[str, ast.Expr]:
    """Build placeholders matching exactly what the query-time runner produces.

    The AST repr must match the runner's AST so that `compute_query_hash`
    produces the same hash — otherwise the DAG's pre-warmed jobs are orphans
    that the query path never finds. Hash-parity tests cover this invariant.
    """
    event_type_filter: ast.Expr = ast.Or(
        exprs=[
            ast.CompareOperation(
                op=ast.CompareOperationOp.Eq,
                left=ast.Field(chain=["event"]),
                right=ast.Constant(value="$pageview"),
            ),
            ast.CompareOperation(
                op=ast.CompareOperationOp.Eq,
                left=ast.Field(chain=["event"]),
                right=ast.Constant(value="$screen"),
            ),
        ]
    )

    if host_filter:
        user_filter: ast.Expr = ast.Call(
            name="equals",
            args=[
                ast.Field(chain=["events", "properties", "$host"]),
                ast.Constant(value=host_filter),
            ],
        )
    else:
        user_filter = ast.Constant(value=True)

    # Must match WebAnalyticsQueryRunner.events_session_property for
    # sessionsV2JoinMode != "uuid" (uuid mode is gated out of eager).
    events_session_id = parse_expr("events.$session_id")

    return {
        "events_session_id": events_session_id,
        "event_type_filter": event_type_filter,
        "user_filter": user_filter,
        "test_account_filter": test_account_filter if test_account_filter is not None else ast.Constant(value=True),
        "pad_minutes": ast.Constant(value=SESSION_FORWARD_PAD_MINUTES),
    }


def _test_account_filter_variants(team: Team) -> list[ast.Expr]:
    """Variants to pre-warm: always the unfiltered case; add the team's
    configured filter when present so `filterTestAccounts=True` queries hit cache too.
    """
    variants: list[ast.Expr] = [ast.Constant(value=True)]

    if isinstance(team.test_account_filters, list) and team.test_account_filters:
        try:
            variants.append(property_to_expr(team.test_account_filters, team=team))
        except Exception:
            logger.warning("web_overview_eager_test_account_filter_failed", team_id=team.pk, exc_info=True)
            EAGER_PRECOMPUTE_ERRORS.labels(
                family=_FAMILY, team_id=str(team.pk), error_type="test_account_filter_failed"
            ).inc()

    return variants


def _select_eager_teams() -> list[int]:
    """Auto-select eager precompute teams by mining `PreaggregationJob` usage.

    Heuristic: count distinct query_hash per team over the last
    `WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS`. Teams above the threshold
    are sorted descending by diversity and capped at `MAX_TEAMS`. Forced teams
    join unconditionally; blocked teams are removed.
    """
    lookback_days = get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS") or 7
    min_threshold = get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD") or 20
    max_teams = get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_MAX_TEAMS") or 20
    forced = set(get_instance_setting(_FORCED_SETTING) or [])
    blocked = set(get_instance_setting(_BLOCKED_SETTING) or [])

    cutoff = django_timezone.now() - timedelta(days=lookback_days)
    candidates = list(
        PreaggregationJob.objects.filter(
            created_at__gte=cutoff,
            status__in=[PreaggregationJob.Status.READY, PreaggregationJob.Status.STALE],
        )
        .values("team_id")
        .annotate(query_diversity=Count("query_hash", distinct=True))
        .filter(query_diversity__gte=min_threshold)
        .order_by("-query_diversity")[:max_teams]
    )

    auto_team_ids = {row["team_id"] for row in candidates}
    final = sorted((auto_team_ids | forced) - blocked)

    EAGER_PRECOMPUTE_AUTO_SELECTED_TEAMS.set(len(auto_team_ids))
    EAGER_PRECOMPUTE_FORCED_TEAMS.set(len(forced - auto_team_ids))
    EAGER_PRECOMPUTE_FINAL_TEAMS.set(len(final))
    for row in candidates:
        EAGER_PRECOMPUTE_TEAM_DIVERSITY.labels(team_id=str(row["team_id"])).set(row["query_diversity"])

    logger.info(
        "web_analytics_eager_precompute_team_selection",
        auto_selected=len(auto_team_ids),
        forced_added=len(forced - auto_team_ids),
        blocked_removed=len(blocked & auto_team_ids),
        final_count=len(final),
        lookback_days=lookback_days,
        min_threshold=min_threshold,
    )

    return final


@dagster.op(retry_policy=eager_precompute_retry_policy)
def select_eager_teams_op(context: dagster.OpExecutionContext) -> list[int]:
    """Mine PreaggregationJob → write Constance `AUTO_SELECTED_TEAM_IDS` → return team list."""
    team_ids = _select_eager_teams()
    set_instance_setting(_AUTO_SELECTED_SETTING, team_ids)
    context.log.info(f"Eager precompute: {len(team_ids)} teams enrolled this cycle")
    context.add_output_metadata({"team_count": len(team_ids), "team_ids": str(team_ids)})
    return team_ids


@dagster.op(retry_policy=eager_precompute_retry_policy)
def warm_eager_precompute_op(context: dagster.OpExecutionContext, team_ids: list[int]) -> None:
    """For each team × date_range × host_filter × test_account_filter combination,
    skip if a fresh READY job exists, otherwise ensure_precomputed. Capped per team."""
    start_time = datetime.now(UTC)
    now_utc = start_time
    date_ranges = _standard_date_ranges(now_utc)
    max_jobs_per_team = get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_MAX_JOBS_PER_TEAM") or 60

    for team_id in team_ids:
        try:
            team = Team.objects.get(pk=team_id)
        except Team.DoesNotExist:
            context.log.warning(f"Team {team_id} not found, skipping")
            continue

        host_variants: list[Optional[str]] = [None]
        top_hosts = _top_host_values(team_id)
        host_variants.extend(top_hosts)
        EAGER_PRECOMPUTE_TOP_HOSTS_MINED.labels(team_id=str(team_id)).set(len(top_hosts))

        ta_filter_variants = _test_account_filter_variants(team)
        jobs_for_team = 0
        truncated_for_team = False

        context.log.info(
            f"Team {team_id}: warming {len(date_ranges)} date ranges × "
            f"{len(host_variants)} host variants × "
            f"{len(ta_filter_variants)} test-account-filter variants"
        )

        for date_range_start, date_range_end in date_ranges:
            for host_filter in host_variants:
                for ta_filter in ta_filter_variants:
                    if jobs_for_team >= max_jobs_per_team:
                        if not truncated_for_team:
                            EAGER_PRECOMPUTE_TRUNCATED.labels(team_id=str(team_id)).inc()
                            context.log.warning(
                                f"Team {team_id}: hit MAX_JOBS_PER_TEAM={max_jobs_per_team} cap, truncating"
                            )
                            truncated_for_team = True
                        continue

                    try:
                        placeholders = _build_dag_placeholders(team, host_filter, ta_filter)
                        existing = read_precomputed_jobs_if_ready(
                            team=team,
                            insert_query=INSERT_QUERY_TEMPLATE,
                            time_range_start=date_range_start,
                            time_range_end=date_range_end,
                            ttl_seconds=INSERT_TTL_SECONDS,
                            table=LazyComputationTable.WEB_OVERVIEW_PREAGGREGATED,
                            placeholders=placeholders,
                        )
                        if existing.ready:
                            EAGER_PRECOMPUTE_SKIPPED_FRESH.labels(family=_FAMILY, team_id=str(team_id)).inc()
                            jobs_for_team += 1
                            continue

                        ensure_precomputed(
                            team=team,
                            insert_query=INSERT_QUERY_TEMPLATE,
                            time_range_start=date_range_start,
                            time_range_end=date_range_end,
                            ttl_seconds=INSERT_TTL_SECONDS,
                            table=LazyComputationTable.WEB_OVERVIEW_PREAGGREGATED,
                            placeholders=placeholders,
                            query_type="web_overview_eager_precompute",
                        )
                        EAGER_PRECOMPUTE_JOBS_TRIGGERED.labels(family=_FAMILY, team_id=str(team_id)).inc()
                        jobs_for_team += 1
                    except Exception as e:
                        EAGER_PRECOMPUTE_ERRORS.labels(
                            family=_FAMILY, team_id=str(team_id), error_type=type(e).__name__
                        ).inc()
                        context.log.exception(
                            f"Error pre-warming team {team_id} range "
                            f"{date_range_start} - {date_range_end}, host={host_filter!r}"
                        )
                        capture_exception(e)

    elapsed = (datetime.now(UTC) - start_time).total_seconds()
    EAGER_PRECOMPUTE_DAG_DURATION_SECONDS.labels(family=_FAMILY).observe(elapsed)
    context.log.info("Eager precompute fan-out complete", duration_seconds=elapsed)


@dagster.job(
    description="Pre-warms web_overview_preaggregated for eager-enrolled teams",
    tags={
        "owner": JobOwners.TEAM_WEB_ANALYTICS.value,
        "dagster/web_analytics_eager_precompute": "web_analytics_eager_precompute",
    },
)
def web_analytics_eager_precompute_job() -> None:
    team_ids = select_eager_teams_op()
    warm_eager_precompute_op(team_ids)


@dagster.schedule(
    cron_schedule="*/15 * * * *",
    job=web_analytics_eager_precompute_job,
    execution_timezone="UTC",
    tags={"owner": JobOwners.TEAM_WEB_ANALYTICS.value},
)
def web_analytics_eager_precompute_schedule(
    context: dagster.ScheduleEvaluationContext,
) -> "dagster.RunRequest | dagster.SkipReason":
    skip_reason = check_for_concurrent_runs(context)
    if skip_reason:
        return skip_reason
    return dagster.RunRequest()
