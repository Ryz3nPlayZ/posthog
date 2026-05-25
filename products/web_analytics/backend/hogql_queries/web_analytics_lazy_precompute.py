"""Shared lazy-precompute building blocks for web analytics query runners.

The web overview and web stats table runners both serve eligible queries from
on-demand precomputed ClickHouse tables via the `lazy_computation` framework.
The pieces that do not depend on which runner is calling — eligibility gating,
filter-expression construction, TTL/sizing constants, UTC-day helpers — live
here so both `web_overview_lazy_precompute.py` and `web_stats_lazy_precompute.py`
share one implementation.
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Optional, Protocol, Union

import structlog
import posthoganalytics
from prometheus_client import Counter

from posthog.schema import EventPropertyFilter, PropertyOperator, WebOverviewQuery, WebStatsTableQuery

from posthog.hogql import ast
from posthog.hogql.property import property_to_expr
from posthog.hogql.transforms.preaggregated_table_transformation import is_integer_timezone

from posthog.models.team import Team

logger = structlog.get_logger(__name__)


# Counts requests refused by `can_use_lazy_precompute`. `family` is the
# `log_prefix` (`web_overview` / `web_stats`); `reason` is the
# `LazyPrecomputeIneligible` subclass name. Together with `_fallback_total`
# and `_success_total`, callers can compute lazy adoption per family.
WEB_ANALYTICS_LAZY_PRECOMPUTE_REJECTED = Counter(
    "web_analytics_lazy_precompute_rejected_total",
    "Requests refused by the lazy precompute gate, by family and rejection reason.",
    ["family", "reason"],
)

# Counts requests that passed the gate but couldn't be served from precompute
# (window empty, no jobs created, current/previous period not yet READY). The
# caller falls back to the raw / v2 path on each of these.
WEB_ANALYTICS_LAZY_PRECOMPUTE_FALLBACK = Counter(
    "web_analytics_lazy_precompute_fallback_total",
    "Lazy precompute fall-throughs after the gate accepted, by family and reason.",
    ["family", "reason"],
)

# Counts requests where the lazy path successfully returned a precomputed row.
WEB_ANALYTICS_LAZY_PRECOMPUTE_SUCCESS = Counter(
    "web_analytics_lazy_precompute_success_total",
    "Requests served from the lazy precompute path, by family.",
    ["family"],
)

# Eager precompute metrics — emitted by the same query-path code as the lazy
# counters above, but labelled separately so dashboards can distinguish "team
# was eager-allowlisted" from "team passed the org-FF + per-query opt-in" rollout.
WEB_ANALYTICS_EAGER_PRECOMPUTE_REJECTED = Counter(
    "web_analytics_eager_precompute_rejected_total",
    "Requests refused by the eager precompute gate, by family and rejection reason.",
    ["family", "reason"],
)

WEB_ANALYTICS_EAGER_PRECOMPUTE_FALLBACK = Counter(
    "web_analytics_eager_precompute_fallback_total",
    "Eager precompute fall-throughs after the gate accepted, by family and reason.",
    ["family", "reason"],
)

WEB_ANALYTICS_EAGER_PRECOMPUTE_SUCCESS = Counter(
    "web_analytics_eager_precompute_success_total",
    "Requests served from the eager precompute path (DAG had pre-warmed the data), by family.",
    ["family"],
)

# Cache hit: an eager-allowlisted team's query found a READY job populated by
# the Dagster job rather than running an inline INSERT. This is the load-bearing
# metric for the "100% hit rate" goal on enrolled teams.
WEB_ANALYTICS_EAGER_PRECOMPUTE_CACHE_HIT = Counter(
    "web_analytics_eager_precompute_cache_hit_total",
    "Eager precompute reads served from a DAG-warmed READY job (no inline INSERT), by family.",
    ["family"],
)

# Bucketing the precompute hourly keeps reads correct for any whole-hour-offset
# timezone — boundaries line up exactly when the team-local window is converted
# to UTC before filtering on `time_window_start`. Half-hour-offset timezones
# (IST, Newfoundland, Nepal, etc.) are explicitly gated out below.
LAZY_TTL_SECONDS: dict[str, int] = {
    "0d": 15 * 60,
    "1d": 60 * 60,
    "7d": 24 * 60 * 60,
    "default": 7 * 24 * 60 * 60,
}

# Today the gate accepts: empty user filters, or a single EventPropertyFilter
# on `$host` with operator `exact`. Test-account filters are always allowed
# (their content is hashed into the cache key).
SUPPORTED_USER_FILTER_KEYS: set[str] = {"$host"}

# Upper bound on the precompute span. Above this, the framework would create
# enough daily jobs that the first request burns INSERT slots for minutes.
MAX_PRECOMPUTE_DAYS = 90

# Forward pad on the per-job event-scan window. The lazy_computation framework
# chunks the precompute span into daily UTC jobs; each job covers
# `[time_window_min, time_window_max)`. A session starting at 23:50 with events
# spilling past midnight would lose its trailing events without a forward pad —
# the HAVING clause attributes the session to its start hour, but the events
# table scan needs to see those trailing events to compute correct per-session
# aggregates. Forward-only is sufficient: the HAVING keeps only sessions whose
# `min(session.$start_timestamp)` falls inside the job window, and every event
# of such a session has `timestamp >= session.$start_timestamp`, so backward
# scanning never picks up anything that survives HAVING.
#
# Sessions longer than 24 h crossing a UTC-day boundary are silently
# undercounted — a documented limitation, not a sizing target (24 h is the
# posthog-js hard cap, covering effectively the whole population).
SESSION_FORWARD_PAD_MINUTES = 24 * 60


class LazyPrecomputeRunner(Protocol):
    """Structural type for the attributes the shared helpers read off a runner.

    Both `WebOverviewQueryRunner` and `WebStatsTableQueryRunner` satisfy this.
    """

    team: Team

    @property
    def query(self) -> Union[WebOverviewQuery, WebStatsTableQuery]: ...

    @property
    def query_date_range(self) -> object: ...

    @property
    def _test_account_filters(self) -> list: ...

    @property
    def events_session_property(self) -> ast.Expr: ...


class LazyPrecomputeIneligible(Exception):
    """Base class for reasons the lazy precompute path is not eligible.

    Raised by the eligibility checks and caught by `can_use_lazy_precompute`,
    which logs the exception class name. Subclass names are the canonical
    identifiers used in logs/metrics — keep them stable across releases.
    """


class OrgFeatureFlagDisabled(LazyPrecomputeIneligible):
    pass


class PerQueryOptInNotSet(LazyPrecomputeIneligible):
    pass


class NonIntegerTimezone(LazyPrecomputeIneligible):
    pass


class ConversionGoalUnsupported(LazyPrecomputeIneligible):
    pass


class SamplingEnabled(LazyPrecomputeIneligible):
    pass


class SessionsV2UuidMode(LazyPrecomputeIneligible):
    pass


class TooManyFilters(LazyPrecomputeIneligible):
    pass


class NonEventPropertyFilter(LazyPrecomputeIneligible):
    pass


class UnsupportedFilterKey(LazyPrecomputeIneligible):
    def __init__(self, key: str):
        self.key = key
        super().__init__(f"key={key!r}")


class UnsupportedFilterOperator(LazyPrecomputeIneligible):
    def __init__(self, operator: object):
        self.operator = operator
        super().__init__(f"operator={operator!r}")


class NonStringOrEmptyFilterValue(LazyPrecomputeIneligible):
    pass


class MissingDateRange(LazyPrecomputeIneligible):
    pass


class DateRangeOverMax(LazyPrecomputeIneligible):
    def __init__(self, days: int):
        self.days = days
        super().__init__(f"days={days} max={MAX_PRECOMPUTE_DAYS}")


class TeamNotInEagerAllowlist(LazyPrecomputeIneligible):
    """Raised when a team is not currently enrolled in eager precompute.

    The enrolled set is the union of WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS
    (written by the Dagster team-selection op) and WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS
    (admin override), minus WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS.
    """

    pass


def can_use_lazy_precompute(
    runner: LazyPrecomputeRunner,
    *,
    log_prefix: str,
    extra_check: Optional[Callable[[LazyPrecomputeRunner], None]] = None,
) -> bool:
    """Return True iff the lazy precompute gate is eligible. Logs the rejection
    reason at INFO level so every fall-through can be attributed.

    `log_prefix` differentiates the log event name per runner (e.g.
    `web_overview` / `web_stats`). `extra_check` lets a runner add its own
    eligibility checks — it must raise a `LazyPrecomputeIneligible` subclass on
    rejection, and runs after the shared checks pass.
    """
    try:
        check_common_eligible(runner)
        if extra_check is not None:
            extra_check(runner)
    except LazyPrecomputeIneligible as exc:
        reason = type(exc).__name__
        WEB_ANALYTICS_LAZY_PRECOMPUTE_REJECTED.labels(family=log_prefix, reason=reason).inc()
        logger.info(
            f"{log_prefix}_lazy_precompute_rejected",
            team_id=runner.team.pk,
            reason=reason,
            detail=str(exc) or None,
        )
        return False
    logger.info(
        f"{log_prefix}_lazy_precompute_eligible",
        team_id=runner.team.pk,
    )
    return True


def check_rollout_gate(runner: LazyPrecomputeRunner) -> None:
    """Lazy-only gate: PostHog feature flag + per-query opt-in.

    Eager precompute has its own rollout mechanism (admin allowlist + auto
    selection from PreaggregationJob usage), so it skips this check entirely
    and only calls `check_query_shape_eligible`.

    - `web-analytics-precompute-toggle` (PostHog feature flag): the same flag
      the frontend already uses to show/hide the "Allow precompute" button in
      the Web Analytics ScenePanel. Evaluated at the organization level. The
      SDK swallows its own exceptions and returns None (falsy) on failure, so
      a flag-service outage fails-closed.
    - `query.useWebAnalyticsPrecompute`: per-query parameter set by the
      "Allow precompute" toggle.
    """
    query = runner.query

    if not posthoganalytics.feature_enabled(
        "web-analytics-precompute-toggle",
        str(runner.team.uuid),
        groups={
            "organization": str(runner.team.organization_id),
            "project": str(runner.team.id),
        },
        group_properties={
            "organization": {"id": str(runner.team.organization_id)},
            "project": {"id": str(runner.team.id)},
        },
        only_evaluate_locally=True,
        send_feature_flag_events=False,
    ):
        raise OrgFeatureFlagDisabled()

    if query.useWebAnalyticsPrecompute is not True:
        raise PerQueryOptInNotSet()


def check_query_shape_eligible(runner: LazyPrecomputeRunner) -> None:
    """Raise a `LazyPrecomputeIneligible` subclass if the query's shape is not
    supported by the precompute schema (regardless of rollout).

    Both the lazy and eager paths share this check — query shape constraints
    are about whether the precomputed table can actually serve the query
    correctly, not about who is allowed to use it.
    """
    query = runner.query

    # Half-hour-offset timezones (IST +5:30, Newfoundland -3:30, Nepal +5:45, etc.)
    # can't be served by UTC hourly buckets without sub-hour precision. Skip them
    # rather than return wrong totals on the boundary days.
    if not is_integer_timezone(runner.team.timezone):
        raise NonIntegerTimezone()

    if query.conversionGoal is not None:
        raise ConversionGoalUnsupported()

    if query.sampling is not None and getattr(query.sampling, "enabled", False):
        raise SamplingEnabled()

    # UUID session-id mode produces `uniqState(UUID)` from
    # `events.$session_id_uuid`, which fails to insert into the web overview
    # table's `uniq_sessions_state AggregateFunction(uniq, String)` column. The
    # web stats table has no session-uniq column (it stores `uniq, UUID` user
    # state only), so this gate is conservative there — kept shared for
    # simplicity until the web overview column is re-typed in a follow-up.
    if query.modifiers and query.modifiers.sessionsV2JoinMode == "uuid":
        raise SessionsV2UuidMode()

    properties = query.properties or []
    if len(properties) > 1:
        raise TooManyFilters()
    for prop in properties:
        if not isinstance(prop, EventPropertyFilter):
            raise NonEventPropertyFilter()
        if prop.key not in SUPPORTED_USER_FILTER_KEYS:
            raise UnsupportedFilterKey(prop.key)
        if prop.operator != PropertyOperator.EXACT:
            raise UnsupportedFilterOperator(prop.operator)
        if not isinstance(prop.value, str) or not prop.value:
            raise NonStringOrEmptyFilterValue()

    date_from = runner.query_date_range.date_from()  # type: ignore[attr-defined]
    date_to = runner.query_date_range.date_to()  # type: ignore[attr-defined]
    if date_from is None or date_to is None:
        raise MissingDateRange()

    days = (date_to - date_from).days
    if days > MAX_PRECOMPUTE_DAYS:
        raise DateRangeOverMax(days)


def check_common_eligible(runner: LazyPrecomputeRunner) -> None:
    """Lazy-path eligibility: rollout gate + query shape.

    Preserves the pre-split behavior so existing callers (web overview / web
    stats / web stats paths lazy modules) need no changes.
    """
    check_rollout_gate(runner)
    check_query_shape_eligible(runner)


def get_eager_enrolled_team_ids() -> set[int]:
    """Return the current eager-enrolled team set.

    Sourced from Constance settings:
      eager_set = (AUTO_SELECTED ∪ FORCED) - BLOCKED

    AUTO_SELECTED is written by the Dagster team-selection op
    (`web_analytics_eager_from_lazy_usage`) and refreshed each cycle.
    FORCED/BLOCKED are admin overrides.
    """
    from posthog.models.instance_setting import get_instance_setting

    auto_selected = set(get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS") or [])
    forced = set(get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS") or [])
    blocked = set(get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS") or [])
    return (auto_selected | forced) - blocked


def can_use_eager_precompute(
    runner: LazyPrecomputeRunner,
    *,
    log_prefix: str,
    extra_check: Optional[Callable[[LazyPrecomputeRunner], None]] = None,
) -> bool:
    """Return True iff the team is eager-enrolled AND the query shape is supported.

    Distinct from the lazy gate: no PostHog feature flag check, no per-query
    opt-in. Eager enrollment is purely allowlist-based — the team either is or
    isn't in the auto-selected/forced set.

    `extra_check` lets runners add runner-specific shape constraints (e.g.
    paths runner requires breakdownBy=PAGE), same as the lazy gate.
    """
    try:
        if runner.team.pk not in get_eager_enrolled_team_ids():
            raise TeamNotInEagerAllowlist()
        check_query_shape_eligible(runner)
        if extra_check is not None:
            extra_check(runner)
    except LazyPrecomputeIneligible as exc:
        reason = type(exc).__name__
        WEB_ANALYTICS_EAGER_PRECOMPUTE_REJECTED.labels(family=log_prefix, reason=reason).inc()
        logger.info(
            f"{log_prefix}_eager_precompute_rejected",
            team_id=runner.team.pk,
            reason=reason,
            detail=str(exc) or None,
        )
        return False
    logger.info(
        f"{log_prefix}_eager_precompute_eligible",
        team_id=runner.team.pk,
    )
    return True


def user_filter_expr(runner: LazyPrecomputeRunner) -> ast.Expr:
    """Build the AST expression that gets substituted into the INSERT's WHERE clause.

    The substituted AST is what `ensure_precomputed` hashes into the cache key —
    different filter values therefore become different precomputed jobs.
    """
    if not runner.query.properties:
        return ast.Constant(value=True)

    # Gate already enforces single EventPropertyFilter with $host exact + string value.
    host_filter = runner.query.properties[0]
    assert isinstance(host_filter, EventPropertyFilter)
    return ast.Call(
        name="equals",
        args=[
            ast.Field(chain=["events", "properties", host_filter.key]),
            ast.Constant(value=host_filter.value),
        ],
    )


def test_account_filter_expr(runner: LazyPrecomputeRunner) -> ast.Expr:
    """Test-account filters land in the placeholder set, so they also shape the cache key.

    `_test_account_filters` may be an empty list when filterTestAccounts is False
    or the project has none configured.
    """
    if not runner._test_account_filters:
        return ast.Constant(value=True)
    return property_to_expr(runner._test_account_filters, team=runner.team)


def events_session_id_expr(runner: LazyPrecomputeRunner) -> ast.Expr:
    return runner.events_session_property


def floor_utc_day(dt_utc: datetime) -> datetime:
    return datetime(dt_utc.year, dt_utc.month, dt_utc.day, tzinfo=UTC)


def ceil_utc_day(dt_utc: datetime) -> datetime:
    floor = floor_utc_day(dt_utc)
    if floor == dt_utc:
        return floor
    return floor + timedelta(days=1)
