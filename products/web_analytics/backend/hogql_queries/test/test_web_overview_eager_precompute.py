"""Tests for eager (Dagster-warmed) web_overview precompute.

Eager and lazy share the execute path — what differs is the gate:
* Lazy: org-FF + per-query `useWebAnalyticsPrecompute=True`
* Eager: Constance allowlist (auto-selected from PreaggregationJob usage +
  FORCED/BLOCKED overrides). No FF, no per-query opt-in.

Tests cover the gate independently of the runtime mining (which depends on
ClickHouse query_log and is fragile in unit tests), via the
`WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS` override that bypasses the
auto-selection scan.
"""

from datetime import UTC, datetime

from freezegun import freeze_time
from posthog.test.base import APIBaseTest, ClickhouseTestMixin, _create_event, _create_person, flush_persons_and_events
from unittest.mock import patch

from django.test import override_settings
from django.utils import timezone as django_timezone

from dagster import build_op_context
from parameterized import parameterized

from posthog.schema import (
    CompareFilter,
    DateRange,
    EventPropertyFilter,
    HogQLQueryModifiers,
    PropertyOperator,
    SessionsV2JoinMode,
    WebAnalyticsSampling,
    WebOverviewQuery,
)

from posthog.hogql import ast
from posthog.hogql.parser import parse_select

from posthog.clickhouse.client import sync_execute
from posthog.clickhouse.preaggregation.web_overview_preaggregated_sql import (
    TRUNCATE_WEB_OVERVIEW_PREAGGREGATED_TABLE_SQL,
)
from posthog.models.instance_setting import override_instance_config
from posthog.models.utils import uuid7

from products.analytics_platform.backend.lazy_computation.lazy_computation_executor import (
    LazyComputationTable,
    QueryInfo,
    compute_query_hash,
)
from products.analytics_platform.backend.models.preaggregation_job import PreaggregationJob
from products.web_analytics.backend.hogql_queries.web_analytics_lazy_precompute import get_eager_enrolled_team_ids
from products.web_analytics.backend.hogql_queries.web_overview import WebOverviewQueryRunner
from products.web_analytics.backend.hogql_queries.web_overview_lazy_precompute import (
    INSERT_QUERY_TEMPLATE,
    _build_placeholders,
    can_use_eager_precompute,
    can_use_lazy_precompute,
)
from products.web_analytics.dags.eager_web_analytics_precompute import (
    _build_dag_placeholders,
    _select_eager_teams,
    _standard_date_ranges,
    warm_eager_precompute_op,
)


def _enable_lazy_mock():
    """Mock the org-FF that the lazy gate checks. Mirrors the existing pattern
    in `test_web_overview_lazy_precompute.py`."""
    return patch(
        "products.web_analytics.backend.hogql_queries.web_analytics_lazy_precompute.posthoganalytics.feature_enabled",
        return_value=True,
    )


@override_settings(IN_UNIT_TESTING=True)
class TestWebOverviewEagerPrecompute(ClickhouseTestMixin, APIBaseTest):
    def setUp(self) -> None:
        super().setUp()
        PreaggregationJob.objects.filter(team_id=self.team.pk).delete()
        sync_execute(TRUNCATE_WEB_OVERVIEW_PREAGGREGATED_TABLE_SQL())

    def _enable_eager(self):
        # FORCED_TEAM_IDS overrides the auto-selection scan so tests don't need
        # to seed PreaggregationJob history. The query path reads via
        # `get_eager_enrolled_team_ids()` which unions AUTO_SELECTED + FORCED.
        return override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk])

    def _seed_two_sessions(self) -> None:
        s1 = str(uuid7("2024-01-02"))
        s2 = str(uuid7("2024-01-03"))
        _create_person(team_id=self.team.pk, distinct_ids=["p1"], properties={"name": "p1"})
        _create_person(team_id=self.team.pk, distinct_ids=["p2"], properties={"name": "p2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2024-01-02T10:00:00Z",
            properties={"$session_id": s1, "$host": "example.com"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2024-01-02T10:05:00Z",
            properties={"$session_id": s1, "$host": "example.com"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2024-01-03T11:00:00Z",
            properties={"$session_id": s2, "$host": "other.com"},
        )
        flush_persons_and_events()

    def _build_query(
        self,
        date_from: str = "2024-01-01",
        date_to: str = "2024-01-07",
        properties: list | None = None,
        compare: bool = False,
        opt_in_lazy: bool = True,
    ) -> WebOverviewQuery:
        return WebOverviewQuery(
            dateRange=DateRange(date_from=date_from, date_to=date_to),
            properties=properties or [],
            compareFilter=CompareFilter(compare=compare) if compare else None,
            useWebAnalyticsPrecompute=opt_in_lazy,
        )

    def _runner(self, query: WebOverviewQuery) -> WebOverviewQueryRunner:
        return WebOverviewQueryRunner(team=self.team, query=query)

    # ─── gate logic ──────────────────────────────────────────────────────────

    def test_disabled_team_cannot_use_eager_precompute(self):
        runner = self._runner(self._build_query())
        assert not can_use_eager_precompute(runner)

    @freeze_time("2040-01-15T12:00:00Z")
    def test_enabled_team_can_use_eager_precompute(self):
        runner = self._runner(self._build_query())
        with self._enable_eager():
            assert can_use_eager_precompute(runner)

    def test_eager_gate_skips_lazy_rollout_gate(self):
        """Eager has its own rollout — no posthoganalytics FF, no per-query opt-in."""
        runner = self._runner(self._build_query(opt_in_lazy=False))
        with self._enable_eager():
            # opt_in_lazy=False would block the lazy gate, but the eager gate
            # doesn't consult `useWebAnalyticsPrecompute` at all.
            assert can_use_eager_precompute(runner)
            assert not can_use_lazy_precompute(runner)

    def test_blocked_team_is_excluded_even_when_forced(self):
        """BLOCKED takes precedence over FORCED, providing an escape hatch
        without removing a team from auto-selection sources."""
        with (
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]),
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS", [self.team.pk]),
        ):
            assert self.team.pk not in get_eager_enrolled_team_ids()
            runner = self._runner(self._build_query())
            assert not can_use_eager_precompute(runner)

    @parameterized.expand([("half_hour_tz", "Asia/Kolkata"), ("half_hour_tz_np", "Asia/Kathmandu")])
    def test_half_hour_timezone_gated_out(self, _name: str, tz: str):
        self.team.timezone = tz
        self.team.save()
        runner = self._runner(self._build_query())
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_conversion_goal_gated_out(self):
        from posthog.schema import ActionConversionGoal

        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=[],
            conversionGoal=ActionConversionGoal(actionId=1),
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_sampling_gated_out(self):
        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=[],
            sampling=WebAnalyticsSampling(enabled=True),
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_sessions_v2_uuid_mode_gated_out(self):
        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=[],
            modifiers=HogQLQueryModifiers(sessionsV2JoinMode=SessionsV2JoinMode.UUID),
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_multi_property_filter_gated_out(self):
        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=[
                EventPropertyFilter(key="$host", value="a.com", operator=PropertyOperator.EXACT),
                EventPropertyFilter(key="$host", value="b.com", operator=PropertyOperator.EXACT),
            ],
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_unsupported_property_key_gated_out(self):
        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=[
                EventPropertyFilter(key="$browser", value="Chrome", operator=PropertyOperator.EXACT),
            ],
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    def test_too_many_days_gated_out(self):
        query = WebOverviewQuery(
            dateRange=DateRange(date_from="2023-01-01", date_to="2024-01-15"),
            properties=[],
        )
        runner = self._runner(query)
        with self._enable_eager():
            assert not can_use_eager_precompute(runner)

    # ─── eager hit/miss/fallback ──────────────────────────────────────────────

    @freeze_time("2040-01-15T12:00:00Z")
    def test_eager_team_triggers_lazy_insert_on_miss(self):
        """When no pre-warmed jobs exist, the eager path runs ensure_precomputed
        inline (shared execute path with lazy)."""
        self._seed_two_sessions()
        runner = self._runner(self._build_query())
        with self._enable_eager():
            row = runner.get_eager_precomputed_row()
        assert row is not None
        assert row[0] > 0

    @freeze_time("2040-01-15T12:00:00Z")
    def test_eager_team_creates_job_on_first_request(self):
        self._seed_two_sessions()
        with self._enable_eager():
            self._runner(self._build_query()).calculate()
        jobs = list(PreaggregationJob.objects.filter(team_id=self.team.pk))
        assert len(jobs) > 0

    @freeze_time("2040-01-15T12:00:00Z")
    def test_pre_warmed_data_is_found_without_new_insert(self):
        """After warm_eager_precompute_op runs, the eager query path hits the cache.

        Uses lazy first (mocked FF) to populate. Then enables eager and confirms
        no new jobs are created on subsequent calls.
        """
        self._seed_two_sessions()

        with _enable_lazy_mock():
            self._runner(self._build_query()).calculate()

        job_count_before = PreaggregationJob.objects.filter(team_id=self.team.pk).count()
        with self._enable_eager():
            self._runner(self._build_query()).calculate()
        job_count_after = PreaggregationJob.objects.filter(team_id=self.team.pk).count()
        assert job_count_after == job_count_before

    @freeze_time("2040-01-15T12:00:00Z")
    def test_disabled_team_uses_raw_path(self):
        self._seed_two_sessions()
        response = self._runner(self._build_query()).calculate()
        assert response.results is not None
        assert len(response.results) == 5

    # ─── hash parity: DAG placeholders ↔ query-path placeholders ────────────

    def _compute_hash(self, placeholders: dict) -> str:
        hash_placeholders = {
            **placeholders,
            "time_window_min": ast.Constant(value="__TIME_WINDOW_MIN__"),
            "time_window_max": ast.Constant(value="__TIME_WINDOW_MAX__"),
        }
        parsed = parse_select(INSERT_QUERY_TEMPLATE, placeholders=hash_placeholders)
        assert isinstance(parsed, ast.SelectQuery)
        return compute_query_hash(
            QueryInfo(
                query=parsed,
                table=LazyComputationTable.WEB_OVERVIEW_PREAGGREGATED,
                timezone=self.team.timezone,
            )
        )

    @freeze_time("2040-01-15T12:00:00Z")
    def test_dag_placeholder_hash_matches_query_path_unfiltered(self):
        runner = self._runner(self._build_query())
        query_hash = self._compute_hash(_build_placeholders(runner))
        dag_hash = self._compute_hash(_build_dag_placeholders(self.team, host_filter=None, test_account_filter=None))
        assert query_hash == dag_hash, (
            "DAG placeholder AST diverged from query-path AST — pre-warmed jobs "
            "would be orphans the read cache lookup never finds."
        )

    @freeze_time("2040-01-15T12:00:00Z")
    def test_dag_placeholder_hash_matches_query_path_with_host_filter(self):
        host_query = self._build_query(
            properties=[EventPropertyFilter(key="$host", value="example.com", operator=PropertyOperator.EXACT)]
        )
        runner = self._runner(host_query)
        query_hash = self._compute_hash(_build_placeholders(runner))
        dag_hash = self._compute_hash(
            _build_dag_placeholders(self.team, host_filter="example.com", test_account_filter=None)
        )
        assert query_hash == dag_hash

    @freeze_time("2040-01-15T12:00:00Z")
    def test_dag_placeholder_host_filter_differs_from_unfiltered(self):
        dag_hash_none = self._compute_hash(_build_dag_placeholders(self.team, host_filter=None))
        dag_hash_example = self._compute_hash(_build_dag_placeholders(self.team, host_filter="example.com"))
        dag_hash_other = self._compute_hash(_build_dag_placeholders(self.team, host_filter="other.com"))
        assert dag_hash_none != dag_hash_example
        assert dag_hash_example != dag_hash_other

    # ─── standard date ranges ─────────────────────────────────────────────────

    @freeze_time("2024-01-15T12:00:00Z")
    def test_standard_date_ranges_are_five_ranges(self):
        assert len(_standard_date_ranges(datetime.now(UTC))) == 5

    @freeze_time("2024-01-15T12:00:00Z")
    def test_standard_date_ranges_today_is_correct(self):
        ranges = _standard_date_ranges(datetime.now(UTC))
        assert ranges[0] == (datetime(2024, 1, 15, tzinfo=UTC), datetime(2024, 1, 16, tzinfo=UTC))

    @freeze_time("2024-01-15T12:00:00Z")
    def test_standard_date_ranges_covers_7d_query(self):
        ranges = _standard_date_ranges(datetime.now(UTC))
        query_start = datetime(2024, 1, 8, tzinfo=UTC)
        query_end = datetime(2024, 1, 16, tzinfo=UTC)
        assert any(r_start <= query_start and r_end >= query_end for r_start, r_end in ranges)

    # ─── warm_eager_precompute_op e2e ─────────────────────────────────────────

    @freeze_time("2024-01-15T12:00:00Z")
    def test_warm_op_creates_ready_jobs(self):
        self._seed_two_sessions()
        with self._enable_eager():
            warm_eager_precompute_op(build_op_context(), [self.team.pk])
        ready_jobs = PreaggregationJob.objects.filter(team_id=self.team.pk, status=PreaggregationJob.Status.READY)
        assert ready_jobs.count() > 0

    @freeze_time("2024-01-15T12:00:00Z")
    def test_warm_op_skips_nonexistent_team(self):
        # Should not raise.
        warm_eager_precompute_op(build_op_context(), [999_999_999])

    @freeze_time("2024-01-15T12:00:00Z")
    def test_warm_op_idempotent_second_run_reuses_jobs(self):
        """Freshness check skips work on the second run — same job count."""
        self._seed_two_sessions()
        with self._enable_eager():
            warm_eager_precompute_op(build_op_context(), [self.team.pk])
            count_after_first = PreaggregationJob.objects.filter(team_id=self.team.pk).count()
            warm_eager_precompute_op(build_op_context(), [self.team.pk])
            count_after_second = PreaggregationJob.objects.filter(team_id=self.team.pk).count()
        assert count_after_second == count_after_first


@override_settings(IN_UNIT_TESTING=True)
class TestEagerTeamSelection(ClickhouseTestMixin, APIBaseTest):
    """Unit tests for `_select_eager_teams` — the PreaggregationJob mining heuristic."""

    def setUp(self) -> None:
        super().setUp()
        PreaggregationJob.objects.filter(team_id=self.team.pk).delete()

    def _create_jobs(self, team_id: int, distinct_hashes: int, *, status=PreaggregationJob.Status.READY):
        now = django_timezone.now()
        for i in range(distinct_hashes):
            PreaggregationJob.objects.create(
                team_id=team_id,
                query_hash=f"hash_{team_id}_{i:04x}",
                time_range_start=now,
                time_range_end=now,
                status=status,
            )

    def test_empty_returns_only_forced(self):
        with (
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 1),
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]),
        ):
            assert _select_eager_teams() == [self.team.pk]

    def test_below_threshold_team_is_not_auto_selected(self):
        self._create_jobs(self.team.pk, distinct_hashes=5)
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 10):
            assert self.team.pk not in _select_eager_teams()

    def test_above_threshold_team_is_auto_selected(self):
        self._create_jobs(self.team.pk, distinct_hashes=20)
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 10):
            assert self.team.pk in _select_eager_teams()

    def test_duplicate_hashes_do_not_inflate_diversity(self):
        """Counting distinct query_hash means a team with the same query run
        1000 times ranks below a team with 20 distinct queries."""
        # Same hash twice, only counts as 1 distinct
        now = django_timezone.now()
        for _ in range(50):
            PreaggregationJob.objects.create(
                team_id=self.team.pk,
                query_hash="single_hash",
                time_range_start=now,
                time_range_end=now,
                status=PreaggregationJob.Status.READY,
            )
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 10):
            assert self.team.pk not in _select_eager_teams()

    def test_blocked_overrides_auto_selection(self):
        self._create_jobs(self.team.pk, distinct_hashes=50)
        with (
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 10),
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS", [self.team.pk]),
        ):
            assert self.team.pk not in _select_eager_teams()

    def test_forced_added_even_below_threshold(self):
        # No jobs created → diversity = 0, well below any threshold
        with (
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_MIN_JOBS_THRESHOLD", 100),
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]),
        ):
            assert self.team.pk in _select_eager_teams()
