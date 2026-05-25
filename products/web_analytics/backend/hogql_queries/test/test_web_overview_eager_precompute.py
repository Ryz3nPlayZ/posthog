"""Tests for the eager web_overview precompute gate.

Eager and lazy share the execute path — what differs is the gate:
* Lazy: org-FF (`web-analytics-precompute-toggle`) + per-query `useWebAnalyticsPrecompute=True`
* Eager: Constance allowlist (auto-selected from PreaggregationJob activity +
  FORCED/BLOCKED overrides). No FF, no per-query opt-in.

The Dagster cache_warming job replays enrolled teams' actual queries from
`metrics_query_log_mv` every hour; those replays land on the eager gate (no
opt-in required), call `execute_lazy_precomputed_read` → `ensure_precomputed`,
and populate the cache. Real user queries then land on the warm cache.

There is no per-team-per-cycle fan-out in this PR — the cache warmer's
existing replay mechanism is the warming strategy. The new
`web_analytics_eager_precompute_team_selection` Dagster asset just refreshes
the AUTO_SELECTED_TEAM_IDS Constance setting once a day.
"""

from datetime import timedelta

from freezegun import freeze_time
from posthog.test.base import APIBaseTest, ClickhouseTestMixin

from django.test import override_settings
from django.utils import timezone as django_timezone

from dagster import build_asset_context
from parameterized import parameterized

from posthog.schema import (
    DateRange,
    EventPropertyFilter,
    HogQLQueryModifiers,
    PropertyOperator,
    SessionsV2JoinMode,
    WebAnalyticsSampling,
    WebOverviewQuery,
)

from posthog.models.instance_setting import get_instance_setting, override_instance_config

from products.analytics_platform.backend.models.preaggregation_job import PreaggregationJob
from products.web_analytics.backend.hogql_queries.web_analytics_lazy_precompute import get_eager_enrolled_team_ids
from products.web_analytics.backend.hogql_queries.web_overview import WebOverviewQueryRunner
from products.web_analytics.backend.hogql_queries.web_overview_lazy_precompute import (
    can_use_eager_precompute,
    can_use_lazy_precompute,
)
from products.web_analytics.dags.cache_warming import get_teams_enabled_for_web_analytics_cache_warming
from products.web_analytics.dags.eager_web_analytics_precompute import (
    _select_eager_teams,
    web_analytics_eager_precompute_team_selection,
)


@override_settings(IN_UNIT_TESTING=True)
class TestEagerPrecomputeGate(ClickhouseTestMixin, APIBaseTest):
    def setUp(self) -> None:
        super().setUp()
        PreaggregationJob.objects.filter(team_id=self.team.pk).delete()

    def _enable_eager(self):
        # FORCED_TEAM_IDS overrides the auto-selection scan so tests don't need
        # to seed PreaggregationJob history. The query path reads via
        # `get_eager_enrolled_team_ids()` which unions AUTO_SELECTED + FORCED.
        return override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk])

    def _build_query(self, *, opt_in_lazy: bool = True, **kwargs) -> WebOverviewQuery:
        return WebOverviewQuery(
            dateRange=DateRange(date_from="2024-01-01", date_to="2024-01-07"),
            properties=kwargs.get("properties", []),
            conversionGoal=kwargs.get("conversionGoal"),
            sampling=kwargs.get("sampling"),
            modifiers=kwargs.get("modifiers"),
            useWebAnalyticsPrecompute=opt_in_lazy,
        )

    def _runner(self, query: WebOverviewQuery) -> WebOverviewQueryRunner:
        return WebOverviewQueryRunner(team=self.team, query=query)

    def test_disabled_team_cannot_use_eager_precompute(self):
        assert not can_use_eager_precompute(self._runner(self._build_query()))

    @freeze_time("2040-01-15T12:00:00Z")
    def test_enabled_team_can_use_eager_precompute(self):
        with self._enable_eager():
            assert can_use_eager_precompute(self._runner(self._build_query()))

    def test_eager_gate_skips_lazy_rollout_gate(self):
        """Eager has its own rollout — no posthoganalytics FF, no per-query opt-in."""
        runner = self._runner(self._build_query(opt_in_lazy=False))
        with self._enable_eager():
            # opt_in_lazy=False blocks the lazy gate, but the eager gate doesn't
            # consult `useWebAnalyticsPrecompute` at all.
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
            assert not can_use_eager_precompute(self._runner(self._build_query()))

    # ─── query-shape gate (shared with lazy via check_query_shape_eligible) ──

    @parameterized.expand([("ist", "Asia/Kolkata"), ("kathmandu", "Asia/Kathmandu")])
    def test_half_hour_timezone_gated_out(self, _name: str, tz: str):
        self.team.timezone = tz
        self.team.save()
        with self._enable_eager():
            assert not can_use_eager_precompute(self._runner(self._build_query()))

    def test_conversion_goal_gated_out(self):
        from posthog.schema import ActionConversionGoal

        with self._enable_eager():
            assert not can_use_eager_precompute(
                self._runner(self._build_query(conversionGoal=ActionConversionGoal(actionId=1)))
            )

    def test_sampling_gated_out(self):
        with self._enable_eager():
            assert not can_use_eager_precompute(
                self._runner(self._build_query(sampling=WebAnalyticsSampling(enabled=True)))
            )

    def test_sessions_v2_uuid_mode_gated_out(self):
        with self._enable_eager():
            assert not can_use_eager_precompute(
                self._runner(
                    self._build_query(modifiers=HogQLQueryModifiers(sessionsV2JoinMode=SessionsV2JoinMode.UUID))
                )
            )

    def test_multi_property_filter_gated_out(self):
        with self._enable_eager():
            assert not can_use_eager_precompute(
                self._runner(
                    self._build_query(
                        properties=[
                            EventPropertyFilter(key="$host", value="a.com", operator=PropertyOperator.EXACT),
                            EventPropertyFilter(key="$host", value="b.com", operator=PropertyOperator.EXACT),
                        ]
                    )
                )
            )

    def test_unsupported_property_key_gated_out(self):
        with self._enable_eager():
            assert not can_use_eager_precompute(
                self._runner(
                    self._build_query(
                        properties=[
                            EventPropertyFilter(key="$browser", value="Chrome", operator=PropertyOperator.EXACT)
                        ]
                    )
                )
            )


@override_settings(IN_UNIT_TESTING=True)
class TestEagerTeamSelection(ClickhouseTestMixin, APIBaseTest):
    """Tests for `_select_eager_teams` — the distinct-team-ids heuristic.

    Auto-selection: any team with at least one PreaggregationJob row in the
    lookback window. Forced teams join unconditionally; blocked teams are
    removed even if they appear in auto-selection.
    """

    def setUp(self) -> None:
        super().setUp()
        PreaggregationJob.objects.filter(team_id=self.team.pk).delete()

    def _create_job(self, team_id: int, *, age_days: int = 0, status=PreaggregationJob.Status.READY):
        now = django_timezone.now()
        job = PreaggregationJob.objects.create(
            team_id=team_id,
            query_hash=f"hash_{team_id}_{age_days}",
            time_range_start=now,
            time_range_end=now + timedelta(hours=1),
            status=status,
        )
        # CreatedMetaFields uses `created_at` from a base class; override via update so freeze_time isn't required.
        PreaggregationJob.objects.filter(pk=job.pk).update(created_at=now - timedelta(days=age_days))
        return job

    def test_no_jobs_returns_only_forced(self):
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]):
            assert _select_eager_teams() == [self.team.pk]

    def test_team_with_any_recent_job_is_auto_selected(self):
        """No threshold — a single job in the lookback window is enough."""
        self._create_job(self.team.pk, age_days=0)
        assert self.team.pk in _select_eager_teams()

    def test_team_with_only_old_jobs_is_not_selected(self):
        """LOOKBACK_DAYS bounds the scan; ancient jobs don't matter."""
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_LOOKBACK_DAYS", 7):
            self._create_job(self.team.pk, age_days=30)
            assert self.team.pk not in _select_eager_teams()

    def test_blocked_overrides_auto_selection(self):
        self._create_job(self.team.pk, age_days=0)
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS", [self.team.pk]):
            assert self.team.pk not in _select_eager_teams()

    def test_forced_added_even_without_any_jobs(self):
        with override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]):
            assert self.team.pk in _select_eager_teams()

    def test_asset_writes_constance_setting(self):
        """The Dagster asset writes its output to AUTO_SELECTED_TEAM_IDS so
        both `can_use_eager_precompute` and cache_warming can read it."""
        self._create_job(self.team.pk, age_days=0)
        web_analytics_eager_precompute_team_selection(build_asset_context())
        assert self.team.pk in (get_instance_setting("WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS") or [])

    def test_cache_warming_team_selection_includes_eager(self):
        """cache_warming.get_teams_enabled_for_web_analytics_cache_warming
        unions the static `WEB_ANALYTICS_WARMING_TEAMS_TO_WARM` allowlist with
        the eager-enrolled set. This is how the cache warmer picks up eager
        teams without a separate replay loop."""
        with (
            override_instance_config("WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS", [self.team.pk]),
            override_instance_config("WEB_ANALYTICS_WARMING_TEAMS_TO_WARM", []),
        ):
            assert self.team.pk in get_teams_enabled_for_web_analytics_cache_warming()
