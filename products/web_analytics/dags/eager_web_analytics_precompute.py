"""Auto-select teams for the web analytics eager precompute path.

This is a thin Dagster asset that runs daily, mines the `PreaggregationJob`
table for distinct team IDs with recent activity, and writes the result back
to the `WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS` Constance
setting. Two callers read that setting:

1. **`can_use_eager_precompute`** (in `web_analytics_lazy_precompute.py`) —
   the query-path gate that lets enrolled teams bypass the lazy rollout
   (org-FF + per-query opt-in).

2. **`get_teams_enabled_for_web_analytics_cache_warming`** (in
   `cache_warming.py`) — the existing cache warmer reads this set, unions
   it with its static allowlist, and replays the enrolled teams' actual
   queries every hour. That replay hits the eager gate and populates the
   lazy precompute cache without any per-team-per-cycle fan-out logic.

The reason this asset is so small: we do NOT need to synthesize
(date_range × host_filter × test_account_filter) combinations to pre-warm.
The cache warmer is already replaying the team's actual queries from
`metrics_query_log_mv`, so the warmed combinations are exactly the
combinations users hit. Any team that already produces preagg jobs is a
team that benefits from eager warming.
"""

from datetime import timedelta

from django.utils import timezone as django_timezone

import dagster
import structlog
from prometheus_client import Gauge

from posthog.dags.common import JobOwners
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


_AUTO_SELECTED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_AUTO_SELECTED_TEAM_IDS"
_FORCED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_FORCED_TEAM_IDS"
_BLOCKED_SETTING = "WEB_ANALYTICS_EAGER_PRECOMPUTE_BLOCKED_TEAM_IDS"


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
