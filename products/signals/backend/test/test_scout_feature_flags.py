"""Tests for `scout_harness/feature_flags.py`.

Locks the contract between this module's gate (used by the coordinator + management
command) and `posthoganalytics.feature_enabled`: both group and group-properties are
populated, eval is remote (no `only_evaluate_locally`, so a full-rollout or
property-targeted flag actually decides instead of coming back `None`), and eval
failures fail closed.
"""

from __future__ import annotations

from posthog.test.base import BaseTest
from unittest.mock import patch

from products.signals.backend.scout_harness.feature_flags import SIGNALS_SCOUT_ROLLOUT_FLAG, team_passes_rollout_flag


class TestTeamPassesRolloutFlag(BaseTest):
    def test_returns_true_when_flag_evaluates_true(self) -> None:
        with patch(
            "products.signals.backend.scout_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=True,
        ):
            assert team_passes_rollout_flag(self.team) is True

    def test_returns_false_when_flag_evaluates_false(self) -> None:
        with patch(
            "products.signals.backend.scout_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=False,
        ):
            assert team_passes_rollout_flag(self.team) is False

    def test_returns_false_when_flag_evaluates_none(self) -> None:
        # Defensive: a None decision (shouldn't happen for a defined flag under remote eval)
        # still coerces to False via bool(None), so the gate never soft-defaults on.
        with patch(
            "products.signals.backend.scout_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=None,
        ):
            assert team_passes_rollout_flag(self.team) is False

    def test_fails_closed_on_eval_exception(self) -> None:
        # Any exception during evaluation should return False — flag-eval failure must
        # never be an implicit allow.
        with (
            patch(
                "products.signals.backend.scout_harness.feature_flags.posthoganalytics.feature_enabled",
                side_effect=RuntimeError("posthoganalytics misconfigured"),
            ),
            patch(
                "products.signals.backend.scout_harness.feature_flags.capture_exception",
            ) as captured,
        ):
            assert team_passes_rollout_flag(self.team) is False
            captured.assert_called_once()

    def test_passes_team_uuid_organization_and_project_groups(self) -> None:
        with patch(
            "products.signals.backend.scout_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=True,
        ) as mock_eval:
            team_passes_rollout_flag(self.team)

        mock_eval.assert_called_once()
        args, kwargs = mock_eval.call_args
        assert args[0] == SIGNALS_SCOUT_ROLLOUT_FLAG
        assert args[1] == str(self.team.uuid)
        assert kwargs["groups"] == {
            "organization": str(self.team.organization_id),
            "project": str(self.team.id),
        }
        assert kwargs["group_properties"] == {
            "organization": {"id": str(self.team.organization_id)},
            "project": {"id": str(self.team.id)},
        }
        # Remote eval — we don't pass only_evaluate_locally, so a full-rollout /
        # property-targeted flag decides instead of returning None and failing closed.
        assert "only_evaluate_locally" not in kwargs
        # No exposure events from the gate — eval is system-side, not user-attributable.
        assert kwargs["send_feature_flag_events"] is False
