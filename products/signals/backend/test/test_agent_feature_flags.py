"""Tests for `agent_harness/feature_flags.py`.

Locks the contract between this module's gate (used by the coordinator + management
command) and `posthoganalytics.feature_enabled`: both group and group-properties are
populated, eval failures fail closed, and `only_evaluate_locally=True` so the gate
doesn't add an HTTP roundtrip on the hot path.
"""

from __future__ import annotations

from posthog.test.base import BaseTest
from unittest.mock import patch

from products.signals.backend.agent_harness.feature_flags import SIGNALS_AGENT_ROLLOUT_FLAG, team_passes_rollout_flag


class TestTeamPassesRolloutFlag(BaseTest):
    def test_returns_true_when_flag_evaluates_true(self) -> None:
        with patch(
            "products.signals.backend.agent_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=True,
        ):
            assert team_passes_rollout_flag(self.team) is True

    def test_returns_false_when_flag_evaluates_false(self) -> None:
        with patch(
            "products.signals.backend.agent_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=False,
        ):
            assert team_passes_rollout_flag(self.team) is False

    def test_returns_false_when_flag_evaluates_none(self) -> None:
        # `posthoganalytics.feature_enabled` returns None when local eval can't decide
        # (and `only_evaluate_locally=True` kept us off the network). bool(None) is False.
        with patch(
            "products.signals.backend.agent_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=None,
        ):
            assert team_passes_rollout_flag(self.team) is False

    def test_fails_closed_on_eval_exception(self) -> None:
        # Any exception during evaluation should return False — flag-eval failure must
        # never be an implicit allow.
        with (
            patch(
                "products.signals.backend.agent_harness.feature_flags.posthoganalytics.feature_enabled",
                side_effect=RuntimeError("posthoganalytics misconfigured"),
            ),
            patch(
                "products.signals.backend.agent_harness.feature_flags.capture_exception",
            ) as captured,
        ):
            assert team_passes_rollout_flag(self.team) is False
            captured.assert_called_once()

    def test_passes_team_uuid_organization_and_project_groups(self) -> None:
        with patch(
            "products.signals.backend.agent_harness.feature_flags.posthoganalytics.feature_enabled",
            return_value=True,
        ) as mock_eval:
            team_passes_rollout_flag(self.team)

        mock_eval.assert_called_once()
        args, kwargs = mock_eval.call_args
        assert args[0] == SIGNALS_AGENT_ROLLOUT_FLAG
        assert args[1] == str(self.team.uuid)
        assert kwargs["groups"] == {
            "organization": str(self.team.organization_id),
            "project": str(self.team.id),
        }
        assert kwargs["group_properties"] == {
            "organization": {"id": str(self.team.organization_id)},
            "project": {"id": str(self.team.id)},
        }
        # Local eval keeps the gate off the hot-path HTTP roundtrip.
        assert kwargs["only_evaluate_locally"] is True
        # No exposure events from the gate — eval is system-side, not user-attributable.
        assert kwargs["send_feature_flag_events"] is False
