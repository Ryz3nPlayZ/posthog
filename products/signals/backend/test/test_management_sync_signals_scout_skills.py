"""Tests for the `sync_signals_scout_skills` management command.

The underlying `sync_canonical_skills` is covered exhaustively in
`test_scout_harness_lazy_seed.py`; here we lock the command surface — argument
plumbing, team selection, dry-run rollback, output formatting.
"""

from __future__ import annotations

from io import StringIO

import pytest
from posthog.test.base import BaseTest
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError

from products.llm_analytics.backend.models.skills import LLMSkill
from products.signals.backend.models import SignalScoutConfig
from products.signals.backend.scout_harness.lazy_seed import CanonicalSkill


def _fake_canonical(name: str, body: str = "# canonical body\n") -> CanonicalSkill:
    from pathlib import Path

    return CanonicalSkill(
        name=name,
        description="test canonical",
        body=body,
        allowed_tools=(),
        files=(),
        source_path=Path("/tmp/fake"),
    )


class TestSyncSignalsScoutSkillsCommand(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        # Default the rollout flag ON for every test that doesn't explicitly exercise
        # the gate. The flag-off and --force paths are covered in the dedicated tests
        # at the bottom of this module; everything above asserts on argument plumbing
        # / sync output and shouldn't be coupled to flag-eval state.
        self._rollout_flag_patcher = patch(
            "products.signals.backend.management.commands.sync_signals_scout_skills.team_passes_rollout_flag",
            return_value=True,
        )
        self._rollout_flag_patcher.start()
        self.addCleanup(self._rollout_flag_patcher.stop)

    def _patch_canonicals(self, canonicals):
        return patch(
            "products.signals.backend.scout_harness.lazy_seed.discover_canonical_skills",
            return_value=canonicals,
        )

    def _patch_rollout_flag(self, *, enabled: bool):
        return patch(
            "products.signals.backend.management.commands.sync_signals_scout_skills.team_passes_rollout_flag",
            return_value=enabled,
        )

    def test_requires_team_id_or_all_enabled(self) -> None:
        with pytest.raises(CommandError, match="--team-id"):
            call_command("sync_signals_scout_skills")

    def test_team_id_and_all_enabled_are_mutually_exclusive(self) -> None:
        with pytest.raises(CommandError, match="mutually exclusive"):
            call_command("sync_signals_scout_skills", "--team-id", str(self.team.id), "--all-enabled")

    def test_unknown_team_id_raises(self) -> None:
        with pytest.raises(CommandError, match="not found"):
            call_command("sync_signals_scout_skills", "--team-id", "999999999")

    def test_team_id_seeds_canonicals(self) -> None:
        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command("sync_signals_scout_skills", "--team-id", str(self.team.id), stdout=out)

        assert LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha", is_latest=True).exists()
        output = out.getvalue()
        assert "+created" in output
        assert "signals-scout-alpha" in output

    def test_all_enabled_iterates_only_enabled_configs(self) -> None:
        # Two teams; only one has enabled config.
        from posthog.models import Team

        other_team = Team.objects.create(organization=self.organization, name="OtherTeam")
        SignalScoutConfig.objects.create(team=self.team, enabled=True)
        SignalScoutConfig.objects.create(team=other_team, enabled=False)

        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command("sync_signals_scout_skills", "--all-enabled", stdout=out)

        # Enabled team got the canonical.
        assert LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha").exists()
        # Disabled team did not.
        assert not LLMSkill.objects.filter(team=other_team, name="signals-scout-alpha").exists()
        assert "Synced 1 team" in out.getvalue()

    def test_dry_run_does_not_persist(self) -> None:
        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command(
                "sync_signals_scout_skills",
                "--team-id",
                str(self.team.id),
                "--dry-run",
                stdout=out,
            )

        # Nothing persisted.
        assert not LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha").exists()
        output = out.getvalue()
        assert "[dry-run]" in output

    def test_no_op_team_renders_no_changes_line(self) -> None:
        # Pre-seed the team with canonical content + matching hash, then re-run sync. Should
        # be a clean no-op without any +/~/= line items.
        canonical = _fake_canonical("signals-scout-alpha")
        with self._patch_canonicals((canonical,)):
            # First call writes canonical + hash.
            call_command("sync_signals_scout_skills", "--team-id", str(self.team.id), stdout=StringIO())
            # Second call should be a clean no-op.
            out = StringIO()
            call_command("sync_signals_scout_skills", "--team-id", str(self.team.id), stdout=out)

        output = out.getvalue()
        assert "no changes" in output

    def test_rollout_flag_off_skips_team_without_force(self) -> None:
        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        # Stop the autouse default-on patcher and replace with a flag-off patch.
        self._rollout_flag_patcher.stop()
        try:
            with self._patch_rollout_flag(enabled=False), self._patch_canonicals((canonical,)):
                call_command("sync_signals_scout_skills", "--team-id", str(self.team.id), stdout=out)
        finally:
            # Restart the autouse patch for tearDown's addCleanup contract; the original
            # registered cleanup is stop(), and stop() is idempotent on already-stopped.
            self._rollout_flag_patcher.start()

        # Nothing persisted, and the per-team gated-skip line is in the output.
        assert not LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha").exists()
        output = out.getvalue()
        assert f"team {self.team.id}: skipped — gated by signals-scout rollout flag" in output
        assert "All matched teams are gated off" in output

    def test_rollout_flag_off_with_force_bypasses_gate(self) -> None:
        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        self._rollout_flag_patcher.stop()
        try:
            with self._patch_rollout_flag(enabled=False), self._patch_canonicals((canonical,)):
                call_command(
                    "sync_signals_scout_skills",
                    "--team-id",
                    str(self.team.id),
                    "--force",
                    stdout=out,
                )
        finally:
            self._rollout_flag_patcher.start()

        # --force bypasses the gate: canonical lands and the gated-skip line is absent.
        assert LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha").exists()
        output = out.getvalue()
        assert "gated by signals-scout rollout flag" not in output
        assert "+created" in output

    def test_all_enabled_filters_to_flag_passing_teams(self) -> None:
        # Two teams enabled in config; flag passes only on `self.team`. The flag-off team
        # is reported as gated-skip and is not synced; the flag-on team is.
        from posthog.models import Team

        flagged_off_team = Team.objects.create(organization=self.organization, name="FlaggedOffTeam")
        SignalScoutConfig.objects.create(team=self.team, enabled=True)
        SignalScoutConfig.objects.create(team=flagged_off_team, enabled=True)

        canonical = _fake_canonical("signals-scout-alpha")
        out = StringIO()
        self._rollout_flag_patcher.stop()
        try:
            with (
                patch(
                    "products.signals.backend.management.commands.sync_signals_scout_skills.team_passes_rollout_flag",
                    side_effect=lambda team: team.id == self.team.id,
                ),
                self._patch_canonicals((canonical,)),
            ):
                call_command("sync_signals_scout_skills", "--all-enabled", stdout=out)
        finally:
            self._rollout_flag_patcher.start()

        assert LLMSkill.objects.filter(team=self.team, name="signals-scout-alpha").exists()
        assert not LLMSkill.objects.filter(team=flagged_off_team, name="signals-scout-alpha").exists()
        output = out.getvalue()
        assert f"team {flagged_off_team.id}: skipped — gated by signals-scout rollout flag" in output
        assert "Synced 1 team" in output
