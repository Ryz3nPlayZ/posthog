"""Tests for the `sync_signals_agent_skills` management command.

The underlying `sync_canonical_skills` is covered exhaustively in
`test_agent_harness_lazy_seed.py`; here we lock the command surface — argument
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
from products.signals.backend.agent_harness.lazy_seed import CanonicalSkill
from products.signals.backend.models import SignalAgentConfig


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


class TestSyncSignalsAgentSkillsCommand(BaseTest):
    def _patch_canonicals(self, canonicals):
        return patch(
            "products.signals.backend.agent_harness.lazy_seed.discover_canonical_skills",
            return_value=canonicals,
        )

    def test_requires_team_id_or_all_enabled(self) -> None:
        with pytest.raises(CommandError, match="--team-id"):
            call_command("sync_signals_agent_skills")

    def test_team_id_and_all_enabled_are_mutually_exclusive(self) -> None:
        with pytest.raises(CommandError, match="mutually exclusive"):
            call_command("sync_signals_agent_skills", "--team-id", str(self.team.id), "--all-enabled")

    def test_unknown_team_id_raises(self) -> None:
        with pytest.raises(CommandError, match="not found"):
            call_command("sync_signals_agent_skills", "--team-id", "999999999")

    def test_team_id_seeds_canonicals(self) -> None:
        canonical = _fake_canonical("signals-agent-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command("sync_signals_agent_skills", "--team-id", str(self.team.id), stdout=out)

        assert LLMSkill.objects.filter(team=self.team, name="signals-agent-alpha", is_latest=True).exists()
        output = out.getvalue()
        assert "+created" in output
        assert "signals-agent-alpha" in output

    def test_all_enabled_iterates_only_enabled_configs(self) -> None:
        # Two teams; only one has enabled config.
        from posthog.models import Team

        other_team = Team.objects.create(organization=self.organization, name="OtherTeam")
        SignalAgentConfig.objects.create(team=self.team, enabled=True)
        SignalAgentConfig.objects.create(team=other_team, enabled=False)

        canonical = _fake_canonical("signals-agent-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command("sync_signals_agent_skills", "--all-enabled", stdout=out)

        # Enabled team got the canonical.
        assert LLMSkill.objects.filter(team=self.team, name="signals-agent-alpha").exists()
        # Disabled team did not.
        assert not LLMSkill.objects.filter(team=other_team, name="signals-agent-alpha").exists()
        assert "Synced 1 team" in out.getvalue()

    def test_dry_run_does_not_persist(self) -> None:
        canonical = _fake_canonical("signals-agent-alpha")
        out = StringIO()
        with self._patch_canonicals((canonical,)):
            call_command(
                "sync_signals_agent_skills",
                "--team-id",
                str(self.team.id),
                "--dry-run",
                stdout=out,
            )

        # Nothing persisted.
        assert not LLMSkill.objects.filter(team=self.team, name="signals-agent-alpha").exists()
        output = out.getvalue()
        assert "[dry-run]" in output

    def test_no_op_team_renders_no_changes_line(self) -> None:
        # Pre-seed the team with canonical content + matching hash, then re-run sync. Should
        # be a clean no-op without any +/~/= line items.
        canonical = _fake_canonical("signals-agent-alpha")
        with self._patch_canonicals((canonical,)):
            # First call writes canonical + hash.
            call_command("sync_signals_agent_skills", "--team-id", str(self.team.id), stdout=StringIO())
            # Second call should be a clean no-op.
            out = StringIO()
            call_command("sync_signals_agent_skills", "--team-id", str(self.team.id), stdout=out)

        output = out.getvalue()
        assert "no changes" in output
