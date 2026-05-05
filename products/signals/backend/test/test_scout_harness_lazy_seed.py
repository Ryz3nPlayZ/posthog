from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from posthog.test.base import BaseTest
from unittest.mock import patch

from products.llm_analytics.backend.models.skills import LLMSkill, LLMSkillFile
from products.signals.backend.agent_harness.lazy_seed import (
    CanonicalSkill,
    CanonicalSkillFile,
    CanonicalSkillParseError,
    SyncResult,
    _compute_canonical_hash,
    _compute_row_hash,
    discover_canonical_skills,
    seed_canonical_skills,
    sync_canonical_skills,
)
from products.signals.backend.agent_harness.skill_loader import load_skill_for_run


def _write_canonical_skill(
    base: Path,
    *,
    dir_name: str,
    frontmatter: str,
    body: str = "# Body\n",
    bundled_files: dict[str, str] | None = None,
) -> Path:
    skill_dir = base / dir_name
    skill_dir.mkdir(parents=True, exist_ok=True)
    skill_md = textwrap.dedent(frontmatter).strip() + "\n" + body
    (skill_dir / "SKILL.md").write_text(skill_md, encoding="utf-8")
    for rel_path, content in (bundled_files or {}).items():
        target = skill_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return skill_dir


def _make_canonical(
    name: str,
    *,
    description: str = "test skill",
    body: str = "# Body\n",
    allowed_tools: tuple[str, ...] = (),
    files: tuple[CanonicalSkillFile, ...] = (),
) -> CanonicalSkill:
    """Build a CanonicalSkill for a unit test without going through disk + frontmatter."""
    return CanonicalSkill(
        name=name,
        description=description,
        body=body,
        allowed_tools=allowed_tools,
        files=files,
        source_path=Path("/tmp/fake"),
    )


class TestDiscoverCanonicalSkills:
    def test_returns_empty_for_missing_dir(self, tmp_path: Path) -> None:
        assert discover_canonical_skills(tmp_path / "does-not-exist") == ()

    def test_walks_signals_agent_prefix_skills_only(self, tmp_path: Path) -> None:
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-agent-foo",
            frontmatter="""
                ---
                name: signals-agent-foo
                description: foo skill
                ---
            """,
            body="# Foo\n",
        )
        _write_canonical_skill(
            tmp_path,
            dir_name="some-other-skill",
            frontmatter="""
                ---
                name: some-other-skill
                description: not a signals-agent
                ---
            """,
            body="# nope\n",
        )
        skills = discover_canonical_skills(tmp_path)
        assert [s.name for s in skills] == ["signals-agent-foo"]

    @pytest.mark.parametrize(
        "frontmatter_key",
        [
            # Backwards-compat form. Predates the agentskills.io spec alignment in this
            # codebase and is still in use by other PHS skills.
            "allowed_tools",
            # Spec form per agentskills.io — preferred for new canonical skills.
            "allowed-tools",
        ],
    )
    def test_parses_allowed_tools_in_either_frontmatter_form(self, tmp_path: Path, frontmatter_key: str) -> None:
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-agent-bar",
            frontmatter=f"""
                ---
                name: signals-agent-bar
                description: bar skill
                {frontmatter_key}:
                  - remember
                  - search_memory
                ---
            """,
            body="# Bar\n",
        )
        skills = discover_canonical_skills(tmp_path)
        assert skills[0].allowed_tools == ("remember", "search_memory")

    def test_rejects_both_allowed_tools_keys_set(self, tmp_path: Path) -> None:
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-agent-bar",
            frontmatter="""
                ---
                name: signals-agent-bar
                description: bar skill
                allowed-tools:
                  - remember
                allowed_tools:
                  - search_memory
                ---
            """,
            body="# Bar\n",
        )
        with pytest.raises(CanonicalSkillParseError, match="both 'allowed-tools' and 'allowed_tools'"):
            discover_canonical_skills(tmp_path)

    def test_parses_bundled_files_under_allowed_subdirs(self, tmp_path: Path) -> None:
        # `_ALLOWED_BUNDLE_SUBDIRS` is kept in lockstep with `hogli build:skills` —
        # `references/` and `scripts/` only. `assets/` and any other subdir are intentionally
        # ignored: silently bundling them here while the AI plugin build skips them would
        # produce different runtime behavior from the same source skill.
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-agent-bar",
            frontmatter="""
                ---
                name: signals-agent-bar
                description: bar skill
                ---
            """,
            body="# Bar\n",
            bundled_files={
                "references/playbook.md": "# Playbook\n",
                "scripts/check.py": "print('hi')\n",
                "assets/template.txt": "hello {{name}}\n",
                "extras/notes.txt": "ignored\n",
            },
        )
        skills = discover_canonical_skills(tmp_path)
        files_by_path = {f.path: f for f in skills[0].files}
        assert "references/playbook.md" in files_by_path
        assert files_by_path["references/playbook.md"].content == "# Playbook\n"
        assert "scripts/check.py" in files_by_path
        # Files outside the allowlist must not leak in — guards the consumer divergence.
        assert "assets/template.txt" not in files_by_path
        assert "extras/notes.txt" not in files_by_path

    def test_missing_frontmatter_raises(self, tmp_path: Path) -> None:
        skill_dir = tmp_path / "signals-agent-foo"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("# no frontmatter\n", encoding="utf-8")
        with pytest.raises(CanonicalSkillParseError):
            discover_canonical_skills(tmp_path)

    def test_wrong_name_prefix_in_frontmatter_raises(self, tmp_path: Path) -> None:
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-agent-bar",
            frontmatter="""
                ---
                name: not-prefixed
                description: bar skill
                ---
            """,
            body="# Bar\n",
        )
        with pytest.raises(CanonicalSkillParseError):
            discover_canonical_skills(tmp_path)

    def test_in_repo_canonical_set_parses_cleanly(self) -> None:
        # Exercises the production manifest at `products/signals/skills/` — growing the
        # canonical set is a deliberate edit, so this serves as the lock.
        skills = discover_canonical_skills()
        names = {s.name for s in skills}
        # The fleet at v1: general (cross-product) + 4 focused specialists. Each is
        # self-contained (no deps between skills); the coordinator samples one per tick.
        # Adding a new specialist is a deliberate edit — extend this set when shipping.
        expected = {
            "signals-agent-general",
            "signals-agent-llm-analytics",
            "signals-agent-logs",
            "signals-agent-error-tracking",
            "signals-agent-revenue-analytics",
        }
        assert expected.issubset(names), f"missing canonical skills: {expected - names}"

    def test_oversized_body_raises(self, tmp_path: Path) -> None:
        # Body byte limit mirrors the REST API contract (MAX_SKILL_BODY_BYTES = 1 MB).
        # A canonical too big to seed should fail at parse time, not on DB write.
        body = "x" * (1_000_001)
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-scout-big-body",
            frontmatter="""
                ---
                name: signals-scout-big-body
                description: oversized body
                ---
            """,
            body=body,
        )
        with pytest.raises(CanonicalSkillParseError, match="byte limit"):
            discover_canonical_skills(tmp_path)

    def test_oversized_bundled_file_raises(self, tmp_path: Path) -> None:
        # Per-file byte limit mirrors MAX_SKILL_FILE_BYTES (1 MB).
        oversized = "x" * (1_000_001)
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-scout-big-file",
            frontmatter="""
                ---
                name: signals-scout-big-file
                description: oversized bundled file
                ---
            """,
            body="# Body\n",
            bundled_files={"references/huge.md": oversized},
        )
        with pytest.raises(CanonicalSkillParseError, match="byte limit"):
            discover_canonical_skills(tmp_path)

    def test_too_many_bundled_files_raises(self, tmp_path: Path) -> None:
        # File count limit mirrors MAX_SKILL_FILE_COUNT (50).
        bundled = {f"references/file_{i:03d}.md": f"# file {i}\n" for i in range(51)}
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-scout-too-many",
            frontmatter="""
                ---
                name: signals-scout-too-many
                description: too many files
                ---
            """,
            body="# Body\n",
            bundled_files=bundled,
        )
        with pytest.raises(CanonicalSkillParseError, match="exceeding the 50 limit"):
            discover_canonical_skills(tmp_path)

    def test_overlong_path_raises(self, tmp_path: Path) -> None:
        # Path length matches LLMSkillFile.path max_length (500). Friendly parse-time
        # error beats the Postgres `value too long for type character varying(500)`.
        # Nested-dir construction because macOS rejects single filename segments >255 chars.
        nested_seg = "a" * 170
        rel_path = f"references/{nested_seg}/{nested_seg}/{nested_seg}/file.md"
        assert len(rel_path) > 500
        _write_canonical_skill(
            tmp_path,
            dir_name="signals-scout-long-path",
            frontmatter="""
                ---
                name: signals-scout-long-path
                description: overlong path
                ---
            """,
            body="# Body\n",
            bundled_files={rel_path: "x"},
        )
        with pytest.raises(CanonicalSkillParseError, match="char limit"):
            discover_canonical_skills(tmp_path)


class TestComputeCanonicalHash:
    def test_same_input_yields_same_hash(self) -> None:
        a = _make_canonical("signals-agent-foo", body="hello", allowed_tools=("a", "b"))
        b = _make_canonical("signals-agent-foo", body="hello", allowed_tools=("a", "b"))
        assert _compute_canonical_hash(a) == _compute_canonical_hash(b)

    def test_body_change_changes_hash(self) -> None:
        a = _make_canonical("signals-agent-foo", body="v1")
        b = _make_canonical("signals-agent-foo", body="v2")
        assert _compute_canonical_hash(a) != _compute_canonical_hash(b)

    def test_description_change_changes_hash(self) -> None:
        a = _make_canonical("signals-agent-foo", description="alpha", body="x")
        b = _make_canonical("signals-agent-foo", description="beta", body="x")
        assert _compute_canonical_hash(a) != _compute_canonical_hash(b)

    def test_allowed_tools_reorder_does_not_change_hash(self) -> None:
        # Sorted internally so frontmatter-listing-order changes don't churn the hash.
        a = _make_canonical("signals-agent-foo", allowed_tools=("a", "b"))
        b = _make_canonical("signals-agent-foo", allowed_tools=("b", "a"))
        assert _compute_canonical_hash(a) == _compute_canonical_hash(b)

    def test_bundle_change_changes_hash(self) -> None:
        # References-only edits are the easy thing to forget; this is the lock.
        f1 = (CanonicalSkillFile(path="references/x.md", content="v1"),)
        f2 = (CanonicalSkillFile(path="references/x.md", content="v2"),)
        a = _make_canonical("signals-agent-foo", files=f1)
        b = _make_canonical("signals-agent-foo", files=f2)
        assert _compute_canonical_hash(a) != _compute_canonical_hash(b)

    def test_canonical_and_row_hashes_agree_when_content_matches(self) -> None:
        """When a row's content matches the canonical exactly, the two hashing helpers
        produce the same digest. This is the round-trip the sync function depends on."""
        canonical = _make_canonical(
            "signals-agent-foo",
            description="d",
            body="b",
            allowed_tools=("x", "y"),
            files=(CanonicalSkillFile(path="references/r.md", content="r"),),
        )
        # Build a fake LLMSkill / LLMSkillFile pair that mirrors the canonical exactly. We
        # avoid hitting the DB for this — _compute_row_hash only reads attributes.
        skill = LLMSkill(
            description=canonical.description,
            body=canonical.body,
            allowed_tools=list(canonical.allowed_tools),
        )
        files = [LLMSkillFile(path=f.path, content=f.content, content_type=f.content_type) for f in canonical.files]
        assert _compute_canonical_hash(canonical) == _compute_row_hash(skill, files)


class TestSyncCanonicalSkills(BaseTest):
    """End-to-end behavior of the canonical-sync function on a real DB.

    Each test patches `discover_canonical_skills` so we control what "canonical" means
    rather than depending on the in-repo fleet content. The in-repo behavior is locked
    by `test_in_repo_canonical_set_parses_cleanly` above.
    """

    def _patch_canonicals(self, canonicals: tuple[CanonicalSkill, ...]):
        return patch(
            "products.signals.backend.agent_harness.lazy_seed.discover_canonical_skills",
            return_value=canonicals,
        )

    def test_creates_rows_for_brand_new_team(self) -> None:
        canonical = _make_canonical("signals-agent-alpha", body="initial")
        with self._patch_canonicals((canonical,)):
            result = sync_canonical_skills(self.team)

        assert result.created_skill_names == ("signals-agent-alpha",)
        assert result.updated_skill_names == ()
        row = LLMSkill.objects.get(team=self.team, name="signals-agent-alpha", is_latest=True, deleted=False)
        assert row.body == "initial"
        assert row.metadata["seeded_by"] == "signals_agent_harness"
        # Hash is now stamped at create time so future syncs can compare.
        assert row.metadata["canonical_hash"] == _compute_canonical_hash(canonical)

    def test_no_op_when_team_row_already_matches_canonical(self) -> None:
        canonical = _make_canonical("signals-agent-alpha", body="initial")
        with self._patch_canonicals((canonical,)):
            sync_canonical_skills(self.team)
            # Second call against unchanged canonical produces no further work.
            result = sync_canonical_skills(self.team)

        assert result.created_skill_names == ()
        assert result.updated_skill_names == ()
        assert LLMSkill.objects.filter(team=self.team, name="signals-agent-alpha", is_latest=True).count() == 1

    def test_updates_when_canonical_changes_and_team_has_not_edited(self) -> None:
        # Initial sync writes v1 with the original content.
        v1 = _make_canonical("signals-agent-alpha", body="v1 body")
        with self._patch_canonicals((v1,)):
            sync_canonical_skills(self.team)

        # We ship a SKILL.md change. Same name, different body. Team hasn't touched theirs.
        v2 = _make_canonical("signals-agent-alpha", body="v2 body — improved scout calibration")
        with self._patch_canonicals((v2,)):
            result = sync_canonical_skills(self.team)

        assert result.updated_skill_names == ("signals-agent-alpha",)
        # Old row demoted, new row at version=2 with the new content.
        rows = LLMSkill.objects.filter(team=self.team, name="signals-agent-alpha").order_by("version")
        assert [r.version for r in rows] == [1, 2]
        latest = rows.get(version=2)
        assert latest.is_latest is True
        assert latest.body == "v2 body — improved scout calibration"
        assert latest.metadata["canonical_hash"] == _compute_canonical_hash(v2)
        # Old row is preserved as version history but no longer latest.
        old = rows.get(version=1)
        assert old.is_latest is False
        assert old.body == "v1 body"

    def test_leaves_diverged_team_edits_alone(self) -> None:
        v1 = _make_canonical("signals-agent-alpha", body="v1 body")
        with self._patch_canonicals((v1,)):
            sync_canonical_skills(self.team)

        # Simulate a user edit: bump the row's body without touching the canonical_hash
        # in metadata. Real PHS edits would do the version-bump dance; for the test we
        # mutate in place since the hash mismatch is what matters.
        row = LLMSkill.objects.get(team=self.team, name="signals-agent-alpha", is_latest=True)
        row.body = "team edited this"
        row.save()

        # Now we ship a v2 canonical. Team's content drifted from stored hash → diverged.
        v2 = _make_canonical("signals-agent-alpha", body="v2 body")
        with self._patch_canonicals((v2,)):
            result = sync_canonical_skills(self.team)

        assert result.diverged_skill_names == ("signals-agent-alpha",)
        assert result.updated_skill_names == ()
        # Team's edit survived.
        latest = LLMSkill.objects.get(team=self.team, name="signals-agent-alpha", is_latest=True)
        assert latest.body == "team edited this"

    def test_skips_tombstoned_rows(self) -> None:
        # Team explicitly deleted the skill — no live row, just a soft-deleted archive.
        # The sync must respect that and not re-create the canonical content.
        LLMSkill.objects.create(
            team=self.team,
            name="signals-agent-alpha",
            description="archived",
            body="team body",
            deleted=True,
            is_latest=False,
        )
        canonical = _make_canonical("signals-agent-alpha", body="latest canonical")
        with self._patch_canonicals((canonical,)):
            result = sync_canonical_skills(self.team)

        assert result.tombstoned_skill_names == ("signals-agent-alpha",)
        assert result.created_skill_names == ()
        assert not LLMSkill.objects.filter(
            team=self.team, name="signals-agent-alpha", deleted=False, is_latest=True
        ).exists()

    def test_creates_new_specialist_for_already_seeded_team(self) -> None:
        # A team got seeded before we shipped a new specialist. Per-canonical iteration
        # means the new one shows up; the existing ones are no-ops.
        existing = _make_canonical("signals-agent-alpha", body="alpha body")
        with self._patch_canonicals((existing,)):
            sync_canonical_skills(self.team)

        new_specialist = _make_canonical("signals-agent-beta", body="beta body")
        with self._patch_canonicals((existing, new_specialist)):
            result = sync_canonical_skills(self.team)

        assert result.created_skill_names == ("signals-agent-beta",)
        assert result.updated_skill_names == ()
        assert LLMSkill.objects.filter(team=self.team, name="signals-agent-beta", is_latest=True).exists()

    def test_backfills_canonical_hash_on_pre_hash_rows(self) -> None:
        # Simulate a pre-existing row from the seed-only era: harness-seeded but missing
        # `canonical_hash` in metadata. Sync should write a baseline equal to the row's
        # current content hash and skip any update decision for this tick.
        row = LLMSkill.objects.create(
            team=self.team,
            name="signals-agent-alpha",
            description="legacy",
            body="legacy body",
            metadata={"seeded_by": "signals_agent_harness", "source": "products/signals/skills"},
        )
        canonical = _make_canonical("signals-agent-alpha", description="legacy", body="legacy body")
        with self._patch_canonicals((canonical,)):
            result = sync_canonical_skills(self.team)

        assert result.backfilled_skill_names == ("signals-agent-alpha",)
        assert result.updated_skill_names == ()
        row.refresh_from_db()
        assert row.metadata.get("canonical_hash") == _compute_row_hash(row, list(row.files.all()))

    def test_backfilled_row_picks_up_canonical_change_on_next_tick(self) -> None:
        # Tick 1: backfill. Tick 2: canonical change → update path.
        row = LLMSkill.objects.create(
            team=self.team,
            name="signals-agent-alpha",
            description="d",
            body="legacy body",
            metadata={"seeded_by": "signals_agent_harness", "source": "products/signals/skills"},
        )
        v1 = _make_canonical("signals-agent-alpha", description="d", body="legacy body")
        with self._patch_canonicals((v1,)):
            sync_canonical_skills(self.team)
        row.refresh_from_db()
        assert row.metadata.get("canonical_hash")  # backfilled

        v2 = _make_canonical("signals-agent-alpha", description="d", body="latest canonical body")
        with self._patch_canonicals((v2,)):
            result = sync_canonical_skills(self.team)

        assert result.updated_skill_names == ("signals-agent-alpha",)
        latest = LLMSkill.objects.get(team=self.team, name="signals-agent-alpha", is_latest=True)
        assert latest.body == "latest canonical body"
        assert latest.version == 2

    def test_bundle_only_change_triggers_update(self) -> None:
        # Editing only references/* should still propagate. Easy to miss if the hash
        # only covered SKILL.md body.
        v1 = _make_canonical(
            "signals-agent-alpha",
            body="same body",
            files=(CanonicalSkillFile(path="references/calib.md", content="v1"),),
        )
        with self._patch_canonicals((v1,)):
            sync_canonical_skills(self.team)

        v2 = _make_canonical(
            "signals-agent-alpha",
            body="same body",
            files=(CanonicalSkillFile(path="references/calib.md", content="v2"),),
        )
        with self._patch_canonicals((v2,)):
            result = sync_canonical_skills(self.team)

        assert result.updated_skill_names == ("signals-agent-alpha",)
        latest = LLMSkill.objects.get(team=self.team, name="signals-agent-alpha", is_latest=True)
        bundle = {f.path: f.content for f in latest.files.all()}
        assert bundle["references/calib.md"] == "v2"

    def test_returns_skipped_reason_when_no_canonicals_on_disk(self) -> None:
        with self._patch_canonicals(()):
            result = sync_canonical_skills(self.team)
        assert result.skipped_reason == "no canonical signals-agent-* skills on disk"
        assert result.created_skill_names == ()

    def test_unrelated_team_skill_is_not_touched(self) -> None:
        # A row whose name doesn't match any canonical (and isn't even prefix-matched)
        # is invisible to the sync — the whole loop is keyed on canonical.name.
        LLMSkill.objects.create(
            team=self.team,
            name="custom-team-skill",
            description="custom",
            body="custom body",
        )
        canonical = _make_canonical("signals-agent-alpha", body="canonical body")
        with self._patch_canonicals((canonical,)):
            sync_canonical_skills(self.team)
        custom = LLMSkill.objects.get(team=self.team, name="custom-team-skill")
        assert custom.body == "custom body"

    def test_returns_sync_result_dataclass(self) -> None:
        # Light shape lock so external callers (management command, coordinator) keep
        # access to all five outcome buckets.
        canonical = _make_canonical("signals-agent-alpha", body="x")
        with self._patch_canonicals((canonical,)):
            result = sync_canonical_skills(self.team)
        assert isinstance(result, SyncResult)
        assert hasattr(result, "created_skill_names")
        assert hasattr(result, "updated_skill_names")
        assert hasattr(result, "diverged_skill_names")
        assert hasattr(result, "tombstoned_skill_names")
        assert hasattr(result, "backfilled_skill_names")


class TestSeedCanonicalSkillsAlias(BaseTest):
    """`seed_canonical_skills` is kept as a thin alias for `sync_canonical_skills` so older
    callsites and external consumers don't break. These tests pin that contract."""

    def test_alias_returns_sync_result(self) -> None:
        result = seed_canonical_skills(self.team)
        assert isinstance(result, SyncResult)

    def test_alias_seeds_real_in_repo_canonicals(self) -> None:
        # No mocking — exercises the real `products/signals/skills/` manifest end-to-end
        # so the in-repo fleet stays loadable and seedable. Equivalent to the legacy
        # "first seed creates rows" invariant.
        result = seed_canonical_skills(self.team)
        assert "signals-agent-general" in result.created_skill_names
        seeded = LLMSkill.objects.get(team=self.team, name="signals-agent-general", is_latest=True)
        assert seeded.body
        assert seeded.metadata["seeded_by"] == "signals_agent_harness"
        assert seeded.metadata.get("canonical_hash")

    def test_seeded_skill_is_loadable_via_load_skill_for_run(self) -> None:
        seed_canonical_skills(self.team)
        loaded = load_skill_for_run(self.team, "signals-agent-general")
        assert loaded.name == "signals-agent-general"
        assert loaded.version == 1
        assert "Signals scout" in loaded.body
