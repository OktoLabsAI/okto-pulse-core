"""Tests for FR/AC canonicalization (spec 9d66847f, Phase 1).

Covers the 11 spec scenarios: canonicalization in create/update_spec, id
preservation + reorder, duplicate text -> distinct ids + idempotency, hash
collision, fixed ``_N`` suffix, duplicate ``dict.id`` fail-closed, ``status``
shape, anti-cycle smoke import, idempotent migrator + dry-run,
``resolve_linked_requirements_to_ids`` and the no-breaking-change regression.
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecCreate, SpecUpdate
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services.spec_entity_canonicalization import (
    DuplicateSpecChildIdError,
    _stable_child_id,
    canonicalize_fr_ac,
    spec_child_id,
    spec_child_text,
)
from okto_pulse.core.services.analytics_service import (
    resolve_linked_requirements_to_ids,
    resolve_linked_fr_indices,
)
from sqlalchemy_spec_materialization_store import (
    materialize_legacy_fr_ac_board,
)

USER = "c4-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ====================================================================
# Unit — canonicalize_fr_ac / _stable_child_id
# ====================================================================


class TestCanonicalizeUnit:
    def test_string_becomes_dict_with_id_and_status(self):
        out = canonicalize_fr_ac("functional_requirement", ["FR one", "FR two"])
        assert all(isinstance(x, dict) for x in out)
        assert [x["text"] for x in out] == ["FR one", "FR two"]
        assert all(x["id"].startswith("fr_") for x in out)
        assert all(x["status"] == "active" for x in out)  # ts_929a642c

    def test_none_returns_none(self):
        assert canonicalize_fr_ac("functional_requirement", None) is None

    def test_duplicate_text_without_id_distinct_and_idempotent(self):
        # ts_dc92950b
        out = canonicalize_fr_ac("functional_requirement", ["same", "same"])
        ids = [x["id"] for x in out]
        assert len(set(ids)) == 2, ids
        assert ids[1] == f"{ids[0]}_1"
        # re-run over the output preserves the same two ids (idempotent)
        again = canonicalize_fr_ac("functional_requirement", out)
        assert [x["id"] for x in again] == ids

    def test_dict_with_id_preserved_and_reorder_stable(self):
        # ts_39043a5b (pure level)
        existing = [
            {"id": "fr_aaaa1111", "text": "A", "status": "active"},
            {"id": "fr_bbbb2222", "text": "B", "status": "active"},
        ]
        # B as string (no id) + A as dict-with-id, reordered
        out = canonicalize_fr_ac(
            "functional_requirement",
            ["B", {"id": "fr_aaaa1111", "text": "A"}],
            existing_items=existing,
        )
        by_text = {x["text"]: x["id"] for x in out}
        assert by_text["A"] == "fr_aaaa1111"
        assert by_text["B"] == "fr_bbbb2222"  # reused via text queue, order-independent

    def test_status_preserved_when_present(self):
        # ts_929a642c
        out = canonicalize_fr_ac(
            "acceptance_criterion", [{"text": "X", "status": "revoked"}]
        )
        assert out[0]["status"] == "revoked"
        assert out[0]["id"].startswith("ac_")

    def test_extra_fields_preserved(self):
        out = canonicalize_fr_ac(
            "functional_requirement", [{"text": "X", "linked_task_ids": ["t1"]}]
        )
        assert out[0]["linked_task_ids"] == ["t1"]

    def test_duplicate_dict_id_is_fail_closed(self):
        # ts_2c038be6
        with pytest.raises(DuplicateSpecChildIdError):
            canonicalize_fr_ac(
                "functional_requirement",
                [{"id": "fr_abc", "text": "A"}, {"id": "fr_abc", "text": "B"}],
            )

    def test_stable_child_id_fixed_underscore_n_suffix(self):
        # ts_ed112fe8 — deterministic integer suffix _N
        used: set[str] = set()
        first = _stable_child_id("functional_requirement", "dup", used)
        used.add(first)
        second = _stable_child_id("functional_requirement", "dup", used)
        used.add(second)
        third = _stable_child_id("functional_requirement", "dup", used)
        assert second == f"{first}_1"
        assert third == f"{first}_2"

    def test_hash_collision_distinct_ids(self, monkeypatch):
        # ts_dfdfdb7c — force md5 to collide for two different texts
        import okto_pulse.core.services.spec_entity_canonicalization as canon

        class _FakeMd5:
            def __init__(self, *_a, **_k):
                pass

            def hexdigest(self):
                return "deadbeef" + "0" * 24

        monkeypatch.setattr(canon.hashlib, "md5", lambda *_a, **_k: _FakeMd5())
        out = canonicalize_fr_ac("functional_requirement", ["TEXT-A", "TEXT-B"])
        ids = [x["id"] for x in out]
        assert ids[0] == "fr_deadbeef"
        assert ids[1] == "fr_deadbeef_1"  # collision -> deterministic suffix
        assert len(set(ids)) == 2


# ====================================================================
# Unit — resolve_linked_requirements_to_ids
# ====================================================================


class TestResolveLinkedRequirementsToIds:
    _FRS = [
        {"id": "fr_1111aaaa", "text": "User can log in"},
        {"id": "fr_2222bbbb", "text": "Session expires"},
    ]

    def test_index_id_text_and_unresolved(self):
        # ts_76bd38de
        resolved, unresolved = resolve_linked_requirements_to_ids(
            ["0", "fr_2222bbbb", "User can log in", "ghost"], self._FRS
        )
        assert resolved == ["fr_1111aaaa", "fr_2222bbbb"]  # dedup, order
        assert unresolved == ["ghost"]


# ====================================================================
# Unit — anti-cycle smoke import (ts_df5e3a51)
# ====================================================================


def test_no_import_cycle_smoke():
    import okto_pulse.core.services.main as m
    import okto_pulse.core.services.spec_structured_entities as sse
    import okto_pulse.core.services.spec_entity_canonicalization as canon

    # leaf re-export still accessible from spec_structured_entities
    assert sse.spec_child_text is canon.spec_child_text
    assert sse.spec_child_id is canon.spec_child_id
    assert spec_child_text({"text": "x"}) == "x"
    assert spec_child_id({"id": "fr_x"}) == "fr_x"
    assert hasattr(m, "canonicalize_fr_ac")


# ====================================================================
# Integration — create_spec / update_spec
# ====================================================================


async def _seed_board(db_factory) -> str:
    board_id = _id("c4-board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="C4 Board", owner_id=USER, settings={}))
        await db.commit()
    return board_id


async def test_create_spec_canonicalizes(db_factory):
    # ts_4955808a
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            USER,
            SpecCreate(
                title="S",
                delivery_context="brownfield",
                functional_requirements=["FR one", "FR two"],
                acceptance_criteria=["AC one"],
            ),
        )
        await db.commit()
        fr = spec.functional_requirements
        ac = spec.acceptance_criteria
    assert [x["text"] for x in fr] == ["FR one", "FR two"]
    assert all(x["id"].startswith("fr_") for x in fr)
    assert ac[0]["id"].startswith("ac_") and ac[0]["text"] == "AC one"


async def test_update_spec_preserves_ids(db_factory):
    # ts_39043a5b
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            USER,
            SpecCreate(
                title="S",
                delivery_context="brownfield",
                functional_requirements=["A", "B"],
            ),
        )
        await db.commit()
        spec_id = spec.id
        original = {x["text"]: x["id"] for x in spec.functional_requirements}

    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id, USER, SpecUpdate(functional_requirements=["B", "A"])  # reordered strings
        )
        await db.commit()
        now = {x["text"]: x["id"] for x in updated.functional_requirements}
    assert now == original  # ids preserved despite reorder + string input


async def test_no_breaking_change_text_links(db_factory):
    # ts_fb5fcb7e — legacy linked_requirements by text still validate after canonicalization
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            USER,
            SpecCreate(
                title="S",
                delivery_context="brownfield",
                functional_requirements=["User can log in"],
                business_rules=[
                    {
                        "id": "br_x",
                        "title": "R",
                        "rule": "r",
                        "when": "w",
                        "then": "t",
                        "linked_requirements": ["User can log in"],  # by TEXT
                    }
                ],
            ),
        )
        await db.commit()
        spec_id = spec.id

    # An update that re-canonicalizes FR must not orphan the text-based BR link.
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id, USER, SpecUpdate(functional_requirements=["User can log in"])
        )
        await db.commit()
    # read-path still resolves the text link to the FR index
    assert resolve_linked_fr_indices(
        ["User can log in"], updated.functional_requirements
    ) == {0}


# ====================================================================
# Integration — migrator (ts_e0731b4b)
# ====================================================================


async def _seed_legacy_spec(db_factory, board_id, frs, acs) -> str:
    spec_id = _id("c4-spec")
    async with db_factory() as db:
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Legacy",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=frs,
                acceptance_criteria=acs,
            )
        )
        await db.commit()
    return spec_id


async def test_migrator_idempotent_and_dry_run(db_factory):
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(db_factory, board_id, ["FR a", "FR b"], ["AC a"])

    # dry-run does not persist
    async with db_factory() as db:
        summary = await materialize_legacy_fr_ac_board(db, board_id, dry_run=True)
    assert summary["scanned"] == 1 and summary["changed"] == 1 and summary["errors"] == 0
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        assert spec.functional_requirements == ["FR a", "FR b"]  # unchanged

    # real run persists
    async with db_factory() as db:
        await materialize_legacy_fr_ac_board(db, board_id, dry_run=False)
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        first_ids = [x["id"] for x in spec.functional_requirements]
    assert all(i.startswith("fr_") for i in first_ids)

    # second real run is idempotent (same ids, reported as skipped)
    async with db_factory() as db:
        summary2 = await materialize_legacy_fr_ac_board(db, board_id, dry_run=False)
    assert summary2["changed"] == 0 and summary2["skipped"] == 1
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        second_ids = [x["id"] for x in spec.functional_requirements]
    assert second_ids == first_ids


async def test_migrator_corrupted_spec_is_atomic(db_factory):
    # IMPL-3 fix (Codex): a spec where ONE field is corrupted (duplicate
    # dict.id) must not partially persist the other field.
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(
        db_factory,
        board_id,
        ["FR legacy"],  # would canonicalize
        [{"id": "ac_dup", "text": "A"}, {"id": "ac_dup", "text": "B"}],  # corrupted
    )

    async with db_factory() as db:
        summary = await materialize_legacy_fr_ac_board(db, board_id, dry_run=False)
    assert summary["errors"] == 1
    assert summary["changed"] == 0

    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
    # nothing persisted: FR is still the legacy string, AC is byte-identical
    assert spec.functional_requirements == ["FR legacy"]
    assert spec.acceptance_criteria == [
        {"id": "ac_dup", "text": "A"},
        {"id": "ac_dup", "text": "B"},
    ]
