"""Behavioral tests for IMPL-4 (spec c61569b2):

AC5 — FR4 non-regression: a spec persisted with linked_requirements in
      INDEX (legacy) resolves correctly in spec_coverage_summary AND passes
      the _validate_spec_linked_refs gate, with identical results to the
      fr_id case.

AC6 — FR5 lazy migration: update_spec on a legacy spec (refs in index)
      rewrites linked_requirements/linked_criteria to canonical fr_/ac_ ids;
      a spec NOT touched by update_spec retains the raw index refs in the DB.
"""
from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecUpdate
from okto_pulse.core.services.analytics_service import spec_coverage_summary
from okto_pulse.core.services.main import SpecService


USER = "impl4-agent"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def _seed_board(db_factory) -> str:
    board_id = _id("impl4-board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="IMPL-4 Board", owner_id=USER, settings={}))
        await db.commit()
    return board_id


async def _seed_legacy_spec(
    db_factory,
    board_id: str,
    *,
    frs: list,
    acs: list,
    business_rules: list | None = None,
    test_scenarios: list | None = None,
) -> str:
    """Persist a spec with legacy string FRs/ACs and arbitrary BRs/scenarios.

    The raw JSON is inserted without going through SpecService so the legacy
    state is preserved exactly (no canonicalization on insert).
    """
    spec_id = _id("impl4-spec")
    async with db_factory() as db:
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Legacy Spec",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=frs,
                acceptance_criteria=acs,
                business_rules=business_rules or [],
                test_scenarios=test_scenarios or [],
                api_contracts=[],
                integration_requirements=[],
                observability_requirements=[],
                decisions=[],
                technical_requirements=[],
            )
        )
        await db.commit()
    return spec_id


# ===========================================================================
# AC5 — FR4 non-regression: index refs resolve identically to fr_id refs
# ===========================================================================


class _FakeSpecForCoverage:
    """Minimal fake spec for spec_coverage_summary (pure-function target)."""

    def __init__(self, *, frs, acs, business_rules=None, test_scenarios=None):
        self.functional_requirements = frs
        self.acceptance_criteria = acs
        self.business_rules = business_rules or []
        self.test_scenarios = test_scenarios or []
        self.api_contracts = []
        self.technical_requirements = []
        self.decisions = []
        self.observability_requirements = []
        self.integration_requirements = []


@pytest.mark.asyncio
async def test_ac5_index_ref_resolves_same_as_fr_id_in_coverage(db_factory):
    """AC5 — spec with linked_requirements in INDEX produces the same
    fr_coverage_pct as a spec whose linked_requirements use the canonical
    fr_id.  Proves the permanent read-resolver (resolve_linked_fr_indices)
    handles both shapes identically.
    """
    # ---- spec A: FRs as legacy strings, BR ref by INDEX ----
    spec_index = _FakeSpecForCoverage(
        frs=["User can register", "User can log in"],
        acs=[],
        business_rules=[
            {
                "id": "br_idx",
                "title": "By index",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0"],  # index ref
            }
        ],
    )

    # ---- spec B: FRs already structured (fr_ids assigned), BR ref by id ----
    spec_frid = _FakeSpecForCoverage(
        frs=[
            {"id": "fr_aabb1122", "text": "User can register", "status": "active"},
            {"id": "fr_ccdd3344", "text": "User can log in", "status": "active"},
        ],
        acs=[],
        business_rules=[
            {
                "id": "br_frid",
                "title": "By fr_id",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["fr_aabb1122"],  # fr_id ref
            }
        ],
    )

    cov_index = spec_coverage_summary(spec_index)
    cov_frid = spec_coverage_summary(spec_frid)

    # Both specs have 1 FR covered out of 2 total → 50 %
    assert cov_index["fr_covered"] == 1, f"index path: {cov_index}"
    assert cov_frid["fr_covered"] == 1, f"fr_id path: {cov_frid}"
    assert cov_index["fr_coverage_pct"] == cov_frid["fr_coverage_pct"], (
        f"index={cov_index['fr_coverage_pct']} != frid={cov_frid['fr_coverage_pct']}"
    )
    assert cov_index["fr_uncovered_indices"] == cov_frid["fr_uncovered_indices"]


@pytest.mark.asyncio
async def test_ac5_index_ref_passes_validate_spec_linked_refs_gate(db_factory):
    """AC5 — updating a field on a legacy spec (with index-based
    linked_requirements in business_rules) passes the referential integrity
    gate without raising ValueError.  The gate must accept index refs just
    as it accepts fr_id refs.
    """
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(
        db_factory,
        board_id,
        frs=["Register endpoint", "Login endpoint"],   # legacy strings
        acs=["Accepts valid token"],
        business_rules=[
            {
                "id": "br_legacy",
                "title": "Index ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0", "1"],  # index refs
            }
        ],
    )

    # Touch the spec via update_spec (only update title — does NOT rewrite FRs).
    # The gate must accept the existing index refs in business_rules.
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id, USER, SpecUpdate(title="Updated Title")
        )
        await db.commit()

    assert updated is not None
    assert updated.title == "Updated Title"
    # business_rules must NOT have been touched (only title changed, no FR migration)
    assert updated.business_rules[0]["linked_requirements"] == ["0", "1"], (
        "Gate must not rewrite refs when FRs were not part of the update"
    )


@pytest.mark.asyncio
async def test_ac5_fr_id_ref_passes_validate_spec_linked_refs_gate(db_factory):
    """AC5 (fr_id parity) — structured spec with fr_id in linked_requirements
    also passes the gate.  Symmetric to the index case.
    """
    board_id = await _seed_board(db_factory)
    # Create spec via SpecService so FRs get canonicalized (fr_ids assigned)
    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            USER,
            __import__(
                "okto_pulse.core.models.schemas",
                fromlist=["SpecCreate"],
            ).SpecCreate(
                title="Structured Spec",
                delivery_context="brownfield",
                functional_requirements=["Register", "Login"],
                business_rules=[
                    {
                        "id": "br_frid",
                        "title": "FR id ref",
                        "rule": "R",
                        "when": "W",
                        "then": "T",
                        "linked_requirements": ["Register"],  # text ref (will validate)
                    }
                ],
            ),
        )
        await db.commit()
        spec_id = spec.id

    # Now update title; gate must accept the fr_id ref already in BRs.
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id, USER, SpecUpdate(title="Updated Title 2")
        )
        await db.commit()

    assert updated is not None
    assert updated.title == "Updated Title 2"


# ===========================================================================
# AC6 — FR5 lazy migration: update_spec materializes index refs to fr_ids
# ===========================================================================


@pytest.mark.asyncio
async def test_ac6_update_spec_with_frs_migrates_linked_requirements_to_fr_ids(db_factory):
    """AC6 — when update_spec is called with functional_requirements (even the
    same list re-passed), legacy index refs in business_rules.linked_requirements
    are rewritten to canonical fr_ids in the persisted row.

    Proof: read the raw DB column before and after update_spec.
    """
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(
        db_factory,
        board_id,
        frs=["FR alpha", "FR beta"],           # legacy strings (no ids)
        acs=["AC one"],
        business_rules=[
            {
                "id": "br_by_index",
                "title": "Index ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0"],   # index ref
            },
            {
                "id": "br_by_text",
                "title": "Text ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["FR beta"],  # text ref
            },
        ],
    )

    # --- Before update_spec: raw DB has index/text refs ---
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
        before_br0_refs = list(before.business_rules[0]["linked_requirements"])
        before_br1_refs = list(before.business_rules[1]["linked_requirements"])

    assert before_br0_refs == ["0"],        "pre-condition: BR0 uses index ref"
    assert before_br1_refs == ["FR beta"],  "pre-condition: BR1 uses text ref"

    # --- Call update_spec with the same FRs (triggers canonicalization + migration) ---
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id,
            USER,
            SpecUpdate(functional_requirements=["FR alpha", "FR beta"]),
        )
        await db.commit()
        spec_id_after = updated.id  # same id

    # --- After update_spec: raw column must have fr_ids ---
    async with db_factory() as db:
        after = await db.get(Spec, spec_id_after)

    # FRs are now structured dicts with fr_ ids
    assert all(isinstance(fr, dict) and fr.get("id", "").startswith("fr_")
               for fr in after.functional_requirements), (
        f"FRs not structured after update_spec: {after.functional_requirements}"
    )

    fr_id_alpha = next(
        fr["id"] for fr in after.functional_requirements if fr["text"] == "FR alpha"
    )
    fr_id_beta = next(
        fr["id"] for fr in after.functional_requirements if fr["text"] == "FR beta"
    )

    after_br0_refs = after.business_rules[0]["linked_requirements"]
    after_br1_refs = after.business_rules[1]["linked_requirements"]

    # Index "0" → fr_id_alpha
    assert after_br0_refs == [fr_id_alpha], (
        f"BR0 linked_requirements not migrated: expected [{fr_id_alpha!r}], got {after_br0_refs}"
    )
    # Text "FR beta" → fr_id_beta
    assert after_br1_refs == [fr_id_beta], (
        f"BR1 linked_requirements not migrated: expected [{fr_id_beta!r}], got {after_br1_refs}"
    )


@pytest.mark.asyncio
async def test_ac6_update_spec_with_acs_migrates_linked_criteria_to_ac_ids(db_factory):
    """AC6 (AC variant) — update_spec with acceptance_criteria rewrites
    index refs in test_scenarios.linked_criteria to canonical ac_ids.
    """
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(
        db_factory,
        board_id,
        frs=[],
        acs=["AC zero", "AC one"],              # legacy strings
        test_scenarios=[
            {
                "id": "ts_by_index",
                "title": "By index",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["0"],        # index ref
            },
            {
                "id": "ts_by_text",
                "title": "By text",
                "scenario_type": "unit",
                "given": "G",
                "when": "W",
                "then": "T",
                "linked_criteria": ["AC one"],   # text ref
            },
        ],
    )

    # Before: raw index/text refs
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
    assert before.test_scenarios[0]["linked_criteria"] == ["0"]
    assert before.test_scenarios[1]["linked_criteria"] == ["AC one"]

    # Touch acceptance_criteria via update_spec
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id,
            USER,
            SpecUpdate(acceptance_criteria=["AC zero", "AC one"]),
        )
        await db.commit()

    async with db_factory() as db:
        after = await db.get(Spec, updated.id)

    ac_id_zero = next(
        ac["id"] for ac in after.acceptance_criteria if ac["text"] == "AC zero"
    )
    ac_id_one = next(
        ac["id"] for ac in after.acceptance_criteria if ac["text"] == "AC one"
    )

    after_ts0_refs = after.test_scenarios[0]["linked_criteria"]
    after_ts1_refs = after.test_scenarios[1]["linked_criteria"]

    assert after_ts0_refs == [ac_id_zero], (
        f"ts[0] linked_criteria not migrated: expected [{ac_id_zero!r}], got {after_ts0_refs}"
    )
    assert after_ts1_refs == [ac_id_one], (
        f"ts[1] linked_criteria not migrated: expected [{ac_id_one!r}], got {after_ts1_refs}"
    )


@pytest.mark.asyncio
async def test_ac6_untouched_spec_retains_index_refs_in_db(db_factory):
    """AC6 (non-touched guard) — a spec that is never passed through
    update_spec with FRs/ACs keeps its raw index refs in the DB column.
    Only touched specs get migrated (lazy on-touch contract).
    """
    board_id = await _seed_board(db_factory)
    spec_id = await _seed_legacy_spec(
        db_factory,
        board_id,
        frs=["FR only"],
        acs=[],
        business_rules=[
            {
                "id": "br_idx",
                "title": "Index ref",
                "rule": "R",
                "when": "W",
                "then": "T",
                "linked_requirements": ["0"],
            }
        ],
    )

    # Touch only the title — does NOT pass functional_requirements, so no migration.
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id, USER, SpecUpdate(title="Title Only Update")
        )
        await db.commit()

    async with db_factory() as db:
        after = await db.get(Spec, updated.id)

    # FRs must remain as legacy strings (not canonicalized)
    assert after.functional_requirements == ["FR only"], (
        f"FRs must not be canonicalized when not in update payload: "
        f"{after.functional_requirements}"
    )
    # linked_requirements must remain as index ref (not migrated)
    assert after.business_rules[0]["linked_requirements"] == ["0"], (
        f"BR refs must not be migrated when FRs were not updated: "
        f"{after.business_rules[0]['linked_requirements']}"
    )
    # But the read-resolver must still work on the raw index refs
    from okto_pulse.core.services.analytics_service import resolve_linked_fr_indices
    resolved = resolve_linked_fr_indices(
        after.business_rules[0]["linked_requirements"],
        after.functional_requirements,
    )
    assert resolved == {0}, f"Read-resolver must still handle index refs: {resolved}"
