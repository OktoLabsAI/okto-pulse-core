"""Native ID links and refusal of incompatible stored requirements."""
from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecUpdate
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
                functional_requirements=[{"id": "fr_register", "text": "Register"}, {"id": "fr_login", "text": "Login"}],
                business_rules=[
                    {
                        "id": "br_frid",
                        "title": "FR id ref",
                        "rule": "R",
                        "when": "W",
                        "then": "T",
                        "linked_requirements": ["fr_register"],  # text ref (will validate)
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
@pytest.mark.parametrize("field,dependent,ref_field", [
    ("functional_requirements", "business_rules", "linked_requirements"),
    ("acceptance_criteria", "test_scenarios", "linked_criteria"),
])
async def test_incompatible_requirement_refusal_preserves_raw_links(db_factory, field, dependent, ref_field):
    """Supersedes lazy conversion: no rewritten requirement or dependent reference."""
    board_id = await _seed_board(db_factory)
    dependents = [{"id": "dependent", ref_field: ["0", "Old text"]}]
    spec_id = await _seed_legacy_spec(db_factory, board_id,
        frs=["Old text"] if field == "functional_requirements" else [],
        acs=["Old text"] if field == "acceptance_criteria" else [],
        **{dependent: dependents})
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        version = spec.version
        with pytest.raises(ValueError, match="incompatible_spec_requirement"):
            await SpecService(db).update_spec(spec_id, USER, SpecUpdate(**{
                field: [{"id": "new-authored-id", "text": "New content"}]
            }))
        assert getattr(spec, field) == ["Old text"]
        assert getattr(spec, dependent) == dependents
        assert spec.version == version
        assert not db.dirty
