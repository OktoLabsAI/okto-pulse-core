"""Decision coverage Analytics payload tests (bug 42e78332).

EntityDetail.tsx reads top-level ``data.decisions`` / ``data.decisions_coverage``
/ ``data.decisions_uncovered_ids`` on the spec entity-detail drilldown (the KPI
+ "Decisions Coverage" panel). These tests prove BOTH spec analytics endpoints
(``board_spec_analytics`` modern + ``_spec_detail`` legacy) surface those fields,
and that ``_coverage_row_for_spec`` carries the decisions parity fields — all
sourced from the SSOT ``spec_coverage_summary`` (no recompute), mirroring the
IR/OR payload pattern (spec 233eaad3).
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from okto_pulse.core.services.analytics_service import _spec_detail
from okto_pulse.core.services.analytics_service import compute_spec_analytics
from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.services.analytics_service import (
    _coverage_row_for_spec,
    spec_coverage_summary,
)

OWNER_ID = "owner-deccov"


async def _seed_spec(db_factory, *, board_id, spec_id, card_id, decisions):
    """Seed a board+spec with the given decisions and one DONE card.

    ``card_id`` is unique per test so the shared db_factory session does not hit
    a Card primary-key collision when tests run together.
    """
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Decision coverage", owner_id=OWNER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec with decisions",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1"],
                functional_requirements=["FR1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
                decisions=decisions,
                integration_requirements=[],
                observability_requirements=[],
                created_by=OWNER_ID,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Active implementation",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=OWNER_ID,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_spec_detail_and_modern_expose_decisions_full_coverage(db_factory):
    """ts_74fd70ff: 2 decisions ativas todas linkadas -> decisions(len 2), 100.0, []."""
    board_id, spec_id, card_id = "deccov-board-100", "deccov-spec-100", "deccov-card-100"
    await _seed_spec(
        db_factory,
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
        decisions=[
            {"id": "dec_a", "status": "active", "linked_task_ids": [card_id]},
            {"id": "dec_b", "status": "active", "linked_task_ids": [card_id]},
        ],
    )
    async with db_factory() as db:
        legacy = await _spec_detail(db, board_id, spec_id)
        modern = await compute_spec_analytics(db, board_id, spec_id)

    for payload in (legacy, modern):
        assert len(payload["decisions"]) == 2
        assert payload["decisions_coverage"] == 100.0
        assert payload["decisions_uncovered_ids"] == []


@pytest.mark.asyncio
async def test_spec_detail_and_modern_list_uncovered_decision(db_factory):
    """ts_628600cc: 1 linkada + 1 nao-linkada -> uncovered=[dec_unlinked], coverage 50.0."""
    board_id, spec_id, card_id = "deccov-board-diff", "deccov-spec-diff", "deccov-card-diff"
    await _seed_spec(
        db_factory,
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
        decisions=[
            {"id": "dec_linked", "status": "active", "linked_task_ids": [card_id]},
            {"id": "dec_unlinked", "status": "active", "linked_task_ids": []},
        ],
    )
    async with db_factory() as db:
        legacy = await _spec_detail(db, board_id, spec_id)
        modern = await compute_spec_analytics(db, board_id, spec_id)

    for payload in (legacy, modern):
        assert len(payload["decisions"]) == 2
        assert payload["decisions_uncovered_ids"] == ["dec_unlinked"]
        assert payload["decisions_coverage"] == 50.0


@pytest.mark.asyncio
async def test_coverage_row_decisions_parity_and_payload_backward_compat(db_factory):
    """ts_0d7d453a: row tem decisions_linked/uncovered_ids/skip == SSOT; payload pre-existente intacto."""
    board_id, spec_id, card_id = "deccov-board-row", "deccov-spec-row", "deccov-card-row"
    await _seed_spec(
        db_factory,
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
        decisions=[
            {"id": "dec_linked", "status": "active", "linked_task_ids": [card_id]},
            {"id": "dec_unlinked", "status": "active", "linked_task_ids": []},
        ],
    )
    async with db_factory() as db:
        spec = (
            await db.execute(select(Spec).where(Spec.id == spec_id))
        ).scalar_one()
        cards = list(
            (await db.execute(select(Card).where(Card.spec_id == spec_id))).scalars().all()
        )
        row = _coverage_row_for_spec(spec, cards=cards)
        summary = spec_coverage_summary(spec, cards=cards)
        legacy = await _spec_detail(db, board_id, spec_id)

    # Parity: the dashboard-row decisions fields mirror the SSOT spec_coverage_summary.
    assert row["decisions_linked"] == summary["decisions_linked"] == 1
    assert (
        row["decisions_uncovered_ids"]
        == summary["decisions_uncovered_ids"]
        == ["dec_unlinked"]
    )
    assert row["skip_decisions_coverage"] == summary["skip_decisions_coverage"]

    # Backward-compat: pre-existing entity-detail payload keys remain present.
    for key in (
        "coverage_summary",
        "integration_requirements",
        "observability_requirements",
        "business_rules",
        "api_contracts",
    ):
        assert key in legacy

    # Consistency: top-level decisions_coverage mirrors the nested SSOT value.
    assert legacy["decisions_coverage"] == legacy["coverage_summary"]["decisions_coverage_pct"]
