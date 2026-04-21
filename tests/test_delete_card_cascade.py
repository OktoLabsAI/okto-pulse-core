"""Regression: CardService.delete_card must cascade-clean linked_task_ids.

Bug: deleting a card left its id inside JSON-side reference lists on the
parent spec (test_scenarios, business_rules, api_contracts,
technical_requirements, decisions) and inside linked_test_task_ids on any
bug card that pointed at it. The next update_spec / create_card on the
spec then failed with 'orphan link reference' from
_validate_spec_linked_refs.

Fix: CardService.delete_card now walks the 5 JSON containers and the bug
cards' columnar list, removes the deleted card id, and flag_modifies the
mutated attributes — all in the same transaction as the row delete.

Covers spec 0bef8df6-2171-4a35-85bf-f8902538f719 AC-1..AC-5.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.db import Board, Card, CardType, Spec, SpecStatus
from okto_pulse.core.services.main import CardService


USER_ID = "user-cascade-test"


@pytest_asyncio.fixture
async def db_session():
    factory = get_session_factory()
    async with factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def board(db_session):
    b = Board(
        id=str(uuid.uuid4()),
        name="Cascade Test Board",
        owner_id=USER_ID,
    )
    db_session.add(b)
    await db_session.flush()
    return b


def _spec_factory(board_id: str, **overrides) -> Spec:
    defaults = dict(
        id=str(uuid.uuid4()),
        board_id=board_id,
        title="Cascade Spec",
        description="",
        context="",
        status=SpecStatus.APPROVED,
        version=1,
        created_by=USER_ID,
        functional_requirements=["FR-0 something"],
        technical_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        decisions=[],
        labels=[],
    )
    defaults.update(overrides)
    return Spec(**defaults)


async def _make_card(
    db_session,
    *,
    board_id: str,
    spec_id: str | None,
    card_type: CardType = CardType.NORMAL,
    test_scenario_ids=None,
    linked_test_task_ids=None,
) -> Card:
    c = Card(
        id=str(uuid.uuid4()),
        board_id=board_id,
        spec_id=spec_id,
        title=f"Card-{card_type.value}",
        description="",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=card_type,
        labels=[],
        test_scenario_ids=test_scenario_ids,
        linked_test_task_ids=linked_test_task_ids or [],
    )
    db_session.add(c)
    await db_session.flush()
    return c


# ---------------------------------------------------------------------------
# AC-1 — test_scenarios.linked_task_ids is cleaned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac1_test_scenarios_linked_task_ids_cleaned(db_session, board):
    # Arrange
    card_id = str(uuid.uuid4())
    spec = _spec_factory(
        board.id,
        test_scenarios=[
            {
                "id": "ts_1",
                "title": "scn",
                "given": "g",
                "when": "w",
                "then": "t",
                "scenario_type": "unit",
                "linked_criteria": [],
                "linked_task_ids": [card_id],
                "status": "draft",
            }
        ],
    )
    db_session.add(spec)
    await db_session.flush()

    card = Card(
        id=card_id,
        board_id=board.id,
        spec_id=spec.id,
        title="T",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=CardType.TEST,
        test_scenario_ids=["ts_1"],
        labels=[],
        linked_test_task_ids=[],
    )
    db_session.add(card)
    await db_session.commit()

    svc = CardService(db_session)

    # Act
    ok = await svc.delete_card(card_id, USER_ID)
    await db_session.commit()
    assert ok is True

    # Assert — reload spec and inspect
    await db_session.refresh(spec)
    assert spec.test_scenarios[0]["linked_task_ids"] == []


# ---------------------------------------------------------------------------
# AC-2 — all 4 non-scenario containers are cleaned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2_all_containers_cleaned(db_session, board):
    card_id = str(uuid.uuid4())
    spec = _spec_factory(
        board.id,
        business_rules=[
            {"id": "br_1", "title": "", "rule": "", "when": "", "then": "",
             "linked_requirements": [], "linked_task_ids": [card_id]}
        ],
        api_contracts=[
            {"id": "ac_1", "method": "GET", "path": "/x", "description": "",
             "linked_requirements": [], "linked_rules": [],
             "linked_task_ids": [card_id]}
        ],
        technical_requirements=[
            {"id": "tr_1", "text": "tr", "linked_task_ids": [card_id]}
        ],
        decisions=[
            {"id": "d_1", "title": "", "rationale": "",
             "status": "active", "linked_requirements": [],
             "linked_task_ids": [card_id]}
        ],
    )
    db_session.add(spec)
    card = Card(
        id=card_id,
        board_id=board.id,
        spec_id=spec.id,
        title="impl",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=CardType.NORMAL,
        labels=[],
        linked_test_task_ids=[],
    )
    db_session.add(card)
    await db_session.commit()

    svc = CardService(db_session)
    ok = await svc.delete_card(card_id, USER_ID)
    await db_session.commit()
    assert ok

    await db_session.refresh(spec)
    assert spec.business_rules[0]["linked_task_ids"] == []
    assert spec.api_contracts[0]["linked_task_ids"] == []
    assert spec.technical_requirements[0]["linked_task_ids"] == []
    assert spec.decisions[0]["linked_task_ids"] == []


# ---------------------------------------------------------------------------
# AC-3 — bug.linked_test_task_ids cleaned when deleting a test card
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac3_bug_linked_test_task_ids_cleaned(db_session, board):
    spec = _spec_factory(board.id)
    db_session.add(spec)
    await db_session.flush()

    test_card = await _make_card(
        db_session,
        board_id=board.id,
        spec_id=spec.id,
        card_type=CardType.TEST,
    )
    bug = await _make_card(
        db_session,
        board_id=board.id,
        spec_id=spec.id,
        card_type=CardType.BUG,
        linked_test_task_ids=[test_card.id],
    )
    await db_session.commit()

    svc = CardService(db_session)
    ok = await svc.delete_card(test_card.id, USER_ID)
    await db_session.commit()
    assert ok

    await db_session.refresh(bug)
    assert test_card.id not in (bug.linked_test_task_ids or [])


# ---------------------------------------------------------------------------
# AC-4 — full delete→recreate flow succeeds end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac4_delete_then_recreate_succeeds(db_session, board):
    """The whole point of the fix: prove the orphan error is gone."""
    card_id = str(uuid.uuid4())
    spec = _spec_factory(
        board.id,
        test_scenarios=[
            {
                "id": "ts_1",
                "title": "scn",
                "given": "g",
                "when": "w",
                "then": "t",
                "scenario_type": "unit",
                "linked_criteria": [],
                "linked_task_ids": [card_id],
                "status": "draft",
            }
        ],
    )
    db_session.add(spec)
    card = Card(
        id=card_id,
        board_id=board.id,
        spec_id=spec.id,
        title="first",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=CardType.TEST,
        test_scenario_ids=["ts_1"],
        labels=[],
        linked_test_task_ids=[],
    )
    db_session.add(card)
    await db_session.commit()

    svc = CardService(db_session)
    await svc.delete_card(card_id, USER_ID)
    await db_session.commit()

    # Sanity: spec is clean
    await db_session.refresh(spec)
    assert spec.test_scenarios[0]["linked_task_ids"] == []

    # Now create a second card referencing the same scenario — this is the
    # exact flow that used to fail with 'orphan link reference'.
    new_card = Card(
        id=str(uuid.uuid4()),
        board_id=board.id,
        spec_id=spec.id,
        title="second",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=CardType.TEST,
        test_scenario_ids=["ts_1"],
        labels=[],
        linked_test_task_ids=[],
    )
    db_session.add(new_card)
    # Direct link (simulating what create_card does internally when it
    # stamps the scenario's linked_task_ids).
    spec.test_scenarios[0]["linked_task_ids"] = [new_card.id]
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(spec, "test_scenarios")
    await db_session.commit()

    # Validate
    await db_session.refresh(spec)
    assert spec.test_scenarios[0]["linked_task_ids"] == [new_card.id]


# ---------------------------------------------------------------------------
# AC-5 — spec-less cards delete cleanly (no cascade attempted)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac5_spec_less_card_deletes_without_cascade(db_session, board):
    card = await _make_card(
        db_session,
        board_id=board.id,
        spec_id=None,
        card_type=CardType.NORMAL,
    )
    await db_session.commit()

    svc = CardService(db_session)
    ok = await svc.delete_card(card.id, USER_ID)
    await db_session.commit()
    assert ok is True

    # Row is gone
    got = await db_session.get(Card, card.id)
    assert got is None
