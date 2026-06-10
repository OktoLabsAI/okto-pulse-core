"""Service integration tests for the shared cognitive closeout gate."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.kg.rebuild_audit import CognitivePendingMarker
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardMove, SpecMove
from okto_pulse.core.services.main import CardService, SpecService
from okto_pulse.core.services.resource_gate import ResourceGateService


USER_ID = "ccg-service-wiring-agent"


def _id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def isolated_closeout_kg_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "ccg-service-wiring"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


def _seed_pending_item(base_dir: Path, board_id: str, source_ref: str) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=board_id,
        kg_generation_id=generate_kg_generation_id(),
        source_set=[
            {
                "artifact_type": source_ref.split(":", 1)[0],
                "id": source_ref.rsplit(":", 1)[-1],
                "source_ref": source_ref,
            }
        ],
        event_ref="evt_ccg_service",
    )


async def _seed_spec(
    *,
    board_settings: dict | None = None,
    decisions: list[dict] | None = None,
) -> tuple[str, str]:
    board_id = _id()
    spec_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="CCG service wiring board",
                owner_id=USER_ID,
                settings=board_settings or {},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="CCG service wiring spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                decisions=decisions or [],
                acceptance_criteria=[],
                test_scenarios=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _spec_status(spec_id: str) -> str:
    db_factory = get_session_factory()
    async with db_factory() as db:
        spec = (await db.execute(select(Spec).where(Spec.id == spec_id))).scalar_one()
        return spec.status.value


@pytest.mark.asyncio
async def test_spec_done_blocks_on_direct_spec_cognitive_item(
    isolated_closeout_kg_dir: Path,
) -> None:
    board_id, spec_id = await _seed_spec()
    _seed_pending_item(isolated_closeout_kg_dir, board_id, f"spec:{spec_id}")

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_spec(
                spec_id=spec_id,
                user_id=USER_ID,
                data=SpecMove(status=SpecStatus.DONE),
                actor_name=USER_ID,
            )
        await db.rollback()

    assert await _spec_status(spec_id) == SpecStatus.IN_PROGRESS.value


@pytest.mark.asyncio
async def test_spec_done_blocks_on_active_decision_child_cognitive_item(
    isolated_closeout_kg_dir: Path,
) -> None:
    decision_id = "dec_keep_kg_safe"
    board_id, spec_id = await _seed_spec(
        decisions=[
            {
                "id": decision_id,
                "title": "Do not close before cognitive consolidation",
                "status": "active",
            }
        ]
    )
    _seed_pending_item(
        isolated_closeout_kg_dir,
        board_id,
        f"decision:{spec_id}:{decision_id}",
    )

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_spec(
                spec_id=spec_id,
                user_id=USER_ID,
                data=SpecMove(status=SpecStatus.DONE),
                actor_name=USER_ID,
            )
        await db.rollback()

    assert await _spec_status(spec_id) == SpecStatus.IN_PROGRESS.value


class _BlockingGate:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def evaluate(self, **kwargs):
        self.calls.append(dict(kwargs))
        return SimpleNamespace(
            allowed=False,
            reason="cognitive_consolidation_pending",
            blocking_count=1,
            blocking_items=(object(),),
        )


async def _seed_card(
    card_type: CardType,
    status: CardStatus,
    *,
    board_settings: dict | None = None,
) -> tuple[str, str, str]:
    board_id = _id()
    spec_id = _id()
    card_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="CCG card board",
                owner_id=USER_ID,
                settings=board_settings or {},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="CCG card spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                acceptance_criteria=[],
                test_scenarios=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="CCG card",
                status=status,
                card_type=card_type,
                position=0,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


async def _card_row(card_id: str) -> Card:
    db_factory = get_session_factory()
    async with db_factory() as db:
        return (await db.execute(select(Card).where(Card.id == card_id))).scalar_one()


async def _mark_card_resources_na(db, board_id: str, card_id: str) -> None:
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            board_id,
            "card",
            card_id,
            resource_type,
            USER_ID,
            justification=f"{resource_type} is intentionally not applicable in this closeout service test.",
            source_channel="ui",
        )


@pytest.mark.asyncio
async def test_submit_task_validation_blocks_before_automatic_done() -> None:
    _, _, card_id = await _seed_card(CardType.NORMAL, CardStatus.VALIDATION)
    gate = _BlockingGate()
    validation_data = {
        "confidence": 99,
        "confidence_justification": "strong",
        "estimated_completeness": 100,
        "completeness_justification": "complete",
        "estimated_drift": 0,
        "drift_justification": "none",
        "recommendation": "approve",
        "general_justification": "ready",
    }

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: gate
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.submit_task_validation(
                card_id=card_id,
                reviewer_id=USER_ID,
                reviewer_name=USER_ID,
                data=validation_data,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.validations in (None, [])
    assert gate.calls[0]["entity_type"] == "task"
    assert gate.calls[0]["target_status"] == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("card_type", "expected_entity_type"),
    [
        (CardType.NORMAL, "task"),
        (CardType.TEST, "test"),
        (CardType.BUG, "bug"),
    ],
)
async def test_move_card_done_blocks_task_test_and_bug_before_status_mutation(
    card_type: CardType,
    expected_entity_type: str,
) -> None:
    _, _, card_id = await _seed_card(card_type, CardStatus.VALIDATION)
    gate = _BlockingGate()

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: gate
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_card(
                card_id=card_id,
                user_id=USER_ID,
                data=CardMove(
                    status=CardStatus.DONE,
                    conclusion="implemented and validated",
                    completeness=100,
                    completeness_justification="complete",
                    drift=0,
                    drift_justification="none",
                ),
                actor_name=USER_ID,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.conclusions in (None, [])
    assert gate.calls[0]["entity_type"] == expected_entity_type
    assert gate.calls[0]["target_status"] == "done"


@pytest.mark.asyncio
async def test_board_skip_allows_done_but_keeps_pending_item_visible_and_status_done(
    isolated_closeout_kg_dir: Path,
) -> None:
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL,
        CardStatus.VALIDATION,
        board_settings={"skip_cognitive_consolidation": True},
    )
    _seed_pending_item(isolated_closeout_kg_dir, board_id, f"task:{card_id}")

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        await service.move_card(
            card_id=card_id,
            user_id=USER_ID,
            data=CardMove(
                status=CardStatus.DONE,
                conclusion="validated with board cognitive skip enabled",
                completeness=100,
                completeness_justification="pending item remains visible; only blocking is bypassed",
                drift=0,
                drift_justification="board skip semantics are unchanged",
            ),
            actor_name=USER_ID,
        )
        await db.commit()

    card = await _card_row(card_id)
    assert card.status == CardStatus.DONE

    store = CognitiveConsolidationItemStore(base_dir=isolated_closeout_kg_dir)
    latest = store.latest_generation(board_id)
    assert latest is not None
    items = store.list_items(board_id, latest)
    assert [(item.source_ref, item.status) for item in items] == [
        (f"task:{card_id}", "pending")
    ]
