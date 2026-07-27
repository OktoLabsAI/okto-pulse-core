"""ITEM 17 — cancellation justification policy.

Covers the shared helper (``okto_pulse.core.services.cancellation``) and one
service-level integration flow per surface:

Helper (pure unit tests over SimpleNamespace entities):
  - cancel without / with blank reason → structured
    ``cancellation_reason_required`` error, no mutation
  - cancel with reason → persists trimmed reason + tz-aware cancelled_at +
    cancelled_by (actor id)
  - a new cancellation REPLACES the previous record
  - reopen (cancelled → other) CLEARS the three fields
  - non-cancellation transitions never touch the fields

Integration (service move flows over the test DB):
  - CardService.move_card, SpecService.move_spec, IdeationService.move_ideation,
    RefinementService.move_refinement, SprintService.move_sprint
  - reopen clears (spec/ideation/refinement cancelled → draft;
    card cancelled → not_started)
  - MCP full entity reads expose the cancellation audit record
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from mcp_runtime_testing import register_mcp_test_runtime

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.cancellation_projection import project_cancellation
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)
from okto_pulse.core.models.schemas import (
    CardMove,
    IdeationMove,
    RefinementMove,
    SpecMove,
    SprintMove,
)
from okto_pulse.core.services.cancellation import (
    CancellationReasonRequiredError,
    apply_cancellation_policy,
)

BOARD_ID = "cancellation-reason-board-001"
USER_ID = "cancellation-reason-user-001"


def _entity(**kwargs):
    base = {
        "cancellation_reason": None,
        "cancelled_at": None,
        "cancelled_by": None,
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


# ============================================================================
# 1. Helper unit tests
# ============================================================================


class TestApplyCancellationPolicy:
    def test_full_read_projection_exposes_cancellation_audit_fields(self):
        cancelled_at = datetime.now(timezone.utc)
        assert project_cancellation(
            _entity(
                cancellation_reason="Obsolete",
                cancelled_at=cancelled_at,
                cancelled_by=USER_ID,
            )
        ) == {
            "cancellation_reason": "Obsolete",
            "cancelled_at": cancelled_at.isoformat(),
            "cancelled_by": USER_ID,
        }

    def test_cancel_without_reason_raises_structured_error(self):
        entity = _entity()
        with pytest.raises(CancellationReasonRequiredError) as exc:
            apply_cancellation_policy(
                entity,
                entity_type="card",
                from_status="in_progress",
                to_status="cancelled",
                reason=None,
                actor_id=USER_ID,
            )
        assert exc.value.code == "cancellation_reason_required"
        payload = exc.value.to_dict()
        assert payload["code"] == "cancellation_reason_required"
        assert payload["entity_type"] == "card"
        # No mutation on failure
        assert entity.cancellation_reason is None
        assert entity.cancelled_at is None
        assert entity.cancelled_by is None

    def test_cancel_with_blank_reason_raises(self):
        entity = _entity()
        with pytest.raises(CancellationReasonRequiredError):
            apply_cancellation_policy(
                entity,
                entity_type="spec",
                from_status="draft",
                to_status="cancelled",
                reason="   \n\t ",
                actor_id=USER_ID,
            )

    def test_cancel_records_reason_actor_and_tz_aware_timestamp(self):
        entity = _entity()
        apply_cancellation_policy(
            entity,
            entity_type="ideation",
            from_status="draft",
            to_status="cancelled",
            reason="  Superseded by ideation #42  ",
            actor_id=USER_ID,
        )
        assert entity.cancellation_reason == "Superseded by ideation #42"
        assert entity.cancelled_by == USER_ID
        assert isinstance(entity.cancelled_at, datetime)
        assert entity.cancelled_at.tzinfo is not None

    def test_new_cancellation_replaces_previous_record(self):
        old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        entity = _entity(
            cancellation_reason="old reason",
            cancelled_at=old_ts,
            cancelled_by="someone-else",
        )
        apply_cancellation_policy(
            entity,
            entity_type="sprint",
            from_status="draft",
            to_status="cancelled",
            reason="new reason",
            actor_id=USER_ID,
        )
        assert entity.cancellation_reason == "new reason"
        assert entity.cancelled_by == USER_ID
        assert entity.cancelled_at > old_ts

    def test_reopen_clears_all_three_fields(self):
        entity = _entity(
            cancellation_reason="why",
            cancelled_at=datetime.now(timezone.utc),
            cancelled_by=USER_ID,
        )
        apply_cancellation_policy(
            entity,
            entity_type="spec",
            from_status="cancelled",
            to_status="draft",
            reason=None,
            actor_id=USER_ID,
        )
        assert entity.cancellation_reason is None
        assert entity.cancelled_at is None
        assert entity.cancelled_by is None

    def test_non_cancellation_transition_is_untouched(self):
        entity = _entity()
        apply_cancellation_policy(
            entity,
            entity_type="card",
            from_status="not_started",
            to_status="in_progress",
            reason="irrelevant",
            actor_id=USER_ID,
        )
        assert entity.cancellation_reason is None
        assert entity.cancelled_at is None
        assert entity.cancelled_by is None

    def test_enum_statuses_are_normalized(self):
        entity = _entity()
        apply_cancellation_policy(
            entity,
            entity_type="card",
            from_status=CardStatus.IN_PROGRESS,
            to_status=CardStatus.CANCELLED,
            reason="enum-driven cancel",
            actor_id=USER_ID,
        )
        assert entity.cancellation_reason == "enum-driven cancel"


# ============================================================================
# 2. Integration — one flow per surface (service move methods)
# ============================================================================


async def _seed_board(db_factory) -> None:
    async with db_factory() as db:
        if await db.get(Board, BOARD_ID) is not None:
            return
        db.add(Board(id=BOARD_ID, name="Cancellation Board", owner_id=USER_ID))
        await db.commit()


async def _seed_ideation(db_factory, status=IdeationStatus.DRAFT) -> str:
    await _seed_board(db_factory)
    ideation_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Ideation(
            id=ideation_id, board_id=BOARD_ID, title="Cancellable Ideation",
            status=status, created_by=USER_ID,
        ))
        await db.commit()
    return ideation_id


async def _seed_refinement(db_factory) -> str:
    ideation_id = await _seed_ideation(db_factory)
    refinement_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Refinement(
            id=refinement_id, ideation_id=ideation_id, board_id=BOARD_ID,
            title="Cancellable Refinement", status=RefinementStatus.DRAFT,
            created_by=USER_ID,
        ))
        await db.commit()
    return refinement_id


async def _seed_spec(db_factory) -> str:
    await _seed_board(db_factory)
    spec_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=BOARD_ID, title="Cancellable Spec",
            status=SpecStatus.DRAFT, created_by=USER_ID,
        ))
        await db.commit()
    return spec_id


async def _seed_sprint(db_factory) -> str:
    spec_id = await _seed_spec(db_factory)
    sprint_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Sprint(
            id=sprint_id, spec_id=spec_id, board_id=BOARD_ID,
            title="Cancellable Sprint", status=SprintStatus.DRAFT,
            created_by=USER_ID,
        ))
        await db.commit()
    return sprint_id


async def _seed_card(db_factory, status=CardStatus.NOT_STARTED) -> str:
    await _seed_board(db_factory)
    card_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Card(
            id=card_id, board_id=BOARD_ID, title="Cancellable Card",
            status=status, card_type=CardType.NORMAL, position=0,
            created_by=USER_ID,
        ))
        await db.commit()
    return card_id


@pytest.mark.asyncio
async def test_mcp_full_entity_reads_expose_cancellation_audit_fields(db_factory):
    ideation_id = await _seed_ideation(db_factory)
    refinement_id = await _seed_refinement(db_factory)
    spec_id = await _seed_spec(db_factory)
    card_id = await _seed_card(db_factory)
    cancelled_at = datetime.now(timezone.utc)

    async with db_factory() as db:
        entities = (
            (await db.get(Ideation, ideation_id), IdeationStatus.CANCELLED),
            (await db.get(Refinement, refinement_id), RefinementStatus.CANCELLED),
            (await db.get(Spec, spec_id), SpecStatus.CANCELLED),
            (await db.get(Card, card_id), CardStatus.CANCELLED),
        )
        for entity, status in entities:
            entity.status = status
            entity.cancellation_reason = "Regression projection"
            entity.cancelled_at = cancelled_at
            entity.cancelled_by = USER_ID
        await db.commit()

    register_mcp_test_runtime(db_factory)
    actor = SimpleNamespace(
        agent_id=USER_ID,
        agent_name=USER_ID,
        board_id=BOARD_ID,
        permissions=["board:read"],
    )

    async def call(name, **kwargs):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=actor),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        payloads = (
            await call(
                "okto_pulse_get_ideation",
                board_id=BOARD_ID,
                ideation_id=ideation_id,
            ),
            await call(
                "okto_pulse_get_refinement",
                board_id=BOARD_ID,
                refinement_id=refinement_id,
            ),
            await call(
                "okto_pulse_get_spec",
                board_id=BOARD_ID,
                spec_id=spec_id,
            ),
            await call(
                "okto_pulse_get_card",
                board_id=BOARD_ID,
                card_id=card_id,
            ),
        )

    for payload in payloads:
        assert payload["cancellation_reason"] == "Regression projection"
        assert payload["cancelled_by"] == USER_ID
        projected_at = datetime.fromisoformat(payload["cancelled_at"])
        if projected_at.tzinfo is None:
            projected_at = projected_at.replace(tzinfo=timezone.utc)
        assert projected_at == cancelled_at


@pytest.mark.asyncio
class TestMoveCardCancellation:
    async def test_cancel_requires_reason(self, db_factory):
        from okto_pulse.core.services.main import CardService
        card_id = await _seed_card(db_factory)
        async with db_factory() as db:
            with pytest.raises(CancellationReasonRequiredError):
                await CardService(db).move_card(
                    card_id, USER_ID, CardMove(status=CardStatus.CANCELLED)
                )
        async with db_factory() as db:
            card = await db.get(Card, card_id)
            assert card.status == CardStatus.NOT_STARTED
            assert card.cancellation_reason is None

    async def test_cancel_persists_and_reopen_clears(self, db_factory):
        from okto_pulse.core.services.main import CardService
        card_id = await _seed_card(db_factory)
        async with db_factory() as db:
            moved = await CardService(db).move_card(
                card_id, USER_ID,
                CardMove(status=CardStatus.CANCELLED, cancellation_reason="Out of scope"),
            )
            assert moved.cancellation_reason == "Out of scope"
            assert moved.cancelled_by == USER_ID
            assert moved.cancelled_at is not None
            await db.commit()
        # Reopen clears the record
        async with db_factory() as db:
            reopened = await CardService(db).move_card(
                card_id, USER_ID, CardMove(status=CardStatus.NOT_STARTED)
            )
            assert reopened.status == CardStatus.NOT_STARTED
            assert reopened.cancellation_reason is None
            assert reopened.cancelled_at is None
            assert reopened.cancelled_by is None


@pytest.mark.asyncio
class TestMoveSpecCancellation:
    async def test_cancel_requires_reason_then_persists_then_reopen_clears(self, db_factory):
        from okto_pulse.core.services.main import SpecService
        spec_id = await _seed_spec(db_factory)
        async with db_factory() as db:
            with pytest.raises(CancellationReasonRequiredError):
                await SpecService(db).move_spec(
                    spec_id, USER_ID, SpecMove(status=SpecStatus.CANCELLED)
                )
        async with db_factory() as db:
            moved = await SpecService(db).move_spec(
                spec_id, USER_ID,
                SpecMove(status=SpecStatus.CANCELLED, cancellation_reason="Duplicated effort"),
            )
            assert moved.cancellation_reason == "Duplicated effort"
            assert moved.cancelled_by == USER_ID
            assert moved.cancelled_at is not None
            await db.commit()
        async with db_factory() as db:
            reopened = await SpecService(db).move_spec(
                spec_id, USER_ID, SpecMove(status=SpecStatus.DRAFT)
            )
            assert reopened.status == SpecStatus.DRAFT
            assert reopened.cancellation_reason is None
            assert reopened.cancelled_at is None
            assert reopened.cancelled_by is None


@pytest.mark.asyncio
class TestMoveIdeationCancellation:
    async def test_cancel_requires_reason_then_persists_then_reopen_clears(
        self, db_factory
    ):
        from okto_pulse.core.services.main import IdeationService
        ideation_id = await _seed_ideation(db_factory)
        async with db_factory() as db:
            with pytest.raises(CancellationReasonRequiredError):
                await IdeationService(db).move_ideation(
                    ideation_id, USER_ID, IdeationMove(status=IdeationStatus.CANCELLED)
                )
        async with db_factory() as db:
            moved = await IdeationService(db).move_ideation(
                ideation_id, USER_ID,
                IdeationMove(
                    status=IdeationStatus.CANCELLED,
                    cancellation_reason="Idea rejected after triage",
                ),
            )
            assert moved.cancellation_reason == "Idea rejected after triage"
            assert moved.cancelled_by == USER_ID
            assert moved.cancelled_at is not None
            await db.commit()
        async with db_factory() as db:
            reopened = await IdeationService(db).move_ideation(
                ideation_id,
                USER_ID,
                IdeationMove(status=IdeationStatus.DRAFT),
            )
            assert reopened.status == IdeationStatus.DRAFT
            assert reopened.version == 2
            assert reopened.cancellation_reason is None
            assert reopened.cancelled_at is None
            assert reopened.cancelled_by is None


@pytest.mark.asyncio
class TestMoveRefinementCancellation:
    async def test_cancel_requires_reason_then_persists_then_reopen_clears(
        self, db_factory
    ):
        from okto_pulse.core.services.main import RefinementService
        refinement_id = await _seed_refinement(db_factory)
        async with db_factory() as db:
            with pytest.raises(CancellationReasonRequiredError):
                await RefinementService(db).move_refinement(
                    refinement_id, USER_ID,
                    RefinementMove(status=RefinementStatus.CANCELLED),
                )
        async with db_factory() as db:
            moved = await RefinementService(db).move_refinement(
                refinement_id, USER_ID,
                RefinementMove(
                    status=RefinementStatus.CANCELLED,
                    cancellation_reason="Refinement no longer needed",
                ),
            )
            assert moved.cancellation_reason == "Refinement no longer needed"
            assert moved.cancelled_by == USER_ID
            assert moved.cancelled_at is not None
            await db.commit()
        async with db_factory() as db:
            reopened = await RefinementService(db).move_refinement(
                refinement_id,
                USER_ID,
                RefinementMove(status=RefinementStatus.DRAFT),
            )
            assert reopened.status == RefinementStatus.DRAFT
            assert reopened.version == 2
            assert reopened.cancellation_reason is None
            assert reopened.cancelled_at is None
            assert reopened.cancelled_by is None


@pytest.mark.asyncio
class TestMoveSprintCancellation:
    async def test_cancel_requires_reason_then_persists_then_reopen_clears(self, db_factory):
        from okto_pulse.core.services.main import SprintService
        sprint_id = await _seed_sprint(db_factory)
        async with db_factory() as db:
            with pytest.raises(CancellationReasonRequiredError):
                await SprintService(db).move_sprint(
                    sprint_id, USER_ID, SprintMove(status=SprintStatus.CANCELLED)
                )
        async with db_factory() as db:
            moved = await SprintService(db).move_sprint(
                sprint_id, USER_ID,
                SprintMove(
                    status=SprintStatus.CANCELLED,
                    cancellation_reason="Sprint plan superseded",
                ),
            )
            assert moved.cancellation_reason == "Sprint plan superseded"
            assert moved.cancelled_by == USER_ID
            assert moved.cancelled_at is not None
        async with db_factory() as db:
            reopened = await SprintService(db).move_sprint(
                sprint_id, USER_ID, SprintMove(status=SprintStatus.DRAFT)
            )
            assert reopened.status == SprintStatus.DRAFT
            assert reopened.cancellation_reason is None
            assert reopened.cancelled_at is None
            assert reopened.cancelled_by is None
