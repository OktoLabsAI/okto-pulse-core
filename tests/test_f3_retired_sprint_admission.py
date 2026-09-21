"""Historical Sprint rows must not participate in Card/Spec execution admission."""
from uuid import uuid4

import pytest

from okto_pulse.core.application.use_cases.allowed_transitions import (
    ListAllowedTransitionsCommand, ListAllowedTransitionsUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.code_traceability import DeliveryContext, DirectSpecDeliveryContextProvenance
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import CardMove, SpecMove
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services.main import CardService, SpecService
from okto_pulse.core.services.main import _direct_spec_source_context_manifest
from sqlalchemy_test_models import Board, Card, CardStatus, Spec, SpecStatus, Sprint, SprintStatus


def _source_context(spec_id):
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD, source_spec_id=spec_id, source_spec_version=1,
    )
    manifest, fingerprint = _direct_spec_source_context_manifest(
        spec_id=spec_id, delivery_context=DeliveryContext.BROWNFIELD,
        provenance=provenance, subject_version=1,
    )
    return dict(delivery_context="brownfield", delivery_context_provenance={
        "value": "brownfield", "source_spec_id": spec_id, "source_spec_version": 1,
    }, source_context_manifest=manifest, source_context_sha256=fingerprint)


async def _preview(db, board_id, entity_type, entity_id, target):
    result = await ListAllowedTransitionsUseCase().execute(
        ListAllowedTransitionsCommand(board_id, entity_type, entity_id=entity_id),
        actor=ActorContext("owner", "test", board_id=board_id),
        uow=resolve_unit_of_work_factory().wrap(db),
    )
    return next(edge for edge in result.read_model.allowed_transitions if edge.to_status == target)


@pytest.mark.asyncio
@pytest.mark.parametrize("sprint_status", [None, *SprintStatus])
@pytest.mark.parametrize("assigned", [False, True])
async def test_normal_start_preview_and_mutation_ignore_retired_sprint_state(sprint_status, assigned):
    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id, sprint_id = (f"{kind}-{suffix}" for kind in ("board", "spec", "card", "sprint"))
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Executing", status=SpecStatus.IN_PROGRESS,
                    created_by="owner", functional_requirements=[{"id": "FR-1", "title": "Work", "linked_task_ids": [card_id]}]))
        if sprint_status is not None:
            db.add(Sprint(id=sprint_id, board_id=board_id, spec_id=spec_id, title="Historical",
                          status=sprint_status, created_by="owner"))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id,
                    sprint_id=sprint_id if assigned and sprint_status else None,
                    title="Normal work", status=CardStatus.NOT_STARTED, created_by="owner"))
        await db.commit()
        edge = await _preview(db, board_id, "card", card_id, CardStatus.STARTED.value)
        assert edge.blocked_reason is None
        await CardService(db).move_card(card_id, "owner", CardMove(status=CardStatus.STARTED))
        edge = await _preview(db, board_id, "card", card_id, CardStatus.IN_PROGRESS.value)
        assert edge.blocked_reason is None
        moved = await CardService(db).move_card(card_id, "owner", CardMove(status=CardStatus.IN_PROGRESS))
        assert moved.status is CardStatus.IN_PROGRESS
        if sprint_status:
            assert (await db.get(Sprint, sprint_id)).status is sprint_status


@pytest.mark.asyncio
@pytest.mark.parametrize("sprint_status", [None, *SprintStatus])
async def test_spec_completion_keeps_pending_task_gate_independent_of_retired_sprints(sprint_status):
    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id = (f"{kind}-{suffix}" for kind in ("board", "spec", "card"))
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Unfinished", status=SpecStatus.IN_PROGRESS,
                    created_by="owner", **_source_context(spec_id)))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Pending", status=CardStatus.IN_PROGRESS, created_by="owner"))
        if sprint_status:
            db.add(Sprint(id=f"sprint-{suffix}", board_id=board_id, spec_id=spec_id, title="Historical",
                          status=sprint_status, created_by="owner"))
        await db.commit()
        edge = await _preview(db, board_id, "spec", spec_id, "done")
        assert "cards_incomplete" in edge.blocked_reason
        assert "sprint" not in edge.blocked_reason.lower()
        with pytest.raises(ValueError, match=r"linked task\(s\)"):
            await SpecService(db).move_spec(spec_id, "owner", SpecMove(status=SpecStatus.DONE))
        assert (await db.get(Spec, spec_id)).status is SpecStatus.IN_PROGRESS
