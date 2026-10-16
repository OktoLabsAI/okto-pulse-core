"""Core owns the effective policy returned by the existing authorized Card read."""

from uuid import uuid4
from unittest.mock import AsyncMock

import pytest
from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope

from sqlalchemy_test_models import Board, Card, Spec
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.card_crud import GetCardCommand, GetCardUseCase
from okto_pulse.core.infra.database import get_session_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_override", (None, 90, 60))
async def test_card_read_resolves_current_policy_without_committing(monkeypatch, spec_override):
    suffix = uuid4().hex
    board_id, spec_id, card_id = [f"{kind}-{suffix}" for kind in ("board", "spec", "card")]
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="Board", owner_id="owner", settings={"max_drift": 12}))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="Spec", created_by="owner",
            architecture_adoption=ArchitectureAdoptionScope(
                board_id=board_id, spec_id=spec_id, adopted_in_edition=1,
                actor_id="owner", inherited_resource_ids=(),
            ).model_dump(mode="json"),
            validation_min_completeness=92, validation_min_confidence=spec_override,
        ))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Task", created_by="owner"))
        await db.commit()

    factory = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext("owner", "rest", board_id=board_id, permissions=["*"])
    commits = AsyncMock(side_effect=AssertionError("Card read committed"))
    async with factory(actor=actor) as uow:
        monkeypatch.setattr(uow, "commit", commits)
        result = await GetCardUseCase().execute(GetCardCommand(card_id), actor=actor, uow=uow)
        config = result.card.validation_config
        assert config is not None
        assert config.min_confidence == (spec_override if spec_override is not None else 70)
        assert config.min_completeness == 92
        assert config.max_drift == 12
        assert config.required is True
        assert config.resolved_sources.min_confidence == ("spec" if spec_override is not None else "board")
        assert config.resolved_sources.min_completeness == "spec"
        commits.assert_not_awaited()
    denied = ActorContext("other", "rest", board_id=board_id)
    async with factory(actor=denied) as uow:
        with pytest.raises(EntityNotFoundError):
            await GetCardUseCase().execute(GetCardCommand(card_id), actor=denied, uow=uow)
    async with get_session_factory()() as db:
        stored = await db.get(Card, card_id)
        assert stored.title == "Task"
        assert stored.spec_id == spec_id
        assert (await db.get(Spec, spec_id)).validation_min_confidence == spec_override
