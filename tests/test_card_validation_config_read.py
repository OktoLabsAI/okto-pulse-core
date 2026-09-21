"""Core owns the effective policy returned by the existing authorized Card read."""

from uuid import uuid4
from unittest.mock import AsyncMock

import pytest

from sqlalchemy_test_models import Board, Card, Spec, Sprint
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.card_crud import GetCardCommand, GetCardUseCase
from okto_pulse.core.infra.database import get_session_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("inherited", "live", "migrated-90", "migrated-60", "foreign"))
async def test_card_read_resolves_current_policy_without_committing(monkeypatch, mode):
    suffix = uuid4().hex
    board_id, spec_id, card_id, sprint_id, foreign_id = [f"{kind}-{suffix}" for kind in ("board", "spec", "card", "sprint", "foreign")]
    policy = None
    if mode.startswith("migrated"):
        policy = {"contract_version": "card-validation-compatibility/v1",
                  "board_id": board_id, "card_id": card_id, "source_spec_id": spec_id,
                  "source_sprint_id": sprint_id, "migration_id": "offline-migration",
                  "overrides": {"min_confidence": int(mode.split('-')[1]), "max_drift": 0, "required": False}}
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="Board", owner_id="owner", settings={"max_drift": 12}))
        db.add(Board(id=foreign_id, name="Foreign", owner_id="other"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", created_by="owner",
                    validation_min_completeness=92))
        db.add(Sprint(id=sprint_id, board_id=foreign_id if mode == "foreign" else board_id,
                      spec_id=spec_id, title="Private Sprint", created_by="owner", validation_min_confidence=95))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Task", created_by="owner",
                    sprint_id=sprint_id if mode in {"live", "foreign"} else None,
                    migrated_validation_policy=policy))
        await db.commit()

    factory = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext("owner", "rest", board_id=board_id, permissions=["*"])
    commits = AsyncMock(side_effect=AssertionError("Card read committed"))
    async with factory(actor=actor) as uow:
        monkeypatch.setattr(uow, "commit", commits)
        result = await GetCardUseCase().execute(GetCardCommand(card_id), actor=actor, uow=uow)
        config = result.card.validation_config
        if mode in {"live", "foreign"}:
            assert config is None
        else:
            assert config is not None
            assert config.min_confidence == (int(mode.split('-')[1]) if policy else 70)
            assert config.min_completeness == 92
            assert config.max_drift == (0 if policy else 12)
            assert config.required is (False if policy else True)
            assert config.resolved_sources.min_confidence == ("card_compatibility" if policy else "board")
            assert config.resolved_sources.min_completeness == "spec"
        commits.assert_not_awaited()
    denied = ActorContext("other", "rest", board_id=board_id)
    async with factory(actor=denied) as uow:
        with pytest.raises(EntityNotFoundError):
            await GetCardUseCase().execute(GetCardCommand(card_id), actor=denied, uow=uow)
    async with get_session_factory()() as db:
        assert (await db.get(Card, card_id)).migrated_validation_policy == policy
