"""No operational Sprint authority; faithful policy compatibility until cutover."""
from uuid import uuid4

import pytest

from okto_pulse.core.application import errors
from okto_pulse.core.application.service_catalog import CoreApplicationServiceCatalog
from okto_pulse.core.application.use_cases.allowed_transitions import allowed_transitions_for_status
from okto_pulse.core.application.use_cases.base import CommandValidationError
from okto_pulse.core.domain.sdlc_registry import SDLC_REGISTRY
from okto_pulse.core.domain.task_validation_policy import FIELDS, plan_migrated_validation_policy
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core import models
from okto_pulse.core.models import schemas
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.services import main
from sqlalchemy_test_models import Board, Card, Spec, SpecStatus, Sprint, SprintStatus


def test_service_and_catalog_expose_no_sprint_operations():
    assert not hasattr(main, "SprintService")
    assert not hasattr(main, "SprintQAService")
    assert not hasattr(main, "SprintOperationError")
    assert not hasattr(errors, "SprintOperationError")
    assert not hasattr(CoreApplicationServiceCatalog, "sprints")
    assert not hasattr(ApplicationServiceCatalog, "sprints")
    assert not hasattr(CoreApplicationServiceCatalog, "sprint_qa")
    assert not hasattr(ApplicationServiceCatalog, "sprint_qa")
    for namespace in (models, schemas):
        assert not hasattr(namespace, "SprintQACreate")
        assert not hasattr(namespace, "SprintQAAnswer")
    for kind, definition in SDLC_REGISTRY.items():
        if kind == "sprint":
            continue  # Historical permission fingerprint, no executable service.
        for edges in definition.transitions.values():
            for edge in edges:
                assert all("sprint" not in value for value in (*edge.preconditions, *edge.reason_codes))


@pytest.mark.parametrize("status", [status.value for status in SprintStatus])
def test_retired_sprint_cannot_discover_a_lifecycle(status):
    with pytest.raises(CommandValidationError, match="Invalid entity_type"):
        allowed_transitions_for_status("sprint", status)


@pytest.mark.asyncio
@pytest.mark.parametrize("confidence", [0, 60, 90, 100])
async def test_card_policy_read_preserves_values_before_and_after_migration(confidence):
    suffix = uuid4().hex
    board_id, spec_id, sprint_id, card_id = (f"{kind}-{suffix}" for kind in ("board", "spec", "sprint", "card"))
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="Compatibility", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Shared", status=SpecStatus.IN_PROGRESS, created_by="owner"))
        db.add(Sprint(id=sprint_id, board_id=board_id, spec_id=spec_id, title="Historical policy",
                      status=SprintStatus.CLOSED, validation_min_confidence=confidence, created_by="owner"))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, sprint_id=sprint_id, title="Preserved", created_by="owner"))
        await db.commit()
        service = main.CardService(db)
        card = await service.get_card(card_id)
        spec = await main.SpecService(db).get_spec(spec_id)
        before = await service.validation_config_for_card(card, spec=spec, board_settings={})
        assert before["min_confidence"] == confidence
        assert before["resolved_sources"]["min_confidence"] == "sprint"
        legacy = await db.get(Sprint, sprint_id)
        preserved = plan_migrated_validation_policy(card=card, spec=spec, sprint=legacy,
            board_settings={}, migration_id="test-offline-capture")
        # Fixture simulates the authorized offline writer, not an executor API.
        card.sprint_id = None
        card.migrated_validation_policy = preserved.model_dump(mode="json")
        after = await service.validation_config_for_card(card, spec=spec, board_settings={})
        assert {field: before[field] for field in FIELDS} == {field: after[field] for field in FIELDS}
        assert after["resolved_sources"]["min_confidence"] == "card_compatibility"
        assert legacy.validation_min_confidence == confidence
