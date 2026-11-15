"""Task validation must report and replay the committed policy subject version."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from test_e2e043_source_context import native_source_factory as _native_source_factory
from okto_pulse.community.adapters.sqlalchemy_models import Board, Card, Spec
from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import (
    bind_semantic_subject_actor,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.services.main import CardService

native_source_factory = _native_source_factory


def _bind(db):
    bind_semantic_subject_actor(
        db, ActorContext("reviewer", "test", actor_kind="user", board_id="board")
    )


async def _seed(factory):
    from okto_pulse.community.adapters.sqlalchemy_critical_context import (
        CommunitySqlAlchemyCriticalContextReader,
    )
    from okto_pulse.core.ports.critical_context import (
        register_critical_context_read_port,
    )

    register_critical_context_read_port(CommunitySqlAlchemyCriticalContextReader())
    async with factory() as db:
        _bind(db)
        db.add(
            Board(
                id="board",
                realm_id="local",
                name="Task version",
                owner_id="reviewer",
                settings={},
            )
        )
        db.add(
            Spec(
                id="spec",
                board_id="board",
                title="Scope",
                created_by="executor",
                architecture_adoption={},
            )
        )
        db.add(
            Card(
                id="card",
                board_id="board",
                spec_id="spec",
                title="Delivered work",
                created_by="executor",
                status=CardStatus.VALIDATION,
            )
        )
        await db.commit()


def _request(version, recommendation):
    return dict(
        expected_subject_version=version,
        idempotency_key="native-version-decision",
        confidence=95,
        confidence_justification="Reviewed the evidence",
        estimated_completeness=100,
        completeness_justification="Scope checked",
        estimated_drift=0,
        drift_justification="No deviation",
        general_justification="Reviewed the implementation",
        recommendation=recommendation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("recommendation", ["approve", "reject"])
async def test_returned_version_matches_persisted_subject_and_exact_replay(
    native_source_factory, monkeypatch, recommendation
):
    # This test isolates SQL/version/replay semantics. Completion gate behavior
    # is covered by the native gate suites, not replaced in production.
    monkeypatch.setattr(
        CardService, "_task_completion_gate_failures", AsyncMock(return_value=())
    )
    await _seed(native_source_factory)
    async with native_source_factory() as db:
        _bind(db)
        service = CardService(db)
        before = await service.get_card("card")
        version = service._card_subject_version(before)
        request = _request(version, recommendation)
        result = await service.submit_task_validation(
            "card", "reviewer", "Reviewer", request
        )
        await db.commit()
    async with native_source_factory() as db:
        _bind(db)
        service = CardService(db)
        current = await service.get_card("card")
        assert (
            result["subject_version"]
            == service._card_subject_version(current)
            == version
        )
        assert result["card_status"] == current.status.value
        assert current.status.value == (
            "done" if recommendation == "approve" else "rejected"
        )
        ledger = deepcopy(current.validations)
        replay = await service.submit_task_validation(
            "card", "reviewer", "Reviewer", request
        )
        assert replay == {**result, "replayed": True}
        assert (await service.get_card("card")).validations == ledger


@pytest.mark.asyncio
async def test_failure_after_validation_staging_rolls_back_decision_and_version(
    native_source_factory, monkeypatch
):
    await _seed(native_source_factory)
    monkeypatch.setattr(
        CardService,
        "_log_activity",
        AsyncMock(side_effect=RuntimeError("injected late failure")),
    )
    async with native_source_factory() as db:
        _bind(db)
        service = CardService(db)
        version = service._card_subject_version(await service.get_card("card"))
        with pytest.raises(RuntimeError, match="injected late failure"):
            await service.submit_task_validation(
                "card", "reviewer", "Reviewer", _request(version, "reject")
            )
        await db.rollback()
    async with native_source_factory() as db:
        current = await CardService(db).get_card("card")
        assert current.status == CardStatus.VALIDATION
        assert not current.validations
        assert not current.rejection_records
        assert CardService._card_subject_version(current) == version
