"""SK-M regressions for direct card creation at the execution-start edge."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select, update

from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    CardType,
    DomainEventRow,
    Spec,
    SpecStatus,
)
from okto_pulse.community.adapters.sqlalchemy_models import SpecDependency
from okto_pulse.core.domain.spec_dependency import SpecDependencyOperationError
from okto_pulse.core.models.schemas import CardCreate, CardMove
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import CardService


USER_ID = "skm-create-started-owner"


def _ids(case: str) -> tuple[str, str, str]:
    return (
        f"skm-cs-{case}-board",
        f"skm-cs-{case}-source",
        f"skm-cs-{case}-target",
    )


async def _seed_specs(
    db_factory,  # noqa: ANN001
    *,
    case: str,
    blocked: bool,
) -> tuple[str, str, str]:
    board_id, source_id, target_id = _ids(case)
    async with db_factory() as db:
        db.add(Board(id=board_id, name="SK-M create STARTED", owner_id=USER_ID))
        db.add_all(
            (
                Spec(
                    id=source_id,
                    board_id=board_id,
                    title="Dependent Spec",
                    status=SpecStatus.APPROVED,
                    edition=3,
                    last_started_edition=None,
                    version=1,
                    created_by=USER_ID,
                ),
                Spec(
                    id=target_id,
                    board_id=board_id,
                    title="Prerequisite Spec",
                    status=(SpecStatus.APPROVED if blocked else SpecStatus.DONE),
                    edition=1,
                    last_started_edition=None,
                    version=1,
                    created_by=USER_ID,
                ),
            )
        )
        await db.flush()
        if blocked:
            db.add(
                SpecDependency(
                    id=f"skm-cs-{case}-dependency",
                    board_id=board_id,
                    dependent_spec_id=source_id,
                    prerequisite_spec_id=target_id,
                    prerequisite_spec_ref=target_id,
                    active=True,
                    resolved_on_create=False,
                    retrospective=False,
                    introduced_at_spec_version=1,
                    source_version_on_create=1,
                    source_status_on_create=SpecStatus.APPROVED.value,
                    target_status_on_create=SpecStatus.APPROVED.value,
                    target_version_on_create=1,
                    target_title_on_create="Prerequisite Spec",
                    target_edition_on_create=1,
                    target_ideation_id_on_create=None,
                    add_idempotency_key="skm-create-started-seed",
                    add_request_digest="a" * 64,
                    created_at=datetime.now(timezone.utc),
                    created_by_id=USER_ID,
                    created_by_type="user",
                    created_by_name="SK-M owner",
                )
            )
        await db.commit()
    return board_id, source_id, target_id


async def _effect_counts(
    db_factory,  # noqa: ANN001
    *,
    board_id: str,
) -> tuple[int, int, int]:
    async with db_factory() as db:
        cards = await db.scalar(
            select(func.count(Card.id)).where(Card.board_id == board_id)
        )
        activities = await db.scalar(
            select(func.count(ActivityLog.id)).where(ActivityLog.board_id == board_id)
        )
        events = await db.scalar(
            select(func.count(DomainEventRow.id)).where(
                DomainEventRow.board_id == board_id
            )
        )
        return int(cards or 0), int(activities or 0), int(events or 0)


def _started_card(title: str, *, spec_id: str) -> CardCreate:
    return CardCreate(
        title=title,
        spec_id=spec_id,
        status=CardStatus.STARTED,
    )


async def _seed_validation_test_card(
    db_factory,  # noqa: ANN001
    *,
    board_id: str,
    spec_id: str,
    case: str,
) -> str:
    card_id = f"skm-cs-{case}-test-card"
    async with db_factory() as db:
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Test card awaiting rework",
                status=CardStatus.VALIDATION,
                card_type=CardType.TEST,
                position=0,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return card_id


@pytest.mark.asyncio
async def test_create_started_blocks_before_card_audit_or_event(
    db_factory,  # noqa: ANN001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    board_id, source_id, target_id = await _seed_specs(
        db_factory,
        case="block",
        blocked=True,
    )

    async def unexpected_effect(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("precedence gate must run before critical-context audit")

    monkeypatch.setattr(
        main_service,
        "_authorize_critical_context_or_raise",
        unexpected_effect,
    )

    async with db_factory() as db:
        with pytest.raises(SpecDependencyOperationError) as caught:
            await CardService(db).create_card(
                board_id,
                USER_ID,
                _started_card("Must remain blocked", spec_id=source_id),
            )

    assert caught.value.code == "spec_dependencies_incomplete"
    assert caught.value.facts["spec_id"] == source_id
    assert caught.value.facts["blocking_count"] == 1
    assert caught.value.facts["blocking_dependencies"][0]["target_spec_id"] == target_id
    assert await _effect_counts(db_factory, board_id=board_id) == (0, 0, 0)
    async with db_factory() as db:
        marker = await db.scalar(
            select(Spec.last_started_edition).where(Spec.id == source_id)
        )
    assert marker is None


@pytest.mark.asyncio
async def test_create_started_marks_current_edition_and_persists_card(
    db_factory,  # noqa: ANN001
) -> None:
    board_id, source_id, _target_id = await _seed_specs(
        db_factory,
        case="success",
        blocked=False,
    )

    async with db_factory() as db:
        created = await CardService(db).create_card(
            board_id,
            USER_ID,
            _started_card("Ready to start", spec_id=source_id),
        )
        assert created is not None
        assert created.status is CardStatus.STARTED
        await db.commit()

    async with db_factory() as db:
        marker = await db.scalar(
            select(Spec.last_started_edition).where(Spec.id == source_id)
        )
        persisted = await db.scalar(
            select(Card).where(
                Card.board_id == board_id,
                Card.title == "Ready to start",
            )
        )
    assert marker == 3
    assert persisted is not None
    assert persisted.status is CardStatus.STARTED


@pytest.mark.asyncio
async def test_create_started_later_failure_rolls_back_marker_and_all_effects(
    db_factory,  # noqa: ANN001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    board_id, source_id, _target_id = await _seed_specs(
        db_factory,
        case="rollback",
        blocked=False,
    )
    add_attempts: list[str] = []

    async def fail_first_effect(
        _db,
        record,
        *_args,
        **_kwargs,  # noqa: ANN001, ANN002, ANN003
    ) -> None:
        add_attempts.append(type(record).__name__)
        raise RuntimeError("injected_after_precedence_gate")

    monkeypatch.setattr(main_service, "_application_add", fail_first_effect)

    async with db_factory() as db:
        with pytest.raises(RuntimeError, match="injected_after_precedence_gate"):
            await CardService(db).create_card(
                board_id,
                USER_ID,
                _started_card("Must roll back", spec_id=source_id),
            )
        # The gate ran before the injected first application effect and staged
        # the lifecycle marker in this transaction.
        staged_marker = await db.scalar(
            select(Spec.last_started_edition)
            .where(Spec.id == source_id)
            .execution_options(populate_existing=True)
        )
        assert staged_marker == 3

    assert add_attempts == ["ApplicationRecord"]
    assert await _effect_counts(db_factory, board_id=board_id) == (0, 0, 0)
    async with db_factory() as db:
        durable_marker = await db.scalar(
            select(Spec.last_started_edition).where(Spec.id == source_id)
        )
    assert durable_marker is None


@pytest.mark.asyncio
async def test_test_card_validation_rework_still_enforces_spec_precedence(
    db_factory,  # noqa: ANN001
) -> None:
    board_id, source_id, target_id = await _seed_specs(
        db_factory,
        case="test-rework-block",
        blocked=True,
    )
    card_id = await _seed_validation_test_card(
        db_factory,
        board_id=board_id,
        spec_id=source_id,
        case="test-rework-block",
    )

    async with db_factory() as db:
        with pytest.raises(SpecDependencyOperationError) as caught:
            await CardService(db).move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    assert caught.value.code == "spec_dependencies_incomplete"
    assert caught.value.facts["blocking_dependencies"][0]["target_spec_id"] == target_id
    assert await _effect_counts(db_factory, board_id=board_id) == (1, 0, 0)
    async with db_factory() as db:
        card = await db.get(Card, card_id)
        marker = await db.scalar(
            select(Spec.last_started_edition).where(Spec.id == source_id)
        )
    assert card is not None
    assert card.status is CardStatus.VALIDATION
    assert marker is None


@pytest.mark.asyncio
async def test_test_card_validation_rework_fences_concurrent_spec_edition_change(
    db_factory,  # noqa: ANN001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    board_id, source_id, _target_id = await _seed_specs(
        db_factory,
        case="test-rework-fence",
        blocked=False,
    )
    card_id = await _seed_validation_test_card(
        db_factory,
        board_id=board_id,
        spec_id=source_id,
        case="test-rework-fence",
    )

    from okto_pulse.core.services.spec_dependency import SpecDependencyService

    original = SpecDependencyService.require_ready_for_execution

    async def change_edition_before_locked_recheck(
        service: SpecDependencyService,
        **kwargs,  # noqa: ANN003
    ):
        # Simulate a lifecycle writer winning after CardService captured its
        # optimistic identity and before the dependency graph fence re-check.
        await service.persistence._session.execute(  # noqa: SLF001
            update(Spec).where(Spec.id == source_id).values(edition=4)
        )
        return await original(service, **kwargs)

    monkeypatch.setattr(
        SpecDependencyService,
        "require_ready_for_execution",
        change_edition_before_locked_recheck,
    )

    async with db_factory() as db:
        with pytest.raises(SpecDependencyOperationError) as caught:
            await CardService(db).move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts == {
        "spec_id": source_id,
        "expected_spec_edition": 3,
        "current_spec_edition": 4,
    }
    assert await _effect_counts(db_factory, board_id=board_id) == (1, 0, 0)
    async with db_factory() as db:
        card = await db.get(Card, card_id)
        edition = await db.scalar(select(Spec.edition).where(Spec.id == source_id))
    assert card is not None
    assert card.status is CardStatus.VALIDATION
    assert edition == 3
