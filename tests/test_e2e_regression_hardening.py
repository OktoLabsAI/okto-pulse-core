"""Regression guards for E2E audit findings closed after the 2026-06-28 run."""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.models.db import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate, SprintMove
from okto_pulse.core.services.main import (
    CardOperationError,
    CardService,
    SpecService,
    SprintOperationError,
    SprintService,
)
from okto_pulse.core.services.resource_gate import (
    ResourceGateService,
    ResourceGateViolation,
)


USER_ID = "e2e-hardening-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _validation_payload() -> dict:
    return {
        "completeness": 95,
        "completeness_justification": "All mandatory checks are satisfied.",
        "assertiveness": 90,
        "assertiveness_justification": "Statements are deterministic.",
        "ambiguity": 5,
        "ambiguity_justification": "No blocking ambiguity remains.",
        "general_justification": "Ready for execution.",
        "recommendation": "approve",
    }


async def _seed_board_and_spec(
    db,
    *,
    board_settings: dict | None = None,
    spec_status: SpecStatus = SpecStatus.APPROVED,
    scenarios: list[dict] | None = None,
) -> tuple[str, str]:
    board_id = _id("board")
    spec_id = _id("spec")
    db.add(
        Board(
            id=board_id,
            name=f"E2E hardening {board_id}",
            owner_id=USER_ID,
            settings=board_settings or {},
        )
    )
    db.add(
        Spec(
            id=spec_id,
            board_id=board_id,
            title=f"E2E hardening spec {spec_id}",
            status=spec_status,
            created_by=USER_ID,
            functional_requirements=[],
            acceptance_criteria=[],
            test_scenarios=scenarios or [],
            business_rules=[],
            api_contracts=[],
            technical_requirements=[],
            integration_requirements=[],
            observability_requirements=[],
            # Spec 4028ebd4 (decision-required gate): submit_spec_validation
            # demands at least one active Decision before any other gate under
            # test here (resource/architecture policy) gets a chance to run.
            # Decision->Task coverage is skipped: that gate has its own suite
            # (tests/test_task_requirement_link_gate.py) and would otherwise
            # fire before the contracts under test.
            decisions=[
                {
                    "id": "dec_seed",
                    "title": "Seed decision",
                    "rationale": "Fixture decision satisfying the decision-required gate.",
                    "status": "active",
                },
            ],
            skip_decisions_coverage=True,
        )
    )
    await db.flush()
    return board_id, spec_id


@pytest.mark.asyncio
async def test_card_service_enforces_max_scenarios_on_create_and_update(db_factory):
    scenarios = [
        {"id": "ts-1", "title": "Scenario 1", "status": "draft", "linked_criteria": []},
        {"id": "ts-2", "title": "Scenario 2", "status": "draft", "linked_criteria": []},
        {"id": "ts-3", "title": "Scenario 3", "status": "draft", "linked_criteria": []},
    ]
    async with db_factory() as db:
        board_id, spec_id = await _seed_board_and_spec(
            db,
            board_settings={"max_scenarios_per_card": 2},
            scenarios=scenarios,
        )
        service = CardService(db)

        with pytest.raises(CardOperationError) as create_exc:
            await service.create_card(
                board_id,
                USER_ID,
                CardCreate(
                    title="Too broad test card",
                    card_type=CardType.TEST,
                    spec_id=spec_id,
                    test_scenario_ids=["ts-1", "ts-2", "ts-3"],
                ),
            )
        assert create_exc.value.code == "max_scenarios_per_card_exceeded"

        card = await service.create_card(
            board_id,
            USER_ID,
            CardCreate(
                title="Valid test card",
                card_type=CardType.TEST,
                spec_id=spec_id,
                test_scenario_ids=["ts-1", "ts-2"],
            ),
        )
        assert card is not None

        with pytest.raises(CardOperationError) as update_exc:
            await service.update_card(
                card.id,
                USER_ID,
                CardUpdate(test_scenario_ids=["ts-1", "ts-2", "ts-3"]),
            )
        assert update_exc.value.code == "max_scenarios_per_card_exceeded"


@pytest.mark.asyncio
async def test_sprint_close_blocks_non_terminal_cards(db_factory):
    async with db_factory() as db:
        board_id, spec_id = await _seed_board_and_spec(
            db,
            spec_status=SpecStatus.IN_PROGRESS,
        )
        sprint = Sprint(
            id=_id("sprint"),
            board_id=board_id,
            spec_id=spec_id,
            title="Sprint with unfinished work",
            status=SprintStatus.REVIEW,
            skip_qualitative_validation=True,
            created_by=USER_ID,
        )
        card = Card(
            id=_id("card"),
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=sprint.id,
            title="Still open",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
        )
        db.add_all([sprint, card])
        await db.flush()

        with pytest.raises(SprintOperationError) as exc:
            await SprintService(db).move_sprint(
                sprint.id,
                USER_ID,
                SprintMove(status=SprintStatus.CLOSED),
            )
        assert exc.value.code == "sprint_has_incomplete_cards"
        assert exc.value.facts["open_cards"][0]["id"] == card.id


@pytest.mark.asyncio
async def test_sprint_close_allows_terminal_cards(db_factory):
    async with db_factory() as db:
        board_id, spec_id = await _seed_board_and_spec(
            db,
            spec_status=SpecStatus.IN_PROGRESS,
        )
        sprint = Sprint(
            id=_id("sprint"),
            board_id=board_id,
            spec_id=spec_id,
            title="Sprint with complete work",
            status=SprintStatus.REVIEW,
            skip_qualitative_validation=True,
            created_by=USER_ID,
        )
        db.add_all(
            [
                sprint,
                Card(
                    id=_id("card"),
                    board_id=board_id,
                    spec_id=spec_id,
                    sprint_id=sprint.id,
                    title="Done work",
                    status=CardStatus.DONE,
                    card_type=CardType.NORMAL,
                    created_by=USER_ID,
                ),
                Card(
                    id=_id("card"),
                    board_id=board_id,
                    spec_id=spec_id,
                    sprint_id=sprint.id,
                    title="Cancelled work",
                    status=CardStatus.CANCELLED,
                    card_type=CardType.NORMAL,
                    created_by=USER_ID,
                ),
            ]
        )
        await db.flush()

        moved = await SprintService(db).move_sprint(
            sprint.id,
            USER_ID,
            SprintMove(status=SprintStatus.CLOSED),
        )
        assert moved.status == SprintStatus.CLOSED


@pytest.mark.asyncio
async def test_bug_regression_gate_blocks_direct_jump_to_validation(db_factory):
    async with db_factory() as db:
        board_id, spec_id = await _seed_board_and_spec(
            db,
            spec_status=SpecStatus.IN_PROGRESS,
        )
        origin = Card(
            id=_id("origin"),
            board_id=board_id,
            spec_id=spec_id,
            title="Origin task",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
        )
        bug = Card(
            id=_id("bug"),
            board_id=board_id,
            spec_id=spec_id,
            origin_task_id=origin.id,
            title="Regression bug",
            status=CardStatus.STARTED,
            card_type=CardType.BUG,
            severity=BugSeverity.MAJOR,
            expected_behavior="Request succeeds",
            observed_behavior="Request fails",
            created_by=USER_ID,
        )
        db.add_all([origin, bug])
        await db.flush()

        with pytest.raises(CardOperationError) as exc:
            await CardService(db).move_card(
                bug.id,
                USER_ID,
                CardMove(status=CardStatus.VALIDATION),
            )
        assert exc.value.code == "missing_regression_test_task"


@pytest.mark.asyncio
async def test_spec_validation_blocks_missing_architecture_when_policy_requires_it(db_factory):
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(
            db,
            board_settings={
                "require_spec_validation": True,
                "auto_derive_spec_resources_enabled": True,
                "auto_derive_spec_resource_types": ["architecture"],
            },
        )

        with pytest.raises(ResourceGateViolation) as exc:
            await SpecService(db).submit_spec_validation(
                spec_id,
                USER_ID,
                "Validator",
                _validation_payload(),
            )
        assert exc.value.code == "resource_gate_spec_missing_architecture"


@pytest.mark.asyncio
async def test_spec_validation_accepts_architecture_na_and_policy_off(db_factory):
    async with db_factory() as db:
        board_id, spec_id = await _seed_board_and_spec(
            db,
            board_settings={
                "require_spec_validation": True,
                "auto_derive_spec_resources_enabled": True,
                "auto_derive_spec_resource_types": ["architecture"],
            },
        )
        await ResourceGateService(db).mark_not_applicable(
            board_id,
            "spec",
            spec_id,
            "architecture",
            USER_ID,
            justification="Architecture is intentionally not applicable to this spec.",
            source_channel="ui",
        )
        result = await SpecService(db).submit_spec_validation(
            spec_id,
            USER_ID,
            "Validator",
            _validation_payload(),
        )
        assert result["spec_status"] == "validated"

    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(
            db,
            board_settings={
                "require_spec_validation": True,
                "auto_derive_spec_resources_enabled": False,
                "auto_derive_spec_resource_types": [],
            },
        )
        result = await SpecService(db).submit_spec_validation(
            spec_id,
            USER_ID,
            "Validator",
            _validation_payload(),
        )
        assert result["spec_status"] == "validated"
