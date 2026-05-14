from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from okto_pulse.core.models.db import (
    ActivityLog,
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
)
from okto_pulse.core.models.schemas import BoardSettings, BoardUpdate, CardCreate
from okto_pulse.core.services.main import BoardService, CardService, SpecService
from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _settings(*resource_types: str) -> dict:
    return {
        "auto_derive_spec_resources_enabled": True,
        "auto_derive_spec_resource_types": list(resource_types),
    }


async def _seed_spec_with_resources(db, *, board_id: str, actor_id: str) -> dict[str, str]:
    spec_id = _id("spec-auto-resources")
    mockup_id = _id("mock-primary")
    kb_id = _id("kb-primary")
    architecture_id = _id("arch-primary")
    db.add(
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with resources",
            status=SpecStatus.APPROVED,
            created_by=actor_id,
            functional_requirements=["FR"],
            acceptance_criteria=["AC"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
            technical_requirements=[],
            decisions=[],
            screen_mockups=[
                {
                    "id": mockup_id,
                    "title": "Primary mockup",
                    "description": "Expected screen",
                    "screen_type": "form",
                    "html_content": "<div>Mockup</div>",
                }
            ],
        )
    )
    db.add(
        SpecKnowledgeBase(
            id=kb_id,
            spec_id=spec_id,
            title="Spec API contract",
            description="Payload contract",
            content="The card needs this contract.",
            mime_type="text/markdown",
            created_by=actor_id,
        )
    )
    db.add(
        ArchitectureDesign(
            id=architecture_id,
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Architecture context for implementation.",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
    )
    return {
        "spec_id": spec_id,
        "mockup_id": mockup_id,
        "knowledge_id": kb_id,
        "architecture_id": architecture_id,
    }


@pytest.mark.asyncio
async def test_board_settings_require_selected_resource_types_when_enabled():
    with pytest.raises(ValidationError, match="auto_derive_spec_resource_types"):
        BoardSettings(
            auto_derive_spec_resources_enabled=True,
            auto_derive_spec_resource_types=[],
        )

    settings = BoardSettings(
        auto_derive_spec_resources_enabled=True,
        auto_derive_spec_resource_types=["knowledge_base", "knowledge_base"],
    )
    assert [item.value for item in settings.auto_derive_spec_resource_types] == ["knowledge_base"]


@pytest.mark.asyncio
async def test_update_board_serializes_resource_type_enums_to_json(db_factory):
    board_id = _id("board-settings-json")
    actor_id = _id("agent-settings-json")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Settings JSON", owner_id=actor_id))
        await db.flush()

        updated = await BoardService(db).update_board(
            board_id,
            actor_id,
            BoardUpdate(
                settings=BoardSettings(
                    auto_derive_spec_resources_enabled=True,
                    auto_derive_spec_resource_types=["knowledge_base", "mockup"],
                )
            ),
        )

        assert updated is not None
        assert updated.settings["auto_derive_spec_resource_types"] == [
            "knowledge_base",
            "mockup",
        ]


@pytest.mark.asyncio
async def test_create_card_auto_propagates_selected_spec_resources_idempotently(db_factory):
    board_id = _id("board-auto-resources")
    actor_id = _id("agent-auto-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Auto resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base", "mockup", "architecture"),
            )
        )
        ids = await _seed_spec_with_resources(db, board_id=board_id, actor_id=actor_id)
        spec_id = ids["spec_id"]
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Implementation card", spec_id=spec_id),
        )
        assert card is not None

        assert [item["id"] for item in card.knowledge_bases or []] == [f"cardkb_{ids['knowledge_id']}"]
        assert [item["id"] for item in card.screen_mockups or []] == [ids["mockup_id"]]

        arch_count = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == card.id)
            )
        ).scalars().all()
        assert len(arch_count) == 1

        retry = await SpecResourcePropagationService(db).propagate_for_card(
            board_id=board_id,
            spec_id=spec_id,
            card_id=card.id,
            actor_id=actor_id,
            trigger="test_retry",
        )
        assert retry["results"]["knowledge_base"]["copied_count"] == 0
        assert retry["results"]["knowledge_base"]["ignored_count"] == 1
        assert retry["results"]["mockup"]["copied_count"] == 0
        assert retry["results"]["mockup"]["ignored_count"] == 1
        assert retry["results"]["architecture"]["copied_count"] == 0
        assert retry["results"]["architecture"]["ignored_count"] == 1

        refreshed = await db.get(Card, card.id)
        assert len(refreshed.knowledge_bases or []) == 1
        assert len(refreshed.screen_mockups or []) == 1
        arch_after_retry = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == card.id)
            )
        ).scalars().all()
        assert len(arch_after_retry) == 1

        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.card_id == card.id,
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        assert audits
        assert audits[0].details["resource_types"] == [
            "knowledge_base",
            "mockup",
            "architecture",
        ]


@pytest.mark.asyncio
async def test_auto_propagation_allows_selected_resource_types_absent_on_spec(db_factory):
    board_id = _id("board-no-resources")
    actor_id = _id("agent-no-resources")
    spec_id = _id("spec-no-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="No resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base", "mockup", "architecture"),
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec without resources",
                status=SpecStatus.APPROVED,
                created_by=actor_id,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Card still created", spec_id=spec_id),
        )

        assert card is not None
        assert card.knowledge_bases in (None, [])
        assert card.screen_mockups in (None, [])
        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.card_id == card.id,
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        assert audits[0].details["results"]["knowledge_base"]["source_count"] == 0
        assert audits[0].details["results"]["mockup"]["source_count"] == 0
        assert audits[0].details["results"]["architecture"]["source_count"] == 0


@pytest.mark.asyncio
async def test_link_card_to_spec_runs_auto_propagation(db_factory):
    board_id = _id("board-link-resources")
    actor_id = _id("agent-link-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Link resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base"),
            )
        )
        ids = await _seed_spec_with_resources(db, board_id=board_id, actor_id=actor_id)
        spec_id = ids["spec_id"]
        card_id = _id("unlinked-card")
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=None,
                title="Existing card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        await db.flush()

        linked = await SpecService(db).link_card(spec_id, card_id, user_id=actor_id)
        assert linked is True

        card = await db.get(Card, card_id)
        assert card.spec_id == spec_id
        assert [item["id"] for item in card.knowledge_bases or []] == [f"cardkb_{ids['knowledge_id']}"]
