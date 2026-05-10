from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationKnowledgeBase,
    IdeationStatus,
    Refinement,
    ResourceNotApplicable,
    Spec,
    SpecKnowledgeBase,
)
from okto_pulse.core.models.schemas import IdeationMove
from okto_pulse.core.services.main import IdeationService
from okto_pulse.core.services.resource_gate import (
    ResourceGateJustificationRequired,
    ResourceGateService,
    ResourceGateViolation,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


@pytest.mark.asyncio
async def test_resource_gate_resolves_direct_inherited_and_na_precedence(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")
    refinement_id = _id("ref")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea with resources",
                created_by=actor_id,
                screen_mockups=[{"id": "mock-1", "title": "Primary flow"}],
            )
        )
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Idea architecture",
                global_description="Architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor_id,
            )
        )
        db.add(
            IdeationKnowledgeBase(
                ideation_id=ideation_id,
                title="Idea KB",
                content="Knowledge",
                created_by=actor_id,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Refinement inherits resources",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        await service.mark_not_applicable(
            board_id,
            "refinement",
            refinement_id,
            "mockup",
            actor_id,
            justification="Mockup initially considered unnecessary",
            source_channel="ui",
        )

        summary = await service.get_summary(board_id, "refinement", refinement_id)

        by_type = {item["resource_type"]: item for item in summary["resources"]}
        assert summary["blocking"] is False
        assert by_type["architecture"]["state"] == "provided"
        assert by_type["architecture"]["direct_count"] == 0
        assert by_type["architecture"]["inherited_count"] == 1
        assert by_type["mockup"]["state"] == "provided"
        assert by_type["mockup"]["na_mark"]["active"] is True
        assert by_type["mockup"]["na_mark"]["effective"] is False
        assert by_type["knowledge_base"]["state"] == "provided"
        assert by_type["knowledge_base"]["inherited_refs"][0]["source_entity_type"] == "ideation"


@pytest.mark.asyncio
async def test_resource_gate_requires_justification_for_mcp_and_returns_warning(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea without resources",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        with pytest.raises(ResourceGateJustificationRequired):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                "architecture",
                actor_id,
                source_channel="mcp",
            )

        result = await service.mark_not_applicable(
            board_id,
            "ideation",
            ideation_id,
            "architecture",
            actor_id,
            justification="Architecture is not needed for this small text-only change.",
            source_channel="mcp",
        )

        by_type = {
            item["resource_type"]: item
            for item in result["summary"]["resources"]
        }
        assert result["warning"]
        assert by_type["architecture"]["state"] == "not_applicable"
        assert by_type["architecture"]["na_mark"]["effective"] is True
        assert by_type["mockup"]["state"] == "missing"


@pytest.mark.asyncio
async def test_resource_gate_clear_na_reveals_missing_state(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec without KB",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        await service.mark_not_applicable(
            board_id,
            "spec",
            spec_id,
            "knowledge_base",
            actor_id,
            source_channel="ui",
        )
        cleared = await service.clear_not_applicable(
            board_id,
            "spec",
            spec_id,
            "knowledge_base",
            actor_id,
            reason="KB is applicable after all",
        )

        by_type = {item["resource_type"]: item for item in cleared["summary"]["resources"]}
        assert cleared["cleared"] == 1
        assert by_type["knowledge_base"]["state"] == "missing"
        assert by_type["knowledge_base"]["na_mark"] is None

        rows = (
            await db.execute(
                ResourceNotApplicable.__table__.select().where(
                    ResourceNotApplicable.board_id == board_id,
                    ResourceNotApplicable.entity_type == "spec",
                    ResourceNotApplicable.entity_id == spec_id,
                )
            )
        ).all()
        assert len(rows) == 1
        assert rows[0]._mapping["active"] is False


@pytest.mark.asyncio
async def test_resource_gate_validates_spec_resources_are_covered_by_non_cancelled_tasks(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with resources",
            created_by=actor_id,
            screen_mockups=[{"id": "mock-1", "title": "Primary flow"}],
        )
        task = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implement resource-aware flow",
            created_by=actor_id,
            card_type=CardType.NORMAL,
            status=CardStatus.NOT_STARTED,
        )
        kb = SpecKnowledgeBase(
            spec_id=spec_id,
            title="Reference notes",
            content="Operational reference",
            created_by=actor_id,
        )
        db.add(spec)
        db.add(task)
        db.add(kb)
        await db.flush()
        architecture = ArchitectureDesign(
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Architecture context",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        db.add(architecture)
        await db.flush()

        service = ResourceGateService(db)
        uncovered = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert uncovered["allowed"] is False
        assert {item["resource_type"] for item in uncovered["uncovered_resources"]} == {
            "architecture",
            "mockup",
            "knowledge_base",
        }

        task.screen_mockups = [{"id": "card-mock-1", "origin_id": "mock-1"}]
        task.knowledge_bases = [{"id": "card-kb-1", "source_kb_id": kb.id}]
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title="Task architecture",
                global_description="Task architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                source_design_id=architecture.id,
                created_by=actor_id,
            )
        )
        await db.flush()

        covered = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert covered["allowed"] is True

        task.status = CardStatus.CANCELLED
        await db.flush()

        cancelled_only = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert cancelled_only["allowed"] is False
        assert {
            item["reason"] for item in cancelled_only["uncovered_resources"]
        } == {"covered_only_by_cancelled_task"}


@pytest.mark.asyncio
async def test_resource_gate_blocks_done_transition_until_resources_provided_or_na(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea requiring explicit resources",
                created_by=actor_id,
                status=IdeationStatus.EVALUATING,
            )
        )
        await db.commit()

        with pytest.raises(ResourceGateViolation):
            await IdeationService(db).move_ideation(
                ideation_id,
                actor_id,
                IdeationMove(status=IdeationStatus.DONE),
            )

        service = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is intentionally not applicable in this test.",
                source_channel="ui",
            )

        moved = await IdeationService(db).move_ideation(
            ideation_id,
            actor_id,
            IdeationMove(status=IdeationStatus.DONE),
        )
        assert moved.status == IdeationStatus.DONE
