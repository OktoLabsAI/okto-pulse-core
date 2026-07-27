"""ts_3ab9c08b — Resource Gate authority across all blocking call-sites."""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.services.resource_gate import (
    ResourceGateService,
    ResourceGateViolation,
)
from okto_pulse.core.services.resource_gate_authority import (
    resource_gate_authority_policy,
)
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    Spec,
    SpecKnowledgeBase,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def test_ts_3ab9c08b_authority_matrix_is_explicit_and_fail_closed():
    for context in ("entity_completion", "spec_validation", "spec_done"):
        policy = resource_gate_authority_policy(context)

        assert policy.to_dict() == {
            "version": 1,
            "context": context,
            "blocking_resource_types": ["architecture", "mockup"],
            "advisory_resource_types": ["knowledge_base"],
        }
        assert policy.authority_for("architecture") == "blocking"
        assert policy.authority_for("mockup") == "blocking"
        assert policy.authority_for("knowledge_base") == "advisory"

    with pytest.raises(ValueError, match="Unknown Resource Gate authority context"):
        resource_gate_authority_policy("unregistered_transition")


@pytest.mark.asyncio
async def test_ts_3ab9c08b_level1_keeps_kb_visible_without_blocking(db_factory):
    board_id = _id("board")
    actor_id = _id("actor")
    ideation_id = _id("ideation")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Advisory KB", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Level 1 authority",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        summary = await service.get_summary(board_id, "ideation", ideation_id)
        by_type = {
            item["resource_type"]: item for item in summary["resources"]
        }

        assert {
            item["resource_type"] for item in summary["missing_resources"]
        } == {"architecture", "mockup"}
        assert by_type["architecture"]["blocking"] is True
        assert by_type["mockup"]["blocking"] is True
        assert by_type["knowledge_base"] == summary["advisory_resources"][0]
        assert by_type["knowledge_base"]["state"] == "missing"
        assert by_type["knowledge_base"]["authority"] == "advisory"
        assert by_type["knowledge_base"]["blocking"] is False
        assert summary["advisory_missing_resources"] == [
            by_type["knowledge_base"]
        ]
        assert summary["resource_lineage"]["attachments"] == []

        blocked = await service.validate_entity_completion(
            board_id,
            "ideation",
            ideation_id,
        )
        assert blocked["allowed"] is False
        assert {
            item["resource_type"] for item in blocked["blocking_resources"]
        } == {"architecture", "mockup"}

        for resource_type in ("architecture", "mockup"):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is out of scope.",
                source_channel="ui",
            )

        allowed = await service.validate_or_raise_entity_completion(
            board_id,
            "ideation",
            ideation_id,
            phase="ideation_done",
        )

    assert allowed["allowed"] is True
    assert allowed["blocking_resources"] == []
    assert allowed["summary"]["blocking"] is False
    assert [
        item["resource_type"]
        for item in allowed["summary"]["advisory_missing_resources"]
    ] == ["knowledge_base"]
    assert {
        item["resource_type"]: item["state"]
        for item in allowed["summary"]["resources"]
    } == {
        "architecture": "not_applicable",
        "mockup": "not_applicable",
        "knowledge_base": "missing",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["spec_validation", "spec_done"])
async def test_ts_3ab9c08b_level2_kb_is_history_not_blocking_obligation(
    db_factory,
    phase,
):
    board_id = _id("board")
    actor_id = _id("actor")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Level 2 authority", owner_id=actor_id))
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with all resource classes",
            created_by=actor_id,
            screen_mockups=[{"id": "mockup-root", "title": "Primary flow"}],
        )
        card = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation task",
            created_by=actor_id,
            card_type=CardType.NORMAL,
            status=CardStatus.IN_PROGRESS,
            knowledge_bases=[
                {
                    "id": "card-kb-copy",
                    "source_kb_id": "kb-root",
                    "title": "Advisory implementation context",
                }
            ],
        )
        db.add_all(
            [
                spec,
                card,
                SpecKnowledgeBase(
                    id="kb-root",
                    spec_id=spec_id,
                    title="Advisory implementation context",
                    content="Useful, but never mandatory.",
                    created_by=actor_id,
                ),
            ]
        )
        await db.flush()
        architecture = ArchitectureDesign(
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Blocking architecture evidence",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        db.add(architecture)
        await db.flush()

        service = ResourceGateService(db)
        with pytest.raises(ResourceGateViolation) as exc_info:
            await service.validate_or_raise_spec_resource_task_coverage(
                board_id,
                spec_id,
                phase=phase,
            )

        assert exc_info.value.code == "resource_gate_spec_task_coverage"
        details = exc_info.value.details
        assert {
            item["resource_type"] for item in details["required_resources"]
        } == {"architecture", "mockup"}
        assert {
            item["resource_type"] for item in details["uncovered_resources"]
        } == {"architecture", "mockup"}
        assert details["summary"]["authority_policy"]["context"] == phase
        assert {
            item["resource_type"]
            for item in details["summary"]["resource_lineage"][
                "coverage_obligations"
            ]
        } == {"architecture", "mockup"}
        advisory = details["summary"]["resource_lineage"][
            "advisory_coverage_resources"
        ]
        assert [item["resource_type"] for item in advisory] == [
            "knowledge_base"
        ]
        assert advisory[0]["unique_resource_id"] == "knowledge_base:kb-root"
        assert advisory[0]["blocking"] is False

        card.screen_mockups = [
            {
                "id": "card-mockup-copy",
                "origin_id": "mockup-root",
                "title": "Primary flow",
            }
        ]
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title="Task architecture copy",
                global_description="Blocking architecture task evidence",
                entities=[],
                interfaces=[],
                diagrams=[],
                source_design_id=architecture.id,
                created_by=actor_id,
            )
        )
        await db.flush()

        allowed = await service.validate_or_raise_spec_resource_task_coverage(
            board_id,
            spec_id,
            phase=phase,
        )

    assert allowed["allowed"] is True
    assert allowed["uncovered_resources"] == []
    assert {
        item["resource_type"] for item in allowed["required_resources"]
    } == {"architecture", "mockup"}
    assert [
        item["resource_type"]
        for item in allowed["advisory_coverage_resources"]
    ] == ["knowledge_base"]
