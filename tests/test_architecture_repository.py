"""Tests for Architecture Design repository behavior."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.models.db import (
    ArchitectureDesignVersion,
    ArchitectureDiagramPayload,
    Board,
    DomainEventRow,
    Ideation,
    Refinement,
    Spec,
)
from okto_pulse.core.models.schemas import ArchitectureDesignCreate, ArchitectureDesignUpdate
from okto_pulse.core.services.architecture import ArchitectureDesignRepository


USER_ID = "architecture-repository-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_ideation(db_factory) -> tuple[str, str]:
    board_id = _id("architecture-board")
    ideation_id = _id("architecture-ideation")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture Repository Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture Repository Ideation",
                description="Seed ideation for architecture tests",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, ideation_id


async def _seed_spec_and_refinement(db_factory) -> tuple[str, str, str, str]:
    board_id = _id("architecture-event-board")
    ideation_id = _id("architecture-event-ideation")
    refinement_id = _id("architecture-event-refinement")
    spec_id = _id("architecture-event-spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture Event Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture Event Ideation",
                description="Seed ideation for event tests",
                created_by=USER_ID,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="Architecture Event Refinement",
                created_by=USER_ID,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                refinement_id=refinement_id,
                title="Architecture Event Spec",
                created_by=USER_ID,
                functional_requirements=[],
                technical_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, ideation_id, refinement_id, spec_id


def _architecture_payload(source_ref: str = "ideation:source") -> ArchitectureDesignCreate:
    return ArchitectureDesignCreate(
        title="Architecture Tab",
        global_description="Architecture data is captured as a first-class artifact.",
        entities=[
            {
                "id": "entity-architecture-repository",
                "name": "ArchitectureDesignRepository",
                "entity_type": "service",
                "responsibility": "Persist architecture envelopes and versions.",
                "technologies": ["SQLAlchemy"],
            }
        ],
        interfaces=[
            {
                "id": "interface-diagram-store",
                "name": "ArchitectureDiagramStore",
                "description": "Stores heavy diagram adapter payloads behind opaque refs.",
                "participants": ["ArchitectureDesignRepository", "ArchitectureDiagramPayload"],
                "contract_type": "repository",
            }
        ],
        diagrams=[
            {
                "id": "diagram-context",
                "title": "Context diagram",
                "diagram_type": "context",
                "format": "excalidraw_json",
                "adapter_payload": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [
                        {"id": "box-1", "type": "rectangle", "x": 10, "y": 20},
                        {"id": "label-1", "type": "text", "text": "Architecture"},
                    ],
                    "appState": {},
                    "files": {},
                },
            }
        ],
        source_ref=source_ref,
        source_version=1,
    )


@pytest.mark.asyncio
async def test_create_design_stores_diagram_payload_separately(db_factory):
    board_id, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)

        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)
        await db.commit()

        assert design.board_id == board_id
        assert design.parent_type == "ideation"
        assert design.ideation_id == ideation_id
        assert design.version == 1
        assert design.global_description.startswith("Architecture data")
        assert len(design.diagrams) == 1

        diagram = design.diagrams[0]
        assert "adapter_payload" not in diagram
        assert diagram["adapter_payload_ref"]
        assert diagram["content_hash"]
        assert diagram["size_bytes"] > 0

        row = (
            await db.execute(
                select(ArchitectureDiagramPayload).where(
                    ArchitectureDiagramPayload.design_id == design.id,
                    ArchitectureDiagramPayload.diagram_id == "diagram-context",
                )
            )
        ).scalar_one()
        assert row.board_id == board_id
        assert row.storage_backend == "database"
        assert row.adapter_payload_json["elements"][1]["text"] == "Architecture"

        versions = (
            await db.execute(
                select(ArchitectureDesignVersion).where(
                    ArchitectureDesignVersion.design_id == design.id,
                )
            )
        ).scalars().all()
        assert [snapshot.version for snapshot in versions] == [1]


@pytest.mark.asyncio
async def test_summary_is_lightweight_and_response_can_include_payloads(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)
        await db.commit()

        listed = await repo.list("ideation", ideation_id)
        assert len(listed) == 1
        assert "adapter_payload" not in listed[0].diagrams[0]

        summary = repo.to_summary(listed[0])
        assert summary.parent_id == ideation_id
        assert summary.diagrams_count == 1
        assert summary.adapter_payload_refs == [listed[0].diagrams[0]["adapter_payload_ref"]]

        loaded = await repo.get(design.id, include_payloads=True)
        assert loaded is not None
        response = repo.to_response(loaded)
        assert response.diagrams[0].adapter_payload["elements"][1]["text"] == "Architecture"


@pytest.mark.asyncio
async def test_update_creates_new_version_and_diff_marks_semantic_changes(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)

        updated = await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                global_description="Architecture now versions semantic changes without review flags.",
                interfaces=[
                    {
                        "id": "interface-diagram-store",
                        "name": "ArchitectureDiagramStore",
                        "description": "Updated interface contract.",
                    }
                ],
                change_summary="Document architecture versioning",
            ),
            USER_ID,
        )
        await db.commit()

        assert updated.version == 2
        diff = await repo.diff(design.id, 1, 2)
        assert diff.changed_fields == ["global_description", "interfaces"]
        assert {"field": "global_description"} in diff.semantic_changes
        assert diff.breaking_change_flag is False
        assert diff.requires_arch_review is False

        versions = (
            await db.execute(
                select(ArchitectureDesignVersion)
                .where(ArchitectureDesignVersion.design_id == design.id)
                .order_by(ArchitectureDesignVersion.version)
            )
        ).scalars().all()
        assert [snapshot.version for snapshot in versions] == [1, 2]
        assert versions[1].change_summary == "Document architecture versioning"


@pytest.mark.asyncio
async def test_change_control_flags_are_ignored(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload(f"ideation:source:{uuid.uuid4()}")
        payload.stale = True
        payload.breaking_change_flag = True
        payload.requires_arch_review = True

        design = await repo.create("ideation", ideation_id, payload, USER_ID)
        updated = await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                stale=True,
                breaking_change_flag=True,
                requires_arch_review=True,
                change_summary="Ignored change-control flags",
            ),
            USER_ID,
        )
        await db.commit()

        assert design.stale is False
        assert design.breaking_change_flag is False
        assert design.requires_arch_review is False
        assert updated.stale is False
        assert updated.breaking_change_flag is False
        assert updated.requires_arch_review is False
        response = repo.to_response(updated)
        assert response.stale is False
        assert response.breaking_change_flag is False
        assert response.requires_arch_review is False


@pytest.mark.asyncio
async def test_spec_architecture_mutations_emit_semantic_changed_events(db_factory):
    board_id, _, _, spec_id = await _seed_spec_and_refinement(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("spec", spec_id, _architecture_payload(), USER_ID)
        await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                global_description="Spec architecture changed semantically.",
                change_summary="Update architecture description",
            ),
            USER_ID,
        )
        await repo.delete(design.id, USER_ID)
        await db.commit()

    async with db_factory() as db:
        events = (
            await db.execute(
                select(DomainEventRow)
                .where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "spec.semantic_changed",
                )
                .order_by(DomainEventRow.occurred_at, DomainEventRow.id)
            )
        ).scalars().all()

        assert len(events) == 3
        assert {event.actor_id for event in events} == {USER_ID}
        assert all(event.payload_json["spec_id"] == spec_id for event in events)
        assert all(
            event.payload_json["changed_fields"] == ["architecture_designs"]
            for event in events
        )


@pytest.mark.asyncio
async def test_refinement_architecture_create_emits_semantic_changed_event(db_factory):
    board_id, _, refinement_id, _ = await _seed_spec_and_refinement(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        await repo.create("refinement", refinement_id, _architecture_payload(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        event = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "refinement.semantic_changed",
                )
            )
        ).scalar_one()

        assert event.actor_id == USER_ID
        assert event.payload_json["refinement_id"] == refinement_id
        assert event.payload_json["changed_fields"] == ["architecture_designs"]
