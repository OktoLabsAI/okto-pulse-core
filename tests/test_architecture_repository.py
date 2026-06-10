"""Tests for Architecture Design repository behavior."""

from __future__ import annotations

import copy
import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.models.db import (
    ArchitectureFinding,
    ArchitectureFindingRun,
    ArchitectureDesignVersion,
    ArchitectureDiagramPayload,
    ArchitectureWarningAcknowledgement,
    Board,
    Card,
    CardStatus,
    CardType,
    DomainEventRow,
    Ideation,
    Refinement,
    Spec,
)
from okto_pulse.core.models.schemas import ArchitectureDesignCreate, ArchitectureDesignUpdate
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitecturePropagationService,
    ArchitectureWarningAcknowledgementRequired,
)
from okto_pulse.core.services.architecture_observability import (
    METRIC_FINDING_RUN_PERSIST_TOTAL,
    METRIC_WARNING_ACK_TOTAL,
    assert_architecture_metric_payload_is_safe,
    get_architecture_metric_samples,
    reset_architecture_observability_for_tests,
)


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


async def _seed_spec_card(db_factory) -> tuple[str, str, str]:
    board_id, _, _, spec_id = await _seed_spec_and_refinement(db_factory)
    card_id = _id("architecture-copy-card")
    async with db_factory() as db:
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Architecture Copy Card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


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
            },
            {
                "id": "entity-diagram-payload",
                "name": "ArchitectureDiagramPayload",
                "entity_type": "database_table",
                "responsibility": "Store large diagram adapter payloads outside the envelope.",
            }
        ],
        interfaces=[
            {
                "id": "interface-diagram-store",
                "name": "ArchitectureDiagramStore",
                "description": "Stores heavy diagram adapter payloads behind opaque refs.",
                "participants": ["entity-architecture-repository", "entity-diagram-payload"],
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
                        {
                            "id": "node-repository",
                            "type": "rectangle",
                            "x": 10,
                            "y": 20,
                            "text": "ArchitectureDesignRepository",
                            "linkedEntityId": "entity-architecture-repository",
                        },
                        {
                            "id": "node-payload",
                            "type": "rectangle",
                            "x": 220,
                            "y": 20,
                            "text": "ArchitectureDiagramPayload",
                            "linkedEntityId": "entity-diagram-payload",
                        },
                        {
                            "id": "edge-repository-payload",
                            "type": "arrow",
                            "sourceElementId": "node-repository",
                            "targetElementId": "node-payload",
                            "linkedInterfaceId": "interface-diagram-store",
                            "connectionType": "elbow",
                        },
                    ],
                    "appState": {},
                    "files": {},
                },
            }
        ],
        source_ref=source_ref,
        source_version=1,
    )


def _invalid_connectivity_justification_payload() -> dict:
    payload = _architecture_payload().model_dump(mode="json")
    payload["diagrams"][0]["is_conceptual"] = True
    payload["diagrams"][0]["connectivity_justifications"] = {
        "node-repository": "todo",
    }
    payload["architecture_warning_acknowledgement"] = {
        "accepted": True,
        "statement": "Source acknowledgement is scoped only to the source design.",
    }
    return payload


def _valid_connectivity_justification_payload() -> dict:
    payload = _architecture_payload().model_dump(mode="json")
    payload["entities"].append(
        {
            "id": "entity-external-audit",
            "name": "External Audit Sink",
            "entity_type": "service",
            "responsibility": "Intentionally shown as an external terminal dependency.",
        }
    )
    payload["diagrams"][0]["is_conceptual"] = True
    payload["diagrams"][0]["adapter_payload"]["elements"].append(
        {
            "id": "node-external-audit",
            "type": "rectangle",
            "linkedEntityId": "entity-external-audit",
            "text": "External Audit Sink",
        }
    )
    payload["diagrams"][0]["connectivity_justifications"] = {
        "node-external-audit": "External audit sink is intentionally shown as a terminal dependency for planning.",
    }
    return payload


@pytest.mark.asyncio
async def test_create_rejects_entity_name_that_duplicates_type(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["entities"] = [
            {
                "id": "entity-api",
                "name": "API",
                "entity_type": "api",
                "responsibility": "Generic name should be rejected.",
            }
        ]

        with pytest.raises(ValueError, match=r"entities\[0\]\.name duplicates entity_type"):
            await repo.create("ideation", ideation_id, payload, USER_ID)


@pytest.mark.asyncio
async def test_create_rejects_interface_with_unknown_participant(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["interfaces"] = [
            {
                "id": "interface-missing",
                "name": "Missing participant contract",
                "participants": ["entity-architecture-repository", "entity-missing"],
                "direction": "source_to_target",
            }
        ]

        with pytest.raises(ValueError, match=r"interfaces\[0\]\.participants\[1\].*entity-missing"):
            await repo.create("ideation", ideation_id, payload, USER_ID)


@pytest.mark.asyncio
async def test_create_accepts_multiple_interfaces_on_same_diagram_connection(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["interfaces"] = [
            {
                "id": "interface-save-diagram",
                "name": "Save diagram",
                "endpoint": "PUT /architecture/{design_id}/diagrams/{diagram_id}/payload",
                "description": "Repository stores the diagram payload row.",
                "participants": ["entity-architecture-repository", "entity-diagram-payload"],
                "direction": "source_to_target",
                "protocol": "REST",
                "contract_type": "OpenAPI",
            },
            {
                "id": "interface-load-diagram",
                "name": "Load diagram",
                "endpoint": "GET /architecture/{design_id}/diagrams/{diagram_id}/payload",
                "description": "Repository loads the diagram payload row.",
                "participants": ["entity-architecture-repository", "entity-diagram-payload"],
                "direction": "target_to_source",
                "protocol": "REST",
                "contract_type": "OpenAPI",
            },
        ]
        payload["diagrams"][0]["adapter_payload"]["elements"] = [
            {"id": "node-repository", "type": "rectangle", "linkedEntityId": "entity-architecture-repository"},
            {"id": "node-payload", "type": "rectangle", "linkedEntityId": "entity-diagram-payload"},
            {
                "id": "edge-diagram-payload",
                "type": "arrow",
                "sourceElementId": "node-repository",
                "targetElementId": "node-payload",
                "linkedInterfaceIds": ["interface-save-diagram", "interface-load-diagram"],
                "connectionType": "elbow",
            },
        ]

        design = await repo.create("ideation", ideation_id, payload, USER_ID)

        assert [item["endpoint"] for item in design.interfaces] == [
            "PUT /architecture/{design_id}/diagrams/{diagram_id}/payload",
            "GET /architecture/{design_id}/diagrams/{diagram_id}/payload",
        ]


@pytest.mark.asyncio
async def test_create_rejects_linked_interface_participants_that_do_not_match_edge(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["interfaces"][0]["participants"] = ["entity-diagram-payload", "entity-architecture-repository"]
        payload["diagrams"][0]["adapter_payload"]["elements"] = [
            {"id": "node-repository", "type": "rectangle", "linkedEntityId": "entity-architecture-repository"},
            {"id": "node-payload", "type": "rectangle", "linkedEntityId": "entity-diagram-payload"},
            {
                "id": "edge-diagram-payload",
                "type": "arrow",
                "sourceElementId": "node-repository",
                "targetElementId": "node-payload",
                "linkedInterfaceIds": ["interface-diagram-store"],
            },
        ]

        with pytest.raises(ValueError, match=r"participants .* do not match the connection endpoints"):
            await repo.create("ideation", ideation_id, payload, USER_ID)


@pytest.mark.asyncio
async def test_update_rejects_invalid_interface_direction_without_version_bump(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)

        with pytest.raises(ValueError, match=r"interfaces\[0\]\.direction='both ways' is invalid"):
            await repo.update(
                design.id,
                ArchitectureDesignUpdate(
                    interfaces=[
                        {
                            "id": "interface-diagram-store",
                            "name": "ArchitectureDiagramStore",
                            "participants": ["entity-architecture-repository", "entity-diagram-payload"],
                            "direction": "both ways",
                        }
                    ]
                ),
                USER_ID,
            )

        loaded = await repo.get(design.id)
        assert loaded is not None
        assert loaded.version == 1


@pytest.mark.asyncio
async def test_create_rejects_diagram_with_unknown_linked_entity(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["diagrams"][0]["adapter_payload"]["elements"][0]["linkedEntityId"] = "entity-missing"

        with pytest.raises(ValueError, match=r"diagrams\[0\]\.adapter_payload\.elements\[0\]\.linkedEntityId.*entity-missing"):
            await repo.create("ideation", ideation_id, payload, USER_ID)


@pytest.mark.asyncio
async def test_create_rejects_non_excalidraw_diagram_format(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _architecture_payload().model_dump(mode="json")
        payload["diagrams"][0]["format"] = "mermaid"
        payload["diagrams"][0]["adapter_payload"] = "graph TD\n  Repository --> Payload"

        with pytest.raises(ValueError, match=r"diagrams\[0\]\.format='mermaid' is unsupported"):
            await repo.create("ideation", ideation_id, payload, USER_ID)


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
        assert row.adapter_payload_json["elements"][1]["text"] == "ArchitectureDiagramPayload"

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
        assert response.diagrams[0].adapter_payload["elements"][1]["text"] == "ArchitectureDiagramPayload"


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
                        "participants": ["entity-architecture-repository", "entity-diagram-payload"],
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
async def test_update_with_structured_warning_requires_explicit_acknowledgement(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)
        reset_architecture_observability_for_tests()
        original_version = design.version

        warning_entities = [
            *design.entities,
            {
                "id": "entity-unmapped-worker",
                "name": "Unmapped Worker",
                "entity_type": "worker",
                "responsibility": "Intentionally missing from the diagram.",
            },
            {
                "id": "entity-unmapped-queue",
                "name": "Unmapped Queue",
                "entity_type": "queue",
                "responsibility": "Intentionally missing from the diagram.",
            },
        ]
        with pytest.raises(ArchitectureWarningAcknowledgementRequired) as exc_info:
            await repo.update(
                design.id,
                ArchitectureDesignUpdate(
                    entities=warning_entities,
                    change_summary="Add unmapped worker",
                ),
                USER_ID,
            )

        assert exc_info.value.reason == "architecture_warning_acknowledgement_required"
        assert exc_info.value.warning_keys
        assert exc_info.value.warnings[0]["code"] == "entity_without_diagram"
        loaded = await repo.get(design.id)
        assert loaded is not None
        assert loaded.version == original_version

        runs = (
            await db.execute(
                select(ArchitectureFindingRun)
                .where(ArchitectureFindingRun.design_id == design.id)
                .order_by(ArchitectureFindingRun.design_version)
            )
        ).scalars().all()
        assert [run.design_version for run in runs] == [1]
        assert runs[0].active_count == 0
        samples = get_architecture_metric_samples()
        assert [
            sample for sample in samples
            if sample["metric_name"] == METRIC_WARNING_ACK_TOTAL
            and sample["labels"]["outcome"] == "required_without_ack"
        ]
        assert not [
            sample for sample in samples
            if sample["metric_name"] == METRIC_FINDING_RUN_PERSIST_TOTAL
        ]
        for sample in samples:
            assert_architecture_metric_payload_is_safe(sample["labels"])


@pytest.mark.asyncio
async def test_update_with_acknowledged_structured_warning_persists_audit_only_ack(db_factory):
    _, ideation_id = await _seed_ideation(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("ideation", ideation_id, _architecture_payload(), USER_ID)
        reset_architecture_observability_for_tests()
        warning_entities = [
            *design.entities,
            {
                "id": "entity-unmapped-worker",
                "name": "Unmapped Worker",
                "entity_type": "worker",
                "responsibility": "Intentionally missing from the diagram.",
            },
            {
                "id": "entity-unmapped-queue",
                "name": "Unmapped Queue",
                "entity_type": "queue",
                "responsibility": "Intentionally missing from the diagram.",
            },
        ]

        updated = await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                entities=warning_entities,
                change_summary="Add acknowledged unmapped worker",
                architecture_warning_acknowledgement={
                    "accepted": True,
                    "statement": "Reviewed warning before save.",
                },
            ),
            USER_ID,
        )
        await db.commit()
        samples = get_architecture_metric_samples()
        assert [
            sample for sample in samples
            if sample["metric_name"] == METRIC_WARNING_ACK_TOTAL
            and sample["labels"]["outcome"] == "accepted_with_ack"
        ]
        assert [
            sample for sample in samples
            if sample["metric_name"] == METRIC_FINDING_RUN_PERSIST_TOTAL
            and sample["labels"]["outcome"] == "persisted"
            and sample["labels"]["warning_count_bucket"] == "2_5"
        ]
        for sample in samples:
            assert_architecture_metric_payload_is_safe(sample["labels"])

    async with db_factory() as db:
        runs = (
            await db.execute(
                select(ArchitectureFindingRun)
                .where(ArchitectureFindingRun.design_id == updated.id)
                .order_by(ArchitectureFindingRun.design_version)
            )
        ).scalars().all()
        assert [run.design_version for run in runs] == [1, 2]
        assert runs[-1].is_current is True
        assert runs[-1].active_count == 2
        assert runs[-1].critic_run_id.startswith(f"archcrit:{updated.id}:v2:")

        findings = (
            await db.execute(
                select(ArchitectureFinding)
                .where(
                    ArchitectureFinding.design_id == updated.id,
                    ArchitectureFinding.critic_run_id == runs[-1].critic_run_id,
                )
                .order_by(ArchitectureFinding.target_ref)
            )
        ).scalars().all()
        assert [finding.warning_code for finding in findings] == [
            "entity_without_diagram",
            "entity_without_diagram",
        ]
        assert [finding.target_ref for finding in findings] == [
            "entity-unmapped-queue",
            "entity-unmapped-worker",
        ]

        acknowledgements = (
            await db.execute(
                select(ArchitectureWarningAcknowledgement)
                .where(ArchitectureWarningAcknowledgement.design_id == updated.id)
                .order_by(ArchitectureWarningAcknowledgement.finding_key)
            )
        ).scalars().all()
        assert len(acknowledgements) == 2
        assert {ack.finding_key for ack in acknowledgements} == {finding.finding_key for finding in findings}
        for acknowledgement in acknowledgements:
            assert acknowledgement.critic_run_id == runs[-1].critic_run_id
            assert acknowledgement.design_version == 2
            assert acknowledgement.actor_type == "user"
            assert acknowledgement.actor_id == USER_ID
            assert acknowledgement.actor_name == USER_ID
            assert acknowledgement.statement == "Reviewed warning before save."
            assert acknowledgement.created_at is not None


@pytest.mark.asyncio
async def test_copy_warning_design_requires_copy_scoped_acknowledgement_and_finding_run(db_factory):
    _, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        source = await repo.create("spec", spec_id, _invalid_connectivity_justification_payload(), USER_ID)
        source_diagram_id = source.diagrams[0]["id"]
        source_payload_ref = source.diagrams[0]["adapter_payload_ref"]
        service = ArchitecturePropagationService(db, repository=repo)

        with pytest.raises(ArchitectureWarningAcknowledgementRequired) as exc_info:
            await service.copy_spec_to_card(spec_id, card_id, USER_ID)

        assert exc_info.value.reason == "architecture_warning_acknowledgement_required"
        assert await repo.list("card", card_id) == []

        copied = (
            await service.copy_spec_to_card(
                spec_id,
                card_id,
                USER_ID,
                architecture_warning_acknowledgement={
                    "accepted": True,
                    "statement": "Copied design warning reviewed independently.",
                },
            )
        )[0]
        await db.flush()

        assert copied.id != source.id
        assert copied.source_design_id == source.id
        assert copied.source_ref == f"architecture_design:{source.id}"
        assert copied.diagrams[0]["source_diagram_id"] == source_diagram_id
        assert copied.diagrams[0]["source_payload_ref"] == source_payload_ref
        assert copied.diagrams[0]["id"] != source_diagram_id
        assert copied.diagrams[0]["adapter_payload_ref"] != source_payload_ref

        runs = (
            await db.execute(
                select(ArchitectureFindingRun)
                .where(ArchitectureFindingRun.design_id.in_([source.id, copied.id]))
                .order_by(ArchitectureFindingRun.design_id)
            )
        ).scalars().all()
        by_design = {run.design_id: run for run in runs}
        assert by_design[source.id].active_count == 1
        assert by_design[copied.id].active_count == 1
        assert by_design[source.id].critic_run_id.startswith(f"archcrit:{source.id}:v1:")
        assert by_design[copied.id].critic_run_id.startswith(f"archcrit:{copied.id}:v1:")
        assert by_design[source.id].critic_run_id != by_design[copied.id].critic_run_id

        copied_finding = (
            await db.execute(
                select(ArchitectureFinding).where(
                    ArchitectureFinding.design_id == copied.id,
                    ArchitectureFinding.warning_code == "conceptual_justification_invalid",
                )
            )
        ).scalar_one()
        source_finding = (
            await db.execute(
                select(ArchitectureFinding).where(
                    ArchitectureFinding.design_id == source.id,
                    ArchitectureFinding.warning_code == "conceptual_justification_invalid",
                )
            )
        ).scalar_one()
        assert copied_finding.finding_key != source_finding.finding_key
        assert copied_finding.diagram_id == copied.diagrams[0]["id"]
        assert copied_finding.diagram_id != source_diagram_id

        acknowledgements = (
            await db.execute(
                select(ArchitectureWarningAcknowledgement)
                .where(ArchitectureWarningAcknowledgement.design_id.in_([source.id, copied.id]))
                .order_by(ArchitectureWarningAcknowledgement.design_id)
            )
        ).scalars().all()
        ack_by_design = {ack.design_id: ack for ack in acknowledgements}
        assert ack_by_design[source.id].statement == "Source acknowledgement is scoped only to the source design."
        assert ack_by_design[copied.id].statement == "Copied design warning reviewed independently."
        assert ack_by_design[source.id].finding_key != ack_by_design[copied.id].finding_key
        assert ack_by_design[source.id].critic_run_id == by_design[source.id].critic_run_id
        assert ack_by_design[copied.id].critic_run_id == by_design[copied.id].critic_run_id
        assert ack_by_design[source.id].design_version == source.version
        assert ack_by_design[copied.id].design_version == copied.version


@pytest.mark.asyncio
async def test_copy_preserves_connectivity_justifications_as_content_but_reevaluates_them(db_factory):
    _, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        source = await repo.create("spec", spec_id, _valid_connectivity_justification_payload(), USER_ID)
        service = ArchitecturePropagationService(db, repository=repo)

        copied = (await service.copy_spec_to_card(spec_id, card_id, USER_ID))[0]
        await db.flush()

        assert copied.diagrams[0]["connectivity_justifications"] == source.diagrams[0]["connectivity_justifications"]
        assert copied.diagrams[0]["source_diagram_id"] == source.diagrams[0]["id"]
        assert copied.diagrams[0]["id"] != source.diagrams[0]["id"]

        runs = (
            await db.execute(
                select(ArchitectureFindingRun)
                .where(ArchitectureFindingRun.design_id.in_([source.id, copied.id]))
                .order_by(ArchitectureFindingRun.design_id)
            )
        ).scalars().all()
        by_design = {run.design_id: run for run in runs}
        assert by_design[source.id].active_count == 0
        assert by_design[copied.id].active_count == 0
        assert by_design[source.id].validator_summary["suppressed_warnings_count"] == 1
        assert by_design[copied.id].validator_summary["suppressed_warnings_count"] == 1
        assert by_design[source.id].validator_summary["structured_warnings_count"] == 0
        assert by_design[copied.id].validator_summary["structured_warnings_count"] == 0
        assert by_design[copied.id].critic_run_id.startswith(f"archcrit:{copied.id}:v1:")

        copied_diagrams = copy.deepcopy(copied.diagrams)
        copied_diagrams[0]["connectivity_justifications"] = {
            "node-external-audit": "todo",
        }
        updated_copy = await repo.update(
            copied.id,
            ArchitectureDesignUpdate(
                diagrams=copied_diagrams,
                change_summary="Make copied justification invalid in copy scope",
                architecture_warning_acknowledgement={
                    "accepted": True,
                    "statement": "Copied design warning reviewed after copy-local re-evaluation.",
                },
            ),
            USER_ID,
        )
        await db.flush()

        current_runs = (
            await db.execute(
                select(ArchitectureFindingRun)
                .where(
                    ArchitectureFindingRun.design_id.in_([source.id, copied.id]),
                    ArchitectureFindingRun.is_current == True,  # noqa: E712
                )
                .order_by(ArchitectureFindingRun.design_id)
            )
        ).scalars().all()
        current_by_design = {run.design_id: run for run in current_runs}
        assert current_by_design[source.id].active_count == 0
        assert current_by_design[source.id].validator_summary["suppressed_warnings_count"] == 1
        assert current_by_design[copied.id].active_count >= 1
        assert current_by_design[copied.id].validator_summary["structured_warnings_count"] >= 1
        assert current_by_design[copied.id].validator_summary["suppressed_warnings_count"] == 0
        assert current_by_design[copied.id].critic_run_id.startswith(f"archcrit:{copied.id}:v{updated_copy.version}:")

        copied_active_findings = (
            await db.execute(
                select(ArchitectureFinding).where(
                    ArchitectureFinding.design_id == copied.id,
                    ArchitectureFinding.lifecycle == "active",
                )
            )
        ).scalars().all()
        assert copied_active_findings
        assert {finding.critic_run_id for finding in copied_active_findings} == {
            current_by_design[copied.id].critic_run_id
        }
        assert {finding.design_version for finding in copied_active_findings} == {updated_copy.version}
        source_active_findings = (
            await db.execute(
                select(ArchitectureFinding).where(
                    ArchitectureFinding.design_id == source.id,
                    ArchitectureFinding.lifecycle == "active",
                )
            )
        ).scalars().all()
        assert source_active_findings == []


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
