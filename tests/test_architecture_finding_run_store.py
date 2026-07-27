"""Architecture finding run persistence tests."""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import ArchitectureDesign, Board, Ideation
from okto_pulse.core.services.architecture import (
    ARCHITECTURE_FINDING_ACTIVE,
    ARCHITECTURE_FINDING_RESOLVED,
    ARCHITECTURE_FINDING_SUPERSEDED,
    ArchitectureFindingKeyCollision,
    ArchitectureFindingRunStore,
    ArchitectureInvalidDesignNotPersisted,
    architecture_warning_target,
    stable_architecture_finding_key,
)


USER_ID = "architecture-finding-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_design(db_factory) -> tuple[str, str]:
    board_id = _id("finding-board")
    ideation_id = _id("finding-ideation")
    design_id = _id("finding-design")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture Finding Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture Finding Ideation",
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=design_id,
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Architecture Finding Design",
                global_description="Stores finding runs and lifecycle.",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, design_id


def _warning(
    code: str,
    *,
    diagram_id: str = "runtime",
    element_id: str | None = None,
    entity_id: str | None = None,
    node_ref: str | None = None,
    path: str = "diagrams[0].adapter_payload.elements[0]",
) -> dict:
    warning = {
        "code": code,
        "severity": "warning",
        "message": f"{code} message",
        "path": path,
        "suggested_fix": f"fix {code}",
        "diagram_id": diagram_id,
        "diagram_type": "runtime",
    }
    if element_id is not None:
        warning["element_id"] = element_id
    if entity_id is not None:
        warning["entity_id"] = entity_id
    if node_ref is not None:
        warning["node_ref"] = node_ref
    return warning


def test_architecture_warning_target_priority_and_stable_key() -> None:
    warning = _warning(
        "isolated_entity_node",
        element_id="node-a",
        entity_id="entity-a",
        node_ref="legacy-node",
    )

    kind, target_ref, path = architecture_warning_target(warning)

    assert kind == "element"
    assert target_ref == "node-a"
    assert path == "diagrams[0].adapter_payload.elements[0]"

    first = stable_architecture_finding_key("design-1", warning)
    second = stable_architecture_finding_key("design-1", dict(reversed(list(warning.items()))))
    assert first == second


@pytest.mark.asyncio
async def test_finding_run_store_computes_active_resolved_and_superseded_lifecycle(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    actor = {"actor_type": "agent", "actor_id": "agent-a", "actor_name": "Agent A"}
    warning_a = _warning("isolated_entity_node", element_id="node-a")
    warning_b = _warning("entity_without_diagram", entity_id="entity-b", path="entities[1]")
    warning_c = _warning("dangling_connector", node_ref="edge-c", path="diagrams[0].adapter_payload.elements[2]")

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        first = await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=1,
            critic_run_id="critic-run-1",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[warning_a, warning_b],
        )
        await db.commit()

        assert first["active_count"] == 2
        assert first["resolved_count"] == 0
        assert first["superseded_count"] == 0

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        second = await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=2,
            critic_run_id="critic-run-2",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[warning_a, warning_c],
        )
        await db.commit()

        assert second["active_count"] == 2
        assert second["resolved_count"] == 1
        assert second["superseded_count"] == 1

        findings = await store.list_findings(design_id=design_id)
        by_lifecycle = {}
        for finding in findings:
            by_lifecycle.setdefault(finding.lifecycle, []).append(finding.warning_code)

        assert sorted(by_lifecycle[ARCHITECTURE_FINDING_ACTIVE]) == [
            "dangling_connector",
            "isolated_entity_node",
        ]
        assert by_lifecycle[ARCHITECTURE_FINDING_RESOLVED] == ["entity_without_diagram"]
        assert by_lifecycle[ARCHITECTURE_FINDING_SUPERSEDED] == ["isolated_entity_node"]
        assert (await store.get_current_run(design_id)).critic_run_id == "critic-run-2"


@pytest.mark.asyncio
async def test_invalid_design_run_is_not_persisted_and_previous_success_remains_current(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    actor = {"actor_type": "user", "actor_id": USER_ID, "actor_name": "Test User"}
    warning = _warning("isolated_entity_node", element_id="node-a")

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=1,
            critic_run_id="valid-run",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[warning],
        )
        await db.commit()

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        with pytest.raises(ArchitectureInvalidDesignNotPersisted):
            await store.upsert_latest_run(
                board_id=board_id,
                design_id=design_id,
                design_version=2,
                critic_run_id="invalid-run",
                actor=actor,
                validator_summary={"valid": False, "issues": ["title is required"]},
                structured_warnings=[],
            )
        await db.rollback()

        current = await store.get_current_run(design_id)
        assert current.critic_run_id == "valid-run"
        assert [f.lifecycle for f in await store.list_findings(design_id=design_id)] == [
            ARCHITECTURE_FINDING_ACTIVE
        ]


@pytest.mark.asyncio
async def test_conflicting_duplicate_key_in_same_run_is_rejected(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    actor = {"actor_type": "agent", "actor_id": "agent-a", "actor_name": "Agent A"}
    warning = _warning("isolated_entity_node", element_id="node-a")
    conflicting = dict(warning)
    conflicting["message"] = "different payload for same key"

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        with pytest.raises(ArchitectureFindingKeyCollision):
            await store.upsert_latest_run(
                board_id=board_id,
                design_id=design_id,
                design_version=1,
                critic_run_id="collision-run",
                actor=actor,
                validator_summary={"valid": True, "issues": []},
                structured_warnings=[warning, conflicting],
            )


@pytest.mark.asyncio
async def test_acknowledgement_records_are_audit_only_and_do_not_clear_active_finding(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    actor = {"actor_type": "user", "actor_id": USER_ID, "actor_name": "Test User"}
    warning = _warning("isolated_entity_node", element_id="node-a")

    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        result = await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=1,
            critic_run_id="ack-run",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[warning],
        )
        finding_key = result["findings"][0]["finding_key"]

        acknowledgements = await store.record_acknowledgements(
            board_id=board_id,
            design_id=design_id,
            critic_run_id="ack-run",
            finding_keys=[finding_key],
            actor=actor,
            statement="Reviewed during save.",
        )
        await db.commit()

        assert len(acknowledgements) == 1
        assert acknowledgements[0].design_version == 1
        assert acknowledgements[0].actor_type == "user"
        active = await store.list_findings(design_id=design_id, lifecycle=ARCHITECTURE_FINDING_ACTIVE)
        assert [finding.finding_key for finding in active] == [finding_key]
