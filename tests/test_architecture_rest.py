"""REST tests for Architecture Design endpoints."""

from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.architecture import router as architecture_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.db import Board, Card, CardStatus, CardType, Ideation, Spec, SpecStatus


USER_ID = "architecture-rest-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _architecture_body(title: str = "Runtime Architecture") -> dict:
    return {
        "title": title,
        "global_description": "Architecture is edited by UI and API.",
        "entities": [
            {
                "id": "entity-api",
                "name": "Architecture API",
                "entity_type": "service",
                "responsibility": "Expose Architecture Design operations.",
            }
        ],
        "interfaces": [
            {
                "id": "interface-payload",
                "name": "Diagram payload",
                "endpoint": "PUT /architecture/{design_id}/diagrams/{diagram_id}/payload",
                "protocol": "REST",
                "contract_type": "request_response",
                "request_schema": {"payload": "object"},
            }
        ],
        "diagrams": [
            {
                "id": "diagram-main",
                "title": "Main diagram",
                "diagram_type": "context",
                "format": "excalidraw_json",
                "adapter_payload": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [{"id": "shape-1", "type": "rectangle"}],
                    "appState": {},
                    "files": {},
                },
            }
        ],
    }


@pytest_asyncio.fixture
async def _client_and_entities():
    db_factory = get_session_factory()
    board_id = _id("architecture-rest-board")
    ideation_id = _id("architecture-rest-ideation")
    spec_id = _id("architecture-rest-spec")
    card_id = _id("architecture-rest-card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture REST Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture REST Ideation",
                created_by=USER_ID,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Architecture REST Spec",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR"],
                acceptance_criteria=["AC"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Architecture REST Card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(architecture_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    return TestClient(app), {
        "board_id": board_id,
        "ideation_id": ideation_id,
        "spec_id": spec_id,
        "card_id": card_id,
    }


def test_rest_crud_payload_import_and_diff(_client_and_entities):
    client, ids = _client_and_entities

    created = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=_architecture_body(),
    )
    assert created.status_code == 201, created.text
    design = created.json()
    assert design["parent_type"] == "ideation"
    assert design["diagrams"][0]["adapter_payload"] is None
    assert design["diagrams"][0]["adapter_payload_ref"]

    listing = client.get(f"/api/v1/ideations/{ids['ideation_id']}/architecture")
    assert listing.status_code == 200
    assert listing.json()[0]["diagrams_count"] == 1

    loaded = client.get(f"/api/v1/architecture/{design['id']}?include_payloads=true")
    assert loaded.status_code == 200
    assert loaded.json()["diagrams"][0]["adapter_payload"]["elements"][0]["id"] == "shape-1"

    payload = client.get(f"/api/v1/architecture/{design['id']}/diagrams/diagram-main/payload")
    assert payload.status_code == 200
    assert payload.json()["payload"]["elements"][0]["id"] == "shape-1"

    imported = client.post(
        f"/api/v1/architecture/{design['id']}/diagrams/import-excalidraw",
        json={
            "title": "Imported sequence",
            "diagram_type": "sequence",
            "payload": {
                "type": "excalidraw",
                "version": 2,
                "elements": [{"id": "shape-2", "type": "text", "text": "Imported"}],
                "appState": {},
                "files": {},
            },
        },
    )
    assert imported.status_code == 200, imported.text
    assert len(imported.json()["diagrams"]) == 2

    patched = client.patch(
        f"/api/v1/architecture/{design['id']}",
        json={
            "interfaces": [
                {
                    "id": "interface-payload",
                    "name": "Diagram payload",
                    "endpoint": "PUT /architecture/{design_id}/diagrams/{diagram_id}/payload",
                    "protocol": "REST",
                    "contract_type": "request_response",
                    "request_schema": {"payload": "object", "diagram_id": "string"},
                }
            ],
            "change_summary": "Contract schema changed",
        },
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["version"] == 3
    assert patched.json()["breaking_change_flag"] is False
    assert patched.json()["requires_arch_review"] is False

    diff = client.get(f"/api/v1/architecture/{design['id']}/diff?from_version=2&to_version=3")
    assert diff.status_code == 200
    assert "interfaces" in diff.json()["changed_fields"]
    assert diff.json()["breaking_change_flag"] is False


def test_rest_validate_architecture_payload_returns_warnings_without_persisting(_client_and_entities):
    client, _ = _client_and_entities
    payload = _architecture_body()
    payload["entities"][0].pop("responsibility")

    critique = client.post("/api/v1/architecture/validate", json=payload)

    assert critique.status_code == 200, critique.text
    body = critique.json()
    assert body["valid"] is True
    assert body["issues"] == []
    assert any("entities[0].responsibility" in item for item in body["warnings"])


def test_copy_architecture_from_spec_to_card_does_not_mark_stale(_client_and_entities):
    client, ids = _client_and_entities
    created = client.post(
        f"/api/v1/specs/{ids['spec_id']}/architecture",
        json=_architecture_body("Spec Architecture"),
    )
    assert created.status_code == 201, created.text
    source = created.json()

    copied = client.post(
        f"/api/v1/cards/{ids['card_id']}/copy-architecture-from-spec/{ids['spec_id']}",
        json={},
    )
    assert copied.status_code == 200, copied.text
    card_design = copied.json()[0]
    assert card_design["parent_type"] == "card"
    assert card_design["source_design_id"] == source["id"]
    assert card_design["source_version"] == 1
    assert card_design["diagrams"][0]["adapter_payload_ref"] != source["diagrams"][0]["adapter_payload_ref"]

    changed = client.patch(
        f"/api/v1/architecture/{source['id']}",
        json={"global_description": "Spec architecture changed upstream."},
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["version"] == 2

    card_architecture = client.get(f"/api/v1/cards/{ids['card_id']}/architecture")
    assert card_architecture.status_code == 200
    assert card_architecture.json()[0]["stale"] is False

    resynced = client.post(
        f"/api/v1/cards/{ids['card_id']}/copy-architecture-from-spec/{ids['spec_id']}",
        json={},
    )
    assert resynced.status_code == 200, resynced.text
    assert resynced.json()[0]["source_version"] == 2
    assert resynced.json()[0]["stale"] is False


def test_spec_lock_blocks_architecture_mutations(_client_and_entities):
    client, ids = _client_and_entities

    created = client.post(
        f"/api/v1/specs/{ids['spec_id']}/architecture",
        json=_architecture_body("Lockable Architecture"),
    )
    assert created.status_code == 201, created.text
    design_id = created.json()["id"]

    db_factory = get_session_factory()

    async def _lock_spec():
        async with db_factory() as db:
            spec = await db.get(Spec, ids["spec_id"])
            spec.validations = [{"id": "val-success", "outcome": "success"}]
            spec.current_validation_id = "val-success"
            await db.commit()

    import anyio

    anyio.run(_lock_spec)

    patched = client.patch(
        f"/api/v1/architecture/{design_id}",
        json={"global_description": "Locked specs cannot be edited."},
    )
    assert patched.status_code == 409

    another = client.post(
        f"/api/v1/specs/{ids['spec_id']}/architecture",
        json=_architecture_body("Blocked Architecture"),
    )
    assert another.status_code == 409


def test_rest_rejects_invalid_architecture_payload_with_context(_client_and_entities):
    client, ids = _client_and_entities
    body = _architecture_body("Invalid Architecture")
    body["entities"] = [
        {
            "id": "entity-api",
            "name": "API",
            "entity_type": "api",
        }
    ]
    body["interfaces"] = [
        {
            "id": "interface-invalid",
            "name": "Invalid contract",
            "participants": ["entity-api", "entity-missing"],
            "direction": "both ways",
        }
    ]

    created = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=body,
    )

    assert created.status_code == 422
    detail = created.json()["detail"]
    assert "entities[0].name duplicates entity_type" in detail
    assert "interfaces[0].direction='both ways' is invalid" in detail
    assert "interfaces[0].participants[1]" in detail
