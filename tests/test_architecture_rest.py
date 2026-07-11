"""REST tests for Architecture Design endpoints."""

from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

from okto_pulse.community.api.architecture import router as architecture_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import ArchitectureFindingRun, Board, Card, CardStatus, CardType, Ideation, Spec, SpecStatus


USER_ID = "architecture-rest-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _architecture_body(title: str = "Runtime Architecture") -> dict:
    return {
        "title": title,
        "global_description": "Architecture is edited by UI and API.",
        "entities": [
            {
                "id": "entity-client",
                "name": "Architecture UI",
                "entity_type": "web_app",
                "responsibility": "Lets users edit architecture diagrams.",
            },
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
                "participants": ["entity-client", "entity-api"],
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
                    "elements": [
                        {
                            "id": "node-client",
                            "type": "rectangle",
                            "linkedEntityId": "entity-client",
                            "text": "Architecture UI",
                        },
                        {
                            "id": "shape-1",
                            "type": "rectangle",
                            "linkedEntityId": "entity-api",
                            "text": "Architecture API",
                        },
                        {
                            "id": "edge-client-api",
                            "type": "arrow",
                            "sourceElementId": "node-client",
                            "targetElementId": "shape-1",
                            "linkedInterfaceId": "interface-payload",
                            "connectionType": "elbow",
                        },
                    ],
                    "appState": {},
                    "files": {},
                },
            }
        ],
    }


def _topology_warning_body() -> dict:
    return {
        "title": "Runtime topology warnings",
        "global_description": "Topology warning contract fixture.",
        "entities": [
            {
                "id": "entity-web",
                "name": "Customer Portal",
                "entity_type": "web_app",
                "responsibility": "Sends requests.",
                "boundaries": "Browser.",
            },
            {
                "id": "entity-api",
                "name": "Pulse API",
                "entity_type": "api",
                "responsibility": "Handles requests.",
                "boundaries": "Backend.",
            },
            {
                "id": "entity-audit",
                "name": "Audit Sink",
                "entity_type": "service",
                "responsibility": "Consumes audit records.",
                "boundaries": "Async sink.",
            },
        ],
        "interfaces": [
            {
                "id": "interface-web-api",
                "name": "Call Pulse API",
                "description": "Customer Portal calls Pulse API.",
                "participants": ["entity-web", "entity-api"],
                "direction": "source_to_target",
                "endpoint": "GET /pulse",
                "protocol": "REST",
                "contract_type": "OpenAPI",
                "request_schema": {"type": "object"},
                "response_schema": {"type": "object"},
            }
        ],
        "diagrams": [
            {
                "id": "diagram-runtime",
                "title": "Runtime",
                "diagram_type": "runtime",
                "format": "excalidraw_json",
                "adapter_payload": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [
                        {
                            "id": "node-web",
                            "type": "rectangle",
                            "linkedEntityId": "entity-web",
                            "text": "Customer Portal",
                            "displayType": "Web App",
                            "architectureKind": "frontend",
                            "iconName": "monitor",
                        },
                        {
                            "id": "node-api",
                            "type": "rectangle",
                            "linkedEntityId": "entity-api",
                            "text": "Pulse API",
                            "displayType": "API",
                            "architectureKind": "service",
                            "iconName": "server",
                        },
                        {
                            "id": "node-audit",
                            "type": "rectangle",
                            "linkedEntityId": "entity-audit",
                            "text": "Audit Sink",
                            "displayType": "Service",
                            "architectureKind": "service",
                            "iconName": "server",
                        },
                        {
                            "id": "edge-web-api",
                            "type": "arrow",
                            "sourceElementId": "node-web",
                            "targetElementId": "node-api",
                            "linkedInterfaceIds": ["interface-web-api"],
                            "connectionType": "elbow",
                        },
                    ],
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
    assert {
        element["id"]
        for element in loaded.json()["diagrams"][0]["adapter_payload"]["elements"]
    } >= {"node-client", "shape-1", "edge-client-api"}

    payload = client.get(f"/api/v1/architecture/{design['id']}/diagrams/diagram-main/payload")
    assert payload.status_code == 200
    assert {
        element["id"]
        for element in payload.json()["payload"]["elements"]
    } >= {"node-client", "shape-1", "edge-client-api"}

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
            "architecture_warning_acknowledgement": {
                "accepted": True,
                "statement": "Imported diagram warning reviewed in test fixture.",
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
                    "participants": ["entity-client", "entity-api"],
                    "request_schema": {"payload": "object", "diagram_id": "string"},
                }
            ],
            "architecture_warning_acknowledgement": {
                "accepted": True,
                "statement": "Existing imported diagram warning reviewed in test fixture.",
            },
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


def test_rest_validate_architecture_payload_returns_structured_topology_warnings(_client_and_entities):
    client, _ = _client_and_entities

    critique = client.post("/api/v1/architecture/validate", json=_topology_warning_body())

    assert critique.status_code == 200, critique.text
    body = critique.json()
    assert body["valid"] is True
    assert body["issues"] == []
    assert body["warnings"] == []
    assert body["suppressed_warnings"] == []
    assert [item["code"] for item in body["structured_warnings"]] == ["isolated_entity_node"]
    warning = body["structured_warnings"][0]
    assert warning["severity"] == "warning"
    assert warning["diagram_id"] == "diagram-runtime"
    assert warning["element_id"] == "node-audit"
    assert warning["suggested_fix"].startswith("Connect 'Audit Sink'")
    assert body["summary"]["structured_warnings_count"] == 1
    assert body["summary"]["structured_warning_codes"] == ["isolated_entity_node"]


def test_rest_save_requires_acknowledgement_for_structured_topology_warnings(_client_and_entities):
    client, ids = _client_and_entities
    warning_body = _topology_warning_body()
    warning_body["diagrams"][0]["diagram_type"] = "context"

    rejected = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=warning_body,
    )

    assert rejected.status_code == 409, rejected.text
    detail = rejected.json()["detail"]
    assert detail["code"] == "architecture_warning_acknowledgement_required"
    assert detail["warning_keys"]
    assert [item["code"] for item in detail["structured_warnings"]] == ["isolated_entity_node"]

    accepted_body = _topology_warning_body()
    accepted_body["diagrams"][0]["diagram_type"] = "context"
    accepted_body["architecture_warning_acknowledgement"] = {
        "accepted": True,
        "statement": "Reviewed isolated node before save.",
    }
    accepted = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=accepted_body,
    )
    assert accepted.status_code == 201, accepted.text


def test_rest_invalid_linked_entity_remains_blocking_issue_not_isolated_warning(_client_and_entities):
    client, _ = _client_and_entities
    payload = _topology_warning_body()
    payload["entities"] = [payload["entities"][0]]
    payload["diagrams"][0]["adapter_payload"]["elements"] = [
        {"id": "node-bad", "type": "rectangle", "linkedEntityId": "entity-missing", "text": "Missing"},
    ]

    critique = client.post("/api/v1/architecture/validate", json=payload)

    assert critique.status_code == 200, critique.text
    body = critique.json()
    assert body["valid"] is False
    assert any("linkedEntityId references 'entity-missing'" in issue for issue in body["issues"])
    assert "isolated_entity_node" not in [item["code"] for item in body["structured_warnings"]]


def test_rest_rejects_non_excalidraw_diagram_format(_client_and_entities):
    client, ids = _client_and_entities
    body = _architecture_body("Mermaid Architecture")
    body["diagrams"][0]["format"] = "mermaid"
    body["diagrams"][0]["adapter_payload"] = "graph TD\n  UI --> API"

    critique = client.post("/api/v1/architecture/validate", json=body)

    assert critique.status_code == 200, critique.text
    validation = critique.json()
    assert validation["valid"] is False
    assert "diagrams[0].format='mermaid' is unsupported" in "\n".join(validation["issues"])

    created = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=body,
    )

    assert created.status_code == 422
    assert "diagrams[0].format='mermaid' is unsupported" in created.json()["detail"]


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
    assert card_design["diagrams"][0]["content_hash"] == source["diagrams"][0]["content_hash"]

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


def test_rest_invalid_update_preserves_previous_successful_finding_run(_client_and_entities):
    client, ids = _client_and_entities
    created = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/architecture",
        json=_architecture_body("Valid Architecture Before Invalid Update"),
    )
    assert created.status_code == 201, created.text
    design_id = created.json()["id"]

    db_factory = get_session_factory()

    async def _current_run_signature():
        async with db_factory() as db:
            runs = (
                await db.execute(
                    select(ArchitectureFindingRun)
                    .where(ArchitectureFindingRun.design_id == design_id)
                    .order_by(ArchitectureFindingRun.design_version)
                )
            ).scalars().all()
            return [
                (
                    run.critic_run_id,
                    run.design_version,
                    run.active_count,
                    run.resolved_count,
                    run.superseded_count,
                )
                for run in runs
            ]

    import anyio

    before = anyio.run(_current_run_signature)
    assert len(before) == 1

    rejected = client.patch(
        f"/api/v1/architecture/{design_id}",
        json={
            "entities": [
                {
                    "id": "entity-api",
                    "name": "API",
                    "entity_type": "api",
                }
            ],
            "interfaces": [
                {
                    "id": "interface-invalid",
                    "name": "Invalid contract",
                    "participants": ["entity-api", "entity-missing"],
                    "direction": "both ways",
                }
            ],
            "change_summary": "Invalid update must not replace the current finding run.",
        },
    )

    assert rejected.status_code == 422
    detail = rejected.json()["detail"]
    assert "entities[0].name duplicates entity_type" in detail
    assert "interfaces[0].direction='both ways' is invalid" in detail
    assert "interfaces[0].participants[1]" in detail

    after = anyio.run(_current_run_signature)
    assert after == before
