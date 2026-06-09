"""MCP tests for Architecture Design tools and task context."""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    ArchitectureDesign,
    ArchitectureFindingRun,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.architecture import ArchitectureFindingRunStore


USER_ID = "architecture-mcp-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "architecture-mcp-agent",
            "board_id": board_id,
            "permissions": ["board:read", "cards:update", "specs:update"],
        },
    )()


def _stub_ctx_with_permissions(board_id: str, permissions):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "architecture-mcp-agent",
            "board_id": board_id,
            "permissions": permissions,
        },
    )()


def _architecture_diagrams() -> list[dict]:
    return [
        {
            "id": "diagram-mcp",
            "title": "MCP diagram",
            "diagram_type": "context",
            "format": "excalidraw_json",
            "adapter_payload": {
                "type": "excalidraw",
                "version": 2,
                "elements": [{"id": "mcp-shape", "type": "rectangle"}],
                "appState": {},
                "files": {},
            },
        }
    ]


def _mermaid_diagrams() -> list[dict]:
    return [
        {
            "id": "diagram-mermaid",
            "title": "Mermaid context",
            "diagram_type": "context",
            "format": "mermaid",
            "adapter_payload": "graph TD\n  UI --> API",
        }
    ]


def _topology_entities() -> list[dict]:
    return [
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
    ]


def _topology_diagrams() -> list[dict]:
    return [
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
    ]


@pytest_asyncio.fixture
async def _seed_spec_card():
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    board_id = _id("architecture-mcp-board")
    spec_id = _id("architecture-mcp-spec")
    card_id = _id("architecture-mcp-card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture MCP Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Architecture MCP Spec",
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
                title="Architecture MCP Card",
                status=CardStatus.STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest_asyncio.fixture(autouse=True)
async def _stub_auth(_seed_spec_card):
    board_id, _, _ = _seed_spec_card
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.mark.asyncio
async def test_mcp_add_list_get_import_and_dump_architecture(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Spec Architecture",
        global_description="Architecture exposed through MCP.",
        diagrams=json.dumps(_architecture_diagrams()),
    )
    assert created.get("success") is True, created
    design_id = created["architecture_design"]["id"]

    listed = await _call(
        "okto_pulse_list_architecture_designs",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
    )
    assert listed.get("success") is True
    assert listed["architecture_designs"][0]["diagrams_count"] == 1

    loaded = await _call(
        "okto_pulse_get_architecture_design",
        board_id=board_id,
        design_id=design_id,
        include_payloads="true",
    )
    assert loaded.get("success") is True
    assert loaded["architecture_design"]["diagrams"][0]["adapter_payload"]["elements"][0]["id"] == "mcp-shape"

    imported = await _call(
        "okto_pulse_import_excalidraw_architecture_diagram",
        board_id=board_id,
        design_id=design_id,
        title="Imported via MCP",
        payload_json=json.dumps(
            {
                "type": "excalidraw",
                "version": 2,
                "elements": [{"id": "imported-shape", "type": "text", "text": "MCP"}],
                "appState": {},
                "files": {},
            }
        ),
    )
    assert imported.get("success") is True
    assert len(imported["architecture_design"]["diagrams"]) == 2

    dumped = await _call(
        "okto_pulse_dump_architecture_diagram",
        board_id=board_id,
        design_id=design_id,
        diagram_id="diagram-mcp",
    )
    assert dumped.get("success") is True
    assert "mcp-shape" in dumped["dump"]


@pytest.mark.asyncio
async def test_mcp_rejects_invalid_architecture_payload_with_context(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Invalid Spec Architecture",
        global_description="This payload should be critiqued before persistence.",
        entities=json.dumps(
            [
                {
                    "id": "entity-api",
                    "name": "API",
                    "entity_type": "api",
                }
            ]
        ),
        interfaces=json.dumps(
            [
                {
                    "id": "interface-invalid",
                    "name": "Invalid interface",
                    "participants": ["entity-api", "entity-missing"],
                    "direction": "both ways",
                }
            ]
        ),
    )

    assert "error" in created
    assert "entities[0].name duplicates entity_type" in created["error"]
    assert "interfaces[0].participants[1]" in created["error"]


@pytest.mark.asyncio
async def test_mcp_save_requires_acknowledgement_for_structured_topology_warnings(_seed_spec_card):
    from okto_pulse.core.infra.database import get_session_factory

    board_id, spec_id, _ = _seed_spec_card
    warning_diagrams = _topology_diagrams()
    warning_diagrams[0]["diagram_type"] = "context"

    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Warning-bearing MCP Architecture",
        global_description="MCP create must reject warning-bearing saves without ack.",
        entities=json.dumps(_topology_entities()),
        interfaces=json.dumps(
            [
                {
                    "id": "interface-web-api",
                    "name": "Call Pulse API",
                    "participants": ["entity-web", "entity-api"],
                    "direction": "source_to_target",
                    "endpoint": "GET /pulse",
                    "protocol": "REST",
                    "contract_type": "OpenAPI",
                    "request_schema": {"type": "object"},
                    "response_schema": {"type": "object"},
                }
            ]
        ),
        diagrams=json.dumps(warning_diagrams),
    )

    assert created.get("success") is False, created
    assert created["code"] == "architecture_warning_acknowledgement_required"
    assert created["warning_keys"]
    assert [item["code"] for item in created["structured_warnings"]] == ["isolated_entity_node"]

    db_factory = get_session_factory()
    async with db_factory() as db:
        runs = (await db.execute(select(ArchitectureFindingRun).where(ArchitectureFindingRun.board_id == board_id))).scalars().all()
        assert runs == []


@pytest.mark.asyncio
async def test_rest_and_mcp_save_acknowledgement_required_payloads_match_semantically(_seed_spec_card):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.core.api.architecture import router as architecture_router
    from okto_pulse.core.infra import auth as _auth_mod
    from okto_pulse.core.infra.database import get_db, get_session_factory

    board_id, spec_id, _ = _seed_spec_card
    warning_diagrams = _topology_diagrams()
    warning_diagrams[0]["diagram_type"] = "context"
    interfaces = [
        {
            "id": "interface-web-api",
            "name": "Call Pulse API",
            "participants": ["entity-web", "entity-api"],
            "direction": "source_to_target",
            "endpoint": "GET /pulse",
            "protocol": "REST",
            "contract_type": "OpenAPI",
            "request_schema": {"type": "object"},
            "response_schema": {"type": "object"},
        }
    ]
    payload = {
        "title": "Warning-bearing Architecture",
        "global_description": "REST and MCP must surface the same warning remediation payload.",
        "entities": _topology_entities(),
        "interfaces": interfaces,
        "diagrams": warning_diagrams,
    }

    db_factory = get_session_factory()
    app = FastAPI()
    app.include_router(architecture_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    rest_response = TestClient(app).post(f"/api/v1/specs/{spec_id}/architecture", json=payload)
    assert rest_response.status_code == 409, rest_response.text
    rest_detail = rest_response.json()["detail"]

    mcp_payload = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title=payload["title"],
        global_description=payload["global_description"],
        entities=json.dumps(payload["entities"]),
        interfaces=json.dumps(interfaces),
        diagrams=json.dumps(warning_diagrams),
    )

    def _warning_semantics(warning: dict) -> dict:
        return {
            key: warning.get(key)
            for key in (
                "code",
                "severity",
                "message",
                "suggested_fix",
                "diagram_id",
                "diagram_type",
                "element_id",
                "entity_id",
                "normalized_target_kind",
                "target_ref",
                "path",
            )
        }

    assert mcp_payload.get("success") is False, mcp_payload
    assert rest_detail["code"] == mcp_payload["code"]
    assert rest_detail["message"] == mcp_payload["message"]
    assert len(rest_detail["warning_keys"]) == len(mcp_payload["warning_keys"]) == 1
    assert [_warning_semantics(item) for item in rest_detail["structured_warnings"]] == [
        _warning_semantics(item) for item in mcp_payload["structured_warnings"]
    ]
    assert rest_detail["code"] == "architecture_warning_acknowledgement_required"
    assert [item["code"] for item in rest_detail["structured_warnings"]] == ["isolated_entity_node"]
    assert rest_detail["structured_warnings"][0]["suggested_fix"].startswith("Connect 'Audit Sink'")


@pytest.mark.asyncio
async def test_mcp_add_architecture_rejects_non_excalidraw_diagram_format(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Mermaid Architecture",
        global_description="Mermaid text belongs in entity descriptions, not diagram format.",
        diagrams=json.dumps(_mermaid_diagrams()),
    )

    assert "error" in created
    assert "diagrams[0].format='mermaid' is unsupported" in created["error"]
    assert "format='excalidraw_json'" in created["error"]


@pytest.mark.asyncio
async def test_mcp_get_architecture_schema_exposes_authoring_contract(_seed_spec_card):
    board_id, _, _ = _seed_spec_card

    schema_resp = await _call("okto_pulse_get_architecture_design_schema", board_id=board_id)

    assert schema_resp.get("success") is True, schema_resp
    schema = schema_resp["schema"]
    assert schema["allowed_values"]["diagram.format"] == ["excalidraw_json"]
    assert schema["allowed_values"]["excalidraw.connectionType"] == ["direct", "elbow"]
    assert "Mermaid" in " ".join(schema["root_contract"]["rules"])
    assert "mcp_server" in schema["entity_type_examples"]
    assert schema["entity_contract"]["anti_patterns"]
    assert "endpoint" in schema["interface_contract"]["recommended"]
    assert "participants" not in schema["interface_contract"]["recommended"]
    assert "interfaces do not own source/target" in " ".join(schema["interface_contract"]["rules"])
    assert schema["interface_contract"]["anti_patterns"]
    assert "linkedInterfaceIds" in schema["excalidraw_adapter_payload_contract"]["edge_element"]
    assert schema["complete_minimal_payload_example"]["diagrams"][0]["format"] == "excalidraw_json"
    # Spec cc497a0d — semantic_node_registry must be exposed for MCP agents.
    registry_section = schema["semantic_node_registry"]
    assert "api" in registry_section["mappings"]
    assert "database" in registry_section["mappings"]
    assert set(registry_section["required_node_fields_for_linked"]) >= {
        "text", "displayType", "architectureKind", "iconName", "linkedEntityId",
    }
    assert "server" in registry_section["allowed_icon_names"]
    flow = registry_section["validation_flow_for_agents"]
    assert any("get_architecture_design_schema" in step for step in flow)
    assert any("validate" in step.lower() for step in flow)


@pytest.mark.asyncio
async def test_mcp_validate_architecture_payload_reports_issues_warnings_and_fixes(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    critique = await _call(
        "okto_pulse_validate_architecture_design_payload",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Invalid Architecture",
        global_description="Dry-run should return contextual feedback.",
        entities=json.dumps(
            [
                {
                    "id": "entity-api",
                    "name": "API",
                    "entity_type": "api",
                }
            ]
        ),
        interfaces=json.dumps(
            [
                {
                    "id": "interface-invalid",
                    "name": "Invalid interface",
                    "description": "Missing valid endpoint and direction.",
                    "participants": ["entity-api", "entity-missing"],
                    "direction": "both ways",
                }
            ]
        ),
        diagrams=json.dumps(
            [
                {
                    "id": "diagram-invalid",
                    "title": "Invalid diagram",
                    "diagram_type": "context",
                    "format": "excalidraw_json",
                    "adapter_payload": {
                        "type": "excalidraw",
                        "version": 2,
                        "elements": [
                            {"id": "node-api", "type": "rectangle", "linkedEntityId": "entity-api"},
                            {
                                "id": "edge-invalid",
                                "type": "arrow",
                                "sourceElementId": "node-api",
                                "targetElementId": "node-missing",
                                "connectionType": "curved",
                            },
                        ],
                        "appState": {},
                        "files": {},
                    },
                }
            ]
        ),
    )

    assert critique.get("success") is True, critique
    assert critique["valid"] is False
    joined_issues = "\n".join(critique["issues"])
    assert "entities[0].name duplicates entity_type" in joined_issues
    assert "interfaces[0].participants[1]" in joined_issues
    assert "interfaces[0].direction='both ways' is invalid" in joined_issues
    assert "connectionType='curved' is invalid" in joined_issues
    assert "targetElementId references 'node-missing'" in joined_issues
    assert any("responsibility" in item for item in critique["warnings"])
    assert any("elbow" in item for item in critique["suggested_fixes"])


@pytest.mark.asyncio
async def test_mcp_validate_architecture_payload_rejects_non_excalidraw_diagram_format(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card

    critique = await _call(
        "okto_pulse_validate_architecture_design_payload",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Mermaid Architecture",
        global_description="Mermaid text belongs in entity descriptions, not diagram format.",
        diagrams=json.dumps(_mermaid_diagrams()),
    )

    assert critique.get("success") is True, critique
    assert critique["valid"] is False
    joined_issues = "\n".join(critique["issues"])
    assert "diagrams[0].format='mermaid' is unsupported" in joined_issues
    assert "format='excalidraw_json'" in joined_issues
    assert any("Mermaid" in item and "diagram formats" in item for item in critique["suggested_fixes"])


@pytest.mark.asyncio
async def test_mcp_validate_architecture_payload_matches_backend_structured_warning_contract(_seed_spec_card):
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.architecture import ArchitectureDesignRepository

    board_id, spec_id, _ = _seed_spec_card
    entities = _topology_entities()
    diagrams = _topology_diagrams()

    critique = await _call(
        "okto_pulse_validate_architecture_design_payload",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Topology Contract",
        global_description="MCP and REST share the backend critic result.",
        entities=json.dumps(entities),
        interfaces=json.dumps([
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
        ]),
        diagrams=json.dumps(diagrams),
    )

    db_factory = get_session_factory()
    interfaces = [
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
    ]
    async with db_factory() as db:
        expected = ArchitectureDesignRepository(db).critique_payload(
            {
                "title": "Topology Contract",
                "global_description": "MCP and REST share the backend critic result.",
                "entities": entities,
                "interfaces": interfaces,
                "diagrams": diagrams,
            }
        )

    assert critique.get("success") is True, critique
    assert critique["valid"] is True
    assert critique["warnings"] == expected["warnings"]
    assert critique["structured_warnings"] == expected["structured_warnings"]
    assert critique["suppressed_warnings"] == expected["suppressed_warnings"]
    assert critique["summary"]["structured_warning_codes"] == ["isolated_entity_node"]
    assert critique["summary"] == expected["summary"]


@pytest.mark.asyncio
async def test_rest_and_mcp_validate_architecture_payload_return_identical_warning_contract(_seed_spec_card):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.core.api.architecture import router as architecture_router
    from okto_pulse.core.infra import auth as _auth_mod
    from okto_pulse.core.infra.database import get_db, get_session_factory

    board_id, spec_id, _ = _seed_spec_card
    entities = _topology_entities()
    diagrams = _topology_diagrams()
    interfaces = [
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
    ]
    payload = {
        "title": "Topology Contract",
        "global_description": "MCP and REST share the backend critic result.",
        "entities": entities,
        "interfaces": interfaces,
        "diagrams": diagrams,
    }

    db_factory = get_session_factory()
    app = FastAPI()
    app.include_router(architecture_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    rest_response = TestClient(app).post("/api/v1/architecture/validate", json=payload)
    rest_response.raise_for_status()
    rest = rest_response.json()

    mcp = await _call(
        "okto_pulse_validate_architecture_design_payload",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title=payload["title"],
        global_description=payload["global_description"],
        entities=json.dumps(entities),
        interfaces=json.dumps(interfaces),
        diagrams=json.dumps(diagrams),
    )

    assert mcp.get("success") is True, mcp
    comparable_keys = [
        "valid",
        "issues",
        "warnings",
        "structured_warnings",
        "suppressed_warnings",
        "suggested_fixes",
        "summary",
    ]
    assert {key: mcp[key] for key in comparable_keys} == {key: rest[key] for key in comparable_keys}
    assert rest["summary"]["structured_warning_codes"] == ["isolated_entity_node"]
    assert rest["summary"]["structured_warnings_count"] == 1


@pytest.mark.asyncio
async def test_mcp_validate_architecture_payload_accepts_complete_payload_without_persisting(_seed_spec_card):
    board_id, spec_id, _ = _seed_spec_card
    entities = [
        {
            "id": "entity-customer-portal",
            "name": "Customer Portal",
            "entity_type": "web_app",
            "responsibility": "Collects checkout input.",
            "boundaries": "Browser UI boundary.",
            "technologies": ["React"],
        },
        {
            "id": "entity-checkout-api",
            "name": "Checkout API",
            "entity_type": "api",
            "responsibility": "Validates checkout and creates orders.",
            "boundaries": "Backend API boundary.",
            "technologies": ["FastAPI"],
        },
    ]
    interfaces = [
        {
            "id": "interface-create-order",
            "name": "Create order",
            "description": "Customer Portal sends checkout data to Checkout API.",
            "participants": ["entity-customer-portal", "entity-checkout-api"],
            "direction": "source_to_target",
            "endpoint": "POST /orders",
            "protocol": "REST",
            "contract_type": "OpenAPI",
            "request_schema": {"type": "object", "required": ["cart_id"]},
            "response_schema": {"type": "object", "required": ["order_id"]},
            "error_contract": {"400": "Invalid checkout payload"},
        }
    ]
    diagrams = [
        {
            "id": "diagram-runtime",
            "title": "Runtime context",
            "diagram_type": "context",
            "format": "excalidraw_json",
            "adapter_payload": {
                "type": "excalidraw",
                "version": 2,
                "elements": [
                    {
                        "id": "node-customer-portal",
                        "type": "rectangle",
                        "linkedEntityId": "entity-customer-portal",
                    },
                    {
                        "id": "node-checkout-api",
                        "type": "rectangle",
                        "linkedEntityId": "entity-checkout-api",
                    },
                    {
                        "id": "edge-create-order",
                        "type": "arrow",
                        "sourceElementId": "node-customer-portal",
                        "targetElementId": "node-checkout-api",
                        "linkedInterfaceIds": ["interface-create-order"],
                        "connectionType": "elbow",
                    },
                ],
                "appState": {},
                "files": {},
            },
        }
    ]

    critique = await _call(
        "okto_pulse_validate_architecture_design_payload",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Checkout Architecture",
        global_description="Customer Portal calls Checkout API to create orders.",
        entities=json.dumps(entities),
        interfaces=json.dumps(interfaces),
        diagrams=json.dumps(diagrams),
    )
    listed = await _call(
        "okto_pulse_list_architecture_designs",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
    )

    assert critique.get("success") is True, critique
    assert critique["valid"] is True
    assert critique["issues"] == []
    assert critique["summary"]["entities_count"] == 2
    assert critique["summary"]["interfaces_count"] == 1
    assert critique["summary"]["linked_entity_elements_count"] == 2
    assert critique["summary"]["linked_interface_elements_count"] == 1
    assert listed.get("success") is True, listed
    assert listed["architecture_designs"] == []


@pytest.mark.asyncio
async def test_mcp_copy_architecture_to_card_and_task_context(_seed_spec_card):
    board_id, spec_id, card_id = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Spec Architecture",
        global_description="Architecture copied into card context.",
        diagrams=json.dumps(_architecture_diagrams()),
    )
    source_id = created["architecture_design"]["id"]

    # profile=full preserves the prior payload shape (back-compat, R2.3).
    copied = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
        profile="full",
    )
    assert copied.get("success") is True, copied
    assert copied["copied"] == 1
    assert copied["architecture_designs"][0]["source_design_id"] == source_id

    context = await _call(
        "okto_pulse_get_task_context",
        board_id=board_id,
        card_id=card_id,
        include_architecture="true",
    )
    assert "architecture_designs" in context["card"]
    assert context["card"]["architecture_designs"][0]["source_design_id"] == source_id
    assert context["spec"]["architecture_designs"][0]["id"] == source_id


@pytest.mark.asyncio
async def test_mcp_task_context_projects_architecture_findings_full_and_summary(_seed_spec_card):
    from okto_pulse.core.infra.database import get_session_factory

    board_id, _spec_id, card_id = _seed_spec_card
    db_factory = get_session_factory()
    async with db_factory() as db:
        designs = {}
        for slug in ("no-findings", "active", "resolved"):
            design = ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title=f"{slug} context architecture",
                global_description=f"{slug} context architecture body.",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
            db.add(design)
            await db.flush()
            designs[slug] = design

        store = ArchitectureFindingRunStore(db)
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["active"].id,
            design_version=designs["active"].version,
            critic_run_id="critic-context-active",
            actor={"actor_id": USER_ID, "actor_type": "agent", "actor_name": "MCP Agent"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[
                {
                    "code": "orphan_entity",
                    "severity": "warning",
                    "message": "Entity is not connected in any diagram.",
                    "suggested_fix": "Connect this entity.",
                    "diagram_id": "diag-context-active",
                    "element_id": "entity-context-active",
                    "path": "$.diagrams[0].elements[0]",
                }
            ],
        )
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["resolved"].id,
            design_version=designs["resolved"].version,
            critic_run_id="critic-context-resolved-before",
            actor={"actor_id": USER_ID, "actor_type": "agent", "actor_name": "MCP Agent"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[
                {
                    "code": "uncovered_interface",
                    "severity": "warning",
                    "message": "Interface is declared but not shown.",
                    "suggested_fix": "Show the interface.",
                    "diagram_id": "diag-context-resolved",
                    "entity_id": "iface-context-resolved",
                    "path": "$.interfaces[0]",
                }
            ],
        )
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["resolved"].id,
            design_version=designs["resolved"].version,
            critic_run_id="critic-context-resolved-after",
            actor={"actor_id": USER_ID, "actor_type": "agent", "actor_name": "MCP Agent"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[],
        )
        await db.commit()

    full_context = await _call(
        "okto_pulse_get_task_context",
        board_id=board_id,
        card_id=card_id,
        include_architecture="true",
        profile="full",
    )
    summary_context = await _call(
        "okto_pulse_get_task_context",
        board_id=board_id,
        card_id=card_id,
        include_architecture="true",
    )

    for context in (full_context, summary_context):
        findings = context["resource_gate_summary"]["architecture_findings"]
        assert findings["owner_type"] == "card"
        assert findings["owner_id"] == card_id
        assert findings["design_count"] == 3
        assert findings["active_count"] == 1
        assert findings["resolved_count"] == 1
        assert findings["by_code"] == {"orphan_entity": 1}
        by_design = {item["design_id"]: item for item in findings["by_design"]}
        assert by_design[designs["no-findings"].id]["active_count"] == 0
        assert by_design[designs["no-findings"].id]["resolved_count"] == 0
        assert by_design[designs["active"].id]["active_count"] == 1
        assert by_design[designs["resolved"].id]["resolved_count"] == 1
        assert findings["top_remediation"][0]["target_ref"] == "entity-context-active"
        assert findings["top_remediation"][0]["source_entity_type"] == "card"

    assert "projection" not in full_context
    assert summary_context["projection"]["profile"] == "summary"
    assert {
        "rel": "read_full_context",
        "target_ref": "okto_pulse_get_task_context",
    } in summary_context["projection"]["follow_up"]
    assert summary_context["card"]["architecture_designs"][0]["counts"]["has_global_description"] is True


@pytest.mark.asyncio
async def test_mcp_spec_lock_blocks_architecture_update(_seed_spec_card):
    from okto_pulse.core.infra.database import get_session_factory

    board_id, spec_id, _ = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Lockable Architecture",
        global_description="Architecture before validation.",
    )
    design_id = created["architecture_design"]["id"]

    db_factory = get_session_factory()
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.validations = [{"id": "val-success", "outcome": "success"}]
        spec.current_validation_id = "val-success"
        await db.commit()

    updated = await _call(
        "okto_pulse_update_architecture_design",
        board_id=board_id,
        design_id=design_id,
        global_description="Should be blocked.",
    )
    assert "error" in updated
    assert "locked" in updated["error"]


@pytest.mark.asyncio
async def test_mcp_architecture_tools_enforce_granular_permissions(_seed_spec_card):
    from okto_pulse.core.infra.permissions import _build_preset_flags, resolve_permissions

    board_id, spec_id, card_id = _seed_spec_card
    created = await _call(
        "okto_pulse_add_architecture_design",
        board_id=board_id,
        parent_type="spec",
        parent_id=spec_id,
        title="Permissioned Architecture",
        global_description="Architecture created with legacy-compatible permissions.",
    )
    assert created.get("success") is True, created

    read_only = resolve_permissions(
        None,
        _build_preset_flags(["board.read", "spec.architecture.read", "card.architecture.read"]),
        None,
    )
    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_stub_ctx_with_permissions(board_id, read_only)),
    ):
        updated = await _call(
            "okto_pulse_update_architecture_design",
            board_id=board_id,
            design_id=created["architecture_design"]["id"],
            global_description="This should be denied.",
        )
        copied = await _call(
            "okto_pulse_copy_architecture_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
        )

    assert "spec.architecture.edit" in updated["error"]
    assert "card.copy_from_spec.architecture" in copied["error"]
