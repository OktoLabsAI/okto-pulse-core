"""Spec R01A FU5-S1B — Architecture design CRUD + validation + propagation report on the UnitOfWork.

Five further ``api/architecture.py`` endpoints that drove off the request session —
``get`` / ``update`` / ``delete`` of a single Architecture Design, the dry-run
payload ``validate``, and the read-only ``propagation-legacy-report`` — now route
through ``GetArchitectureDesignUseCase`` / ``UpdateArchitectureDesignUseCase`` /
``DeleteArchitectureDesignUseCase`` / ``ValidateArchitecturePayloadUseCase`` /
``ArchitecturePropagationLegacyReportUseCase`` + ``get_unit_of_work``; each adapter
only maps the result/errors to HTTP.

Oracles exercise the migrated status codes + bodies (get 200 / 404 "Architecture
design not found", update 200 re-projected body / 404 / 409 card-read-only / 409
spec-locked, delete 204→404 / 404 / 409 card-read-only, validate 200 with field
warnings + structured topology warnings + ``design_id`` finding-key enrichment,
propagation report 200 empty), the use case raising ``EntityNotFoundError`` for a
missing design, and an AST signature check proving the five endpoints take ``uow``
(not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import architecture as architecture_api
from okto_pulse.core.api.architecture import router as architecture_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.services.architecture import CARD_ARCHITECTURE_READ_ONLY_MESSAGE

USER = "r01a-fu5-s1b-user"
PREFIX = "/api/v1"
SPEC_LOCKED_DETAIL = (
    "Spec is locked because validation passed. Move it back to draft or approved "
    "to edit architecture."
)
_ENDPOINTS = (
    "get_architecture_design",
    "update_architecture_design",
    "delete_architecture_design",
    "validate_architecture_payload",
    "architecture_propagation_legacy_report",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(architecture_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


def _architecture_body(title: str = "FU5-S1B Architecture") -> dict:
    """A payload that critiques clean (creates without acknowledgement)."""
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
            },
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
    """Mirror of the behavior-test fixture that yields one ``isolated_entity_node``
    structured warning (the audit sink is unconnected in the runtime diagram)."""
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
                        },
                        {
                            "id": "node-api",
                            "type": "rectangle",
                            "linkedEntityId": "entity-api",
                            "text": "Pulse API",
                        },
                        {
                            "id": "node-audit",
                            "type": "rectangle",
                            "linkedEntityId": "entity-audit",
                            "text": "Audit Sink",
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


async def _seed_parents() -> dict[str, str]:
    """Seed Board + one of each architecture parent (ideation/spec/card) via raw
    models. Unique per call so tests do not collide on the shared session factory."""
    from okto_pulse.core.models.db import Board, Card, Ideation, Spec

    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"board-fu5s1b-{suffix}",
        "ideation": f"ideation-fu5s1b-{suffix}",
        "spec": f"spec-fu5s1b-{suffix}",
        "card": f"card-fu5s1b-{suffix}",
    }
    async with get_session_factory()() as db:
        db.add(Board(id=ids["board"], name="fu5s1b", owner_id=USER))
        db.add(
            Ideation(
                id=ids["ideation"],
                board_id=ids["board"],
                title="fu5s1b-ideation",
                created_by=USER,
            )
        )
        db.add(
            Spec(
                id=ids["spec"],
                board_id=ids["board"],
                title="fu5s1b-spec",
                created_by=USER,
            )
        )
        db.add(
            Card(
                id=ids["card"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="fu5s1b-card",
                created_by=USER,
            )
        )
        await db.commit()
    return ids


async def _seed_design(parent_type: str, parent_id: str, title: str = "Seed") -> str:
    """Create one Architecture Design under ``parent_type`` via the existing
    repository (the same ``select``/ORM the legacy endpoints delegated to).
    ``allow_card_parent_write`` lets us seed a ``card`` design directly so the
    read-only gate can be exercised on update/delete."""
    from okto_pulse.core.services.architecture import ArchitectureDesignRepository

    async with get_session_factory()() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create(
            parent_type,
            parent_id,
            _architecture_body(title),
            USER,
            allow_card_parent_write=True,
        )
        design_id = design.id
        await db.commit()
    return design_id


async def _lock_spec(spec_id: str) -> None:
    """Mark a spec's current validation outcome as success (architecture locked)."""
    from okto_pulse.core.models.db import Spec

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        spec.validations = [{"id": "val-success", "outcome": "success"}]
        spec.current_validation_id = "val-success"
        await db.commit()


def _missing(kind: str = "design") -> str:
    return f"{kind}-missing-{uuid.uuid4().hex[:8]}"


# --- get --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_architecture_design_200(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.get(f"{PREFIX}/architecture/{design_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == design_id
    assert body["parent_type"] == "ideation"


@pytest.mark.asyncio
async def test_get_architecture_design_404(client) -> None:
    resp = client.get(f"{PREFIX}/architecture/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Architecture design not found"


# --- update -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_architecture_design_200(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.patch(
        f"{PREFIX}/architecture/{design_id}",
        json={"global_description": "Architecture changed via the migrated PATCH."},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == design_id
    assert body["global_description"] == "Architecture changed via the migrated PATCH."


@pytest.mark.asyncio
async def test_update_architecture_design_404(client) -> None:
    resp = client.patch(
        f"{PREFIX}/architecture/{_missing()}",
        json={"global_description": "nope"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Architecture design not found"


@pytest.mark.asyncio
async def test_update_architecture_design_409_card_read_only(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("card", ids["card"])
    resp = client.patch(
        f"{PREFIX}/architecture/{design_id}",
        json={"global_description": "card designs are read-only"},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == CARD_ARCHITECTURE_READ_ONLY_MESSAGE


@pytest.mark.asyncio
async def test_update_architecture_design_409_spec_locked(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("spec", ids["spec"])
    await _lock_spec(ids["spec"])
    resp = client.patch(
        f"{PREFIX}/architecture/{design_id}",
        json={"global_description": "locked specs cannot be edited"},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == SPEC_LOCKED_DETAIL


# --- delete -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_architecture_design_204_then_404(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.delete(f"{PREFIX}/architecture/{design_id}")
    assert resp.status_code == 204, resp.text
    assert client.get(f"{PREFIX}/architecture/{design_id}").status_code == 404
    gone = client.delete(f"{PREFIX}/architecture/{design_id}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Architecture design not found"


@pytest.mark.asyncio
async def test_delete_architecture_design_404(client) -> None:
    resp = client.delete(f"{PREFIX}/architecture/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Architecture design not found"


@pytest.mark.asyncio
async def test_delete_architecture_design_409_card_read_only(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("card", ids["card"])
    resp = client.delete(f"{PREFIX}/architecture/{design_id}")
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == CARD_ARCHITECTURE_READ_ONLY_MESSAGE


# --- validate (dry-run, no persistence) -------------------------------------


def test_validate_architecture_payload_200_field_warnings(client) -> None:
    payload = _architecture_body()
    payload["entities"][0].pop("responsibility")
    resp = client.post(f"{PREFIX}/architecture/validate", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valid"] is True
    assert body["issues"] == []
    assert any("entities[0].responsibility" in item for item in body["warnings"])


def test_validate_architecture_payload_200_structured_topology(client) -> None:
    resp = client.post(f"{PREFIX}/architecture/validate", json=_topology_warning_body())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valid"] is True
    assert [item["code"] for item in body["structured_warnings"]] == ["isolated_entity_node"]
    # No design_id supplied → no finding_key enrichment.
    assert "finding_key" not in body["structured_warnings"][0]


def test_validate_architecture_payload_200_design_id_enriches_finding_key(client) -> None:
    payload = _topology_warning_body()
    payload["design_id"] = "design-fu5s1b-fixed"
    resp = client.post(f"{PREFIX}/architecture/validate", json=payload)
    assert resp.status_code == 200, resp.text
    structured = resp.json()["structured_warnings"]
    assert structured
    assert all(warning.get("finding_key") for warning in structured)


# --- propagation legacy report (read-only diagnostic) -----------------------


@pytest.mark.asyncio
async def test_propagation_legacy_report_200_empty(client) -> None:
    ids = await _seed_parents()
    resp = client.get(
        f"{PREFIX}/architecture/propagation-legacy-report",
        params={"board_id": ids["board"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["board_id"] == ids["board"]
    assert body["items"] == []
    assert body["scanned_total"] == 0
    assert body["mutation_performed"] is False


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_architecture_design_use_case_raises_for_missing_design() -> None:
    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(_missing()),
                actor=actor,
                uow=uow,
            )


def test_fu5_s1b_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(architecture_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
