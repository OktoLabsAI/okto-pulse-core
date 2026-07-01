"""Spec R01A FU5-S1C — Architecture diagram payload / import / diff / copy on the UnitOfWork.

The final five ``api/architecture.py`` endpoints that drove off the request
session — the per-diagram payload ``get`` and ``put``, the Excalidraw ``import``,
the version ``diff``, and the spec→card ``copy`` — now route through
``GetArchitectureDiagramPayloadUseCase`` / ``UpdateArchitectureDiagramPayloadUseCase``
/ ``ImportExcalidrawArchitectureDiagramUseCase`` / ``GetArchitectureDiffUseCase`` /
``CopyArchitectureFromSpecToCardUseCase`` + ``get_unit_of_work``; each adapter only
maps the result/errors to HTTP.

Oracles exercise the migrated status codes + bodies:

* payload get — 200 with the externalized excalidraw payload + stat metadata; 404
  "Architecture design not found" (missing design) vs 404 "Diagram payload not
  found" (missing diagram / no ``adapter_payload_ref``) — the two distinct legacy
  details preserved through the use case's ``EntityNotFoundError`` entity type.
* payload put — 200 re-projected body; 404 design; 404 "Diagram not found"; 409
  card-read-only; 409 spec-locked.
* excalidraw import — 200 appended (diagram count grows); 404 "Diagram not found"
  on a missing ``replace_diagram_id``; 409 card-read-only.
* diff — 200 with ``changed_fields``; 404 on a missing version
  (``_http_error_from_value``).
* copy — 200 list of card designs; 404 "card not found" on a missing card.

Plus the use cases raising ``EntityNotFoundError`` for a missing design / missing
card, and an AST signature check proving the five endpoints take ``uow`` (not a raw
``AsyncSession``).
"""

from __future__ import annotations

import copy
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

USER = "r01a-fu5-s1c-user"
PREFIX = "/api/v1"
SPEC_LOCKED_DETAIL = (
    "Spec is locked because validation passed. Move it back to draft or approved "
    "to edit architecture."
)
_ENDPOINTS = (
    "get_architecture_diagram_payload",
    "update_architecture_diagram_payload",
    "import_excalidraw_architecture_diagram",
    "get_architecture_diff",
    "copy_architecture_from_spec_to_card",
)
_DIAGRAM_ID = "diagram-main"


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


def _architecture_body(title: str = "FU5-S1C Architecture") -> dict:
    """A payload that critiques clean (creates / updates without acknowledgement).
    Its single diagram id is ``diagram-main`` so the payload endpoints can target
    it."""
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
                "id": _DIAGRAM_ID,
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


def _diagram_main_payload() -> dict:
    """The clean, fully-linked excalidraw scene of ``diagram-main`` — re-PUTting it
    leaves the design clean (no acknowledgement required)."""
    return copy.deepcopy(_architecture_body()["diagrams"][0]["adapter_payload"])


async def _seed_parents() -> dict[str, str]:
    """Seed Board + ideation/spec/card parents via raw models. Unique per call so
    tests do not collide on the shared session factory."""
    from okto_pulse.core.models.db import Board, Card, Ideation, Spec

    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"board-fu5s1c-{suffix}",
        "ideation": f"ideation-fu5s1c-{suffix}",
        "spec": f"spec-fu5s1c-{suffix}",
        "card": f"card-fu5s1c-{suffix}",
    }
    async with get_session_factory()() as db:
        db.add(Board(id=ids["board"], name="fu5s1c", owner_id=USER))
        db.add(
            Ideation(
                id=ids["ideation"],
                board_id=ids["board"],
                title="fu5s1c-ideation",
                created_by=USER,
            )
        )
        db.add(
            Spec(
                id=ids["spec"],
                board_id=ids["board"],
                title="fu5s1c-spec",
                created_by=USER,
            )
        )
        db.add(
            Card(
                id=ids["card"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="fu5s1c-card",
                created_by=USER,
            )
        )
        await db.commit()
    return ids


async def _seed_design(parent_type: str, parent_id: str, title: str = "Seed") -> str:
    """Create one Architecture Design (with ``diagram-main`` externalized to the
    payload store) via the existing repository — the same ``select``/ORM the legacy
    endpoints delegated to. ``allow_card_parent_write`` lets us seed a ``card``
    design so the read-only gate can be exercised."""
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


async def _seed_design_two_versions(parent_type: str, parent_id: str) -> str:
    """Seed a design then apply two clean ``global_description`` updates so version
    rows 2 and 3 exist (mirrors the behavior test's diff(from=2, to=3))."""
    from okto_pulse.core.models.schemas import ArchitectureDesignUpdate
    from okto_pulse.core.services.architecture import ArchitectureDesignRepository

    design_id = await _seed_design(parent_type, parent_id)
    async with get_session_factory()() as db:
        repo = ArchitectureDesignRepository(db)
        await repo.update(
            design_id,
            ArchitectureDesignUpdate(
                global_description="Version 2 description.",
                change_summary="bump v2",
            ),
            USER,
        )
        await repo.update(
            design_id,
            ArchitectureDesignUpdate(
                global_description="Version 3 description.",
                change_summary="bump v3",
            ),
            USER,
        )
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


# --- diagram payload get ----------------------------------------------------


@pytest.mark.asyncio
async def test_get_diagram_payload_200(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.get(f"{PREFIX}/architecture/{design_id}/diagrams/{_DIAGRAM_ID}/payload")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["design_id"] == design_id
    assert body["diagram_id"] == _DIAGRAM_ID
    assert body["format"]
    assert body["content_hash"]
    assert body["size_bytes"] > 0
    assert {element["id"] for element in body["payload"]["elements"]} >= {
        "node-client",
        "shape-1",
        "edge-client-api",
    }


@pytest.mark.asyncio
async def test_get_diagram_payload_404_design_missing(client) -> None:
    resp = client.get(f"{PREFIX}/architecture/{_missing()}/diagrams/{_DIAGRAM_ID}/payload")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Architecture design not found"


@pytest.mark.asyncio
async def test_get_diagram_payload_404_diagram_missing(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.get(
        f"{PREFIX}/architecture/{design_id}/diagrams/{_missing('diagram')}/payload"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Diagram payload not found"


# --- diagram payload update -------------------------------------------------


@pytest.mark.asyncio
async def test_update_diagram_payload_200(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.put(
        f"{PREFIX}/architecture/{design_id}/diagrams/{_DIAGRAM_ID}/payload",
        json={
            "payload": _diagram_main_payload(),
            "format": "excalidraw_json",
            "change_summary": "Refreshed diagram payload via migrated PUT.",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == design_id
    assert body["version"] == 2


@pytest.mark.asyncio
async def test_update_diagram_payload_404_design_missing(client) -> None:
    resp = client.put(
        f"{PREFIX}/architecture/{_missing()}/diagrams/{_DIAGRAM_ID}/payload",
        json={"payload": _diagram_main_payload()},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Architecture design not found"


@pytest.mark.asyncio
async def test_update_diagram_payload_404_diagram_missing(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.put(
        f"{PREFIX}/architecture/{design_id}/diagrams/{_missing('diagram')}/payload",
        json={"payload": _diagram_main_payload()},
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == "Diagram not found"


@pytest.mark.asyncio
async def test_update_diagram_payload_409_card_read_only(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("card", ids["card"])
    resp = client.put(
        f"{PREFIX}/architecture/{design_id}/diagrams/{_DIAGRAM_ID}/payload",
        json={"payload": _diagram_main_payload()},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == CARD_ARCHITECTURE_READ_ONLY_MESSAGE


@pytest.mark.asyncio
async def test_update_diagram_payload_409_spec_locked(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("spec", ids["spec"])
    await _lock_spec(ids["spec"])
    resp = client.put(
        f"{PREFIX}/architecture/{design_id}/diagrams/{_DIAGRAM_ID}/payload",
        json={"payload": _diagram_main_payload()},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == SPEC_LOCKED_DETAIL


# --- excalidraw import ------------------------------------------------------


@pytest.mark.asyncio
async def test_import_excalidraw_200_appends(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.post(
        f"{PREFIX}/architecture/{design_id}/diagrams/import-excalidraw",
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
                "statement": "Imported diagram warning reviewed in S1C oracle.",
            },
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["diagrams"]) == 2


@pytest.mark.asyncio
async def test_import_excalidraw_404_replace_missing(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.post(
        f"{PREFIX}/architecture/{design_id}/diagrams/import-excalidraw",
        json={
            "title": "Replace nothing",
            "payload": {"type": "excalidraw", "version": 2, "elements": []},
            "replace_diagram_id": _missing("diagram"),
            "architecture_warning_acknowledgement": {
                "accepted": True,
                "statement": "Acknowledged in S1C oracle.",
            },
        },
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == "Diagram not found"


@pytest.mark.asyncio
async def test_import_excalidraw_409_card_read_only(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("card", ids["card"])
    resp = client.post(
        f"{PREFIX}/architecture/{design_id}/diagrams/import-excalidraw",
        json={
            "title": "Imported into card",
            "payload": {"type": "excalidraw", "version": 2, "elements": []},
        },
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == CARD_ARCHITECTURE_READ_ONLY_MESSAGE


# --- version diff -----------------------------------------------------------


@pytest.mark.asyncio
async def test_get_diff_200(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design_two_versions("ideation", ids["ideation"])
    resp = client.get(
        f"{PREFIX}/architecture/{design_id}/diff",
        params={"from_version": 2, "to_version": 3},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["design_id"] == design_id
    assert "global_description" in body["changed_fields"]
    assert body["breaking_change_flag"] is False


@pytest.mark.asyncio
async def test_get_diff_404_version_missing(client) -> None:
    ids = await _seed_parents()
    design_id = await _seed_design("ideation", ids["ideation"])
    resp = client.get(
        f"{PREFIX}/architecture/{design_id}/diff",
        params={"from_version": 50, "to_version": 51},
    )
    assert resp.status_code == 404, resp.text
    assert "not found" in resp.json()["detail"]


# --- copy spec architecture to card -----------------------------------------


@pytest.mark.asyncio
async def test_copy_architecture_from_spec_to_card_200(client) -> None:
    ids = await _seed_parents()
    source_id = await _seed_design("spec", ids["spec"], title="Spec Architecture")
    resp = client.post(
        f"{PREFIX}/cards/{ids['card']}/copy-architecture-from-spec/{ids['spec']}",
        json={},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body
    assert body[0]["parent_type"] == "card"
    assert body[0]["source_design_id"] == source_id


@pytest.mark.asyncio
async def test_copy_architecture_404_card_missing(client) -> None:
    ids = await _seed_parents()
    await _seed_design("spec", ids["spec"], title="Spec Architecture")
    resp = client.post(
        f"{PREFIX}/cards/{_missing('card')}/copy-architecture-from-spec/{ids['spec']}",
        json={},
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == "card not found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_diagram_payload_use_case_raises_for_missing_design() -> None:
    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDiagramPayloadCommand,
        GetArchitectureDiagramPayloadUseCase,
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
            await GetArchitectureDiagramPayloadUseCase().execute(
                GetArchitectureDiagramPayloadCommand(_missing(), _DIAGRAM_ID),
                actor=actor,
                uow=uow,
            )


@pytest.mark.asyncio
async def test_copy_use_case_raises_for_missing_card() -> None:
    from okto_pulse.core.application.use_cases.architecture_crud import (
        CopyArchitectureFromSpecToCardCommand,
        CopyArchitectureFromSpecToCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError) as excinfo:
        async with uowf(actor=actor) as uow:
            await CopyArchitectureFromSpecToCardUseCase().execute(
                CopyArchitectureFromSpecToCardCommand(
                    _missing("card"), _missing("spec"), None, None
                ),
                actor=actor,
                uow=uow,
            )
    assert excinfo.value.entity_type == "card"


def test_fu5_s1c_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(architecture_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
