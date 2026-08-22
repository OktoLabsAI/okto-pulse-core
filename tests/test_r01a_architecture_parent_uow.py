"""Spec R01A FU5-S1A — Architecture parent list/create on the UnitOfWork.

The eight ``api/architecture.py`` parent list/create endpoints (ideation /
refinement / spec / card × list / create) now route through
``ListArchitectureUseCase`` / ``CreateArchitectureUseCase`` + ``get_unit_of_work``;
each adapter only maps the result/errors to HTTP. Oracles: list 200 + 404 (per
parent type, "{parent_type} not found"), create 201 then list reflects it,
create 404 for a missing parent, create 409 for a locked spec, create 409 for a
read-only card parent, the use cases raising ``EntityNotFoundError`` for a missing
parent, and an AST signature check proving the eight endpoints take ``uow`` (not a
raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import architecture as architecture_api
from okto_pulse.community.api.architecture import router as architecture_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.services.architecture import CARD_ARCHITECTURE_READ_ONLY_MESSAGE

USER = "r01a-fu5-s1a-user"
PREFIX = "/api/v1"
SPEC_LOCKED_DETAIL = (
    "Spec is locked because validation passed. Move it to Draft to open a new "
    "edition before editing architecture."
)
_ENDPOINTS = (
    "list_ideation_architecture",
    "create_ideation_architecture",
    "list_refinement_architecture",
    "create_refinement_architecture",
    "list_spec_architecture",
    "create_spec_architecture",
    "list_card_architecture",
    "create_card_architecture",
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


def _architecture_body(title: str = "FU5-S1A Architecture") -> dict:
    """A payload that critiques clean (creates 201 without acknowledgement)."""
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


async def _seed_parents() -> dict[str, str]:
    """Seed Board + one of each architecture parent (ideation/refinement/spec/card)
    via raw models and return their ids. Unique per call so tests do not collide on
    the shared session factory."""
    from sqlalchemy_test_models import Board, Card, Ideation, Refinement, Spec

    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"board-fu5s1a-{suffix}",
        "ideation": f"ideation-fu5s1a-{suffix}",
        "refinement": f"refinement-fu5s1a-{suffix}",
        "spec": f"spec-fu5s1a-{suffix}",
        "card": f"card-fu5s1a-{suffix}",
    }
    async with get_session_factory()() as db:
        db.add(Board(id=ids["board"], name="fu5s1a", owner_id=USER))
        db.add(
            Ideation(
                id=ids["ideation"],
                board_id=ids["board"],
                title="fu5s1a-ideation",
                created_by=USER,
            )
        )
        db.add(
            Refinement(
                id=ids["refinement"],
                ideation_id=ids["ideation"],
                board_id=ids["board"],
                title="fu5s1a-refinement",
                created_by=USER,
            )
        )
        db.add(
            Spec(
                id=ids["spec"],
                board_id=ids["board"],
                title="fu5s1a-spec",
                created_by=USER,
            )
        )
        db.add(
            Card(
                id=ids["card"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="fu5s1a-card",
                created_by=USER,
            )
        )
        await db.commit()
    return ids


async def _lock_spec(spec_id: str) -> None:
    """Mark a spec's current validation outcome as success (architecture locked)."""
    from sqlalchemy_test_models import Spec

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        spec.validations = [{"id": "val-success", "outcome": "success"}]
        spec.current_validation_id = "val-success"
        await db.commit()


def _missing(parent_type: str) -> str:
    return f"{parent_type}-missing-{uuid.uuid4().hex[:8]}"


# --- list -------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_type", ["ideation", "refinement", "spec", "card"])
async def test_list_architecture_200_empty(client, parent_type) -> None:
    ids = await _seed_parents()
    resp = client.get(f"{PREFIX}/{parent_type}s/{ids[parent_type]}/architecture")
    assert resp.status_code == 200, resp.text
    assert resp.json() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_type", ["ideation", "refinement", "spec", "card"])
async def test_list_architecture_404_per_parent_type(client, parent_type) -> None:
    resp = client.get(f"{PREFIX}/{parent_type}s/{_missing(parent_type)}/architecture")
    assert resp.status_code == 404
    assert resp.json()["detail"] == f"{parent_type} not found"


# --- create -----------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_type", ["ideation", "refinement", "spec"])
async def test_create_architecture_201_then_listed(client, parent_type) -> None:
    ids = await _seed_parents()
    created = client.post(
        f"{PREFIX}/{parent_type}s/{ids[parent_type]}/architecture",
        json=_architecture_body(),
    )
    assert created.status_code == 201, created.text
    design = created.json()
    assert design["parent_type"] == parent_type
    assert design["parent_id"] == ids[parent_type]
    assert design["id"]

    listing = client.get(f"{PREFIX}/{parent_type}s/{ids[parent_type]}/architecture")
    assert listing.status_code == 200
    summaries = listing.json()
    assert len(summaries) == 1
    assert summaries[0]["id"] == design["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_type", ["ideation", "refinement", "spec", "card"])
async def test_create_architecture_404_missing_parent(client, parent_type) -> None:
    resp = client.post(
        f"{PREFIX}/{parent_type}s/{_missing(parent_type)}/architecture",
        json=_architecture_body(),
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == f"{parent_type} not found"


@pytest.mark.asyncio
async def test_create_spec_architecture_409_when_locked(client) -> None:
    ids = await _seed_parents()
    first = client.post(
        f"{PREFIX}/specs/{ids['spec']}/architecture",
        json=_architecture_body("Lockable"),
    )
    assert first.status_code == 201, first.text

    await _lock_spec(ids["spec"])

    blocked = client.post(
        f"{PREFIX}/specs/{ids['spec']}/architecture",
        json=_architecture_body("Blocked"),
    )
    assert blocked.status_code == 409, blocked.text
    assert blocked.json()["detail"] == SPEC_LOCKED_DETAIL


@pytest.mark.asyncio
async def test_create_card_architecture_409_read_only(client) -> None:
    ids = await _seed_parents()
    resp = client.post(
        f"{PREFIX}/cards/{ids['card']}/architecture",
        json=_architecture_body("Card Direct Authoring"),
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == CARD_ARCHITECTURE_READ_ONLY_MESSAGE


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_list_architecture_use_case_raises_for_missing_parent() -> None:
    from okto_pulse.core.application.use_cases.architecture_crud import (
        ListArchitectureCommand,
        ListArchitectureUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await ListArchitectureUseCase().execute(
                ListArchitectureCommand("ideation", _missing("ideation")),
                actor=actor,
                uow=uow,
            )


@pytest.mark.asyncio
async def test_create_architecture_use_case_raises_for_missing_parent() -> None:
    from okto_pulse.core.application.use_cases.architecture_crud import (
        CreateArchitectureCommand,
        CreateArchitectureUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await CreateArchitectureUseCase().execute(
                CreateArchitectureCommand(
                    "refinement", _missing("refinement"), _architecture_body()
                ),
                actor=actor,
                uow=uow,
            )


def test_fu5_s1a_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(architecture_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
