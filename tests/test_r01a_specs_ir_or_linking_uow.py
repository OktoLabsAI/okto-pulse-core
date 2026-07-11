"""Spec R01A REST-FU3e-S3 — spec integration / observability requirement
task-linking on the UnitOfWork.

The two requirement-linking endpoints now route through transport-free use
cases + ``get_unit_of_work``; each adapter only maps the result/errors back to
HTTP:

  link_task_to_integration_requirement    -> LinkTaskToIntegrationRequirementUseCase
  link_task_to_observability_requirement  -> LinkTaskToObservabilityRequirementUseCase

Both legacy endpoints enforced ``_require_permissions`` (the
``spec.*_requirements.link_task`` + ``card.link_to.{ir,or}`` pair) inside the
HTTP handler; that guard moves into the use case (resolved via
``services.main.resolve_user_permissions`` → ``check_permission`` →
``PermissionDeniedError`` → 403). Persistence is solely ``update_spec`` (no
card-side mutation), so the oracle asserts the spec-side ``linked_task_ids``
only — exactly as the legacy code.

Oracles per endpoint: happy path (200, spec-side state verified), the three
distinct legacy 404 details (spec / card / requirement), the 403 when the
link_task permission is denied, and the 422 when ``update_spec`` rejects a
pre-existing orphan ``linked_task_ids`` ref. Plus a use-case-level
``EntityNotFoundError`` for a missing spec and an AST signature check proving the
endpoints take ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import specs as specs_api
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import Board, Card, Spec, SpecStatus

USER = "r01a-fu3e-s3-user"
PREFIX = "/api/v1"
_ENDPOINTS = (
    "link_task_to_integration_requirement",
    "link_task_to_observability_requirement",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    # No permission patch: with no Agent seeded, resolve_user_permissions falls
    # back to the all-True default set, so the happy/404/422 paths clear the
    # in-use-case guard exactly like the board owner did under the legacy code.
    return TestClient(app)


async def _seed(
    *,
    spec_status: SpecStatus = SpecStatus.DRAFT,
    integration_requirements: list[dict] | None = None,
    observability_requirements: list[dict] | None = None,
) -> tuple[str, str, str]:
    """Seed a board + spec + card and return ``(board_id, spec_id, card_id)``."""
    bid = f"board-fu3es3-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu3es3-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu3es3-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu3es3", owner_id=USER))
        db.add(
            Spec(
                id=sid,
                board_id=bid,
                title="fu3es3-spec",
                status=spec_status,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                integration_requirements=integration_requirements or [],
                observability_requirements=observability_requirements or [],
            )
        )
        db.add(
            Card(
                id=cid,
                board_id=bid,
                spec_id=None,
                title="fu3es3-card",
                created_by=USER,
            )
        )
        await db.commit()
    return bid, sid, cid


async def _get_spec(spec_id: str) -> Spec | None:
    async with get_session_factory()() as db:
        return await db.get(Spec, spec_id)


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


# --- integration requirement ------------------------------------------------


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_200(client) -> None:
    _, sid, cid = await _seed(
        integration_requirements=[{"id": "ir1", "title": "IR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/integration-requirements/ir1/link-task/{cid}"
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "success": True,
        "spec_id": sid,
        "requirement_id": "ir1",
        "card_id": cid,
    }
    spec = await _get_spec(sid)
    assert spec.integration_requirements[0]["linked_task_ids"] == [cid]


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_spec_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/specs/{_missing()}/integration-requirements/ir1/link-task/{_missing()}"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_card_404(client) -> None:
    _, sid, _ = await _seed(
        integration_requirements=[{"id": "ir1", "title": "IR one", "linked_task_ids": []}]
    )
    missing_card = _missing()
    resp = client.post(
        f"{PREFIX}/specs/{sid}/integration-requirements/ir1/link-task/{missing_card}"
    )
    assert resp.status_code == 404
    assert (
        resp.json()["detail"]
        == f"Card '{missing_card}' not found — cannot link a non-existent card."
    )


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_requirement_404(client) -> None:
    _, sid, cid = await _seed(
        integration_requirements=[{"id": "ir1", "title": "IR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/integration-requirements/nope/link-task/{cid}"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Integration requirement 'nope' not found in spec."


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_permission_403(
    client, monkeypatch
) -> None:
    from okto_pulse.core.infra.permissions import PermissionSet

    async def _deny(db, user_id, board_id):
        return PermissionSet(
            {"spec": {"integration_requirements": {"link_task": False}}}
        )

    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions", _deny
    )
    _, sid, cid = await _seed(
        integration_requirements=[{"id": "ir1", "title": "IR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/integration-requirements/ir1/link-task/{cid}"
    )
    assert resp.status_code == 403
    spec = await _get_spec(sid)
    assert spec.integration_requirements[0]["linked_task_ids"] == []


@pytest.mark.asyncio
async def test_link_task_to_integration_requirement_orphan_422(client) -> None:
    ghost = f"ghost-{uuid.uuid4().hex[:8]}"
    _, sid, cid = await _seed(
        integration_requirements=[
            {"id": "ir1", "title": "IR one", "linked_task_ids": [ghost]}
        ]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/integration-requirements/ir1/link-task/{cid}"
    )
    assert resp.status_code == 422
    assert ghost in resp.json()["detail"]


# --- observability requirement ----------------------------------------------


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_200(client) -> None:
    _, sid, cid = await _seed(
        observability_requirements=[{"id": "or1", "title": "OR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/observability-requirements/or1/link-task/{cid}"
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "success": True,
        "spec_id": sid,
        "requirement_id": "or1",
        "card_id": cid,
    }
    spec = await _get_spec(sid)
    assert spec.observability_requirements[0]["linked_task_ids"] == [cid]


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_spec_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/specs/{_missing()}/observability-requirements/or1/link-task/{_missing()}"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_card_404(client) -> None:
    _, sid, _ = await _seed(
        observability_requirements=[{"id": "or1", "title": "OR one", "linked_task_ids": []}]
    )
    missing_card = _missing()
    resp = client.post(
        f"{PREFIX}/specs/{sid}/observability-requirements/or1/link-task/{missing_card}"
    )
    assert resp.status_code == 404
    assert (
        resp.json()["detail"]
        == f"Card '{missing_card}' not found — cannot link a non-existent card."
    )


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_requirement_404(client) -> None:
    _, sid, cid = await _seed(
        observability_requirements=[{"id": "or1", "title": "OR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/observability-requirements/nope/link-task/{cid}"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Observability requirement 'nope' not found in spec."


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_permission_403(
    client, monkeypatch
) -> None:
    from okto_pulse.core.infra.permissions import PermissionSet

    async def _deny(db, user_id, board_id):
        return PermissionSet(
            {"spec": {"observability_requirements": {"link_task": False}}}
        )

    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions", _deny
    )
    _, sid, cid = await _seed(
        observability_requirements=[{"id": "or1", "title": "OR one", "linked_task_ids": []}]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/observability-requirements/or1/link-task/{cid}"
    )
    assert resp.status_code == 403
    spec = await _get_spec(sid)
    assert spec.observability_requirements[0]["linked_task_ids"] == []


@pytest.mark.asyncio
async def test_link_task_to_observability_requirement_orphan_422(client) -> None:
    ghost = f"ghost-{uuid.uuid4().hex[:8]}"
    _, sid, cid = await _seed(
        observability_requirements=[
            {"id": "or1", "title": "OR one", "linked_task_ids": [ghost]}
        ]
    )
    resp = client.post(
        f"{PREFIX}/specs/{sid}/observability-requirements/or1/link-task/{cid}"
    )
    assert resp.status_code == 422
    assert ghost in resp.json()["detail"]


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_link_ir_use_case_raises_for_missing_spec() -> None:
    from okto_pulse.core.application.use_cases import (
        LinkTaskToIntegrationRequirementCommand,
        LinkTaskToIntegrationRequirementUseCase,
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
            await LinkTaskToIntegrationRequirementUseCase().execute(
                LinkTaskToIntegrationRequirementCommand(_missing(), "ir1", _missing()),
                actor=actor,
                uow=uow,
            )


@pytest.mark.asyncio
async def test_link_or_use_case_raises_for_missing_spec() -> None:
    from okto_pulse.core.application.use_cases import (
        LinkTaskToObservabilityRequirementCommand,
        LinkTaskToObservabilityRequirementUseCase,
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
            await LinkTaskToObservabilityRequirementUseCase().execute(
                LinkTaskToObservabilityRequirementCommand(_missing(), "or1", _missing()),
                actor=actor,
                uow=uow,
            )


def test_fu3e_s3_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
