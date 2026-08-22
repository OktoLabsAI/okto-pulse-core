"""Spec R01A REST-FU3b-S1 — structured spec entities + validation gate on UoW.

The four structured-entity endpoints (create/update/operate/preview) now route
through ``RunStructuredSpecEntityUseCase`` + ``get_unit_of_work`` (the transport
helper only maps the result to HTTP); ``submit_spec_validation`` gets its hybrid
wiring fixed (``PulseUnitOfWork`` instead of a raw ``AsyncSession`` passed as the
uow); ``list_spec_validations`` routes through ``ListSpecValidationsUseCase``.
Oracles: structured create 200 + spec 404 + operate-needs-operation 422 +
validation submit/list 404 (proves the uow path) + list 200 + use-case + AST.
"""

from __future__ import annotations

import inspect
import uuid
from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from okto_pulse.community.api import specs as specs_api
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu3b-s1-user"
OTHER = "r01a-fu3b-s1-other"
PREFIX = "/api/v1"
_ENDPOINTS = (
    "create_structured_spec_entity",
    "update_structured_spec_entity",
    "operate_structured_spec_entity",
    "preview_structured_spec_entity_impact",
    "submit_spec_validation",
    "list_spec_validations",
)

_BUSINESS_RULE = {
    "id": "br_fu3bs1",
    "title": "FU3b-S1 boundary",
    "rule": "Structured spec child edits flow through the use case",
    "when": "A caller mutates a spec child entity",
    "then": "The use case validates and persists it under the UoW",
}


async def _allow_permissions(db, user_id, board_id):
    from okto_pulse.core.infra.permissions import resolve_permissions

    return resolve_permissions(None, None, None)


@pytest.fixture
def client(monkeypatch):
    # The structured-entity + validation flows resolve permissions inside the use
    # case via services.main.resolve_user_permissions — patch the canonical home.
    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions", _allow_permissions
    )
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


async def _seed_spec(owner: str = USER) -> str:
    from sqlalchemy_test_models import Board
    from okto_pulse.core.models.schemas import SpecCreate
    from okto_pulse.core.services import SpecService

    bid = f"board-fu3bs1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu3bs1", owner_id=owner))
        await db.commit()
    async with get_session_factory()() as db:
        spec = await SpecService(db).create_spec(
            bid,
            owner,
            SpecCreate(
                title=f"fu3bs1-{uuid.uuid4().hex[:6]}",
                delivery_context="brownfield",
            ),
        )
        await db.commit()
        return spec.id


async def _validation_state(spec_id: str) -> dict:
    from sqlalchemy_test_models import ActivityLog, Spec

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        activity_count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.board_id == spec.board_id)
        )
        return {
            "status": spec.status,
            "version": spec.version,
            "validations": deepcopy(spec.validations),
            "current_validation_id": spec.current_validation_id,
            "activity_count": activity_count,
        }


def _missing() -> str:
    return f"spec-missing-{uuid.uuid4().hex[:8]}"


def _struct_url(spec_id: str, entity_type: str, entity_id: str | None = None) -> str:
    base = f"{PREFIX}/specs/{spec_id}/structured-entities/{entity_type}"
    return f"{base}/{entity_id}" if entity_id else base


# --- structured entities ----------------------------------------------------


@pytest.mark.asyncio
async def test_create_structured_business_rule_200(client) -> None:
    spec_id = await _seed_spec()
    resp = client.post(
        _struct_url(spec_id, "business_rule"),
        json={"payload": _BUSINESS_RULE, "expected_spec_version": 1},
    )
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_structured_spec_404(client) -> None:
    resp = client.post(
        _struct_url(_missing(), "business_rule"), json={"payload": _BUSINESS_RULE}
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_structured_spec_foreign_board_has_no_mutation(client) -> None:
    from sqlalchemy_test_models import Spec

    spec_id = await _seed_spec(owner=OTHER)
    response = client.post(
        _struct_url(spec_id, "business_rule"),
        json={"payload": _BUSINESS_RULE, "expected_spec_version": 1},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Spec not found"
    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.business_rules in (None, [])
        assert spec.version == 1


@pytest.mark.asyncio
async def test_operate_requires_operation_422(client) -> None:
    spec_id = await _seed_spec()
    resp = client.post(
        _struct_url(spec_id, "business_rule", "br_fu3bs1"), json={"payload": {}}
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == "operation is required."


# --- validation gate (hybrid fix + reader) ----------------------------------


def _valid_submit_data() -> dict:
    # Justifications must clear the command's ≥20-char shape gate so the request
    # reaches the spec lookup (otherwise CommandValidationError → 400 fires first).
    return {
        "expected_validation_edition": 1,
        "expected_spec_version": 1,
        "expected_head_revision": 0,
        "confidence": 90,
        "confidence_justification": "The evidence supports a confident decision",
        "clarity": 90,
        "clarity_justification": "The requirements use clear domain language",
        "assertiveness": 85,
        "assertiveness_justification": "FRs are measurable with no weasel words",
        "decidability": 90,
        "decidability_justification": "Every criterion has a binary outcome",
        "ambiguity": 15,
        "ambiguity_justification": "Glossary added and terms defined clearly",
        "recommendation": "approve",
    }


@pytest.mark.asyncio
async def test_submit_validation_missing_spec_404(client) -> None:
    """Proves the hybrid fix: the gate now flows through the UoW and a missing
    spec surfaces as 404 (EntityNotFoundError) instead of a 500."""
    resp = client.post(
        f"{PREFIX}/specs/{_missing()}/validation", json=_valid_submit_data()
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_list_validations_200_and_404(client) -> None:
    spec_id = await _seed_spec()
    ok = client.get(f"{PREFIX}/specs/{spec_id}/validations")
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body["spec_id"] == spec_id and "validations" in body
    miss = client.get(f"{PREFIX}/specs/{_missing()}/validations")
    assert miss.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permission_flags",
    (
        {"entity": {"read": True}, "validation": {"read": False}},
        {"entity": {"read": False}, "validation": {"read": True}},
    ),
)
async def test_legacy_validation_history_requires_entity_and_validation_reads(
    client,
    monkeypatch,
    permission_flags: dict,
) -> None:
    from okto_pulse.core.domain.permissions import PermissionSet

    async def _restricted_permissions(db, user_id, board_id):
        return PermissionSet({"spec": permission_flags})

    monkeypatch.setattr(
        "okto_pulse.core.services.main.resolve_user_permissions",
        _restricted_permissions,
    )
    spec_id = await _seed_spec()

    history = client.get(f"{PREFIX}/specs/{spec_id}/validations")
    current = client.get(f"{PREFIX}/specs/{spec_id}/validations/current")

    assert history.status_code == current.status_code == 403


@pytest.mark.asyncio
async def test_validation_read_and_submit_foreign_board_fail_closed_without_audit(
    client,
) -> None:
    spec_id = await _seed_spec(owner=OTHER)
    before = await _validation_state(spec_id)

    listed = client.get(f"{PREFIX}/specs/{spec_id}/validations")
    submitted = client.post(
        f"{PREFIX}/specs/{spec_id}/validation", json=_valid_submit_data()
    )

    assert listed.status_code == 404, listed.text
    assert listed.json()["detail"] == "Spec not found"
    assert submitted.status_code == 404, submitted.text
    assert submitted.json()["detail"] == "Spec not found"
    assert await _validation_state(spec_id) == before


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_run_structured_use_case_raises_for_missing_spec() -> None:
    from okto_pulse.core.application.use_cases import (
        RunStructuredSpecEntityCommand,
        RunStructuredSpecEntityUseCase,
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
            await RunStructuredSpecEntityUseCase().execute(
                RunStructuredSpecEntityCommand(_missing(), "business_rule", "create"),
                actor=actor,
                uow=uow,
            )


def test_fu3b_s1_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
