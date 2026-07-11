"""Card S-LANE-01 — canonical ``invalid_lane_type`` envelope across REST + MCP.

An invalid sprint ``lane_type`` (e.g. ``release_validation``) must surface a
single canonical envelope on BOTH transports instead of leaking the raw Pydantic
surface, and must be fail-closed (no sprint created / no lane mutated). Valid
values (``normal`` / ``hotfix``) keep working and persisting. ``SprintService``
stays transport-neutral and the enum stays bounded to ``normal``/``hotfix``.

Scenarios:
- TS-LANE-01: REST create release_validation → envelope, no sprint created.
- TS-LANE-02: REST update release_validation on a normal sprint → envelope, stays normal.
- TS-LANE-03: MCP create release_validation → envelope, no pydantic leak, no sprint.
- TS-LANE-04: MCP update release_validation on a hotfix sprint → envelope, stays hotfix.
- TS-LANE-05: REST + MCP create/update with normal and hotfix → still work + persist.
- TS-LANE-06: SprintService imports no REST/MCP/envelope symbols; enum = {normal, hotfix}.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from okto_pulse.community.app import install_request_validation_handler
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.inbound.enum_error_envelope import canonical_enum_error
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
)

USER_ID = "lane-type-user"
INVALID_LANE = "release_validation"
EXPECTED_ENVELOPE = {
    "code": "invalid_lane_type",
    "field": "lane_type",
    "received_value": INVALID_LANE,
    "accepted_values": ["normal", "hotfix"],
    "mutation_applied": False,
}
# Substrings that would betray a raw Pydantic / traceback leak.
LEAK_MARKERS = ("pydantic", "errors.pydantic.dev", "traceback", "Traceback", "stack")


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _spec(spec_id: str, board_id: str, status: SpecStatus) -> Spec:
    return Spec(
        id=spec_id,
        board_id=board_id,
        title="Lane Type Spec",
        status=status,
        archived=False,
        acceptance_criteria=["AC1"],
        functional_requirements=["FR1"],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        technical_requirements=[],
        decisions=[],
        created_by=USER_ID,
    )


async def _seed(db_factory):
    """Seed a board with a normal spec, a done spec, and two draft sprints."""
    board_id = _id("lane-board")
    spec_normal_id = _id("lane-spec-normal")
    spec_done_id = _id("lane-spec-done")
    sprint_normal_id = _id("lane-sprint-normal")
    sprint_hotfix_id = _id("lane-sprint-hotfix")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lane Type Board", owner_id=USER_ID))
        db.add(_spec(spec_normal_id, board_id, SpecStatus.IN_PROGRESS))
        db.add(_spec(spec_done_id, board_id, SpecStatus.DONE))
        db.add(Sprint(
            id=sprint_normal_id, board_id=board_id, spec_id=spec_normal_id,
            title="Normal Lane Sprint", status=SprintStatus.DRAFT,
            lane_type=SprintLaneType.NORMAL, created_by=USER_ID,
        ))
        db.add(Sprint(
            id=sprint_hotfix_id, board_id=board_id, spec_id=spec_done_id,
            title="Hotfix Lane Sprint", status=SprintStatus.DRAFT,
            lane_type=SprintLaneType.HOTFIX, created_by=USER_ID,
        ))
        await db.commit()

    return {
        "board_id": board_id,
        "spec_normal_id": spec_normal_id,
        "spec_done_id": spec_done_id,
        "sprint_normal_id": sprint_normal_id,
        "sprint_hotfix_id": sprint_hotfix_id,
    }


async def _count_sprints(db_factory, board_id: str) -> int:
    async with db_factory() as db:
        return (await db.execute(
            select(func.count()).select_from(Sprint).where(Sprint.board_id == board_id)
        )).scalar() or 0


async def _lane_of(db_factory, sprint_id: str) -> SprintLaneType:
    async with db_factory() as db:
        sprint = await db.get(Sprint, sprint_id)
        return sprint.lane_type


def _assert_no_leak(raw_text: str) -> None:
    for marker in LEAK_MARKERS:
        assert marker not in raw_text, f"raw response leaked {marker!r}: {raw_text}"


# ============================================================================
# REST surface
# ============================================================================


@pytest_asyncio.fixture
async def rest_ctx(db_factory):
    """Minimal FastAPI app wired exactly like production for sprint routes:
    the sprint router + the canonical RequestValidationError handler installed
    by the SAME installer ``create_app`` uses (no copy-drift)."""
    ids = await _seed(db_factory)

    from okto_pulse.community.api.sprints import router as sprints_router

    app = FastAPI()
    install_request_validation_handler(app)
    app.include_router(sprints_router)

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    return TestClient(app), ids


def _create_body(spec_id: str, lane_type: str, title: str) -> dict:
    return {"title": title, "spec_id": spec_id, "lane_type": lane_type}


async def test_ts_lane_01_rest_create_invalid_lane_type(rest_ctx, db_factory):
    """TS-LANE-01: REST create with release_validation → envelope, no sprint created."""
    client, ids = rest_ctx
    board_id, spec_id = ids["board_id"], ids["spec_normal_id"]
    before = await _count_sprints(db_factory, board_id)

    resp = client.post(
        f"/boards/{board_id}/specs/{spec_id}/sprints",
        json=_create_body(spec_id, INVALID_LANE, "Should Not Persist"),
    )

    assert resp.status_code == 422
    assert resp.json() == EXPECTED_ENVELOPE
    _assert_no_leak(resp.text)
    # Fail-closed: nothing persisted.
    assert await _count_sprints(db_factory, board_id) == before
    async with db_factory() as db:
        leaked = (await db.execute(
            select(func.count()).select_from(Sprint).where(Sprint.title == "Should Not Persist")
        )).scalar() or 0
    assert leaked == 0


async def test_ts_lane_02_rest_update_invalid_keeps_normal(rest_ctx, db_factory):
    """TS-LANE-02: REST update release_validation on a normal sprint → envelope, stays normal."""
    client, ids = rest_ctx
    sprint_id = ids["sprint_normal_id"]
    assert await _lane_of(db_factory, sprint_id) == SprintLaneType.NORMAL

    resp = client.patch(f"/sprints/{sprint_id}", json={"lane_type": INVALID_LANE})

    assert resp.status_code == 422
    assert resp.json() == EXPECTED_ENVELOPE
    _assert_no_leak(resp.text)
    assert await _lane_of(db_factory, sprint_id) == SprintLaneType.NORMAL


async def test_ts_lane_05_rest_valid_values_persist(rest_ctx, db_factory):
    """TS-LANE-05 (REST half): normal + hotfix create still work and persist."""
    client, ids = rest_ctx

    normal = client.post(
        f"/boards/{ids['board_id']}/specs/{ids['spec_normal_id']}/sprints",
        json=_create_body(ids["spec_normal_id"], "normal", "REST Normal Create"),
    )
    assert normal.status_code == 201
    assert normal.json()["lane_type"] == "normal"

    # Hotfix is eligible on a DONE spec (no origin required).
    hotfix = client.post(
        f"/boards/{ids['board_id']}/specs/{ids['spec_done_id']}/sprints",
        json=_create_body(ids["spec_done_id"], "hotfix", "REST Hotfix Create"),
    )
    assert hotfix.status_code == 201
    assert hotfix.json()["lane_type"] == "hotfix"

    async with db_factory() as db:
        rows = (await db.execute(
            select(Sprint.title, Sprint.lane_type).where(
                Sprint.title.in_(["REST Normal Create", "REST Hotfix Create"])
            )
        )).all()
    persisted = {title: lane for title, lane in rows}
    assert persisted["REST Normal Create"] == SprintLaneType.NORMAL
    assert persisted["REST Hotfix Create"] == SprintLaneType.HOTFIX


async def test_rest_unmapped_validation_error_uses_default(rest_ctx):
    """Boundedness guard (FR #3): a non-lane_type validation error keeps FastAPI's
    default 422 shape (``detail`` list) — the canonical envelope must NOT swallow it."""
    client, ids = rest_ctx
    # Missing required ``title`` and ``spec_id`` → default RequestValidationError.
    resp = client.post(
        f"/boards/{ids['board_id']}/specs/{ids['spec_normal_id']}/sprints",
        json={"lane_type": "normal"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert "detail" in body
    assert body.get("code") != "invalid_lane_type"


# ============================================================================
# MCP surface
# ============================================================================


def _stub_ctx(board_id: str):
    return type("Ctx", (), {
        "agent_id": USER_ID,
        "agent_name": "lane-type-agent",
        "board_id": board_id,
        "permissions": ["board:read", "specs:update", "sprint.entity.create"],
    })()


async def _call(name: str, **kwargs) -> str:
    """Invoke an MCP tool's underlying function and return the RAW json string."""
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return await tool.fn(**kwargs)


async def test_ts_lane_03_mcp_create_invalid_lane_type(db_factory):
    """TS-LANE-03: MCP create release_validation → envelope, no pydantic leak, no sprint."""
    ids = await _seed(db_factory)
    board_id, spec_id = ids["board_id"], ids["spec_normal_id"]
    before = await _count_sprints(db_factory, board_id)

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        raw = await _call(
            "okto_pulse_create_sprint",
            board_id=board_id, spec_id=spec_id,
            title="MCP Should Not Persist", lane_type=INVALID_LANE,
        )

    assert json.loads(raw) == EXPECTED_ENVELOPE
    _assert_no_leak(raw)
    assert await _count_sprints(db_factory, board_id) == before


async def test_ts_lane_04_mcp_update_invalid_keeps_hotfix(db_factory):
    """TS-LANE-04: MCP update release_validation on a hotfix sprint → envelope, stays hotfix."""
    ids = await _seed(db_factory)
    board_id, sprint_id = ids["board_id"], ids["sprint_hotfix_id"]
    assert await _lane_of(db_factory, sprint_id) == SprintLaneType.HOTFIX

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        raw = await _call(
            "okto_pulse_update_sprint",
            board_id=board_id, sprint_id=sprint_id, lane_type=INVALID_LANE,
        )

    assert json.loads(raw) == EXPECTED_ENVELOPE
    _assert_no_leak(raw)
    assert await _lane_of(db_factory, sprint_id) == SprintLaneType.HOTFIX


async def test_ts_lane_05_mcp_valid_values_persist(db_factory):
    """TS-LANE-05 (MCP half): create normal + hotfix and update lane_type=normal persist."""
    ids = await _seed(db_factory)
    board_id = ids["board_id"]

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        normal_raw = await _call(
            "okto_pulse_create_sprint",
            board_id=board_id, spec_id=ids["spec_normal_id"],
            title="MCP Normal Create", lane_type="normal",
        )
        hotfix_raw = await _call(
            "okto_pulse_create_sprint",
            board_id=board_id, spec_id=ids["spec_done_id"],
            title="MCP Hotfix Create", lane_type="hotfix",
        )
        update_raw = await _call(
            "okto_pulse_update_sprint",
            board_id=board_id, sprint_id=ids["sprint_normal_id"], lane_type="normal",
        )

    normal = json.loads(normal_raw)
    hotfix = json.loads(hotfix_raw)
    update = json.loads(update_raw)
    assert normal.get("success") is True and normal["sprint"]["lane_type"] == "normal"
    assert hotfix.get("success") is True and hotfix["sprint"]["lane_type"] == "hotfix"
    assert update.get("success") is True and update["sprint"]["lane_type"] == "normal"

    async with db_factory() as db:
        rows = (await db.execute(
            select(Sprint.title, Sprint.lane_type).where(
                Sprint.title.in_(["MCP Normal Create", "MCP Hotfix Create"])
            )
        )).all()
    persisted = {title: lane for title, lane in rows}
    assert persisted["MCP Normal Create"] == SprintLaneType.NORMAL
    assert persisted["MCP Hotfix Create"] == SprintLaneType.HOTFIX


# ============================================================================
# Transport-neutrality + bounded enum (unit)
# ============================================================================


def test_ts_lane_06_enum_bounded_and_service_transport_neutral():
    """TS-LANE-06: enum stays {normal, hotfix} and SprintService imports no
    REST/MCP/envelope component (stays transport-neutral)."""
    assert [member.value for member in SprintLaneType] == ["normal", "hotfix"]

    # The envelope's accepted set is derived from the enum, never hardcoded apart.
    assert EXPECTED_ENVELOPE["accepted_values"] == [m.value for m in SprintLaneType]

    service_src = Path(
        "src/okto_pulse/core/services/main.py"
    ).read_text(encoding="utf-8")
    forbidden = (
        "enum_error_envelope",
        "canonical_enum_error",
        "RequestValidationError",
        "core.inbound",
        "core.mcp.server",
        "fastapi",
        "JSONResponse",
    )
    for token in forbidden:
        assert token not in service_src, (
            f"SprintService module must stay transport-neutral; found {token!r}"
        )


def test_canonical_enum_error_is_bounded_to_lane_type():
    """The normalizer only fires for an ``enum``-typed lane_type error; any other
    shape returns None so the caller keeps default handling."""
    lane_error = [{"type": "enum", "loc": ["body", "lane_type"], "input": INVALID_LANE}]
    assert canonical_enum_error(lane_error) == EXPECTED_ENVELOPE

    # Unmapped field → None.
    assert canonical_enum_error(
        [{"type": "enum", "loc": ["body", "status"], "input": "weird"}]
    ) is None
    # Mapped field but non-enum error type → None (do not over-match).
    assert canonical_enum_error(
        [{"type": "string_type", "loc": ["body", "lane_type"], "input": 5}]
    ) is None
    # Empty / missing loc → None.
    assert canonical_enum_error([{"type": "enum", "loc": [], "input": "x"}]) is None
