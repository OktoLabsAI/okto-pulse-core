"""Spec #09 card 77188b6f — inbound adapter contracts + GoldenInboundReplay.

Proves the first-cut migration is behavior-preserving by comparing, per flow,
the BEFORE path (the pre-migration direct-service logic) against the AFTER path
(the migrated thin REST handler / MCP tool) field-by-field via
``GoldenInboundReplay`` — not a loose snapshot. Also unit-tests the harness and
that the adapter contracts are thin (build ActorContext + map errors only).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.community.api.boards import create_board as rest_create_board
from okto_pulse.community.api.ideations import move_ideation as rest_move_ideation
from okto_pulse.core.application.golden_inbound_replay import (
    FlowOutcome,
    GoldenInboundReplay,
    capture_outcome,
)
from okto_pulse.core.application.use_cases import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
)
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.community.inbound.rest_adapter import RESTAdapterContract
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWork
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.models import BoardCreate, IdeationMove
from sqlalchemy_test_models import Board, Ideation, IdeationStatus
from okto_pulse.core.services import BoardService
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.main import AmbiguityGateError
from okto_pulse.core.services.resource_gate import ResourceGateError

from fastapi import HTTPException

USER_ID = "inbound09-user"
AGENT_NAME = "inbound09-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _wrap_uow(db):
    return resolve_unit_of_work_factory().wrap(db)


# --------------------------------------------------------------------------- #
# MCP harness (mirrors tests/test_ambiguity_gate_skip_mcp_e2e.py)
# --------------------------------------------------------------------------- #


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": AGENT_NAME, "permissions": None},
    )()


async def _call_tool(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.fixture
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())):
        yield


# --------------------------------------------------------------------------- #
# normalizers (drop volatile fields so before/after compare on the contract)
# --------------------------------------------------------------------------- #


def _norm_board(board) -> dict:
    attached = getattr(board, "values", None)
    if attached is None:
        attached = board.__dict__
    return {
        "name": board.name,
        "owner_id": board.owner_id,
        "agents": attached.get("agents"),
        "settings_present": attached.get("settings") is not None,
    }


def _norm_ideation(ideation) -> dict:
    status = ideation.status
    return {"status": getattr(status, "value", status)}


# --------------------------------------------------------------------------- #
# GoldenInboundReplay harness unit tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_replay_reports_equivalence_and_deltas():
    async def ok_runner():
        return {"a": 1, "b": 2}

    before = await capture_outcome(ok_runner, side_effects={"committed": 1})
    after = await capture_outcome(ok_runner, side_effects={"committed": 1})
    report = GoldenInboundReplay("flow", "rest").compare(before, after)
    assert report.equivalent
    assert report.deltas == []

    # A differing payload is a delta...
    other = FlowOutcome(ok=True, payload={"a": 1, "b": 99}, side_effects={"committed": 1})
    delta_report = GoldenInboundReplay("flow", "rest").compare(before, other)
    assert not delta_report.equivalent
    assert any(d.field == "payload" for d in delta_report.deltas)

    # ...unless explicitly accepted.
    accepted = GoldenInboundReplay("flow", "rest", accepted_deltas=("payload",)).compare(
        before, other
    )
    assert accepted.equivalent


@pytest.mark.asyncio
async def test_capture_outcome_captures_errors():
    async def boom():
        raise ValueError("nope")

    outcome = await capture_outcome(boom)
    assert outcome.ok is False
    assert outcome.error_type == "ValueError"
    assert outcome.error_message == "nope"


# --------------------------------------------------------------------------- #
# Adapter contracts are thin: actor builders + error mapping only
# --------------------------------------------------------------------------- #


def test_rest_adapter_actor():
    actor = RESTAdapterContract.actor("u1", realm_id="r1", board_id="b1")
    assert isinstance(actor, ActorContext)
    assert (actor.actor_id, actor.source, actor.realm_id, actor.board_id) == (
        "u1",
        "rest",
        "r1",
        "b1",
    )
    assert actor.actor_name is None  # REST resolves the name downstream


def test_rest_adapter_http_error_mapping():
    assert RESTAdapterContract.http_error(CommandValidationError("bad")).status_code == 400
    nf = RESTAdapterContract.http_error(
        EntityNotFoundError("spec", "s1"), not_found_detail="Spec not found"
    )
    assert nf.status_code == 404 and nf.detail == "Spec not found"
    assert RESTAdapterContract.http_error(AmbiguityGateError("amb")).status_code == 400
    rge = RESTAdapterContract.http_error(
        ResourceGateError("some_code", "blocked", details={"x": 1})
    )
    assert rge.status_code == 409
    assert rge.detail == {"error": "some_code", "message": "blocked", "details": {"x": 1}}
    assert RESTAdapterContract.http_error(ValueError("v")).status_code == 409
    # Not an error this contract owns -> re-raised, not silently swallowed.
    with pytest.raises(KeyError):
        RESTAdapterContract.http_error(KeyError("k"))


def test_mcp_adapter_actor_and_error():
    actor = MCPAdapterContract.actor(_stub_ctx(), board_id="b1")
    assert actor.source == "mcp"
    assert actor.actor_id == USER_ID
    assert actor.actor_name == AGENT_NAME  # MCP agent name preserved for audit
    assert actor.board_id == "b1"
    assert MCPAdapterContract.error(ValueError("boom")) == json.dumps({"error": "boom"})


# --------------------------------------------------------------------------- #
# REST create_board: BEFORE (direct service) vs AFTER (migrated handler)
# --------------------------------------------------------------------------- #


async def _before_create_board(db, *, name: str) -> object:
    service = BoardService(db)
    board = await service.create_board(USER_ID, BoardCreate(name=name), realm_id=None)
    await db.commit()
    board = await service.get_board(board.id)
    attached = getattr(board, "attach", None)
    values = {
        "agents": [],
        "settings": BoardGovernanceService.normalize_settings(
            getattr(board, "settings", None)
        ),
    }
    if callable(attached):
        for key, value in values.items():
            attached(key, value)
    else:
        board.__dict__.update(values)
    return board


@pytest.mark.asyncio
async def test_create_board_rest_equivalent(db_factory):
    name = "Equivalence Board 09"
    async with db_factory() as db:
        before = await capture_outcome(
            lambda: _before_create_board(db, name=name), normalize=_norm_board
        )
    async with db_factory() as db:
        after = await capture_outcome(
            # create_board migrated (spec #04) to a request-scoped PulseUnitOfWork;
            # the handler now takes `uow` instead of a raw `db`.
            lambda: rest_create_board(
                data=BoardCreate(name=name),
                user_id=USER_ID,
                realm_id=None,
                uow=SQLAlchemyUnitOfWork(db),
            ),
            normalize=_norm_board,
        )
    report = GoldenInboundReplay("create_board", "rest_only").compare(before, after)
    assert report.equivalent, report.as_dict()
    assert after.payload["name"] == name
    assert after.payload["agents"] == []
    assert after.payload["settings_present"] is True


# --------------------------------------------------------------------------- #
# REST move_ideation: BEFORE vs AFTER on parallel identical ideations
# --------------------------------------------------------------------------- #


async def _seed_board_with_ideations(db, n: int) -> tuple[str, list[str]]:
    board_id = _id("board")
    db.add(Board(id=board_id, name="Inbound09", owner_id=USER_ID, settings={}))
    ids = []
    for _ in range(n):
        iid = _id("idea")
        db.add(
            Ideation(
                id=iid,
                board_id=board_id,
                title="Inbound09 ideation",
                created_by=USER_ID,
                status=IdeationStatus.DRAFT,
            )
        )
        ids.append(iid)
    await db.commit()
    return board_id, ids


@pytest.mark.asyncio
async def test_move_ideation_rest_equivalent(db_factory):
    async with db_factory() as db:
        _, (id_before, id_after) = await _seed_board_with_ideations(db, 2)

    async def _before():
        async with db_factory() as db:
            uow = _wrap_uow(db)
            service = uow.services.ideations
            await service.move_ideation(
                id_before, USER_ID, IdeationMove(status=IdeationStatus.REVIEW)
            )
            await uow.commit()
            return await service.get_ideation(id_before)

    async def _after():
        async with db_factory() as db:
            return await rest_move_ideation(
                ideation_id=id_after,
                data=IdeationMove(status=IdeationStatus.REVIEW),
                user_id=USER_ID,
                uow=_wrap_uow(db),
            )

    before = await capture_outcome(_before, normalize=_norm_ideation)
    after = await capture_outcome(_after, normalize=_norm_ideation)
    report = GoldenInboundReplay("move_ideation", "rest").compare(before, after)
    assert report.equivalent, report.as_dict()
    assert after.payload["status"] == IdeationStatus.REVIEW.value


@pytest.mark.asyncio
async def test_move_ideation_rest_not_found_is_404(db_factory):
    async with db_factory() as db:
        with pytest.raises(HTTPException) as exc_info:
            await rest_move_ideation(
                ideation_id="missing-09",
                data=IdeationMove(status=IdeationStatus.REVIEW),
                user_id=USER_ID,
                uow=_wrap_uow(db),
            )
    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Ideation not found"


# --------------------------------------------------------------------------- #
# MCP move_ideation: migrated tool preserves the compact payload + envelopes
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_mcp_move_ideation_payload_and_envelopes(db_factory, _stub_auth):
    async with db_factory() as db:
        board_id, (ideation_id,) = await _seed_board_with_ideations(db, 1)

    ok = await _call_tool(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id=ideation_id,
        status="review",
    )
    assert ok == {
        "success": True,
        "ideation_id": ideation_id,
        "from_status": "draft",
        "to_status": "review",
    }

    not_found = await _call_tool(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id="missing-09",
        status="review",
    )
    assert not_found == {"error": "Ideation not found"}

    bad_status = await _call_tool(
        "okto_pulse_move_ideation",
        board_id=board_id,
        ideation_id=ideation_id,
        status="not-a-status",
    )
    assert "error" in bad_status and "Invalid status" in bad_status["error"]


# --------------------------------------------------------------------------- #
# MCP submit_spec_validation: missing spec surfaces the JSON error envelope
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_mcp_submit_spec_validation_missing_spec_envelope(db_factory, _stub_auth):
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Inbound09", owner_id=USER_ID, settings={}))
        await db.commit()

    result = await _call_tool(
        "okto_pulse_submit_spec_validation",
        board_id=board_id,
        spec_id="missing-spec-09",
        completeness=95,
        completeness_justification="complete enough",
        assertiveness=95,
        assertiveness_justification="assertive enough",
        ambiguity=5,
        ambiguity_justification="low ambiguity",
        general_justification="general justification well over twenty chars",
        recommendation="approve",
    )
    # Inline MCP validation passes; the use case pre-check raises EntityNotFoundError
    # which the tool maps to the JSON error envelope (uniform not-found wording).
    assert result == {"error": "Spec not found"}
