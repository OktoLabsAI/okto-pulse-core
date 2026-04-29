"""Bug e62180c0 — MCP create_card / update_card swallowed corrupted card_type.

Before the fix:
  - okto_pulse_create_card accepted any string for card_type. Garbage like
    "boug" or "tasc" passed straight through to the DB, surfacing later as
    a CardType(...) ValueError on read.
  - severity had the same issue on both create and update.
  - Bug cards skipped cross-field requirements (origin_task_id, severity,
    expected_behavior, observed_behavior).

After the fix:
  - card_type is validated against CardType enum at the handler boundary.
  - severity (when provided) is validated against BugSeverity.
  - Bug cards reject create unless the four mandatory fields are populated.

These tests pin both the source-level validation block AND the runtime
behavior by invoking the handler with a fully wired async DB session.
"""

from __future__ import annotations

import inspect
import json
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.models.db import Board, BugSeverity, CardType, Spec, SpecStatus
from okto_pulse.core.mcp import server as mcp_server


BOARD_ID = "card-type-val-board-001"
USER_ID = "card-type-val-agent-001"


# ---------------------------------------------------------------------------
# Source contract — guard the validation block from regressing.
# ---------------------------------------------------------------------------

def _handler_block(name: str) -> str:
    """Slice the file source between the handler def and the next top-level def.

    Avoids inspect.getsource which returns the FastMCP decorator wrapper.
    """
    from pathlib import Path
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    marker = f"async def {name}("
    start = src.index(marker)
    rest = src[start + len(marker):]
    next_def = rest.find("\nasync def ")
    end = start + len(marker) + (next_def if next_def != -1 else len(rest))
    return src[start:end]


def test_create_card_source_validates_card_type():
    block = _handler_block("okto_pulse_create_card")
    assert "CardType(" in block, "create_card must validate against CardType enum"
    assert "Invalid card_type" in block


def test_create_card_source_validates_severity():
    block = _handler_block("okto_pulse_create_card")
    assert "BugSeverity(" in block
    assert "Invalid severity" in block


def test_create_card_source_enforces_bug_required_fields():
    block = _handler_block("okto_pulse_create_card")
    for field in ("origin_task_id", "severity", "expected_behavior", "observed_behavior"):
        assert field in block, f"bug-card requirement {field!r} must be enforced"
    assert "Bug cards require non-empty" in block


def test_update_card_source_validates_severity():
    block = _handler_block("okto_pulse_update_card")
    assert "BugSeverity(" in block
    assert "Invalid severity" in block


# ---------------------------------------------------------------------------
# Functional tests — invoke the handler against the real DB.
# ---------------------------------------------------------------------------

@pytest.fixture
async def _seed_board_and_spec():
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    async with db_factory() as db:
        if await db.get(Board, BOARD_ID) is None:
            db.add(Board(id=BOARD_ID, name="Card Type Validation", owner_id=USER_ID))
            await db.flush()

        spec_id = str(uuid.uuid4())
        db.add(Spec(
            id=spec_id,
            board_id=BOARD_ID,
            title="Spec for validation",
            status=SpecStatus.APPROVED,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        ))
        await db.commit()
        return spec_id


def _stub_ctx():
    """Mimic the AgentContext returned by _get_agent_ctx — minimum surface."""
    return type("Ctx", (), {
        "agent_id": USER_ID,
        "agent_name": "validator-test",
        "permissions": ["card.entity.create", "card.entity.create_test"],
    })()


async def _call_create_card(**kwargs) -> dict:
    """Resolve the registered FastMCP handler and invoke it directly."""
    from okto_pulse.core.infra.database import get_session_factory
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool("okto_pulse_create_card")
    fn = tool.fn  # underlying async function (after xml_safety + body)
    result_json = await fn(**kwargs)
    return json.loads(result_json)


@pytest.mark.asyncio
async def test_create_card_rejects_invalid_card_type(_seed_board_and_spec):
    spec_id = _seed_board_and_spec
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        payload = await _call_create_card(
            board_id=BOARD_ID,
            title="should fail",
            spec_id=spec_id,
            card_type="boug",  # garbage — typo
        )
    assert "error" in payload
    assert "Invalid card_type" in payload["error"]
    assert "boug" in payload["error"]


@pytest.mark.asyncio
async def test_create_card_rejects_invalid_severity(_seed_board_and_spec):
    spec_id = _seed_board_and_spec
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        payload = await _call_create_card(
            board_id=BOARD_ID,
            title="should fail",
            spec_id=spec_id,
            card_type="bug",
            severity="cataclysm",  # invalid
            origin_task_id="some-id",
            expected_behavior="x",
            observed_behavior="y",
        )
    assert "error" in payload
    assert "Invalid severity" in payload["error"]


@pytest.mark.asyncio
async def test_create_card_rejects_bug_missing_required_fields(_seed_board_and_spec):
    spec_id = _seed_board_and_spec
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        payload = await _call_create_card(
            board_id=BOARD_ID,
            title="bug missing fields",
            spec_id=spec_id,
            card_type="bug",
        )
    assert "error" in payload
    msg = payload["error"]
    assert "Bug cards require" in msg
    for field in ("origin_task_id", "severity", "expected_behavior", "observed_behavior"):
        assert field in msg


@pytest.mark.asyncio
async def test_create_card_accepts_valid_normal(_seed_board_and_spec):
    """Sanity check: validation does NOT break the happy path."""
    spec_id = _seed_board_and_spec
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        payload = await _call_create_card(
            board_id=BOARD_ID,
            title="happy normal card",
            spec_id=spec_id,
            card_type="normal",
        )
    assert payload.get("success") is True or "id" in payload or "card" in payload


# ---------------------------------------------------------------------------
# Enum sanity — pin the accepted values so future drift is caught.
# ---------------------------------------------------------------------------

def test_card_type_enum_values():
    assert {t.value for t in CardType} == {"normal", "bug", "test"}


def test_bug_severity_enum_values():
    assert {s.value for s in BugSeverity} == {"critical", "major", "minor"}
