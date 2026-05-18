"""Tests for the consolidated polymorphic list handlers (spec P0.B).

Coverage mandated by TR-B5:
1. Structure — new handlers return the expected consolidated payloads.
2. Structured error — invalid entity_type returns error_code='unsupported_entity'.
3. Filter validation — invalid filter key returns error_code='invalid_filter'.
4. Legacy entity-specific list tools are not registered.

Design
------
Tests operate on the pure Python functions (not over-the-wire MCP transport)
by calling the tool coroutines directly after injecting a session factory.
This keeps tests fast, hermetic, and free of network I/O while still
exercising the real service/DB layer.

The ``_db_init`` and ``_test_logging`` fixtures from conftest.py are session-
and function-scoped respectively; they apply automatically (autouse=True).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.models.db import (
    Agent,
    AgentBoard,
    Board,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOARD_ID = "consol-list-board-001"
AGENT_ID = "consol-agent-001"
API_KEY = "consol-api-key-001"

LEGACY_LIST_TOOLS = (
    "okto_pulse_list_topics",
    "okto_pulse_list_stories",
    "okto_pulse_list_ideations",
    "okto_pulse_list_ideation_snapshots",
    "okto_pulse_list_ideation_knowledge",
    "okto_pulse_list_ideation_qa",
    "okto_pulse_list_refinements",
    "okto_pulse_list_refinement_qa",
    "okto_pulse_list_specs",
    "okto_pulse_list_card_knowledge",
    "okto_pulse_list_spec_qa",
    "okto_pulse_list_spec_knowledge",
    "okto_pulse_list_refinement_snapshots",
    "okto_pulse_list_refinement_knowledge",
    "okto_pulse_list_sprints",
)

CONSOLIDATED_LIST_TOOLS = (
    "okto_pulse_list_by_board",
    "okto_pulse_list_qa",
    "okto_pulse_list_knowledge",
    "okto_pulse_list_snapshots",
)

def _get_tool_fn(name: str):
    """Unwrap FastMCP FunctionTool to its underlying async function."""
    import okto_pulse.core.mcp.server as _srv
    tool = _srv.mcp._tool_manager._tools.get(name)
    if tool is None:
        raise KeyError(f"Tool not registered: {name}")
    return tool.fn


async def _call_tool(name: str, **kwargs) -> str:
    """Call a registered MCP tool by name, bypassing FunctionTool wrapper."""
    return await _get_tool_fn(name)(**kwargs)




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse(result: str) -> dict:
    """Parse JSON response from an MCP tool call."""
    return json.loads(result)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_factory():
    """Return the session factory wired up in conftest._db_init."""
    from okto_pulse.core.infra.database import get_session_factory
    return get_session_factory()


@pytest.fixture(scope="module")
async def seeded_board(db_factory):
    """Create a minimal board with one of every entity type we need.

    Returns a dict of IDs for use in test assertions.
    """
    from sqlalchemy import select

    ideation_id = str(uuid.uuid4())
    refinement_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())

    async with db_factory() as db:
        # Skip if already seeded
        existing = (
            await db.execute(select(Board).where(Board.id == BOARD_ID))
        ).scalar_one_or_none()
        if existing is not None:
            return {
                "board_id": BOARD_ID,
                "agent_id": AGENT_ID,
                "ideation_id": ideation_id,
                "refinement_id": refinement_id,
                "spec_id": spec_id,
            }

        import copy
        from okto_pulse.core.services.main import AgentService
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY
        db.add(Board(id=BOARD_ID, name="Consolidated List Test Board", owner_id="owner-1"))
        db.add(Agent(
            id=AGENT_ID,
            name="test-agent",
            board_id=BOARD_ID,
            api_key=API_KEY,
            api_key_hash=AgentService.hash_api_key(API_KEY),
            permission_flags=copy.deepcopy(PERMISSION_REGISTRY),
            is_active=True,
            created_by="system",
        ))
        db.add(AgentBoard(
            agent_id=AGENT_ID,
            board_id=BOARD_ID,
            granted_by="system",
        ))
        db.add(Ideation(
            id=ideation_id,
            board_id=BOARD_ID,
            title="Test ideation",
            description="desc",
            status=IdeationStatus.DRAFT,
            created_by=AGENT_ID,
        ))
        db.add(Refinement(
            id=refinement_id,
            ideation_id=ideation_id,
            board_id=BOARD_ID,
            title="Test refinement",
            description="desc",
            status=RefinementStatus.DRAFT,
            created_by=AGENT_ID,
        ))
        db.add(Spec(
            id=spec_id,
            board_id=BOARD_ID,
            title="Test spec",
            description="desc",
            status=SpecStatus.DRAFT,
            created_by=AGENT_ID,
        ))
        await db.commit()

    return {
        "board_id": BOARD_ID,
        "agent_id": AGENT_ID,
        "ideation_id": ideation_id,
        "refinement_id": refinement_id,
        "spec_id": spec_id,
    }


@pytest.fixture(autouse=True)
def _wire_mcp_session(db_factory):
    """Inject the session factory into the MCP server before each test."""
    from okto_pulse.core.mcp.server import register_session_factory
    register_session_factory(db_factory)


@pytest.fixture(autouse=True)
def _set_api_key():
    """Set the MCP active API key so _get_agent_ctx() can resolve the agent."""
    from okto_pulse.core.mcp.server import _active_api_key
    token = _active_api_key.set(API_KEY)
    yield
    _active_api_key.reset(token)


# ---------------------------------------------------------------------------
# 1. validate_filters() unit tests (filters.py)
# ---------------------------------------------------------------------------


def test_validate_filters_empty_ok():
    from okto_pulse.core.mcp.filters import validate_filters
    ok, err = validate_filters("spec", {}, scope="by_board")
    assert ok is True
    assert err is None


def test_validate_filters_none_ok():
    from okto_pulse.core.mcp.filters import validate_filters
    ok, err = validate_filters("spec", None, scope="by_board")  # type: ignore[arg-type]
    assert ok is True
    assert err is None


def test_validate_filters_valid_key_ok():
    from okto_pulse.core.mcp.filters import validate_filters
    ok, err = validate_filters("spec", {"status": "draft"}, scope="by_board")
    assert ok is True
    assert err is None


def test_validate_filters_invalid_key_rejected():
    from okto_pulse.core.mcp.filters import validate_filters
    ok, err = validate_filters("spec", {"invalid_key": "x"}, scope="by_board")
    assert ok is False
    assert err is not None
    assert "invalid_key" in err


def test_validate_filters_unknown_scope_rejects_all():
    from okto_pulse.core.mcp.filters import validate_filters
    ok, err = validate_filters("spec", {"status": "draft"}, scope="unknown_scope")
    assert ok is False


def test_supported_entity_types():
    from okto_pulse.core.mcp.filters import supported_entity_types
    types_by_board = supported_entity_types("by_board")
    assert "spec" in types_by_board
    assert "ideation" in types_by_board
    assert "refinement" in types_by_board
    assert "sprint" in types_by_board
    assert "story" in types_by_board
    assert "topic" in types_by_board


# ---------------------------------------------------------------------------
# 2. _structured_error() unit tests (helpers.py)
# ---------------------------------------------------------------------------


def test_structured_error_basic():
    from okto_pulse.core.mcp.helpers import _structured_error
    result = _structured_error("unsupported_entity", ["spec", "ideation"], None, "bad entity")
    data = json.loads(result)
    assert data["error_code"] == "unsupported_entity"
    assert data["supported"] == ["spec", "ideation"]
    assert "error" in data  # legacy compat key
    assert "suggested_tool" not in data  # None → omitted


def test_structured_error_with_suggested_tool():
    from okto_pulse.core.mcp.helpers import _structured_error
    result = _structured_error("invalid_filter", [], "okto_pulse_list_by_board", "bad filter")
    data = json.loads(result)
    assert data["suggested_tool"] == "okto_pulse_list_by_board"
    assert data["error_code"] == "invalid_filter"


# ---------------------------------------------------------------------------
# 3. Structured error: invalid entity_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_unsupported_entity():
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="invalid_type",
    )
    data = _parse(result)
    assert data.get("error_code") == "unsupported_entity"
    assert "invalid_type" in data.get("error", "") or "invalid_type" in data.get("detail", "")
    assert "supported" in data
    assert "spec" in data["supported"]


@pytest.mark.asyncio
async def test_list_qa_unsupported_entity():
    result = await _call_tool("okto_pulse_list_qa",
        board_id=BOARD_ID,
        entity_type="sprint",
        entity_id="some-id",
    )
    data = _parse(result)
    assert data.get("error_code") == "unsupported_entity"


@pytest.mark.asyncio
async def test_list_knowledge_unsupported_entity():
    result = await _call_tool("okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="topic",
        entity_id="some-id",
    )
    data = _parse(result)
    assert data.get("error_code") == "unsupported_entity"


@pytest.mark.asyncio
async def test_list_snapshots_unsupported_entity():
    result = await _call_tool("okto_pulse_list_snapshots",
        board_id=BOARD_ID,
        entity_type="spec",
        entity_id="some-id",
    )
    data = _parse(result)
    assert data.get("error_code") == "unsupported_entity"


# ---------------------------------------------------------------------------
# 4. Filter validation: invalid filter key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_invalid_filter_key():
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
        filters={"invalid_key": "x"},
    )
    data = _parse(result)
    assert data.get("error_code") == "invalid_filter"


@pytest.mark.asyncio
async def test_list_knowledge_invalid_filter_key():
    result = await _call_tool("okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="spec",
        entity_id="some-id",
        filters={"invalid_key": "x"},
    )
    data = _parse(result)
    assert data.get("error_code") == "invalid_filter"


# ---------------------------------------------------------------------------
# 4b. Bug regression — filters accepts JSON string (post-release fix eb782ae4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_accepts_filters_as_json_string():
    """Regression for bug eb782ae4: filters passed as JSON string must be
    auto-decoded by the handler. MCP transports often serialise complex
    parameters as strings."""
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
        filters='{"status": "draft"}',
    )
    data = _parse(result)
    # The handler must not return a Pydantic dict_type validation error.
    # It may return items, an empty list, or a permission/auth error —
    # but NOT 'Input should be a valid dictionary'.
    assert "Input should be a valid dictionary" not in str(data)
    # If it returns a structured error, it should NOT be about filter shape.
    if data.get("error_code"):
        assert data["error_code"] != "invalid_filter" or "Invalid JSON" not in data.get("detail", "")


@pytest.mark.asyncio
async def test_list_by_board_rejects_malformed_filters_json():
    """Regression for bug eb782ae4: an invalid JSON string must produce a
    structured_error with error_code='invalid_filter', not crash Pydantic."""
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
        filters='{not valid json',
    )
    data = _parse(result)
    assert data.get("error_code") == "invalid_filter"
    assert "Invalid JSON" in data.get("detail", "") or "Invalid JSON" in data.get("error", "")


@pytest.mark.asyncio
async def test_list_qa_accepts_filters_as_json_string():
    """Regression for bug eb782ae4: list_qa must also accept filters as JSON string."""
    result = await _call_tool("okto_pulse_list_qa",
        board_id=BOARD_ID,
        entity_type="spec",
        entity_id="some-id",
        filters='{"asked_by": "anyone"}',
    )
    data = _parse(result)
    assert "Input should be a valid dictionary" not in str(data)


@pytest.mark.asyncio
async def test_list_knowledge_accepts_filters_as_json_string():
    """Regression for bug eb782ae4: list_knowledge must also accept filters as JSON string."""
    result = await _call_tool("okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="spec",
        entity_id="some-id",
        filters='{"mime_type": "text/markdown"}',
    )
    data = _parse(result)
    assert "Input should be a valid dictionary" not in str(data)


@pytest.mark.asyncio
async def test_list_by_board_accepts_empty_string_filters():
    """Regression for bug eb782ae4: an empty/whitespace filters string must be
    treated as no filter, not as a JSON parse error."""
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
        filters="   ",
    )
    data = _parse(result)
    assert "Invalid JSON" not in str(data)
    assert "Input should be a valid dictionary" not in str(data)


# ---------------------------------------------------------------------------
# 4c. Bug regression — consolidated limit default is 100 (post-release fix 717e9915)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_spec_default_limit_is_100(seeded_board):
    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
    )
    data = _parse(result)
    assert data.get("limit") == 100, f"Expected limit=100, got {data.get('limit')}"


@pytest.mark.asyncio
async def test_list_by_board_ideation_default_limit_is_100(seeded_board):
    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="ideation",
    )
    data = _parse(result)
    assert data.get("limit") == 100, f"Expected limit=100, got {data.get('limit')}"


# ---------------------------------------------------------------------------
# 5. Structure — consolidated list_by_board payloads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_spec_structure(seeded_board):
    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
    )
    data = _parse(result)

    assert data.get("entity_type") == "spec"
    assert "items" in data, f"handler missing 'items': {data}"
    if data["items"]:
        expected_keys = {
            "id",
            "title",
            "description",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        }
        assert expected_keys <= set(data["items"][0].keys())


@pytest.mark.asyncio
async def test_list_by_board_ideation_structure(seeded_board):
    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="ideation",
    )
    data = _parse(result)

    assert data.get("entity_type") == "ideation"
    assert "items" in data
    if data["items"]:
        expected_keys = {
            "id",
            "title",
            "description",
            "problem_statement",
            "complexity",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        }
        assert expected_keys <= set(data["items"][0].keys())


@pytest.mark.asyncio
async def test_list_by_board_refinement_requires_ideation_id(seeded_board):
    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="refinement",
    )
    data = _parse(result)
    assert data.get("error_code") == "missing_required_filter"


@pytest.mark.asyncio
async def test_list_by_board_refinement_with_ideation_id(seeded_board):
    ideation_id = seeded_board["ideation_id"]

    result = await _call_tool(
        "okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="refinement",
        filters={"ideation_id": ideation_id},
    )
    data = _parse(result)

    assert data.get("entity_type") == "refinement"
    assert data.get("ideation_id") == ideation_id
    assert "items" in data, f"handler missing 'items': {data}"
    if data["items"]:
        expected_keys = {
            "id",
            "title",
            "description",
            "in_scope",
            "out_of_scope",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        }
        assert expected_keys <= set(data["items"][0].keys())


# ---------------------------------------------------------------------------
# 6. Legacy entity-specific list tools are removed from the MCP registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_list_tools_are_not_registered():
    import okto_pulse.core.mcp.server as srv

    tools = await srv.mcp.get_tools()
    registered_legacy_tools = set(LEGACY_LIST_TOOLS) & set(tools.keys())
    assert registered_legacy_tools == set()


@pytest.mark.asyncio
async def test_consolidated_list_tools_are_registered():
    import okto_pulse.core.mcp.server as srv

    tools = await srv.mcp.get_tools()
    assert set(CONSOLIDATED_LIST_TOOLS) <= set(tools.keys())


# ---------------------------------------------------------------------------
# 7. Deprecated list logging was removed with the legacy tools
# ---------------------------------------------------------------------------


def test_deprecated_list_logging_helpers_are_absent():
    import okto_pulse.core.mcp.server as srv

    assert not hasattr(srv, "_log_deprecated_invocation")
    assert not hasattr(srv, "_DEPRECATION_SAMPLE_RATE")


# ---------------------------------------------------------------------------
# 8. okto_pulse_list_by_board: valid filters pass through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_valid_status_filter(seeded_board):
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="spec",
        filters={"status": "draft"},
    )
    data = _parse(result)
    # No error — valid filter key
    assert "error_code" not in data
    assert "items" in data


@pytest.mark.asyncio
async def test_list_by_board_sprint_requires_spec_id():
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="sprint",
    )
    data = _parse(result)
    assert data.get("error_code") == "missing_required_filter"


# ---------------------------------------------------------------------------
# 9. New handlers return entity_type in response
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_by_board_returns_entity_type(seeded_board):
    result = await _call_tool("okto_pulse_list_by_board",
        board_id=BOARD_ID,
        entity_type="ideation",
    )
    data = _parse(result)
    assert data.get("entity_type") == "ideation"


@pytest.mark.asyncio
async def test_list_snapshots_ideation(seeded_board):
    result = await _call_tool("okto_pulse_list_snapshots",
        board_id=BOARD_ID,
        entity_type="ideation",
        entity_id=seeded_board["ideation_id"],
    )
    data = _parse(result)
    assert "error_code" not in data
    assert data.get("entity_type") == "ideation"
    assert "snapshots" in data


@pytest.mark.asyncio
async def test_list_snapshots_refinement(seeded_board):
    result = await _call_tool("okto_pulse_list_snapshots",
        board_id=BOARD_ID,
        entity_type="refinement",
        entity_id=seeded_board["refinement_id"],
    )
    data = _parse(result)
    assert "error_code" not in data
    assert data.get("entity_type") == "refinement"
    assert "snapshots" in data


# ---------------------------------------------------------------------------
# 10. TS-b624b4d8: agent_instructions.md has no primary refs to old tools
# ---------------------------------------------------------------------------


def test_agent_instructions_do_not_reference_legacy_list_tools():
    """The agent-facing instructions must only expose consolidated list tools."""
    ai_path = (
        Path(__file__).resolve().parent.parent
        / "src" / "okto_pulse" / "core" / "mcp" / "agent_instructions.md"
    )
    assert ai_path.exists(), f"agent_instructions.md not found at {ai_path}"
    text = ai_path.read_text(encoding="utf-8")

    leaked = [tool for tool in LEGACY_LIST_TOOLS if tool in text]
    assert leaked == []
