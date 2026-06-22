"""R5-IMP4 (card 994b370d) — agent-facing docstrings / remediation / error texts
never instruct an agent to apply or clear a skip (AC4).

Every agent-facing surface that touches skip/no_action must frame it as a HUMAN
decision (authorized UI/REST) and tell the agent to surface read-only state +
request a human decision — never to apply the skip itself.

Anti-test-theater: the gate remediation text is asserted from the REAL
AmbiguityGateError raised by ``move_ideation``; the human_control_required message
is read from the REAL MCP tool return; the tool docstrings are read from the live
registered MCP tools.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Ideation, IdeationStatus
from okto_pulse.core.models.schemas import IdeationMove
from okto_pulse.core.services.main import AmbiguityGateError, IdeationService

ACTOR = "r5-imp4-actor"

# Imperative phrasings that would tell an AGENT to apply/clear a skip itself.
_AGENT_SKIP_IMPERATIVES = (
    "or skip this ideation",
    "skip this gate for the ideation",
    "use this to execute the 'skip this ideation' remediation",
    "set the per-ideation max ambiguity gate skip override.",
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _no_agent_skip_imperative(text: str) -> None:
    low = text.lower()
    for phrase in _AGENT_SKIP_IMPERATIVES:
        assert phrase not in low, f"agent-skip imperative leaked: {phrase!r}"


# ===========================================================================
# Ambiguity gate remediation (REAL AmbiguityGateError) — human-only framing
# ===========================================================================


async def _seed(db, *, ambiguity):
    board_id, ideation_id = _id("board"), _id("idea")
    db.add(Board(id=board_id, name="Gate", owner_id=ACTOR,
                 settings={"require_ideation_ambiguity_gate": True,
                           "max_ideation_ambiguity": 3}))
    db.add(Ideation(id=ideation_id, board_id=board_id, title="g", created_by=ACTOR,
                    status=IdeationStatus.EVALUATING,
                    scope_assessment=(None if ambiguity is None else {"ambiguity": ambiguity})))
    await db.flush()
    return ideation_id


@pytest.mark.asyncio
async def test_ambiguity_gate_unevaluated_remediation_is_human_only(db_factory):
    async with db_factory() as db:
        ideation_id = await _seed(db, ambiguity=None)
        with pytest.raises(AmbiguityGateError) as exc:
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE))
    msg = str(exc.value)
    # Legacy substring preserved (so other gate tests stay green) ...
    assert "ambiguity has not been evaluated" in msg
    # ... and the remediation now frames skip as a HUMAN decision, not an agent action.
    assert "human" in msg.lower()
    _no_agent_skip_imperative(msg)


@pytest.mark.asyncio
async def test_ambiguity_gate_over_threshold_remediation_is_human_only(db_factory):
    async with db_factory() as db:
        ideation_id = await _seed(db, ambiguity=4)
        with pytest.raises(AmbiguityGateError) as exc:
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE))
    msg = str(exc.value)
    assert "ambiguity score 4 exceeds configured max 3" in msg
    assert "human" in msg.lower()
    # Agent-doable remediations stay (reduce ambiguity / raise threshold) ...
    assert "reduce ambiguity" in msg.lower()
    # ... but the skip is framed as human-only, not an agent imperative.
    _no_agent_skip_imperative(msg)


# ===========================================================================
# human_control_required message (REAL MCP return) — requests a human decision
# ===========================================================================


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 imp4 agent"
        self.permissions = set()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


@pytest.mark.asyncio
async def test_human_control_required_message_requests_human_decision(db_factory):
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp4", owner_id=ACTOR))
        await db.commit()

    out = await _call("okto_pulse_kg_record_cognitive_skip",
                      board_id=board_id, source_ref="bug:b1", reason_code="trivial_fix")
    msg = out["message"]
    assert out["code"] == "human_control_required"
    # The message asks for a human decision and never tells the agent to do the skip.
    assert "human" in msg.lower()
    assert re.search(r"escalate to a human|human (decision|operator)", msg.lower())
    # No bare imperative like "apply the skip" / "record the skip" directed at the agent.
    assert "must not" in msg.lower() or "human-only" in msg.lower() or "human only" in msg.lower()


# ===========================================================================
# Agent-facing docstrings — human-only language, no agent-skip instruction
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", [
    "okto_pulse_set_ideation_ambiguity_gate_skip",
    "okto_pulse_kg_record_cognitive_skip",
    "okto_pulse_kg_clear_cognitive_skip",
    "okto_pulse_kg_evaluate_bug_cognitive_closure",
])
async def test_blocked_skip_tool_docstrings_are_human_only(tool_name):
    tool = await mcp_server.mcp.get_tool(tool_name)
    doc = tool.fn.__doc__ or ""
    assert "human" in doc.lower(), tool_name
    _no_agent_skip_imperative(doc)


@pytest.mark.asyncio
async def test_evaluate_bug_docstring_flags_skip_as_human_only():
    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_evaluate_bug_cognitive_closure")
    doc = (tool.fn.__doc__ or "").lower()
    # The escape hatch doc must say skip/no_action is human-only; evaluate/create_learning stay.
    assert "human_control_required" in doc
    assert "create_learning" in doc and "evaluate" in doc


# ===========================================================================
# tool-docs/ideation.md — the skip section is documented human-only
# ===========================================================================


def test_ideation_tool_doc_skip_section_is_human_only():
    doc_path = (
        Path(__file__).resolve().parents[1]
        / "src" / "okto_pulse" / "core" / "mcp" / "resources" / "reference"
        / "tool-docs" / "ideation.md"
    )
    text = doc_path.read_text(encoding="utf-8")
    # Locate the skip section.
    marker = "## `okto_pulse_set_ideation_ambiguity_gate_skip`"
    assert marker in text
    section = text.split(marker, 1)[1].split("\n## ", 1)[0]
    assert "human_control_required" in section
    assert "human-only" in section.lower() or "human decision" in section.lower()
    _no_agent_skip_imperative(section)
