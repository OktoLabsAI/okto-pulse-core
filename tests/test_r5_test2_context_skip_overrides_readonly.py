"""R5-TEST2 (card ca64e685) — MCP contexts expose the skip-override read-model
read-only, and the skip remediation is human-only (no agent skip instruction).

* ts_62759ca4: okto_pulse_get_ideation_context / okto_pulse_get_spec_context expose
  ``skip_overrides`` with gate / effective_state / source / applied_by / applied_at /
  reason_code / justification / evidence_refs / revisit_at (when present) and
  ``mutation_allowed=false``.
* ts_af750802: the override block carries NO agent-mutation affordance, and a real
  agent skip attempt is refused with ``human_control_required`` (requests a human
  decision). Docstring/remediation human-only framing is covered in depth by
  R5-IMP4 (test_r5_imp4_human_only_remediation_text.py).

Anti-test-theater: the skip state is created through the REAL human write paths
(IdeationService.set_ambiguity_gate_skip + a real cognitive ledger record); the
``skip_overrides`` block is read back from the REAL MCP context tools.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import os
import sys
import tempfile
import uuid
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5t2_"))

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.main import IdeationService
from okto_pulse.core.services.skip_overrides import GATE_AMBIGUITY, GATE_COGNITIVE

USER_ID = "r5-test2-human"
UUID_A = "aaaaaaaa-2222-4222-8222-aaaaaaaaaaaa"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 test2 agent"
        # Full Spec context includes the read-only Code Traceability projection;
        # grant that independent read permission so this test reaches the skip
        # override projection it is intended to verify.
        self.permissions = (
            "code_traceability.investigation.read",
            "code_traceability.evidence.read",
            "code_traceability.target.read",
            "code_traceability.overlap.read",
        )


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


def _seed_cognitive_skip(board, gen, *, source_ref, reason_code, justification,
                         evidence_refs, revisit_at=None, actor="agent:human-reviewer"):
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    item = {
        "item_id": compute_cognitive_item_id(board, gen, source_ref),
        "board_id": board, "kg_generation_id": gen, "source_ref": source_ref,
        "artifact_type": source_ref.split(":", 1)[0],
        "status": CognitiveItemStatus.SKIPPED.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "updated_at": "2026-06-17T01:00:00+00:00",
        "reason_code": reason_code, "justification": justification,
        "evidence_refs": list(evidence_refs), "actor": actor,
    }
    if revisit_at is not None:
        item["revisit_at"] = revisit_at
    record = {"board_id": board, "kg_generation_id": gen,
              "pending_count": 0, "pending_refs": [], "status": "complete",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    store.artifact_store.write_json_atomic(store._record_key(board, gen), record)


# ===========================================================================
# ts_62759ca4 — ideation context exposes the ambiguity skip override read-only
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_62759ca4_ideation_context_exposes_ambiguity_skip_override(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test2", owner_id=USER_ID,
                     settings={"require_ideation_ambiguity_gate": True,
                               "max_ideation_ambiguity": 3}))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()
    # Human applies the skip through the REAL write path.
    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id,
            USER_ID,
            True,
            reason="Accepted for this validation edition.",
            expected_ideation_version=1,
            expected_ideation_edition=1,
            source="rest",
        )
        await db.commit()

    ctx = await _call("okto_pulse_get_ideation_context", board_id=board_id,
                      ideation_id=ideation_id)
    assert "skip_overrides" in ctx, ctx
    overrides = ctx["skip_overrides"]
    assert len(overrides) == 1, overrides
    o = overrides[0]
    assert o["gate"] == GATE_AMBIGUITY
    assert o["effective_state"] == "active"
    assert o["source"].endswith(":rest")
    assert o["applied_by"] == USER_ID
    assert o["applied_at"] is not None
    assert o["mutation_allowed"] is False
    assert o["target_ref"] == f"ideation:{ideation_id}"
    # ambiguity skip carries no reason_code/justification/evidence.
    assert o["reason_code"] is None and o["justification"] is None
    assert o["evidence_refs"] == []


# ===========================================================================
# ts_62759ca4 — spec context exposes the cognitive skip override read-only
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_62759ca4_spec_context_exposes_cognitive_skip_override(db_factory, _tmp_rebuild_dir):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    _seed_cognitive_skip(board_id, "gen-1", source_ref=f"card:{card_id}",
                         reason_code="duplicate_bug", justification="dup of X",
                         evidence_refs=("card:other",), revisit_at=None)
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test2", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="s", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="c",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()

    ctx = await _call("okto_pulse_get_spec_context", board_id=board_id,
                      spec_id=spec_id, profile="full")
    assert "skip_overrides" in ctx, ctx
    overrides = ctx["skip_overrides"]
    assert len(overrides) == 1, overrides
    o = overrides[0]
    assert o["gate"] == GATE_COGNITIVE
    assert o["effective_state"] == "active"
    assert o["reason_code"] == "duplicate_bug"
    assert o["justification"] == "dup of X"
    assert o["evidence_refs"] == ["card:other"]
    assert o["applied_by"] == "agent:human-reviewer"
    assert o["applied_at"] == "2026-06-17T01:00:00+00:00"
    assert o["mutation_allowed"] is False
    assert o["target_ref"] == f"card:{card_id}"
    assert "revisit_at" in o


# ===========================================================================
# ts_62759ca4 — empty (present but read-only []) when no skip
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_62759ca4_contexts_skip_overrides_empty_when_none(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    spec_id = _id("spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test2", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        db.add(Spec(id=spec_id, board_id=board_id, title="s", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        await db.commit()

    idea_ctx = await _call("okto_pulse_get_ideation_context", board_id=board_id,
                           ideation_id=ideation_id)
    spec_ctx = await _call("okto_pulse_get_spec_context", board_id=board_id,
                           spec_id=spec_id, profile="full")
    assert idea_ctx["skip_overrides"] == []
    assert "skip_overrides" in spec_ctx, spec_ctx
    assert spec_ctx["skip_overrides"] == []


# ===========================================================================
# ts_af750802 — the override block is read-only; agent skip is refused human-only
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_af750802_override_is_read_only_and_agent_skip_refused(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test2", owner_id=USER_ID,
                     settings={"require_ideation_ambiguity_gate": True,
                               "max_ideation_ambiguity": 3}))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()
    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id,
            USER_ID,
            True,
            reason="Accepted for this validation edition.",
            expected_ideation_version=1,
            expected_ideation_edition=1,
            source="rest",
        )
        await db.commit()

    ctx = await _call("okto_pulse_get_ideation_context", board_id=board_id,
                      ideation_id=ideation_id)
    o = ctx["skip_overrides"][0]
    # Read-only: no agent-mutation affordance in the override block.
    assert o["mutation_allowed"] is False
    for mutate_key in ("next_action", "tool", "required_tool", "follow_up"):
        assert mutate_key not in o, mutate_key

    # And an agent attempting to apply the skip is refused human-only (remediation
    # asks for a human decision; never instructs the agent to skip).
    refusal = await _call("okto_pulse_set_ideation_ambiguity_gate_skip",
                          board_id=board_id, ideation_id=ideation_id,
                          skip_ambiguity_gate=False)
    assert refusal["code"] == "human_control_required"
    assert refusal["details"]["required_actor"] == "human"
    assert "human" in refusal["message"].lower()
    # The agent skip did NOT mutate the existing skip state (still applied by human).
    async with db_factory() as db:
        ideation = await db.get(Ideation, ideation_id)
    assert ideation.skip_ambiguity_gate is True
