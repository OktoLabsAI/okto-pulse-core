"""RKG-04 — MCP wrapper/contract tests for the connectivity-guard DLQ tools.

Proves the operator-consumable path is fail-closed: the MCP reprocess tool calls
the SAFE service (never the generic broad reprocess), blocks empty/out-of-class
selections, and on the happy path requeues without duplicating the queue row.
Invoked the production way: mcp.get_tool(name).fn(...).
"""

from __future__ import annotations

import contextlib
import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from sqlalchemy_test_models import ConsolidationDeadLetter, ConsolidationQueue
from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
    CONNECTIVITY_GUARD_SIGNATURE,
)


def _mcp(monkeypatch, db_factory):
    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_ctx(_board_id):
        return SimpleNamespace(agent_id="mcp-agent", permissions=["*"])

    @contextlib.asynccontextmanager
    async def _fake_db():
        async with db_factory() as db:
            yield db

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _fake_db)
    monkeypatch.setattr(mcp_server, "check_permission", lambda *a, **k: None)
    return mcp_server


async def _call(mcp_server, name, **kwargs):
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


async def _seed_dlq(db_factory, board_id, *, error=CONNECTIVITY_GUARD_SIGNATURE) -> str:
    aid = uuid.uuid4().hex
    async with db_factory() as db:
        row = ConsolidationDeadLetter(
            board_id=board_id, artifact_type="spec", artifact_id=aid, attempts=5,
            errors=[{"attempt": 5, "error_type": "KGPrimitiveError", "message": error}],
            dead_lettered_at=datetime.now(timezone.utc),
        )
        db.add(row)
        await db.flush()
        dlq_id = row.id
        await db.commit()
    return dlq_id


def _force_not_quarantined(monkeypatch):
    import okto_pulse.core.services.kg_health_service as health_mod

    async def _healthy(_board_id, _db, scheduler_control=None):
        return {"overall_state": "healthy"}

    monkeypatch.setattr(health_mod, "get_kg_health", _healthy)


@pytest.mark.asyncio
async def test_mcp_reprocess_empty_selection_blocks(db_factory, monkeypatch):
    mcp = _mcp(monkeypatch, db_factory)
    _force_not_quarantined(monkeypatch)
    board_id = f"rkg04mcp-{uuid.uuid4().hex[:8]}"
    await _seed_dlq(db_factory, board_id)

    res = await _call(
        mcp, "okto_pulse_kg_connectivity_dlq_reprocess",
        board_id=board_id, dead_letter_ids="", process_now="false")

    assert res["blocked"] is True
    assert "no_dlq_selected" in res["reasons"]  # never a broad reprocess (TR1)
    assert res["removed_dlq"] is False


@pytest.mark.asyncio
async def test_mcp_reprocess_out_of_class_blocks_untouched(db_factory, monkeypatch):
    mcp = _mcp(monkeypatch, db_factory)
    _force_not_quarantined(monkeypatch)
    board_id = f"rkg04mcp-{uuid.uuid4().hex[:8]}"
    other = await _seed_dlq(db_factory, board_id, error="some unrelated failure")

    res = await _call(
        mcp, "okto_pulse_kg_connectivity_dlq_reprocess",
        board_id=board_id, dead_letter_ids=other, process_now="false")

    assert res["blocked"] is True
    assert "selected_dlq_out_of_class" in res["reasons"]
    async with db_factory() as db:
        rows = (await db.execute(select(ConsolidationDeadLetter).where(
            ConsolidationDeadLetter.board_id == board_id))).scalars().all()
    assert any(r.id == other for r in rows)  # untouched


@pytest.mark.asyncio
async def test_mcp_diagnose_then_reprocess_requeues_without_duplicate(db_factory, monkeypatch):
    mcp = _mcp(monkeypatch, db_factory)
    _force_not_quarantined(monkeypatch)
    board_id = f"rkg04mcp-{uuid.uuid4().hex[:8]}"
    dlq_id = await _seed_dlq(db_factory, board_id)

    diag = await _call(mcp, "okto_pulse_kg_connectivity_dlq_diagnose", board_id=board_id)
    assert dlq_id in diag["dead_letter_ids"]

    res = await _call(
        mcp, "okto_pulse_kg_connectivity_dlq_reprocess",
        board_id=board_id, dead_letter_ids=dlq_id, process_now="false")
    assert res["blocked"] is False and res["success"] is True

    async with db_factory() as db:
        dlq = (await db.execute(select(ConsolidationDeadLetter).where(
            ConsolidationDeadLetter.board_id == board_id))).scalars().all()
        q = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id))).scalars().all()
    assert not dlq  # DLQ removed
    assert len(q) == 1  # requeued, no duplicate
