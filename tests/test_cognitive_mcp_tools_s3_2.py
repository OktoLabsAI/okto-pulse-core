"""S3.2 / card 3979c220 — MCP cognitive readiness tools + central write-path
(spec 2731a346).

Exercita os tools MCP via mcp.get_tool(name).fn(...) com auth/db fake:
  * okto_pulse_kg_list_cognitive_readiness_items (twin do read-model S3.1)
  * okto_pulse_kg_evaluate_cognitive_readiness
  * okto_pulse_kg_record_cognitive_skip   (write-path CENTRAL)
  * okto_pulse_kg_clear_cognitive_skip     (reopen CENTRAL)
  * okto_pulse_kg_list_cognitive_dlq
  * okto_pulse_kg_evaluate_bug_cognitive_closure (twin S2 no catálogo)

Cenários: ts_9862838e (lista pending/debt/terminal/DLQ acionável), ts_e1b66ffa
(skip rejeitado por DLQ/debt 409 sem write), ts_87b584b4 (reason_code cognitivo ≠
cause técnico; unknown rejeitado), ts_d5d8b99e (twin de bug operacional retorna
evidence matrix/readiness), + paridade ts_ee91c90f (aliases/uma precedência) e
ts_d3ae61b3 (todos os tiers + task advisory). Enforcement (`would_block_done`)
delegado a _cognitive_readiness_blocking_active + GATE_BLOCKING_TIERS, nunca
recomputado (carry-forward do validador de S3.1).
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from okto_pulse.core.models.db import Board, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)
UUID_A = "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa"
UUID_B = "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb"
UUID_C = "cccccccc-3333-3333-3333-cccccccccccc"
UUID_T = "dddddddd-4444-4444-4444-dddddddddddd"
FUTURE = (NOW + timedelta(days=2)).isoformat()
PAST = (NOW - timedelta(hours=1)).isoformat()


def _seed_items(store, board, gen, specs):
    path = store._record_path(board, gen)
    path.parent.mkdir(parents=True, exist_ok=True)
    items, pending_refs = [], []
    for spec in specs:
        src = spec["source_ref"]
        iid = compute_cognitive_item_id(board, gen, src)
        d = {
            "item_id": iid, "board_id": board, "kg_generation_id": gen,
            "source_ref": src,
            "artifact_type": spec.get("artifact_type", src.split(":", 1)[0]),
            "status": spec["status"],
            "recorded_at": "2026-06-17T00:00:00+00:00",
        }
        for k in ("reason_code", "revisit_at", "outcome_type"):
            if spec.get(k) is not None:
                d[k] = spec[k]
        items.append(d)
        if spec["status"] == CognitiveItemStatus.PENDING.value:
            pending_refs.append(src)
    record = {
        "pending_count": len(pending_refs), "pending_refs": pending_refs,
        "status": "pending" if pending_refs else "consolidated",
        "recorded_at": "2026-06-17T00:00:00+00:00", "items": items,
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(record, fh)


async def _board(db_factory, board_id, settings=None):
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(
                id=board_id, name="mcp-cog", owner_id="owner-mcp",
                settings=settings or {},
            ))
            await db.commit()


def _wire(monkeypatch, tmp_path, db_factory):
    """Set the store base_dir env + fake auth/db; return the mcp server module."""
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_ctx(board_id):
        return SimpleNamespace(agent_id="mcp-agent")

    @contextlib.asynccontextmanager
    async def _fake_db():
        async with db_factory() as db:
            yield db

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _fake_db)
    # R01A MCP-FU3: the migrated cognitive tools obtain a PulseUnitOfWork from the
    # MCP session factory; register it (the same db_factory the _fake_db patch
    # yields) so get_unit_of_work_factory_for_mcp() resolves to the test session.
    monkeypatch.setattr(mcp_server, "_mcp_session_factory", db_factory)
    return mcp_server


def _enable_global_flag(monkeypatch):
    from okto_pulse.core.infra import config as config_mod
    settings = config_mod.get_settings()
    monkeypatch.setattr(
        settings, "cognitive_readiness_blocking_enabled", True, raising=False
    )


async def _call(mcp_server, name, **kwargs):
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


def _by_artifact(items):
    out = {}
    for it in items:
        out.setdefault(it["artifact_id"], []).append(it)
    return out


# ---------------------------------------------------------------------------
# ts_9862838e — lista pending, debt, terminal history e DLQ acionável
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_9862838e_list_all_signals(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-list", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.CONSOLIDATED.value},
    ])
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_C,
            source_ref=f"card:{UUID_C}", content_hash="h",
            target_status="done", canonical_state="failed",
        )
        db.add(ConsolidationDeadLetter(
            id="dlq-list", board_id=board, artifact_type="card", artifact_id=UUID_T,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, limit=200,
    )
    by_art = _by_artifact(out["items"])
    # cada sinal presente com artifact_id/source/readiness_effect.
    assert by_art[f"card:{UUID_A}"][0]["readiness_effect"] == "blocking_cognitive"
    assert by_art[f"card:{UUID_B}"][0]["readiness_effect"] == "ready_committed"
    assert by_art[f"card:{UUID_C}"][0]["signal"] == "open_canonical_debt"
    assert by_art[f"card:{UUID_C}"][0]["readiness_effect"] == "blocking_technical"
    dlq = by_art[f"card:{UUID_T}"][0]
    assert dlq["signal"] == "dlq" and dlq["error_cause"] == "technical_dlq"
    # summary acionável + enforcement exposto.
    assert out["summary"]["technical_blocking_signals"] == 2
    assert out["summary"]["cognitive_pending_signals"] == 1
    assert out["summary"]["enforcement_active"] is False
    assert out["precedence"][0] == "technical_dlq"


# ---------------------------------------------------------------------------
# ts_e1b66ffa — skip rejeitado por DLQ/debt aberto, 409, sem write
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_e1b66ffa_skip_blocked_by_dlq_409_no_write(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-skip-dlq", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-skip", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="trivial_fix",
    )
    # R5-IMP1: the agent-facing MCP skip is HUMAN-only — it fails closed BEFORE the
    # service, so the DLQ-409 guard now runs on the human REST path (covered in
    # test_cognitive_action_center_rest_s3_3). No ledger write either way.
    assert out["code"] == "human_control_required"
    assert out["details"]["mutation_allowed"] is False
    iid = compute_cognitive_item_id(board, gen, f"bug:{UUID_A}")
    persisted = {i.item_id: i for i in store.list_items(board, gen)}
    assert persisted[iid].status == CognitiveItemStatus.PENDING.value


@pytest.mark.asyncio
async def test_skip_blocked_by_open_debt_409(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-skip-debt", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="blocked",
        )
        await db.commit()

    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="duplicate_bug",
    )
    # R5-IMP1: MCP skip is human-only (the debt-409 guard now runs on REST, S3.3).
    assert out["code"] == "human_control_required"
    assert out["details"]["mutation_allowed"] is False


# ---------------------------------------------------------------------------
# ts_87b584b4 — reason_code cognitivo ≠ cause técnico; unknown rejeitado
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_87b584b4_reason_code_registry_closed(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-registry", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    # R5-IMP1: MCP skip is human-only — reason_code validation now runs on the human
    # REST path (S3.3 covers invalid_reason_code). The agent surface refuses first.
    out = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="made_up",
    )
    assert out["code"] == "human_control_required"

    out2 = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="technical_dlq",
    )
    assert out2["code"] == "human_control_required"

    # na listagem, DLQ aparece como error_cause técnico, reason_code cognitivo None.
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-reg", board_id=board, artifact_type="card", artifact_id=UUID_B,
            original_queue_id="q1", attempts=2,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    listed = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, signal="dlq",
    )
    row = listed["items"][0]
    assert row["error_cause"] == "technical_dlq" and row["reason_code"] is None


@pytest.mark.asyncio
async def test_revisit_required_needs_future_revisit_at(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-revisit", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    # R5-IMP1: MCP skip is human-only — both the revisit_at_required guard AND the
    # valid skip run on the human REST path (S3.3). The agent surface refuses and
    # never writes the ledger.
    out = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="path_b_pending",
    )
    assert out["code"] == "human_control_required"

    out2 = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="path_b_pending",
        revisit_at=FUTURE,
    )
    assert out2["code"] == "human_control_required"
    # No write: the item never became skipped via the agent surface.
    iid = compute_cognitive_item_id(board, gen, f"bug:{UUID_A}")
    persisted = {i.item_id: i for i in store.list_items(board, gen)}
    assert persisted[iid].status == CognitiveItemStatus.PENDING.value


# ---------------------------------------------------------------------------
# record (happy terminal) → clear (reopen central) — write-path central
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_then_clear_reopen_central(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-clear", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    # R5-IMP1: both the agent MCP record AND clear refuse (human-only); the central
    # write/reopen path is exercised by the human REST surface (S3.3 parity test).
    rec = await _call(
        mcp_server, "okto_pulse_kg_record_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}", reason_code="trivial_fix",
        justification="nothing reusable", evidence_refs=["e1"],
    )
    assert rec["code"] == "human_control_required"
    assert rec["details"]["mutation_allowed"] is False

    cleared = await _call(
        mcp_server, "okto_pulse_kg_clear_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}",
    )
    assert cleared["code"] == "human_control_required"

    # No write by EITHER agent call — the item stays PENDING (never skipped).
    iid = compute_cognitive_item_id(board, gen, f"bug:{UUID_A}")
    persisted = {i.item_id: i for i in store.list_items(board, gen)}[iid]
    assert persisted.status == CognitiveItemStatus.PENDING.value
    assert persisted.reason_code is None


@pytest.mark.asyncio
async def test_clear_non_skipped_409(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-clear-409", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_clear_cognitive_skip",
        board_id=board, source_ref=f"bug:{UUID_A}",
    )
    # R5-IMP1: MCP clear is human-only (the not_skipped-409 guard runs on REST, S3.3).
    assert out["code"] == "human_control_required"


# ---------------------------------------------------------------------------
# evaluate tool espelha o serviço (técnico vs cognitivo/advisory)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_tool_mirrors_service(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-eval", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"task:{UUID_T}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "task"},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-eval", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    tech = await _call(
        mcp_server, "okto_pulse_kg_evaluate_cognitive_readiness",
        board_id=board, source_ref=f"card:{UUID_A}",
    )
    assert tech["readiness_effect"] == "blocking_technical" and tech["blocking"] is True

    task = await _call(
        mcp_server, "okto_pulse_kg_evaluate_cognitive_readiness",
        board_id=board, source_ref=f"task:{UUID_T}",
    )
    assert task["readiness_effect"] == "blocking_cognitive"
    assert task["would_block_done"] is False  # cognitive nunca enforça (advisory)


# ---------------------------------------------------------------------------
# would_block_done — enforcement delegado, nunca recomputado
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_would_block_done_enforcement(tmp_path, db_factory, monkeypatch):
    # board com policy blocking + flag global ligada → enforcement ativo.
    board, gen = "mcp-enforce", "gen-1"
    await _board(db_factory, board, settings={"cognitive_readiness_policy": "blocking"})
    _enable_global_flag(monkeypatch)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.PENDING.value},
        {"source_ref": f"task:{UUID_T}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "task"},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-enf", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, limit=200,
    )
    assert out["summary"]["enforcement_active"] is True
    by_art = _by_artifact(out["items"])
    # técnico (DLQ) sob enforcement → would_block_done True.
    assert by_art[f"card:{UUID_A}"][0]["would_block_done"] is True
    # cognitivo ativo (card) → blocking True MAS would_block_done False (não está no gate).
    card_active = by_art[f"card:{UUID_B}"][0]
    assert card_active["blocking"] is True and card_active["would_block_done"] is False
    # task ativa → advisory, would_block_done False.
    assert by_art[f"task:{UUID_T}"][0]["would_block_done"] is False


# ---------------------------------------------------------------------------
# Paridade ts_ee91c90f — card/bug aliases, UMA precedência (lado MCP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_ee91c90f_mcp_aliases_one_precedence(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-alias", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},
    ])
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",
        )
        await db.commit()
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, limit=200,
    )
    assert {it["artifact_id"] for it in out["items"]} == {f"card:{UUID_A}"}
    for r in out["items"]:
        assert set(r["aliases"]) == {f"bug:{UUID_A}", f"card:{UUID_A}"}
        assert r["readiness_effect"] == "blocking_technical"  # debt vence o skip


# ---------------------------------------------------------------------------
# Paridade ts_d3ae61b3 — todos os tiers + task advisory (lado MCP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_d3ae61b3_mcp_all_tiers_task_advisory(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-tiers", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "evidence_insufficient", "revisit_at": PAST},  # vencido
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},  # válido
        {"source_ref": f"task:{UUID_T}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "task"},  # advisory cognitivo
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, limit=200,
    )
    by_art = _by_artifact(out["items"])
    assert by_art[f"card:{UUID_A}"][0]["readiness_effect"] == "blocking_revisit_lapsed"
    assert by_art[f"card:{UUID_B}"][0]["readiness_effect"] == "ready_skip"
    task = by_art[f"task:{UUID_T}"][0]
    assert task["readiness_effect"] == "blocking_cognitive"
    assert task["readiness_effect"] != "blocking_technical"


# ---------------------------------------------------------------------------
# ts_d5d8b99e — twin de bug no catálogo operacional retorna matrix/readiness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_d5d8b99e_bug_twin_in_catalog(tmp_path, db_factory, monkeypatch):
    board, gen = "mcp-bugtwin", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "bug"},
    ])
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)

    # registrado no catálogo operacional.
    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_evaluate_bug_cognitive_closure")
    assert tool is not None

    out = await _call(
        mcp_server, "okto_pulse_kg_evaluate_bug_cognitive_closure",
        board_id=board, bug_id=UUID_A,
        evidence={"root_cause": "rc", "fix_narrative": "fix"},
        requested_action="evaluate",
    )
    assert "error" not in out
    assert "readiness_effect" in out
    assert "evidence_classification" in out
    assert out["evidence_classification"]["has_reusable_learning"] is True
    assert "bug_action_label" in out


# ---------------------------------------------------------------------------
# DLQ list tool — diagnóstico técnico acionável
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_cognitive_dlq_tool(tmp_path, db_factory, monkeypatch):
    board = "mcp-dlqtool"
    await _board(db_factory, board)
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-tool", board_id=board, artifact_type="bug", artifact_id=UUID_A,
            original_queue_id="q9", attempts=4,
            errors=[{"attempt": 1, "error_type": "Boom", "message": "kaput"}],
        ))
        await db.commit()
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_dlq", board_id=board,
    )
    assert out["total"] == 1
    row = out["items"][0]
    assert row["artifact_id"] == f"card:{UUID_A}"  # bug normaliza p/ card
    assert row["error_cause"] == "technical_dlq"
    assert row["readiness_effect"] == "blocking_technical"
    assert row["errors"]  # histórico de erros presente para ação


@pytest.mark.asyncio
async def test_list_invalid_filter_400_via_mcp(tmp_path, db_factory, monkeypatch):
    board = "mcp-badfilter"
    await _board(db_factory, board)
    mcp_server = _wire(monkeypatch, tmp_path, db_factory)
    out = await _call(
        mcp_server, "okto_pulse_kg_list_cognitive_readiness_items",
        board_id=board, signal="nope",
    )
    assert out["error"] == "invalid_filter" and out["status_code"] == 400
