"""S3.3 / card 974f5146 — REST skip/clear/metrics + would_block_done + parity
(spec 2731a346).

Cobre o lado CORE do S3.3 (a UI consome estes endpoints):
  * POST /kg/{board}/cognitive-readiness/skip  — write-path central (400/409).
  * POST /kg/{board}/cognitive-readiness/clear — reopen central (409).
  * GET  /kg/{board}/cognitive-readiness/metrics — métricas bounded.
  * GET  .../items agora anota would_block_done + enforcement_active.
  * Paridade REST↔MCP via builder compartilhado (tr_d9f9f65e).

Os endpoints são chamados diretamente (async) para evitar o portal sync do
TestClient; a rota registrada é checada à parte.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import okto_pulse.community.api.cognitive_action_center as ac_api
from okto_pulse.community.api.cognitive_action_center import (
    CognitiveClearRequest,
    CognitiveSkipRequest,
    clear_cognitive_skip_endpoint,
    get_cognitive_readiness_metrics,
    list_cognitive_readiness_items,
    record_cognitive_skip_endpoint,
)
from okto_pulse.community.api.router import api_router
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from sqlalchemy_test_models import Board, ConsolidationDeadLetter

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)
UUID_A = "aaaaaaaa-5555-5555-5555-aaaaaaaaaaaa"
UUID_B = "bbbbbbbb-6666-6666-6666-bbbbbbbbbbbb"
UUID_C = "cccccccc-7777-7777-7777-cccccccccccc"
UUID_T = "dddddddd-8888-8888-8888-dddddddddddd"
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
            "recorded_at": spec.get("recorded_at", "2026-06-17T00:00:00+00:00"),
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
            db.add(Board(id=board_id, name="ac-rest3", owner_id="o", settings=settings or {}))
            await db.commit()


def _use_store(monkeypatch, store):
    monkeypatch.setattr(
        ac_api, "build_default_readiness_service",
        lambda: CognitiveReadinessService(store, now=lambda: NOW),
        raising=True,
    )


def _enable_global_flag(monkeypatch):
    from okto_pulse.core.infra import config as config_mod
    settings = config_mod.get_settings()
    monkeypatch.setattr(
        settings, "cognitive_readiness_blocking_enabled", True, raising=False
    )


async def _list(board, db, **over):
    """Call the list endpoint directly with explicit args (FastAPI Query()
    defaults aren't resolved when calling the function outside the app)."""
    kwargs = dict(
        signal="all", artifact_id=None, source_ref=None, reason_code=None,
        status=None, search=None, limit=50, offset=0, kg_generation_id=None,
    )
    kwargs.update(over)
    return await list_cognitive_readiness_items(board, db=db, actor="u", **kwargs)


SKIP_KEYS = {
    "item_id", "status", "outcome_type", "reason_code", "justification",
    "evidence_refs", "actor", "revisit_at", "updated_at", "classification",
    "readiness_effect", "blocking", "would_block_done", "precedence_explanation",
}
CLEAR_KEYS = {
    "item_id", "status", "reason_code", "revisit_at", "actor", "updated_at",
    "readiness_effect", "blocking", "would_block_done", "precedence_explanation",
}


# ---------------------------------------------------------------------------
# Rota registrada
# ---------------------------------------------------------------------------


def _route_paths(routes) -> set[str]:
    paths: set[str] = set()
    for route in routes:
        effective_route_contexts = getattr(route, "effective_route_contexts", None)
        if callable(effective_route_contexts):
            for context in effective_route_contexts():
                path = getattr(context, "path", None)
                if path:
                    paths.add(path)
        path = getattr(route, "path", None)
        if path:
            paths.add(path)
        nested = getattr(route, "routes", None)
        if nested:
            paths.update(_route_paths(nested))
    return paths


def test_routes_registered():
    paths = _route_paths(api_router.routes)
    assert "/api/v1/kg/{board_id}/cognitive-readiness/skip" in paths
    assert "/api/v1/kg/{board_id}/cognitive-readiness/clear" in paths
    assert "/api/v1/kg/{board_id}/cognitive-readiness/metrics" in paths


# ---------------------------------------------------------------------------
# Skip REST — happy terminal + 400/409 sem write
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_skip_happy_terminal(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-skip", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    _use_store(monkeypatch, store)
    async with db_factory() as db:
        out = await record_cognitive_skip_endpoint(
            board,
            CognitiveSkipRequest(
                source_ref=f"bug:{UUID_A}", reason_code="trivial_fix",
                justification="nada reutilizável", evidence_refs=["e1"],
            ),
            db, actor="rest-user",
        )
    assert set(out.keys()) == SKIP_KEYS
    assert out["status"] == "skipped" and out["classification"] == "terminal"
    assert out["readiness_effect"] == "ready_skip"
    assert out["would_block_done"] is False
    assert out["justification"] == "nada reutilizável" and out["evidence_refs"] == ["e1"]


@pytest.mark.asyncio
async def test_rest_skip_409_by_dlq_no_write(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-skip-dlq", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="s33-dlq-3a", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    _use_store(monkeypatch, store)
    with pytest.raises(HTTPException) as exc:
        async with db_factory() as db:
            await record_cognitive_skip_endpoint(
                board,
                CognitiveSkipRequest(source_ref=f"bug:{UUID_A}", reason_code="trivial_fix"),
                db, actor="u",
            )
    assert exc.value.status_code == 409
    assert exc.value.detail["error"] == "technical_debt_cannot_be_skipped"
    # sem write
    iid = compute_cognitive_item_id(board, gen, f"bug:{UUID_A}")
    assert {i.item_id: i for i in store.list_items(board, gen)}[iid].status == (
        CognitiveItemStatus.PENDING.value
    )


@pytest.mark.asyncio
async def test_rest_skip_400_invalid_reason(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-skip-badreason", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    _use_store(monkeypatch, store)
    for bad in ("made_up", "technical_dlq", "canonical_debt_open"):
        with pytest.raises(HTTPException) as exc:
            async with db_factory() as db:
                await record_cognitive_skip_endpoint(
                    board,
                    CognitiveSkipRequest(source_ref=f"bug:{UUID_A}", reason_code=bad),
                    db, actor="u",
                )
        assert exc.value.status_code == 400
        assert exc.value.detail["error"] == "invalid_reason_code"


@pytest.mark.asyncio
async def test_rest_skip_400_revisit_required(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-skip-revisit", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    _use_store(monkeypatch, store)
    with pytest.raises(HTTPException) as exc:
        async with db_factory() as db:
            await record_cognitive_skip_endpoint(
                board,
                CognitiveSkipRequest(source_ref=f"bug:{UUID_A}", reason_code="path_b_pending"),
                db, actor="u",
            )
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "revisit_at_required"

    # com revisit futuro → ok, classification revisit_required
    async with db_factory() as db:
        out = await record_cognitive_skip_endpoint(
            board,
            CognitiveSkipRequest(
                source_ref=f"bug:{UUID_A}", reason_code="path_b_pending", revisit_at=FUTURE,
            ),
            db, actor="u",
        )
    assert out["classification"] == "revisit_required" and out["revisit_at"] == FUTURE


# ---------------------------------------------------------------------------
# Clear REST — reopen central + 409
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_clear_reopen(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-clear", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},
    ])
    _use_store(monkeypatch, store)
    async with db_factory() as db:
        out = await clear_cognitive_skip_endpoint(
            board, CognitiveClearRequest(source_ref=f"bug:{UUID_A}"), db, actor="rest-user",
        )
    assert set(out.keys()) == CLEAR_KEYS
    assert out["status"] == CognitiveItemStatus.PENDING.value
    assert out["reason_code"] is None
    assert out["readiness_effect"] == "blocking_cognitive"
    assert out["actor"] == "rest-user"


@pytest.mark.asyncio
async def test_rest_clear_409_not_skipped(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-clear-409", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    _use_store(monkeypatch, store)
    with pytest.raises(HTTPException) as exc:
        async with db_factory() as db:
            await clear_cognitive_skip_endpoint(
                board, CognitiveClearRequest(source_ref=f"bug:{UUID_A}"), db, actor="u",
            )
    assert exc.value.status_code == 409
    assert exc.value.detail["error"] == "cognitive_item_not_skipped"


# ---------------------------------------------------------------------------
# Metrics REST — labels bounded, sem free-text
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_metrics_bounded(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-metrics", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.PENDING.value,
         "recorded_at": NOW.isoformat()},
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix", "recorded_at": (NOW - timedelta(days=3)).isoformat()},
        # reason_code fora do registry fechado → DEVE virar label bounded "other".
        {"source_ref": f"card:{UUID_C}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "weird_freetext_xyz", "recorded_at": (NOW - timedelta(days=30)).isoformat()},
        {"source_ref": f"card:{UUID_T}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "evidence_insufficient", "revisit_at": PAST},  # vencido
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="s33-dlq-m", board_id=board, artifact_type="card", artifact_id="ffffffff-0000-0000-0000-ffffffffffff",
            original_queue_id="q1", attempts=2,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    _use_store(monkeypatch, store)
    async with db_factory() as db:
        m = await get_cognitive_readiness_metrics(board, kg_generation_id=None, db=db, actor="u")

    # labels bounded presentes
    assert m["by_status"][CognitiveItemStatus.SKIPPED.value] == 3
    assert m["by_reason_code"]["trivial_fix"] == 1
    assert m["by_reason_code"]["other"] == 1            # clamp do free-text
    assert "weird_freetext_xyz" not in m["by_reason_code"]
    assert m["by_readiness_effect"]["blocking_revisit_lapsed"] == 1
    assert m["expired_revisit_skips"] == 1
    assert m["technical_dlq"] == 1
    assert set(m["by_age_bucket"]).issubset({"lt_1d", "1d_7d", "gt_7d", "unknown"})
    # NENHUM free-text como label
    flat = json.dumps(m)
    assert "weird_freetext_xyz" not in flat


# ---------------------------------------------------------------------------
# List REST — would_block_done / enforcement_active
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_list_enforcement_annotation(tmp_path, db_factory, monkeypatch):
    # advisory (default): DLQ blocking mas would_block_done False.
    board, gen = "ac3-list-adv", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="s33-dlq-adv", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=2,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    _use_store(monkeypatch, store)
    async with db_factory() as db:
        out = await _list(board, db)
    assert out["summary"]["enforcement_active"] is False
    dlq = out["items"][0]
    assert dlq["blocking"] is True and dlq["would_block_done"] is False


@pytest.mark.asyncio
async def test_rest_list_enforcement_active_blocks(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-list-enf", "gen-1"
    await _board(db_factory, board, settings={"cognitive_readiness_policy": "blocking"})
    _enable_global_flag(monkeypatch)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"task:{UUID_T}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "task"},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="s33-dlq-enf", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=2,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()
    _use_store(monkeypatch, store)
    async with db_factory() as db:
        out = await _list(board, db, limit=200)
    assert out["summary"]["enforcement_active"] is True
    by_art = {it["artifact_id"]: it for it in out["items"]}
    assert by_art[f"card:{UUID_A}"]["would_block_done"] is True       # técnico enforça
    assert by_art[f"task:{UUID_T}"]["would_block_done"] is False      # task advisory


# ---------------------------------------------------------------------------
# Paridade REST ↔ MCP (tr_d9f9f65e) — mesmo shape via builder compartilhado
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rest_mcp_skip_parity(tmp_path, db_factory, monkeypatch):
    board, gen = "ac3-parity", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
        {"source_ref": f"bug:{UUID_B}", "status": CognitiveItemStatus.PENDING.value},
    ])
    _use_store(monkeypatch, store)

    # REST skip (UUID_A)
    async with db_factory() as db:
        rest = await record_cognitive_skip_endpoint(
            board, CognitiveSkipRequest(source_ref=f"bug:{UUID_A}", reason_code="trivial_fix"),
            db, actor="rest-user",
        )

    # MCP skip (UUID_B) — mesmo store via env
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_ctx(board_id):
        return SimpleNamespace(agent_id="mcp-agent")

    import contextlib

    @contextlib.asynccontextmanager
    async def _fake_db():
        async with db_factory() as db:
            yield db

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _fake_db)
    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_record_cognitive_skip")
    mcp = json.loads(await tool.fn(
        board_id=board, source_ref=f"bug:{UUID_B}", reason_code="trivial_fix",
    ))

    # R5-IMP1: the agent-facing MCP skip is HUMAN-only — it no longer mirrors the
    # REST skip response. REST (the human surface) still returns the full skip
    # payload; MCP returns the human_control_required refusal. The shared-builder
    # parity now holds across the HUMAN surfaces (REST + UI), not the agent MCP.
    assert set(rest.keys()) == SKIP_KEYS
    assert rest["classification"] == "terminal"
    assert rest["readiness_effect"] == "ready_skip"
    assert mcp["code"] == "human_control_required"
    assert mcp["details"]["mutation_allowed"] is False
    assert mcp["details"]["state_changed"] is False


def test_rest_routes_via_client(tmp_path, db_factory):
    # smoke: as rotas POST/GET existem e respondem (400 invalid_filter prova o GET items).
    app = FastAPI()
    app.include_router(api_router)
    from okto_pulse.core.infra import auth as _auth_mod
    from okto_pulse.core.infra.database import get_db

    async def _fake_user():
        return "u"

    async def _fake_db():
        async with db_factory() as s:
            yield s

    app.dependency_overrides[_auth_mod.require_user] = _fake_user
    app.dependency_overrides[get_db] = _fake_db
    client = TestClient(app)
    r = client.get("/api/v1/kg/any-board/cognitive-readiness/items", params={"signal": "nope"})
    assert r.status_code == 400 and r.json()["detail"]["error"] == "invalid_filter"
