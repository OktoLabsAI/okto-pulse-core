"""S3.1 / card 38a014f7 — Cognitive Action Center read-model + REST
(spec 2731a346 / api_8ee8f41b).

Cenários sustentados:
  * ts_290926c7 — Action Center é SÓ consumidor/projeção: nenhuma escrita em
    store/tabela/fila/ledger (teeth: write-paths explodem; arquivo e DB intactos).
  * ts_c66892f6 — open canonical debt bloqueia (blocking_technical); committed
    (terminal) informa (não bloqueia); DLQ bloqueia (blocking_technical).
  * ts_95b27c55 — skip válido (ready_skip) vs revisit-required vencido
    (blocking_revisit_lapsed); labels de sinal vêm do conjunto fechado.
  * ts_ee91c90f — card:<uuid> e bug:<uuid> reconciliam no MESMO artifact_id e
    compartilham UMA precedência (aliases agregados).
  * ts_d3ae61b3 — todos os tiers projetados + task/test com item ativo ficam
    advisory (blocking_cognitive), nunca escalados a bloqueio técnico.

A precedência NUNCA é recomputada aqui: cada verdict vem de
CognitiveReadinessService.evaluate_artifact (tr_6bfe98e7 / tr_0aab9bea).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from okto_pulse.core.api.router import api_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.cognitive_action_center import (
    PRECEDENCE_ORDER,
    SIGNAL_DLQ,
    SIGNAL_OPEN_CANONICAL_DEBT,
    SIGNAL_REVISIT_REQUIRED,
    SIGNAL_SKIPPED,
    SIGNAL_TERMINAL_HISTORY,
    SOURCE_CANONICAL_DEBT,
    SOURCE_COGNITIVE_ITEM,
    SOURCE_DLQ,
    CognitiveActionCenterReadModel,
    VALID_SIGNAL_FILTERS,
)
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    compute_cognitive_item_id,
)
from okto_pulse.core.models.db import Board, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)
UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
UUID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
UUID_C = "cccccccc-cccc-cccc-cccc-cccccccccccc"
UUID_T = "dddddddd-dddd-dddd-dddd-dddddddddddd"


def _seed_items(store, board, gen, specs):
    """Write a generation record with arbitrary cognitive items.

    ``specs`` — list of dicts with at least ``source_ref`` + ``status``; optional
    ``reason_code`` / ``revisit_at`` / ``outcome_type`` / ``artifact_type``.
    """
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
        "pending_count": len(pending_refs),
        "pending_refs": pending_refs,
        "status": "pending" if pending_refs else "consolidated",
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "items": items,
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(record, fh)
    return path


async def _board(db_factory, board_id):
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="action-center", owner_id="owner-ac"))
            await db.commit()


def _read_model(store):
    return CognitiveActionCenterReadModel(
        CognitiveReadinessService(store, now=lambda: NOW)
    )


def _by_artifact(items):
    out = {}
    for it in items:
        out.setdefault(it["artifact_id"], []).append(it)
    return out


# ---------------------------------------------------------------------------
# ts_290926c7 — Action Center não escreve nada (read-only)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_290926c7_read_model_never_writes(tmp_path, db_factory, monkeypatch):
    board, gen = "ac-readonly", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    path = _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-ro", board_id=board, artifact_type="card", artifact_id=UUID_B,
            original_queue_id="q1", attempts=2,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    # Teeth: qualquer write-path do store explode.
    def _boom(*_a, **_k):
        raise AssertionError("Action Center é read-only — não pode escrever no store")

    monkeypatch.setattr(CognitiveConsolidationItemStore, "update_item", _boom, raising=True)
    monkeypatch.setattr(
        CognitiveConsolidationItemStore, "materialize_from_marker", _boom, raising=True
    )

    file_before = path.read_bytes()
    async with db_factory() as db:
        dlq_before = (await db.execute(
            select(func.count()).select_from(ConsolidationDeadLetter)
            .where(ConsolidationDeadLetter.board_id == board)
        )).scalar_one()
        result = await _read_model(store).list_signals(db, board_id=board)

    assert result["items"] and result["precedence"] == list(PRECEDENCE_ORDER)
    # nada foi escrito: arquivo idêntico, contagem DLQ idêntica.
    assert path.read_bytes() == file_before
    async with db_factory() as db:
        dlq_after = (await db.execute(
            select(func.count()).select_from(ConsolidationDeadLetter)
            .where(ConsolidationDeadLetter.board_id == board)
        )).scalar_one()
    assert dlq_after == dlq_before


# ---------------------------------------------------------------------------
# ts_c66892f6 — open debt bloqueia / terminal informa / DLQ bloqueia técnico
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_c66892f6_debt_terminal_dlq(tmp_path, db_factory):
    board, gen = "ac-debt", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [])  # nenhum item cognitivo

    async with db_factory() as db:
        # A: open debt (failed)
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",
        )
        # B: committed debt (terminal/informacional)
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_B,
            source_ref=f"card:{UUID_B}", content_hash="h",
            target_status="done", canonical_state="committed",
        )
        # C: DLQ técnico
        db.add(ConsolidationDeadLetter(
            id="dlq-c", board_id=board, artifact_type="card", artifact_id=UUID_C,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    async with db_factory() as db:
        result = await _read_model(store).list_signals(db, board_id=board, limit=200)
    by_art = _by_artifact(result["items"])

    a = by_art[f"card:{UUID_A}"][0]
    assert a["signal"] == SIGNAL_OPEN_CANONICAL_DEBT
    assert a["signal_source"] == SOURCE_CANONICAL_DEBT
    assert a["blocking"] is True and a["readiness_effect"] == "blocking_technical"
    assert a["error_cause"] == "canonical_debt_open" and a["reason_code"] is None

    b = by_art[f"card:{UUID_B}"][0]
    assert b["signal"] == SIGNAL_TERMINAL_HISTORY
    assert b["blocking"] is False  # terminal informa, não bloqueia

    c = by_art[f"card:{UUID_C}"][0]
    assert c["signal"] == SIGNAL_DLQ and c["signal_source"] == SOURCE_DLQ
    assert c["blocking"] is True and c["readiness_effect"] == "blocking_technical"
    assert c["error_cause"] == "technical_dlq"

    # summary distingue técnico de cognitivo sem acoplar get_kg_health.
    assert result["summary"]["technical_blocking_signals"] == 2  # debt aberto + dlq
    assert result["summary"]["cognitive_pending_signals"] == 0


# ---------------------------------------------------------------------------
# ts_95b27c55 — skip válido vs revisit-required vencido + labels fechados
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_95b27c55_skip_valid_vs_expired(tmp_path, db_factory):
    board, gen = "ac-skip", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        # válido: terminal (trivial_fix) → ready_skip
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix",
         "outcome_type": CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value},
        # vencido: revisit-required no passado → blocking_revisit_lapsed
        {"source_ref": f"bug:{UUID_B}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "evidence_insufficient",
         "revisit_at": (NOW - timedelta(hours=1)).isoformat(),
         "outcome_type": CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value},
    ])

    async with db_factory() as db:
        result = await _read_model(store).list_signals(db, board_id=board, limit=200)
    by_art = _by_artifact(result["items"])

    valid = by_art[f"card:{UUID_A}"][0]
    assert valid["signal"] == SIGNAL_SKIPPED
    assert valid["blocking"] is False and valid["readiness_effect"] == "ready_skip"
    assert valid["reason_code"] == "trivial_fix"

    expired = by_art[f"card:{UUID_B}"][0]
    assert expired["signal"] == SIGNAL_REVISIT_REQUIRED  # label do conjunto fechado
    assert expired["signal"] in VALID_SIGNAL_FILTERS
    assert expired["blocking"] is True
    assert expired["readiness_effect"] == "blocking_revisit_lapsed"
    assert expired["reason_code"] == "evidence_insufficient"


# ---------------------------------------------------------------------------
# ts_ee91c90f — card:<uuid> e bug:<uuid> → mesmo artifact_id, uma precedência
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_ee91c90f_card_bug_aliases_one_precedence(tmp_path, db_factory):
    board, gen = "ac-alias", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    # item cognitivo sob bug:<uuid> (skip válido, isolado → ready_skip)...
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},
    ])
    # ...e canonical debt aberto sob card:<uuid> (mesma artifact normalizada).
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",
        )
        await db.commit()

    async with db_factory() as db:
        result = await _read_model(store).list_signals(db, board_id=board, limit=200)

    # ambos os sinais colapsam no mesmo artifact_id normalizado.
    assert {it["artifact_id"] for it in result["items"]} == {f"card:{UUID_A}"}
    rows = result["items"]
    assert len(rows) == 2  # cognitive_item + canonical_debt
    # aliases agregam os dois source_refs originais.
    for r in rows:
        assert set(r["aliases"]) == {f"bug:{UUID_A}", f"card:{UUID_A}"}
    # UMA precedência: debt OPEN vence o skip → AMBAS as linhas refletem o
    # mesmo verdict (blocking_technical), nunca recomputado por linha.
    assert {r["blocking"] for r in rows} == {True}
    assert {r["readiness_effect"] for r in rows} == {"blocking_technical"}

    # filtro por bug:<uuid> normaliza e acha as duas linhas do artifact.
    async with db_factory() as db:
        filtered = await _read_model(store).list_signals(
            db, board_id=board, artifact_id=f"bug:{UUID_A}", limit=200
        )
    assert len(filtered["items"]) == 2


# ---------------------------------------------------------------------------
# ts_d3ae61b3 — todos os tiers + task/test advisory (cognitive, não técnico)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_d3ae61b3_all_tiers_and_task_advisory(tmp_path, db_factory):
    board, gen = "ac-tiers", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        # card com item ativo → cognitive (advisory no gate, blocking no verdict)
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
        # card consolidado → terminal history ready_committed
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.CONSOLIDATED.value},
        # TASK com item ativo → advisory cognitivo (nunca escalado a técnico)
        {"source_ref": f"task:{UUID_T}", "status": CognitiveItemStatus.PENDING.value,
         "artifact_type": "task"},
    ])
    async with db_factory() as db:
        # DLQ técnico (tier 1)
        db.add(ConsolidationDeadLetter(
            id="dlq-t", board_id=board, artifact_type="card", artifact_id=UUID_C,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    async with db_factory() as db:
        result = await _read_model(store).list_signals(db, board_id=board, limit=200)
    by_art = _by_artifact(result["items"])

    # tier técnico (DLQ)
    assert by_art[f"card:{UUID_C}"][0]["readiness_effect"] == "blocking_technical"
    # tier cognitivo ativo
    assert by_art[f"card:{UUID_A}"][0]["readiness_effect"] == "blocking_cognitive"
    # terminal informacional
    assert by_art[f"card:{UUID_B}"][0]["readiness_effect"] == "ready_committed"
    assert by_art[f"card:{UUID_B}"][0]["blocking"] is False

    # task com item ativo: advisory COGNITIVO — nunca blocking_technical.
    task_row = by_art[f"task:{UUID_T}"][0]
    assert task_row["artifact_type"] == "task"
    assert task_row["readiness_effect"] == "blocking_cognitive"
    assert task_row["readiness_effect"] != "blocking_technical"

    # summary: 1 técnico (dlq) + 2 cognitivos pendentes (card ativo + task ativo).
    assert result["summary"]["technical_blocking_signals"] == 1
    assert result["summary"]["cognitive_pending_signals"] == 2


# ---------------------------------------------------------------------------
# Filtros, paginação e fonte indisponível (read-model)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_signal_filter_and_pagination(tmp_path, db_factory):
    board, gen = "ac-filter", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
        {"source_ref": f"card:{UUID_B}", "status": CognitiveItemStatus.PENDING.value},
        {"source_ref": f"card:{UUID_C}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},
    ])

    rm = _read_model(store)
    async with db_factory() as db:
        only_skipped = await rm.list_signals(db, board_id=board, signal="skipped")
    assert {it["signal"] for it in only_skipped["items"]} == {SIGNAL_SKIPPED}
    assert only_skipped["summary"]["total"] == 1

    async with db_factory() as db:
        page = await rm.list_signals(
            db, board_id=board, signal="cognitive_pending", limit=1, offset=0
        )
    assert len(page["items"]) == 1
    assert page["summary"]["total"] == 2  # total pré-paginação
    assert page["summary"]["limit"] == 1 and page["summary"]["offset"] == 0


@pytest.mark.asyncio
async def test_invalid_signal_filter_raises_400(tmp_path, db_factory):
    board = "ac-badfilter"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    rm = _read_model(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await rm.list_signals(db, board_id=board, signal="made_up")
    assert exc.value.code == "invalid_filter" and exc.value.http_status == 400


@pytest.mark.asyncio
async def test_source_failure_raises_503(tmp_path, db_factory, monkeypatch):
    board, gen = "ac-503", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"card:{UUID_A}", "status": CognitiveItemStatus.PENDING.value},
    ])

    async def _boom_gather(self, *a, **k):
        raise RuntimeError("store offline")

    monkeypatch.setattr(
        CognitiveActionCenterReadModel, "_gather", _boom_gather, raising=True
    )
    rm = _read_model(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await rm.list_signals(db, board_id=board)
    assert exc.value.code == "readiness_source_unavailable"
    assert exc.value.http_status == 503


# ---------------------------------------------------------------------------
# REST — rota registrada, 400 invalid_filter, happy-path, 503 mapeado
# ---------------------------------------------------------------------------


def test_route_is_registered():
    paths = {route.path for route in api_router.routes}
    assert "/api/v1/kg/{board_id}/cognitive-readiness/items" in paths


@pytest_asyncio.fixture
async def _client(tmp_path, db_factory):
    import uuid as _uuid
    suffix = _uuid.uuid4().hex[:8]
    board, gen = f"ac-rest-{suffix}", "gen-1"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_items(store, board, gen, [
        {"source_ref": f"bug:{UUID_A}", "status": CognitiveItemStatus.SKIPPED.value,
         "reason_code": "trivial_fix"},
    ])
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id=f"dlq-rest-{suffix}", board_id=board, artifact_type="card",
            artifact_id=UUID_C, original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    import okto_pulse.core.api.cognitive_action_center as ac_api

    def _svc():
        return CognitiveReadinessService(store, now=lambda: NOW)

    # injeta o store de teste no endpoint (sem tocar default_rebuild_base_dir).
    _orig = ac_api.build_default_readiness_service
    ac_api.build_default_readiness_service = _svc

    app = FastAPI()
    app.include_router(api_router)

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: "rest-user"
    try:
        yield TestClient(app), board, store
    finally:
        ac_api.build_default_readiness_service = _orig


def test_rest_happy_path(_client):
    client, board, _store = _client
    resp = client.get(f"/api/v1/kg/{board}/cognitive-readiness/items")
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body and "summary" in body
    assert body["precedence"] == list(PRECEDENCE_ORDER)
    arts = {it["artifact_id"] for it in body["items"]}
    assert f"card:{UUID_A}" in arts and f"card:{UUID_C}" in arts
    # DLQ técnico vem como blocking_technical.
    dlq = next(it for it in body["items"] if it["signal_source"] == SOURCE_DLQ)
    assert dlq["readiness_effect"] == "blocking_technical"
    # skip válido cognitivo.
    cog = next(it for it in body["items"] if it["signal_source"] == SOURCE_COGNITIVE_ITEM)
    assert cog["readiness_effect"] == "ready_skip"


def test_rest_invalid_filter_400(_client):
    client, board, _store = _client
    resp = client.get(
        f"/api/v1/kg/{board}/cognitive-readiness/items", params={"signal": "nope"}
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "invalid_filter"


def test_rest_source_unavailable_503(_client, monkeypatch):
    client, board, store = _client

    async def _boom_gather(self, *a, **k):
        raise RuntimeError("offline")

    monkeypatch.setattr(
        CognitiveActionCenterReadModel, "_gather", _boom_gather, raising=True
    )
    resp = client.get(f"/api/v1/kg/{board}/cognitive-readiness/items")
    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "readiness_source_unavailable"
