"""S1.2 / card 3326e3e4 — CognitiveReadinessService + skip/no_action ledger-only.

Cenários (Cognitive Closure S1, spec 2012f38d):
  * ts_0627174f — precedência completa dos 6 tiers (puro, determinístico).
  * ts_28af4e42 — registry fechado rejeita reason desconhecido e sinal técnico,
    sem escrita.
  * ts_0bafd8e6 — skip NÃO mascara DLQ/canonical debt: 409 sem escrita.
  * ts_d8123753 — skip ledger-only: grava via store.update_item, NUNCA abre o
    grafo KG / connectivity guard.
  * ts_66b34cda — skip revisit-required vencido volta a bloquear readiness.
  * ts_59247a00 — open debt bloqueia; terminal committed history informa;
    task/test sem cognição reutilizável fica advisory/non-blocking.
"""

from __future__ import annotations

import json

import pytest

from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
    ReadinessTier,
    compose_readiness,
    validate_skip_reason,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    compute_cognitive_item_id,
)
from okto_pulse.core.models.db import Board, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

from datetime import datetime, timedelta, timezone

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)
UUID_A = "11111111-1111-1111-1111-111111111111"
ARTIFACT = f"card:{UUID_A}"


def _item(status, *, reason_code=None, revisit_at=None, source_ref=f"bug:{UUID_A}"):
    return CognitiveConsolidationItem(
        item_id="x", board_id="b", kg_generation_id="g",
        source_ref=source_ref, artifact_type="bug",
        status=status, recorded_at="t",
        reason_code=reason_code, revisit_at=revisit_at,
    )


# ---------------------------------------------------------------------------
# ts_0627174f — precedência completa (puro)
# ---------------------------------------------------------------------------


def test_ts0627174f_six_tier_precedence():
    base = dict(artifact_id=ARTIFACT, now=NOW)
    skip_valid = _item(CognitiveItemStatus.SKIPPED.value, reason_code="trivial_fix")
    active = _item(CognitiveItemStatus.PENDING.value)

    # Tier 1: DLQ vence até skip válido + ausência de item (skip não mascara).
    v = compose_readiness(**base, technical_dlq=True, canonical_debt_open=True,
                          cognitive_items=[skip_valid, active])
    assert v.tier == ReadinessTier.TECHNICAL_DLQ.value and v.blocking

    # Tier 2: canonical_debt OPEN vence cognitive/skip.
    v = compose_readiness(**base, technical_dlq=False, canonical_debt_open=True,
                          cognitive_items=[skip_valid, active])
    assert v.tier == ReadinessTier.CANONICAL_DEBT_OPEN.value and v.blocking

    # Tier 3: cognitive ativo bloqueia (sem debt/dlq).
    v = compose_readiness(**base, technical_dlq=False, canonical_debt_open=False,
                          cognitive_items=[active, skip_valid])
    assert v.tier == ReadinessTier.COGNITIVE_ACTIVE.value and v.blocking

    # Tier 4: skip revisit-required vencido re-bloqueia.
    expired = _item(CognitiveItemStatus.SKIPPED.value, reason_code="evidence_insufficient",
                    revisit_at=(NOW - timedelta(hours=1)).isoformat())
    v = compose_readiness(**base, technical_dlq=False, canonical_debt_open=False,
                          cognitive_items=[expired])
    assert v.tier == ReadinessTier.SKIP_EXPIRED.value and v.blocking

    # Tier 5: skip válido desbloqueia.
    v = compose_readiness(**base, technical_dlq=False, canonical_debt_open=False,
                          cognitive_items=[skip_valid])
    assert v.tier == ReadinessTier.SKIP_VALID.value and v.ready and not v.blocking

    # Tier 6: terminal committed history informa (não bloqueia).
    v = compose_readiness(**base, technical_dlq=False, canonical_debt_open=False,
                          cognitive_items=[_item(CognitiveItemStatus.CONSOLIDATED.value)])
    assert v.tier == ReadinessTier.TERMINAL_HISTORY.value and v.ready and not v.blocking


def test_skip_does_not_mask_open_debt_in_composition():
    # mesmo com skip válido, debt OPEN vence (br_658945e6).
    v = compose_readiness(
        artifact_id=ARTIFACT, now=NOW, technical_dlq=False, canonical_debt_open=True,
        cognitive_items=[_item(CognitiveItemStatus.SKIPPED.value, reason_code="duplicate_bug")],
    )
    assert v.tier == ReadinessTier.CANONICAL_DEBT_OPEN.value and v.blocking


# ---------------------------------------------------------------------------
# ts_28af4e42 — registry fechado rejeita sem escrita (validação pura)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["technical_dlq", "canonical_debt_open", "connectivity_guard", "made_up", ""])
def test_ts28af4e42_registry_rejects_unknown_and_technical(bad):
    with pytest.raises(CognitiveReadinessError) as exc:
        validate_skip_reason(bad, (NOW + timedelta(days=1)).isoformat(), now=NOW)
    assert exc.value.code == "invalid_reason_code"
    assert exc.value.http_status == 400


@pytest.mark.parametrize("reason", ["root_cause_unconfirmed", "evidence_insufficient",
                                    "path_b_pending", "external_context_missing"])
def test_revisit_required_needs_future_revisit_at(reason):
    # ausente
    with pytest.raises(CognitiveReadinessError) as e1:
        validate_skip_reason(reason, None, now=NOW)
    assert e1.value.code == "revisit_at_required" and e1.value.http_status == 400
    # passado
    with pytest.raises(CognitiveReadinessError) as e2:
        validate_skip_reason(reason, (NOW - timedelta(hours=1)).isoformat(), now=NOW)
    assert e2.value.code == "revisit_at_required"
    # futuro → ok
    validate_skip_reason(reason, (NOW + timedelta(days=1)).isoformat(), now=NOW)


@pytest.mark.parametrize("reason", ["no_reusable_learning", "duplicate_bug", "trivial_fix"])
def test_terminal_reason_codes_need_no_revisit_at(reason):
    validate_skip_reason(reason, None, now=NOW)  # não levanta


# ---------------------------------------------------------------------------
# Helpers async (store file-backed + DB real)
# ---------------------------------------------------------------------------


def _seed_pending_item(store, board, gen, source_ref):
    path = store._record_path(board, gen)
    path.parent.mkdir(parents=True, exist_ok=True)
    iid = compute_cognitive_item_id(board, gen, source_ref)
    record = {
        "pending_count": 1,
        "pending_refs": [source_ref],
        "status": "pending",
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "items": [{
            "item_id": iid, "board_id": board, "kg_generation_id": gen,
            "source_ref": source_ref, "artifact_type": source_ref.split(":", 1)[0],
            "status": CognitiveItemStatus.PENDING.value,
            "recorded_at": "2026-06-17T00:00:00+00:00",
        }],
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(record, fh)
    return iid


async def _board(db_factory, board_id):
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="readiness", owner_id="owner-r"))
            await db.commit()


def _service(store):
    return CognitiveReadinessService(store, now=lambda: NOW)


# ---------------------------------------------------------------------------
# ts_d8123753 — skip ledger-only não abre o grafo KG / connectivity guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_d8123753_skip_is_ledger_only(tmp_path, db_factory, monkeypatch):
    board, gen = "rd-ledger", "gen-1"
    src = f"bug:{UUID_A}"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_pending_item(store, board, gen, src)

    # Teeth: se o skip tocar o grafo KG, isto explode.
    import okto_pulse.core.kg.schema as schema_mod

    def _boom(*_a, **_k):
        raise AssertionError("skip ledger-only NÃO pode abrir o grafo KG")

    monkeypatch.setattr(schema_mod, "open_board_connection", _boom, raising=True)

    svc = _service(store)
    async with db_factory() as db:
        item = await svc.record_cognitive_skip(
            db, board_id=board, source_ref=src,
            reason_code="trivial_fix", actor="agent-x",
            justification="nothing reusable",
        )
    assert item.status == CognitiveItemStatus.SKIPPED.value
    assert item.outcome_type == CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value
    assert item.reason_code == "trivial_fix"
    assert item.artifact_id == ARTIFACT
    # ledger persistido + nenhum grafo de board criado.
    persisted = {i.item_id: i for i in store.list_items(board, gen)}
    iid = compute_cognitive_item_id(board, gen, src)
    assert persisted[iid].status == CognitiveItemStatus.SKIPPED.value


# ---------------------------------------------------------------------------
# ts_0bafd8e6 — skip não mascara DLQ/canonical debt: 409 sem escrita
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_0bafd8e6_skip_blocked_by_dlq_409_no_write(tmp_path, db_factory):
    board, gen = "rd-dlq", "gen-1"
    src = f"bug:{UUID_A}"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    iid = _seed_pending_item(store, board, gen, src)

    async with db_factory() as db:
        # DLQ para card:<uuid> (mesma artifact normalizada que bug:<uuid>).
        db.add(ConsolidationDeadLetter(
            id="dlq-1", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    svc = _service(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await svc.record_cognitive_skip(
                db, board_id=board, source_ref=src,
                reason_code="trivial_fix", actor="agent-x",
            )
    assert exc.value.code == "technical_debt_cannot_be_skipped"
    assert exc.value.http_status == 409
    # sem escrita: item continua pending.
    assert {i.item_id: i for i in store.list_items(board, gen)}[iid].status == (
        CognitiveItemStatus.PENDING.value
    )


@pytest.mark.asyncio
async def test_skip_blocked_by_open_canonical_debt_409_no_write(tmp_path, db_factory):
    board, gen = "rd-debt", "gen-1"
    src = f"bug:{UUID_A}"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    iid = _seed_pending_item(store, board, gen, src)

    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",  # OPEN
        )
        await db.commit()

    svc = _service(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await svc.record_cognitive_skip(
                db, board_id=board, source_ref=src,
                reason_code="duplicate_bug", actor="agent-x",
            )
    assert exc.value.code == "technical_debt_cannot_be_skipped"
    assert {i.item_id: i for i in store.list_items(board, gen)}[iid].status == (
        CognitiveItemStatus.PENDING.value
    )


# ---------------------------------------------------------------------------
# ts_66b34cda — skip revisit-required vencido volta a bloquear (via service)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_66b34cda_expired_revisit_skip_reblocks(tmp_path, db_factory):
    board, gen = "rd-revisit", "gen-1"
    src = f"bug:{UUID_A}"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_pending_item(store, board, gen, src)
    svc = _service(store)

    # registra skip revisit-required com revisit_at FUTURO → desbloqueia.
    async with db_factory() as db:
        await svc.record_cognitive_skip(
            db, board_id=board, source_ref=src,
            reason_code="path_b_pending", actor="agent-x",
            revisit_at=(NOW + timedelta(days=1)).isoformat(),
        )
        v_valid = await svc.evaluate_artifact(db, board_id=board, source_ref=src)
    assert v_valid.ready and v_valid.tier == ReadinessTier.SKIP_VALID.value

    # agora o "tempo passou": serviço com now após o revisit_at → re-bloqueia.
    later = NOW + timedelta(days=2)
    svc_later = CognitiveReadinessService(store, now=lambda: later)
    async with db_factory() as db:
        v_expired = await svc_later.evaluate_artifact(db, board_id=board, source_ref=src)
    assert v_expired.blocking and v_expired.tier == ReadinessTier.SKIP_EXPIRED.value


# ---------------------------------------------------------------------------
# ts_59247a00 — open debt bloqueia; terminal informa; task/test advisory
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts_59247a00_open_debt_blocks_and_advisory(tmp_path, db_factory):
    board, gen = "rd-mix", "gen-1"
    src = f"bug:{UUID_A}"
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_pending_item(store, board, gen, src)
    svc = _service(store)

    # open debt → bloqueia
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="blocked",  # OPEN
        )
        await db.commit()
    async with db_factory() as db:
        v = await svc.evaluate_artifact(db, board_id=board, source_ref=src)
    assert v.blocking and v.tier == ReadinessTier.CANONICAL_DEBT_OPEN.value

    # debt committed (terminal) → não bloqueia mais; sem item ativo, sem cognição
    # reutilizável → advisory.
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="committed",  # TERMINAL
        )
        await db.commit()
    # artefato SEM item cognitivo (outro uuid) + sem cognição reutilizável → advisory
    other = "bug:22222222-2222-2222-2222-222222222222"
    async with db_factory() as db:
        v_adv = await svc.evaluate_artifact(
            db, board_id=board, source_ref=other, has_reusable_cognition=False,
        )
    assert v_adv.ready and not v_adv.blocking
    assert v_adv.tier == ReadinessTier.ADVISORY_NO_COGNITION.value
