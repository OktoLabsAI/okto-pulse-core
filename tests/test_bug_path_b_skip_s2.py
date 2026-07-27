"""S2 / card a32cd8c1 — Path B, no-action de bug e salvaguardas DLQ/debt.

Cenários (spec 6b91a862), todos sobre o write-path central
CognitiveReadinessService.record_cognitive_skip via o wrapper fino
record_bug_cognitive_skip (sem precedência/store/enum local):
  * ts_22aa3b7e — bug trivial/duplicado/sem learning reutilizável grava
    skipped + no_action_required com reason terminal, SEM Learning artificial.
  * ts_6b31aba2 — path_b_pending exige revisit_at (400 revisit_at_required);
    revisit_at vencido volta a bloquear readiness.
  * ts_9fa1bd98 — DLQ/canonical_debt técnico aberto para card:<uuid> NÃO pode ser
    mascarado por skip de bug:<uuid>; 409 technical_debt_cannot_be_skipped antes
    de qualquer escrita.
  * ts_c4f11007 — falha técnica (debt/DLQ aberto) bloqueia no_action; Path B
    consome PATH_B_SEMANTIC_GAP, distinto de PATH_C_HOTFIX_LANE.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.bug_cognitive_closure import (
    BugWorkflowRemediationPath,
    build_bug_path_b_remediation,
    record_bug_cognitive_skip,
)
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
    ReadinessTier,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from sqlalchemy_test_models import Board, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)
UUID_A = "11111111-1111-1111-1111-111111111111"


def _seed_bug_pending(store: CognitiveConsolidationItemStore, board: str, gen: str) -> None:
    store.materialize_from_marker(
        board_id=board,
        kg_generation_id=gen,
        event_ref="evt_bug_pathb",
        source_set=[{
            "artifact_type": "bug",
            "id": UUID_A,
            "source_ref": f"bug:{UUID_A}",
            "content_hash": "h-bug",
        }],
    )


async def _board(db_factory, board_id: str) -> None:
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="bug-pathb", owner_id="owner-b"))
            await db.commit()


def _service(store, *, now: datetime = NOW) -> CognitiveReadinessService:
    return CognitiveReadinessService(store, now=lambda: now)


def _item(store, board, gen):
    return store.list_items(board, gen)[0]


# ---------------------------------------------------------------------------
# ts_22aa3b7e — trivial/duplicate/no-reusable → no_action, sem Learning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["trivial_fix", "duplicate_bug", "no_reusable_learning"])
async def test_ts22aa3b7e_terminal_no_action_without_artificial_learning(
    tmp_path, db_factory, monkeypatch, reason,
):
    board, gen = f"bug-noact-{reason}", generate_kg_generation_id()
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_bug_pending(store, board, gen)

    # ledger-only: se o skip tocar o grafo KG (fabricar Learning/node), explode.
    import kg_schema_testing as schema_mod
    monkeypatch.setattr(
        schema_mod, "open_board_connection",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no_action skip não pode criar Learning/node no KG")
        ),
        raising=True,
    )

    svc = _service(store)
    async with db_factory() as db:
        item = await record_bug_cognitive_skip(
            svc, db, board_id=board, bug_id=UUID_A,
            reason_code=reason, actor="agent-b",
            justification="no reusable learning for this bug",
        )
    assert item.status == CognitiveItemStatus.SKIPPED.value
    assert item.outcome_type == CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value
    assert item.reason_code == reason
    assert item.artifact_id == f"card:{UUID_A}"
    # nenhuma outcome de Learning real foi fabricada.
    assert item.outcome_type != CognitivePendingOutcomeType.FORMAL_DECISION_PROMOTED.value
    assert item.generated_candidate_decision_ids == ()
    assert item.promoted_formal_decision_ids == ()


# ---------------------------------------------------------------------------
# ts_6b31aba2 — path_b_pending exige revisit_at; vencido volta a bloquear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts6b31aba2_path_b_pending_requires_revisit_at(tmp_path, db_factory):
    board, gen = "bug-pathb-revisit", generate_kg_generation_id()
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = _service(store)

    # sem revisit_at → 400 revisit_at_required, item permanece pending.
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await record_bug_cognitive_skip(
                svc, db, board_id=board, bug_id=UUID_A,
                reason_code="path_b_pending", actor="agent-b",
            )
    assert exc.value.code == "revisit_at_required" and exc.value.http_status == 400
    assert _item(store, board, gen).status == CognitiveItemStatus.PENDING.value

    # com revisit_at futuro → registrado; readiness desbloqueia (SKIP_VALID).
    async with db_factory() as db:
        await record_bug_cognitive_skip(
            svc, db, board_id=board, bug_id=UUID_A,
            reason_code="path_b_pending", actor="agent-b",
            revisit_at=(NOW + timedelta(days=2)).isoformat(),
        )
        v = await svc.evaluate_artifact(db, board_id=board, source_ref=f"bug:{UUID_A}")
    assert v.ready and v.tier == ReadinessTier.SKIP_VALID.value

    # tempo passou do revisit_at → readiness volta a bloquear (SKIP_EXPIRED).
    later = CognitiveReadinessService(store, now=lambda: NOW + timedelta(days=3))
    async with db_factory() as db:
        v2 = await later.evaluate_artifact(db, board_id=board, source_ref=f"bug:{UUID_A}")
    assert v2.blocking and v2.tier == ReadinessTier.SKIP_EXPIRED.value


# ---------------------------------------------------------------------------
# ts_9fa1bd98 — no-mask: DLQ/debt aberto bloqueia skip de bug com 409 sem escrita
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts9fa1bd98_open_dlq_blocks_bug_skip_409_no_write(tmp_path, db_factory):
    board, gen = "bug-nomask-dlq", generate_kg_generation_id()
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_bug_pending(store, board, gen)
    # DLQ técnico referencia o MESMO artefato como card:<uuid>.
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-bug", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()

    svc = _service(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await record_bug_cognitive_skip(
                svc, db, board_id=board, bug_id=UUID_A,
                reason_code="trivial_fix", actor="agent-b",
            )
    assert exc.value.code == "technical_debt_cannot_be_skipped"
    assert exc.value.http_status == 409
    # sem escrita: item permanece pending.
    assert _item(store, board, gen).status == CognitiveItemStatus.PENDING.value


# ---------------------------------------------------------------------------
# ts_c4f11007 — falha técnica → debt/DLQ (não no_action) + Path B distinto
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tsc4f11007_open_canonical_debt_blocks_no_action(tmp_path, db_factory):
    # Quando há evidência reutilizável mas a criação técnica falhou, isso vira
    # canonical_debt/DLQ (produzido em 13b43f3d). Aqui o seam: enquanto o debt
    # estiver OPEN, o no_action é BLOQUEADO (409), nunca mascarando a falha.
    board, gen = "bug-techfail", generate_kg_generation_id()
    await _board(db_factory, board)
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_bug_pending(store, board, gen)
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",  # OPEN (falha técnica)
        )
        await db.commit()

    svc = _service(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await record_bug_cognitive_skip(
                svc, db, board_id=board, bug_id=UUID_A,
                reason_code="no_reusable_learning", actor="agent-b",
            )
    assert exc.value.code == "technical_debt_cannot_be_skipped"
    assert _item(store, board, gen).status == CognitiveItemStatus.PENDING.value


def test_bug_path_b_remediation_consumes_semantic_gap_not_hotfix_lane():
    msg = build_bug_path_b_remediation()
    assert msg.remediation_path == BugWorkflowRemediationPath.PATH_B_SEMANTIC_GAP
    # distinto do hotfix lane (dec_085b0f9e): NUNCA fundir Path B com Path C.
    assert msg.remediation_path != BugWorkflowRemediationPath.PATH_C_HOTFIX_LANE
    assert msg.semantic_gap_required is True
