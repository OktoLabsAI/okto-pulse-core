"""Regressão (bug 3e532069 / scenario ts_2d919388): um card aprovado por
``submit_task_validation`` (validation→done) tem que PROMOVER para canonical no KG.

Defeito declarado: a transição validation→done via gate de validação persistia
``card.status=done`` SEM publicar ``card.moved``; o ConsolidationEnqueuer escuta
``card.moved`` / ``card.conclusion_added``, então sem o evento o node
``card:<id>`` ficava ``working`` mesmo depois de done (canonical-only por
source_artifact_ref retornava zero).

Esta regressão prova a cadeia COMPLETA, end-to-end, pelos caminhos de produção:

  submit_task_validation(approve) → DomainEventRow card.moved (validation→done)
  → EventDeliveryProcessor drena → ConsolidationQueue(card) → _process_queue_entry
  → card:<id> aparece em canonical-only com graph_layer=canonical.

Dentes: sem o evento card.moved não há enqueue; sem enqueue o node permanece
working e a asserção canonical-only (predicado fail-closed centralizado) falha.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.kg.cypher_templates import layer_filter_clause
from okto_pulse.core.kg.rebuild_sources import RebuildSourceEnumerator
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.application.processors.consolidation import _process_queue_entry
from sqlalchemy_test_models import (
    Board,
    ConsolidationQueue,
    DomainEventRow,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardCreate, CardMove
from sqlalchemy_test_models import CardStatus
from okto_pulse.core.services.main import CardService
from okto_pulse.core.services.resource_gate import ResourceGateService
from kg_registry_testing import configure_real_graph_test_kg_registry
from sqlalchemy_domain_event_delivery_store import build_test_event_processor


async def _seed_board_spec_card(db_factory, board_id: str, spec_id: str) -> str:
    """Board + spec(approved) + 1 card normal em in_progress. Retorna card_id."""
    async with db_factory() as db:
        db.add(Board(id=board_id, name="valdone", owner_id="owner-valdone"))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Validation→done spec",
                status=SpecStatus.APPROVED,
                created_by="owner-valdone",
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
            )
        )
        await db.commit()

    async with db_factory() as db:
        svc = CardService(db)
        card = await svc.create_card(
            board_id,
            "owner-valdone",
            CardCreate(
                title="Impl card promovido por validation gate",
                status=CardStatus.IN_PROGRESS,
                spec_id=spec_id,
            ),
        )
        card_id = card.id
        await db.commit()
    return card_id


async def _approve_via_validation_gate(db_factory, board_id: str, card_id: str) -> dict:
    """move→validation (com executor report) + resources N/A + submit_task_validation
    approve, tudo numa sessão; commit ao final para persistir os DomainEventRow."""
    async with db_factory() as db:
        svc = CardService(db)
        await svc.move_card(
            card_id,
            "owner-valdone",
            CardMove(
                status=CardStatus.VALIDATION,
                conclusion="Implementado e pronto para validação",
                completeness=100,
                completeness_justification="Todos os AC implementados",
                drift=0,
                drift_justification="Sem desvio do plano",
            ),
        )
        rg = ResourceGateService(db)
        for rt in ("architecture", "mockup", "knowledge_base"):
            await rg.mark_not_applicable(
                board_id, "card", card_id, rt, "owner-valdone",
                justification=f"{rt} n/a nesta regressão", source_channel="ui",
            )
        result = await svc.submit_task_validation(
            card_id,
            "reviewer-valdone",
            "Reviewer Valdone",
            {
                "confidence": 95,
                "confidence_justification": "Revisei implementação e testes",
                "estimated_completeness": 100,
                "completeness_justification": "Tudo presente",
                "estimated_drift": 0,
                "drift_justification": "Segue o plano",
                "general_justification": "Aprovado após revisar o executor report.",
                "recommendation": "approve",
            },
        )
        await db.commit()
    return result


def _count_nodes(board_id: str, ref_prefix: str, *, canonical_only: bool) -> int:
    cypher = "MATCH (n) WHERE n.source_artifact_ref STARTS WITH $ref "
    params: dict = {"ref": ref_prefix}
    if canonical_only:
        # Mesmo predicado fail-closed de produção (bug 07bdf670).
        cypher += f"AND {layer_filter_clause('n')} "
        params["graph_layer"] = "canonical"
    cypher += "RETURN count(n)"
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(cypher, params)
        try:
            return int(res.get_next()[0]) if res.has_next() else 0
        finally:
            try:
                res.close()
            except Exception:
                pass


@pytest.mark.asyncio
async def test_validation_gate_emits_card_moved_done(db_factory):
    """ts_2d919388 (parte 1): submit_task_validation approve PUBLICA o
    DomainEventRow card.moved (validation→done) que o ConsolidationEnqueuer consome."""
    board_id = f"valdone-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    card_id = await _seed_board_spec_card(db_factory, board_id, spec_id)

    result = await _approve_via_validation_gate(db_factory, board_id, card_id)
    assert result["outcome"] == "success"

    async with db_factory() as db:
        rows = (
            await db.execute(
                select(DomainEventRow).where(
                    DomainEventRow.board_id == board_id,
                    DomainEventRow.event_type == "card.moved",
                )
            )
        ).scalars().all()

    done_moves = [
        r for r in rows
        if (r.payload_json or {}).get("from_status") == "validation"
        and (r.payload_json or {}).get("to_status") == "done"
        and (r.payload_json or {}).get("card_id") == card_id
    ]
    assert done_moves, (
        "submit_task_validation aprovado NÃO publicou card.moved validation→done "
        "(o ConsolidationEnqueuer nunca re-enfileiraria o card)"
    )


@pytest.mark.asyncio
async def test_validation_gate_promotes_card_to_canonical(db_factory):
    """ts_2d919388 (parte 2): após o evento, o enqueuer cria ConsolidationQueue(card)
    e, drenada a fila, card:<id> aparece em canonical-only (graph_layer=canonical)."""
    configure_real_graph_test_kg_registry()
    board_id = f"valdone-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    bootstrap_board_graph(board_id)
    card_id = await _seed_board_spec_card(db_factory, board_id, spec_id)

    await _approve_via_validation_gate(db_factory, board_id, card_id)

    # O ConsolidationEnqueuer consome os DomainEventRow via dispatcher.
    dispatcher = build_test_event_processor(db_factory)
    try:
        await dispatcher.recover_orphans()
        await dispatcher.process_batch()
        await asyncio.sleep(0.5)
    finally:
        pass
    async with db_factory() as db:
        card_q = (
            await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "card",
                    ConsolidationQueue.artifact_id == card_id,
                )
            )
        ).scalars().all()
    assert card_q, (
        "ConsolidationEnqueuer não enfileirou o card após card.moved "
        "(defeito de re-enfileiramento na transição via validation gate)"
    )

    # Drena a entry do card pelo worker real.
    async with db_factory() as db:
        entry = (
            await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "card",
                    ConsolidationQueue.artifact_id == card_id,
                )
            )
        ).scalars().first()
        ok = await _process_queue_entry(db, entry)
    assert ok is True, "worker falhou ao consolidar o card done"

    ref_prefix = f"card:{card_id}"
    total = _count_nodes(board_id, ref_prefix, canonical_only=False)
    canonical = _count_nodes(board_id, ref_prefix, canonical_only=True)

    assert total > 0, "o card não materializou nenhum node (teste vacuoso)"
    assert canonical > 0, (
        "card done NÃO promoveu para canonical no KG — canonical-only por "
        f"{ref_prefix!r} retornou zero (o node ficou working)"
    )


def test_ts2_manifest_includes_done_card_as_canonical():
    """Cenário ts_2d919388 (TS2 — manifest canonical inclui apenas artefatos
    maduros): o RebuildSourceEnumerator coloca um card done em canonical_sources
    com graph_layer=canonical e maturity_status=canonical_eligible; o card
    não-maduro fica fora do canonical."""
    rows = [
        {
            "artifact_type": "task",
            "id": "t-done",
            "source_ref": "task:t-done",
            "source_version": "v1",
            "content_hash": "h-done",
            "created_at": "2026-05-01T00:00:00Z",
            "status": "done",
        },
        {
            "artifact_type": "task",
            "id": "t-prog",
            "source_ref": "task:t-prog",
            "source_version": "v1",
            "content_hash": "h-prog",
            "created_at": "2026-05-01T00:00:01Z",
            "status": "in_progress",
        },
    ]
    result = RebuildSourceEnumerator(source_store=lambda _b: rows).enumerate(board_id="b1")

    canonical = {r.source_ref: r for r in result.sources}
    assert "task:t-done" in canonical, "card done não entrou em canonical_sources"
    done_row = canonical["task:t-done"]
    assert done_row.graph_layer == "canonical"
    assert done_row.maturity_status == "canonical_eligible"
    assert done_row.artifact_type == "task"
    assert done_row.source_artifact_status == "done"
    assert done_row.content_hash == "h-done"
    assert done_row.source_version == "v1"
    # O card não-maduro (in_progress) NÃO pode estar em canonical.
    assert "task:t-prog" not in canonical
