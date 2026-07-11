"""Regressão (bug 06b05a25 / scenario ts_754aba25): uma spec em estado NÃO-final
— draft / review / approved / validated — não pode materializar no layer KG
``canonical``. A consulta canonical-only por ``source_artifact_ref`` tem que
retornar ZERO antes de a spec chegar a ``done``.

Diferente de ``test_kg_spec_children_canonicalization`` (que inspeciona a emissão
em memória do worker) e de ``test_kg_layer_propagation`` (que semeia nodes
direto), esta regressão exercita o caminho REAL de enqueue → consolidation →
materialization via ``_process_queue_entry``, persiste no grafo do board e LÊ com
o mesmo predicado canonical-only FAIL-CLOSED de produção, via o helper
centralizado ``cypher_templates.layer_filter_clause`` (``graph_layer =
'canonical'`` estrito; NULL/ausente não é canonical — bug 07bdf670).
Sem source-inspection: o veredito vem do grafo persistido em disco.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.kg.cypher_templates import layer_filter_clause
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.application.processors.consolidation import _process_queue_entry
from sqlalchemy_test_models import Board, ConsolidationQueue, Spec


# Estados não-finais: por CANONICAL_STATUS_BY_ARTIFACT_TYPE["spec"] == {"done"},
# qualquer um destes é working-only e não deve aparecer em canonical.
NON_FINAL_STATES = ["draft", "review", "approved", "validated"]


def _spec_payload(spec_id: str, board_id: str, status: str) -> dict:
    """Uma spec com famílias estruturadas suficientes para o worker emitir
    múltiplos child nodes com source_artifact_ref começando em ``spec:<id>``."""
    return dict(
        id=spec_id,
        board_id=board_id,
        title="Layer-aware partitioning spec",
        description="Spec de regressão para não-vazamento canonical por maturidade.",
        context="## Decisions\n- Use PostgreSQL\n",
        functional_requirements=[
            {"title": "FR-1", "text": "Particiona canonical só em done."},
            {"title": "FR-2", "text": "Working não vaza para canonical."},
        ],
        technical_requirements=[
            {"title": "TR-1", "text": "graph_layer respeita a maturidade da spec."}
        ],
        acceptance_criteria=[
            {"title": "AC-1", "text": "canonical-only por source = 0 antes de done."}
        ],
        business_rules=[
            {"title": "BR-1", "rule": "done é o único estado canonical da spec."}
        ],
        status=status,
        created_by="owner-regression",
    )


async def _materialize_spec(db_factory, status: str) -> tuple[str, str]:
    """Persiste board + spec(status) + queue entry e roda o worker REAL.

    Retorna ``(board_id, spec_id)``. Cada chamada usa um board novo para isolar
    a contagem por grafo (a asserção filtra por ``spec:<id>`` de qualquer forma).
    """
    board_id = f"canonleak-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as session:
        session.add(Board(id=board_id, name="canon-leak", owner_id="owner-regression"))
        session.add(Spec(**_spec_payload(spec_id, board_id, status)))
        session.add(
            ConsolidationQueue(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                priority="normal",
                source="event:spec.semantic_changed",
                triggered_by_event="spec.semantic_changed",
                status="pending",
            )
        )
        await session.commit()
        entry = (
            await session.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.artifact_id == spec_id
                )
            )
        ).scalar_one()
        ok = await _process_queue_entry(session, entry)
    assert ok is True, f"worker falhou ao consolidar spec status={status!r}"
    return board_id, spec_id


def _count_spec_nodes(board_id: str, spec_id: str, *, canonical_only: bool) -> int:
    """Conta nodes do grafo cujo source_artifact_ref pertence à spec.

    Quando ``canonical_only`` aplica o MESMO predicado FAIL-CLOSED de produção
    via o helper centralizado ``cypher_templates.layer_filter_clause`` (bug
    07bdf670: ``graph_layer = $graph_layer`` estrito, NULL/ausente NÃO é
    canonical) — idêntico a GET_ALL_NODES / get_related_context / query_global.
    """
    ref_prefix = f"spec:{spec_id}"
    cypher = "MATCH (n) WHERE n.source_artifact_ref STARTS WITH $ref "
    params: dict = {"ref": ref_prefix}
    if canonical_only:
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
@pytest.mark.parametrize("status", NON_FINAL_STATES)
async def test_non_final_spec_never_materializes_canonical(db_factory, status):
    """ts_754aba25 — spec em draft/review/approved/validated: canonical-only = 0."""
    board_id, spec_id = await _materialize_spec(db_factory, status)

    total = _count_spec_nodes(board_id, spec_id, canonical_only=False)
    canonical = _count_spec_nodes(board_id, spec_id, canonical_only=True)

    # A spec MATERIALIZA (no layer working): garante que o zero canonical abaixo
    # não é vacuoso (zero por nada ter sido escrito).
    assert total > 0, (
        f"spec status={status!r} não materializou nenhum node — asserção vacuosa"
    )
    # A regra do bug: nenhum node da spec não-madura pode ser canonical.
    assert canonical == 0, (
        f"VAZAMENTO canonical: spec em status={status!r} expôs {canonical} "
        f"de {total} node(s) no layer canonical (esperado 0 antes de done)"
    )


@pytest.mark.asyncio
async def test_done_spec_materializes_canonical_positive_control(db_factory):
    """Controle positivo: o predicado canonical-only TEM dentes — uma spec done
    expõe nodes canonical. Sem isto, o zero do teste acima poderia ser um falso
    verde (filtro errado retornando zero para tudo)."""
    board_id, spec_id = await _materialize_spec(db_factory, "done")

    total = _count_spec_nodes(board_id, spec_id, canonical_only=False)
    canonical = _count_spec_nodes(board_id, spec_id, canonical_only=True)

    assert total > 0, "spec done não materializou nenhum node"
    assert canonical > 0, (
        "spec done não expôs nenhum node canonical — o predicado canonical-only "
        "não tem dentes (o zero do caso não-final seria vacuoso)"
    )
    # Numa spec done, TODO node da spec é canonical (não há working remanescente).
    assert canonical == total, (
        f"spec done deveria materializar todos os {total} nodes como canonical, "
        f"mas só {canonical} são canonical"
    )
