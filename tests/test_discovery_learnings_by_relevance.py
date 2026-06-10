"""Intent learnings_by_relevance — todos os Learning nodes do board
ordenados por relevance_score desc (pedido do owner, 2026-06-10).

O handler lê o grafo direto (read-only, via to_thread) e degrada com
resultado vazio tipado quando o board não tem grafo."""

from __future__ import annotations

import os

import pytest

from okto_pulse.core.kg.schema import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)
from okto_pulse.core.services.discovery_executor import (
    _exec_learnings_by_relevance,
)


@pytest.fixture
def learning_board():
    bid = f"board-lbr-{os.urandom(3).hex()}"
    bootstrap_board_graph(bid)
    with open_board_connection(bid) as (_db, conn):
        for i, (title, score) in enumerate(
            (("low lesson", 0.3), ("top lesson", 1.2), ("mid lesson", 0.7))
        ):
            conn.execute(
                "CREATE (:Learning {id: $id, title: $title, content: $c, "
                "relevance_score: $s, source_artifact_ref: $ref, "
                "source_confidence: 0.9})",
                {
                    "id": f"learning_lbr_{i}",
                    "title": title,
                    "c": f"content {i}",
                    "s": score,
                    "ref": f"card:lbr-{i}",
                },
            )
    yield bid
    close_all_connections(bid)


@pytest.mark.asyncio
async def test_returns_all_learnings_ordered_by_relevance_desc(learning_board):
    result = await _exec_learnings_by_relevance(learning_board)

    assert result["execution"] == "real_tool"
    assert result["tool_binding"] == "okto_pulse_kg_list_learnings_by_relevance"
    titles = [r["title"] for r in result["rows"]]
    assert titles == ["top lesson", "mid lesson", "low lesson"]
    scores = [r["meta"]["relevance_score"] for r in result["rows"]]
    assert scores == sorted(scores, reverse=True)
    assert result["rows"][0]["meta"]["node_type"] == "Learning"


@pytest.mark.asyncio
async def test_board_without_graph_degrades_to_typed_empty():
    result = await _exec_learnings_by_relevance(
        "board-lbr-missing-" + os.urandom(3).hex()
    )
    # bootstrap-on-open cria grafo vazio OU o caminho degrada com warning;
    # em ambos os casos o shape é o contrato do renderer table.
    assert result["rows"] == []
    assert result["columns"] == ["Learning", "Relevance"]


@pytest.fixture
def decisions_board():
    bid = f"board-kd-{os.urandom(3).hex()}"
    bootstrap_board_graph(bid)
    with open_board_connection(bid) as (_db, conn):
        # 3 decisions: alta relevância/0 conexões, média/2 conexões, baixa/0
        for i, (title, score) in enumerate(
            (("lonely high", 1.2), ("connected mid", 0.8), ("lonely low", 0.3))
        ):
            conn.execute(
                "CREATE (:Decision {id: $id, title: $title, content: $c, "
                "relevance_score: $s, source_artifact_ref: $ref, "
                "source_confidence: 0.9})",
                {
                    "id": f"decision_kd_{i}",
                    "title": title,
                    "c": f"content {i}",
                    "s": score,
                    "ref": f"spec:kd:decision:dec_{i}",
                },
            )
        # conecta a decision do meio a 2 vizinhas (judgement edges)
        conn.execute(
            "MATCH (a:Decision {id:'decision_kd_1'}), (b:Decision {id:'decision_kd_0'}) "
            "CREATE (a)-[:depends_on {confidence: 0.9, created_by: 'test', layer: 'cognitive'}]->(b)"
        )
        conn.execute(
            "MATCH (a:Decision {id:'decision_kd_1'}), (b:Decision {id:'decision_kd_2'}) "
            "CREATE (a)-[:supersedes {confidence: 0.9, created_by: 'test', layer: 'cognitive'}]->(b)"
        )
    yield bid
    close_all_connections(bid)


@pytest.mark.asyncio
async def test_key_decisions_blend_relevance_and_connections(decisions_board):
    from okto_pulse.core.services.discovery_executor import _exec_key_decisions

    result = await _exec_key_decisions(decisions_board)

    assert result["execution"] == "real_tool"
    assert result["tool_binding"] == "okto_pulse_kg_list_key_decisions"
    rows = result["rows"]
    assert len(rows) == 3
    # combined desc: a "connected mid" (2 conexões, deg_norm=1) empata em
    # peso com a "lonely high" (rel_norm=1): 0.6*0.555+0.4*1=0.733 vs
    # 0.6*1+0.4*0.4(=1 conexão? não, lonely high tem 1 conexão incoming!)
    # -> validamos apenas a ordenação monotônica do combined_score.
    scores = [r["meta"]["combined_score"] for r in rows]
    assert scores == sorted(scores, reverse=True)
    # a decision mais conectada nunca é a última
    titles = [r["title"] for r in rows]
    assert titles[-1] == "lonely low"
    assert rows[0]["meta"]["connections"] >= 1
