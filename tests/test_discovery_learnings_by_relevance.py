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
