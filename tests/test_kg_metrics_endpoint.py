"""Tests for GET /api/v1/kg/boards/{id}/metrics.

Covers:
- Empty graph → all ratios = 0, kg_bootstrapped=False for missing board.
- Mixed-layer graph → ratios computed correctly.
- rule_id histogram reflects the emitter used.
"""

from __future__ import annotations

import gc
import os

import pytest

from okto_pulse.core.kg.connection_pool import reset_connection_pool_for_tests
from okto_pulse.core.kg.schema import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def board():
    bid = f"board-metrics-{os.urandom(4).hex()}"
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections()
    reset_connection_pool_for_tests()


def _seed_mixed_graph(board_id: str) -> None:
    """Insert 2 Decisions + 3 Requirements + edges spread across layers."""
    def seed():
        with open_board_connection(board_id) as (_db, conn):
            for i in range(2):
                conn.execute(
                    f"CREATE (n:Decision {{id: 'd{i}', title: 'd{i}', content: 'c'}})"
                )
            for i in range(3):
                conn.execute(
                    f"CREATE (n:Requirement {{id: 'r{i}', title: 'r{i}', content: 'c'}})"
                )
            # 3 deterministic derives_from
            for i, (d, r) in enumerate([(0,0), (0,1), (1,2)]):
                conn.execute(
                    f"MATCH (a:Decision {{id: 'd{d}'}}), "
                    f"(b:Requirement {{id: 'r{r}'}}) "
                    f"CREATE (a)-[:derives_from {{confidence: 1.0, "
                    f"created_by_session_id: 'sess_w', "
                    f"created_at: timestamp('2026-04-17T12:00:00'), "
                    f"layer: 'deterministic', rule_id: 'derives_from/v2', "
                    f"created_by: 'worker_layer1', fallback_reason: ''}}]->(b)"
                )
            # 1 cognitive depends_on (Decision→Decision)
            conn.execute(
                "MATCH (a:Decision {id: 'd0'}), (b:Decision {id: 'd1'}) "
                "CREATE (a)-[:depends_on {confidence: 0.8, "
                "created_by_session_id: 'sess_c', "
                "created_at: timestamp('2026-04-17T12:01:00'), "
                "layer: 'cognitive', rule_id: 'depends_on/llm_v1', "
                "created_by: 'sess_c', fallback_reason: ''}]->(b)"
            )
            # 1 fallback mentions (Decision→Entity) — needs Entity first
            conn.execute(
                "CREATE (n:Entity {id: 'e1', title: 'Redis', content: 'Redis'})"
            )
            conn.execute(
                "MATCH (a:Decision {id: 'd0'}), (b:Entity {id: 'e1'}) "
                "CREATE (a)-[:mentions {confidence: 0.85, "
                "created_by_session_id: 'sess_f', "
                "created_at: timestamp('2026-04-17T12:02:00'), "
                "layer: 'fallback', rule_id: 'mentions/cognitive_v1', "
                "created_by: 'sess_f', fallback_reason: 'no_tech_match'}]->(b)"
            )
    seed()
    gc.collect()


def test_metrics_empty_board(board):
    from okto_pulse.core.api.kg_routes import get_kg_metrics
    import asyncio
    result = asyncio.run(get_kg_metrics(board))
    assert result["kg_bootstrapped"] is True
    assert result["edges_total"] == 0
    assert result["deterministic_edge_ratio"] == 0.0
    assert result["fallback_edge_ratio"] == 0.0


def test_metrics_missing_board_returns_noop():
    from okto_pulse.core.api.kg_routes import get_kg_metrics
    import asyncio
    result = asyncio.run(get_kg_metrics("nonexistent-xxx"))
    assert result["kg_bootstrapped"] is False
    assert result["edges_total"] == 0


def test_metrics_mixed_layers(board):
    from okto_pulse.core.api.kg_routes import get_kg_metrics
    import asyncio
    _seed_mixed_graph(board)
    result = asyncio.run(get_kg_metrics(board))
    assert result["edges_total"] == 5
    assert result["edge_count_by_layer"]["deterministic"] == 3
    assert result["edge_count_by_layer"]["cognitive"] == 1
    assert result["edge_count_by_layer"]["fallback"] == 1
    assert result["deterministic_edge_ratio"] == 0.6
    assert result["cognitive_edge_ratio"] == 0.2
    assert result["fallback_edge_ratio"] == 0.2
    # rule histogram
    assert result["edge_count_by_rule"]["derives_from/v2"] == 3


def test_metrics_node_counts(board):
    from okto_pulse.core.api.kg_routes import get_kg_metrics
    import asyncio
    _seed_mixed_graph(board)
    result = asyncio.run(get_kg_metrics(board))
    assert result["node_count_by_type"]["Decision"] == 2
    assert result["node_count_by_type"]["Requirement"] == 3
    assert result["node_count_by_type"]["Entity"] == 1


def test_metrics_includes_health_targets(board):
    from okto_pulse.core.api.kg_routes import get_kg_metrics
    import asyncio
    result = asyncio.run(get_kg_metrics(board))
    ht = result["health_targets"]
    assert ht["deterministic_edge_ratio_min"] == 0.70
    assert ht["fallback_edge_ratio_max"] == 0.15
    assert ht["cognitive_edge_ratio_target_range"] == [0.15, 0.30]
