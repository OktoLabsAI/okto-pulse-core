"""MKG-B C4 — attestation_boost in relevance scoring, dual-path (scenario S5).

Single pure formula (TR5) consumed by the tick recompute (_compute_relevance
via _fetch_node_inputs) AND the on-read reorder (_apply_decay_reorder /
find_by_topic). AC5: identical nodes differing only in attestation_count
order by corroboration; factor saturates at 1.5x; AC7: NULL reads as 1 and
is exactly neutral.
"""

from __future__ import annotations

import gc
import math
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.scoring import (
    ATTESTATION_BOOST_CAP,
    _apply_decay_reorder,
    _compute_relevance,
    _recompute_relevance,
    attestation_boost,
)

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_materialized_board_connection,
    open_board_connection,
)


@pytest.fixture
def kg_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_attboost_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


# ---------------------------------------------------------------------------
# Pure formula (FR6, D4)
# ---------------------------------------------------------------------------


def test_boost_neutral_at_baseline_and_null():
    # AC7 — NULL reads as 1; count<=1 is EXACTLY neutral (calibration of D4:
    # a single attestation carries no corroboration).
    assert attestation_boost(None) == 1.0
    assert attestation_boost(1) == 1.0
    assert attestation_boost(0) == 1.0  # defensive coercion


def test_boost_monotonic_and_saturating():
    values = [attestation_boost(c) for c in (1, 2, 5, 20, 100, 1000, 10**6)]
    assert values == sorted(values)
    assert attestation_boost(20) == pytest.approx(1 + 0.1 * math.log(20))
    # AC5 — saturation: never exceeds 1.5x even at count=1000.
    assert attestation_boost(1000) == ATTESTATION_BOOST_CAP == 1.5
    assert attestation_boost(10**6) == 1.5


def test_compute_relevance_orders_by_attestation():
    base = _compute_relevance(0.7, 4, 0.0, 0.0, attestation_count=1)
    boosted = _compute_relevance(0.7, 4, 0.0, 0.0, attestation_count=20)
    assert boosted > base
    assert boosted == pytest.approx(base * attestation_boost(20))
    # Backward compat: omitted == None == count 1.
    assert _compute_relevance(0.7, 4, 0.0, 0.0) == base


def test_compute_relevance_boost_respects_global_clamp():
    got = _compute_relevance(1.0, 100, 10.0, 0.0, attestation_count=1000)
    assert got == 1.5


# ---------------------------------------------------------------------------
# Read-path reorder (TR5 — same factor, on-read)
# ---------------------------------------------------------------------------


def _row(node_id: str, count):
    return {
        "node_id": node_id,
        "relevance_score": 0.9,
        "query_hits": 0,
        "last_queried_at": None,
        "attestation_count": count,
    }


def test_reorder_ranks_attested_above_identical_unattested():
    rows = [_row("plain", 1), _row("attested", 20)]
    out = _apply_decay_reorder(rows, 2)
    assert [r["node_id"] for r in out] == ["attested", "plain"]
    # Legacy rows without the key stay neutral (no KeyError, no shift).
    legacy = {k: v for k, v in _row("legacy", 1).items() if k != "attestation_count"}
    out2 = _apply_decay_reorder([legacy, _row("attested", 20)], 2)
    assert [r["node_id"] for r in out2] == ["attested", "legacy"]


# ---------------------------------------------------------------------------
# Real graph — recompute (tick path) + find_by_topic (read path)
# ---------------------------------------------------------------------------


def _seed_decision(kconn, node_id: str, count) -> None:
    kconn.execute(
        "CREATE (n:Decision {id: $id, title: 'Fato corroborado', content: 'c',"
        " source_confidence: 0.8, relevance_score: 0.9, query_hits: 0,"
        " graph_layer: 'canonical', source_artifact_ref: 'spec:attboost',"
        " attestation_count: $count})",
        {"id": node_id, "count": count},
    )


def test_s5_recompute_persists_higher_score_for_attested_node(kg_board):
    conn_ctx = open_materialized_board_connection(kg_board)
    with conn_ctx as (_kdb, kconn):
        _seed_decision(kconn, "decision_plain", 1)
        _seed_decision(kconn, "decision_attested", 20)

        plain = _recompute_relevance(kconn, kg_board, "Decision", "decision_plain")
        attested = _recompute_relevance(
            kconn, kg_board, "Decision", "decision_attested"
        )
        assert attested > plain
        assert attested == pytest.approx(plain * attestation_boost(20))

        # Persisted scores reflect the boost (tick path writes back).
        res = kconn.execute(
            "MATCH (n:Decision) WHERE n.source_artifact_ref = 'spec:attboost' "
            "RETURN n.id, n.relevance_score"
        )
        scores = {row[0]: float(row[1]) for row in res.rows}
        assert scores["decision_attested"] > scores["decision_plain"]


def test_s5_null_attestation_reads_as_one_in_recompute(kg_board):
    conn_ctx = open_materialized_board_connection(kg_board)
    with conn_ctx as (_kdb, kconn):
        # NULL column (legacy row shape) — AC7: reads as 1, never fails.
        kconn.execute(
            "CREATE (n:Decision {id: 'decision_null', title: 'Legado',"
            " content: 'c', source_confidence: 0.8, relevance_score: 0.9,"
            " query_hits: 0, graph_layer: 'canonical'})"
        )
        _seed_decision(kconn, "decision_one", 1)
        null_score = _recompute_relevance(
            kconn, kg_board, "Decision", "decision_null"
        )
        one_score = _recompute_relevance(kconn, kg_board, "Decision", "decision_one")
        assert null_score is not None
        assert null_score == pytest.approx(one_score)


def test_s5_find_by_topic_orders_by_attestation_on_read(kg_board):
    from okto_pulse.community.adapters.kuzu_graph_store import (
        CommunityKuzuGraphStore,
    )
    from okto_pulse.core.kg.interfaces.graph_store import QueryFilters

    conn_ctx = open_board_connection(kg_board)
    with conn_ctx as (_kdb, kconn):
        _seed_decision(kconn, "decision_plain", 1)
        _seed_decision(kconn, "decision_attested", 20)

    store = CommunityKuzuGraphStore()
    rows = store.find_by_topic(kg_board, "Decision", "Fato", QueryFilters())
    ids = [r[0] for r in rows]
    assert ids.index("decision_attested") < ids.index("decision_plain")
