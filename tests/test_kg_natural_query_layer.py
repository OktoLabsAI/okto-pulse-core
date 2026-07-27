"""SPEC e2598178 / card 9a348ee0 / scenario ts_7928ed3b — natural KG query
graph_layer contract + metadata/null layer audit.

Proves the okto_pulse_kg_query_natural layer contract end-to-end:
* valid calls (default/working/all) echo ``applied_graph_layer``;
* an invalid graph_layer FAILS CLOSED with a structured error BEFORE any
  retrieval touches the graph (no silent fallback to old unfiltered behavior);
* graph_layer null/un-stamped nodes are labelled ``legacy_unknown`` and
  BoardMeta is labelled ``metadata`` — both reported in ``layer_audit`` but
  NEVER counted as canonical/working artifact leakage, and never returned under
  a canonical/working scope (only under ``all``).

KG Health note (card 9a348ee0, codex D4): these run against isolated, freshly
bootstrapped fixture graphs (KG Health = healthy by construction), so the
empirical layer verification executes here. The live-board recovery_needed
gate is the coordinator's/owner's responsibility, not a branch of the query.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_natural_query_layer.py
"""

from __future__ import annotations

import os
import tempfile

import pytest

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    _apply_graph_layer_to_natural_results,
    _classify_natural_layer,
    execute_natural_query,
)


def _board(name: str) -> str:
    os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_nlayer_"))
    from kg_schema_testing import bootstrap_board_graph

    bootstrap_board_graph(name)
    return name


def _seed_bug(board_id: str, node_id: str, title: str, layer: str | None) -> None:
    from kg_schema_testing import open_board_connection

    layer_clause = f", graph_layer: '{layer}'" if layer is not None else ""
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"CREATE (n:Bug {{id: '{node_id}', title: '{title}', "
            f"content: 'x', source_artifact_ref: 'bug:{node_id}', "
            f"source_confidence: 1.0, relevance_score: 0.5{layer_clause}}})"
        )


# ---------------------------------------------------------------------------
# applied_graph_layer echo (valid calls)
# ---------------------------------------------------------------------------


def test_default_layer_is_canonical_and_echoed():
    board_id = _board("nlayer-echo-default")
    _seed_bug(board_id, "bug_c", "echo canonical", "canonical")

    res = execute_natural_query(board_id, "echo", min_confidence=0.0)

    assert res["applied_graph_layer"] == "canonical"
    assert res["layer_audit"]["applied_graph_layer"] == "canonical"
    assert set(res["layer_audit"]["counts_by_layer"]) == {
        "canonical",
        "working",
        "legacy_unknown",
        "metadata",
    }


def test_working_and_all_layers_are_echoed():
    board_id = _board("nlayer-echo-wa")
    _seed_bug(board_id, "bug_c", "echo canonical", "canonical")

    for layer in ("working", "all"):
        res = execute_natural_query(
            board_id, "echo", min_confidence=0.0, graph_layer=layer
        )
        assert res["applied_graph_layer"] == layer
        assert res["layer_audit"]["applied_graph_layer"] == layer


# ---------------------------------------------------------------------------
# invalid graph_layer fails closed BEFORE execution
# ---------------------------------------------------------------------------


def test_invalid_graph_layer_fails_closed_before_execution():
    # Board never bootstrapped: the invalid graph_layer must be rejected at the
    # boundary BEFORE any retrieval would touch the (absent) graph. A structured
    # invalid_param error, not a graph/kuzu error, proves the order.
    with pytest.raises(TierPowerError) as exc_info:
        execute_natural_query(
            "nlayer-never-bootstrapped", "q", graph_layer="bogus"
        )
    assert exc_info.value.code == "invalid_param"


# ---------------------------------------------------------------------------
# null / metadata layer audit — counted but never leaked into canonical/working
# ---------------------------------------------------------------------------


def test_null_and_working_excluded_from_canonical_with_audit():
    board_id = _board("nlayer-exclusion")
    _seed_bug(board_id, "bug_canon", "Layerscope canonical", "canonical")
    _seed_bug(board_id, "bug_work", "Layerscope working", "working")
    _seed_bug(board_id, "bug_legacy", "Layerscope legacy", None)  # null layer

    rows = [
        {"node_id": "bug_canon", "node_type": "Bug", "title": "Layerscope canonical", "similarity": 0.9},
        {"node_id": "bug_work", "node_type": "Bug", "title": "Layerscope working", "similarity": 0.8},
        {"node_id": "bug_legacy", "node_type": "Bug", "title": "Layerscope legacy", "similarity": 0.7},
    ]

    # Default canonical: only the canonical node survives; working + null-layer
    # nodes are COUNTED in the audit but excluded from the result + leakage.
    kept, audit = _apply_graph_layer_to_natural_results(
        board_id, [dict(r) for r in rows], "canonical"
    )
    assert [r["node_id"] for r in kept] == ["bug_canon"]
    assert audit["counts_by_layer"] == {
        "canonical": 1,
        "working": 1,
        "legacy_unknown": 1,
        "metadata": 0,
    }
    # legacy_unknown/metadata never leak into the canonical/working result set.
    assert audit["non_artifact_excluded"] == 1
    assert all(r["node_id"] not in {"bug_work", "bug_legacy"} for r in kept)

    # working scope: only the working node.
    kept_w, _ = _apply_graph_layer_to_natural_results(
        board_id, [dict(r) for r in rows], "working"
    )
    assert [r["node_id"] for r in kept_w] == ["bug_work"]

    # all scope: every node surfaces, each labelled with its bucket.
    kept_all, _ = _apply_graph_layer_to_natural_results(
        board_id, [dict(r) for r in rows], "all"
    )
    assert {r["node_id"] for r in kept_all} == {"bug_canon", "bug_work", "bug_legacy"}
    by_id = {r["node_id"]: r["graph_layer"] for r in kept_all}
    assert by_id == {
        "bug_canon": "canonical",
        "bug_work": "working",
        "bug_legacy": "legacy_unknown",
    }


def test_classify_natural_layer_buckets():
    # canonical/working pass through; null/none/unknown are the conservative
    # legacy_unknown bucket; BoardMeta is non-artifact metadata regardless of
    # any layer value it might carry (ac_6fabaaec).
    assert _classify_natural_layer("Bug", "canonical") == "canonical"
    assert _classify_natural_layer("Requirement", "working") == "working"
    assert _classify_natural_layer("Bug", None) == "legacy_unknown"
    assert _classify_natural_layer("Bug", "none") == "legacy_unknown"
    assert _classify_natural_layer("Bug", "legacy_unknown") == "legacy_unknown"
    assert _classify_natural_layer("BoardMeta", "canonical") == "metadata"
    assert _classify_natural_layer("BoardMeta", None) == "metadata"
