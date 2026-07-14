"""MKG-C C5 — query-time equivalence fold (scenario S5).

Pure fold helpers + real-service behavior: with an active equivalence the
recall surfaces present the survivor in place of members; after revoke the
members reappear (cache invalidated); raw cypher is not intercepted (the
doc records the exclusion).
"""

from __future__ import annotations

import asyncio
import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.equivalence_fold import (
    fold_pair_rows,
    fold_result_values,
    fold_rows,
    invalidate_equivalence_fold_cache,
    load_equivalence_mapping,
)
from okto_pulse.core.kg.dedup_migration import (
    migrate_dedup_entities,
    unmerge_equivalence,
)
from okto_pulse.core.ports.kg_equivalence_ledger import (
    require_equivalence_ledger,
)

from kg_registry_testing import configure_real_graph_test_kg_registry
from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)

from test_kg_dedup_reversible import (  # noqa: F401  (harness reuse)
    REF,
    _seed_duplicates,
)

MAPPING = {"entity_dup0": "entity_dup2", "entity_dup1": "entity_dup2"}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_fold_rows_rewrites_and_dedupes_keeping_best_score():
    rows = [
        {"id": "entity_dup0", "combined_score": 0.7},
        {"id": "entity_dup2", "combined_score": 0.9},
        {"id": "entity_dup1", "combined_score": 0.95},
        {"id": "entity_x", "combined_score": 0.5},
    ]
    out = fold_rows(
        rows, MAPPING, id_keys=("id",), dedupe_key="id", score_key="combined_score"
    )
    assert [r["id"] for r in out] == ["entity_dup2", "entity_x"]
    assert out[0]["combined_score"] == 0.95  # best of the folded group


def test_fold_pair_rows_drops_self_pairs_and_dedupes():
    rows = [
        {"id_a": "entity_dup0", "id_b": "entity_dup2", "confidence": 0.9},
        {"id_a": "entity_dup1", "id_b": "entity_x", "confidence": 0.8},
        {"id_a": "entity_x", "id_b": "entity_dup0", "confidence": 0.7},
    ]
    out = fold_pair_rows(rows, MAPPING, key_a="id_a", key_b="id_b")
    # First collapses onto itself (member vs survivor) — dropped; the other
    # two fold to the same unordered pair — deduped to one.
    assert len(out) == 1
    assert {out[0]["id_a"], out[0]["id_b"]} == {"entity_dup2", "entity_x"}


def test_fold_result_values_rewrites_cells_and_dedupes():
    rows = [["entity_dup0", "t"], ["entity_dup2", "t"], ["entity_y", "u"]]
    out = fold_result_values(rows, MAPPING)
    assert out == [["entity_dup2", "t"], ["entity_y", "u"]]


def test_empty_mapping_is_identity():
    rows = [{"id": "a"}]
    assert fold_rows(rows, {}, id_keys=("id",)) is rows
    assert fold_pair_rows(rows, {}, key_a="id", key_b="id") is rows


# ---------------------------------------------------------------------------
# Real graph + service surfaces
# ---------------------------------------------------------------------------


@pytest.fixture
def fold_board(monkeypatch):
    from okto_pulse.core.infra.config import configure_settings, get_settings

    original_settings = get_settings()
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_fold_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()
    invalidate_equivalence_fold_cache()
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    _seed_duplicates(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    invalidate_equivalence_fold_cache()
    gc.collect()
    configure_settings(original_settings)
    shutil.rmtree(base, ignore_errors=True)


def test_s5_mapping_loads_from_ledger_and_invalidates(fold_board):
    assert load_equivalence_mapping(fold_board) == {}
    migrate_dedup_entities(fold_board, confirmed=True)

    mapping = load_equivalence_mapping(fold_board)
    assert mapping == MAPPING

    ledger = require_equivalence_ledger()
    record = asyncio.run(ledger.active_for_board(fold_board))[0]
    unmerge_equivalence(fold_board, record.record_id)
    # Revoke invalidated the cache — members unfold immediately.
    assert load_equivalence_mapping(fold_board) == {}


def test_s5_related_context_folds_members_and_unfolds_on_revoke(fold_board):
    """The scenario's 'edges apontando para membros' case: an ACTIVE
    equivalence whose members are still live in the graph (e.g. a logical
    merge decision recorded ahead of materialization). The fold — not the
    MKG-D tombstone filter — is what presents the survivor here."""
    from okto_pulse.core.kg.equivalence_fold import (
        invalidate_equivalence_fold_cache as _invalidate,
    )
    from okto_pulse.core.kg.kg_service import KGService
    from okto_pulse.core.ports.kg_equivalence_ledger import EquivalenceRecord

    # A hub artifact whose neighborhood traverses the duplicate members.
    with open_board_connection(fold_board) as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_center', title: 'centro',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:ctx'})"
        )
        for member in ("entity_dup0", "entity_dup1"):
            kconn.execute(
                f"MATCH (a:Entity {{id: '{member}'}}),"
                f" (b:Entity {{id: 'entity_center'}}) "
                f"CREATE (a)-[r:belongs_to {{confidence: 1.0,"
                f" layer: 'cognitive', created_by_session_id: 's',"
                f" created_at: timestamp('2026-03-02T00:00:00'),"
                f" rule_id: '', created_by: 's', fallback_reason: ''}}]->(b)"
            )

    # Equivalence recorded WITHOUT tombstoning: members stay live.
    ledger = require_equivalence_ledger()
    record = EquivalenceRecord(
        record_id=f"eqv_{uuid.uuid4().hex[:16]}",
        board_id=fold_board,
        node_type="Entity",
        survivor_id="entity_dup2",
        merged_ids=("entity_dup0", "entity_dup1"),
        operation="dedup_entities",
        created_by="test:fold",
    )
    asyncio.run(ledger.append(record))
    _invalidate(fold_board)

    service = KGService()
    rows = service.get_related_context(
        fold_board, "spec:ctx", graph_layer="all"
    )
    assert rows, "neighborhood should traverse the live members"
    hop_ids = {r["hop1_id"] for r in rows} | {r["center_id"] for r in rows}
    # Members folded into the survivor everywhere they appear.
    assert "entity_dup0" not in hop_ids
    assert "entity_dup1" not in hop_ids
    assert "entity_dup2" in hop_ids

    # Revoke → members reappear on the same surface (cache invalidated).
    asyncio.run(ledger.revoke(record.record_id, "unmerge"))
    _invalidate(fold_board)

    rows_after = service.get_related_context(
        fold_board, "spec:ctx", graph_layer="all"
    )
    hop_ids_after = {r["hop1_id"] for r in rows_after}
    assert {"entity_dup0", "entity_dup1"} & hop_ids_after


def test_s5_cypher_raw_is_not_intercepted(fold_board):
    migrate_dedup_entities(fold_board, confirmed=True)
    # Raw connection query still sees the member ids (tombstoned, present).
    with open_board_connection(fold_board) as (_kdb, kconn):
        res = kconn.execute(
            f"MATCH (n:Entity) WHERE n.source_artifact_ref = '{REF}' "
            "RETURN n.id"
        )
        ids = set()
        while res.has_next():
            ids.add(res.get_next()[0])
        res.close()
    assert {"entity_dup0", "entity_dup1", "entity_dup2"} <= ids
    # And the documented exclusion exists.
    from pathlib import Path as _P

    doc = _P("src/okto_pulse/core/mcp/resources/reference/tool-docs/kg.md")
    text = doc.read_text(encoding="utf-8")
    assert "Equivalence fold exclusion" in text
