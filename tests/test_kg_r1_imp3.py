"""R1-IMP3 — fail-closed legacy_unknown in the Global Discovery schema migration.

Spec 29b35f60 / card 7eced1f9 (FR5/AC4/AC5/TR3). A legacy ``DecisionDigest`` that
predates the ``graph_layer`` column must be backfilled to ``legacy_unknown`` —
NEVER implicitly ``canonical`` — so it stays OUT of canonical-only discovery
until the R1-IMP1 parity reconciler maps it from the board graph.

Per AC6, this schema card MAY construct legacy DecisionDigest state directly: the
pipeline cannot produce a NULL-layer digest, and the pipeline-driven FR5 proof
already lives in the validated ``test_kg_r1_imp1.py``
(``test_legacy_missing_layer_stays_out_of_canonical``). These tests target the
migration/projection backfill specifically.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r1i3_"))

import pytest

from okto_pulse.core.kg.cypher_templates import (
    layer_filter_clause,
    layer_label_projection,
)
from okto_pulse.core.kg.embedding import get_embedding_provider
from global_graph_testing import (
    bootstrap_global_discovery,
    ensure_global_discovery_layer_schema,
    open_global_connection,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.write_barrier import under_global_safe_write

LEGACY_UNKNOWN = "legacy_unknown"
_NULL_SENTINEL = "<null>"


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_global():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


def _emb():
    return get_embedding_provider().encode("r1 imp3 legacy digest")


def _create_digest(*, digest_id, original_node_id, node_type="Decision", layer=...):
    """Direct global CREATE (AC6 legacy fixture). ``layer=...`` (Ellipsis) omits
    graph_layer entirely -> NULL, i.e. a pre-column legacy row."""
    _gdb, gconn = open_global_connection()
    try:
        if layer is ...:
            gconn.execute(
                "CREATE (d:DecisionDigest {id:$did, board_id:'r1i3-board', "
                "original_node_id:$oid, title:'legacy', one_line_summary:'legacy', "
                "node_type:$nt, embedding:$e, "
                "created_at:timestamp('2026-06-15T00:00:00')})",
                {"did": digest_id, "oid": original_node_id, "nt": node_type, "e": _emb()},
            )
        else:
            gconn.execute(
                "CREATE (d:DecisionDigest {id:$did, board_id:'r1i3-board', "
                "original_node_id:$oid, title:'seeded', one_line_summary:'seeded', "
                "node_type:$nt, graph_layer:$l, embedding:$e, "
                "created_at:timestamp('2026-06-15T00:00:00')})",
                {"did": digest_id, "oid": original_node_id, "nt": node_type,
                 "l": layer, "e": _emb()},
            )
    finally:
        del gconn, _gdb


def _digest(digest_id) -> tuple[str, str]:
    _gdb, gconn = open_global_connection()
    try:
        res = gconn.execute(
            "MATCH (d:DecisionDigest {id:$did}) "
            "RETURN d.graph_layer, d.original_node_id",
            {"did": digest_id},
        )
        if not res.has_next():
            return (None, None)
        row = res.get_next()
        layer = row[0] if row[0] is not None else _NULL_SENTINEL
        return (str(layer), str(row[1]))
    finally:
        del gconn, _gdb


def _run_migration():
    with under_global_safe_write("r1i3-test", "ensure_layer_schema"):
        ensure_global_discovery_layer_schema()


def test_migration_backfills_null_to_legacy_unknown_not_canonical():
    did = f"dd_legacy_{uuid.uuid4().hex[:8]}"
    oid = f"node_{uuid.uuid4().hex[:8]}"
    _create_digest(digest_id=did, original_node_id=oid, layer=...)
    # Precondition: the legacy row truly has no graph_layer.
    assert _digest(did)[0] == _NULL_SENTINEL

    _run_migration()

    layer, original = _digest(did)
    assert layer == LEGACY_UNKNOWN, f"NULL must backfill to legacy_unknown, got {layer!r}"
    assert layer != "canonical", "implicit NULL->canonical backfill is forbidden (FR5)"
    assert original == oid, "original_node_id identity must be preserved"


def test_migration_preserves_existing_layers():
    canon = f"dd_c_{uuid.uuid4().hex[:8]}"
    work = f"dd_w_{uuid.uuid4().hex[:8]}"
    _create_digest(digest_id=canon, original_node_id="c_node", layer="canonical")
    _create_digest(digest_id=work, original_node_id="w_node", layer="working")

    _run_migration()

    # The backfill only touches NULL rows; explicit layers are untouched.
    assert _digest(canon)[0] == "canonical"
    assert _digest(work)[0] == "working"


def test_legacy_unknown_excluded_from_canonical_filter_included_in_all():
    did = f"dd_leg2_{uuid.uuid4().hex[:8]}"
    oid = f"node2_{uuid.uuid4().hex[:8]}"
    _create_digest(digest_id=did, original_node_id=oid, layer=...)
    _run_migration()

    # Use the SAME fail-closed clause/projection query_global uses.
    def _match(conn, layer_param):
        cypher = (
            "MATCH (d:DecisionDigest) WHERE d.id = $did AND "
            f"{layer_filter_clause('d')} "
            f"RETURN d.id, {layer_label_projection('d')}"
        )
        res = conn.execute(cypher, {"did": did, "graph_layer": layer_param})
        rows = []
        while res.has_next():
            row = res.get_next()
            rows.append((str(row[0]), str(row[1])))
        return rows

    _gdb, gconn = open_global_connection()
    try:
        canonical_rows = _match(gconn, "canonical")
        all_rows = _match(gconn, "all")
    finally:
        del gconn, _gdb

    assert canonical_rows == [], "legacy_unknown digest leaked into a canonical filter"
    assert len(all_rows) == 1, "all query must still surface the legacy digest"
    assert all_rows[0][1] == LEGACY_UNKNOWN
