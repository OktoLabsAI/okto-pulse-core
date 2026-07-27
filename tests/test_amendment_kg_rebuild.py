"""SPEC 7ea1e4be / card 74a6d258 — amendment sources in KG rebuild + canonicality.

Covers FR5/FR6 + TR3/TR4 + G4:
* source-maturity classification of amendment (working-only before done; canonical
  only at done + complete lineage);
* process_amendment materializes a SEPARATE canonical node with belongs_to edges
  to the original spec / origin bug / regression test task + board provenance, and
  NEVER re-emits the original spec node (AC1);
* amendment sources are counted in expected_layers and enqueued for
  materialization (not counted-but-skipped → no false MATERIALIZED_LAYER_MISMATCH);
* complete empty amendment partitions and fail-closed legacy schemas.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_amendment_kg_rebuild.py
"""

from __future__ import annotations

import pytest

import okto_pulse.core.kg.rebuild_service as rebuild_service
from okto_pulse.core.kg.board_rebuild_adapter import (
    _DETERMINISTIC_SOURCE_ARTIFACT_TYPES,
    _expected_layers_from_sources,
)
from okto_pulse.core.kg.rebuild_service import (
    RebuildStepResult,
    _verify_materialized_layers,
)
from okto_pulse.core.kg.source_maturity import (
    CANONICAL_ARTIFACT_TYPES,
    REBUILD_ARTIFACT_TYPES,
    WORKING_ARTIFACT_TYPES,
    classify_source_for_kg,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.application.rebuild_ports import SourceUnavailableError

AMD = "amendment_hotfix_revision"


def _classify(status, lineage_complete):
    return classify_source_for_kg(
        artifact_type=AMD,
        artifact_status=status,
        content_hash="h",
        lineage_complete=lineage_complete,
    )


# ---------------------------------------------------------------------------
# A. Source-maturity classification (working-only before done; canonical only
#    at done + complete lineage)
# ---------------------------------------------------------------------------


def test_amendment_is_registered_in_type_tuples():
    assert AMD in WORKING_ARTIFACT_TYPES
    assert AMD in REBUILD_ARTIFACT_TYPES
    assert AMD in CANONICAL_ARTIFACT_TYPES
    # MUST be enqueued for materialization (else counted-but-skipped -> false
    # MATERIALIZED_LAYER_MISMATCH).
    assert AMD in _DETERMINISTIC_SOURCE_ARTIFACT_TYPES


def test_amendment_source_maturity_matrix():
    # done + complete -> canonical
    canonical = _classify("done", True)
    assert canonical.graph_layer == "canonical"
    assert canonical.disposition == "canonical"

    # done + incomplete -> working (NOT canonical), explicit reason
    incomplete = _classify("done", False)
    assert incomplete.graph_layer == "working"
    assert incomplete.disposition == "skipped_by_maturity"
    assert incomplete.reason_code == "amendment_lineage_incomplete"

    # before done -> working-only
    for status in ("draft", "review", "approved"):
        c = _classify(status, True)
        assert c.graph_layer == "working", status
        assert c.disposition != "canonical", status

    # cancelled -> never materializes
    assert _classify("cancelled", True).disposition == "skipped_cancelled"


# ---------------------------------------------------------------------------
# C. process_amendment — separate canonical node + belongs_to edges; AC1
# ---------------------------------------------------------------------------


def _amendment(**over):
    base = {
        "id": "amd0000abcd",
        "board_id": "board-1",
        "original_spec_id": "spec0000efgh",
        "origin_bug_id": "bug0000ijkl",
        "status": "done",
        "lineage_state": "complete",
        "regression_test_task_ids": ["tc0000mnop"],
        "regression_scenario_ids": ["ts_reg1"],
        "automated_regression_refs": ["tests/test_x.py::test_y"],
    }
    base.update(over)
    return base


def test_process_amendment_done_complete_canonical_with_edges():
    result = DeterministicWorker().process_amendment(_amendment())

    nodes = {n.candidate_id: n for n in result.nodes}
    amd_cid = "amendment_amd0000a_entity"
    assert amd_cid in nodes
    assert nodes[amd_cid].node_type == "Entity"
    assert nodes[amd_cid].graph_layer == "canonical"
    assert nodes[amd_cid].source_artifact_ref == "amendment_hotfix_revision:amd0000abcd"

    # AC1: the original spec node is NEVER emitted/re-canonicalized by the
    # amendment — only the amendment node (+ board root) are produced here.
    assert not any(n.source_artifact_ref.startswith("spec:") for n in result.nodes)
    assert not any(n.source_artifact_ref.startswith("card:") for n in result.nodes)

    # belongs_to edges with explicit stable rule_ids (codex condition 1), all
    # FROM the amendment node (so the targets are never mutated).
    spec_edge = next(e for e in result.edges if "amendment_to_original_spec" in e.rule_id)
    assert spec_edge.from_candidate_id == amd_cid
    assert spec_edge.to_candidate_id == "spec_spec0000_entity"
    assert spec_edge.edge_type == "belongs_to"

    bug_edge = next(e for e in result.edges if "amendment_to_origin_bug" in e.rule_id)
    assert bug_edge.to_candidate_id == "card_bug0000i_entity"

    reg_edge = next(e for e in result.edges if "amendment_to_regression_test_task" in e.rule_id)
    assert reg_edge.to_candidate_id == "card_tc0000mn_entity"

    # Board-root provenance edge (always resolvable -> ordering-safe).
    assert any("amendment_to_board" in e.rule_id for e in result.edges)
    assert all(e.from_candidate_id == amd_cid for e in result.edges)

    # scenario ids + automated refs are carried as searchable content (no
    # dangling placeholder edges).
    assert "ts_reg1" in nodes[amd_cid].content
    assert "tests/test_x.py::test_y" in nodes[amd_cid].content


def test_process_amendment_working_only_before_done():
    for status in ("draft", "approved"):
        result = DeterministicWorker().process_amendment(_amendment(status=status))
        amd = next(n for n in result.nodes if n.candidate_id == "amendment_amd0000a_entity")
        assert amd.graph_layer == "working", status


def test_process_amendment_done_incomplete_is_working():
    result = DeterministicWorker().process_amendment(
        _amendment(status="done", lineage_state="incomplete")
    )
    amd = next(n for n in result.nodes if n.candidate_id == "amendment_amd0000a_entity")
    assert amd.graph_layer == "working"


# ---------------------------------------------------------------------------
# D/G4. expected_layers counts the amendment partition
# ---------------------------------------------------------------------------


def test_expected_layers_counts_amendment_canonical():
    # A done+complete amendment classified canonical contributes to the expected
    # canonical partition; combined with it being in _DETERMINISTIC_SOURCE_ARTIFACT_TYPES
    # (enqueued) + process_amendment materializing a canonical node, the rebuild
    # cannot raise a false MATERIALIZED_LAYER_MISMATCH for it.
    sources = [{"graph_layer": "canonical", "artifact_type": AMD}]
    assert _expected_layers_from_sources(sources).get("canonical") == 1

    # amendment as the SOLE canonical source still registers expected canonical=1
    mixed = [
        {"graph_layer": "working", "artifact_type": "spec"},
        {"graph_layer": "canonical", "artifact_type": AMD},
    ]
    counts = _expected_layers_from_sources(mixed)
    assert counts.get("canonical") == 1 and counts.get("working") == 1


# ---------------------------------------------------------------------------
# B + TR4. board_source_store enumeration + backward compatibility
# ---------------------------------------------------------------------------


def _temp_pulse_db(tmp_path):
    """A self-contained sync SQLite pulse.db with the full schema (create_all)."""
    from sqlalchemy import create_engine
    from sqlalchemy_test_models import Base
    import sqlalchemy_test_models  # noqa: F401 - register all tables

    db_file = tmp_path / "pulse.db"
    engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(engine)
    return db_file, engine  # Path (BoardSourceStore calls db_path.exists())


def test_board_source_store_enumerates_amendment(tmp_path):
    from sqlalchemy.orm import Session
    from sqlalchemy_test_models import (
        AmendmentHotfixRevision,
        Board,
        Spec,
        SpecStatus,
    )
    from okto_pulse.core.domain.amendment_eligibility import (
        AmendmentLineageState,
        AmendmentRevisionStatus,
    )
    from okto_pulse.community.adapters.board_source_reader import BoardSourceStore

    db_path, engine = _temp_pulse_db(tmp_path)
    board_id = "board-amd"
    with Session(engine) as s:
        s.add(Board(id=board_id, name="AMD-KG", owner_id="u"))
        s.add(Spec(id="spec1", board_id=board_id, title="S", status=SpecStatus.DONE, created_by="u"))
        s.add(AmendmentHotfixRevision(
            id="amd1", board_id=board_id, original_spec_id="spec1", origin_bug_id="bug1",
            status=AmendmentRevisionStatus.DONE, lineage_state=AmendmentLineageState.COMPLETE,
            created_by="u",
        ))
        s.commit()

    rows = BoardSourceStore(db_path).fetch(board_id)
    amendment_rows = [r for r in rows if r["artifact_type"] == AMD]
    assert len(amendment_rows) == 1
    row = amendment_rows[0]
    assert row["id"] == "amd1"
    assert row["source_ref"] == "amendment_hotfix_revision:amd1"
    assert row["status"] == "done"
    assert row["lineage_complete"] is True
    # the enumerated row classifies canonical (done + complete lineage)
    classification = classify_source_for_kg(
        artifact_type=row["artifact_type"],
        artifact_status=row["status"],
        content_hash=row["content_hash"],
        lineage_complete=row["lineage_complete"],
    )
    assert classification.graph_layer == "canonical"


def test_board_source_store_amendment_working_when_not_done(tmp_path):
    from sqlalchemy.orm import Session
    from sqlalchemy_test_models import AmendmentHotfixRevision, Board
    from okto_pulse.core.domain.amendment_eligibility import (
        AmendmentLineageState,
        AmendmentRevisionStatus,
    )
    from okto_pulse.community.adapters.board_source_reader import BoardSourceStore

    db_path, engine = _temp_pulse_db(tmp_path)
    board_id = "board-amd2"
    with Session(engine) as s:
        s.add(Board(id=board_id, name="AMD2", owner_id="u"))
        s.add(AmendmentHotfixRevision(
            id="amd2", board_id=board_id, original_spec_id="spec1", origin_bug_id="bug1",
            status=AmendmentRevisionStatus.APPROVED, lineage_state=AmendmentLineageState.COMPLETE,
            created_by="u",
        ))
        s.commit()

    row = next(r for r in BoardSourceStore(db_path).fetch(board_id) if r["artifact_type"] == AMD)
    assert classify_source_for_kg(
        artifact_type=row["artifact_type"], artifact_status=row["status"],
        content_hash=row["content_hash"], lineage_complete=row["lineage_complete"],
    ).graph_layer == "working"  # approved -> working-only before done


def test_board_without_amendment_table_is_incomplete_fail_closed(tmp_path):
    # A missing authoritative partition cannot be treated as a proven empty set:
    # the durable snapshot is explicitly incomplete and sequence consumption fails.
    from sqlalchemy import text
    from sqlalchemy.orm import Session
    from sqlalchemy_test_models import Board
    from okto_pulse.community.adapters.board_source_reader import BoardSourceStore

    db_path, engine = _temp_pulse_db(tmp_path)
    board_id = "board-legacy"
    with Session(engine) as s:
        s.add(Board(id=board_id, name="LEGACY", owner_id="u"))
        s.commit()
    # Simulate an older schema with no Path B table.
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS amendment_hotfix_revisions"))

    snapshot = BoardSourceStore(db_path).fetch(board_id)

    assert snapshot.rows == ()
    assert snapshot.complete is False
    assert snapshot.cause == "table_missing"
    with pytest.raises(SourceUnavailableError):
        list(snapshot)


# ---------------------------------------------------------------------------
# B2 + TR3. content_hash is lineage-EXACT: origin_task_ids / affected_task_ids
#           are load-bearing (codex reinforcement #1). Proves the
#           AMENDMENT_CONTENT_COLUMNS adjustment has real effect, and that
#           volatile fields (id / created_at) are excluded.
# ---------------------------------------------------------------------------


def _hash_for_amendment(tmp_path, tag, **fields):
    """Enumerate ONE amendment (isolated db) and return its content_hash."""
    from sqlalchemy.orm import Session
    from sqlalchemy_test_models import AmendmentHotfixRevision, Board
    from okto_pulse.core.domain.amendment_eligibility import (
        AmendmentLineageState,
        AmendmentRevisionStatus,
    )
    from okto_pulse.community.adapters.board_source_reader import BoardSourceStore

    sub = tmp_path / tag
    sub.mkdir()
    db_path, engine = _temp_pulse_db(sub)
    board_id = "board-hash"
    with Session(engine) as s:
        s.add(Board(id=board_id, name="HASH", owner_id="u"))
        s.add(AmendmentHotfixRevision(
            id="amdH", board_id=board_id, original_spec_id="spec1",
            origin_bug_id="bug1", status=AmendmentRevisionStatus.DONE,
            lineage_state=AmendmentLineageState.COMPLETE, created_by="u",
            **fields,
        ))
        s.commit()
    row = next(
        r for r in BoardSourceStore(db_path).fetch(board_id) if r["artifact_type"] == AMD
    )
    return row["content_hash"]


def test_content_hash_is_lineage_exact(tmp_path):
    base = _hash_for_amendment(
        tmp_path, "base", origin_task_ids=["t1"], affected_task_ids=["a1"]
    )
    same = _hash_for_amendment(
        tmp_path, "same", origin_task_ids=["t1"], affected_task_ids=["a1"]
    )
    diff_origin = _hash_for_amendment(
        tmp_path, "origin", origin_task_ids=["t1", "t2"], affected_task_ids=["a1"]
    )
    diff_affected = _hash_for_amendment(
        tmp_path, "affected", origin_task_ids=["t1"], affected_task_ids=["a1", "a2"]
    )

    # identical content columns -> identical hash (id + created_at are NOT in
    # AMENDMENT_CONTENT_COLUMNS, so audit/identity churn never re-materializes).
    assert base == same
    # origin_task_ids is load-bearing in the hash (codex adjustment #2).
    assert diff_origin != base
    # affected_task_ids is load-bearing in the hash.
    assert diff_affected != base
    assert diff_origin != diff_affected


# ---------------------------------------------------------------------------
# G4 (main tooth). enqueue_sources REALLY inserts an amendment row into
#                  consolidation_queue with artifact_type=amendment_hotfix_revision
#                  (not remapped to 'card', not filtered) — the concrete guard
#                  against counted-but-never-enqueued -> false MATERIALIZED_LAYER_MISMATCH.
# ---------------------------------------------------------------------------


def test_amendment_source_is_really_enqueued_not_filtered(tmp_path):
    import sqlite3

    from sqlalchemy.orm import Session
    from sqlalchemy_test_models import Board
    from okto_pulse.community.adapters.board_rebuild_ingestion import (
        BoardRebuildIngestionAdapter,
    )

    db_path, engine = _temp_pulse_db(tmp_path)
    board_id = "board-enq"
    with Session(engine) as s:
        s.add(Board(id=board_id, name="ENQ", owner_id="u"))
        s.commit()

    adapter = BoardRebuildIngestionAdapter(db_path=db_path)
    counts = adapter.enqueue_sources(
        board_id=board_id,
        run_id="run-1",
        sources=[
            {"artifact_type": AMD, "id": "amd1"},
            {"artifact_type": "spec", "id": "spec1"},
        ],
    )
    # both sources are deterministic types -> both enqueued (amendment NOT filtered).
    assert counts["inserted"] == 2

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        amd_rows = conn.execute(
            "SELECT artifact_type, status FROM consolidation_queue "
            "WHERE board_id=? AND artifact_id=?",
            (board_id, "amd1"),
        ).fetchall()
    finally:
        conn.close()

    assert len(amd_rows) == 1
    # The queue row carries the amendment artifact_type VERBATIM (not remapped to
    # 'card'), so _process_queue_entry loads AmendmentHotfixRevision and dispatches
    # process_amendment -> the materialized partition matches expected_by_layer.
    assert amd_rows[0]["artifact_type"] == AMD
    assert amd_rows[0]["status"] == "pending"

    # idempotent re-enqueue leaves the active pending row alone (no duplicate row).
    counts2 = adapter.enqueue_sources(
        board_id=board_id,
        run_id="run-2",
        sources=[{"artifact_type": AMD, "id": "amd1"}],
    )
    assert counts2["inserted"] == 0
    assert counts2["reset_to_pending"] == 0
    assert counts2["left_alone"] == 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        amd_rows_after = conn.execute(
            "SELECT artifact_type, status, source FROM consolidation_queue "
            "WHERE board_id=? AND artifact_id=?",
            (board_id, "amd1"),
        ).fetchall()
    finally:
        conn.close()

    assert len(amd_rows_after) == 1
    assert amd_rows_after[0]["artifact_type"] == AMD
    assert amd_rows_after[0]["status"] == "pending"
    assert amd_rows_after[0]["source"] == "rebuild:run-1"


# ---------------------------------------------------------------------------
# TS2 / AC4 (scenario ts_1cfe2c71). KG rebuild after amendment CREATION (working)
# AND after amendment DONE (canonical) produces NO materialized_layer_mismatch
# for amendment sources. Ties the REAL expected partition derived from a
# classified amendment source to the formal rebuild guard
# (rebuild_service._verify_materialized_layers, the same guard that enforces
# MATERIALIZED_LAYER_MISMATCH). Non-tautological: graph_layer is derived from
# classify_source_for_kg, not hand-written.
# ---------------------------------------------------------------------------


def _rebuild_step(expected):
    return RebuildStepResult(ok=True, counts={"expected_by_layer": expected})


def _amendment_source_after_classify(status, lineage_complete):
    """Mirror the rebuild pipeline: classify an amendment source row and attach
    the resolved graph_layer (what rebuild_sources does before the guard runs)."""
    c = classify_source_for_kg(
        artifact_type=AMD,
        artifact_status=status,
        content_hash="h",
        lineage_complete=lineage_complete,
    )
    return {"id": "amd1", "artifact_type": AMD, "graph_layer": c.graph_layer}


def test_ac4_amendment_no_materialized_layer_mismatch(monkeypatch):
    # after CREATION (draft, complete lineage) -> working partition.
    working_src = _amendment_source_after_classify("draft", True)
    assert working_src["graph_layer"] == "working"
    expected_working = _expected_layers_from_sources([working_src])
    assert expected_working == {"working": 1}
    # amendment materialized in working -> guard passes (no mismatch).
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts", lambda _b: {"working": 1}
    )
    assert _verify_materialized_layers("board-ac4", _rebuild_step(expected_working)) is None

    # after DONE (done, complete lineage) -> canonical partition.
    canonical_src = _amendment_source_after_classify("done", True)
    assert canonical_src["graph_layer"] == "canonical"
    expected_canonical = _expected_layers_from_sources([canonical_src])
    assert expected_canonical == {"canonical": 1}
    # amendment materialized in canonical -> guard passes (no mismatch).
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts", lambda _b: {"canonical": 1}
    )
    assert _verify_materialized_layers("board-ac4", _rebuild_step(expected_canonical)) is None


def test_ac4_guard_fails_if_amendment_counted_but_not_materialized(monkeypatch):
    # Negative tooth: a done amendment is expected in the canonical partition; if
    # it is NOT materialized the guard MUST fail closed. Proves the guard would
    # catch a real dropped amendment partition (not rubber-stamp), and that
    # enqueueing the amendment — test above — is what prevents this false mismatch.
    canonical_src = _amendment_source_after_classify("done", True)
    expected = _expected_layers_from_sources([canonical_src])
    assert expected == {"canonical": 1}
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts", lambda _b: {}
    )
    res = _verify_materialized_layers("board-ac4", _rebuild_step(expected))
    assert res is not None
    detail, _materialized = res
    assert "materialized_layer_mismatch" in detail
    assert "canonical" in detail
