"""SPEC4 card 89f61f6f / scenario ts_df112b35 — Confirmed rebuild preserves
partitions and graph files.

Own end-to-end evidence for the FULL confirmed-rebuild flow (manifest →
confirm → run), NOT source enumeration in isolation:
* the run promotes a generation ONLY when every expected partition materialized
  (canonical + working), and fails closed (MATERIALIZED_LAYER_MISMATCH, no
  promotion) when a partition is dropped — no false COMPLETED-incomplete;
* a rebuild QUARANTINES the existing board graph store (moved + preserved) and
  never hard-deletes it.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_confirmed_rebuild_safety.py
"""

from __future__ import annotations

import uuid
from pathlib import Path

import okto_pulse.core.kg.rebuild_service as rebuild_service
from okto_pulse.core.kg.rebuild_service import (
    RebuildBlockReason,
    RebuildOutcome,
    RebuildStepResult,
)

# Reuse the rebuild_service confirm/run harness (sibling test module on sys.path).
from test_kg_rebuild_service import _build_service, _issue_confirmation


def _step_with_expected(expected: dict) -> object:
    """A controlled rebuild step that reports the partitions the resolved source
    set expects to materialize (the orchestrator queries the REAL materialized
    layers + verifies presence before promoting)."""

    def _adapter(_req):
        return RebuildStepResult(ok=True, counts={"expected_by_layer": expected})

    return _adapter


def _run(service, manifest_store, confirmation_store):
    cid, mref, phash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    return service.run(
        confirmation_id=cid,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=phash,
        manifest_ref=mref,
        reason="recovery",
    )


# ---------------------------------------------------------------------------
# Partition completeness through the confirm → run path
# ---------------------------------------------------------------------------


def test_confirmed_rebuild_promotes_only_when_partitions_complete(tmp_path: Path, monkeypatch):
    service, manifest_store, confirmation_store, _lock = _build_service(
        tmp_path, step_adapter=_step_with_expected({"canonical": 1, "working": 1})
    )
    # both expected partitions materialized in the rebuilt graph.
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts",
        lambda _b: {"canonical": 3, "working": 2},
    )
    result = _run(service, manifest_store, confirmation_store)
    # the partition guard PASSED (every expected layer materialized) → COMPLETED.
    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.reason == RebuildBlockReason.OK.value


def test_confirmed_rebuild_fails_closed_when_a_partition_drops(tmp_path: Path, monkeypatch):
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path, step_adapter=_step_with_expected({"canonical": 1, "working": 1})
    )
    # the WORKING partition did not materialize → must NOT report COMPLETED.
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts",
        lambda _b: {"canonical": 3},
    )
    result = _run(service, manifest_store, confirmation_store)
    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.reason == RebuildBlockReason.MATERIALIZED_LAYER_MISMATCH.value
    assert result.current_kg_generation_id is None  # generation NOT promoted
    assert lock.inspect(board_id="b1") is None  # lock released


def test_confirmed_rebuild_fails_when_safe_write_lifecycle_does_not_apply(tmp_path: Path):
    # C5: queue drain + safe-write (checkpoint/flush/fsync/close-reopen) must be
    # confirmed before COMPLETED — a lifecycle that does not apply fails closed,
    # never reporting a promoted-but-unflushed graph as success.
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=_step_with_expected({"canonical": 1}),
        lifecycle_step_ok=False,
    )
    result = _run(service, manifest_store, confirmation_store)
    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.reason == RebuildBlockReason.LIFECYCLE_FAILED.value
    assert result.current_kg_generation_id is None
    assert lock.inspect(board_id="b1") is None  # lock released even on failure


# ---------------------------------------------------------------------------
# Quarantine semantics — existing graph store is preserved, not hard-deleted
# ---------------------------------------------------------------------------


def test_confirmed_rebuild_quarantines_existing_graph_not_delete():
    from okto_pulse.core.kg.schema import (
        board_kuzu_path,
        bootstrap_board_graph,
        open_board_connection,
        purge_board_graph_storage,
    )

    board_id = f"quar-{uuid.uuid4().hex[:8]}"
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Decision {id:'d1', title:'seed', graph_layer:'canonical', "
            "source_confidence:1.0})"
        )

    graph_path = board_kuzu_path(board_id)
    assert graph_path.exists(), "expected a materialized board graph store"
    # boards/<id>/graph.lbug -> parents[2] is the kg root holding boards/ + quarantine/.
    quarantine_root = graph_path.parents[2] / "quarantine"
    quar_before = (
        sum(1 for _ in quarantine_root.rglob("*")) if quarantine_root.exists() else 0
    )

    moved = purge_board_graph_storage(board_id, reason="explicit_rebuild:test")

    assert moved, "purge moved nothing — no graph store to quarantine"
    assert not graph_path.exists(), "original graph store still at the live path"
    # PRESERVED, not deleted: the files now live under the quarantine area.
    assert quarantine_root.exists(), "no quarantine directory was created"
    quar_after = sum(1 for _ in quarantine_root.rglob("*"))
    assert quar_after > quar_before, "graph store was deleted, not quarantined"
