"""MKG-C C3 — reversible dedup (scenarios S2, S3, S8).

S2: the confirmed default tombstones duplicates (superseded_by=survivor),
deletes nothing, re-points NOTHING (edge counts identical) and appends the
complete ledger snapshot BEFORE mutating (an injected failure after the
append preserves the snapshot). S3: policy enforcement — hard-delete is
forbidden; an unconfirmed write is refused with actionable remediation.
S8: --dry-run stays zero-mutation and zero-ledger.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.curation_policy import CurationPolicyError
from okto_pulse.core.kg.dedup_migration import migrate_dedup_entities
from okto_pulse.core.ports.kg_equivalence_ledger import (
    require_equivalence_ledger,
)

from kg_registry_testing import configure_real_graph_test_kg_registry
from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)

REF = "spec:dedup-target"


@pytest.fixture
def dedup_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_dedupv2_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    _seed_duplicates(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _seed_duplicates(board_id: str) -> None:
    """3 Entities sharing REF (most recent = survivor) + edges touching
    the duplicates so the no-repoint invariant is observable."""

    with open_board_connection(board_id) as (_kdb, kconn):
        for i, ts in enumerate(
            ("2026-01-01T00:00:00", "2026-02-01T00:00:00", "2026-03-01T00:00:00")
        ):
            kconn.execute(
                f"CREATE (n:Entity {{id: 'entity_dup{i}', title: 'Dup {i}',"
                f" content: 'c{i}', source_confidence: 0.9,"
                f" graph_layer: 'canonical', source_artifact_ref: '{REF}',"
                f" created_at: timestamp('{ts}')}})"
            )
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_other', title: 'Outro',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:other'})"
        )
        for i in (0, 1):
            kconn.execute(
                f"MATCH (a:Entity {{id: 'entity_dup{i}'}}),"
                f" (b:Entity {{id: 'entity_other'}}) "
                f"CREATE (a)-[r:belongs_to {{confidence: 1.0,"
                f" layer: 'cognitive', created_by_session_id: 's',"
                f" created_at: timestamp('2026-03-02T00:00:00'),"
                f" rule_id: '', created_by: 's', fallback_reason: ''}}]->(b)"
            )


def _edge_endpoints(board_id: str) -> list[tuple[str, str]]:
    with open_board_connection(board_id) as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (a:Entity)-[r:belongs_to]->(b:Entity) "
            "RETURN a.id, b.id ORDER BY a.id, b.id"
        )
        rows = []
        try:
            while res.has_next():
                r = res.get_next()
                rows.append((r[0], r[1]))
        finally:
            try:
                res.close()
            except Exception:
                pass
        return rows


def _entity_states(board_id: str) -> dict[str, str | None]:
    with open_board_connection(board_id) as (_kdb, kconn):
        res = kconn.execute(
            f"MATCH (n:Entity) WHERE n.source_artifact_ref = '{REF}' "
            "RETURN n.id, n.superseded_by"
        )
        out: dict[str, str | None] = {}
        try:
            while res.has_next():
                r = res.get_next()
                out[r[0]] = r[1]
        finally:
            try:
                res.close()
            except Exception:
                pass
        return out


def test_s2_confirmed_dedup_tombstones_without_repoint(dedup_board):
    edges_before = _edge_endpoints(dedup_board)
    report = migrate_dedup_entities(dedup_board, confirmed=True)

    assert report["mode"] == "tombstone"
    assert report["nodes_tombstoned"] == 2
    assert report["ledger_records_created"] == 1
    assert report["total_duplicates_removed"] == 0
    assert report["edges_repointed"] == 0

    # All 3 nodes preserved; the 2 older ones tombstoned to the survivor.
    states = _entity_states(dedup_board)
    assert len(states) == 3
    assert states["entity_dup2"] is None
    assert states["entity_dup0"] == "entity_dup2"
    assert states["entity_dup1"] == "entity_dup2"

    # ZERO edge re-point: endpoints byte-identical.
    assert _edge_endpoints(dedup_board) == edges_before

    # Ledger record carries the COMPLETE snapshot.
    import asyncio

    ledger = require_equivalence_ledger()
    active = asyncio.run(ledger.active_for_board(dedup_board))
    assert len(active) == 1
    record = active[0]
    assert record.survivor_id == "entity_dup2"
    assert set(record.merged_ids) == {"entity_dup0", "entity_dup1"}
    evidence = dict(record.evidence)
    assert {n["id"] for n in evidence["nodes"]} == {
        "entity_dup0", "entity_dup1", "entity_dup2",
    }
    edge_pairs = {(e["from"], e["to"]) for e in evidence["edges"]}
    assert ("entity_dup0", "entity_other") in edge_pairs
    assert ("entity_dup1", "entity_other") in edge_pairs
    assert all("confidence" in e["props"] for e in evidence["edges"])

    # Idempotent: a re-run sees only ACTIVE members → zero actions.
    report2 = migrate_dedup_entities(dedup_board, confirmed=True)
    assert report2["groups"] == 0
    assert report2["ledger_records_created"] == 0


def test_s2_ledger_written_before_mutation_survives_failure(
    dedup_board, monkeypatch
):
    import okto_pulse.core.kg.dedup_migration as dm

    def _boom(*args, **kwargs):
        raise RuntimeError("injected after ledger append")

    monkeypatch.setattr(dm, "_tombstone_members", _boom)
    with pytest.raises(Exception):
        migrate_dedup_entities(dedup_board, confirmed=True)

    # The snapshot was appended BEFORE the (failed) mutation — preserved.
    import asyncio

    ledger = require_equivalence_ledger()
    active = asyncio.run(ledger.active_for_board(dedup_board))
    assert len(active) == 1
    assert dict(active[0].evidence)["edges"]
    # And the graph was NOT tombstoned by the failed run.
    states = _entity_states(dedup_board)
    assert all(v is None for v in states.values())


def test_s3_hard_delete_forbidden_and_unconfirmed_refused(dedup_board):
    with pytest.raises(CurationPolicyError) as excinfo:
        migrate_dedup_entities(dedup_board, confirmed=True, hard_delete=True)
    assert excinfo.value.level == "forbidden"

    with pytest.raises(CurationPolicyError) as excinfo2:
        migrate_dedup_entities(dedup_board)
    assert excinfo2.value.level == "propose_only"
    assert "--confirm" in excinfo2.value.remediation

    # Nothing was written by either refusal.
    states = _entity_states(dedup_board)
    assert all(v is None for v in states.values())
    import asyncio

    ledger = require_equivalence_ledger()
    assert asyncio.run(ledger.active_for_board(dedup_board)) == ()


def test_s8_dry_run_zero_mutation_zero_ledger(dedup_board):
    edges_before = _edge_endpoints(dedup_board)
    report = migrate_dedup_entities(dedup_board, dry_run=True)

    assert report["mode"] == "dry_run"
    assert report["groups"] == 1
    assert report["duplicates_planned"] == 2
    assert report["edges_planned"] >= 2  # simulation still counts

    states = _entity_states(dedup_board)
    assert all(v is None for v in states.values())
    assert _edge_endpoints(dedup_board) == edges_before

    import asyncio

    ledger = require_equivalence_ledger()
    assert asyncio.run(ledger.active_for_board(dedup_board)) == ()
