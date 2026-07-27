"""MKG-C C4 — logical un-merge (scenario S4).

kg unmerge clears the tombstones the record created (members return to
default recall), revokes the ledger record (preserved for audit), never
re-points edges, and is idempotent on an already-revoked record.
"""

from __future__ import annotations

import asyncio
import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.dedup_migration import (
    migrate_dedup_entities,
    unmerge_equivalence,
)
from okto_pulse.core.ports.kg_equivalence_ledger import (
    EquivalenceLedgerError,
    require_equivalence_ledger,
)

from kg_registry_testing import configure_real_graph_test_kg_registry
from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
)

from test_kg_dedup_reversible import (  # noqa: F401  (harness reuse)
    REF,
    _edge_endpoints,
    _entity_states,
    _seed_duplicates,
)


@pytest.fixture
def unmerge_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_unmerge_"))
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


def _merge_and_get_record(board_id: str):
    report = migrate_dedup_entities(board_id, confirmed=True)
    assert report["ledger_records_created"] == 1
    ledger = require_equivalence_ledger()
    active = asyncio.run(ledger.active_for_board(board_id))
    assert len(active) == 1
    return active[0]


def test_s4_unmerge_restores_members_and_is_idempotent(unmerge_board):
    record = _merge_and_get_record(unmerge_board)
    edges_before = _edge_endpoints(unmerge_board)

    result = unmerge_equivalence(unmerge_board, record.record_id)
    assert result["revoked"] is True
    assert result["members_restored"] == 2
    assert result["survivor_id"] == "entity_dup2"

    # Tombstones cleared — members back to default recall state.
    states = _entity_states(unmerge_board)
    assert all(v is None for v in states.values())
    # Edges untouched (never re-pointed in either direction).
    assert _edge_endpoints(unmerge_board) == edges_before

    # Ledger record preserved for audit, revoked.
    ledger = require_equivalence_ledger()
    loaded = asyncio.run(ledger.get(record.record_id))
    assert loaded is not None
    assert not loaded.is_active
    assert loaded.revoke_reason == "unmerge"
    assert asyncio.run(ledger.active_for_board(unmerge_board)) == ()

    # Second un-merge: idempotent no-op with warning flag.
    again = unmerge_equivalence(unmerge_board, record.record_id)
    assert again["already_revoked"] is True
    assert again["members_restored"] == 0


def test_s4_unmerge_guards_foreign_tombstones(unmerge_board):
    """A member later superseded by a DIFFERENT mechanism keeps its
    tombstone — the reason guard only clears what this record created."""
    from kg_schema_testing import open_board_connection

    record = _merge_and_get_record(unmerge_board)
    # Simulate an unrelated supersession overwriting one member's trail.
    with open_board_connection(unmerge_board) as (_kdb, kconn):
        kconn.execute(
            "MATCH (n:Entity) WHERE n.id = 'entity_dup0' "
            "SET n.revocation_reason = 'title change on NC-8 reuse'"
        )

    result = unmerge_equivalence(unmerge_board, record.record_id)
    assert result["revoked"] is True

    states = _entity_states(unmerge_board)
    # dup1 restored (this record's tombstone); dup0 kept (foreign reason).
    assert states["entity_dup1"] is None
    assert states["entity_dup0"] == "entity_dup2"


def test_s4_unknown_record_and_board_mismatch_fail_structured(unmerge_board):
    record = _merge_and_get_record(unmerge_board)
    with pytest.raises(EquivalenceLedgerError) as excinfo:
        unmerge_equivalence(unmerge_board, "eqv_nao_existe")
    assert excinfo.value.failure_reason == "equivalence_record_not_found"

    with pytest.raises(EquivalenceLedgerError) as excinfo2:
        unmerge_equivalence(str(uuid.uuid4()), record.record_id)
    assert excinfo2.value.failure_reason == "equivalence_record_board_mismatch"
