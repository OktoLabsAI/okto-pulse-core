"""MKG-C C6 — propose→approve lane with state proof (scenario S7).

--propose persists the canonical plan + deterministic hash WITHOUT mutating
anything; --approve executes exactly the plan when the graph is unchanged
(tombstone + ledger, proposal resolved) and refuses with stale_proposal —
zero mutation — when the group state diverged.
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
    StaleProposalError,
    approve_dedup_proposal,
    compute_proposal_hash,
    propose_dedup_entities,
)
from okto_pulse.core.ports.kg_curation_proposals import (
    CurationProposalError,
    require_curation_proposal_store,
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
    _entity_states,
    _seed_duplicates,
)


@pytest.fixture
def proposal_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_props_"))
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


def test_s7_propose_persists_plan_without_mutation(proposal_board):
    result = propose_dedup_entities(proposal_board)
    assert result["proposal_id"].startswith("prop_")
    assert result["groups"] == 1
    assert result["duplicates_planned"] == 2
    # Deterministic hash over the canonical serialization.
    assert result["proposal_hash"] == compute_proposal_hash(result["plan"])

    # ZERO mutation: graph untouched, equivalence ledger empty.
    states = _entity_states(proposal_board)
    assert all(v is None for v in states.values())
    ledger = require_equivalence_ledger()
    assert asyncio.run(ledger.active_for_board(proposal_board)) == ()

    # Proposal is pending and inspectable.
    store = require_curation_proposal_store()
    pending = asyncio.run(store.pending_for_board(proposal_board))
    assert [p.proposal_id for p in pending] == [result["proposal_id"]]


def test_s7_approve_unchanged_state_executes_and_resolves(proposal_board):
    proposal = propose_dedup_entities(proposal_board)
    report = approve_dedup_proposal(proposal_board, proposal["proposal_id"])

    assert report["proposal_status"] == "resolved"
    assert report["nodes_tombstoned"] == 2
    assert report["ledger_records_created"] == 1

    states = _entity_states(proposal_board)
    assert states["entity_dup0"] == "entity_dup2"
    assert states["entity_dup1"] == "entity_dup2"

    store = require_curation_proposal_store()
    resolved = asyncio.run(store.get(proposal["proposal_id"]))
    assert resolved.status == "resolved"
    assert asyncio.run(store.pending_for_board(proposal_board)) == ()

    # An already-resolved proposal cannot be approved again.
    with pytest.raises(CurationProposalError) as excinfo:
        approve_dedup_proposal(proposal_board, proposal["proposal_id"])
    assert excinfo.value.failure_reason == "curation_proposal_already_resolved"


def test_s7_approve_stale_state_refuses_without_mutation(proposal_board):
    proposal = propose_dedup_entities(proposal_board)

    # The group state changes: a NEW duplicate lands after the proposal.
    with open_board_connection(proposal_board) as (_kdb, kconn):
        kconn.execute(
            f"CREATE (n:Entity {{id: 'entity_dup3', title: 'Dup 3',"
            f" content: 'c3', source_confidence: 0.9,"
            f" graph_layer: 'canonical', source_artifact_ref: '{REF}',"
            f" created_at: timestamp('2026-04-01T00:00:00')}})"
        )

    with pytest.raises(StaleProposalError) as excinfo:
        approve_dedup_proposal(proposal_board, proposal["proposal_id"])
    assert excinfo.value.code == "stale_proposal"
    assert excinfo.value.proposal_id == proposal["proposal_id"]

    # NOTHING was mutated: no tombstones, no ledger records; the proposal
    # stays pending (the operator may re-propose).
    states = _entity_states(proposal_board)
    assert all(v is None for v in states.values())
    ledger = require_equivalence_ledger()
    assert asyncio.run(ledger.active_for_board(proposal_board)) == ()
    store = require_curation_proposal_store()
    still = asyncio.run(store.get(proposal["proposal_id"]))
    assert still.status == "pending"


def test_s7_unknown_proposal_fails_structured(proposal_board):
    with pytest.raises(CurationProposalError) as excinfo:
        approve_dedup_proposal(proposal_board, "prop_nao_existe")
    assert excinfo.value.failure_reason == "curation_proposal_not_found"
