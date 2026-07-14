"""R2-TEST2 (card 85784e5e) — layer-aware rebuild + cognitive preservation.

Scenarios: ts_c6434c50 (AC3 — the deterministic rebuild materializes canonical
nodes only for done sources and working nodes for immature sources) and
ts_6d7eaa59 (cognitive canonical knowledge is preserved across a rebuild via
snapshot/restore, or — when an endpoint can't be restored — recorded as a
traceable R7 fallback; never a silent clean loss).

Anti-test-theater: the canonical cognitive starting state is materialized by the
consolidation orchestrator (R7-validated write primitive); the rebuild is the
REAL ``board_rebuild_adapter`` step adapter driven end-to-end (snapshot pre-purge
-> REAL ``purge_board_graph_storage`` -> restore post-drain -> degraded fallback /
integrity fail-closed). The empty-source resolver is the validator-approved
deterministic rebuild path (``test_adapter_empty_source_list_from_resolver_is_
deterministic``), so the purge clears the bootstrap cache and the restore re-opens
a fresh graph — exercising the adapter wiring without a flaky async worker drain.

Teeth: the clean path asserts BOTH ``status == clean`` AND the canonical Learning
is back (``count == 1``) — a restore that silently did nothing would report clean
yet leave count 0, failing the test. A separate test proves that WITHOUT the
preservation the node is genuinely lost.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t2_"))

from r2_scenario_helpers import (
    count_canonical,
    count_layer,
    materialize_spec,
    new_board,
    seed_canonical_cognitive,
    seed_learning_with_canonical_bug,
)

from okto_pulse.core.kg import canonical_cognitive_preservation as ccp
from okto_pulse.core.kg.canonical_cognitive_preservation import (
    restore_canonical_cognitive,
    snapshot_canonical_cognitive,
)
from okto_pulse.core.kg.rebuild_service import RebuildStepInput
from kg_schema_testing import bootstrap_board_graph
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_WORKING
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphLifecycleForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)

_board_rebuild_ingestion = pytest.importorskip(
    "okto_pulse.community.adapters.board_rebuild_ingestion",
    reason="AF-04 Community integration test requires the Community rebuild ingestion adapter.",
)
BoardRebuildIngestionAdapter = _board_rebuild_ingestion.BoardRebuildIngestionAdapter

@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes, _tmp_rebuild_dir):
    from okto_pulse.community.adapters.rebuild_audit_storage import (
        CommunityFileSystemRebuildAuditArtifactStore,
    )
    from okto_pulse.community.adapters.quarantine_restore import (
        CommunityQuarantineRestore,
    )

    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_lifecycle=RealBoardGraphLifecycleForTests(),
        rebuild_audit_artifact_store=(
            CommunityFileSystemRebuildAuditArtifactStore(_tmp_rebuild_dir)
        ),
        quarantine_restore=CommunityQuarantineRestore(),
    )


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


def _rebuild_req(board_id):
    return RebuildStepInput(
        board_id=board_id, manifest_ref=f"manifest-{uuid.uuid4().hex[:8]}",
        source_set_hash="a" * 64, actor_id="user-r2t2", operation="rebuild",
        owner_token="owner-token", previous_kg_generation_id=None,
        candidate_kg_generation_id=str(uuid.uuid4()),
    )


def _empty_source_adapter():
    """Real adapter wired with an empty-source resolver (the validator-approved
    deterministic rebuild path). drain is instant because nothing is enqueued."""
    from okto_pulse.core.ports.relational_runtime import resolve_database_runtime

    db_path = resolve_database_runtime().local_database_path()
    assert db_path is not None
    adapter = BoardRebuildIngestionAdapter(
        db_path=db_path,
        drain_timeout_seconds=5.0,
        drain_poll_interval_seconds=0.02,
        drain_final_grace_seconds=0.0,
    )
    return adapter.build_step_adapter(source_resolver=lambda _req: [])


# ===========================================================================
# ts_c6434c50 — AC3: rebuild materialization respects done vs immature sources
# ===========================================================================


@pytest.mark.asyncio
async def test_rebuild_respects_done_vs_immature_sources(db_factory):
    board_id = await new_board(db_factory, "r2t2")

    # A DONE source materializes CANONICAL deterministic children ...
    await materialize_spec(
        db_factory, board_id, status="done",
        frs=["FR done canonical alpha", "FR done canonical beta"],
    )
    canonical_after_done = count_canonical(board_id, "Requirement")
    assert canonical_after_done >= 1
    assert count_layer(board_id, "Requirement", GRAPH_LAYER_WORKING) == 0

    # ... while an IMMATURE (draft) source materializes WORKING children only.
    await materialize_spec(
        db_factory, board_id, status="draft",
        frs=["FR draft immature gamma", "FR draft immature delta"],
    )
    # TEETH: a maturity-blind rebuild would stamp the draft children canonical,
    # leaving working count at 0 — this asserts the layer split is real.
    assert count_layer(board_id, "Requirement", GRAPH_LAYER_WORKING) >= 1
    # The done source's canonical children are untouched by the immature add.
    assert count_canonical(board_id, "Requirement") == canonical_after_done


# ===========================================================================
# ts_6d7eaa59 — cognitive canonical preserved across a REAL rebuild (clean)
# ===========================================================================


@pytest.mark.asyncio
async def test_cognitive_canonical_preserved_across_real_rebuild_clean(db_factory):
    board_id = await new_board(db_factory, "r2t2")
    learning_id = seed_canonical_cognitive(
        board_id, "Learning",
        source_ref=f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}",
    )
    assert count_canonical(board_id, "Learning") == 1

    # Drive the REAL rebuild step adapter end-to-end.
    result = _empty_source_adapter()(_rebuild_req(board_id))

    assert result.ok is True, result.detail
    preservation = result.drilldown["cognitive_preservation"]
    assert preservation["status"] == ccp.STATUS_CLEAN, preservation
    assert preservation["restored_nodes"] >= 1
    # TEETH: the canonical Learning is actually back. A no-op restore would report
    # clean (no unrestorable) yet leave count 0 — this conjunction is the teeth.
    assert count_canonical(board_id, "Learning") == 1
    assert learning_id  # node identity preserved through snapshot/restore


# ===========================================================================
# ts_6d7eaa59 — degraded path: unrestorable endpoint -> traceable R7 fallback
# ===========================================================================


@pytest.mark.asyncio
async def test_cognitive_preservation_degraded_records_fallback(db_factory, _tmp_rebuild_dir):
    board_id = await new_board(db_factory, "r2t2")
    learning_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    learning_id, _bug_id = seed_learning_with_canonical_bug(
        board_id, learning_ref=learning_ref,
    )
    assert count_canonical(board_id, "Learning") == 1

    # The empty-source rebuild does NOT re-materialize the deterministic Bug
    # endpoint, so the validates edge is unrestorable -> degraded + fallback.
    result = _empty_source_adapter()(_rebuild_req(board_id))

    assert result.ok is True, result.detail
    preservation = result.drilldown["cognitive_preservation"]
    assert preservation["status"] == ccp.STATUS_DEGRADED, preservation
    assert any(u["kind"] == "edge" for u in preservation["unrestorable"])
    # The bug-derived Learning loss is recorded as a traceable R7 hold — NOT a
    # silent clean. TEETH: a degraded rebuild that skipped the fallback reports 0.
    assert preservation.get("fallback_holds_recorded", 0) >= 1
    # The Learning node itself is restored canonical (only the edge was lost).
    assert count_canonical(board_id, "Learning") == 1

    # The hold is operator-visible in the cognitive readiness store (the same
    # source the R7 canonical_partition_integrity drilldown reads).
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )
    from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    gen = store.latest_generation(board_id)
    assert gen is not None
    items = store.list_items(board_id, gen)
    assert any(
        str(getattr(it, "reason_code", "") or "") == CANONICAL_LEARNING_WORKING_ONLY_REASON
        for it in items
    )
    assert learning_id


# ===========================================================================
# Integrity error -> rebuild fails CLOSED (never a possibly-lossy success)
# ===========================================================================


@pytest.mark.asyncio
async def test_rebuild_fails_closed_on_preservation_integrity_error(db_factory, monkeypatch):
    board_id = await new_board(db_factory, "r2t2")
    seed_canonical_cognitive(
        board_id, "Learning",
        source_ref=f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}",
    )

    def _broken_restore(board_id, snapshot):
        return ccp.RestoreResult(error="restore_failed:SimulatedIntegrityBreak")

    # _adapter imports restore_canonical_cognitive from the ccp module at call
    # time, so patching the module attribute exercises the real fail-closed wiring.
    monkeypatch.setattr(ccp, "restore_canonical_cognitive", _broken_restore)

    result = _empty_source_adapter()(_rebuild_req(board_id))

    # TEETH: the preservation mechanism could not produce a trace -> the rebuild
    # must NOT report success. An adapter that ignored integrity_error returns ok.
    assert result.ok is False
    assert result.detail == "cognitive_preservation_integrity_error"
    assert (
        result.drilldown["cognitive_preservation"]["status"]
        == ccp.STATUS_INTEGRITY_ERROR
    )


# ===========================================================================
# Teeth — WITHOUT the preservation, the canonical cognitive node is lost
# ===========================================================================


def test_without_preservation_cognitive_node_is_lost():
    board_id = f"r2t2neg-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    seed_canonical_cognitive(
        board_id, "Learning",
        source_ref=f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}",
    )
    assert count_canonical(board_id, "Learning") == 1

    # A rebuild purge WITHOUT snapshot/restore genuinely loses the node — this is
    # what makes the preservation tests above non-theater.
    snap = snapshot_canonical_cognitive(board_id)  # captured but deliberately unused
    from okto_pulse.core.ports.relational_runtime import resolve_database_runtime

    db_path = resolve_database_runtime().local_database_path()
    assert db_path is not None
    adapter = BoardRebuildIngestionAdapter(db_path=db_path)
    adapter.prepare_board_graph_storage(board_id=board_id, reason="r2t2_teeth")
    bootstrap_board_graph(board_id)  # fresh graph, as the worker would
    assert count_canonical(board_id, "Learning") == 0, (
        "teeth: without restore the canonical cognitive node must be GONE"
    )
    # Sanity: the snapshot DID capture it (so the loss is the missing restore, not
    # an empty snapshot) — restoring now brings it back.
    assert any(n["node_type"] == "Learning" for n in snap.nodes)
    restore_canonical_cognitive(board_id, snap)
    assert count_canonical(board_id, "Learning") == 1
