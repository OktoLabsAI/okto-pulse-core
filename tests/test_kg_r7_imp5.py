"""R7 IMP5 — Global Discovery canonical-only completeness filter (TS8, ts_d6106ac9).

Spec 7e0a5a28 / card 79caef5c. The owner-raised inconsistency: a Learning can
look connected in the working+canonical graph (``connected_all``) yet be
disconnected in canonical-only. ``connected_all`` is DIAGNOSTIC; the rule for
canonical publication is ``complete_canonical``: a canonical Learning whose
mandatory semantic (Bug) evidence is working-only, or which carries an open
canonical_debt / active cognitive_pending hold, must NOT be published/treated as
complete canonical in Global Discovery.

Every worker/query test runs the REAL board graph + REAL outbox worker + REAL
global discovery graph (no source inspection — validator bar). The fix corrects
the DIGEST publication layer to 'working'; the canonical board-graph SOURCE node
is NEVER demoted — tests assert that explicitly.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_imp5_"))

from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON,
    PARTITION_TARGET_STATUS,
)
from okto_pulse.core.kg.canonical_partition_integrity import (
    CLASSIFICATION_WEAK_PROVENANCE,
    evaluate_canonical_learning_publication,
    pending_or_debt_exclusions,
)
from okto_pulse.core.kg.global_discovery.layer_parity import (
    resolve_expected_digest_layer,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.global_discovery import metrics as gdm
from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor
from global_graph_testing import (
    bootstrap_global_discovery,
    execute_global_read,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from okto_pulse.core.kg.primitives import _apply_graph_node_create
from okto_pulse.core.kg.rebuild_audit import (
    normalize_cognitive_artifact_id,
    record_cognitive_working_only_hold,
)
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from sqlalchemy_test_models import Board, GlobalUpdateOutbox, KuzuNodeRef
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)

USER_ID = "user-r7-imp5"
QUERY_TEXT = "caching strategy for the gateway learning"


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes, _tmp_rebuild_dir):
    from okto_pulse.community.adapters.rebuild_audit_storage import (
        CommunityFileSystemRebuildAuditArtifactStore,
    )

    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        rebuild_audit_artifact_store=(
            CommunityFileSystemRebuildAuditArtifactStore(_tmp_rebuild_dir)
        ),
    )


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_global():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


@pytest.fixture(autouse=True)
def _reset_gd_metrics():
    gdm.reset_global_discovery_metrics()
    yield
    gdm.reset_global_discovery_metrics()


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    """Point the cognitive store (used by the pending/debt overlay) at a tmp dir."""
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers — real board graph + real outbox flow
# ---------------------------------------------------------------------------


async def _new_board(db_factory) -> str:
    board_id = f"imp5-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r7 imp5", owner_id=USER_ID))
            await db.commit()
    return board_id


def _node_attrs(source_ref, graph_layer, maturity, *, title, embedding):
    return {
        "title": title, "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "test", "source_confidence": 1.0, "relevance_score": 0.5,
        "query_hits": 0, "last_queried_at": None, "priority_boost": 0.0,
        "human_curated": False, "embedding": embedding,
        "graph_layer": graph_layer, "maturity_status": maturity,
    }


def _seed_learning(
    board_id, *, source_ref, title=QUERY_TEXT, canonical=0, working=0, relates_to=(),
) -> tuple[str, list[str], list[str]]:
    """Seed one CANONICAL Learning (embedded on ``title``) with ``canonical`` /
    ``working`` layer Bug evidence via ``validates`` edges. ``relates_to`` is an
    iterable of ``(endpoint_node_type, endpoint_layer)`` — each materializes an
    endpoint node + a ``relates_to`` edge (S-KG-02 non-bug taxonomy association).
    Returns (learning_id, canonical_bug_ids, working_bug_ids)."""
    learning_id = f"imp5l_{uuid.uuid4().hex[:12]}"
    canon_ids: list[str] = []
    work_ids: list[str] = []
    emb = get_embedding_provider().encode(title)
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_graph_node_create(
            orch, "Learning", learning_id,
            _node_attrs(
                source_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE,
                title=title, embedding=emb,
            ),
        )
        for _ in range(canonical):
            bug_id = f"imp5cb_{uuid.uuid4().hex[:10]}"
            _apply_graph_node_create(
                orch, "Bug", bug_id,
                _node_attrs(
                    f"bug:{bug_id}", GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE,
                    title=f"canon bug {bug_id}", embedding=[0.0] * 384,
                ),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug")
            canon_ids.append(bug_id)
        for _ in range(working):
            bug_id = f"imp5wb_{uuid.uuid4().hex[:10]}"
            _apply_graph_node_create(
                orch, "Bug", bug_id,
                _node_attrs(
                    f"bug:{bug_id}", GRAPH_LAYER_WORKING, MATURITY_WORKING_IMMATURE,
                    title=f"working bug {bug_id}", embedding=[0.0] * 384,
                ),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 0.9}, from_type="Learning", to_type="Bug")
            work_ids.append(bug_id)
        for endpoint_type, layer in relates_to:
            maturity = (
                MATURITY_CANONICAL_ELIGIBLE if layer == GRAPH_LAYER_CANONICAL
                else MATURITY_WORKING_IMMATURE
            )
            ep_id = f"imp5ep_{uuid.uuid4().hex[:10]}"
            _apply_graph_node_create(
                orch, endpoint_type, ep_id,
                _node_attrs(
                    f"{endpoint_type.lower()}:{ep_id}", layer, maturity,
                    title=f"ep {ep_id}", embedding=[0.0] * 384,
                ),
            )
            orch.create_edge(edge_type="relates_to", from_id=learning_id, to_id=ep_id,
                             attrs={"confidence": 1.0}, from_type="Learning", to_type=endpoint_type)
    return learning_id, canon_ids, work_ids


def _delete_node(board_id, node_type, node_id):
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) DETACH DELETE n", {"id": node_id},
        )


async def _ensure_outbox_audit_parent(db, board_id: str, session_id: str) -> None:
    """Persist the relational parents required by KuzuNodeRef fixtures."""
    from datetime import datetime, timezone

    from sqlalchemy_test_models import ConsolidationAudit

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name=f"R7 IMP5 {board_id}", owner_id=USER_ID))
        await db.flush()
    if await db.get(ConsolidationAudit, session_id) is None:
        now = datetime.now(timezone.utc)
        db.add(ConsolidationAudit(
            session_id=session_id,
            board_id=board_id,
            artifact_id=f"outbox-{session_id[-16:]}",
            artifact_type="test_fixture",
            agent_id=USER_ID,
            started_at=now,
            committed_at=now,
        ))
        await db.flush()


async def _run_outbox(db_factory, board_id, refs) -> int:
    """Insert KuzuNodeRef add-rows + one outbox event, then process it.

    ``refs`` is a list of (node_type, node_id). Returns processed count."""
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))  # isolate from other modules
        await _ensure_outbox_audit_parent(db, board_id, session_id)
        for node_type, node_id in refs:
            db.add(KuzuNodeRef(
                session_id=session_id, board_id=board_id,
                kuzu_node_id=node_id, kuzu_node_type=node_type, operation="add",
            ))
        db.add(GlobalUpdateOutbox(
            event_id=str(uuid.uuid4()), board_id=board_id, session_id=session_id,
            event_type="consolidation_committed",
            payload={"session_id": session_id, "nodes_added": len(refs)},
        ))
        await db.commit()
    worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
    return await worker.process_once()


def _digest_layer(board_id, node_id) -> str | None:
    res = execute_global_read(
        "MATCH (d:DecisionDigest) WHERE d.board_id = $b AND d.original_node_id = $n "
        "RETURN d.graph_layer",
        {"b": board_id, "n": node_id},
    )
    return str(res.rows[0][0]) if res.rows else None


def _source_layer(board_id, learning_id) -> str | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (l:Learning {id: $id}) RETURN l.graph_layer", {"id": learning_id},
        )
        return str(res.get_next()[0]) if res.has_next() else None


def _bug_ref(learning_source_ref: str) -> str:
    """Build a canonical-debt/cognitive source_ref aligned to a Learning's ref."""
    return learning_source_ref


# ===========================================================================
# Policy (pure) — the complete_canonical publication rule (FR8)
# ===========================================================================


def test_policy_working_only_is_not_publishable():
    publishable, reason = evaluate_canonical_learning_publication(
        source_artifact_ref="card:bug:abc:learning:1", canonical_bug_count=0,
    )
    assert publishable is False
    assert reason == CANONICAL_LEARNING_WORKING_ONLY_REASON


def test_policy_mixed_is_publishable_working_never_counts():
    # >=1 canonical endpoint => publishable even with working edges present.
    publishable, reason = evaluate_canonical_learning_publication(
        source_artifact_ref="card:bug:abc:learning:1", canonical_bug_count=1,
    )
    assert publishable is True
    assert reason is None


def test_policy_non_bug_requires_resolved_source_and_canonical_taxonomy():
    # S-KG-02 / TR60 supersedes the R7-IMP5 "#4 non-bug always publishable" rule:
    # a non-bug Learning is complete-canonical ONLY with a RESOLVED source AND a
    # canonical relates_to to one of the seven S-KG-01 taxonomy endpoints. The
    # authority shares the single classifier so it never diverges from the
    # read-model diagnostic (no un-sourced/un-associated non-bug canonical leak on
    # the digest/parity path).

    # Resolved source, NO canonical taxonomy association -> NOT publishable.
    publishable, reason = evaluate_canonical_learning_publication(
        source_artifact_ref="spec:spec-1:learning:0", canonical_bug_count=0,
    )
    assert publishable is False
    assert reason == CLASSIFICATION_WEAK_PROVENANCE

    # Resolved source + a canonical relates_to taxonomy endpoint -> publishable.
    publishable, reason = evaluate_canonical_learning_publication(
        source_artifact_ref="spec:spec-1:learning:0", canonical_bug_count=0,
        relates_to_endpoints=(("Decision", GRAPH_LAYER_CANONICAL),),
    )
    assert publishable is True
    assert reason is None

    # A working-layer taxonomy endpoint is fail-closed (mirrors the guard).
    publishable, _reason = evaluate_canonical_learning_publication(
        source_artifact_ref="spec:spec-1:learning:0", canonical_bug_count=0,
        relates_to_endpoints=(("Decision", GRAPH_LAYER_WORKING),),
    )
    assert publishable is False


def test_resolve_expected_digest_layer_non_bug_downgrades_without_taxonomy():
    # The shared resolver downgrades a non-bug canonical Learning to 'working' when
    # the publication authority does not publish, and keeps it 'canonical' when a
    # canonical taxonomy relates_to is present (S-KG-02 threading).
    layer, reason = resolve_expected_digest_layer(
        node_type="Learning", raw_graph_layer=GRAPH_LAYER_CANONICAL,
        source_artifact_ref="spec:spec-1:learning:0", canonical_bug_count=0,
        relates_to_endpoints=(),
    )
    assert layer == GRAPH_LAYER_WORKING
    assert reason == CLASSIFICATION_WEAK_PROVENANCE

    layer, reason = resolve_expected_digest_layer(
        node_type="Learning", raw_graph_layer=GRAPH_LAYER_CANONICAL,
        source_artifact_ref="spec:spec-1:learning:0", canonical_bug_count=0,
        relates_to_endpoints=(("Decision", GRAPH_LAYER_CANONICAL),),
    )
    assert layer == GRAPH_LAYER_CANONICAL
    assert reason is None


def test_policy_overlay_debt_pending_never_masked():
    # Even with canonical evidence, an open debt/pending overlay forces exclusion.
    publishable, reason = evaluate_canonical_learning_publication(
        source_artifact_ref="card:bug:abc:learning:1", canonical_bug_count=3,
        overlay_exclusion_reason=HISTORICAL_DEBT_REASON,
    )
    assert publishable is False
    assert reason == HISTORICAL_DEBT_REASON


# ===========================================================================
# Overlay — pending/debt artifact map (normalized matching, debt outranks)
# ===========================================================================


@pytest.mark.asyncio
async def test_pending_or_debt_exclusions_maps_both_and_normalizes(
    db_factory, _tmp_rebuild_dir,
):
    board_id = await _new_board(db_factory)

    # A debt registered under a `card:<uuid>` alias must be keyed at the SAME
    # normalized artifact_id as a `bug:<uuid>` lookup (#3 normalization).
    shared_uuid = uuid.uuid4().hex
    debt_ref = f"card:{shared_uuid}"
    pending_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"

    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id=shared_uuid[:36],
            source_ref=debt_ref, content_hash="imp5_debt",
            target_status=PARTITION_TARGET_STATUS, canonical_state="pending",
            failure_reason=HISTORICAL_DEBT_REASON,
        )
        await db.commit()

    record_cognitive_working_only_hold(
        board_id=board_id,
        hold_payload={
            "reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
            "source_ref": pending_ref, "artifact_type": "bug",
            "observed_endpoints": [f"kg:b@{GRAPH_LAYER_WORKING}"],
            "session_id": "sess-imp5",
        },
        actor_id="system:historical_consolidation",
        base_dir=_tmp_rebuild_dir,
    )

    async with db_factory() as db:
        out = await pending_or_debt_exclusions(db, board_id=board_id)

    # Debt is keyed at the alias-collapsed id (card:<uuid> == bug:<uuid>).
    assert out[normalize_cognitive_artifact_id(f"bug:{shared_uuid}")] == HISTORICAL_DEBT_REASON
    assert out[normalize_cognitive_artifact_id(pending_ref)] == CANONICAL_LEARNING_WORKING_ONLY_REASON


# ===========================================================================
# Worker e2e — DIGEST publication-layer correction (source node untouched)
# ===========================================================================


@pytest.mark.asyncio
async def test_worker_excludes_working_only_learning_from_canonical(db_factory):
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid, _c, _w = _seed_learning(board_id, source_ref=ref, canonical=0, working=1)

    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1

    # The DIGEST publication layer is downgraded to 'working' (excluded from
    # canonical-only), but the board-graph SOURCE node stays canonical (NOT a
    # source demotion — adjustment #1).
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING
    assert _source_layer(board_id, lid) == GRAPH_LAYER_CANONICAL
    # Metric counted the working-only exclusion.
    assert gdm.get_canonical_incomplete_excluded_count(
        board_id=board_id, reason_code=CANONICAL_LEARNING_WORKING_ONLY_REASON,
    ) == 1


@pytest.mark.asyncio
async def test_worker_keeps_mixed_learning_canonical(db_factory):
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid, _c, _w = _seed_learning(board_id, source_ref=ref, canonical=1, working=1)

    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    # Mixed with >=1 canonical endpoint stays complete-canonical.
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_CANONICAL
    assert gdm.get_canonical_incomplete_excluded_count(board_id=board_id) == 0


@pytest.mark.asyncio
async def test_worker_non_bug_taxonomy_gates_digest_layer(db_factory):
    # S-KG-02 / TR60 end-to-end (threading proof): the outbox worker reads the
    # Learning's relates_to taxonomy endpoints and publishes the digest at
    # 'canonical' ONLY when a canonical S-KG-01 endpoint association exists; a
    # resolved-but-unassociated non-bug Learning is downgraded to 'working'
    # (supersedes the R7-IMP5 "#4 always kept canonical").
    board_id = await _new_board(db_factory)
    gdm.reset_canonical_incomplete_excluded_counter()

    # (a) resolved source, NO canonical taxonomy relates_to -> downgraded to working.
    ref_weak = f"spec:spec-{uuid.uuid4().hex}:learning:0"
    lid_weak, _c, _w = _seed_learning(board_id, source_ref=ref_weak, canonical=0, working=0)
    assert await _run_outbox(db_factory, board_id, [("Learning", lid_weak)]) == 1
    assert _digest_layer(board_id, lid_weak) == GRAPH_LAYER_WORKING
    assert _source_layer(board_id, lid_weak) == GRAPH_LAYER_CANONICAL  # source NEVER demoted
    assert gdm.get_canonical_incomplete_excluded_count(board_id=board_id) == 1

    # (b) same shape + a CANONICAL relates_to taxonomy endpoint -> kept canonical.
    ref_ok = f"spec:spec-{uuid.uuid4().hex}:learning:0"
    lid_ok, _c2, _w2 = _seed_learning(
        board_id, source_ref=ref_ok, canonical=0, working=0,
        relates_to=(("Decision", GRAPH_LAYER_CANONICAL),),
    )
    assert await _run_outbox(db_factory, board_id, [("Learning", lid_ok)]) == 1
    assert _digest_layer(board_id, lid_ok) == GRAPH_LAYER_CANONICAL
    # No NEW exclusion for the publishable one.
    assert gdm.get_canonical_incomplete_excluded_count(board_id=board_id) == 1


@pytest.mark.asyncio
async def test_worker_corrects_stale_canonical_digest(db_factory):
    """Adjustment #2 — a digest already published as canonical (pre-fix) is
    UPDATED to 'working' when the Learning becomes working-only and the worker
    re-runs (the SET branch, not just CREATE)."""
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid, canon_ids, _w = _seed_learning(board_id, source_ref=ref, canonical=1, working=1)

    # First run: mixed => digest published canonical (the "stale canonical" row).
    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_CANONICAL

    # The only canonical evidence disappears => the Learning is now working-only.
    _delete_node(board_id, "Bug", canon_ids[0])

    # Re-run: the existing canonical digest must be corrected to 'working'.
    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING
    assert _source_layer(board_id, lid) == GRAPH_LAYER_CANONICAL


@pytest.mark.asyncio
async def test_worker_debt_overlay_excludes_even_with_canonical_evidence(
    db_factory, _tmp_rebuild_dir,
):
    """Adjustment #3 — an OPEN canonical_debt for the artifact keeps the Learning
    out of complete-canonical publication even though its graph evidence looks
    complete (never masked)."""
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid, _c, _w = _seed_learning(board_id, source_ref=ref, canonical=1, working=0)

    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id=uuid.uuid4().hex[:36],
            source_ref=_bug_ref(ref), content_hash="imp5_overlay_debt",
            target_status=PARTITION_TARGET_STATUS, canonical_state="pending",
            failure_reason=HISTORICAL_DEBT_REASON,
        )
        await db.commit()

    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING
    assert _source_layer(board_id, lid) == GRAPH_LAYER_CANONICAL
    assert gdm.get_canonical_incomplete_excluded_count(
        board_id=board_id, reason_code=HISTORICAL_DEBT_REASON,
    ) == 1


# ===========================================================================
# query_global — the PUBLIC canonical-only path does not leak; all stays diagnostic
# ===========================================================================


@pytest.mark.asyncio
async def test_query_global_canonical_excludes_incomplete_all_includes(db_factory):
    """Adjustment #5 — canonical-only query_global does NOT return the
    working-only Learning, while graph_layer=all still surfaces it (publication-
    layer diagnostic, NOT a working source). A complete-canonical Learning is the
    control that DOES surface in canonical."""
    board_id = await _new_board(db_factory)
    incomplete_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    complete_ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    # Both embedded on the SAME text so the vector query matches both — the layer
    # filter is the only differentiator.
    l_incomplete, _c1, _w1 = _seed_learning(
        board_id, source_ref=incomplete_ref, canonical=0, working=1,
    )
    l_complete, _c2, _w2 = _seed_learning(
        board_id, source_ref=complete_ref, canonical=1, working=0,
    )
    assert await _run_outbox(
        db_factory, board_id, [("Learning", l_incomplete), ("Learning", l_complete)],
    ) == 1

    svc = get_kg_service()

    canonical_rows = svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="canonical", min_similarity=0.1,
    )
    canonical_ids = {r["id"] for r in canonical_rows}
    assert l_complete in canonical_ids, "complete-canonical Learning must surface"
    assert l_incomplete not in canonical_ids, (
        "working-only Learning LEAKED into canonical-only query_global"
    )

    all_rows = svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="all", min_similarity=0.1,
    )
    all_ids = {r["id"] for r in all_rows}
    # all is diagnostic: it still surfaces the incomplete fact (publication-layer
    # 'working' diagnostic), proving the fix MARKS rather than ERASES it.
    assert {l_incomplete, l_complete} <= all_ids
