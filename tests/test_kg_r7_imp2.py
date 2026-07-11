"""R7 IMP2 — historical CanonicalDebt + layer-aware replay (reconcile).

Behavioral coverage for spec 7e0a5a28 / card 030d1342:

- TS3  a canonical bug-derived Learning ALREADY materialized with only a
       working Bug is recorded as CanonicalDebt (remediation debt), NOT a
       cognitive pending item and NOT DLQ.
- TS6  once the same Learning gains a canonical Bug validates edge, the
       post-commit reconcile closes the debt with canonical-only evidence.
- negative  working-layer evidence can NEVER close the debt (the
       evidence_layer=canonical pre-filter drops it; a Learning that still only
       has a working Bug keeps its debt open).
- regression  a provenance-only Learning (NOT bug-derived) never produces a
       false-positive debt.

Storage is the existing CanonicalDebt ledger (reused via canonical_debt_service);
no new store/worker. Tests drive the real detector/reconcile against a real
board graph + SQL session.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON,
    PARTITION_TARGET_STATUS,
    _canonical_only_evidence,
    _stable_content_hash,
    detect_historical_canonical_learning_debt,
    reconcile_canonical_learning_partition_debt,
)
from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from sqlalchemy_test_models import Board
from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
)

USER_ID = "user-r7-imp2"


# ---------------------------------------------------------------------------
# Board + graph setup / seed helpers
# ---------------------------------------------------------------------------


async def _setup_board(db_factory) -> str:
    """Create a unique board graph (Kùzu) + SQL Board row (CanonicalDebt FK)."""
    from kg_schema_testing import bootstrap_board_graph

    board_id = f"r7imp2-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r7 imp2", owner_id=USER_ID))
            await db.commit()
    return board_id


def _seed_node(
    kconn,
    orch,
    node_type: str,
    node_id: str,
    source_ref: str,
    *,
    graph_layer: str,
    maturity_status: str,
) -> None:
    _apply_kuzu_node_create_with_timestamp(
        orch,
        node_type,
        node_id,
        {
            "title": f"R7 imp2 {node_type}",
            "content": "",
            "context": "",
            "justification": "",
            "source_artifact_ref": source_ref,
            "created_at": "2026-06-08T00:00:00+00:00",
            "created_by_agent": "test",
            "source_confidence": 1.0,
            "relevance_score": 0.5,
            "query_hits": 0,
            "last_queried_at": None,
            "priority_boost": 0.0,
            "human_curated": False,
            "embedding": [0.0] * 384,
            "graph_layer": graph_layer,
            "maturity_status": maturity_status,
        },
    )


def _seed_learning_validating_bug(
    board_id: str, *, learning_source_ref: str, bug_layer: str
) -> tuple[str, str]:
    """Materialize a canonical Learning that validates a Bug at ``bug_layer``."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"r7l_{uuid.uuid4().hex[:12]}"
    bug_id = f"r7b_{uuid.uuid4().hex[:12]}"
    bug_maturity = (
        MATURITY_CANONICAL_ELIGIBLE
        if bug_layer == GRAPH_LAYER_CANONICAL
        else MATURITY_WORKING_IMMATURE
    )
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"r7seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn, orch, "Learning", learning_id, learning_source_ref,
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        _seed_node(
            kconn, orch, "Bug", bug_id, f"bug:{bug_id}",
            graph_layer=bug_layer, maturity_status=bug_maturity,
        )
        orch.create_edge(
            edge_type="validates",
            from_id=learning_id,
            to_id=bug_id,
            attrs={"confidence": 0.9},
            from_type="Learning",
            to_type="Bug",
        )
    return learning_id, bug_id


def _add_canonical_bug_validates(board_id: str, learning_id: str) -> str:
    """Give an EXISTING Learning a validates edge to a fresh CANONICAL Bug."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    bug_id = f"r7cb_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"r7seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn, orch, "Bug", bug_id, f"bug:{bug_id}",
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        orch.create_edge(
            edge_type="validates",
            from_id=learning_id,
            to_id=bug_id,
            attrs={"confidence": 1.0},
            from_type="Learning",
            to_type="Bug",
        )
    return bug_id


def _seed_provenance_learning(board_id: str, source_ref: str) -> str:
    """Canonical Learning connected to an Entity by belongs_to, NOT bug-derived."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"r7pl_{uuid.uuid4().hex[:12]}"
    entity_id = f"r7pe_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"r7seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn, orch, "Learning", learning_id, source_ref,
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        _seed_node(
            kconn, orch, "Entity", entity_id, f"entity:{entity_id}",
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        orch.create_edge(
            edge_type="belongs_to",
            from_id=learning_id,
            to_id=entity_id,
            attrs={"confidence": 1.0},
            from_type="Learning",
            to_type="Entity",
        )
    return learning_id


# ---------------------------------------------------------------------------
# TS3 — historical working-only canonical Learning -> CanonicalDebt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts3_historical_working_only_learning_records_canonical_debt(db_factory):
    board_id = await _setup_board(db_factory)
    source_ref = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    _seed_learning_validating_bug(
        board_id, learning_source_ref=source_ref, bug_layer=GRAPH_LAYER_WORKING
    )

    async with db_factory() as db:
        result = await detect_historical_canonical_learning_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()
    assert result["opened"] == 1

    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total == 1
    debt = listed.items[0]
    # Recorded as CanonicalDebt (remediation), NOT cognitive pending / DLQ.
    assert debt["failure_reason"] == HISTORICAL_DEBT_REASON
    assert debt["target_status"] == PARTITION_TARGET_STATUS
    assert debt["source_ref"] == source_ref
    assert debt["graph_layer"] == GRAPH_LAYER_CANONICAL
    assert debt["canonical_state"] in OPEN_STATES


@pytest.mark.asyncio
async def test_ts3_detect_is_idempotent(db_factory):
    board_id = await _setup_board(db_factory)
    source_ref = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    _seed_learning_validating_bug(
        board_id, learning_source_ref=source_ref, bug_layer=GRAPH_LAYER_WORKING
    )
    for _ in range(2):
        async with db_factory() as db:
            await detect_historical_canonical_learning_debt(
                db, board_id=board_id, actor_id="claude-coder"
            )
            await db.commit()
    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total == 1  # upsert keyed by source hash — no duplicate row


# ---------------------------------------------------------------------------
# TS6 — canonical Bug evidence closes the debt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts6_canonical_bug_evidence_closes_debt(db_factory):
    board_id = await _setup_board(db_factory)
    source_ref = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    learning_id, _working_bug = _seed_learning_validating_bug(
        board_id, learning_source_ref=source_ref, bug_layer=GRAPH_LAYER_WORKING
    )
    async with db_factory() as db:
        await detect_historical_canonical_learning_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()

    # The bug matures: the SAME Learning gains a validates -> canonical Bug edge.
    _add_canonical_bug_validates(board_id, learning_id)

    async with db_factory() as db:
        result = await reconcile_canonical_learning_partition_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()
    assert result["committed_count"] == 1

    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total == 1
    assert listed.items[0]["canonical_state"] == "committed"


# ---------------------------------------------------------------------------
# negative — working evidence never closes the debt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_working_only_learning_keeps_debt_open_on_reconcile(db_factory):
    board_id = await _setup_board(db_factory)
    source_ref = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    learning_id, _working_bug = _seed_learning_validating_bug(
        board_id, learning_source_ref=source_ref, bug_layer=GRAPH_LAYER_WORKING
    )
    async with db_factory() as db:
        await detect_historical_canonical_learning_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()

    # Learning STILL only has a working Bug — reconcile must close nothing.
    async with db_factory() as db:
        result = await reconcile_canonical_learning_partition_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()
    assert result["committed_count"] == 0

    # Even with a hash-matching evidence row at the WORKING layer, the pre-filter
    # drops it: working evidence can never close canonical debt.
    forged = {
        "source_ref": source_ref,
        "content_hash": _stable_content_hash(source_ref, learning_id),
        "evidence_layer": GRAPH_LAYER_WORKING,
    }
    async with db_factory() as db:
        result2 = await reconcile_canonical_learning_partition_debt(
            db, board_id=board_id, actor_id="claude-coder", extra_evidence=[forged]
        )
        await db.commit()
    assert result2["committed_count"] == 0

    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.items[0]["canonical_state"] in OPEN_STATES


def test_pre_filter_drops_non_canonical_evidence():
    rows = [
        {"source_ref": "a", "content_hash": "h", "evidence_layer": "canonical"},
        {"source_ref": "b", "content_hash": "h", "evidence_layer": "working"},
        {"source_ref": "c", "content_hash": "h"},  # missing layer
        {"source_ref": "d", "content_hash": "h", "evidence_layer": "none"},
    ]
    kept = _canonical_only_evidence(rows)
    assert len(kept) == 1
    assert kept[0]["source_ref"] == "a"


# ---------------------------------------------------------------------------
# regression — provenance-only Learning produces no false-positive debt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provenance_only_learning_no_false_positive_debt(db_factory):
    board_id = await _setup_board(db_factory)
    source_ref = f"learning:provenance:{uuid.uuid4()}"  # NOT bug-derived
    _seed_provenance_learning(board_id, source_ref)

    async with db_factory() as db:
        result = await detect_historical_canonical_learning_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()
    assert result["opened"] == 0

    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total == 0
