"""Behavioral test for Global Discovery health/diagnostics (spec 849d6292,
Batch 3 — impl card c9909c53, test card 75121717 = ts_43b23305 AC8).

After the outbox digests valid nodes and SKIPS at least one legacy node without
an embedding, ``check_global`` must surface (a) the digested-type count aligned
to ``VECTOR_INDEX_TYPES`` and (b) the missing-embedding skip count — WITHOUT
recommending a rebuild, since a missing embedding on legacy data is a backfill,
not a rebuild. Exercises the REAL board graph + REAL outbox + REAL global graph.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gdh_"))

from sqlalchemy import delete

from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.global_discovery import metrics as gdm
from okto_pulse.core.application.processors.global_outbox import (
    DIGESTED_NODE_TYPES,
    GlobalOutboxProcessor,
)
from global_graph_testing import (
    bootstrap_global_discovery,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.health import check_global
from kg_schema_testing import (
    VECTOR_INDEX_TYPES,
    bootstrap_board_graph,
    open_board_connection,
)
from sqlalchemy_test_models import (
    Board,
    ConsolidationAudit,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(cypher_executor=RealBoardCypherExecutorForTests())


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


def _seed(board_id, node_type, node_id, *, with_embedding):
    with open_board_connection(board_id) as (_db, conn):
        if with_embedding:
            emb = get_embedding_provider().encode(f"{node_type} {node_id}")
            conn.execute(
                f"CREATE (n:{node_type} {{id:$id, title:$t, embedding:$e, "
                f"graph_layer:'canonical'}})",
                {"id": node_id, "t": node_id, "e": emb},
            )
        else:
            conn.execute(
                f"CREATE (n:{node_type} {{id:$id, title:$t, "
                f"graph_layer:'canonical'}})",
                {"id": node_id, "t": node_id},
            )


async def _run_outbox(db_factory, board_id, refs):
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        if await db.get(Board, board_id) is None:
            db.add(
                Board(
                    id=board_id,
                    name=f"Global Discovery Health {board_id}",
                    owner_id="global-discovery-health-test",
                )
            )
            await db.flush()
        now = datetime.now(timezone.utc)
        db.add(
            ConsolidationAudit(
                session_id=session_id,
                board_id=board_id,
                artifact_id="global-discovery-health",
                artifact_type="test",
                agent_id="global-discovery-health-test",
                started_at=now,
                committed_at=now,
            )
        )
        await db.flush()
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
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


@pytest.mark.asyncio
async def test_health_exposes_digested_types_and_skips_without_recommending_rebuild(
    db_factory,
):
    board_id = f"gdh-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    # A valid embedded Requirement + a legacy Bug WITHOUT embedding.
    _seed(board_id, "Requirement", "req_ok", with_embedding=True)
    _seed(board_id, "Bug", "bug_legacy", with_embedding=False)

    processed = await _run_outbox(
        board_id=board_id, db_factory=db_factory,
        refs=[("Requirement", "req_ok"), ("Bug", "bug_legacy")],
    )
    assert processed == 1

    health = check_global(board_id)

    # (a) digested-type count is aligned to VECTOR_INDEX_TYPES.
    assert health.counts["digested_types"] == len(VECTOR_INDEX_TYPES)
    assert len(DIGESTED_NODE_TYPES) == len(VECTOR_INDEX_TYPES)
    # The valid node was digested.
    assert health.counts["digests"] >= 1
    # (b) the missing-embedding skip is surfaced.
    assert health.counts["missing_embedding_skipped"] >= 1

    # AC8: the diagnostic must NOT recommend a rebuild for legacy missing
    # embeddings — it names the backfill instead.
    lowered = health.details.lower()
    assert "rebuild" in lowered  # appears only as "NOT a rebuild"
    assert "not a rebuild" in lowered
    assert "backfill" in lowered


@pytest.mark.asyncio
async def test_health_clean_board_reports_zero_skips(db_factory):
    board_id = f"gdh-clean-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    _seed(board_id, "Requirement", "req_ok", with_embedding=True)

    await _run_outbox(
        board_id=board_id, db_factory=db_factory,
        refs=[("Requirement", "req_ok")],
    )
    health = check_global(board_id)

    assert health.counts["missing_embedding_skipped"] == 0
    # No skip → the rebuild-avoidance note is absent (no false alarm).
    assert "not a rebuild" not in health.details.lower()
    assert health.healthy is True
