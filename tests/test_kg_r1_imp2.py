"""R1-IMP2 — query_global/MCP/REST layer coherence + KG Health
digest_vs_board_layer_mismatch.

Spec 29b35f60 / card 125dae57 (FR4/FR5/TR4/AC3/AC5). The read/diagnostic layer
around the R1 parity fix:
- query_global honors canonical|working|all using the effective digest layer,
  legacy_unknown fail-closed (AC5);
- KG Health surfaces digest_vs_board_layer_mismatch ONLY when the published
  DecisionDigest.graph_layer diverges from expected_digest_layer, ranked below
  canonical_debt/cognitive_pending/R7 partition (AC3/TR4);
- the R1-IMP1 reconciler clears the mismatch on drain.

All pipeline-driven (board graph -> outbox worker -> reconcile -> health/query);
no direct DecisionDigest seeding is used as proof.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r1i2_"))

from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.global_discovery import metrics as gdm
from okto_pulse.core.kg.global_discovery import layer_parity
from okto_pulse.core.kg.global_discovery.layer_parity import (
    PARITY_EVALUATED,
    PARITY_NOT_EVALUATED,
    PARITY_STATUS_AVAILABLE,
    PARITY_STATUS_UNAVAILABLE,
    collect_digest_layer_mismatch_inputs,
    detect_digest_layer_mismatches,
    evaluate_digest_layer_mismatch_inputs,
    list_digest_layer_mismatches,
)
from okto_pulse.core.application.processors.global_outbox import (
    DIGESTED_NODE_TYPES,
    GlobalOutboxProcessor,
)
from global_graph_testing import (
    bootstrap_global_discovery,
    execute_global_read,
    execute_global_write,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from sqlalchemy_test_models import Board, GlobalUpdateOutbox, KuzuNodeRef
from okto_pulse.core.services.kg_health_service import get_kg_health
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)

USER_ID = "user-r1-imp2"
QUERY_TEXT = "gateway caching parity health learning"
MISMATCH_CODE = "digest_vs_board_layer_mismatch"


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


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _new_board(db_factory) -> str:
    board_id = f"r1i2-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r1 imp2", owner_id=USER_ID))
            await db.commit()
    return board_id


def _seed_node(board_id, node_type, node_id, *, layer="canonical", title=QUERY_TEXT):
    emb = get_embedding_provider().encode(title)
    with open_board_connection(board_id) as (_db, conn):
        if layer is None:
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, embedding: $e}})",
                {"id": node_id, "t": title, "e": emb},
            )
        else:
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, embedding: $e, "
                f"graph_layer: $l}})",
                {"id": node_id, "t": title, "e": emb, "l": layer},
            )


def _set_node_layer(board_id, node_type, node_id, layer):
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) SET n.graph_layer = $l",
            {"id": node_id, "l": layer},
        )


async def _ensure_outbox_audit_parent(db, board_id: str, session_id: str) -> None:
    """Persist the relational parents required by KuzuNodeRef fixtures."""
    from datetime import datetime, timezone

    from sqlalchemy_test_models import ConsolidationAudit

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name=f"R1 IMP2 {board_id}", owner_id=USER_ID))
        await db.flush()
    if await db.get(ConsolidationAudit, session_id) is None:
        now = datetime.now(timezone.utc)
        db.add(
            ConsolidationAudit(
                session_id=session_id,
                board_id=board_id,
                artifact_id=f"outbox-{session_id[-16:]}",
                artifact_type="test_fixture",
                agent_id=USER_ID,
                started_at=now,
                committed_at=now,
            )
        )
        await db.flush()


async def _run_outbox(db_factory, board_id, refs) -> int:
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        await _ensure_outbox_audit_parent(db, board_id, session_id)
        for node_type, node_id in refs:
            db.add(
                KuzuNodeRef(
                    session_id=session_id,
                    board_id=board_id,
                    kuzu_node_id=node_id,
                    kuzu_node_type=node_type,
                    operation="add",
                )
            )
        db.add(
            GlobalUpdateOutbox(
                event_id=str(uuid.uuid4()),
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": len(refs)},
            )
        )
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


async def _run_outbox_no_refs(db_factory, board_id) -> int:
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(
            GlobalUpdateOutbox(
                event_id=str(uuid.uuid4()),
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 0},
            )
        )
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


def _digest_layer(board_id, node_id) -> str | None:
    res = execute_global_read(
        "MATCH (d:DecisionDigest) WHERE d.board_id = $b AND d.original_node_id = $n "
        "RETURN coalesce(d.graph_layer, 'legacy_unknown')",
        {"b": board_id, "n": node_id},
    )
    return str(res.rows[0][0]) if res.rows else None


async def _make_stale_canonical_digest(db_factory, board_id) -> str:
    """Seed a canonical Decision -> digest canonical; then demote the board node
    to working WITHOUT running the worker, leaving the digest stale (canonical)
    while expected is working."""
    nid = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", nid, layer="canonical")
    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1
    assert _digest_layer(board_id, nid) == "canonical"
    _set_node_layer(board_id, "Decision", nid, "working")  # no worker run -> stale
    return nid


def _issues_by_code(health, code):
    return [i for i in health.get("health_issues", []) if i.get("code") == code]


# ===========================================================================
# Detector + reconcile
# ===========================================================================


@pytest.mark.asyncio
async def test_unavailable_parity_is_preserved_as_not_evaluated(monkeypatch):
    unavailable_inputs = {
        "status": "unavailable",
        "reason": "global_discovery_read_failed",
        "digests": [],
        "board_meta": {},
        "needs_overlay": False,
    }
    monkeypatch.setattr(
        layer_parity,
        "collect_digest_layer_mismatch_inputs",
        lambda board_id: unavailable_inputs,
    )

    evaluated = await detect_digest_layer_mismatches(
        object(),
        board_id="board-unavailable",
    )
    assert evaluated == {
        "status": PARITY_STATUS_UNAVAILABLE,
        "evaluation": PARITY_NOT_EVALUATED,
        "reason": "global_discovery_read_failed",
        "items": [],
    }

    # The detector has one status-bearing contract: an empty item list without
    # an evaluated state can never be interpreted as healthy.
    drilldown = await list_digest_layer_mismatches(
        object(), board_id="board-unavailable"
    )
    assert drilldown["items"] == []
    assert drilldown["status"] == PARITY_STATUS_UNAVAILABLE
    assert drilldown["evaluation"] == PARITY_NOT_EVALUATED
    assert drilldown["evaluation_reason"] == "global_discovery_read_failed"


def test_collector_uses_board_type_for_mixed_duplicate_rows_and_skips_ghosts(
    monkeypatch,
):
    """Global digest type/cache corruption never selects the source label."""
    from okto_pulse.core.kg import interfaces as interfaces_module
    from okto_pulse.core.application.processors import (
        global_outbox as outbox_module,
    )

    board_id = "board-authoritative-type"
    source_id = "shared-corrupt-identity"
    ghost_id = "missing-source-identity"
    digest_id = f"dd_{board_id[:8]}_{source_id}"

    class _ReadOnlyGlobalRuntime:
        def execute(self, statement, params=None):
            # A historical file may contain physical duplicates whose cached
            # node_type values disagree.  The diagnostic must not even project
            # that untrusted property.
            assert "d.node_type" not in statement
            assert not any(
                token in statement.upper()
                for token in (" SET ", " DELETE ", " CREATE ", " MERGE ")
            )
            assert params == {"bid": board_id}
            return type(
                "Rows",
                (),
                {
                    "rows": [
                        [digest_id, source_id, "working"],
                        [digest_id, source_id, "canonical"],
                        [f"dd_{board_id[:8]}_{ghost_id}", ghost_id, "working"],
                    ]
                },
            )()

    class _BoardInventory:
        def __init__(self):
            self.labels: list[str] = []

        def execute_read_only(
            self, selected_board_id, statement, params=None, *, max_rows=1000
        ):
            assert selected_board_id == board_id
            assert params == {"after_id": ""}
            assert max_rows > 0
            assert statement.startswith("MATCH (n:")
            assert "n.embedding IS NOT NULL" in statement
            assert "n.revocation_reason IS NULL" in statement
            assert "n.superseded_by IS NULL" in statement
            assert " RETURN n.id" in statement
            label = statement.removeprefix("MATCH (n:").split(")", 1)[0]
            self.labels.append(label)
            rows = [[source_id, 1]] if label == "Decision" else []
            return {"rows": rows}

    inventory = _BoardInventory()
    runtime = _ReadOnlyGlobalRuntime()
    registry = type(
        "Registry",
        (),
        {
            "cypher_executor": inventory,
            "require_global_discovery_runtime": lambda self: runtime,
        },
    )()
    monkeypatch.setattr(
        interfaces_module,
        "get_kg_registry",
        lambda: registry,
    )
    monkeypatch.setattr(outbox_module, "get_kg_registry", lambda: registry)

    detailed_meta_calls: list[dict[str, str]] = []

    def _board_meta(_board_id, source_types):
        assert _board_id == board_id
        detailed_meta_calls.append(dict(source_types))
        return {
            source_id: {
                # Deliberately wrong echo: the inventory label must win.
                "node_type": "Learning",
                "graph_layer": "canonical",
                "source_artifact_ref": "",
                "canonical_bug_count": 0,
                "relates_to_endpoints": [],
            }
        }

    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(_board_meta),
    )

    inputs = collect_digest_layer_mismatch_inputs(board_id)

    assert inputs["status"] == "available"
    assert inventory.labels == list(DIGESTED_NODE_TYPES)
    assert detailed_meta_calls == [{source_id: "Decision"}]
    assert inputs["board_meta"][source_id]["node_type"] == "Decision"
    assert inputs["needs_overlay"] is False
    assert [
        row["node_type"]
        for row in inputs["digests"]
        if row["original_node_id"] == source_id
    ] == ["Decision", "Decision"]
    assert (
        next(row for row in inputs["digests"] if row["original_node_id"] == ghost_id)[
            "node_type"
        ]
        == ""
    )

    # One duplicate row is stale against the board's canonical Decision.  The
    # other duplicate agrees, and the vanished source is prune territory.
    assert evaluate_digest_layer_mismatch_inputs(inputs) == {
        "status": PARITY_STATUS_AVAILABLE,
        "evaluation": PARITY_EVALUATED,
        "reason": "ok",
        "items": [
            {
                "board_id": board_id,
                "digest_id": digest_id,
                "original_node_id": source_id,
                "node_type": "Decision",
                "expected_layer": "canonical",
                "actual_layer": "working",
                "source_artifact_ref": "",
            }
        ],
    }


@pytest.mark.asyncio
async def test_detector_finds_stale_digest_then_reconcile_clears(db_factory):
    board_id = await _new_board(db_factory)
    nid = await _make_stale_canonical_digest(db_factory, board_id)

    async with db_factory() as db:
        evaluation = await detect_digest_layer_mismatches(db, board_id=board_id)
    assert evaluation["status"] == PARITY_STATUS_AVAILABLE
    assert evaluation["evaluation"] == PARITY_EVALUATED
    mismatches = evaluation["items"]
    assert len(mismatches) == 1
    m = mismatches[0]
    assert m["original_node_id"] == nid
    assert m["expected_layer"] == "working"
    assert m["actual_layer"] == "canonical"
    assert m["board_id"] == board_id

    # The R1-IMP1 reconciler corrects it on the next drain.
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digest_layer(board_id, nid) == "working"
    async with db_factory() as db:
        evaluation = await detect_digest_layer_mismatches(db, board_id=board_id)
    assert evaluation["status"] == PARITY_STATUS_AVAILABLE
    assert evaluation["evaluation"] == PARITY_EVALUATED
    assert evaluation["items"] == []


# ===========================================================================
# KG Health surfacing (AC3) + cleanup
# ===========================================================================


@pytest.mark.asyncio
async def test_health_surfaces_mismatch_with_fields_then_clears(db_factory):
    board_id = await _new_board(db_factory)
    nid = await _make_stale_canonical_digest(db_factory, board_id)

    async with db_factory() as db:
        health = await get_kg_health(board_id, db)
    issues = _issues_by_code(health, MISMATCH_CODE)
    assert len(issues) == 1, issues
    issue = issues[0]
    assert issue["count"] == 1
    assert issue["drill_down_tool"] == "okto_pulse_kg_digest_layer_mismatch_list"
    # AC3: the issue carries the diagnostic fields.
    sample = issue["sample"]
    assert sample["original_node_id"] == nid
    assert sample["expected_layer"] == "working"
    assert sample["actual_layer"] == "canonical"
    assert sample["board_id"] == board_id
    assert "digest_id" in sample

    # After reconcile/drain the mismatch disappears from Health.
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    async with db_factory() as db:
        health2 = await get_kg_health(board_id, db)
    assert _issues_by_code(health2, MISMATCH_CODE) == []


# ===========================================================================
# Precedence (TR4) — never replaces a stronger cause
# ===========================================================================


@pytest.mark.asyncio
async def test_mismatch_does_not_override_canonical_debt(db_factory):
    from okto_pulse.core.kg.canonical_learning_partition import PARTITION_TARGET_STATUS
    from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

    board_id = await _new_board(db_factory)
    await _make_stale_canonical_digest(db_factory, board_id)  # a real mismatch
    async with db_factory() as db:
        await upsert_canonical_debt(
            db,
            board_id=board_id,
            artifact_type="bug",
            artifact_id="bug-prec",
            source_ref=f"card:bug:{uuid.uuid4()}:learning:p",
            content_hash="r1i2_prec",
            target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending",
            failure_reason="some_reason",
        )
        await db.commit()
        health = await get_kg_health(board_id, db)

    # TR4 invariant: with a stronger cause present (canonical_debt_open ranks
    # above digest_vs_board_layer_mismatch), the mismatch NEVER claims primary —
    # yet it is still surfaced as a (non-primary) issue, not hidden. The exact
    # primary may be an even-stronger cause (e.g. decay_scheduler_debt on a fresh
    # board); what matters is the mismatch did not override the stronger ones.
    assert health["primary_health_cause"] != MISMATCH_CODE
    assert len(_issues_by_code(health, MISMATCH_CODE)) == 1
    assert len(_issues_by_code(health, "canonical_debt_open")) == 1


# ===========================================================================
# Drilldown read model (MCP/REST share it) + metric
# ===========================================================================


@pytest.mark.asyncio
async def test_drilldown_lists_mismatch_and_emits_metric(db_factory):
    board_id = await _new_board(db_factory)
    nid = await _make_stale_canonical_digest(db_factory, board_id)

    gdm.reset_global_discovery_metrics()
    async with db_factory() as db:
        result = await list_digest_layer_mismatches(db, board_id=board_id)

    assert result["count"] == 1
    assert result["health_issue_code"] == MISMATCH_CODE
    item = result["items"][0]
    assert item["original_node_id"] == nid
    assert item["expected_layer"] == "working" and item["actual_layer"] == "canonical"
    assert "source_artifact_ref" in item and "digest_id" in item
    # Bounded metric is queryable by board + expected/actual layer.
    assert gdm.get_digest_layer_mismatch_count(board_id=board_id) == 1
    assert (
        gdm.get_digest_layer_mismatch_count(
            board_id=board_id,
            expected_layer="working",
            actual_layer="canonical",
        )
        == 1
    )
    assert "board_id" in gdm.get_digest_layer_mismatch_labels()


# ===========================================================================
# query_global layer coherence (FR4/AC5) + legacy_unknown fail-closed
# ===========================================================================


@pytest.mark.asyncio
async def test_query_global_layer_coherence_and_legacy_fail_closed(db_factory):
    board_id = await _new_board(db_factory)
    canon = f"dec_c_{uuid.uuid4().hex[:8]}"
    work = f"dec_w_{uuid.uuid4().hex[:8]}"
    legacy = f"req_l_{uuid.uuid4().hex[:8]}"
    _seed_node(board_id, "Decision", canon, layer="canonical")
    _seed_node(board_id, "Decision", work, layer="working")
    _seed_node(board_id, "Requirement", legacy, layer=None)  # no graph_layer
    assert (
        await _run_outbox(
            db_factory,
            board_id,
            [("Decision", canon), ("Decision", work), ("Requirement", legacy)],
        )
        == 1
    )

    svc = get_kg_service()

    def _ids(layer):
        return {
            r["id"]
            for r in svc.query_global(
                QUERY_TEXT,
                user_boards=[board_id],
                graph_layer=layer,
                min_similarity=0.1,
            )
        }

    canonical_ids = _ids("canonical")
    working_ids = _ids("working")
    all_ids = _ids("all")

    # canonical-only: only the canonical node; never working or legacy_unknown.
    assert canon in canonical_ids
    assert work not in canonical_ids
    assert legacy not in canonical_ids
    # working scope returns the working node, not the canonical one.
    assert work in working_ids and canon not in working_ids
    # all is diagnostic: surfaces every layer including legacy_unknown.
    assert {canon, work, legacy} <= all_ids


# ===========================================================================
# Scenario gap fills (R1 test cards)
# ===========================================================================


@pytest.mark.asyncio
async def test_legacy_digest_later_gets_expected_layer_via_reconcile(db_factory):
    """ts_94e83637 (2nd clause): a legacy_unknown digest is fail-closed, and once
    its board node acquires a real layer the reconciler maps it to the expected
    layer (no longer stuck outside canonical)."""
    board_id = await _new_board(db_factory)
    nid = f"req_{uuid.uuid4().hex[:8]}"
    _seed_node(board_id, "Requirement", nid, layer=None)  # no layer -> legacy_unknown
    assert await _run_outbox(db_factory, board_id, [("Requirement", nid)]) == 1
    assert _digest_layer(board_id, nid) == "legacy_unknown"

    # The board node later acquires a canonical layer; reconcile maps it.
    _set_node_layer(board_id, "Requirement", nid, "canonical")
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digest_layer(board_id, nid) == "canonical"


def _seed_digest_directly(board_id, *, digest_id, original_node_id, layer):
    """THEATER: write a DecisionDigest straight into the global graph, bypassing
    the board graph -> outbox pipeline. Used ONLY to PROVE the pipeline rejects
    fabricated state (anti-test-theater), never as parity proof."""
    emb = get_embedding_provider().encode("theater digest")
    execute_global_write(
        "CREATE (d:DecisionDigest {id:$did, board_id:$bid, original_node_id:$oid, "
        "title:'theater', one_line_summary:'theater', node_type:'Decision', "
        "graph_layer:$l, embedding:$e, created_at:timestamp('2026-06-15T00:00:00')})",
        {
            "did": digest_id,
            "bid": board_id,
            "oid": original_node_id,
            "l": layer,
            "e": emb,
        },
        operation="test_r1_imp2_seed_theater_digest",
    )
    execute_global_write(
        "MATCH (b:Board {board_id:$bid}), (d:DecisionDigest {id:$did}) "
        "MERGE (b)-[:CONTAINS_DECISION]->(d)",
        {"bid": board_id, "did": digest_id},
        operation="test_r1_imp2_link_theater_digest",
    )


@pytest.mark.asyncio
async def test_seed_only_digest_rejected_by_pipeline(db_factory):
    """ts_1fbc3bf8 anti-test-theater: a directly-seeded DecisionDigest is NOT a
    valid canonical proof. The pipeline PRUNES a ghost digest (no board node) and
    RECONCILES a fabricated layer down to the board-derived expected layer. Only
    a digest backed by a real board node at the right layer survives canonical."""
    board_id = await _new_board(db_factory)
    # Seed one REAL digest via the pipeline so the global Board node exists.
    real = f"dec_real_{uuid.uuid4().hex[:8]}"
    _seed_node(board_id, "Decision", real, layer="canonical")
    assert await _run_outbox(db_factory, board_id, [("Decision", real)]) == 1
    assert _digest_layer(board_id, real) == "canonical"

    # (a) GHOST: fabricated canonical digest with NO board node behind it.
    ghost = "ghost_node_theater"
    _seed_digest_directly(
        board_id,
        digest_id=f"dd_{board_id[:8]}_{ghost}",
        original_node_id=ghost,
        layer="canonical",
    )
    # (b) LIAR: a real board node at 'working' but a digest fabricating 'canonical'.
    liar = f"dec_liar_{uuid.uuid4().hex[:8]}"
    _seed_node(board_id, "Decision", liar, layer="working")
    _seed_digest_directly(
        board_id,
        digest_id=f"dd_{board_id[:8]}_{liar}",
        original_node_id=liar,
        layer="canonical",
    )

    # One pipeline drain (prune + reconcile) — no fresh add refs.
    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    # Ghost fabrication (no source) is pruned away.
    assert _digest_layer(board_id, ghost) is None
    # Fabricated 'canonical' layer is reconciled down to the board's real 'working'.
    assert _digest_layer(board_id, liar) == "working"
    # The genuine pipeline-produced canonical digest survives.
    assert _digest_layer(board_id, real) == "canonical"
