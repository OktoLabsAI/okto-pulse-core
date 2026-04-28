"""NC-8 — Tests for the dedup migration CLI command (spec 7f23535f).

Covers:
- TS4: 5 duplicates consolidated to 1 canonical, edges preserved
- TS5: --dry-run does not alter the KG
- TS6: idempotent migration on board without duplicates
- TS9: report formats (table + JSON structure)

Strategy: inject duplicates directly via Cypher CREATE on an in-memory
test Kuzu graph, then run migrate_dedup_entities and assert via Cypher
counts + report shape.
"""

from __future__ import annotations

import gc
import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest


pytestmark = pytest.mark.asyncio


@pytest.fixture
def dedup_migration_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_nc8_mig_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")

    yield base

    try:
        from okto_pulse.core.kg.schema import close_all_connections
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _bootstrap_board() -> str:
    """Create an empty per-board Kuzu graph and return its id."""
    from okto_pulse.core.kg.schema import bootstrap_board_graph
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    gc.collect()
    return board_id


def _inject_entity(
    board_id: str, source_ref: str, title: str, when_iso: str
) -> str:
    """Insert one Entity node directly via Cypher with a controlled
    created_at — used to fabricate duplicates with known ordering.
    """
    from okto_pulse.core.kg.schema import open_board_connection
    node_id = f"entity_{uuid.uuid4().hex[:12]}"
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Entity {id: $id, title: $t, content: '', context: '', "
            "justification: '', source_artifact_ref: $r, "
            "created_at: timestamp($ca), created_by_agent: 'test', "
            "source_confidence: 1.0, relevance_score: 0.5, query_hits: 0, "
            "last_queried_at: NULL, priority_boost: 0.0, "
            "human_curated: false, embedding: $emb, "
            "source_session_id: 'kgses_test'})",
            {
                "id": node_id, "t": title, "r": source_ref,
                "ca": when_iso, "emb": [0.0] * 384,
            },
        )
    return node_id


def _inject_belongs_to_edge(
    board_id: str, from_id: str, to_id: str
) -> None:
    """Create a `belongs_to` rel between two existing Entity nodes."""
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        kconn.execute(
            "MATCH (a:Entity), (b:Entity) WHERE a.id = $f AND b.id = $t "
            "CREATE (a)-[:belongs_to {confidence: 1.0, layer: 'deterministic', "
            "rule_id: 'test_rule', created_by: 'test', fallback_reason: ''}]->(b)",
            {"f": from_id, "t": to_id},
        )


def _count_entities(board_id: str, source_ref: str) -> int:
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r RETURN count(n)",
            {"r": source_ref},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


def _count_belongs_to_into(board_id: str, target_id: str) -> int:
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (:Entity)-[r:belongs_to]->(b:Entity) "
            "WHERE b.id = $t RETURN count(r)",
            {"t": target_id},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# TS4 — 5 duplicates consolidated to 1, edges preserved
# ---------------------------------------------------------------------------


async def test_ts4_migration_consolidates_duplicates_preserves_edges(
    dedup_migration_tempdir,
):
    from okto_pulse.core.kg.dedup_migration import migrate_dedup_entities

    board_id = _bootstrap_board()
    source_ref = f"spec:{uuid.uuid4()}"

    # 5 Entity duplicates with monotonically increasing created_at —
    # the LAST one (index 4) becomes the canonical because the migration
    # picks max(created_at).
    dup_ids: list[str] = []
    for i in range(5):
        dup_ids.append(
            _inject_entity(
                board_id, source_ref, f"Duplicate {i}",
                f"2026-04-27T17:30:0{i}",
            )
        )
    expected_canonical = dup_ids[4]

    # Each duplicate gets 2 belongs_to edges from distinct fabricated
    # source nodes (cards), making 10 total edges that must survive.
    source_card_ids: list[str] = []
    for j in range(10):
        cid = _inject_entity(
            board_id, f"card:{uuid.uuid4()}", f"Source Card {j}",
            "2026-04-27T17:00:00",
        )
        source_card_ids.append(cid)

    edge_pairs = []
    for i, dup_id in enumerate(dup_ids):
        for k in range(2):
            src = source_card_ids[i * 2 + k]
            _inject_belongs_to_edge(board_id, src, dup_id)
            edge_pairs.append((src, dup_id))

    # Sanity: 5 duplicates + 10 source cards = 15 entities for this ref/cards
    assert _count_entities(board_id, source_ref) == 5

    report = migrate_dedup_entities(board_id, dry_run=False)

    assert report["groups"] == 1
    assert report["total_duplicates_removed"] == 4
    # Edges_repointed accounts for both inbound + outbound passes per
    # duplicate. Each dup has 2 inbound belongs_to edges → 4 dups × 2 = 8.
    assert report["edges_repointed"] >= 8

    # Only the canonical survives.
    assert _count_entities(board_id, source_ref) == 1
    # All 10 belongs_to edges from source cards now point to canonical.
    assert _count_belongs_to_into(board_id, expected_canonical) == 10


# ---------------------------------------------------------------------------
# TS5 — --dry-run does not alter the KG
# ---------------------------------------------------------------------------


async def test_ts5_dry_run_does_not_alter_graph(dedup_migration_tempdir):
    from okto_pulse.core.kg.dedup_migration import migrate_dedup_entities

    board_id = _bootstrap_board()
    source_ref = f"spec:{uuid.uuid4()}"

    dup_ids = []
    for i in range(3):
        dup_ids.append(
            _inject_entity(
                board_id, source_ref, f"Dry-Run Dup {i}",
                f"2026-04-27T18:30:0{i}",
            )
        )

    count_before = _count_entities(board_id, source_ref)
    assert count_before == 3

    report = migrate_dedup_entities(board_id, dry_run=True)
    assert report["dry_run"] is True
    assert report["groups"] == 1
    # Dry-run reports planned actions but executes none.
    assert report["total_duplicates_removed"] == 0
    assert report["edges_repointed"] == 0
    # Still has the planned counts surfaced for ops triage:
    assert report["duplicates_planned"] == 2

    count_after = _count_entities(board_id, source_ref)
    assert count_after == count_before, (
        "dry-run must not modify the graph — got count change"
    )


# ---------------------------------------------------------------------------
# TS6 — idempotent on board without duplicates
# ---------------------------------------------------------------------------


async def test_ts6_idempotent_on_clean_board(dedup_migration_tempdir):
    from okto_pulse.core.kg.dedup_migration import migrate_dedup_entities

    board_id = _bootstrap_board()
    # No duplicates.
    _inject_entity(
        board_id, f"spec:{uuid.uuid4()}", "Lonely Spec",
        "2026-04-27T19:00:00",
    )
    _inject_entity(
        board_id, f"card:{uuid.uuid4()}", "Lonely Card",
        "2026-04-27T19:00:01",
    )

    first = migrate_dedup_entities(board_id, dry_run=False)
    assert first["groups"] == 0
    assert first["total_duplicates_removed"] == 0
    assert first["edges_repointed"] == 0

    second = migrate_dedup_entities(board_id, dry_run=False)
    assert second["groups"] == 0
    assert second["total_duplicates_removed"] == 0
    assert second["edges_repointed"] == 0


# ---------------------------------------------------------------------------
# TS9 — report formats (table + JSON)
# ---------------------------------------------------------------------------


async def test_ts9_report_formats_render_correctly(dedup_migration_tempdir):
    from okto_pulse.core.kg.dedup_migration import (
        format_report_table,
        migrate_dedup_entities,
    )

    board_id = _bootstrap_board()
    source_ref = f"spec:{uuid.uuid4()}"
    for i in range(2):
        _inject_entity(
            board_id, source_ref, f"Format-Test Dup {i}",
            f"2026-04-27T20:00:0{i}",
        )

    report = migrate_dedup_entities(board_id, dry_run=True)

    # JSON shape — required keys for ops automation
    json_str = json.dumps(report, default=str)
    parsed = json.loads(json_str)
    for key in (
        "board_id", "dry_run", "groups",
        "total_duplicates_removed", "duplicates_planned",
        "edges_repointed", "edges_planned",
        "started_at", "executed_at", "details",
    ):
        assert key in parsed, f"missing key in JSON report: {key}"
    assert isinstance(parsed["details"], list)
    assert parsed["details"][0]["node_type"] == "Entity"

    # Table format — must mention column headers + canonical id
    table = format_report_table(report)
    assert "node_type" in table
    assert "source_artifact_ref" in table
    assert "duplicates_found" in table
    assert "canonical_id" in table
    assert "edges_repointed" in table
    assert "DRY-RUN" in table
    assert parsed["details"][0]["canonical_id"][:10] in table
