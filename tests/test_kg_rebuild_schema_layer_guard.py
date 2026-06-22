"""Behavioral tests for legacy-board graph_layer schema hardening.

Spec eaf185c9 — AC6 / FR6 / TR6 (impl card 81a96a49, test card 671a70e3 =
ts_f187f4d2). Observability requirement or_1f52d4fd
(``kg_rebuild_schema_layer_migration_failure``).

These tests exercise the REAL embedded graph engine and the REAL consolidation
worker failure path — no source inspection (TR7). The "legacy" board is forged
by dropping the ``graph_layer``/``maturity_status`` columns from a node table
so a query that depends on them raises the genuine
``Cannot find property graph_layer for n`` binder error, exactly as a
pre-v0.3.0 board would.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg import schema_layer_guard as guard
from okto_pulse.core.kg.schema import (
    bootstrap_board_graph,
    open_board_connection,
)
from okto_pulse.core.kg.schema_layer_guard import (
    SchemaLayerOutcome,
    ensure_graph_layer_schema,
    is_graph_layer_schema_error,
)
from okto_pulse.core.kg.workers.consolidation import ConsolidationWorker
from okto_pulse.core.models.db import ConsolidationQueue


@pytest.fixture(autouse=True)
def _reset_schema_layer_counter():
    guard.reset_schema_layer_migration_counter()
    yield
    guard.reset_schema_layer_migration_counter()


def _legacy_board_with_seeded_node() -> tuple[str, str]:
    """Create a board graph, seed one Entity, then drop the maturity columns to
    simulate a pre-v0.3.0 legacy graph. Returns (board_id, raw_error) where
    raw_error is the genuine binder error a graph_layer query now raises."""
    board_id = f"legacy-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Entity {id: 'e1', title: 'T', content: 'c', "
            "source_artifact_ref: 'spec:x'})"
        )
        for col in ("graph_layer", "maturity_status"):
            conn.execute(f"ALTER TABLE Entity DROP {col}")

    raw_error = None
    with open_board_connection(board_id) as (_db, conn):
        try:
            conn.execute(
                "MATCH (n:Entity) WHERE n.graph_layer = 'canonical' "
                "RETURN count(n)"
            )
        except Exception as exc:  # noqa: BLE001 — we want the engine's text
            raw_error = str(exc)
    assert raw_error is not None, "dropping graph_layer should make the query raise"
    return board_id, raw_error


def _query_canonical_count(board_id: str) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (n:Entity) WHERE n.graph_layer = 'canonical' RETURN count(n)"
        )
        row = res.get_next() if res.has_next() else None
        return int(row[0]) if row else -1


# ---------------------------------------------------------------------------
# AC6 main branch — migrate/backfill BEFORE the dependent query succeeds
# ---------------------------------------------------------------------------


def test_legacy_graph_missing_layer_migrates_before_query():
    board_id, raw_error = _legacy_board_with_seeded_node()

    # The raw binder error is recognised as a missing-layer-schema error.
    assert "graph_layer" in raw_error.lower()
    assert is_graph_layer_schema_error(raw_error)

    remediation = ensure_graph_layer_schema(board_id, raw_error=raw_error)

    # then: migration actually added the columns and the board can be queried.
    assert remediation.outcome == SchemaLayerOutcome.MIGRATED
    assert remediation.recovered is True
    added_cols = [c for cols in remediation.columns_added.values() for c in cols]
    assert "graph_layer" in added_cols
    assert "maturity_status" in added_cols

    # The previously-failing query now succeeds AND the legacy row was
    # backfilled to canonical (so it is visible to canonical reads).
    assert _query_canonical_count(board_id) == 1

    # or_1f52d4fd: exactly one handled sample, ZERO unhandled (threshold).
    assert guard.get_schema_layer_migration_event_count(
        board_id=board_id, outcome=SchemaLayerOutcome.MIGRATED
    ) == 1
    assert guard.get_schema_layer_migration_event_count(
        board_id=board_id, outcome=SchemaLayerOutcome.MIGRATION_FAILED
    ) == 0


# ---------------------------------------------------------------------------
# AC6 second branch — unmigratable board yields a STRUCTURED diagnostic
# ---------------------------------------------------------------------------


def test_unmigratable_graph_yields_structured_error_not_raw():
    raw_error = "Binder exception: Cannot find property graph_layer for n."
    # A board whose graph file does not exist cannot be migrated.
    board_id = f"missing-{uuid.uuid4().hex[:10]}"

    remediation = ensure_graph_layer_schema(board_id, raw_error=raw_error)

    assert remediation.outcome == SchemaLayerOutcome.MIGRATION_FAILED
    assert remediation.needs_structured_error is True
    assert remediation.recovered is False

    msg = remediation.structured_message
    assert msg is not None
    # Names the operational action (the migrate-schema tripleta) + the board.
    assert "okto_pulse_kg_migrate_schema" in msg
    assert board_id in msg
    # The raw error survives only as CONTEXT — it is not the whole message.
    assert "underlying_error" in msg
    assert msg != raw_error
    assert msg.strip() != raw_error.strip()

    assert guard.get_schema_layer_migration_event_count(
        board_id=board_id, outcome=SchemaLayerOutcome.MIGRATION_FAILED
    ) == 1


# ---------------------------------------------------------------------------
# Detector precision — no false positives on benign / unrelated errors
# ---------------------------------------------------------------------------


def test_detector_ignores_benign_and_unrelated_errors():
    # Benign idempotent ADD (column already exists) is NOT a missing-schema error.
    assert not is_graph_layer_schema_error(
        "Binder exception: property graph_layer already exists"
    )
    assert not is_graph_layer_schema_error(
        "Table Entity already has property maturity_status"
    )
    # Unrelated runtime errors never trip the guard.
    assert not is_graph_layer_schema_error("connection reset by peer")
    assert not is_graph_layer_schema_error(
        "Cannot find property relevance_score for n"
    )
    # The genuine missing-column signatures DO match.
    assert is_graph_layer_schema_error("Cannot find property graph_layer for n")
    assert is_graph_layer_schema_error(
        "Binder exception: property maturity_status does not exist"
    )


# ---------------------------------------------------------------------------
# Worker wiring — recovery re-pendings instead of dead-lettering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_recovers_legacy_schema_instead_of_dead_letter(db_factory):
    board_id, raw_error = _legacy_board_with_seeded_node()
    worker = ConsolidationWorker(session_factory=db_factory)

    entry = ConsolidationQueue(
        id=str(uuid.uuid4()),
        board_id=board_id,
        artifact_type="story",
        artifact_id="a1",
        attempts=0,
        status="claimed",
        worker_id="w-1",
    )

    # On recovery the guard returns BEFORE any DB work, so db is unused.
    await worker._mark_failed(
        None, entry, error_text=raw_error, max_attempts=5
    )

    # then: the entry was re-pended for an immediate retry, NOT dead-lettered.
    assert entry.status == "pending"
    assert entry.last_error is None
    assert entry.next_retry_at is not None
    assert entry.worker_id is None
    assert entry.attempts == 0  # recovery does not charge an attempt

    # The schema is now usable — the dependent query succeeds.
    assert _query_canonical_count(board_id) == 1


# ---------------------------------------------------------------------------
# Worker wiring — unrecoverable schema dead-letters the STRUCTURED diagnostic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_dead_letters_structured_diagnostic_not_raw(
    db_factory, monkeypatch
):
    raw_error = "Binder exception: Cannot find property graph_layer for n."
    board_id = f"missing-{uuid.uuid4().hex[:10]}"  # no graph file → unmigratable

    captured: dict[str, str] = {}

    async def _capture_route_to_dead_letter(db, entry, *, error_text, **kwargs):
        captured["error_text"] = error_text
        return None

    monkeypatch.setattr(
        "okto_pulse.core.kg.workers.consolidation.route_to_dead_letter",
        _capture_route_to_dead_letter,
    )

    worker = ConsolidationWorker(session_factory=db_factory)
    entry = ConsolidationQueue(
        id=str(uuid.uuid4()),
        board_id=board_id,
        artifact_type="story",  # not spec/refinement → no canonical-debt branch
        artifact_id="a2",
        attempts=4,  # +1 == max_attempts(5) → routes to DLQ
        status="claimed",
    )

    async with db_factory() as db:
        await worker._mark_failed(
            db, entry, error_text=raw_error, max_attempts=5
        )

    # then: the DLQ received the STRUCTURED, actionable diagnostic — the raw
    # binder error is NOT the sole content routed.
    routed = captured.get("error_text")
    assert routed is not None
    assert routed != raw_error
    assert "okto_pulse_kg_migrate_schema" in routed
    assert "Operational action" in routed
    assert board_id in routed

    # or_1f52d4fd recorded the unhandled occurrence.
    assert guard.get_schema_layer_migration_event_count(
        board_id=board_id, outcome=SchemaLayerOutcome.MIGRATION_FAILED
    ) == 1
