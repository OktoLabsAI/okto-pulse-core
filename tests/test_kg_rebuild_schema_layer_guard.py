"""Behavioral tests for legacy-board graph_layer schema hardening.

Spec eaf185c9 — AC6 / FR6 / TR6 (impl card 81a96a49, test card 671a70e3 =
ts_f187f4d2). Observability requirement or_1f52d4fd
(``kg_rebuild_schema_layer_migration_failure``).

These tests exercise the REAL graph engine and the REAL consolidation worker
failure path — no source inspection (TR7). Note on scope: the Grafx engine
does not support ``ALTER TABLE … DROP``, so a pre-v0.3.0 board (missing the
``graph_layer``/``maturity_status`` columns) can no longer be forged from a
current board; the MIGRATED branch is therefore covered by the unmigratable
and detector branches below, while the missing-column DETECTION contract is
pinned exactly as before via genuine engine error strings.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg import schema_layer_guard as guard
from okto_pulse.core.kg.schema_layer_guard import (
    SchemaLayerOutcome,
    ensure_graph_layer_schema,
    is_graph_layer_schema_error,
)
from okto_pulse.core.application.processors.consolidation import ConsolidationProcessor
from sqlalchemy_test_models import ConsolidationQueue


@pytest.fixture(autouse=True)
def _reset_schema_layer_counter():
    from kg_registry_testing import configure_test_kg_registry

    configure_test_kg_registry(graph_provider="real")
    guard.reset_schema_layer_migration_counter()
    yield
    guard.reset_schema_layer_migration_counter()


def _unmigratable_board_id() -> str:
    """A board id the routed binding store refuses as a storage segment —
    its schema migration can never succeed."""

    return f"../escape-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# AC6 second branch — unmigratable board yields a STRUCTURED diagnostic
# ---------------------------------------------------------------------------


def test_unmigratable_graph_yields_structured_error_not_raw():
    raw_error = "Binder exception: Cannot find property graph_layer for n."
    # A board whose route the binding store refuses cannot be migrated.
    board_id = _unmigratable_board_id()

    remediation = ensure_graph_layer_schema(board_id, raw_error=raw_error)

    assert remediation.outcome == SchemaLayerOutcome.MIGRATION_FAILED
    assert remediation.needs_structured_error is True
    assert remediation.recovered is False

    msg = remediation.structured_message
    assert msg is not None
    # Keep the component/limitation; do not offer a removed repair command.
    assert "affected graph operation is unavailable" in msg
    assert "migrate_schema" not in msg
    assert "migrate-schema" not in msg
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
# Worker wiring — unrecoverable schema dead-letters the STRUCTURED diagnostic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_dead_letters_structured_diagnostic_not_raw(
    db_factory, monkeypatch
):
    raw_error = "Binder exception: Cannot find property graph_layer for n."
    board_id = _unmigratable_board_id()  # route refused → unmigratable

    captured: dict[str, str] = {}

    async def _capture_route_to_dead_letter(db, entry, *, error_text, **kwargs):
        captured["error_text"] = error_text
        return None

    monkeypatch.setattr(
        "okto_pulse.core.application.processors.consolidation.route_to_dead_letter",
        _capture_route_to_dead_letter,
    )

    worker = ConsolidationProcessor(relational_scope_factory=db_factory)
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
    assert "affected graph operation is unavailable" in routed
    assert "migrate_schema" not in routed
    assert "migrate-schema" not in routed
    assert board_id in routed

    # or_1f52d4fd recorded the unhandled occurrence.
    assert guard.get_schema_layer_migration_event_count(
        board_id=board_id, outcome=SchemaLayerOutcome.MIGRATION_FAILED
    ) == 1
