"""Tests for spec bdcda842 Sprint 1 (Foundation) — IMPL-1 + IMPL-5.

Covers AC6, AC8, AC14 (foundational scenarios that gate the rest of the
sprint chain).
"""

from __future__ import annotations

import warnings

import pytest

from okto_pulse.core.infra.config import configure_settings, get_settings
from sqlalchemy_test_models import (
    ConsolidationDeadLetter,
    ConsolidationQueue,
)


@pytest.fixture(autouse=True)
def _restore_core_settings():
    """Snapshot + restore the CoreSettings singleton around each test."""
    original = get_settings()
    yield
    configure_settings(original)


@pytest.fixture(autouse=True)
def _isolate_legacy_env(monkeypatch):
    """Ensure the legacy KG_MAX_QUEUE_DEPTH env var doesn't leak between tests."""
    monkeypatch.delenv("KG_MAX_QUEUE_DEPTH", raising=False)
    monkeypatch.delenv("KG_QUEUE_ALERT_THRESHOLD", raising=False)


# ----------------------------------------------------------------------
# IMPL-1 smoke — schema migrations took effect
# ----------------------------------------------------------------------


def test_impl1_consolidation_queue_has_resilience_columns():
    """ORM model exposes the 4 new columns from TR1 (worker_id, claim_timeout_at,
    attempts, next_retry_at). No DB roundtrip — just SQLAlchemy reflection on
    the declarative class."""
    cols = {c.name for c in ConsolidationQueue.__table__.columns}
    assert "worker_id" in cols
    assert "claim_timeout_at" in cols
    assert "attempts" in cols
    assert "next_retry_at" in cols


def test_impl1_consolidation_dead_letter_table_exists():
    """ConsolidationDeadLetter ORM class is registered with the expected
    columns from TR2."""
    cols = {c.name for c in ConsolidationDeadLetter.__table__.columns}
    expected = {
        "id", "board_id", "artifact_type", "artifact_id",
        "original_queue_id", "attempts", "errors",
        "dead_lettered_at", "created_at",
    }
    assert expected.issubset(cols)


# ----------------------------------------------------------------------
# AC6 — PUT kg_grafx_buffer_pool_mb dispara restart_required (Graph DB)
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# Native startup values never convert the removed queue-depth environment alias.
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted, expected", [({}, 321), ({"kg_queue_alert_threshold": 777}, 777)])
async def test_removed_queue_depth_alias_does_not_override_native_settings(
    monkeypatch, persisted, expected,
):
    from unittest.mock import AsyncMock
    import sqlalchemy_test_runtime_settings_service as service

    base = get_settings()
    configure_settings(type(base)(**{**base.model_dump(), "kg_queue_alert_threshold": 321}))
    monkeypatch.setenv("KG_MAX_QUEUE_DEPTH", "500")
    monkeypatch.setattr(service, "_load_persisted_rows", AsyncMock(return_value=persisted))
    with warnings.catch_warnings(record=True) as captured:
        snapshot = await service.apply_persisted_settings_to_core_settings()
    assert snapshot["kg_queue_alert_threshold"] == expected
    assert get_settings().kg_queue_alert_threshold == expected
    assert not hasattr(service, "_resolve_legacy_env_aliases")
    assert not any("KG_MAX_QUEUE_DEPTH" in str(item.message) for item in captured)


# ----------------------------------------------------------------------
# AC14 — PUT out-of-range retorna 422 e não persiste
# ----------------------------------------------------------------------
