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
# AC8 — KG_MAX_QUEUE_DEPTH legacy env mapeia para alert_threshold + warning
# ----------------------------------------------------------------------


def test_ac8_legacy_env_maps_with_deprecation_warning(monkeypatch, caplog):
    """AC8: KG_MAX_QUEUE_DEPTH=500 (env) sem KG_QUEUE_ALERT_THRESHOLD →
    alert_threshold=500 + DeprecationWarning emitido."""
    from sqlalchemy_test_runtime_settings_service import _resolve_legacy_env_aliases

    monkeypatch.setenv("KG_MAX_QUEUE_DEPTH", "500")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        resolved = _resolve_legacy_env_aliases()

    assert resolved.get("kg_queue_alert_threshold") == 500
    deprecation = [w for w in captured if issubclass(w.category, DeprecationWarning)]
    assert len(deprecation) == 1
    msg = str(deprecation[0].message)
    assert "KG_MAX_QUEUE_DEPTH" in msg
    assert "v0.5.0" in msg
    assert "kg_queue_alert_threshold" in msg


def test_ac8_legacy_env_yields_to_canonical(monkeypatch):
    """Quando ambos KG_MAX_QUEUE_DEPTH e KG_QUEUE_ALERT_THRESHOLD estão
    setados, canonical wins e legacy é ignorado (sem warning emitido para
    o canonical)."""
    from sqlalchemy_test_runtime_settings_service import _resolve_legacy_env_aliases

    monkeypatch.setenv("KG_MAX_QUEUE_DEPTH", "500")
    monkeypatch.setenv("KG_QUEUE_ALERT_THRESHOLD", "9999")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        resolved = _resolve_legacy_env_aliases()

    # Legacy alias is skipped because canonical env is set.
    assert "kg_queue_alert_threshold" not in resolved
    assert not any(
        issubclass(w.category, DeprecationWarning) for w in captured
    )


# ----------------------------------------------------------------------
# AC14 — PUT out-of-range retorna 422 e não persiste
# ----------------------------------------------------------------------
