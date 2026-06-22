"""SPEC4 card 2e913ac3 — structured recovery root-cause diagnostics + at-risk on
unavailable recovery drill-down (kg_health_service).

Covers: the four distinguished root-cause categories with bounded fields
(_build_kg_root_cause), the read-only source-enumeration and safe-write probes,
the get_kg_health integration (additive root_cause block), and card detail #4 —
a board that would read HEALTHY must become at_risk when the source-enumeration
recovery drill-down is unavailable.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_recovery_root_cause.py
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

import okto_pulse.core.services.kg_health_service as kgh
from okto_pulse.core.kg import health_state as hs
from okto_pulse.core.models.db import Board
from okto_pulse.core.services.kg_health_service import (
    _build_kg_root_cause,
    _probe_rebuild_source_diagnostics,
    _probe_safe_write_diagnostics,
    get_kg_health,
)


def _src(*, count=3, fail=False, err=None):
    if fail:
        return {"source_count": None, "enumeration_failure": True, "error": err}
    return {"source_count": count, "enumeration_failure": False, "error": None}


def _sw(*, outcome="unknown", drain=False, outcomes=None):
    return {
        "last_safe_write_outcome": outcome,
        "drain_failure": drain,
        "outcomes": outcomes or {},
    }


# ---------------------------------------------------------------------------
# _build_kg_root_cause — pure, deterministic
# ---------------------------------------------------------------------------


def test_root_cause_distinguishes_four_categories_with_bounded_fields():
    rc = _build_kg_root_cause(
        total_nodes=0,
        queue_depth=2,
        dead_letter_count=8,
        active_queue={"classification": "stuck", "total_active_depth": 2},
        empty_after_materialized_history=True,
        combined_reasons=[
            "graph:wal_or_commit_errors.present",
            "graph:empty_after_materialized_history",
        ],
        source_diag=_src(count=5),
        safe_write_diag=_sw(outcome="failed", drain=True, outcomes={"failed": 1}),
    )
    cats = rc["categories"]
    assert cats["wal_or_commit_errors"]["present"] is True
    assert cats["empty_after_materialized_history"]["present"] is True
    assert cats["empty_after_materialized_history"]["materialized_node_count"] == 0
    assert cats["empty_after_materialized_history"]["source_count"] == 5
    assert cats["safe_write_drain_failure"]["present"] is True
    assert cats["source_enumeration_failure"]["present"] is False
    # bounded top-level fields
    assert rc["materialized_node_count"] == 0
    assert rc["source_count"] == 5
    assert rc["queue_state"] == {
        "queue_depth": 2,
        "active_classification": "stuck",
        "active_depth": 2,
        "dead_letter_count": 8,
    }
    assert rc["last_safe_write_outcome"] == "failed"
    assert set(rc["present_categories"]) == {
        "wal_or_commit_errors",
        "empty_after_materialized_history",
        "safe_write_drain_failure",
    }
    assert rc["drilldown_unavailable"] is False


def test_root_cause_source_enumeration_failure_marks_drilldown_unavailable():
    rc = _build_kg_root_cause(
        total_nodes=10,
        queue_depth=0,
        dead_letter_count=0,
        active_queue={"classification": "idle", "total_active_depth": 0},
        empty_after_materialized_history=False,
        combined_reasons=[],
        source_diag=_src(fail=True, err="BoardSourceStoreError: db is locked"),
        safe_write_diag=_sw(),
    )
    enum_cat = rc["categories"]["source_enumeration_failure"]
    assert enum_cat["present"] is True
    assert "BoardSourceStoreError" in enum_cat["error"]
    assert rc["drilldown_unavailable"] is True
    assert rc["source_count"] is None
    assert rc["last_safe_write_outcome"] == "unknown"
    assert rc["present_categories"] == ["source_enumeration_failure"]


# ---------------------------------------------------------------------------
# _probe_safe_write_diagnostics — read-only counter derivation
# ---------------------------------------------------------------------------


def test_safe_write_probe_unknown_when_no_record(monkeypatch):
    monkeypatch.setattr(
        "okto_pulse.core.kg.safe_write_lifecycle.get_lifecycle_counter_samples",
        lambda: [],
    )
    out = _probe_safe_write_diagnostics("board-x")
    assert out == {
        "last_safe_write_outcome": "unknown",
        "drain_failure": False,
        "outcomes": {},
    }


def test_safe_write_probe_reports_worst_and_drain_failure(monkeypatch):
    samples = [
        {"board_id": "board-x", "outcome": "applied", "count": 3},
        {"board_id": "board-x", "outcome": "failed", "count": 1},
        {"board_id": "other", "outcome": "boundary_violation", "count": 9},
    ]
    monkeypatch.setattr(
        "okto_pulse.core.kg.safe_write_lifecycle.get_lifecycle_counter_samples",
        lambda: samples,
    )
    out = _probe_safe_write_diagnostics("board-x")
    assert out["last_safe_write_outcome"] == "failed"  # worst observed for board-x
    assert out["drain_failure"] is True
    assert out["outcomes"] == {"applied": 3, "failed": 1}  # other board excluded


# ---------------------------------------------------------------------------
# _probe_rebuild_source_diagnostics — read-only, never raises
# ---------------------------------------------------------------------------


def test_source_probe_failure_is_bounded_not_raised(monkeypatch):
    class _BadStore:
        def __init__(self, **_kw):
            pass

        def fetch(self, _board_id):
            raise RuntimeError("db is locked")

    monkeypatch.setattr(
        "okto_pulse.core.kg.board_source_store.BoardSourceStore", _BadStore
    )
    out = _probe_rebuild_source_diagnostics("board-x")
    assert out["enumeration_failure"] is True
    assert "RuntimeError" in out["error"]
    assert out["source_count"] is None


# ---------------------------------------------------------------------------
# get_kg_health integration
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def fresh_board(db_factory):
    bid = f"rc-board-{uuid.uuid4().hex[:8]}"
    async with db_factory() as s:
        s.add(Board(id=bid, name="rc", owner_id="rc-user"))
        await s.commit()
    return bid


@pytest.mark.asyncio
async def test_get_kg_health_includes_structured_root_cause(db_factory, fresh_board):
    async with db_factory() as s:
        health = await get_kg_health(fresh_board, s)
    assert "root_cause" in health
    rc = health["root_cause"]
    assert set(rc["categories"]) == {
        "wal_or_commit_errors",
        "empty_after_materialized_history",
        "source_enumeration_failure",
        "safe_write_drain_failure",
    }
    for field in (
        "materialized_node_count",
        "source_count",
        "queue_state",
        "last_safe_write_outcome",
        "drilldown_unavailable",
        "present_categories",
    ):
        assert field in rc, field


@pytest.mark.asyncio
async def test_unavailable_source_drilldown_downgrades_healthy_to_at_risk(
    db_factory, fresh_board, monkeypatch
):
    # Force the low-level classifier to HEALTHY so we isolate the card #4 rule.
    healthy = hs.HealthClassification(
        state=hs.HealthState.HEALTHY,
        metric_status=hs.MetricStatus.AVAILABLE,
        reasons=tuple(),
        correlation_id=None,
    )
    monkeypatch.setattr(
        hs.KGHealthStateClassifier, "evaluate", lambda self, **_kw: healthy
    )
    # The source-enumeration recovery drill-down is unavailable.
    monkeypatch.setattr(
        kgh,
        "_probe_rebuild_source_diagnostics",
        lambda board_id: {
            "source_count": None,
            "enumeration_failure": True,
            "error": "RuntimeError: store down",
        },
    )
    async with db_factory() as s:
        health = await get_kg_health(fresh_board, s)
    # card detail #4: an unavailable recovery drill-down must NOT read healthy.
    assert health["root_cause"]["drilldown_unavailable"] is True
    assert health["overall_state"] == "at_risk"
    assert "drilldown.source_enumeration.unavailable" in health["classification_reasons"]


# ---------------------------------------------------------------------------
# Stage B — drill-down rows expose a bounded suggested next_action
# (FR fr_e29f863b / AC ac_26acf1db)
# ---------------------------------------------------------------------------


def test_canonical_debt_next_action_mapping():
    from okto_pulse.core.services import canonical_debt_service as cds

    f = cds._canonical_debt_next_action
    assert f("retry_scheduled", None) == "wait_for_scheduled_retry"
    assert f("blocked", None) == "resolve_blocker_then_retry"
    assert f("failed", None) == "retry_eligible_inspect_failure_reason"
    assert f("failed", "dlq_1") == "reprocess_via_okto_pulse_kg_dead_letter_reprocess"
    assert f("zzz_unknown", None) == "inspect_canonical_debt"
    # every purely-terminal state maps to the terminal action.
    for st in cds.TERMINAL_STATES:
        if st not in cds.RETRYABLE_STATES:
            assert f(st, None) == "inspect_terminal_debt_no_auto_retry", st


def test_canonical_debt_to_dict_includes_next_action():
    from types import SimpleNamespace

    from okto_pulse.core.services.canonical_debt_service import canonical_debt_to_dict

    row = SimpleNamespace(
        id="cd1", board_id="b", artifact_type="spec", artifact_id="s1",
        source_ref="spec:s1", source_version="1", content_hash="h",
        target_status="done", canonical_state="failed", graph_layer="canonical",
        maturity_status="canonical_eligible", failure_reason="boom",
        last_error="boom", retry_count=2, next_retry_at=None, last_attempt_at=None,
        owner_agent_id=None, correlation_id=None, queue_ref=None, dlq_ref=None,
        evidence_ref=None, created_at=None, updated_at=None,
    )
    d = canonical_debt_to_dict(row)
    # artifact type/id + state/error already present; next_action is the add.
    assert d["artifact_type"] == "spec" and d["artifact_id"] == "s1"
    assert d["canonical_state"] == "failed" and d["last_error"] == "boom"
    assert d["next_action"] == "retry_eligible_inspect_failure_reason"


def test_dlq_row_to_dict_includes_next_action():
    from types import SimpleNamespace

    from okto_pulse.core.services.dead_letter_inspector_service import _row_to_dict

    row = SimpleNamespace(
        id="dl1", board_id="b", artifact_type="card", artifact_id="c1",
        original_queue_id="q1", attempts=3, errors=[{"message": "kaboom"}],
        dead_lettered_at=None,
    )
    d = _row_to_dict(row)
    assert d["artifact_type"] == "card" and d["artifact_id"] == "c1"
    assert d["last_error"] == "kaboom"
    assert "reprocess" in d["next_action"]


def test_active_queue_next_action_mapping():
    from okto_pulse.core.services.queue_health_service import _active_queue_next_action

    assert (
        _active_queue_next_action("backpressure", "running")
        == "investigate_backpressure_pause_writes_or_scale"
    )
    assert _active_queue_next_action("stuck", "stopped") == "start_consolidation_worker"
    assert (
        _active_queue_next_action("stuck", "running")
        == "inspect_stuck_queue_check_worker"
    )
    assert (
        _active_queue_next_action("transient", "running")
        == "monitor_transient_inflight_work"
    )
    assert _active_queue_next_action("idle", "running") == "none"
