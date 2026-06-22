"""SPEC4 card 619e58e1 (G1): partition-aware materialization guard for the formal
KG rebuild flow.

The run must NOT promote a generation when a partition (``graph_layer``) the
resolved source set expected fails to materialize — both the silent-canonical
-loss case (anti-empty alone would wrongly pass) and the empty_after_materialized
recovery failure. source->node is not 1:1, so the guard is per-layer PRESENCE
(every expected layer must have materialized nodes), reporting both maps.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_rebuild_layer_safety.py
"""

from __future__ import annotations

from pathlib import Path

import okto_pulse.core.kg.rebuild_service as rebuild_service
from okto_pulse.core.kg.board_rebuild_adapter import _expected_layers_from_sources
from okto_pulse.core.kg.rebuild_service import (
    RebuildBlockReason,
    RebuildOutcome,
    RebuildStepResult,
    _verify_materialized_layers,
)

# Reuse the rebuild_service test harness (sibling test module on sys.path).
from test_kg_rebuild_service import _build_service, _issue_confirmation


def _step(expected) -> RebuildStepResult:
    return RebuildStepResult(ok=True, counts={"expected_by_layer": expected})


# ---------------------------------------------------------------------------
# _expected_layers_from_sources (deterministic, no graph touch)
# ---------------------------------------------------------------------------


def test_expected_layers_counts_sources_by_graph_layer():
    sources = [
        {"id": "s1", "graph_layer": "canonical"},
        {"id": "s2", "graph_layer": "canonical"},
        {"id": "s3", "graph_layer": "working"},
        {"id": "s4"},  # missing graph_layer -> unclassified
    ]
    assert _expected_layers_from_sources(sources) == {
        "canonical": 2,
        "working": 1,
        "unclassified": 1,
    }
    assert _expected_layers_from_sources(()) == {}


# ---------------------------------------------------------------------------
# _verify_materialized_layers (unit) — per-layer presence
# ---------------------------------------------------------------------------


def test_guard_passes_when_every_expected_layer_materialized(monkeypatch):
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts",
        lambda _b: {"canonical": 3, "working": 7},
    )
    assert _verify_materialized_layers("b", _step({"canonical": 1, "working": 1})) is None


def test_guard_fails_when_expected_canonical_missing_but_working_present(monkeypatch):
    # codex's required case: anti-empty must NOT pass when a partition is lost.
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts",
        lambda _b: {"working": 7},  # canonical absent
    )
    res = _verify_materialized_layers("b", _step({"canonical": 1, "working": 1}))
    assert res is not None
    detail, materialized = res
    assert "materialized_layer_mismatch" in detail
    assert "canonical" in detail  # the missing layer is named
    assert materialized == {"working": 7}  # both maps reported for audit


def test_guard_fails_when_graph_empty(monkeypatch):
    # empty_after_materialized subcase: every expected layer is missing.
    monkeypatch.setattr(rebuild_service, "_materialized_layer_counts", lambda _b: {})
    res = _verify_materialized_layers("b", _step({"canonical": 2, "working": 1}))
    assert res is not None
    detail, _ = res
    assert "canonical" in detail and "working" in detail


def test_guard_noop_when_no_expected_partition():
    # legacy/fake step result without expected_by_layer must not trip the guard.
    assert _verify_materialized_layers("b", RebuildStepResult(ok=True)) is None
    assert _verify_materialized_layers("b", _step({})) is None


# ---------------------------------------------------------------------------
# integration: the full run flow fails closed + does not promote
# ---------------------------------------------------------------------------


def _adapter_with_expected(expected):
    def _a(_req):
        return RebuildStepResult(ok=True, counts={"expected_by_layer": expected})

    return _a


def test_run_fails_closed_and_does_not_promote_on_partition_loss(tmp_path: Path, monkeypatch):
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path, step_adapter=_adapter_with_expected({"canonical": 1, "working": 1})
    )
    # canonical partition was NOT materialized (only working) → must fail closed.
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts", lambda _b: {"working": 4}
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="recovery",
    )
    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.reason == RebuildBlockReason.MATERIALIZED_LAYER_MISMATCH.value
    # generation NOT promoted; lock released.
    assert result.current_kg_generation_id is None
    assert lock.inspect(board_id="b1") is None


def test_run_completes_when_all_expected_layers_materialize(tmp_path: Path, monkeypatch):
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path, step_adapter=_adapter_with_expected({"canonical": 1, "working": 1})
    )
    # both expected partitions materialized → promote/COMPLETED.
    monkeypatch.setattr(
        rebuild_service, "_materialized_layer_counts",
        lambda _b: {"canonical": 2, "working": 4},
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="recovery",
    )
    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.reason == RebuildBlockReason.OK.value
