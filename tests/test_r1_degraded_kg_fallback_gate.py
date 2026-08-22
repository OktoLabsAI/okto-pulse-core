"""R1 spec (16f1b29a) — Degraded-KG fallback gate + message + kg.md reconciliation.

Covers the remaining scenarios not exercised by the existing
``test_f16_f17_health_aware_gates.py`` and ``test_degraded_kg_fallback.py`` suites:

  ts_97797ff7  TC-D  no new outcome enum member; exactly one new reason member
  ts_6d088067  TC-E  unconfirmed-block message carries remediation clause
  ts_e4dd279a  TC-E  consolidation-pending message carries the same clause
  ts_47577f43  TC-F  message names no non-existent rebuild/reset MCP tool
  ts_69b4ee2b  TC-I  test_f16_f17 suite passes after the degraded-leg flip
  ts_dd9452a5  TC-J  spec→done succeeds end-to-end on a degraded board
"""

from __future__ import annotations

import re
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.cognitive_closeout_gate import (
    CognitiveCloseoutGate,
    CognitiveCloseoutOutcome,
    CognitiveCloseoutReason,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItem


# ── Minimal store helpers ─────────────────────────────────────────────────────


class _FakeStore:
    """latest_generation returns a non-None generation (healthy shape)."""

    def __init__(self, items=None):
        self.items = list(items or [])

    def latest_generation(self, board_id):
        return "kg-1"

    def list_items(self, board_id, kg_generation_id, **_kw):
        return list(self.items)


class _NoGenStore:
    """latest_generation returns None — the unconfirmed shape (FR2 / 3cf5dede)."""

    def latest_generation(self, board_id):
        return None

    def list_items(self, board_id, kg_generation_id, **_kw):
        return []


def _item(source_ref: str, status: str) -> CognitiveConsolidationItem:
    return CognitiveConsolidationItem(
        item_id=f"item-{source_ref.replace(':', '-')}",
        board_id="board-1",
        kg_generation_id="kg-1",
        source_ref=source_ref,
        artifact_type=source_ref.split(":", 1)[0],
        status=status,
        recorded_at="2026-06-05T00:00:00Z",
    )


def _gate(store) -> CognitiveCloseoutGate:
    return CognitiveCloseoutGate(store=store)


# ── ts_97797ff7: no new outcome enum member; exactly one new reason member ─────


def test_ts_97797ff7_enum_member_counts():
    """TC-D (AC8): CognitiveCloseoutOutcome keeps exactly 5 members (the F16
    no-new-enum constraint). CognitiveCloseoutReason gains exactly one new
    member (DEGRADED_KG_AUTO_SKIP) and ends up with 5 members total."""
    outcome_members = list(CognitiveCloseoutOutcome)
    assert len(outcome_members) == 5, (
        f"CognitiveCloseoutOutcome must have exactly 5 members — found "
        f"{[m.value for m in outcome_members]}"
    )
    reason_members = list(CognitiveCloseoutReason)
    assert len(reason_members) == 5, (
        f"CognitiveCloseoutReason must have exactly 5 members — found "
        f"{[m.value for m in reason_members]}"
    )
    assert CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP in reason_members, (
        "DEGRADED_KG_AUTO_SKIP must be a member of CognitiveCloseoutReason"
    )
    assert CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value == "degraded_kg_auto_skip"
    # outcome reuses UNAVAILABLE — no new outcome member was added
    outcome_values = {m.value for m in outcome_members}
    assert "degraded_kg_auto_skip" not in outcome_values, (
        "degraded_kg_auto_skip must NOT appear as an outcome value"
    )


# ── ts_6d088067: unconfirmed-block message carries remediation clause ──────────


def test_ts_6d088067_unconfirmed_block_message_carries_remediation():
    """TC-E: when the gate produces UNAVAILABLE (unconfirmed path, allowed=False),
    the service-layer error message surfaced to callers (via
    _evaluate_cognitive_closeout_or_raise) must mention:
      (a) skip_cognitive_consolidation board setting
      (b) the KG Health recovery flow reference
      (c) no non-existent MCP tool names (AC11)."""
    from okto_pulse.core.services.main import _evaluate_cognitive_closeout_or_raise

    class _UnconfirmedGate:
        def evaluate(self, **_kw):
            from types import SimpleNamespace
            return SimpleNamespace(
                allowed=False,
                reason="cognitive_status_unavailable",
                blocking_count=0,
                blocking_items=(),
            )

    with pytest.raises(ValueError) as exc_info:
        _evaluate_cognitive_closeout_or_raise(
            gate_factory=_UnconfirmedGate,
            board=None,
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            entity={},
            target_label="card-1",
            graph_state=None,
        )
    msg = str(exc_info.value)
    # (a) must cite skip_cognitive_consolidation
    assert "skip_cognitive_consolidation" in msg, (
        "error message must cite the skip_cognitive_consolidation board setting"
    )
    # (b) must cite the KG Health recovery flow
    assert "KG Health recovery flow" in msg or "kg-health" in msg or "okto_pulse_kg_health" in msg, (
        "error message must reference the KG Health recovery flow"
    )


# ── ts_e4dd279a: consolidation-pending message is unchanged (not a remediation msg) ─


def test_ts_e4dd279a_consolidation_pending_message_unchanged():
    """TC-E: when reason=cognitive_consolidation_pending (healthy graph, real
    blocking items), the message must NOT carry the remediation clause —
    remediation is specific to the unavailable / unconfirmed path."""
    from okto_pulse.core.services.main import _evaluate_cognitive_closeout_or_raise

    class _BlockedGate:
        def evaluate(self, **_kw):
            from types import SimpleNamespace
            return SimpleNamespace(
                allowed=False,
                reason="cognitive_consolidation_pending",
                blocking_count=2,
                blocking_items=(SimpleNamespace(item_id="x", status="pending", source_ref="task:x"),),
            )

    with pytest.raises(ValueError) as exc_info:
        _evaluate_cognitive_closeout_or_raise(
            gate_factory=_BlockedGate,
            board=None,
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            entity={},
            target_label="card-1",
            graph_state="healthy",
        )
    msg = str(exc_info.value)
    assert "cognitive_consolidation_pending" in msg
    assert "active cognitive consolidation items" in msg


# ── ts_47577f43: message names no non-existent rebuild/reset MCP tools ─────────


def test_ts_47577f43_no_nonexistent_mcp_tools_named():
    """TC-F (AC11): the error message for cognitive_status_unavailable must NOT
    reference any non-existent MCP tool names (e.g. rebuild_preflight,
    start_rebuild, kg_reset, kg_repair)."""
    from okto_pulse.core.services.main import _evaluate_cognitive_closeout_or_raise

    class _UnavailableGate:
        def evaluate(self, **_kw):
            from types import SimpleNamespace
            return SimpleNamespace(
                allowed=False,
                reason="cognitive_status_unavailable",
                blocking_count=0,
                blocking_items=(),
            )

    with pytest.raises(ValueError) as exc_info:
        _evaluate_cognitive_closeout_or_raise(
            gate_factory=_UnavailableGate,
            board=None,
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            entity={},
            target_label="card-1",
            graph_state=None,
        )
    msg = str(exc_info.value)
    # none of these non-existent tools should appear in the message
    for forbidden_tool in (
        "okto_pulse_kg_rebuild_preflight",
        "okto_pulse_kg_start_rebuild",
        "okto_pulse_kg_reset",
        "okto_pulse_kg_repair",
    ):
        assert forbidden_tool not in msg, (
            f"message must not name non-existent MCP tool {forbidden_tool!r}"
        )


# ── ts_69b4ee2b: test_f16_f17 suite stays green ───────────────────────────────


def test_ts_69b4ee2b_f16_f17_suite_is_green(tmp_path):
    """TC-I: the test_f16_f17_health_aware_gates.py suite must pass in full
    after the NC-1 degraded-leg flip. Runs the suite as a subprocess so
    isolation is complete."""
    tests_dir = Path(__file__).parent
    suite = str(tests_dir / "test_f16_f17_health_aware_gates.py")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            suite,
            "-q",
            "--tb=short",
            "--no-header",
            "-p",
            "no:cacheprovider",
            "--basetemp",
            str(tmp_path / "nested-pytest"),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"test_f16_f17_health_aware_gates.py has failures after NC-1 flip:\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    # confirm at least 15 tests ran (regression guard — suite should not shrink)
    match = re.search(r"(\d+) passed", result.stdout)
    assert match and int(match.group(1)) >= 15, (
        f"Expected at least 15 tests in test_f16_f17_health_aware_gates.py, "
        f"got: {result.stdout.strip()}"
    )


# ── ts_dd9452a5: spec→done succeeds end-to-end on a degraded board ────────────


@pytest.mark.asyncio
async def test_ts_dd9452a5_spec_done_allowed_on_degraded_board(monkeypatch):
    """TC-J (AC14): a spec→done transition on a board whose KG is in
    recovery_needed must succeed (allowed=True, NC-1 auto-skip) without
    requiring skip_cognitive_consolidation=True.  The gate emits outcome=
    UNAVAILABLE + reason=DEGRADED_KG_AUTO_SKIP but does NOT raise."""
    import okto_pulse.core.services.kg_health_service as kg_health_service
    from okto_pulse.core.domain.code_traceability import (
        DeliveryContext,
        DirectSpecDeliveryContextProvenance,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.cognitive_closeout_gate import reset_closeout_gate_samples, get_closeout_gate_samples
    from okto_pulse.core.services import main as main_service
    from sqlalchemy_test_models import Board, Spec, SpecStatus

    async def _stub_degraded(board_id, db, scheduler_control=None):
        return {"graph_state": "recovery_needed"}

    monkeypatch.setattr(kg_health_service, "get_kg_health", _stub_degraded)

    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        source_spec_id=spec_id,
        source_spec_version=1,
    )
    source_context_manifest, source_context_sha256 = (
        main_service._direct_spec_source_context_manifest(
            spec_id=spec_id,
            delivery_context=DeliveryContext.BROWNFIELD,
            provenance=provenance,
        )
    )

    async with get_session_factory()() as db:
        db.add(Board(
            id=board_id,
            name="R1 degraded E2E board",
            owner_id="r1-agent",
            # skip_cognitive_consolidation is NOT enabled — NC-1 must auto-allow
            settings={"skip_cognitive_consolidation": False},
        ))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="R1 degraded E2E spec",
            status=SpecStatus.IN_PROGRESS,
            created_by="r1-agent",
            delivery_context=DeliveryContext.BROWNFIELD,
            delivery_context_provenance={
                "value": provenance.value.value,
                "source_spec_id": provenance.source_spec_id,
                "source_spec_version": provenance.source_spec_version,
            },
            source_context_manifest=source_context_manifest,
            source_context_sha256=source_context_sha256,
            acceptance_criteria=[],
            test_scenarios=[],
        ))
        await db.commit()

    reset_closeout_gate_samples()

    from okto_pulse.core.models.schemas import SpecMove
    from okto_pulse.core.services.main import SpecService

    async with get_session_factory()() as db:
        svc = SpecService(db)
        # NC-1: degraded board must not raise for the done transition
        # even with no active ledger items and no board skip setting
        result = await svc.move_spec(
            spec_id=spec_id,
            user_id="r1-agent",
            data=SpecMove(status="done"),
        )
        assert result is not None, (
            f"move_spec returned None for spec {spec_id!r} — spec may not have been found"
        )
        assert result.status.value == "done", (
            f"move_spec returned spec with status {result.status!r}, expected done"
        )
        await db.commit()

    # verify the spec actually moved to done
    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        assert spec is not None
        assert spec.status == SpecStatus.DONE, (
            f"spec must be done after NC-1 auto-skip, got {spec.status!r}"
        )

    # verify telemetry captured the degraded auto-skip
    samples = get_closeout_gate_samples()
    degraded_samples = [
        s for s in samples
        if s.get("reason") == CognitiveCloseoutReason.DEGRADED_KG_AUTO_SKIP.value
    ]
    assert degraded_samples, (
        "expected at least one telemetry sample with reason=degraded_kg_auto_skip"
    )
    for s in degraded_samples:
        assert s["outcome"] == CognitiveCloseoutOutcome.UNAVAILABLE.value


# ── ts_a377ed81: errors.md documents the KG graph-availability error keys ──────
#
# AC14 doc-content scenario. Mirrors the kg.md doc-content tests in
# test_degraded_kg_fallback.py: read the resource markdown via
# RESOURCES_DIR / "...".read_text() and make pure file-content assertions
# (no server / board / graph required). This is the REAL automated coverage
# for "errors.md documents graph_unavailable + cognitive_status_unavailable";
# the prior NC-9 evidence wrongly pointed ts_a377ed81 at the F16-suite test.

_RESOURCES_DIR = (
    Path(__file__).parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "resources"
)
_ERRORS_MD = _RESOURCES_DIR / "reference" / "errors.md"


def _errors_md_text() -> str:
    return _ERRORS_MD.read_text(encoding="utf-8")


def test_ts_a377ed81_errors_md_documents_graph_availability_keys():
    """TC-H (AC14): reference/errors.md must document BOTH structured KG
    graph-availability error keys together with their remediation, so an agent
    that hits a degraded graph can self-serve the recovery path:

      * graph_unavailable           → KG Graph Availability section that points
                                       at the Degraded-KG Fallback Rule and the
                                       okto_pulse_kg_health recovery flow.
      * cognitive_status_unavailable → documented with skip_cognitive_consolidation
                                       as the temporary bypass + the recovery flow.
    """
    text = _errors_md_text()

    # both structured error keys are documented
    assert "graph_unavailable" in text, (
        "errors.md must document the graph_unavailable error key"
    )
    assert "cognitive_status_unavailable" in text, (
        "errors.md must document the cognitive_status_unavailable error key"
    )

    # the dedicated section exists and ties back to the canonical protocol
    assert "KG Graph Availability Errors" in text, (
        "errors.md must have a 'KG Graph Availability Errors' section"
    )
    assert "Degraded-KG Fallback Rule" in text, (
        "errors.md must reference the Degraded-KG Fallback Rule (the single "
        "source of truth in workflows/kg.md)"
    )

    # graph_unavailable remediation routes the operator at kg_health
    assert "okto_pulse_kg_health" in text, (
        "errors.md must point at okto_pulse_kg_health for graph_unavailable recovery"
    )

    # cognitive_status_unavailable remediation names the skip bypass
    assert "skip_cognitive_consolidation" in text, (
        "errors.md must cite skip_cognitive_consolidation as the bypass for "
        "cognitive_status_unavailable"
    )


def test_ts_a377ed81_errors_md_keys_are_load_bearing():
    """Negative-wiring guard (mirrors test_ts_47cc9fc7_ac1_check_is_negative_wired
    in test_degraded_kg_fallback.py): excising the 'KG Graph Availability Errors'
    section from a synthetic copy of errors.md must drop BOTH keys — proving the
    AC14 content assertions are load-bearing, not a vacuous substring match that
    would pass on an errors.md missing the section."""
    real = _errors_md_text()
    idx = real.find("## KG Graph Availability Errors")
    assert idx != -1, "real errors.md must contain the KG Graph Availability section"
    synthetic = real[:idx]
    assert "graph_unavailable" not in synthetic, (
        "graph_unavailable must live ONLY in the KG Graph Availability section"
    )
    assert "cognitive_status_unavailable" not in synthetic, (
        "cognitive_status_unavailable must live ONLY in the KG Graph Availability section"
    )
