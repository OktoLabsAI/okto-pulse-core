"""FCC-07E-IMP2 — Final Clean Core ORCHESTRATION layer tests.

Covers the closed E-IMP2 scope: wiring the canonical FCC-07A/B/C/D reports into
the FCC-07E-IMP1 shell. The teeth are real:

* Each report A/B/C/D maps to a ``RunnerGateResult`` whose ``status`` is a pure
  COLLAPSE of the report's own verdict (``ok`` / ``overall_status`` / per-adapter
  ``status``) — never re-derived. Proven by injecting a blocking report while the
  real builders are monkeypatched to RAISE: a green run is only possible because
  the orchestrator read the injected verdict and never re-ran the gate.
* The C->B feed: FCC-07C's ``dependency_audit_passed`` projection
  (``as_adapter_evidence``) is threaded into ``bind_and_validate_removals``'s
  ``dependency_audit_evidence`` and FLIPS the FCC-07B outcome (blocked -> ready).
* The C->B->D/E sequence and the final aggregate ``RunnerReport``.
* Injectable: each report accepts a result OR a zero-arg callable.
* Boundary hygiene: the orchestrator imports no Community code.

No Pulse server is started; the fixtures are synthetic canonical reports plus the
real adapter inventory for the feed test.
"""

from __future__ import annotations

import ast
from pathlib import Path

import okto_pulse.core.application.boundary.final_clean_core_orchestrator as _orch_mod
from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    AdapterEvidence,
)
from okto_pulse.core.application.boundary.community_packaging_audit import (
    DependencyAuditEvidenceRow,
    DependencyAuditProjection,
)
from okto_pulse.core.application.boundary.conformance_matrix import (
    ConformanceMatrixReport,
)
from okto_pulse.core.application.boundary.final_clean_core_orchestrator import (
    GATE_FCC07A,
    GATE_FCC07B,
    GATE_FCC07C,
    GATE_FCC07D,
    orchestrate_final_clean_core,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    PackagingOwnershipReport,
    PackagingOwnershipRow,
)
from okto_pulse.core.application.boundary.readiness_aggregator import (
    ReadinessAggregateReport,
    ReadinessAggregateRow,
)
from okto_pulse.core.application.boundary.removal_evidence_binding import (
    RemovalEvidenceBinding,
)
from okto_pulse.core.application.boundary.runtime_composition_guard import (
    RuntimeCompositionGuardReport,
)
from okto_pulse.core.application.boundary.testing_provider_policy import ProviderVerdict

ORCH_PY = Path(_orch_mod.__file__).resolve()


# --------------------------------------------------------------------------- #
# Synthetic canonical-report factories (the public report DTOs only).
# --------------------------------------------------------------------------- #
def fake_matrix(*, ok: bool, missing: tuple[str, ...] = ()) -> ConformanceMatrixReport:
    return ConformanceMatrixReport(
        ok=ok,
        rows=(),
        dependency_report_ok=ok,
        adapter_keys_required=(),
        adapter_keys_covered=(),
        adapter_keys_missing=missing,
    )


def _ownership_row(symbol: str, adapter_key: str | None) -> PackagingOwnershipRow:
    return PackagingOwnershipRow(
        surface="source",
        symbol=symbol,
        classification="community_owned",
        scope="runtime",
        action="block",
        severity="blocking",
        adapter_key=adapter_key,
        owning_fcc_or_wave="R05-KG",
        diagnostic_code="import_boundary_violation",
        remediation=f"move '{symbol}' out of the productive core",
    )


def fake_ownership(
    *, ok: bool, blocking: tuple[PackagingOwnershipRow, ...] = ()
) -> PackagingOwnershipReport:
    return PackagingOwnershipReport(
        ok=ok,
        status="passed" if ok else "blocking",
        rows=blocking,
        blocking=blocking,
        temporary_exceptions=(),
        scoped_out=(),
        ledger_integrity_ok=True,
        matrix_ok=ok,
        surfaces_audited=("manifest", "lock", "source", "wheel"),
        adapter_keys_missing=(),
    )


def fake_projection(passed_by_key: dict[str, bool]) -> DependencyAuditProjection:
    rows = tuple(
        DependencyAuditEvidenceRow(
            adapter_key=key,
            owner="okto-pulse-community/kg",
            community_owned=True,
            dependency_families=(),
            dependency_audit_passed=passed,
            core_ownership_blocking_symbols=(),
            community_blocking_families=(),
            community_optional_only_families=(),
            reasons=("clean",) if passed else ("community_dependency_not_declared:x",),
        )
        for key, passed in sorted(passed_by_key.items())
    )
    return DependencyAuditProjection(
        ok=all(r.dependency_audit_passed for r in rows),
        community_pyproject_path="synthetic://pyproject.toml",
        rows=rows,
    )


def _readiness_row(adapter_key: str, status: str) -> ReadinessAggregateRow:
    return ReadinessAggregateRow(
        adapter_key=adapter_key,
        status=status,  # type: ignore[arg-type]
        reasons=(),
        missing_evidence=(),
        failed_evidence=(),
        owning_fcc=None,
        oracles_required=(),
        next_specs=(),
        evidence_field_impact=(),
        remediation=f"remediate {adapter_key}",
    )


def fake_readiness(rows: tuple[ReadinessAggregateRow, ...]) -> ReadinessAggregateReport:
    return ReadinessAggregateReport(rows=rows)


def fake_guard(
    *, violations: tuple[ProviderVerdict, ...] = ()
) -> RuntimeCompositionGuardReport:
    return RuntimeCompositionGuardReport(
        context="production",
        verdicts=violations,
        violations=violations,
        production_allowed=0,
        test_only_allowed=0,
    )


def _violation(provider_key: str) -> ProviderVerdict:
    return ProviderVerdict(
        module="okto_pulse.core.kg.providers.testing.memory_graph_store",
        classification="violation",
        reason="test-only provider in production",
        provider_key=provider_key,
        object_type="MemoryGraphStore",
        remediation="register a runtime adapter",
        composition_path=f"RuntimeComposition.providers.{provider_key}",
    )


def _gate(report, gate_id: str):
    return next(g for g in report.gates if g.gate_id == gate_id)


def _gates(report, gate_id: str):
    return [g for g in report.gates if g.gate_id == gate_id]


def _gate_for_adapter(report, gate_id: str, adapter_key: str):
    return next(
        g
        for g in report.gates
        if g.gate_id == gate_id and g.adapter_key == adapter_key
    )


# --------------------------------------------------------------------------- #
# Each report A/B/C/D -> RunnerGateResult with the right gate_id + collapsed
# status (report.ok=True -> success; report blocking -> blocked).
# --------------------------------------------------------------------------- #
def test_each_report_maps_to_gate_with_collapsed_status_success():
    report = orchestrate_final_clean_core(
        mode="quick",
        conformance_matrix=fake_matrix(ok=True),
        packaging_ownership=fake_ownership(ok=True),
        readiness=fake_readiness((_readiness_row("kuzu_graph_store", "ready"),)),
        provider_guard=fake_guard(),
    )

    assert _gate(report, GATE_FCC07A).status == "success"
    assert _gate(report, GATE_FCC07C).status == "success"
    assert _gate(report, GATE_FCC07B).status == "success"
    assert _gate(report, GATE_FCC07D).status == "success"
    assert report.overall_status == "success"
    assert report.exit_code == 0


def test_each_report_maps_to_gate_with_collapsed_status_blocked():
    report = orchestrate_final_clean_core(
        mode="full",
        conformance_matrix=fake_matrix(ok=False, missing=("kuzu_graph_store",)),
        packaging_ownership=fake_ownership(
            ok=False, blocking=(_ownership_row("kuzu", "kuzu_graph_store"),)
        ),
        readiness=fake_readiness((_readiness_row("inmemory_cache_backend", "blocked"),)),
        provider_guard=fake_guard(violations=(_violation("event_bus"),)),
    )

    assert _gate(report, GATE_FCC07A).status == "blocked"
    assert _gate(report, GATE_FCC07C).status == "blocked"
    assert _gate(report, GATE_FCC07B).status == "blocked"
    assert _gate(report, GATE_FCC07D).status == "blocked"
    assert report.overall_status == "blocked"
    assert report.exit_code != 0


def test_matrix_blocked_surfaces_missing_adapter_keys_in_remediation():
    report = orchestrate_final_clean_core(
        mode="quick", conformance_matrix=fake_matrix(ok=False, missing=("foo", "bar"))
    )
    gate = _gate(report, GATE_FCC07A)
    assert gate.status == "blocked"
    assert gate.remediation and "foo" in gate.remediation and "bar" in gate.remediation


def test_ownership_blocking_emits_one_gate_per_blocking_finding_with_identity():
    report = orchestrate_final_clean_core(
        mode="quick",
        packaging_ownership=fake_ownership(
            ok=False,
            blocking=(
                _ownership_row("kuzu", "kuzu_graph_store"),
                _ownership_row("torch", "sentence_transformer_embedding_provider"),
            ),
        ),
    )
    c_gates = _gates(report, GATE_FCC07C)
    assert len(c_gates) == 2
    assert all(g.status == "blocked" for g in c_gates)
    assert {g.adapter_key for g in c_gates} == {
        "kuzu_graph_store",
        "sentence_transformer_embedding_provider",
    }
    assert {g.dependency_family for g in c_gates} == {"kuzu", "torch"}


def test_provider_guard_violation_surfaces_provider_key():
    report = orchestrate_final_clean_core(
        mode="quick",
        provider_guard=fake_guard(
            violations=(_violation("event_bus"), _violation("graph_store"))
        ),
    )
    d_gates = _gates(report, GATE_FCC07D)
    assert len(d_gates) == 2
    assert all(g.status == "blocked" for g in d_gates)
    assert {g.provider_key for g in d_gates} == {"event_bus", "graph_store"}


def test_deferred_adapter_collapses_to_skipped_and_does_not_block():
    report = orchestrate_final_clean_core(
        mode="quick",
        readiness=fake_readiness(
            (
                _readiness_row("a_ready", "ready"),
                _readiness_row("z_deferred", "deferred"),
            )
        ),
    )
    statuses = {g.adapter_key: g.status for g in _gates(report, GATE_FCC07B)}
    assert statuses == {"a_ready": "success", "z_deferred": "skipped"}
    assert report.summary.skipped == 1
    assert report.exit_code == 0


# --------------------------------------------------------------------------- #
# The C->B feed: C's dependency_audit_passed FLIPS the FCC-07B outcome.
# --------------------------------------------------------------------------- #
def _full_binding_except_dep_audit() -> RemovalEvidenceBinding:
    """A binding for the real ``kuzu_graph_store`` adapter whose evidence is
    complete EXCEPT ``dependency_audit_passed`` (left None) — so the readiness
    verdict hinges entirely on the C->B feed."""
    return RemovalEvidenceBinding(
        adapter_key="kuzu_graph_store",
        evidence=AdapterEvidence(
            port_closed=True,
            community_registered=True,
            oracle_passed=True,
            import_audit_passed=True,
            dependency_audit_passed=None,  # <- supplied ONLY by FCC-07C's feed
            register_before_remove_passed=True,
        ),
        declared_removal_ref="R-P2-05",
    )


def test_c_to_b_feed_flips_readiness_to_ready():
    # FCC-07C says dependency_audit_passed=True for kuzu -> the binder merges it
    # into the otherwise-complete evidence -> readiness becomes ready -> success.
    report = orchestrate_final_clean_core(
        mode="quick",
        packaging_ownership=fake_ownership(ok=True),
        dependency_audit=fake_projection({"kuzu_graph_store": True}),
        removal_bindings=[_full_binding_except_dep_audit()],
    )
    b_gate = _gate(report, GATE_FCC07B)
    assert b_gate.adapter_key == "kuzu_graph_store"
    assert b_gate.status == "success"
    # evidence_fields reflect the merged input: C's dependency_audit_passed=True.
    assert b_gate.evidence_fields.as_map()["dependency_audit_passed"] is True


def test_without_c_feed_readiness_stays_blocked():
    # No FCC-07C projection -> dependency_audit_passed stays None -> blocked.
    report = orchestrate_final_clean_core(
        mode="quick",
        removal_bindings=[_full_binding_except_dep_audit()],
    )
    b_gate = _gate(report, GATE_FCC07B)
    assert b_gate.adapter_key == "kuzu_graph_store"
    assert b_gate.status == "blocked"
    assert b_gate.evidence_fields.as_map()["dependency_audit_passed"] is None


def test_c_feed_false_keeps_b_blocked():
    report = orchestrate_final_clean_core(
        mode="quick",
        dependency_audit=fake_projection({"kuzu_graph_store": False}),
        removal_bindings=[_full_binding_except_dep_audit()],
    )
    b_gate = _gate(report, GATE_FCC07B)
    assert b_gate.status == "blocked"
    assert b_gate.evidence_fields.as_map()["dependency_audit_passed"] is False


def test_c_to_b_feed_via_adapter_evidence_path():
    # The evidence-map B build path also threads C's dependency_audit_passed. The
    # aggregator reports the WHOLE inventory, so select the kuzu row explicitly.
    report = orchestrate_final_clean_core(
        mode="quick",
        dependency_audit=fake_projection({"kuzu_graph_store": True}),
        adapter_evidence={
            "kuzu_graph_store": AdapterEvidence(
                port_closed=True,
                community_registered=True,
                oracle_passed=True,
                import_audit_passed=True,
                dependency_audit_passed=None,
                register_before_remove_passed=True,
            )
        },
    )
    b_gate = _gate_for_adapter(report, GATE_FCC07B, "kuzu_graph_store")
    assert b_gate.status == "success"
    assert b_gate.evidence_fields.as_map()["dependency_audit_passed"] is True


# --------------------------------------------------------------------------- #
# NON-duplication: the gate status comes from the injected report; the real
# builders are NEVER re-run (monkeypatched to raise — a green run proves it).
# --------------------------------------------------------------------------- #
def test_no_rule_duplication_status_comes_from_injected_report(monkeypatch):
    def _boom(*args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("real builder must not run when a report is injected")

    # If the orchestrator re-derived ANY verdict it would hit one of these.
    monkeypatch.setattr(_orch_mod, "build_conformance_matrix", _boom)
    monkeypatch.setattr(_orch_mod.PackagingOwnershipGate, "run", _boom)
    monkeypatch.setattr(_orch_mod, "run_community_packaging_audit", _boom)
    monkeypatch.setattr(_orch_mod, "validate_runtime_composition_providers", _boom)
    monkeypatch.setattr(_orch_mod, "validate_injected_providers", _boom)

    # Inject a BLOCKING C report; with use_real_builders=False the orchestrator
    # must trust the injected verdict and never re-run the ownership gate.
    report = orchestrate_final_clean_core(
        mode="quick",
        conformance_matrix=fake_matrix(ok=True),
        packaging_ownership=fake_ownership(
            ok=False, blocking=(_ownership_row("kuzu", "kuzu_graph_store"),)
        ),
        readiness=fake_readiness((_readiness_row("x", "ready"),)),
        provider_guard=fake_guard(),
    )

    assert _gate(report, GATE_FCC07C).status == "blocked"
    assert _gate(report, GATE_FCC07A).status == "success"
    assert report.overall_status == "blocked"


# --------------------------------------------------------------------------- #
# Sequence C->B->D/E + the final aggregate RunnerReport.
# --------------------------------------------------------------------------- #
def test_aggregate_report_collects_all_four_gates_and_mode():
    report = orchestrate_final_clean_core(
        mode="full",
        conformance_matrix=fake_matrix(ok=True),
        packaging_ownership=fake_ownership(ok=True),
        dependency_audit=fake_projection({"kuzu_graph_store": True}),
        removal_bindings=[_full_binding_except_dep_audit()],
        provider_guard=fake_guard(),
    )
    assert {g.gate_id for g in report.gates} == {
        GATE_FCC07A,
        GATE_FCC07B,
        GATE_FCC07C,
        GATE_FCC07D,
    }
    assert report.mode == "full"
    assert report.overall_status == "success"
    # JSON-serialisable + deterministic via the reused shell projection.
    assert report.as_dict()["mode"] == "full"


def test_required_gate_ids_fail_closed_when_a_gate_is_absent():
    # FCC07D is not supplied at all -> the shell synthesises a blocking row.
    report = orchestrate_final_clean_core(
        mode="quick",
        conformance_matrix=fake_matrix(ok=True),
        required_gate_ids=[GATE_FCC07A, GATE_FCC07D],
    )
    synth = _gate(report, GATE_FCC07D)
    assert synth.status == "blocked"
    assert synth.remediation and "did not run" in synth.remediation
    assert report.overall_status == "blocked"


# --------------------------------------------------------------------------- #
# Injectable: each report accepts a result OR a zero-arg callable.
# --------------------------------------------------------------------------- #
def test_reports_accept_zero_arg_callables():
    report = orchestrate_final_clean_core(
        mode="quick",
        conformance_matrix=lambda: fake_matrix(ok=True),
        packaging_ownership=lambda: fake_ownership(ok=True),
        readiness=lambda: fake_readiness((_readiness_row("x", "ready"),)),
        provider_guard=lambda: fake_guard(),
    )
    assert report.overall_status == "success"
    assert {g.gate_id for g in report.gates} == {
        GATE_FCC07A,
        GATE_FCC07B,
        GATE_FCC07C,
        GATE_FCC07D,
    }


def test_command_results_failure_blocks_even_with_clean_gates():
    from okto_pulse.core.application.boundary.final_clean_core_runner import (
        RunnerCommandResult,
    )

    report = orchestrate_final_clean_core(
        mode="quick",
        conformance_matrix=fake_matrix(ok=True),
        command_results=[
            RunnerCommandResult(command="pytest tests/test_x.py", exit_code=1)
        ],
    )
    assert report.summary.blocking == 0
    assert report.exit_code != 0
    assert report.overall_status == "blocked"


# --------------------------------------------------------------------------- #
# Real builders: the orchestrator can run the actual FCC-07A/C builders (not just
# fixtures). Smoke only — proves the wiring is not stubbed.
# --------------------------------------------------------------------------- #
def test_real_builders_smoke_a_and_c():
    report = orchestrate_final_clean_core(mode="quick", use_real_builders=True)
    a_gate = _gate(report, GATE_FCC07A)
    c_gate = _gate(report, GATE_FCC07C)
    assert a_gate.status in {"success", "blocked"}
    assert c_gate.status in {"success", "blocked"}


# --------------------------------------------------------------------------- #
# Boundary hygiene + public surface.
# --------------------------------------------------------------------------- #
def test_orchestrator_module_does_not_import_community():
    tree = ast.parse(ORCH_PY.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert not any(name.startswith("okto_pulse.community") for name in imported), imported


def test_orchestrator_exported_from_boundary_package():
    import okto_pulse.core.application.boundary as boundary

    assert "orchestrate_final_clean_core" in boundary.__all__
    assert boundary.orchestrate_final_clean_core is orchestrate_final_clean_core
    report = boundary.orchestrate_final_clean_core(
        mode="quick", conformance_matrix=fake_matrix(ok=True)
    )
    assert report.overall_status == "success"
