"""FCC-07E-IMP2 — Final Clean Core runner ORCHESTRATION layer.

This module is the WIRING that turns the canonical FCC-07A/B/C/D reports into the
:class:`~okto_pulse.core.application.boundary.final_clean_core_runner.RunnerGateResult`
inputs of the FCC-07E-IMP1 SHELL
(:func:`~okto_pulse.core.application.boundary.final_clean_core_runner.run_final_clean_core`)
and produces the deterministic ``RunnerReport``.

It is the layer the E-IMP1 docstring deferred ("It NEVER wires the real
FCC-07A/B/C/D gates (that is FCC-07E-IMP2)"). It is ADDITIVE, deterministic and
synchronous, imports ONLY public core symbols (never ``okto_pulse.community``)
and touches no private symbol of any other boundary module.

What it DOES (closed E-IMP2 scope):

* CONSUMES the four canonical reports as PUBLIC interfaces — injected results,
  zero-arg callables that produce them, OR (when nothing is injected) the real
  builders — and MAPS each report to one or more ``RunnerGateResult`` rows. The
  gate ``status`` is a pure COLLAPSE of the report's own verdict
  (``ConformanceMatrixReport.ok`` / ``ReadinessAggregateRow.status`` /
  ``PackagingOwnershipReport.ok`` / ``RuntimeCompositionGuardReport.overall_status``)
  into the shell's ``success`` | ``blocked`` | ``skipped`` enum. The decision is
  NEVER re-derived here — token mapping, readiness, ownership policy, provider
  policy and ``dependency_audit_passed`` all stay where they are computed.
* PRESERVES the C->B->D/E sequence: FCC-07C's ``dependency_audit_passed``
  projection (:meth:`DependencyAuditProjection.as_adapter_evidence`) FEEDS the
  ``dependency_audit_evidence`` of FCC-07B's
  :func:`bind_and_validate_removals`; C produces it, B consumes it, E orchestrates
  and reports it.
* Calls :func:`run_final_clean_core` (the E-IMP1 shell — NOT reimplemented) with
  the mapped gate results and returns the ``RunnerReport``.

What it does NOT do (out of E-IMP2): run focused pytest (E-IMP3), run the
full-mode smoke with a real venv (E-IMP4), or docs (E-IMP5). The shell is reused
verbatim.

Collapse table (report verdict -> shell ``RunnerGateStatus``):

* FCC-07A ``ConformanceMatrixReport.ok``         -> ``success`` / ``blocked``
* FCC-07B ``ReadinessAggregateRow.status``       -> ``ready``=``success`` /
  ``blocked``=``blocked`` / ``deferred``=``skipped`` (one row per adapter)
* FCC-07C ``PackagingOwnershipReport.ok``        -> ``success`` / ``blocked``
  (one ``blocked`` row per ``PackagingOwnershipReport.blocking`` finding)
* FCC-07D ``RuntimeCompositionGuardReport.overall_status`` (``passed``/``blocked``)
  -> ``success`` / ``blocked`` (one ``blocked`` row per violation)
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Union

from .adapter_readiness_inventory import AdapterEvidence, AdapterInventoryEntry
from .community_packaging_audit import (
    DependencyAuditProjection,
    run_community_packaging_audit,
)
from .conformance_matrix import ConformanceMatrixReport, build_conformance_matrix
from .final_clean_core_runner import (
    CommandInput,
    RunnerGateResult,
    RunnerGateStatus,
    RunnerMode,
    RunnerReport,
    run_final_clean_core,
)
from .packaging_ownership_gate import (
    PackagingOwnershipGate,
    PackagingOwnershipGateInput,
    PackagingOwnershipReport,
)
from .readiness_aggregator import (
    ReadinessAggregateReport,
    aggregate_adapter_readiness,
)
from .removal_evidence_binding import (
    RemovalBindingReport,
    RemovalEvidenceBinding,
    bind_and_validate_removals,
)
from .runtime_composition_guard import (
    RuntimeCompositionGuardReport,
    RuntimeProviderObservation,
    validate_injected_providers,
    validate_runtime_composition_providers,
)
from .testing_provider_policy import ProviderContext

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids a runtime composition import
    from pathlib import Path

    from ...composition import RuntimeComposition

# --------------------------------------------------------------------------- #
# Stable gate identity (the card's FCC07A..FCC07D gate ids) + defaults.
# --------------------------------------------------------------------------- #
GATE_FCC07A = "FCC07A"
GATE_FCC07B = "FCC07B"
GATE_FCC07C = "FCC07C"
GATE_FCC07D = "FCC07D"

#: Default human spec labels per gate (overridable via ``spec_ids``).
DEFAULT_SPEC_IDS: dict[str, str] = {
    GATE_FCC07A: "FCC-07A",
    GATE_FCC07B: "FCC-07B",
    GATE_FCC07C: "FCC-07C",
    GATE_FCC07D: "FCC-07D",
}

#: Default owner per gate (purely descriptive; never consulted for the decision).
DEFAULT_OWNERS: dict[str, str] = {
    GATE_FCC07A: "okto-pulse-core/boundary",
    GATE_FCC07B: "okto-pulse-core/boundary",
    GATE_FCC07C: "okto-pulse-core/architecture",
    GATE_FCC07D: "okto-pulse-core/runtime",
}

#: COLLAPSE of an FCC-07B readiness row status into the shell gate status. The
#: decision is produced by ``evaluate_adapter_readiness`` (FCC-07B) — this is a
#: pure projection of its result, not a re-derivation. ``deferred`` (the port
#: boundary is still open, an upstream predecessor must close it) is NOT a clean-
#: core blocker, so it collapses to ``skipped``.
READINESS_STATUS_TO_GATE: dict[str, RunnerGateStatus] = {
    "ready": "success",
    "blocked": "blocked",
    "deferred": "skipped",
}

#: A B report exposes ``.rows`` either as readiness rows (IMP1 aggregator) or the
#: bound subset (IMP2 binding report) — both carry ``adapter_key`` / ``status`` /
#: ``remediation``, so the projection is duck-typed over the shared shape.
BReport = Union[ReadinessAggregateReport, RemovalBindingReport]


def _resolve(value: object) -> object:
    """Resolve a report that may be supplied as an already-built result OR a
    zero-arg callable producing one. Report DTOs are frozen dataclasses (not
    callable), so only an explicit callable is invoked."""
    return value() if callable(value) else value


# --------------------------------------------------------------------------- #
# Per-report mappers: each maps a CANONICAL report -> RunnerGateResult row(s).
# The gate status is a pure collapse of the report's own verdict (never
# re-derived). Identity fields (adapter_key / dependency_family / provider_key)
# are PROJECTED from the report's findings.
# --------------------------------------------------------------------------- #
def map_conformance_matrix(
    report: ConformanceMatrixReport, *, spec_id: str, owner: str | None
) -> tuple[RunnerGateResult, ...]:
    """FCC-07A -> one gate row. ``status = success`` iff ``report.ok`` (the matrix
    has no blocking row); the matrix is a holistic gate so no single identity key
    is attached, but unmet required adapter keys flow into the remediation."""
    status: RunnerGateStatus = "success" if report.ok else "blocked"
    remediation: str | None = None
    if not report.ok:
        if report.adapter_keys_missing:
            remediation = (
                "FCC-07A conformance matrix is non-conformant; missing required "
                f"adapter keys: {', '.join(report.adapter_keys_missing)}."
            )
        else:
            remediation = (
                "FCC-07A conformance matrix has a blocking row; resolve every "
                "blocking dependency/import finding."
            )
    return (
        RunnerGateResult(
            gate_id=GATE_FCC07A,
            spec_id=spec_id,
            status=status,
            owner=owner,
            remediation=remediation,
        ),
    )


def map_readiness(
    report: BReport,
    *,
    spec_id: str,
    owner: str | None,
    evidence_by_key: Mapping[str, AdapterEvidence] | None = None,
) -> tuple[RunnerGateResult, ...]:
    """FCC-07B -> one gate row PER adapter row. The per-adapter ``status`` is the
    VERBATIM collapse of ``ReadinessAggregateRow.status`` (produced by
    ``evaluate_adapter_readiness``); ``evidence_fields`` carries the adapter's
    :class:`AdapterEvidence` (the merged input, incl. FCC-07C's
    ``dependency_audit_passed``) when available."""
    evidence_by_key = evidence_by_key or {}
    rows: list[RunnerGateResult] = []
    for row in sorted(report.rows, key=lambda r: r.adapter_key):
        rows.append(
            RunnerGateResult(
                gate_id=GATE_FCC07B,
                spec_id=spec_id,
                status=READINESS_STATUS_TO_GATE[row.status],
                owner=owner,
                adapter_key=row.adapter_key,
                evidence_fields=evidence_by_key.get(
                    row.adapter_key, AdapterEvidence()
                ),
                remediation=row.remediation,
            )
        )
    return tuple(rows)


def map_packaging_ownership(
    report: PackagingOwnershipReport, *, spec_id: str, owner: str | None
) -> tuple[RunnerGateResult, ...]:
    """FCC-07C -> gate row(s). ``status = success`` iff ``report.ok`` (no blocking
    ownership row AND the ledger integrity holds). When blocked, one ``blocked``
    row per ``report.blocking`` finding so the offending ``adapter_key`` /
    dependency token is surfaced — the ownership policy is NOT re-evaluated, the
    blocking set is read straight off the injected report."""
    if report.ok:
        return (
            RunnerGateResult(
                gate_id=GATE_FCC07C,
                spec_id=spec_id,
                status="success",
                owner=owner,
            ),
        )
    if report.blocking:
        return tuple(
            RunnerGateResult(
                gate_id=GATE_FCC07C,
                spec_id=spec_id,
                status="blocked",
                owner=owner,
                adapter_key=brow.adapter_key,
                dependency_family=brow.symbol,
                remediation=brow.remediation,
            )
            for brow in report.blocking
        )
    # ok is False with no blocking row -> ledger integrity failure (fail-closed).
    return (
        RunnerGateResult(
            gate_id=GATE_FCC07C,
            spec_id=spec_id,
            status="blocked",
            owner=owner,
            remediation=(
                "FCC-07C packaging ownership failed its ledger-integrity check "
                "(ledger_integrity_ok is False)."
            ),
        ),
    )


def map_provider_guard(
    report: RuntimeCompositionGuardReport, *, spec_id: str, owner: str | None
) -> tuple[RunnerGateResult, ...]:
    """FCC-07D -> gate row(s). ``status = success`` iff
    ``report.overall_status == "passed"``. When blocked, one ``blocked`` row per
    violation so the offending ``provider_key`` is surfaced — the test-only
    provider policy is NOT re-run, the violations are read off the report."""
    if report.overall_status == "passed":
        return (
            RunnerGateResult(
                gate_id=GATE_FCC07D,
                spec_id=spec_id,
                status="success",
                owner=owner,
            ),
        )
    return tuple(
        RunnerGateResult(
            gate_id=GATE_FCC07D,
            spec_id=spec_id,
            status="blocked",
            owner=owner,
            provider_key=verdict.provider_key,
            remediation=(
                verdict.remediation
                + (
                    f" (composition_path={verdict.composition_path})"
                    if verdict.composition_path
                    else ""
                )
                if verdict.remediation
                else None
            ),
        )
        for verdict in report.violations
    )


# --------------------------------------------------------------------------- #
# C->B feed helper.
# --------------------------------------------------------------------------- #
def _c_evidence(
    projection: DependencyAuditProjection | None,
) -> dict[str, AdapterEvidence]:
    """The FCC-07C ``{adapter_key: AdapterEvidence(dependency_audit_passed=...)}``
    projection that FEEDS FCC-07B (empty when C is not supplied)."""
    return projection.as_adapter_evidence() if projection is not None else {}


def _merge_dependency_audit(
    evidence: AdapterEvidence,
    adapter_key: str,
    c_evidence: Mapping[str, AdapterEvidence],
) -> AdapterEvidence:
    """Override only ``dependency_audit_passed`` with FCC-07C's authoritative
    value (the single-field FEED). Used to reconstruct the merged INPUT for
    ``evidence_fields`` display — the readiness DECISION is still produced by
    FCC-07B's evaluator/binder, never here."""
    c = c_evidence.get(adapter_key)
    if c is None:
        return evidence
    return replace(evidence, dependency_audit_passed=c.dependency_audit_passed)


def _build_b_report(
    *,
    c_evidence: Mapping[str, AdapterEvidence],
    removal_bindings: Iterable[RemovalEvidenceBinding] | None,
    adapter_evidence: Mapping[str, AdapterEvidence] | None,
    inventory: tuple[AdapterInventoryEntry, ...] | None,
) -> tuple[BReport, dict[str, AdapterEvidence]]:
    """Build the FCC-07B report from inputs, threading the C->B feed.

    Returns ``(report, evidence_for_display)`` where ``evidence_for_display`` is
    the merged ``{adapter_key: AdapterEvidence}`` used to populate the gate rows'
    ``evidence_fields``.

    * With ``removal_bindings``: REUSES :func:`bind_and_validate_removals` and
      passes ``dependency_audit_evidence=c_evidence`` — that is the sanctioned
      C->B feed; the binder performs the merge that drives the verdict.
    * Else, with ``adapter_evidence``: merges C's ``dependency_audit_passed`` into
      each adapter's evidence and REUSES :func:`aggregate_adapter_readiness`.
    """
    if removal_bindings is not None:
        bindings = list(removal_bindings)
        report = bind_and_validate_removals(
            bindings,
            dependency_audit_evidence=c_evidence,
            inventory=inventory,
        )
        display = {
            b.adapter_key: _merge_dependency_audit(
                b.evidence or AdapterEvidence(), b.adapter_key, c_evidence
            )
            for b in bindings
        }
        return report, display
    if adapter_evidence is not None:
        merged = {
            key: _merge_dependency_audit(value, key, c_evidence)
            for key, value in adapter_evidence.items()
        }
        report = aggregate_adapter_readiness(merged, inventory=inventory)
        return report, dict(merged)
    raise ValueError(
        "FCC-07B build path requires either removal_bindings or adapter_evidence "
        "(or inject a pre-built readiness/binding report via 'readiness')."
    )


# --------------------------------------------------------------------------- #
# The orchestrator.
# --------------------------------------------------------------------------- #
def orchestrate_final_clean_core(
    *,
    mode: RunnerMode,
    # ---- FCC-07A (matrix) ----
    conformance_matrix: ConformanceMatrixReport
    | Callable[[], ConformanceMatrixReport]
    | None = None,
    # ---- FCC-07C (packaging ownership + dependency_audit projection) ----
    packaging_ownership: PackagingOwnershipReport
    | Callable[[], PackagingOwnershipReport]
    | None = None,
    dependency_audit: DependencyAuditProjection
    | Callable[[], DependencyAuditProjection]
    | None = None,
    # ---- FCC-07B (readiness / removal binding) ----
    readiness: BReport | Callable[[], BReport] | None = None,
    removal_bindings: Iterable[RemovalEvidenceBinding] | None = None,
    adapter_evidence: Mapping[str, AdapterEvidence] | None = None,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
    # ---- FCC-07D (runtime composition provider guard) ----
    provider_guard: RuntimeCompositionGuardReport
    | Callable[[], RuntimeCompositionGuardReport]
    | None = None,
    runtime_composition: RuntimeComposition | None = None,
    provider_observations: Iterable[RuntimeProviderObservation] | None = None,
    provider_context: ProviderContext = "production",
    # ---- E shell knobs ----
    command_results: Iterable[CommandInput] = (),
    required_gate_ids: Iterable[str] | None = None,
    spec_ids: Mapping[str, str] | None = None,
    # ---- real-builder inputs (only consulted when nothing is injected) ----
    use_real_builders: bool = False,
    community_pyproject_path: Path | None = None,
) -> RunnerReport:
    """Orchestrate the FCC-07A/B/C/D reports into one deterministic ``RunnerReport``.

    Each gate may be supplied THREE ways (in precedence order): an already-built
    report (or zero-arg callable returning one), a set of typed INPUTS the
    orchestrator builds the report from (e.g. ``removal_bindings`` for B, a
    ``runtime_composition`` for D), or — when ``use_real_builders`` is set and the
    gate has no injected source — the real builder. A gate with no resolvable
    source is simply not mapped; use ``required_gate_ids`` to force the shell's
    fail-closed presence check.

    The C->B feed (``dependency_audit_passed``) is threaded automatically: FCC-07C
    is resolved before FCC-07B and its
    :meth:`DependencyAuditProjection.as_adapter_evidence` projection is passed as
    the ``dependency_audit_evidence`` of :func:`bind_and_validate_removals`.

    Returns the ``RunnerReport`` from the reused E-IMP1 shell
    (:func:`run_final_clean_core`); ``mode`` is recorded and validated there.
    """
    spec_ids = {**DEFAULT_SPEC_IDS, **(spec_ids or {})}
    gates: list[RunnerGateResult] = []

    # ---- FCC-07A ---------------------------------------------------------- #
    a_report = _resolve_a(conformance_matrix, use_real_builders)
    if a_report is not None:
        gates.extend(
            map_conformance_matrix(
                a_report,
                spec_id=spec_ids[GATE_FCC07A],
                owner=DEFAULT_OWNERS[GATE_FCC07A],
            )
        )

    # ---- FCC-07C (resolved BEFORE B so its projection can FEED B) --------- #
    c_report, c_projection = _resolve_c(
        packaging_ownership,
        dependency_audit,
        use_real_builders=use_real_builders,
        community_pyproject_path=community_pyproject_path,
        inventory=inventory,
    )
    if c_report is not None:
        gates.extend(
            map_packaging_ownership(
                c_report,
                spec_id=spec_ids[GATE_FCC07C],
                owner=DEFAULT_OWNERS[GATE_FCC07C],
            )
        )

    # ---- FCC-07B (CONSUMES C's dependency_audit projection) --------------- #
    b_report, b_evidence = _resolve_b(
        readiness=readiness,
        removal_bindings=removal_bindings,
        adapter_evidence=adapter_evidence,
        inventory=inventory,
        c_projection=c_projection,
    )
    if b_report is not None:
        gates.extend(
            map_readiness(
                b_report,
                spec_id=spec_ids[GATE_FCC07B],
                owner=DEFAULT_OWNERS[GATE_FCC07B],
                evidence_by_key=b_evidence,
            )
        )

    # ---- FCC-07D ---------------------------------------------------------- #
    d_report = _resolve_d(
        provider_guard,
        runtime_composition=runtime_composition,
        provider_observations=provider_observations,
        provider_context=provider_context,
    )
    if d_report is not None:
        gates.extend(
            map_provider_guard(
                d_report,
                spec_id=spec_ids[GATE_FCC07D],
                owner=DEFAULT_OWNERS[GATE_FCC07D],
            )
        )

    # ---- E shell (reused verbatim — NOT reimplemented) -------------------- #
    return run_final_clean_core(
        mode=mode,
        gate_results=gates,
        command_results=command_results,
        required_gate_ids=required_gate_ids,
    )


# --------------------------------------------------------------------------- #
# Per-gate source resolution (injected result / callable / typed inputs / real).
# --------------------------------------------------------------------------- #
def _resolve_a(
    conformance_matrix: ConformanceMatrixReport
    | Callable[[], ConformanceMatrixReport]
    | None,
    use_real_builders: bool,
) -> ConformanceMatrixReport | None:
    if conformance_matrix is not None:
        return _resolve(conformance_matrix)  # type: ignore[return-value]
    if use_real_builders:
        return build_conformance_matrix()
    return None


def _resolve_c(
    packaging_ownership: PackagingOwnershipReport
    | Callable[[], PackagingOwnershipReport]
    | None,
    dependency_audit: DependencyAuditProjection
    | Callable[[], DependencyAuditProjection]
    | None,
    *,
    use_real_builders: bool,
    community_pyproject_path: Path | None,
    inventory: tuple[AdapterInventoryEntry, ...] | None,
) -> tuple[PackagingOwnershipReport | None, DependencyAuditProjection | None]:
    c_report: PackagingOwnershipReport | None = (
        _resolve(packaging_ownership)  # type: ignore[assignment]
        if packaging_ownership is not None
        else None
    )
    c_projection: DependencyAuditProjection | None = (
        _resolve(dependency_audit)  # type: ignore[assignment]
        if dependency_audit is not None
        else None
    )
    if not use_real_builders:
        return c_report, c_projection
    # Real builders fill whatever was not injected.
    if c_report is None:
        c_report = PackagingOwnershipGate().run(PackagingOwnershipGateInput())
    if c_projection is None and community_pyproject_path is not None:
        _community_audit, c_projection = run_community_packaging_audit(
            community_pyproject_path=community_pyproject_path,
            ownership_report=c_report,
            inventory=inventory,
        )
    return c_report, c_projection


def _resolve_b(
    *,
    readiness: BReport | Callable[[], BReport] | None,
    removal_bindings: Iterable[RemovalEvidenceBinding] | None,
    adapter_evidence: Mapping[str, AdapterEvidence] | None,
    inventory: tuple[AdapterInventoryEntry, ...] | None,
    c_projection: DependencyAuditProjection | None,
) -> tuple[BReport | None, dict[str, AdapterEvidence]]:
    if readiness is not None:
        # A pre-built report: the C->B feed is assumed to have happened upstream;
        # the supplied adapter_evidence (if any) populates evidence_fields.
        return _resolve(readiness), dict(adapter_evidence or {})  # type: ignore[return-value]
    if removal_bindings is not None or adapter_evidence is not None:
        return _build_b_report(
            c_evidence=_c_evidence(c_projection),
            removal_bindings=removal_bindings,
            adapter_evidence=adapter_evidence,
            inventory=inventory,
        )
    return None, {}


def _resolve_d(
    provider_guard: RuntimeCompositionGuardReport
    | Callable[[], RuntimeCompositionGuardReport]
    | None,
    *,
    runtime_composition: RuntimeComposition | None,
    provider_observations: Iterable[RuntimeProviderObservation] | None,
    provider_context: ProviderContext,
) -> RuntimeCompositionGuardReport | None:
    if provider_guard is not None:
        return _resolve(provider_guard)  # type: ignore[return-value]
    if provider_observations is not None:
        return validate_injected_providers(
            provider_observations, context=provider_context
        )
    if runtime_composition is not None:
        return validate_runtime_composition_providers(
            runtime_composition, context=provider_context
        )
    return None


__all__ = [
    "GATE_FCC07A",
    "GATE_FCC07B",
    "GATE_FCC07C",
    "GATE_FCC07D",
    "DEFAULT_SPEC_IDS",
    "DEFAULT_OWNERS",
    "READINESS_STATUS_TO_GATE",
    "BReport",
    "map_conformance_matrix",
    "map_readiness",
    "map_packaging_ownership",
    "map_provider_guard",
    "orchestrate_final_clean_core",
]
