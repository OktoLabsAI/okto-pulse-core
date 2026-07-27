"""FCC-07B-IMP1 — canonical readiness AGGREGATOR over ``AdapterEvidence``.

This module is an ADDITIVE, deterministic, synchronous projection that joins the
canonical adapter inventory (:func:`build_adapter_inventory`) with externally
supplied :class:`AdapterEvidence` (one per ``adapter_key``) and emits a single,
machine-readable readiness report — one row per inventory adapter.

It is the AGGREGATOR layer of FCC-07B. It NEVER re-implements the readiness
semantics: every per-adapter verdict is produced VERBATIM by
:func:`evaluate_adapter_readiness` (FR2). :class:`AdapterEvidence` is the ONE
canonical evidence DTO (FR1) — parallel / look-alike evidence DTOs are rejected
at the input boundary.

Evidence for an adapter may originate in either the core OR the Community edition
(e.g. a Community Onda-A dependency audit). **The core MUST NOT import**
``okto_pulse.community``; Community evidence therefore enters ONLY as external
input (a plain ``{adapter_key: AdapterEvidence}`` mapping / fixture), never via an
import of any Community function.

Scope — FCC-07B-IMP1 is the AGGREGATOR only:

* Derivable here (from verdict + inventory): ``adapter_key``, ``status``,
  ``reasons``, ``missing_evidence``, ``failed_evidence``, ``owning_fcc``
  (<- ``entry.wave``), ``oracles_required`` (<- ``entry.oracles_required``),
  ``next_specs`` (<- ``verdict.next_specs``), ``evidence_field_impact``
  (deterministic map of each missing/failed ``REQUIRED_EVIDENCE`` field ->
  impact) and ``remediation`` (<- ``entry.removal_criterion``).
* Binding-only placeholders — filled by FCC-07B-IMP2's ``RemovalEvidenceBinding``:
  ``source_module``, ``source_test_or_oracle`` and ``declared_removal_ref`` stay
  ``None`` here, because they describe a *declared removal* and the test/oracle
  that proves it — provenance the aggregator cannot derive from the inventory.

Explicitly OUT of scope (FCC-07B-IMP2, do NOT implement here): the
``RemovalEvidenceBinding`` contract (FR3), fail-closed handling of
unknown/duplicate ``adapter_key`` and removal-without-evidence (FR4), and the
formal downstream consumer (FR6). Unknown evidence keys are therefore TOLERATED
(ignored), never rejected, in this layer.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .adapter_readiness_inventory import (
    REQUIRED_EVIDENCE,
    AdapterEvidence,
    AdapterInventoryEntry,
    AdapterReadinessVerdict,
    AdapterStatus,
    build_adapter_inventory,
    evaluate_adapter_readiness,
)

#: Stable per-field impact codes (deterministic; derived from the verdict only).
GATEWAY_IMPACT = "deferral_gateway:port_not_closed"
MISSING_IMPACT = "blocks_ready:missing_evidence"
FAILED_IMPACT = "blocks_ready:failed_evidence"


@dataclass(frozen=True)
class ReadinessAggregateRow:
    """One deterministic readiness row for a single adapter.

    ``missing_evidence`` / ``failed_evidence`` / ``oracles_required`` /
    ``next_specs`` / ``reasons`` / ``evidence_field_impact`` are ALWAYS tuples —
    empty when not applicable, NEVER ``None``.
    """

    adapter_key: str
    status: AdapterStatus
    reasons: tuple[str, ...]
    missing_evidence: tuple[str, ...]
    failed_evidence: tuple[str, ...]
    owning_fcc: str | None
    oracles_required: tuple[str, ...]
    next_specs: tuple[str, ...]
    #: Ordered (field, impact_code) pairs for every missing/failed REQUIRED_EVIDENCE
    #: field, in canonical ``REQUIRED_EVIDENCE`` order. Empty when ``ready``.
    evidence_field_impact: tuple[tuple[str, str], ...]
    remediation: str | None
    # --- binding-only placeholders (FCC-07B-IMP2 RemovalEvidenceBinding fills) ---
    source_module: str | None = None
    source_test_or_oracle: str | None = None
    declared_removal_ref: str | None = None

    def as_dict(self) -> dict[str, object]:
        """Machine-readable projection (FR5). Every FR5 field is present; tuple
        fields project to lists and ``evidence_field_impact`` to an ordered map."""
        return {
            "adapter_key": self.adapter_key,
            "status": self.status,
            "reasons": list(self.reasons),
            "missing_evidence": list(self.missing_evidence),
            "failed_evidence": list(self.failed_evidence),
            "owning_fcc": self.owning_fcc,
            "oracles_required": list(self.oracles_required),
            "next_specs": list(self.next_specs),
            "evidence_field_impact": {
                field: impact for field, impact in self.evidence_field_impact
            },
            "remediation": self.remediation,
            "source_module": self.source_module,
            "source_test_or_oracle": self.source_test_or_oracle,
            "declared_removal_ref": self.declared_removal_ref,
        }


@dataclass(frozen=True)
class ReadinessAggregateReport:
    """Deterministic, JSON-serialisable aggregate report (rows sorted by
    ``adapter_key``)."""

    rows: tuple[ReadinessAggregateRow, ...]

    @property
    def status_counts(self) -> dict[str, int]:
        counts = {"ready": 0, "blocked": 0, "deferred": 0}
        for row in self.rows:
            counts[row.status] += 1
        return counts

    def row_for(self, adapter_key: str) -> ReadinessAggregateRow | None:
        for row in self.rows:
            if row.adapter_key == adapter_key:
                return row
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "status_counts": self.status_counts,
            "rows": [row.as_dict() for row in self.rows],
        }


def _evidence_field_impact(
    verdict: AdapterReadinessVerdict,
) -> tuple[tuple[str, str], ...]:
    """Map each missing/failed ``REQUIRED_EVIDENCE`` field to a stable impact code.

    Deterministic and derived ONLY from the verdict: iterated in canonical
    ``REQUIRED_EVIDENCE`` order so the result is reproducible. ``port_closed`` is
    the gateway field — when it is unsatisfied the adapter is deferred; every
    other unsatisfied field blocks readiness.
    """
    missing = set(verdict.missing_evidence)
    failed = set(verdict.failed_evidence)
    impacts: list[tuple[str, str]] = []
    for field in REQUIRED_EVIDENCE:
        unsatisfied = field in missing or field in failed
        if not unsatisfied:
            continue
        if field == "port_closed":
            impacts.append((field, GATEWAY_IMPACT))
        elif field in failed:
            impacts.append((field, FAILED_IMPACT))
        else:
            impacts.append((field, MISSING_IMPACT))
    return tuple(impacts)


def _validate_evidence_mapping(evidence_by_key: Mapping[str, AdapterEvidence]) -> None:
    """FR1: ``AdapterEvidence`` is the ONE canonical evidence DTO. A parallel /
    look-alike DTO (even with identical field names) is rejected here so it can
    never silently masquerade as evidence."""
    if not isinstance(evidence_by_key, Mapping):
        raise TypeError(
            "readiness aggregator expects a Mapping[str, AdapterEvidence]; got "
            f"{type(evidence_by_key).__name__!r}"
        )
    for key, value in evidence_by_key.items():
        if not isinstance(key, str):
            raise TypeError(
                "evidence keys must be adapter_key strings; got "
                f"{type(key).__name__!r}"
            )
        if not isinstance(value, AdapterEvidence):
            raise TypeError(
                "readiness aggregator only accepts AdapterEvidence as the canonical "
                f"evidence DTO; got {type(value).__name__!r} for adapter_key "
                f"{key!r}. Parallel/look-alike evidence DTOs are rejected "
                "(FCC-07B FR1)."
            )


def _row_for_entry(
    entry: AdapterInventoryEntry,
    evidence: AdapterEvidence,
) -> ReadinessAggregateRow:
    # FR2: the verdict is produced VERBATIM by the canonical evaluator. The
    # aggregator NEVER re-derives status/missing/failed/next_specs.
    verdict = evaluate_adapter_readiness(entry, evidence)
    return ReadinessAggregateRow(
        adapter_key=entry.adapter_key,
        status=verdict.status,
        reasons=verdict.reasons,
        missing_evidence=verdict.missing_evidence,
        failed_evidence=verdict.failed_evidence,
        owning_fcc=entry.wave or None,
        oracles_required=entry.oracles_required,
        next_specs=verdict.next_specs,
        evidence_field_impact=_evidence_field_impact(verdict),
        remediation=entry.removal_criterion or None,
        # binding-only fields stay None — FCC-07B-IMP2 fills them.
        source_module=None,
        source_test_or_oracle=None,
        declared_removal_ref=None,
    )


def aggregate_adapter_readiness(
    evidence_by_key: Mapping[str, AdapterEvidence],
    *,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
) -> ReadinessAggregateReport:
    """Aggregate adapter readiness from externally supplied evidence.

    Joins the canonical adapter inventory with ``evidence_by_key`` and emits one
    deterministic :class:`ReadinessAggregateRow` per inventory entry, sorted by
    ``adapter_key``. Adapters without supplied evidence default to an empty
    :class:`AdapterEvidence` (-> deferred, the port boundary is still open).

    ``evidence_by_key`` may carry evidence sourced from BOTH the core and the
    Community edition; it is plain external input (no Community import). Unknown
    evidence keys are tolerated/ignored here (the unknown/duplicate fail-closed
    contract is FCC-07B-IMP2).
    """
    _validate_evidence_mapping(evidence_by_key)
    entries = inventory if inventory is not None else build_adapter_inventory()
    rows = tuple(
        _row_for_entry(
            entry,
            evidence_by_key.get(entry.adapter_key, AdapterEvidence()),
        )
        for entry in sorted(entries, key=lambda e: e.adapter_key)
    )
    return ReadinessAggregateReport(rows=rows)


@dataclass(frozen=True)
class ReadinessAggregator:
    """Thin, reusable handle binding an (optional) inventory to the aggregator
    function. Stateless — :meth:`aggregate` delegates to
    :func:`aggregate_adapter_readiness`."""

    inventory: tuple[AdapterInventoryEntry, ...] | None = None

    def aggregate(
        self, evidence_by_key: Mapping[str, AdapterEvidence]
    ) -> ReadinessAggregateReport:
        return aggregate_adapter_readiness(evidence_by_key, inventory=self.inventory)


__all__ = [
    "GATEWAY_IMPACT",
    "MISSING_IMPACT",
    "FAILED_IMPACT",
    "ReadinessAggregateRow",
    "ReadinessAggregateReport",
    "ReadinessAggregator",
    "aggregate_adapter_readiness",
]
