"""FCC-07B-IMP2 — ``RemovalEvidenceBinding`` contract + fail-closed binding gate.

This is the BINDING layer ABOVE the FCC-07B-IMP1 aggregator
(:mod:`readiness_aggregator`). IMP1 joins the canonical adapter inventory with
externally supplied :class:`AdapterEvidence` and emits one deterministic
readiness row per inventory adapter — but it leaves three provenance fields
(``source_module`` / ``source_test_or_oracle`` / ``declared_removal_ref``)
``None`` and TOLERATES unknown/duplicate evidence keys. IMP2 closes that gap:

* It defines the structured :class:`RemovalEvidenceBinding` (TR6) — the explicit,
  per-declared-removal record that ties an ``adapter_key`` to its
  :class:`AdapterEvidence`, the module being removed (``source_module``), the
  test/oracle that proves the removal safe (``source_test_or_oracle``), the
  declared removal reference (``declared_removal_ref``), the owning FCC, the
  required oracles, a static per-field impact annotation and the remediation.
* :func:`bind_and_validate_removals` validates the binding SET FAIL-CLOSED (FR3/
  FR4/TR5) and, for the valid bindings, REUSES
  :func:`aggregate_adapter_readiness` (it NEVER re-implements the readiness
  verdict) and ATTACHES the binding provenance — filling the three placeholder
  fields IMP1 leaves ``None`` (FR5).

``dependency_audit_passed`` is an INPUT sourced from FCC-07C
(:func:`...community_packaging_audit.project_dependency_audit_evidence`): the
caller passes the ``{adapter_key: AdapterEvidence}`` projection in as
``dependency_audit_evidence`` and this layer MERGES that single field into each
binding's evidence. B NEVER recalculates the packaging/dependency audit.

Fail-closed contract (TR5) — every diagnostic carries ``adapter_key`` + origin +
corrective action (FR4):

* ``unknown_adapter_key`` — a binding references an ``adapter_key`` absent from
  :func:`build_adapter_inventory` / ``REQUIRED_ADAPTER_KEYS``. STRUCTURAL: the
  whole run refuses to mark ANY adapter ``ready`` (``ready_suppressed``). (AC4)
* ``duplicate_binding`` — two+ bindings for the same ``adapter_key``. STRUCTURAL
  (same ready-suppression); no row is emitted for the ambiguous key.
* ``removal_without_evidence`` — a binding declares a removal
  (``declared_removal_ref``) but carries no :class:`AdapterEvidence`: a
  ``blocked`` "missing binding" row with the owning FCC + remediation. (AC5)
* ``evidence_without_removal`` — a binding carries evidence but declares no
  removal (``declared_removal_ref is None``): that adapter is forced
  ``blocked`` (never ``ready`` from unbound evidence).

The core NEVER imports ``okto_pulse.community`` — FCC-07C evidence enters ONLY as
a plain ``{adapter_key: AdapterEvidence}`` mapping. Deterministic: rows are
sorted by ``adapter_key``, diagnostics by ``(adapter_key, code)``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace

from .adapter_readiness_inventory import (
    REQUIRED_ADAPTER_KEYS,
    REQUIRED_EVIDENCE,
    AdapterEvidence,
    AdapterInventoryEntry,
    AdapterStatus,
    build_adapter_inventory,
)
from .readiness_aggregator import (
    GATEWAY_IMPACT,
    MISSING_IMPACT,
    ReadinessAggregateRow,
    aggregate_adapter_readiness,
)

# --------------------------------------------------------------------------- #
# Stable diagnostic codes (fail-closed contract referenced by the tests).
# --------------------------------------------------------------------------- #
DIAG_UNKNOWN_ADAPTER_KEY = "unknown_adapter_key"
DIAG_DUPLICATE_BINDING = "duplicate_binding"
DIAG_REMOVAL_WITHOUT_EVIDENCE = "removal_without_evidence"
DIAG_EVIDENCE_WITHOUT_REMOVAL = "evidence_without_removal"

#: Reason appended to a row whose ``ready`` verdict is refused because the binding
#: SET is structurally invalid (unknown/duplicate present anywhere).
REASON_BINDING_SET_INVALID = "fail_closed:binding_set_invalid"
#: Reason appended to a row whose evidence is not bound to a declared removal.
REASON_EVIDENCE_WITHOUT_REMOVAL = "fail_closed:evidence_without_declared_removal"
#: Status/reason for a declared removal that carries no evidence (missing binding).
REASON_MISSING_BINDING_EVIDENCE = "missing_binding_evidence"

#: Static, evidence-INDEPENDENT role each ``REQUIRED_EVIDENCE`` field plays in a
#: declared removal. This is the binding's ``evidence_field_impact`` annotation —
#: distinct from the IMP1 row's ``evidence_field_impact`` (which lists only the
#: fields actually unsatisfied for the supplied evidence). ``dependency_audit_passed``
#: is explicitly flagged as sourced from FCC-07C (B never recomputes it).
_FIELD_ROLE: dict[str, str] = {
    "port_closed": "gateway:predecessor_port_must_close",
    "community_registered": "blocks_removal:community_registration_required",
    "oracle_passed": "blocks_removal:behavioral_oracle_required",
    "import_audit_passed": "blocks_removal:no_residual_import",
    "dependency_audit_passed": "blocks_removal:sourced_from_fcc07c",
    "register_before_remove_passed": "blocks_removal:register_before_remove",
}


def _declared_field_impact() -> tuple[tuple[str, str], ...]:
    """The static per-field impact annotation (canonical ``REQUIRED_EVIDENCE`` order)."""
    return tuple((field, _FIELD_ROLE[field]) for field in REQUIRED_EVIDENCE)


def _all_unsatisfied_impact() -> tuple[tuple[str, str], ...]:
    """Impact map for a row with NO evidence at all (every field unsatisfied),
    reusing the IMP1 public impact codes for consistency."""
    return tuple(
        (field, GATEWAY_IMPACT if field == "port_closed" else MISSING_IMPACT)
        for field in REQUIRED_EVIDENCE
    )


# =========================================================================== #
# TR6 — the RemovalEvidenceBinding contract (EXACTLY nine fields).
# =========================================================================== #
@dataclass(frozen=True)
class RemovalEvidenceBinding:
    """Explicit binding of one declared adapter removal to its evidence (TR6/FR3).

    Field provenance:

    * ``adapter_key`` / ``evidence`` / ``declared_removal_ref`` /
      ``source_module`` / ``source_test_or_oracle`` are DECLARED by the binding
      author. ``evidence`` may be ``None`` — that is the fail-closed
      "declared removal without evidence" case (AC5).
    * ``owning_fcc`` / ``oracles_required`` / ``remediation`` are derivable from
      the inventory entry (see :meth:`for_adapter`).
    * ``evidence_field_impact`` is the static per-field role annotation
      (:func:`_declared_field_impact`).
    * ``evidence.dependency_audit_passed`` is an INPUT sourced from FCC-07C — it
      is merged in by :func:`bind_and_validate_removals`, never recomputed here.
    """

    adapter_key: str
    evidence: AdapterEvidence | None = None
    declared_removal_ref: str | None = None
    owning_fcc: str | None = None
    source_module: str | None = None
    source_test_or_oracle: str | None = None
    oracles_required: tuple[str, ...] = ()
    evidence_field_impact: tuple[tuple[str, str], ...] = ()
    remediation: str | None = None

    @classmethod
    def for_adapter(
        cls,
        entry: AdapterInventoryEntry,
        *,
        evidence: AdapterEvidence | None,
        declared_removal_ref: str | None,
        source_module: str | None,
        source_test_or_oracle: str | None,
    ) -> RemovalEvidenceBinding:
        """Build a binding for a KNOWN inventory ``entry``, deriving the
        inventory-sourced fields (``owning_fcc`` <- ``entry.wave``,
        ``oracles_required`` <- ``entry.oracles_required``, ``remediation`` <-
        ``entry.removal_criterion``) and the static ``evidence_field_impact``."""
        return cls(
            adapter_key=entry.adapter_key,
            evidence=evidence,
            declared_removal_ref=declared_removal_ref,
            owning_fcc=entry.wave or None,
            source_module=source_module,
            source_test_or_oracle=source_test_or_oracle,
            oracles_required=entry.oracles_required,
            evidence_field_impact=_declared_field_impact(),
            remediation=entry.removal_criterion or None,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "adapter_key": self.adapter_key,
            "evidence": self.evidence.as_map() if self.evidence is not None else None,
            "declared_removal_ref": self.declared_removal_ref,
            "owning_fcc": self.owning_fcc,
            "source_module": self.source_module,
            "source_test_or_oracle": self.source_test_or_oracle,
            "oracles_required": list(self.oracles_required),
            "evidence_field_impact": {
                field: impact for field, impact in self.evidence_field_impact
            },
            "remediation": self.remediation,
        }


# =========================================================================== #
# Structured fail-closed diagnostic (FR4 — adapter_key + origin + corrective).
# =========================================================================== #
@dataclass(frozen=True)
class BindingDiagnostic:
    """One fail-closed binding violation. ``blocking`` is always ``True`` here —
    every fail-closed condition refuses readiness for its subject."""

    code: str
    adapter_key: str
    origin: str
    message: str
    owning_fcc: str | None = None
    declared_removal_ref: str | None = None
    remediation: str | None = None
    blocking: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "adapter_key": self.adapter_key,
            "origin": self.origin,
            "message": self.message,
            "owning_fcc": self.owning_fcc,
            "declared_removal_ref": self.declared_removal_ref,
            "remediation": self.remediation,
            "blocking": self.blocking,
        }


@dataclass(frozen=True)
class RemovalBindingReport:
    """Deterministic report of the binding gate (rows sorted by ``adapter_key``).

    ``rows`` covers ONLY the BOUND adapters (one per declared removal), with the
    three IMP1 placeholder fields filled from the binding. ``ok`` is ``True``
    only when there is NO fail-closed diagnostic. ``ready_suppressed`` is ``True``
    when a structural error (unknown/duplicate) forced a would-be-``ready`` row
    down to ``blocked``.
    """

    ok: bool
    status: AdapterStatus
    rows: tuple[ReadinessAggregateRow, ...]
    diagnostics: tuple[BindingDiagnostic, ...]
    ready_suppressed: bool

    @property
    def blocking(self) -> tuple[BindingDiagnostic, ...]:
        return tuple(d for d in self.diagnostics if d.blocking)

    def row_for(self, adapter_key: str) -> ReadinessAggregateRow | None:
        for row in self.rows:
            if row.adapter_key == adapter_key:
                return row
        return None

    def diagnostics_for(self, adapter_key: str) -> tuple[BindingDiagnostic, ...]:
        return tuple(d for d in self.diagnostics if d.adapter_key == adapter_key)

    @property
    def status_counts(self) -> dict[str, int]:
        counts = {"ready": 0, "blocked": 0, "deferred": 0}
        for row in self.rows:
            counts[row.status] += 1
        return counts

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "status": self.status,
            "ready_suppressed": self.ready_suppressed,
            "status_counts": self.status_counts,
            "rows": [row.as_dict() for row in self.rows],
            "diagnostics": [d.as_dict() for d in self.diagnostics],
        }


# --------------------------------------------------------------------------- #
# Input validation (binding layer counterpart of FR1).
# --------------------------------------------------------------------------- #
def _validate_bindings(bindings: list[RemovalEvidenceBinding]) -> None:
    for value in bindings:
        if not isinstance(value, RemovalEvidenceBinding):
            raise TypeError(
                "bind_and_validate_removals only accepts RemovalEvidenceBinding; "
                f"got {type(value).__name__!r}. Parallel/look-alike binding DTOs "
                "are rejected (FCC-07B-IMP2 TR6)."
            )
        if value.evidence is not None and not isinstance(
            value.evidence, AdapterEvidence
        ):
            raise TypeError(
                "RemovalEvidenceBinding.evidence must be AdapterEvidence | None "
                f"(the canonical evidence DTO); got {type(value.evidence).__name__!r} "
                f"for adapter_key {value.adapter_key!r}."
            )


def _validate_dependency_audit_evidence(
    evidence: Mapping[str, AdapterEvidence],
) -> None:
    if not isinstance(evidence, Mapping):
        raise TypeError(
            "dependency_audit_evidence must be a Mapping[str, AdapterEvidence] "
            f"(the FCC-07C projection); got {type(evidence).__name__!r}."
        )
    for key, value in evidence.items():
        if not isinstance(key, str) or not isinstance(value, AdapterEvidence):
            raise TypeError(
                "dependency_audit_evidence must map adapter_key strings to "
                f"AdapterEvidence; got {type(key).__name__!r} -> "
                f"{type(value).__name__!r}."
            )


# --------------------------------------------------------------------------- #
# Diagnostic builders.
# --------------------------------------------------------------------------- #
def _unknown_diag(binding: RemovalEvidenceBinding) -> BindingDiagnostic:
    return BindingDiagnostic(
        code=DIAG_UNKNOWN_ADAPTER_KEY,
        adapter_key=binding.adapter_key,
        origin="declared_binding",
        message=(
            f"binding declares adapter_key {binding.adapter_key!r} which is not in "
            "build_adapter_inventory()/REQUIRED_ADAPTER_KEYS; no adapter is marked "
            "ready while the binding set is structurally invalid."
        ),
        owning_fcc=binding.owning_fcc,
        declared_removal_ref=binding.declared_removal_ref,
        remediation=(
            f"Remove the binding for {binding.adapter_key!r}, or add the adapter to "
            "build_adapter_inventory() (REQUIRED_ADAPTER_KEYS) before binding "
            "removal evidence to it."
        ),
    )


def _unknown_c_evidence_diag(adapter_key: str) -> BindingDiagnostic:
    """AC4: an unknown adapter_key supplied by the FCC-07C evidence projection.

    Same structural failure as an unknown BINDING key — the evidence references an
    adapter that does not exist in ``build_adapter_inventory()``, so no adapter may
    be marked ready while the evidence set is structurally invalid.
    """
    return BindingDiagnostic(
        code=DIAG_UNKNOWN_ADAPTER_KEY,
        adapter_key=adapter_key,
        origin="dependency_audit_evidence",
        message=(
            f"FCC-07C dependency_audit_evidence supplies adapter_key {adapter_key!r} "
            "which is not in build_adapter_inventory()/REQUIRED_ADAPTER_KEYS; no "
            "adapter is marked ready while the evidence set is structurally invalid."
        ),
        remediation=(
            f"Remove the {adapter_key!r} entry from the FCC-07C "
            "dependency_audit_evidence projection, or add the adapter to "
            "build_adapter_inventory() (REQUIRED_ADAPTER_KEYS)."
        ),
    )


def _duplicate_diag(
    adapter_key: str, entry: AdapterInventoryEntry | None, count: int
) -> BindingDiagnostic:
    return BindingDiagnostic(
        code=DIAG_DUPLICATE_BINDING,
        adapter_key=adapter_key,
        origin="declared_binding",
        message=(
            f"{count} removal-evidence bindings declared for adapter_key "
            f"{adapter_key!r}; exactly one binding per adapter_key is allowed. The "
            "ambiguous adapter is not marked ready."
        ),
        owning_fcc=(entry.wave or None) if entry is not None else None,
        remediation=(
            "Consolidate the duplicate bindings into a single "
            f"RemovalEvidenceBinding for adapter_key {adapter_key!r}."
        ),
    )


def _removal_without_evidence_diag(
    binding: RemovalEvidenceBinding, entry: AdapterInventoryEntry
) -> BindingDiagnostic:
    return BindingDiagnostic(
        code=DIAG_REMOVAL_WITHOUT_EVIDENCE,
        adapter_key=binding.adapter_key,
        origin="declared_binding",
        message=(
            f"binding for adapter_key {binding.adapter_key!r} declares a removal "
            f"(declared_removal_ref={binding.declared_removal_ref!r}) but carries no "
            "AdapterEvidence: missing binding, removal stays blocked."
        ),
        owning_fcc=binding.owning_fcc or (entry.wave or None),
        declared_removal_ref=binding.declared_removal_ref,
        remediation=binding.remediation or (entry.removal_criterion or None),
    )


def _evidence_without_removal_diag(
    binding: RemovalEvidenceBinding, entry: AdapterInventoryEntry
) -> BindingDiagnostic:
    return BindingDiagnostic(
        code=DIAG_EVIDENCE_WITHOUT_REMOVAL,
        adapter_key=binding.adapter_key,
        origin="declared_binding",
        message=(
            f"binding for adapter_key {binding.adapter_key!r} carries evidence but "
            "declares no removal (declared_removal_ref is None); evidence is not "
            "bound to a declared removal and cannot make the adapter ready."
        ),
        owning_fcc=binding.owning_fcc or (entry.wave or None),
        remediation=(
            "Set declared_removal_ref to the FCC removal this evidence proves, or "
            "drop the unbound evidence."
        ),
    )


# --------------------------------------------------------------------------- #
# Row helpers.
# --------------------------------------------------------------------------- #
def _attach_binding_fields(
    row: ReadinessAggregateRow, binding: RemovalEvidenceBinding
) -> ReadinessAggregateRow:
    """Fill the three IMP1 placeholder fields from the binding (FR5)."""
    return replace(
        row,
        source_module=binding.source_module,
        source_test_or_oracle=binding.source_test_or_oracle,
        declared_removal_ref=binding.declared_removal_ref,
    )


def _force_blocked(
    row: ReadinessAggregateRow, reason: str
) -> ReadinessAggregateRow:
    reasons = row.reasons + (reason,)
    if row.status == "ready":
        return replace(row, status="blocked", reasons=reasons)
    return replace(row, reasons=reasons)


def _missing_binding_row(
    binding: RemovalEvidenceBinding, entry: AdapterInventoryEntry
) -> ReadinessAggregateRow:
    """A blocked "missing binding" row for a declared removal with no evidence (AC5)."""
    return ReadinessAggregateRow(
        adapter_key=binding.adapter_key,
        status="blocked",
        reasons=(REASON_MISSING_BINDING_EVIDENCE,),
        missing_evidence=REQUIRED_EVIDENCE,
        failed_evidence=(),
        owning_fcc=binding.owning_fcc or (entry.wave or None),
        oracles_required=binding.oracles_required or entry.oracles_required,
        next_specs=(entry.wave,) if entry.wave else (),
        evidence_field_impact=_all_unsatisfied_impact(),
        remediation=binding.remediation or (entry.removal_criterion or None),
        source_module=binding.source_module,
        source_test_or_oracle=binding.source_test_or_oracle,
        declared_removal_ref=binding.declared_removal_ref,
    )


def _merge_dependency_audit(
    evidence: AdapterEvidence,
    adapter_key: str,
    dependency_audit_evidence: Mapping[str, AdapterEvidence],
) -> AdapterEvidence:
    """Merge FCC-07C's ``dependency_audit_passed`` INPUT into the binding evidence.

    FCC-07C is the SINGLE authorized source of ``dependency_audit_passed``; when
    it supplies a value for this ``adapter_key`` it OVERRIDES whatever the binding
    declared. B never recomputes the packaging/dependency audit.
    """
    c_evidence = dependency_audit_evidence.get(adapter_key)
    if c_evidence is None:
        return evidence
    return replace(
        evidence, dependency_audit_passed=c_evidence.dependency_audit_passed
    )


# =========================================================================== #
# The fail-closed binding gate.
# =========================================================================== #
def bind_and_validate_removals(
    bindings: Iterable[RemovalEvidenceBinding],
    *,
    dependency_audit_evidence: Mapping[str, AdapterEvidence] | None = None,
    inventory: tuple[AdapterInventoryEntry, ...] | None = None,
) -> RemovalBindingReport:
    """Validate a set of removal-evidence bindings FAIL-CLOSED and project the
    bound readiness rows.

    For every VALID binding the readiness verdict is produced by
    :func:`aggregate_adapter_readiness` (REUSED verbatim — IMP2 never
    re-implements the readiness semantics) and the binding provenance is attached,
    filling the ``source_module`` / ``source_test_or_oracle`` /
    ``declared_removal_ref`` fields IMP1 leaves ``None`` (FR5).

    ``dependency_audit_evidence`` is the FCC-07C ``{adapter_key: AdapterEvidence}``
    projection; its ``dependency_audit_passed`` field is merged into each binding's
    evidence (INPUT, never recomputed).
    """
    entries = inventory if inventory is not None else build_adapter_inventory()
    by_key = {entry.adapter_key: entry for entry in entries}
    known_keys = set(by_key) | set(REQUIRED_ADAPTER_KEYS)

    bindings = list(bindings)
    _validate_bindings(bindings)
    c_evidence: Mapping[str, AdapterEvidence] = dependency_audit_evidence or {}
    _validate_dependency_audit_evidence(c_evidence)

    counts = Counter(b.adapter_key for b in bindings)
    duplicates = {key for key, n in counts.items() if n > 1}

    diagnostics: list[BindingDiagnostic] = []
    structural_error = False
    seen_unknown: set[str] = set()
    seen_duplicate: set[str] = set()

    # AC4: an unknown adapter_key in the FCC-07C dependency_audit_evidence
    # projection is the SAME structural failure as an unknown binding key — the
    # evidence references an adapter outside build_adapter_inventory(). Fail closed
    # so no adapter is marked ready (covers the Community-evidence source, not just
    # the declared bindings).
    for key in sorted(set(c_evidence) - known_keys):
        structural_error = True
        if key not in seen_unknown:
            diagnostics.append(_unknown_c_evidence_diag(key))
            seen_unknown.add(key)

    # adapter_key -> (binding, merged_evidence) for the rows we aggregate via IMP1.
    aggregatable: dict[str, RemovalEvidenceBinding] = {}
    merged_evidence: dict[str, AdapterEvidence] = {}
    force_blocked: set[str] = set()
    missing_evidence: list[RemovalEvidenceBinding] = []

    for binding in bindings:
        key = binding.adapter_key
        if key not in known_keys:
            structural_error = True
            if key not in seen_unknown:
                diagnostics.append(_unknown_diag(binding))
                seen_unknown.add(key)
            continue
        if key in duplicates:
            structural_error = True
            if key not in seen_duplicate:
                diagnostics.append(
                    _duplicate_diag(key, by_key.get(key), counts[key])
                )
                seen_duplicate.add(key)
            continue
        entry = by_key[key]
        if binding.evidence is None:
            # Declared removal (or empty binding) WITHOUT evidence -> missing binding.
            diagnostics.append(_removal_without_evidence_diag(binding, entry))
            missing_evidence.append(binding)
            continue
        if binding.declared_removal_ref is None:
            # Evidence present but not bound to a declared removal -> forced blocked.
            diagnostics.append(_evidence_without_removal_diag(binding, entry))
            force_blocked.add(key)
        aggregatable[key] = binding
        merged_evidence[key] = _merge_dependency_audit(
            binding.evidence, key, c_evidence
        )

    # REUSE the IMP1 aggregator for the valid, evidence-carrying bindings.
    rows: list[ReadinessAggregateRow] = []
    if merged_evidence:
        agg = aggregate_adapter_readiness(merged_evidence, inventory=entries)
        for key in aggregatable:
            base = agg.row_for(key)
            assert base is not None  # key came from the inventory
            row = _attach_binding_fields(base, aggregatable[key])
            if key in force_blocked:
                row = _force_blocked(row, REASON_EVIDENCE_WITHOUT_REMOVAL)
            rows.append(row)

    for binding in missing_evidence:
        rows.append(_missing_binding_row(binding, by_key[binding.adapter_key]))

    # STRUCTURAL fail-closed: an unknown/duplicate ANYWHERE refuses readiness for
    # every adapter in the run (AC4: "no adapter is marked ready").
    ready_suppressed = False
    if structural_error:
        suppressed: list[ReadinessAggregateRow] = []
        for row in rows:
            if row.status == "ready":
                ready_suppressed = True
                row = _force_blocked(row, REASON_BINDING_SET_INVALID)
            suppressed.append(row)
        rows = suppressed

    rows.sort(key=lambda r: r.adapter_key)
    diagnostics.sort(key=lambda d: (d.adapter_key, d.code))

    ok = not diagnostics
    has_blocking = any(d.blocking for d in diagnostics)
    if has_blocking or any(r.status == "blocked" for r in rows):
        status: AdapterStatus = "blocked"
    elif rows and all(r.status == "ready" for r in rows):
        status = "ready"
    else:
        status = "deferred"

    return RemovalBindingReport(
        ok=ok,
        status=status,
        rows=tuple(rows),
        diagnostics=tuple(diagnostics),
        ready_suppressed=ready_suppressed,
    )


@dataclass(frozen=True)
class RemovalEvidenceBinder:
    """Thin, reusable handle binding an (optional) inventory to the gate.
    Stateless — :meth:`bind` delegates to :func:`bind_and_validate_removals`."""

    inventory: tuple[AdapterInventoryEntry, ...] | None = None

    def bind(
        self,
        bindings: Iterable[RemovalEvidenceBinding],
        *,
        dependency_audit_evidence: Mapping[str, AdapterEvidence] | None = None,
    ) -> RemovalBindingReport:
        return bind_and_validate_removals(
            bindings,
            dependency_audit_evidence=dependency_audit_evidence,
            inventory=self.inventory,
        )


__all__ = [
    "DIAG_UNKNOWN_ADAPTER_KEY",
    "DIAG_DUPLICATE_BINDING",
    "DIAG_REMOVAL_WITHOUT_EVIDENCE",
    "DIAG_EVIDENCE_WITHOUT_REMOVAL",
    "REASON_BINDING_SET_INVALID",
    "REASON_EVIDENCE_WITHOUT_REMOVAL",
    "REASON_MISSING_BINDING_EVIDENCE",
    "RemovalEvidenceBinding",
    "BindingDiagnostic",
    "RemovalBindingReport",
    "RemovalEvidenceBinder",
    "bind_and_validate_removals",
]
