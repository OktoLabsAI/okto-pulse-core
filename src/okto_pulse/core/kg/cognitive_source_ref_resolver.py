"""RKG-02 — the shared canonical cognitive source_ref resolver.

This module is the SINGLE authorized entry for interpreting a cognitive
``source_ref`` (BR3 / br_e820e81c). The connectivity guard, canonical-learning
partition, cognitive readiness/read-model and the reprocessing paths must route
through :func:`resolve_cognitive_source_ref` instead of keeping divergent local
parsers.

Contract (FR1 / BR1): every resolution returns a canonical identity AND an
actionable diagnostic — ``source_ref_original``, ``canonical_artifact_ref``,
``base_ref``, ``source_kind``, ``resolution_status`` and ``remediation``.

Type-awareness (FR2 / BR2 / TR1): a plain ``card:<uuid>`` is a Bug alias ONLY
when the card is backed by a *canonical* Bug. The caller injects that knowledge
through ``canonical_bug_probe`` (the connectivity guard probes the existing
canonical Bug nodes; other callers may probe SQL ``Card.card_type == bug``).
When the probe says a ``card:<uuid>`` is NOT a canonical bug, the resolution is
``non_bug_card`` (fail-closed, actionable) and is NEVER coerced into a generic
Bug alias. ``bug:<uuid>`` and ``card:bug:<uuid>`` are always bug-derived and
equivalent. This resolver NEVER fabricates edges or identities to satisfy
connectivity (TR2) — it only reports.
"""

from __future__ import annotations

import uuid as _uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

# Per-concept ref suffixes the spec/bug extractors append.
_CONCEPT_MARKERS: tuple[str, ...] = (":learning:", ":alternative:", ":assumption:", ":decision:")

_KNOWN_TYPES: frozenset[str] = frozenset({
    "bug", "card", "spec", "decision", "refinement", "task", "test", "final_report",
})


class CognitiveSourceKind(str, Enum):
    BUG = "bug"
    CARD = "card"
    SPEC = "spec"
    DECISION = "decision"
    REFINEMENT = "refinement"
    TASK = "task"
    TEST = "test"
    FINAL_REPORT = "final_report"
    CHILD = "child"  # per-concept ref (spec:<id>:alternative:<hash>)
    UNKNOWN = "unknown"


class CognitiveRefResolutionStatus(str, Enum):
    RESOLVED = "resolved"
    # card:<uuid> referenced as bug-derived but the card is NOT a canonical bug.
    NON_BUG_CARD = "non_bug_card"
    FINAL_REPORT_ALLOWLISTED = "final_report_allowlisted"
    INVALID_SOURCE_REF = "invalid_source_ref"
    UNRESOLVED_SOURCE_REF = "unresolved_source_ref"


CanonicalBugProbe = Callable[[str], bool]


@dataclass(frozen=True, slots=True)
class CognitiveRefResolution:
    source_ref_original: str
    canonical_artifact_ref: str
    base_ref: str
    source_kind: str
    resolution_status: str
    is_bug_derived: bool
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_ref_original": self.source_ref_original,
            "canonical_artifact_ref": self.canonical_artifact_ref,
            "base_ref": self.base_ref,
            "source_kind": self.source_kind,
            "resolution_status": self.resolution_status,
            "is_bug_derived": self.is_bug_derived,
            "remediation": self.remediation,
        }


def _is_uuid(value: str) -> bool:
    try:
        _uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def strip_concept_suffix(source_ref: str) -> str:
    """Recover the base artifact ref by removing a per-concept suffix."""
    s = source_ref or ""
    for marker in _CONCEPT_MARKERS:
        idx = s.find(marker)
        if idx != -1:
            return s[:idx]
    return s


def _had_concept_suffix(source_ref: str) -> bool:
    return any(m in (source_ref or "") for m in _CONCEPT_MARKERS)


def _bug_uuid_from(base_ref: str) -> str | None:
    """Extract the bug UUID from a bug-derived base ref so bug:<uuid>,
    card:bug:<uuid> and :bug:<uuid> forms collapse to one identity (AC1)."""
    parts = base_ref.split(":")
    if base_ref.startswith("card:bug:") and len(parts) >= 3 and _is_uuid(parts[2]):
        return parts[2]
    if base_ref.startswith("bug:") and len(parts) >= 2 and _is_uuid(parts[1]):
        return parts[1]
    if "bug" in parts:
        idx = parts.index("bug")
        if idx + 1 < len(parts) and _is_uuid(parts[idx + 1]):
            return parts[idx + 1]
    return None


def resolve_cognitive_source_ref(
    source_ref: Any,
    *,
    canonical_bug_probe: CanonicalBugProbe | None = None,
) -> CognitiveRefResolution:
    """Resolve a cognitive source_ref to its canonical identity + diagnostic.

    ``canonical_bug_probe(uuid) -> bool`` decides whether a plain ``card:<uuid>``
    is backed by a canonical Bug. When omitted, a plain ``card:<uuid>`` resolves
    to a (non-bug-derived) card without raising — the type-aware fail-closed
    behaviour only applies when a probe is supplied.
    """
    original = "" if source_ref is None else str(source_ref)
    ref = original.strip()
    base = strip_concept_suffix(ref)
    canonical = normalize_cognitive_artifact_id(base)

    if not ref or ":" not in ref:
        return CognitiveRefResolution(
            original, canonical or ref, base, CognitiveSourceKind.UNKNOWN.value,
            CognitiveRefResolutionStatus.INVALID_SOURCE_REF.value, False,
            "source_ref must be of the form <type>:<id> (e.g. bug:<uuid>, spec:<id>)")

    head = ref.split(":", 1)[0].strip().lower()
    is_child = _had_concept_suffix(ref)

    # final_report: allowlisted technical root (AC3) — never bug-derived.
    if head == "final_report":
        return CognitiveRefResolution(
            original, canonical, base, CognitiveSourceKind.FINAL_REPORT.value,
            CognitiveRefResolutionStatus.FINAL_REPORT_ALLOWLISTED.value, False,
            "final_report is an allowlisted technical root; provenance is not required")

    # Explicit bug forms are always bug-derived and equivalent (FR2). Collapse
    # bug:/card:bug:/:bug: to the single card:<uuid> identity (AC1).
    if head == "bug" or base.startswith("card:bug:") or ":bug:" in base:
        bug_uuid = _bug_uuid_from(base)
        bug_canonical = f"card:{bug_uuid.lower()}" if bug_uuid else canonical
        return CognitiveRefResolution(
            original, bug_canonical, base, CognitiveSourceKind.BUG.value,
            CognitiveRefResolutionStatus.RESOLVED.value, True, "")

    # Plain card:<uuid> — TYPE-AWARE (FR2 / BR2 / TR1).
    if head == "card":
        rest = base.split(":", 1)[1].strip() if ":" in base else ""
        if _is_uuid(rest):
            if canonical_bug_probe is not None:
                if canonical_bug_probe(rest):
                    return CognitiveRefResolution(
                        original, canonical, base, CognitiveSourceKind.BUG.value,
                        CognitiveRefResolutionStatus.RESOLVED.value, True, "")
                return CognitiveRefResolution(
                    original, canonical, base, CognitiveSourceKind.CARD.value,
                    CognitiveRefResolutionStatus.NON_BUG_CARD.value, False,
                    "card:<uuid> is not backed by a canonical Bug; a bug-derived Learning "
                    "must reference bug:<id> or a card that is a canonical bug")
            # No probe: a plain card ref resolves to a (non-bug) card without failing.
            return CognitiveRefResolution(
                original, canonical, base, CognitiveSourceKind.CARD.value,
                CognitiveRefResolutionStatus.RESOLVED.value, False, "")
        return CognitiveRefResolution(
            original, canonical, base, CognitiveSourceKind.CARD.value,
            CognitiveRefResolutionStatus.INVALID_SOURCE_REF.value, False,
            "card ref tail is not a UUID")

    if head in _KNOWN_TYPES:
        kind = CognitiveSourceKind.CHILD.value if is_child else head
        return CognitiveRefResolution(
            original, canonical, base, kind,
            CognitiveRefResolutionStatus.RESOLVED.value, False, "")

    return CognitiveRefResolution(
        original, canonical, base, CognitiveSourceKind.UNKNOWN.value,
        CognitiveRefResolutionStatus.UNRESOLVED_SOURCE_REF.value, False,
        f"unsupported source_ref type '{head}'")
