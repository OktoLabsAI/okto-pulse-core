"""KG source maturity policy for working/canonical graph partitioning.

The rebuild path materializes canonical sources plus non-expired working
sources. Canonical eligibility is still strict: immature rows carry
``graph_layer=working`` and must not be exposed by canonical-only KG queries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

GRAPH_LAYER_CANONICAL = "canonical"
GRAPH_LAYER_WORKING = "working"
GRAPH_LAYER_NONE = "none"
DEFAULT_WORKING_TTL_DAYS = 7

DISPOSITION_CANONICAL = "canonical"
DISPOSITION_WORKING = "working"
DISPOSITION_SKIPPED_CANCELLED = "skipped_cancelled"
DISPOSITION_SKIPPED_BY_MATURITY = "skipped_by_maturity"
DISPOSITION_SKIPPED_EXPIRED_WORKING = "skipped_expired_working"
DISPOSITION_LEGACY_UNKNOWN = "legacy_unknown"

MATURITY_CANONICAL_ELIGIBLE = "canonical_eligible"
MATURITY_WORKING_IMMATURE = "working_immature"
MATURITY_WORKING_STALE = "working_stale"
MATURITY_WORKING_SUPERSEDED = "working_superseded"
MATURITY_WORKING_DISCARDED = "working_discarded"
MATURITY_CANCELLED = "cancelled"
MATURITY_LEGACY_UNKNOWN = "legacy_unknown"

CANCELLATION_REVOCATION_REASON = "source_cancelled"
CANCELLATION_SCORE_PENALTY = 0.5

CANONICAL_STATUS_BY_ARTIFACT_TYPE: dict[str, frozenset[str]] = {
    "refinement": frozenset({"done"}),
    "spec": frozenset({"done"}),
    "task": frozenset({"done"}),
    "test": frozenset({"done"}),
    "bug": frozenset({"done"}),
    # Path B amendment (spec 7ea1e4be): canonical only at done — AND only with
    # complete lineage, enforced by the lineage_complete guard below so it stays
    # aligned with evaluate_amendment_eligibility.canonicalization_candidate.
    "amendment_hotfix_revision": frozenset({"done"}),
    # Code Traceability is a deterministic projection of governed Pulse rows.
    # Accepted/current rows are canonical facts; conflict/revocation lifecycle
    # states remain visible only in the working partition.
    "code_investigation_receipt": frozenset({"accepted"}),
    "code_evidence": frozenset({"active"}),
    "implementation_target": frozenset({"active"}),
}

WORKING_ARTIFACT_TYPES = frozenset(
    {
        "story",
        "ideation",
        "refinement",
        "spec",
        "task",
        "test",
        "bug",
        "sprint",
        "amendment_hotfix_revision",
        "code_investigation_receipt",
        "code_evidence",
        "implementation_target",
    }
)

REBUILD_ARTIFACT_TYPES: tuple[str, ...] = (
    "story",
    "ideation",
    "refinement",
    "spec",
    "sprint",
    "task",
    "test",
    "bug",
    "amendment_hotfix_revision",
    "code_investigation_receipt",
    "code_evidence",
    "implementation_target",
)

CANONICAL_ARTIFACT_TYPES: tuple[str, ...] = (
    "refinement",
    "spec",
    "task",
    "test",
    "bug",
    "amendment_hotfix_revision",
    "code_investigation_receipt",
    "code_evidence",
    "implementation_target",
)

TERMINAL_CANCELLED_STATUSES = frozenset({"cancelled", "archived"})
EXPIRED_WORKING_STATUSES = frozenset({"superseded", "discarded", "stale"})


@dataclass(frozen=True, slots=True)
class SourceMaturityClassification:
    artifact_type: str
    artifact_status: str
    graph_layer: str
    maturity_status: str
    disposition: str
    reason_code: str
    expires_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "artifact_status": self.artifact_status,
            "graph_layer": self.graph_layer,
            "maturity_status": self.maturity_status,
            "disposition": self.disposition,
            "reason_code": self.reason_code,
            "expires_at": self.expires_at,
        }


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def classify_source_for_kg(
    *,
    artifact_type: str,
    artifact_status: Any,
    content_hash: str | None,
    updated_at: Any = None,
    now: datetime | None = None,
    working_ttl_days: int = DEFAULT_WORKING_TTL_DAYS,
    has_minimal_evidence: bool = True,
    lineage_complete: bool = True,
) -> SourceMaturityClassification:
    """Classify one SDLC source into canonical/working/debt partitions.

    Strict canonical rules:
    - story and ideation never enter canonical; they are working-only.
    - refinement enters canonical only at done.
    - spec enters canonical only at done.
    - task/test/bug enter canonical only at done.
    - sprint remains working/diagnostic-only until deterministic rebuild
      materializes sprint sources end-to-end.
    """

    kind = str(artifact_type or "").strip().lower()
    status = _normalize_status(artifact_status)
    if not kind or kind not in WORKING_ARTIFACT_TYPES:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_NONE,
            maturity_status=MATURITY_LEGACY_UNKNOWN,
            disposition=DISPOSITION_LEGACY_UNKNOWN,
            reason_code="unknown_artifact_type",
        )
    if status in TERMINAL_CANCELLED_STATUSES:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_NONE,
            maturity_status=MATURITY_CANCELLED,
            disposition=DISPOSITION_SKIPPED_CANCELLED,
            reason_code="terminal_cancelled",
        )
    if not status or not content_hash:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_NONE,
            maturity_status=MATURITY_LEGACY_UNKNOWN,
            disposition=DISPOSITION_LEGACY_UNKNOWN,
            reason_code="missing_status_or_content_hash",
        )
    if status in EXPIRED_WORKING_STATUSES:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_WORKING,
            maturity_status=(
                MATURITY_WORKING_SUPERSEDED
                if status == "superseded"
                else MATURITY_WORKING_DISCARDED
                if status == "discarded"
                else MATURITY_WORKING_STALE
            ),
            disposition=DISPOSITION_SKIPPED_EXPIRED_WORKING,
            reason_code=f"{kind}_{status}",
        )

    if kind == "story":
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_WORKING,
            maturity_status=MATURITY_WORKING_IMMATURE,
            disposition=DISPOSITION_WORKING,
            reason_code="story_never_canonical",
        )
    if kind == "bug" and status == "done" and not has_minimal_evidence:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_WORKING,
            maturity_status=MATURITY_WORKING_IMMATURE,
            disposition=DISPOSITION_SKIPPED_BY_MATURITY,
            reason_code="bug_done_without_minimal_evidence",
        )
    if (
        kind == "amendment_hotfix_revision"
        and status == "done"
        and not lineage_complete
    ):
        # Path B amendment (spec 7ea1e4be FR5): a done amendment whose lineage is
        # NOT complete stays working-only — never canonical. This mirrors
        # evaluate_amendment_eligibility.canonicalization_candidate
        # (status==done AND lineage_state==complete). A done+complete amendment
        # falls through to the canonical_statuses match below; any non-done
        # status falls through to the working fallback (working-only before done).
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_WORKING,
            maturity_status=MATURITY_WORKING_IMMATURE,
            disposition=DISPOSITION_SKIPPED_BY_MATURITY,
            reason_code="amendment_lineage_incomplete",
        )
    if kind == "sprint":
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_WORKING,
            maturity_status=MATURITY_WORKING_IMMATURE,
            disposition=DISPOSITION_SKIPPED_BY_MATURITY,
            reason_code="sprint_not_canonical",
        )

    canonical_statuses = CANONICAL_STATUS_BY_ARTIFACT_TYPE.get(kind, frozenset())
    if status in canonical_statuses:
        return SourceMaturityClassification(
            artifact_type=kind,
            artifact_status=status,
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
            disposition=DISPOSITION_CANONICAL,
            reason_code="canonical_status_matched",
        )

    ttl_base = _parse_dt(updated_at)
    expires_at = None
    if ttl_base is not None and working_ttl_days > 0:
        expires = ttl_base + timedelta(days=working_ttl_days)
        expires_at = expires.isoformat()
        now_dt = now or datetime.now(timezone.utc)
        if now_dt > expires:
            return SourceMaturityClassification(
                artifact_type=kind,
                artifact_status=status,
                graph_layer=GRAPH_LAYER_WORKING,
                maturity_status=MATURITY_WORKING_STALE,
                disposition=DISPOSITION_SKIPPED_EXPIRED_WORKING,
                reason_code=f"{kind}_{status}_working_expired",
                expires_at=expires_at,
            )

    return SourceMaturityClassification(
        artifact_type=kind,
        artifact_status=status,
        graph_layer=GRAPH_LAYER_WORKING,
        maturity_status=MATURITY_WORKING_IMMATURE,
        disposition=DISPOSITION_WORKING
        if kind == "ideation"
        else DISPOSITION_SKIPPED_BY_MATURITY,
        reason_code=(
            "ideation_never_canonical"
            if kind == "ideation"
            else f"{kind}_{status}_not_canonical"
        ),
        expires_at=expires_at,
    )


__all__ = [
    "CANONICAL_ARTIFACT_TYPES",
    "CANONICAL_STATUS_BY_ARTIFACT_TYPE",
    "CANCELLATION_REVOCATION_REASON",
    "CANCELLATION_SCORE_PENALTY",
    "DEFAULT_WORKING_TTL_DAYS",
    "DISPOSITION_CANONICAL",
    "DISPOSITION_LEGACY_UNKNOWN",
    "DISPOSITION_SKIPPED_BY_MATURITY",
    "DISPOSITION_SKIPPED_CANCELLED",
    "DISPOSITION_SKIPPED_EXPIRED_WORKING",
    "DISPOSITION_WORKING",
    "GRAPH_LAYER_CANONICAL",
    "GRAPH_LAYER_NONE",
    "GRAPH_LAYER_WORKING",
    "MATURITY_CANONICAL_ELIGIBLE",
    "MATURITY_LEGACY_UNKNOWN",
    "MATURITY_WORKING_IMMATURE",
    "REBUILD_ARTIFACT_TYPES",
    "SourceMaturityClassification",
    "WORKING_ARTIFACT_TYPES",
    "classify_source_for_kg",
]
