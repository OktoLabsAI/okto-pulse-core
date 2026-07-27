"""Spec C — read-only, forward-only diagnostic for legacy Architecture Design snapshots.

A "legacy snapshot" is a copied Architecture Design (``source_design_id`` set) whose
SOURCE is now ineligible for propagation under the canonical policy (Spec A): the source
has active critic findings, its verdict cannot be loaded, or the source design is gone.

This report is strictly READ-ONLY / bounded / idempotent (TR db2e907b): it never backfills,
resolves findings, mutates snapshots, or changes any SDLC status. It only surfaces the
forward-only debt so an operator/agent can fix the SOURCE design (QA decision: forward-only,
no destructive mutation).
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.application_persistence import bounded_page_offset
from okto_pulse.core.ports.architecture_legacy import (
    get_architecture_legacy_snapshot_read_port,
)
from okto_pulse.core.services.architecture import (
    PROPAGATION_REVALIDATION_MISSING_RUN,
    PROPAGATION_VERDICT_UNAVAILABLE,
    ArchitecturePropagationEligibilityPolicy,
)
from okto_pulse.core.services.architecture_observability import (
    observe_architecture_propagation_legacy_report,
)

# legacy_status taxonomy (locked by Spec C).
LEGACY_STATUS_SOURCE_BLOCKED = "source_blocked"  # source exists with active findings
LEGACY_STATUS_VERDICT_MISSING = "verdict_missing"  # no current run / had to revalidate
LEGACY_STATUS_SOURCE_UNAVAILABLE = (
    "source_unavailable"  # source gone / verdict unloadable
)

LEGACY_STATUS_VALUES = (
    LEGACY_STATUS_SOURCE_BLOCKED,
    LEGACY_STATUS_VERDICT_MISSING,
    LEGACY_STATUS_SOURCE_UNAVAILABLE,
)

_MAX_LIMIT = 200


def _classify_legacy_status(eligibility: Any) -> str | None:
    """Map a propagation eligibility verdict to a legacy_status, or None when the source
    is eligible (the snapshot is NOT legacy debt)."""
    if eligibility.eligible:
        return None
    if eligibility.verdict_status == PROPAGATION_VERDICT_UNAVAILABLE:
        return LEGACY_STATUS_SOURCE_UNAVAILABLE
    if eligibility.revalidation_reason == PROPAGATION_REVALIDATION_MISSING_RUN:
        return LEGACY_STATUS_VERDICT_MISSING
    return LEGACY_STATUS_SOURCE_BLOCKED


async def build_propagation_legacy_report(
    db: Any,
    *,
    board_id: str,
    limit: int = 100,
    offset: int = 0,
    include_clean: bool = False,
    parent_type_filter: str | None = None,
    surface: str = "service",
) -> dict[str, Any]:
    """Build a bounded, read-only legacy propagation report for a board.

    Scans copied Architecture Designs (``source_design_id`` set) page by page and classifies
    each by the SOURCE's current propagation eligibility. Returns only legacy (problematic)
    items unless ``include_clean`` is true. Never mutates anything.
    """
    limit = max(1, min(int(limit), _MAX_LIMIT))
    # Backstop: reject an offset above int64 before it reaches the read port's
    # SQL OFFSET binding (which would raise a raw OverflowError). Boundaries
    # (REST le=PAGE_OFFSET_MAX / MCP invalid_pagination) reject earlier; this
    # keeps a direct/service-level call fail-closed with a typed ValueError.
    offset = bounded_page_offset(offset)

    page = await get_architecture_legacy_snapshot_read_port().list_page(
        db,
        board_id=board_id,
        parent_type_filter=parent_type_filter,
        limit=limit,
        offset=offset,
    )

    policy = ArchitecturePropagationEligibilityPolicy(db)
    items: list[dict[str, Any]] = []
    for target in page.items:
        eligibility = await policy.evaluate(str(target.source_design_id))
        legacy_status = _classify_legacy_status(eligibility)
        if legacy_status is None and not include_clean:
            continue
        items.append(
            {
                "target_design_id": target.id,
                "target_parent": {"type": target.parent_type, "id": target.parent_id},
                "source_design_id": target.source_design_id,
                "source_ref": target.source_ref,
                "source_version": target.source_version,
                "legacy_status": legacy_status or "eligible",
                "verdict_status": eligibility.verdict_status,
                "finding_keys": list(eligibility.finding_keys),
                "remediation": eligibility.remediation,
                "mutation_performed": False,
            }
        )

    observe_architecture_propagation_legacy_report(
        board_id=board_id,
        scanned_count=page.total,
        legacy_count=len(items),
        surface=surface,
    )

    return {
        "board_id": board_id,
        "items": items,
        "scanned_total": page.total,
        "limit": limit,
        "offset": offset,
        "mutation_performed": False,
    }
