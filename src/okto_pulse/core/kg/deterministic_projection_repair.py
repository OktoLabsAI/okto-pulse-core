"""Bounded replay admission; the existing deterministic worker owns extraction.

This schedules source projections, not cognitive consolidation. No graph nodes,
source content, cognitive ledger states or broad backfill markers are authored.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.ports.kg_operational import get_kg_worker_audit_port
from okto_pulse.core.ports.relational_effects import (
    ConsolidationQueueUpsert,
    get_relational_effects_port,
)


async def stage_spec_projection_repair(
    context: object, *, board_id: str, spec_ids: tuple[str, ...],
    actor_id: str, reason: str,
) -> dict[str, object]:
    """Stage queue + audit atomically; caller owns commit and worker wake-up.

    Caller must authorize and validate the complete selection before staging.
    Tombstones suppress admission atomically. Active/paused/rebuild work keeps
    its current owner; a suppressed row is never reported as successfully queued.
    """
    correlation_id = str(uuid4())
    queued: list[str] = []
    coalesced_or_fenced: list[str] = []
    effects = get_relational_effects_port()
    for spec_id in spec_ids:
        changed = await effects.upsert_consolidation_queue_unless_tombstoned(
            context,
            ConsolidationQueueUpsert(
                board_id=board_id, artifact_type="spec", artifact_id=spec_id,
                priority="high", source="deterministic_projection_repair",
                triggered_by_event="kg.deterministic_projection_repair.requested",
                payload={"projection_repair_request": correlation_id},
                coalesce_active=True,
            ),
        )
        (queued if changed else coalesced_or_fenced).append(spec_id)
    result = {
        "board_id": board_id, "correlation_id": correlation_id,
        "queued_spec_ids": queued, "coalesced_or_fenced_spec_ids": coalesced_or_fenced,
        "queued_count": len(queued), "cognitive_consolidation_started": False,
    }
    await get_kg_worker_audit_port().record_audit_event(
        context,
        payload={
            "session_id": correlation_id, "board_id": board_id,
            "artifact_id": board_id, "artifact_type": "projection_repair",
            "agent_id": actor_id, "started_at": datetime.now(timezone.utc),
            # No committed_at: scheduling is not a graph commit receipt.
            "summary_text": json.dumps({"reason": reason, **result}, sort_keys=True),
        },
    )
    return result
