"""Markers for the current telemetry transport contract.

Usage batches contain additive deltas. Product snapshots contain point-in-time
values and travel through a separate endpoint; they must never be summed as
usage deltas. The fixed era value is part of the active wire contract.
"""

from __future__ import annotations

ERA_POST_FIX = "post_fix"
SEMANTICS_DELTA = "delta"
SEMANTICS_SNAPSHOT = "snapshot"

POST_FIX_DELTA_MARKER: dict[str, str] = {
    "era": ERA_POST_FIX,
    "semantics": SEMANTICS_DELTA,
}

POST_FIX_SNAPSHOT_MARKER: dict[str, str] = {
    "era": ERA_POST_FIX,
    "semantics": SEMANTICS_SNAPSHOT,
}
