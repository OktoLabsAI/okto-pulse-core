"""Application orchestration for idempotent legacy FR/AC materialization."""

from __future__ import annotations

from okto_pulse.core.domain.spec_materialization import (
    plan_legacy_fr_ac_materialization,
)
from okto_pulse.core.ports.spec_materialization import SpecMaterializationStore


async def materialize_legacy_fr_ac_board(
    store: SpecMaterializationStore,
    board_id: str,
    *,
    dry_run: bool = True,
) -> dict[str, object]:
    specs = await store.list_specs(board_id)
    plan = plan_legacy_fr_ac_materialization(specs)
    summary: dict[str, object] = {
        "board_id": board_id,
        "dry_run": dry_run,
        "scanned": plan.scanned,
        "changed": plan.changed,
        "skipped": plan.skipped,
        "errors": plan.errors,
    }
    if not dry_run:
        await store.apply(plan)
    return summary


__all__ = ["materialize_legacy_fr_ac_board"]
