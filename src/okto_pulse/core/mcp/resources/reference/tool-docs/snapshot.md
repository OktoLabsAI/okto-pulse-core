---
version: "1.0"
---

# Tool docs — `snapshot`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_list_snapshots`

List version snapshots for an ideation or refinement.

    Consolidates: list_ideation_snapshots, list_refinement_snapshots.

    Each snapshot is an immutable copy of the entity's state at the moment
    it was marked as 'done'.

    Args:
        board_id: Board ID
        entity_type: One of: ideation, refinement
        entity_id: ID of the entity

    Returns:
        JSON {snapshots: [...], count: int, entity_type: str} or structured error
