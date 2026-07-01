# Consolidated List Tools (P0.B)

The following 4 polymorphic tools are the supported MCP list surface.
The 15 entity-specific `list_*` MCP tools are no longer registered.

| New tool | Replaces |
|---|---|
| `okto_pulse_list_by_board` | `list_specs`, `list_ideations`, `list_refinements`, `list_sprints`, `list_stories`, `list_topics` |
| `okto_pulse_list_qa` | `list_spec_qa`, `list_ideation_qa`, `list_refinement_qa` |
| `okto_pulse_list_knowledge` | `list_spec_knowledge`, `list_ideation_knowledge`, `list_refinement_knowledge`, `list_card_knowledge` |
| `okto_pulse_list_snapshots` | `list_ideation_snapshots`, `list_refinement_snapshots` |

## Required filters

- `entity_type='refinement'` requires `filters={'ideation_id': '...'}` in `list_by_board`.
- `entity_type='sprint'` requires `filters={'spec_id': '...'}` in `list_by_board`.

## Derivation pending

- `entity_type='ideation'` accepts `filters={'derivation_pending': true}` to list DONE medium/large ideations with zero active child refinements.
- `entity_type='refinement'` accepts `filters={'ideation_id': '...', 'derivation_pending': true}` to list DONE refinements with zero active child specs.
- Cancelled or archived child refinements/specs are not counted as active derivations.
