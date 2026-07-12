---
version: "1.0"
---
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

## Filters by entity_type (`list_by_board`)

Filter keys are validated server-side per `entity_type`. Passing an unknown key
returns a structured error `error_code='invalid_filter'` whose `supported` field
lists the allowed keys and whose `invalid_keys` field echoes the rejected ones.

| entity_type | Allowed filter keys | Required |
|---|---|---|
| `spec` | `status`, `labels`, `assignee_id` | — |
| `ideation` | `status`, `labels`, `derivation_pending` | — |
| `refinement` | `ideation_id`, `status`, `labels`, `derivation_pending` | `ideation_id` |
| `sprint` | `spec_id`, `status` | `spec_id` |
| `story` | `status`, `topic_id`, `linked`, `converted`, `include_archived` | — |
| `topic` | `include_archived` | — |

Examples:

```
okto_pulse_list_by_board(board_id, entity_type="spec", filters={"status": "draft"})
okto_pulse_list_by_board(board_id, entity_type="ideation", filters={"derivation_pending": true})
okto_pulse_list_by_board(board_id, entity_type="refinement", filters={"ideation_id": "...", "derivation_pending": true})
okto_pulse_list_by_board(board_id, entity_type="sprint", filters={"spec_id": "..."})
```

`filters` may also be passed as a JSON string (`'{"status": "draft"}'`); it is
auto-decoded by the handler.

## Derivation pending

`derivation_pending` (bool) is the **canonical surface for listing done work that
still lacks a derived child** — do not fall back to raw API calls or manual scans.

- `entity_type='ideation'` with `filters={'derivation_pending': true}` lists DONE
  ideations pending derivation: medium/large ideations with zero active child
  refinements, and small ideations with zero active direct specs.
- `entity_type='refinement'` with `filters={'ideation_id': '...', 'derivation_pending': true}`
  lists DONE refinements with zero active child specs.
- Cancelled or archived child refinements/specs are not counted as active derivations.
- Each returned item carries `derivation_pending`, `active_refinement_count` (ideations)
  and `active_spec_count` fields.

### End-to-end triage: pending derivations

1. List the backlog:
   `okto_pulse_list_by_board(board_id, entity_type="ideation", filters={"derivation_pending": true})`
2. Open a candidate: `okto_pulse_get_ideation(board_id, ideation_id)` — check `complexity`.
3. Derive:
   - small ideation → `okto_pulse_derive_spec_from_ideation(board_id, ideation_id)`
   - medium/large ideation → `okto_pulse_create_refinement(...)`; once the refinement is
     DONE, find pending ones via
     `okto_pulse_list_by_board(board_id, entity_type="refinement", filters={"ideation_id": "...", "derivation_pending": true})`
     and call `okto_pulse_derive_spec_from_refinement(board_id, refinement_id)`.
