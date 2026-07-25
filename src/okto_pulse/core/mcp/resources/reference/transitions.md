---
version: "1.0"
---
# Status Transitions — Mandatory Gates

**When unsure, call `okto_pulse_get_allowed_transitions`** — it returns the valid next statuses (and blocking gates) for the entity's current state. It is always correct, even when a board overrides defaults.

## Normal cards (`card_type = "normal"`)

| From | To | Pre-requisites |
|------|-----|---------------|
| `not_started` | `started` | Spec must be `in_progress` or later |
| `started` | `in_progress` | — |
| `in_progress` | `validation` | — |
| `validation` | `done` | `okto_pulse_submit_task_validation` with `recommendation=approve` |
| Any | `on_hold` | — |
| Any | `cancelled` | — |

**When moving to `validation`**, include: `conclusion`, `completeness`, `completeness_justification`, `drift`, `drift_justification`.

## Test cards (`card_type = "test"`)

| From | To | Pre-requisites |
|------|-----|---------------|
| `not_started` | `started` | Spec must be `validated` or later |
| `not_started` | `in_progress` | Spec must be `validated` or later (direct start is accepted by the API for executable test cards) |
| `started` | `in_progress` | Spec must be `validated` or later |
| `started`/`in_progress`/`validation` | `done` | ALL linked test scenarios must be `passed` or `automated` + `conclusion` + completeness/drift |

Type rules — scenario cap (`max_scenarios_per_card`), evidence gate, validation-gate skip, scenario updates on locked specs: see `okto-pulse://reference/card_types`.

## Sprint transitions

| From | To | Pre-requisites |
|------|-----|---------------|
| `draft` | `active` | Must have assigned cards |
| `active` | `review` | Scoped test scenarios must be `passed` |
| `review` | `closed` | `okto_pulse_submit_sprint_evaluation` with `recommendation=approve` |

## Spec transitions

| From | To | Pre-requisites |
|------|-----|---------------|
| `draft` | `review` | — |
| `review` | `approved` | — |
| `approved` | `validated` | `okto_pulse_submit_spec_validation` with all coverage gates passing + `recommendation=approve` |
| `validated` | `in_progress` | `okto_pulse_submit_spec_evaluation` with `recommendation=approve` |
| `in_progress` | `done` | All linked non-bug, non-archived cards `done` or `cancelled`; when sprints exist, all sprints `closed`/`cancelled` (minimum 1 closed) |

## Ideation transitions

`draft` → `review` → `approved` → `evaluating` → `done`; `cancelled` from any status except `done`; `cancelled` → `draft` reopens a new editable iteration and clears the cancellation record. Editing only in `draft`, evaluation only in `evaluating`, derivations only from `done`. Details: `okto-pulse://workflows/ideations`.

## Refinement transitions

`draft` → `review` → `approved` → `done`; `done` or `cancelled` → `draft` starts a new editable version, and reopening a cancellation clears its audit record. Details: `okto-pulse://workflows/refinements`.

## Story transitions

`draft` → `triage` → `ready` → `converted`, via `okto_pulse_move_story(status=...)`. Only `ready` stories can be linked/converted; archived stories must be restored first. Details: `okto-pulse://workflows/stories`.

## Amendment revision transitions

`draft` → `review` → `approved` → `done`; terminal: `cancelled`/`superseded` (never resurrected — create a new revision). Promotion to `approved`/`done` requires `lineage_state=complete` first. Use `okto_pulse_transition_amendment_revision`; new revisions MUST start `draft`.

## Bug cards

Bug lifecycle and the regression gate (Path A/B): see `okto-pulse://reference/card_types`.
