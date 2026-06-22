# Card Status Transitions — Mandatory Gates

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
| `not_started` | `in_progress` | Spec must be `validated` or later; direct start is accepted by the API for executable test cards. |
| `started` | `in_progress` | Spec must be `validated` or later |
| `started`/`in_progress`/`validation` | `done` | ALL linked test scenarios must be `passed` or `automated` + `conclusion` + completeness/drift. On `validated`/`done` specs, scenario status/evidence updates are allowed only when the scenario is already linked to an executable test card. |

Test cards skip `okto_pulse_submit_task_validation`; scenario status/evidence is the gate. A single test card may link at most `board.settings.max_scenarios_per_card` scenarios (default 3, board-specific values such as 2 are valid).

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
| `in_progress` | `done` | All cards done |
