---
version: "1.0"
---
# Status Transitions — Mandatory Gates

Executable guideline gate/currentness rules are defined in
`okto-pulse://reference/policy-compliance`.

**When unsure, call `okto_pulse_get_allowed_transitions`** — it returns the valid next statuses (and blocking gates) for the entity's current state. It is always correct, even when a board overrides defaults.

## Normal cards (`card_type = "normal"`)

| From | To | Pre-requisites |
|------|-----|---------------|
| `not_started` | `started` | Spec must be `in_progress` or later |
| `started` | `in_progress` | — |
| `in_progress` | `validation` | — |
| `validation` | `done` | Internal consequence of a successful `okto_pulse_submit_task_validation` and all completion gates |
| `validation` | `rejected` | Internal consequence only: failed validation or admitted governed completion blocker; never an inbound manual transition |
| `rejected` | `in_progress` | Executor accepts the sealed cause and starts a new attempt; this is the only public exit |
| `started`/`in_progress`/`validation` | `on_hold` | Rejected is excluded; it can only start rework |
| `not_started`/`started`/`in_progress`/`validation`/`on_hold` | `cancelled` | Rejected and Done are excluded |

**When moving to `validation`**, include: `conclusion`, `completeness`, `completeness_justification`, `drift`, `drift_justification`.

`rejected` is available only to Normal and Bug cards. Test-card transitions are unchanged. Direct create, drag/drop, generic move, or permission grants cannot assign Rejected. A same-status `rejected` → `rejected` move is accepted only as a position reorder inside the existing Rejected column; it does not create or alter lifecycle state or causal history.

## Test cards (`card_type = "test"`)

| From | To | Pre-requisites |
|------|-----|---------------|
| `not_started` | `started` | Spec must be `validated` or later |
| `not_started` | `in_progress` | Spec must be `validated` or later (direct start is accepted by the API for executable test cards) |
| `started` | `in_progress` | Spec must be `validated` or later |
| `validation` | `in_progress` | Test-only rework; Spec dependencies must still be ready |
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
| `approved` | `validated` | `okto_pulse_submit_spec_validation` with all coverage gates passing + `recommendation=approve`; when the curated checklist binding is `blocking`, a passing Current `/specify/v1` result for the same Spec edition is also required (see `okto-pulse://reference/spec_gates`) |
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

## Code Traceability gates

`code_traceability.mode` resolves to `advisory` (default) or `blocking`.
Historical absent, `null`, or `off` values resolve to `advisory`; `off` is no
longer an authored policy. Neither mode authorizes Pulse Core or Pulse
Community to access source code. All source facts come from bounded receipts
submitted after an authenticated external agent performs the preflight and
investigation in its own environment.

| Transition | Current Code Traceability requirement in `blocking` mode |
|---|---|
| Refinement → `review`/`approved`/`done` | Current accepted agent receipt and Evidence/disposition coverage required by policy, or an applicable human waiver. |
| Spec `draft` → `review` | Every inherited Evidence item is linked to applicable Spec entities or has an explicit disposition; coverage is 100%. |
| Card `not_started`/`started` → executable state | Every active required Target has a current accepted resolution; blocking overlaps are resolved by dependency or current acknowledgement. |
| Card → `validation`/`done` | Every active required Target has an execution disposition bound to a current accepted result receipt. |

In `advisory` mode the same currentness, coverage, and overlap decisions remain
visible but do not independently block the edge. `partial`, `unavailable`,
revoked, expired, conflicted, or outdated receipts never become silently
current. Read full gate context and `okto_pulse_get_allowed_transitions`
immediately before a move.

Canonical protocol: `okto-pulse://reference/code-traceability`.
