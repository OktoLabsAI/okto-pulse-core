---
version: "1.0"
---

# Card Types — Normal, Test & Bug Rules

## There Are Three Types of Cards

1. **Implementation cards** (`card_type="normal"`) — implement functional/technical requirements from the spec.
2. **Test cards** (`card_type="test"`, with `test_scenario_ids`) — implement, execute, or validate test scenarios defined in the spec.
3. **Bug cards** (`card_type="bug"`) — track and fix bugs discovered during or after implementation.

## Normal Card Rules

- `card_type="normal"` → spec must be in `approved`, `in_progress`, or `done`
- Goes through the standard validation gate when `validation_config.required == true`
- Requires conclusion, completeness, and drift when moving to `validation` or `done`
- Transition: `not_started` → `started` → `in_progress` → `validation` → `done`

## Test Card Rules

- `card_type="test"` **requires** `test_scenario_ids` to be non-empty — the server rejects card creation without it.
- A single test card may link at most `board.settings.max_scenarios_per_card` scenarios (default 3; some boards configure 2). Split larger sets before creation; the API rejects over-limit requests with `max_scenarios_per_card_exceeded`.
- Spec must be in `approved`, `validated`, `in_progress`, or `done` for test card creation.
- The scenario-coverage gate counts **only cards with `card_type="test"`**. A `card_type="normal"` card with `test_scenario_ids` does NOT count toward scenario coverage.
- **Always use `card_type="test"` when the intent is to cover a scenario.**
- Test cards skip `okto_pulse_submit_task_validation` — moving to `done` is controlled by scenario status.
- Test cards may move `not_started` → `started` → `in_progress`, and the API also accepts direct `not_started` → `in_progress` when the same prerequisites are met.
- When moving test card to `done`, `okto_pulse_move_card` requires: `conclusion`, `completeness`, `completeness_justification`, `drift`, `drift_justification`.
- **Before moving to `done`**: ALL linked test scenarios must be `passed` or `automated` (not `draft` or `ready`). Call `okto_pulse_update_test_scenario_status` first. If the spec is already `validated` or `done`, this status update is allowed only for a scenario already linked to an executable test card (`started`, `in_progress`, `validation`, or `done`); it records operational evidence and does not unlock semantic spec content.

**Test card naming convention:** Prefix with `[TEST]`:
Example: `[TEST] E2E — Valid OAuth2 token grants access`

| Status to set | Evidence required |
|---|---|
| `draft`, `ready` | none |
| `automated` | `test_file_path` + `test_function` |
| `passed`, `failed` | `last_run_at` + (`output_snippet` OR `test_run_id`) |

Pass evidence as a JSON string in the `evidence` parameter of `okto_pulse_update_test_scenario_status`.

## Bug Card Rules

- `card_type="bug"` → spec must be in `approved`, `in_progress`, or `done`
- `origin_task_id` is **required** — the task that has the bug. `spec_id` is auto-resolved from it.
- Bug cards can ONLY be created with status `not_started` or `started`.
- Required fields: `severity` (`critical` | `major` | `minor`), `expected_behavior`, `observed_behavior`.
- Optional fields: `steps_to_reproduce`, `action_plan`.

**Bug card lifecycle (enforced by the system):**

```
1. Create bug card (status: not_started)
2. Triage the regression path (status: started)
   ├── Path A: reuse an existing scenario only if it is eligible by lineage
   │   (same spec and linked to the bug origin task or affected task)
   └── Path B: if no eligible scenario exists or expected behavior changed,
       remediate via a formal AmendmentHotfixRevision — create/associate it,
       complete lineage, register re-executable evidence, validator confirms
       coverage (refinement/spec-revision authoring alone does NOT satisfy
       the bug gate)
3. Create a fresh test task & link to bug (still started)
   └── okto_pulse_update_card(card_id=bug_id, linked_test_task_ids="<test_task_id>")
4. Move to in_progress (BLOCKED until step 3 is done)
   └── System validates:
       ✓ At least 1 test task linked
       ✓ Each test task has test_scenario_ids
       ✓ Each test task belongs to the same spec as the bug
       ✓ Each referenced scenario is eligible by origin/affected-task lineage
       ✓ Each test TASK (card) was created AFTER the bug card — pre-existing scenarios DO count as regression coverage; the "after the bug" temporal applies to the test TASK, not the scenario
5. Fix the bug (in_progress)
6. Complete (done) — provide conclusion with what was fixed
```

Use `okto_pulse_resolve_bug_regression_scenarios` before creating the test card when eligibility is not obvious. Same-spec membership alone is not enough; unrelated or cross-spec scenarios are semantic gap signals, not acceptable shortcuts around the bug regression gate.

## Coverage Gate Interactions

**Normal and test cards contribute differently:**

| Card type | Counts for scenario coverage gate? | Counts for BR/task linkage gate? |
|---|---|---|
| `card_type="normal"` | NO (even if `test_scenario_ids` set) | YES |
| `card_type="test"` | YES | YES |
| `card_type="bug"` | NO | YES |

**A spec cannot move to `done` unless:**
- Every acceptance criterion has at least one test scenario.
- Every test scenario has at least one linked `card_type="test"` card.
- All linked non-bug, non-archived cards are `done` or `cancelled`.
