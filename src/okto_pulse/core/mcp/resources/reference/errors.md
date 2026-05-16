---
version: "1.0"
---

# Common Errors and How to Fix Them

This table is the **single source of truth** for MCP-level errors. Before any ad hoc retry or workaround, consult this section and apply the canonical fix.

## Resource Gate

| Error message | Cause | Fix |
|---|---|---|
| `resource_gate_missing_resources` | Architecture, Mockup, or Knowledge Base is missing and not marked N/A for the entity being validated, started, or completed | Call `okto_pulse_get_resource_gate_summary`, then attach the missing artifact. For cards/tasks/tests/bugs, copy inherited artifacts with `okto_pulse_copy_architecture_to_card`, `okto_pulse_copy_mockups_to_card`, or `okto_pulse_copy_knowledge_to_card`, or add direct card KEs with `okto_pulse_add_card_knowledge`. Use N/A only with a real `justification`. |
| `invalid_entity_type` | Resource Gate was called with a non-canonical entity type such as `task`, `test`, or `bug` | Retry with the matrix above: `ideation`, `refinement`, `spec`, or `card`. Tasks, tests, and bugs must use `entity_type=card`. |

## Stories / Topics

| Error message | Cause | Fix |
|---|---|---|
| `topic_not_empty` | Tried to delete a Topic that still has active or archived Stories | Inspect the returned counts, then use `okto_pulse_merge_topics`, `okto_pulse_move_story`, or `okto_pulse_archive_story`. Do not retry delete until the Topic is empty. |
| `Only ready Stories can be converted` | Story is still `draft`, `triage`, `converted`, or archived | List the Story, resolve triage, then call `okto_pulse_move_story(status="ready")` before link/convert. Archived Stories must be restored first. |
| `Story can only be linked to editable Ideations` | Target Ideation is `done`, `cancelled`, archived, or otherwise frozen | Pick an editable Ideation (`draft`, `review`, `approved`, or `evaluating`) or create/restore the correct Ideation before linking. |

## Card / Move Transitions

| Error message | Cause | Fix |
|---|---|---|
| `"A conclusion is required when moving a card to Validation"` / `"A conclusion is required when moving a card to Done"` | Missing executor report: `conclusion`, `completeness`, `completeness_justification`, `drift`, `drift_justification` | Add all 5 parameters to `okto_pulse_move_card`. |
| `"Card type 'test' is not subject to validation gate"` | Called `okto_pulse_submit_task_validation` on a test card | Test cards skip the validation gate — move directly to `done` after scenarios are `passed`. |
| `"N test scenario(s) still have status 'draft'"` | Test card's linked scenarios not updated | Call `okto_pulse_update_test_scenario_status(status="passed")` for each linked scenario, then retry `okto_pulse_move_card`. |
| `"Cannot move card forward: spec must be at least 'in_progress'"` | Spec is in `approved` or `validated` | Move the spec to `in_progress` first via `okto_pulse_move_spec` (requires `okto_pulse_submit_spec_evaluation` with `recommendation=approve` on a `validated` spec). |
| `"Validation gate is active. Move card to 'validation' first"` | Tried to move a normal card directly to `done` | Move to `validation` with the executor report, then `okto_pulse_submit_task_validation`. |

## Card Creation

| Error message | Cause | Fix |
|---|---|---|
| `"Every task must be linked to a spec"` | `spec_id` missing on `okto_pulse_create_card` | Always pass `spec_id`. |
| `"<Type> cards can only be created for specs in <list> status. Spec '<title>' is currently '<status>'."` | Spec status doesn't accept card creation of this `card_type` | See card type governance rules. Move the spec forward with `okto_pulse_move_spec`. |
| `"Test scenario(s) not found in spec '<title>': [...]"` | Passed `test_scenario_ids` that don't exist on that spec | List scenarios with `okto_pulse_list_test_scenarios` and use a valid id. |

## Bug Cards

| Error message | Cause | Fix |
|---|---|---|
| `"origin_task_id is required for bug cards"` | Missing `origin_task_id` | Pass the id of the task where the bug was found. |
| `"Bug cards can only be created with status not_started or started"` | Tried to create in a later status | Create as `not_started`, then advance via `okto_pulse_move_card`. |
| `"Bug card requires at least 1 new test task linked"` | Moving a bug to `in_progress` without test coverage | Create a new `card_type="test"` regression card and link it to the bug via `okto_pulse_update_card(linked_test_task_ids=...)`, then move. |
| `"Linked test task has no test_scenario_ids"` | The linked card is not a proper test task | Link it to a scenario via `okto_pulse_link_task_to_scenario`, or recreate with `card_type="test"` + `test_scenario_ids`. |
| `"Test task belongs to a different spec"` | The linked test task is on another spec | Create the test task on the same spec as the bug. |
| `"Linked test task must be created after this bug card"` | The linked regression task predates the bug | Create a new `card_type="test"` card after the bug. |
| `"Test scenario does not exist in spec"` | Scenario was deleted or the id is wrong | Create a new scenario with `okto_pulse_add_test_scenario`. |

## Spec Coverage / Validation

| Error message | Cause | Fix |
|---|---|---|
| `"Cannot start this card: N test scenario(s) have no linked task cards"` | Scenarios have no test cards linked | For each uncovered scenario, create a test card (`card_type="test"` + `test_scenario_ids`) and/or call `okto_pulse_link_task_to_scenario`. |
| `"Cannot start this card: N functional requirement(s) have no linked business rules"` | FR→BR coverage incomplete | Call `okto_pulse_add_business_rule` with `linked_requirements` referencing the uncovered FR indices. |
| `"Cannot start this card: N business rule(s) have no linked task cards"` | BR→Task coverage incomplete | Call `okto_pulse_link_task_to_rule` for each unlinked BR. |
| `"Cannot validate spec: N business rule(s) have no linked task cards"` | Same, at validation time | Same fix — link implementation tasks to every BR. |
| `"Cannot validate spec: N test scenario(s) have no linked test cards"` | Scenario side | Create/link test cards for every scenario. |
| `"Cannot move spec to 'done': N acceptance criteria lack test scenarios"` | AC→Scenario coverage incomplete | Create a scenario for every uncovered AC (use `linked_criteria` with the 0-based index). |
| `"Cannot move spec to 'done': N linked task(s) are not yet done or cancelled"` | Open task cards still attached | Complete or cancel the pending task cards (bugs are excluded from this check). |

## Multi-Value Parameters (`parse_multi_value`)

| Error message | Cause | Fix |
|---|---|---|
| `"malformed JSON for multi-value param: ... (at pos N)"` | Input started with `[` so the JSON path was taken, but the JSON was invalid | Fix the JSON syntax (quoting, brackets). |
| `"malformed multi-value: expected list, got <type>"` | JSON decoded to a non-list (e.g. an object) | Send an array, not an object. |
| `"malformed multi-value: expected string items, got <type> at index N"` | JSON array had a non-string item | Every item must be a string. |
