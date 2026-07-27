---
version: "1.0"
---

# Spec Validation Gate & Evaluation Gates

## Spec Status Transitions

Transitions table: see `okto-pulse://reference/transitions` (single source). Note the `in_progress` → `done` gate: all linked non-bug, non-archived cards must be `done` or `cancelled`, and when the spec has sprints, all sprints must be `closed` or `cancelled` (minimum 1 closed — see `okto-pulse://workflows/sprints`).

## Spec Validation Gate — `okto_pulse_submit_spec_validation`

When the board has `require_spec_validation=true`, advancing from `approved` to `validated` is gated.

**The canonical flow:**
1. Populate the spec in `draft` → move through `review` → `approved`.
2. Iterate coverage until ALL deterministic gates are green: AC coverage 100%, FR→BR coverage 100%, scenario→task linkage 100%, contract→task linkage 100%, TR→task linkage 100%.
3. Call `okto_pulse_submit_spec_validation(board_id, spec_id, completeness, completeness_justification, assertiveness, assertiveness_justification, ambiguity, ambiguity_justification, general_justification, recommendation)`.
4. The gate runs coverage checks first. If any fail, you get a coverage error — fix the gap and retry.
5. If coverage passes, the gate computes `outcome` atomically:
   - `outcome=failed` if ANY threshold is violated OR `recommendation=reject`
   - `outcome=success` ONLY if all thresholds pass AND `recommendation=approve`
6. On `success`, the spec is atomically promoted to `validated` AND enters **content lock**.

**Thresholds (default 80/80/30):**

All scores are 0-100 integers, not 1-5. A 1-5-style score is treated literally
as 1/100 through 5/100 and will usually fail the configured thresholds.

| Dimension | Direction | Default threshold |
|---|---|---|
| `completeness` | Higher is better (min) | 80 |
| `assertiveness` | Higher is better (min) | 80 |
| `ambiguity` | LOWER is better (max) | 30 |

**Anti-pattern — GRAVE violation:** Re-submitting with higher numbers without actually improving the spec. The correct response to a failed dimension is to ADD content (new test scenarios, refined BRs, edge cases) until you genuinely believe the score is higher.

**FR coverage source:** the deterministic FR coverage gate is `Functional Requirement -> Business Rule`. It reads `business_rules[].linked_requirements`; direct task links on `functional_requirements[].linked_task_ids` are traceability only and do not close `fr_coverage_pct`.

**MCP tools for the gate:**
- `okto_pulse_submit_spec_validation(...)` — requires spec in `approved` status.
- `okto_pulse_list_spec_validations(board_id, spec_id)` — returns full history with `active=true` on the current pointer.
- `okto_pulse_move_spec(board_id, spec_id, status="draft")` — the single-hop unlock path from `validated` or `approved`. Clears `current_validation_id` but preserves the validations array.

## Spec Evaluation Gate — `okto_pulse_submit_spec_evaluation`

After a spec reaches `validated`, it must undergo qualitative evaluation before moving to `in_progress`.

**Tool:** `okto_pulse_submit_spec_evaluation(board_id, spec_id, breakdown_completeness, breakdown_justification, granularity, granularity_justification, dependency_coherence, dependency_justification, test_coverage_quality, test_coverage_justification, overall_score, overall_justification, recommendation)`

**Evaluation dimensions (each scored 0-100 with mandatory justification):**

| Dimension | What to assess |
|-----------|---------------|
| `breakdown_completeness` | Do derived cards fully cover the spec's scope? Are any FRs, TRs, or ACs not addressed by any card? |
| `granularity` | Are cards properly sized for independent execution? |
| `dependency_coherence` | Do card dependencies reflect the real execution order? Any circular deps or missing prerequisites? |
| `test_coverage_quality` | Do test scenarios cover happy paths AND edge cases? Are Given/When/Then concrete and verifiable? |
| `overall_score` | Holistic assessment — is this spec ready for execution? |

**Recommendations:**
- `approve` — spec is ready for execution (required for validated → in_progress transition)
- `request_changes` — spec has issues that should be addressed. Spec stays in `validated`.
- `reject` — spec is fundamentally flawed and needs significant rework. **Blocks** the spec.

**Gate enforcement (validated → in_progress):**
- At least 1 evaluation with `recommendation="approve"`
- Zero evaluations with `recommendation="reject"`
- Average `overall_score` of approvals ≥ `validation_threshold` (default 70)
- Unless `skip_qualitative_validation` flag is set

**When evaluating, always:**
1. Read the full spec: `okto_pulse_get_spec(board_id, spec_id)`
2. Review all test scenarios: `okto_pulse_list_test_scenarios(board_id, spec_id)` — check coverage map
3. Review business rules: `okto_pulse_list_business_rules(board_id, spec_id)`
4. Review API contracts: `okto_pulse_list_api_contracts(board_id, spec_id)`
5. Check that every FR maps to ≥1 card, every AC maps to ≥1 test scenario, every test scenario maps to ≥1 test card
6. Verify card granularity and dependencies make sense for parallel execution

## Decisions Coverage Gate

`skip_decisions_coverage` defaults to `False` on newly created specs. `okto_pulse_submit_spec_validation` calls `check_decisions_coverage` and rejects the spec if any Decision with `status="active"` has no `linked_task_ids`.

**Coverage summary** in `okto_pulse_get_spec_context`:
- `decisions_total`: total count of `active` decisions.
- `decisions_linked`: `active` decisions that have at least one entry in `linked_task_ids`.
- `decisions_coverage_pct`: 0-100.
- `decisions_uncovered_ids`: list of `decision.id` values with no linked tasks.

## Task Validation Gate — `okto_pulse_submit_task_validation`

When the **Task Validation Gate** is enabled (`validation_config.required == true`), cards must pass through an independent validation before moving to `done`.

**Deterministic thresholds:**

| Threshold | Default | Rule |
|-----------|---------|------|
| `min_confidence` | 70 | `confidence < min_confidence` → **auto-fail** |
| `min_completeness` | 80 | `estimated_completeness < min_completeness` → **auto-fail** |
| `max_drift` | 50 | `estimated_drift > max_drift` → **auto-fail** |

**Threshold violations auto-fail the validation regardless of the reviewer's recommendation.** Even with `recommendation="approve"`, the validation fails if any threshold is violated.

The `resolved_from` field in `validation_config` tells you which level provided the active configuration (`"board"`, `"spec"`, or `"sprint"`).

**Independent reviewer policy:** `reviewer_separation_mode` is resolved from the board before any task-validation mutation. The full task context projects the current caller's `reviewer_separation` decision against card creator, assignee, and executor identities. `enforce` blocks with the action-required code `reviewer_separation_required`; `warn` and `off` proceed and persist the decision in the append-only validation. Legacy persisted boards with the setting absent resolve explicitly to `off` / `legacy_absent_compat`; new boards and new default-board template versions use `enforce` unless configured otherwise.

**Coverage summary tools:**
- `okto_pulse_list_spec_evaluations(board_id, spec_id)` — evaluations history.
- `okto_pulse_get_spec_evaluation(board_id, spec_id, evaluation_id)` — single evaluation detail.
- `okto_pulse_list_spec_validations(board_id, spec_id)` — validation gate history.
- `okto_pulse_list_task_validations(board_id, card_id)` — task-level validation history.
