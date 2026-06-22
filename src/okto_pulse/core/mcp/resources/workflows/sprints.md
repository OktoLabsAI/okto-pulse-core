---
version: "1.0"
---

# Sprints Workflow — Lifecycle & Evaluation

## 2.10 Sprints — Incremental Delivery Slices

Sprints break large specs into incremental deliverables with scoped gates and evaluations.

**Lifecycle:** draft → active → review → closed (cancelled from any state)

**When to use sprints:**
- Specs with many tasks (typically 6+ cards) benefit from sprint breakdown
- The system automatically suggests sprints during spec validation (approved → validated) when task count exceeds the threshold
- Sprints are optional — specs can work without them

**Creating sprints:**
1. Use `okto_pulse_suggest_sprints(board_id, spec_id, threshold?)` to get AI-suggested breakdown
2. Create sprints with `okto_pulse_create_sprint` — scope test_scenario_ids and business_rule_ids from the spec
3. Assign cards with `okto_pulse_assign_tasks_to_sprint(board_id, sprint_id, card_ids)`

### Sprint Lane Model

Sprints expose `lane_type` so normal delivery work and post-closure bug work are distinguishable in API, MCP, analytics, history, and KG discovery.

| `lane_type` | Meaning |
|-------------|---------|
| `normal` | Default delivery sprint. Existing sprints with no lane metadata are treated as `normal`. |
| `hotfix` | Post-closure execution lane for bug and regression test cards on a done spec or closed origin sprint. |

Hotfix lanes carry optional lineage fields:
- `origin_sprint_id` — the closed original sprint that produced the post-closure work, when applicable.
- `origin_bug_id` — the bug card that triggered the lane, when available.

An `active` hotfix lane satisfies the same sprint ownership gate as an `active` normal sprint. It does not bypass bug governance: bug cards in hotfix lanes still need the required linked post-bug test card when the board bug regression gate applies.

### MANDATORY — Detailed Sprint Fields

When creating or updating a sprint, the following fields MUST be filled with meaningful, detailed content:

- **`title`** — descriptive name that communicates the sprint's focus (e.g., "Sprint 1 — Auth Layer + JWT Validation", not "Sprint 1")
- **`description`** — comprehensive description of what the sprint covers. MUST include:
  - The scope boundary (what is IN this sprint vs. deferred to later sprints)
  - The key deliverables expected at the end
  - Any dependencies on previous sprints or external systems
  - Risk factors or areas of uncertainty
- **`objective`** — a clear, specific statement of what this sprint aims to achieve. Not a vague goal like "implement features" but a concrete target.
- **`expected_outcome`** — a verifiable description of what "done" looks like for this sprint. Must be concrete enough that someone can verify it.

**Bad examples (DO NOT write like this):**
- objective: "Implement sprint 1 features" ← vague, says nothing
- description: "First sprint" ← no scope, no deliverables, no context
- expected_outcome: "Everything works" ← not verifiable

**Good examples:**
- objective: "Establish the data layer and core CRUD operations for the sprint entity, including lifecycle validation, card assignment, and scope resolution from the parent spec."
- expected_outcome: "POST/GET/PATCH/DELETE sprint endpoints functional, move endpoint enforces all gates, assign-tasks links cards correctly. 8 test scenarios pass."

### Sprint Scope — Inherited from Parent Spec

A sprint's scope is computed from its assigned cards' relationships to the parent spec:
- **Test Scenarios**: Union of sprint-level `test_scenario_ids` + spec test scenarios where `linked_task_ids` includes any sprint card
- **Business Rules**: Union of sprint-level `business_rule_ids` + spec BRs where `linked_task_ids` includes any sprint card
- **Technical Requirements**: Spec TRs where `linked_task_ids` includes any sprint card
- **API Contracts**: Spec contracts where `linked_task_ids` includes any sprint card

For scope to resolve correctly, **you MUST link spec artifacts to cards** using `okto_pulse_link_task` with the correct `target_type` (`scenario`, `rule`, `contract`, `tr`, `ir`, `or`, `decision`, or `fr`).

### Sprint Gates

| Transition | Gate |
|------------|------|
| draft → active | At least 1 card assigned |
| active → review | Scoped test scenarios must be passed (unless skip_test_coverage) |
| review → closed | Qualitative evaluation with at least 1 approval, 0 rejects, avg score ≥ threshold |

**Card behavior with sprints:**
- If a spec has sprints, `card.sprint_id` is **mandatory** — cards without a sprint cannot advance
- Cards can only advance when their sprint is in `active` status

**Spec done gate with sprints:**
- All sprints must be `closed` or `cancelled` (minimum 1 closed)
- Coverage gates evaluate the total spec, not individual sprints

### Sprint Evaluation (4 Dimensions + Overall)

When a sprint is in `review` status, submit an evaluation via `okto_pulse_submit_sprint_evaluation`. Each dimension scores 0-100 with a mandatory justification:

| Dimension | What to evaluate |
|-----------|-----------------|
| `breakdown_completeness` | Do the assigned cards fully cover the sprint's scoped requirements? Are there gaps? |
| `granularity` | Are cards properly sized? Too large = hard to track, too small = overhead. |
| `dependency_coherence` | Do card dependencies make sense? Are there circular deps or missing prerequisites? |
| `test_coverage_quality` | Do test scenarios cover happy paths AND edge cases? Are they actually verifiable? |
| `overall_score` | Overall assessment considering all dimensions. |

**recommendation:** `approve` (sprint can close), `request_changes` (needs rework), `reject` (fundamentally flawed)

**Permission flags:** 25 flags under `sprint.*` — entity (9), move (4), interact_in (5), qa (3), evaluations (3), history_read (1)

### Sprint Status Transitions

| From | To | Pre-requisites |
|------|-----|---------------|
| `draft` | `active` | Must have assigned cards |
| `active` | `review` | Scoped test scenarios must be `passed` (unless `skip_test_coverage`) |
| `review` | `closed` | `okto_pulse_submit_sprint_evaluation` with `recommendation=approve` must pass |
