---
version: "1.0"
---

# Cards Workflow — Implementation, Bug & Test Execution

## 2.7b Architecture Design — Structural Artifacts (Summary)

Architecture Design is the first-class place for system structure. See the full reference in the spec workflow or the architecture tools documentation. Use `okto_pulse_copy_architecture_to_card` before moving to `in_progress`.

## 2.8 Cards (Tasks)

### Card-Level Artifact Attachment (MANDATORY)

> **A card must be self-contained.** Any agent or human picking up a card must be able to execute it from the card alone, without re-querying the parent spec.

**Three attachment paths:**

| Source of the artifact | Tool | When to use |
|---|---|---|
| KE / mockup / Architecture Design already exists on the parent spec | `okto_pulse_copy_knowledge_to_card` / `okto_pulse_copy_mockups_to_card` / `okto_pulse_copy_architecture_to_card` | Default path. Pass `knowledge_ids` / `screen_ids` / `design_ids` to scope a subset; omit to copy all. |
| KE specific to this task that should NOT live on the spec | `okto_pulse_add_card_knowledge` | Use when the knowledge is task-scoped. |
| Mockup specific to this card | `okto_pulse_add_screen_mockup(board_id, card_id, ..., entity_type="card")` | Use for card-scoped UI deliverables, bug repro screenshots, or per-card layout variants. |

**Mandatory before moving the card to `in_progress`:**

1. Run `okto_pulse_get_task_context(board_id, card_id, include_knowledge=true, include_mockups=true, include_architecture=true)` and inspect what is already attached.
2. For each KE / mockup / Architecture Design the task needs, decide:
   - **Already on the card** → no action.
   - **On the parent spec, relevant to this task** → call the copy tool.
   - **Not yet captured anywhere** → create it with `okto_pulse_add_card_knowledge`.
3. Skip explicitly when the task genuinely needs no artifact — but post a one-line comment justifying the skip.

### Governance Rules (enforced by the system)

1. **Every card must be linked to a spec** — `spec_id` is mandatory in `okto_pulse_create_card`.
2. **Spec status rules for card creation**:
   - `card_type="normal"` → spec must be in `approved`, `in_progress`, or `done`
   - `card_type="test"` → spec must be in `approved`, `validated`, `in_progress`, or `done`
   - `card_type="bug"` → spec must be in `approved`, `in_progress`, or `done`
3. **A spec cannot move to `done` without full test coverage** — every acceptance criterion must have at least one test scenario linked.
4. **A spec cannot move to `done` if it has pending tasks** — all linked non-bug, non-archived cards must be `done` or `cancelled` first.
5. **No card can advance to `started`/`in_progress` unless ALL test scenarios have linked task cards**.
6. **No card can advance unless ALL functional requirements have linked business rules**.
7. **Mandatory card execution pre-flight sequence** — `okto_pulse_get_task_context` → attach applicable artifacts → `okto_pulse_move_card(status="in_progress")` → begin work.

### There Are Three Types of Cards

1. **Implementation cards** (`card_type="normal"`) — implement functional/technical requirements from the spec.
2. **Test cards** (`card_type="test"`, with `test_scenario_ids`) — implement, execute, or validate test scenarios defined in the spec. Key rules:
   - `card_type="test"` **requires** `test_scenario_ids` to be non-empty.
   - The scenario-coverage gate counts **only cards with `card_type="test"`**. A `card_type="normal"` card with `test_scenario_ids` does NOT count toward scenario coverage.
   - Always use `card_type="test"` when the intent is to cover a scenario.
3. **Bug cards** (`card_type="bug"`) — track and fix bugs discovered during or after implementation.

### When Creating Cards from a Spec (MANDATORY ORDER)

1. **Get full task context**: `okto_pulse_get_task_context(board_id, card_id)` — returns the card + spec with all requirements, TRs, BRs, test scenarios, API contracts, KBs, and mockups.
2. **Read test scenarios**: `okto_pulse_list_test_scenarios(board_id, spec_id)`.
3. **Read business rules and API contracts**: `okto_pulse_list_business_rules(board_id, spec_id)` and `okto_pulse_list_api_contracts(board_id, spec_id)`.
4. **Review conclusions of dependencies**: for every card this one will depend on, call `okto_pulse_get_task_conclusions(board_id, dep_card_id)`.
5. **Create test cards FIRST** — one per test scenario, with `card_type="test"`, `test_scenario_ids`, and `spec_id`.
6. **IMMEDIATELY link each test card to its scenario(s)** via `okto_pulse_link_task_to_scenario(board_id, spec_id, scenario_id, card_id)`.
7. **Verify full linkage**: run `okto_pulse_list_test_scenarios` — every scenario must show at least one linked task.
8. **THEN create implementation cards** (`card_type="normal"`) — always pass `spec_id`.
9. **MANDATORY — Copy artifacts into every card**. Use `okto_pulse_copy_mockups_to_card`, `okto_pulse_copy_knowledge_to_card`, `okto_pulse_copy_architecture_to_card`, and `okto_pulse_copy_qa_to_card`.
10. **Write detailed card descriptions** including: what specifically needs to be built, which FRs/TRs/BRs this card addresses, which test scenarios this card should satisfy, which API contracts define the interfaces, and relevant technical constraints.

**Test card naming convention:** Prefix test cards with `[TEST]` to distinguish them.
Example: `[TEST] E2E — Valid OAuth2 token grants access`

### Anti-Patterns — DO NOT Do These

| Anti-pattern | Why it's bad | What to do instead |
|---|---|---|
| One big test card for all scenarios | No granular traceability | Create one test card per scenario or per small group of closely related scenarios |
| Test card without `okto_pulse_link_task_to_scenario` | Scenario shows "no tasks" — no way to know which card validates it | Always call `okto_pulse_link_task_to_scenario` after creating a test card |
| Starting work without `okto_pulse_get_task_context` | Implementing blind = guaranteed drift | ALWAYS call `okto_pulse_get_task_context` with all include flags BEFORE any work |
| Card still `not_started` while writing code | Board is inaccurate | Move to `in_progress` BEFORE first line of code |

## 2.9 Bug Cards — Post-Delivery Bug Tracking

Bug cards track defects discovered after tasks are completed. They enforce a test-first workflow: you MUST create test scenarios and test tasks BEFORE you can start fixing the bug.

**Creating a bug card:**
```
okto_pulse_create_card(
  board_id, title="Login returns 500 with uppercase email",
  spec_id="<spec_id>",           # auto-resolved from origin task
  card_type="bug",
  origin_task_id="<task_id>",    # REQUIRED — the task that has the bug
  severity="critical",           # critical | major | minor
  expected_behavior="Should accept any case email and return 200 with JWT",
  observed_behavior="Returns HTTP 500 when email contains uppercase letters",
  steps_to_reproduce="1. POST /auth/login with email 'User@Email.COM'\n2. Observe 500 error",
  action_plan="Normalize email to lowercase before DB query"
)
```

**Bug card workflow (enforced by the system):**

1. Create bug card (status: not_started)
2. Triage & create test scenarios (status: started) — create NEW test scenario(s) on the spec
3. Create test task & link to bug (still started) — create test card, link: `okto_pulse_update_card(card_id=bug_id, linked_test_task_ids="<test_task_id>")`
4. Move to in_progress (BLOCKED until step 3 is done) — system validates linked test tasks, scenario existence, temporal order
5. Fix the bug (in_progress) — implement the fix, run tests, update scenario statuses
6. Complete (done) — provide conclusion with what was fixed

**If you get an error moving a bug card:** see the "Common Errors and How to Fix Them" section in the error reference.

## 2.11 Task Validation Workflow — Independent Quality Checkpoint

When the **Task Validation Gate** is enabled, cards must pass through an independent validation before moving to `done`.

- Gate is enabled when `validation_config.required == true` for the card
- Applies to `card_type: "normal"` and `card_type: "bug"`
- **Excluded:** `card_type: "test"` — test cards are validated by test scenario pass/fail status, not the gate

### Implementor Workflow

1. Retrieve context — `okto_pulse_get_task_context(board_id, card_id)`. Check `validation_config.required`.
2. **MANDATORY for restarts** — if the card has a failed validation, read `threshold_violations`, `confidence_justification`, `completeness_justification`, `drift_justification`, `general_justification` before changing the implementation.
3. Move to in_progress — before starting work.
4. Implement the task.
5. Link artifacts — attach knowledge bases, mockups, or comments as work progresses.
6. Move to validation — `okto_pulse_move_card(status="validation", conclusion=..., completeness=..., completeness_justification=..., drift=..., drift_justification=...)`.
7. Wait — another agent or human with `card.validation.submit` permission will validate your work.

### Validator Workflow

1. Find cards awaiting validation — `okto_pulse_list_cards_by_status(board_id, status="validation")`
2. Get full context for each card — `okto_pulse_get_task_context(board_id, card_id)`
3. Analyze the work — review implementation against card description and spec requirements
4. Submit validation — `okto_pulse_submit_task_validation(board_id, card_id, ...)` with:
   - `confidence` (0-100) + `confidence_justification`
   - `estimated_completeness` (0-100) + `completeness_justification`
   - `estimated_drift` (0-100) + `drift_justification`
   - `general_justification` + `recommendation` (`"approve"` or `"reject"`)
5. System routes automatically — you do NOT need to move the card.

### Deterministic Thresholds

| Threshold | Default | Rule |
|-----------|---------|------|
| `min_confidence` | 70 | `confidence < min_confidence` → **auto-fail** |
| `min_completeness` | 80 | `estimated_completeness < min_completeness` → **auto-fail** |
| `max_drift` | 50 | `estimated_drift > max_drift` → **auto-fail** |
