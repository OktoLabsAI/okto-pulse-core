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

Bug cards track defects discovered after tasks are completed. They enforce a test-first workflow: you MUST create a fresh regression test card before you can start fixing the bug. The test card may reuse an existing scenario only when that scenario is eligible by lineage.

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
2. Triage the regression path (status: started):

   - **Path A — reuse eligible existing scenario:** use `okto_pulse_resolve_bug_regression_scenarios` or the REST candidate preview to find scenarios on the bug spec that are linked to the bug `origin_task_id` or explicitly supplied `affected_task_ids`. Reuse only those eligible scenarios. Linking an existing eligible scenario is a traceability-only update: leave validated spec content unchanged and create a fresh post-bug test card that references the eligible scenario.
   - **Path B — amendment lineage (the semantic gap remediation for cross-spec regression evidence):** if no eligible same-spec scenario exists, the candidate is unrelated/cross-spec, or expected behavior changed, remediate through a formal `AmendmentHotfixRevision`. NEVER satisfy the gate by linking a same-spec scenario that is not linked to the origin/affected tasks, and there is **no skip/override** — the gate is only remediated, never bypassed. Executable sequence:
     1. **Create/associate the amendment** — `okto_pulse_create_amendment_revision` (binds to the bug's OWN `done`/`validated` (locked) spec and starts as `draft`), or `okto_pulse_associate_amendment_revision_artifacts` to attach regression artifacts onto an existing revision for this bug.
     2. **Complete the lineage + promote the revision** — declare the exact origin/affected-task membership (authoritative from the bug, not invented by the amendment), revision spec, and the declared regression scenario/test-task artifacts, then advance the revision with `okto_pulse_transition_amendment_revision` to `lineage_state=complete` and a non-blocking `status` (`approved`/`done`). Fail-closed: `lineage_state=complete` needs the declared scenario + test-task artifacts and the bug's origin task; `approved`/`done` need complete lineage; a `cancelled`/`superseded` revision is terminal (open a new revision). This promotion **never** confirms coverage — that is the validator's step below.
     3. **Register re-executable evidence** — the regression test card's scenario is `passed`/`automated` with a direct test pointer (`test_file_path`+`test_function`) or an explicit replayable `evidence_class` such as `mcp_replay_manifest` plus `expected_output_snapshot`.
     4. **Validator confirms coverage** — the validator calls `okto_pulse_confirm_amendment_coverage`, the ONLY writer of the non-forgeable `coverage_confirmed` signal (re-executable evidence is necessary but NOT sufficient — a real validator attestation bound to this amendment + artifact is required).
     5. **Gate allows the bug** — only after step 4. Until then the bug is `coverage_pending`: lineage may be eligible but the bug is NOT closure-ready. Inspect at any time with `okto_pulse_list_amendment_revisions` / `okto_pulse_get_amendment_revision`.
   - **Path C — hotfix lane (execution only):** if the spec is `done` or the origin sprint is closed, assign the bug and its regression test card to an active `lane_type="hotfix"` sprint. Path C is an execution lane only — it does **NOT** replace Path B amendment lineage. Keep the original closed delivery sprint unchanged.
3. Create test task & link to bug (still started) — create a new post-bug `card_type="test"` card with the eligible `test_scenario_ids`, then link: `okto_pulse_update_card(card_id=bug_id, linked_test_task_ids="<test_task_id>")`.
4. Move to in_progress (BLOCKED until step 3 is done) — system validates linked test tasks, scenario existence, same-spec ownership, eligibility by origin/affected-task lineage, and that the test TASK (card) was created after the bug. The "after the bug" temporal applies to the test TASK, not the scenario — a pre-existing eligible scenario is valid regression coverage.
5. Fix the bug (in_progress) — implement the fix, run tests, update scenario statuses. On a `validated`/`done` spec, `okto_pulse_update_test_scenario_status` is still allowed for a scenario already linked to an executable test card, because this records operational evidence instead of editing semantic spec content.
6. Complete (done) — provide conclusion with what was fixed

> **Canonical sources (forward pointers):** eligible-scenario reuse and the Path B amendment-revision tools are defined in `reference/tool-docs/card.md`; content lock behavior is covered by **SpecLockedError** and the Path B error codes (`missing_amendment_revision`, `coverage_pending`, `gate_bypass_not_allowed`, …) in `reference/errors.md`. The steps above intentionally separate Path A (same-spec traceability reuse), Path B (amendment lineage + re-executable evidence + validator coverage confirmation), and Path C (hotfix execution lane — never a substitute for Path B lineage).

**If you get an error moving a bug card:** see the "Common Errors and How to Fix Them" section in the error reference.

### Historical bug closure via Path B — operational checklist

Reprocessing or closing a bug that surfaced AFTER its spec was locked
(`done`/`validated`) runs through the SAME Path B gate as any other bug — there
is **no administrative shortcut and no `skip`/`override`/`force` path**. Work the
checklist below; every item must be green before the bug closes. It is
re-executable by an agent end to end and is proven by `tests/test_path_b_e2e.py`.

**Pre-conditions**

- [ ] The bug's spec is `done`/`validated` (locked). If it is still `in_progress`, edit the spec directly — no amendment is needed.
- [ ] You have the bug's authoritative `origin_task_id` (and any real `affected_task_ids`). Membership is authoritative from the bug, never invented by the amendment.
- [ ] KG health is clean for the board BEFORE you start: `okto_pulse_kg_health` reports no related canonical debt and no related dead-letter (DLQ) entries (cross-check `okto_pulse_kg_canonical_debt_list` and `okto_pulse_kg_dead_letter_list`). Resolve any related debt/DLQ first.

**Closure steps (all via Path B, re-executable)**

1. Create or associate a formal `AmendmentHotfixRevision` for the bug — `okto_pulse_create_amendment_revision` (binds to the bug's own locked spec and starts as `draft`) or `okto_pulse_associate_amendment_revision_artifacts` onto an existing revision for this bug.
2. Complete the lineage: exact origin/affected-task membership, revision spec, and the declared regression scenario plus a post-bug regression test task.
3. Register FRESH re-executable evidence: the regression test card's scenario is `passed`/`automated` with `test_file_path`+`test_function`, or an explicit replayable `evidence_class` such as `mcp_replay_manifest` plus `expected_output_snapshot`, from a GREEN run. Stale or reused evidence does not count.
4. The validator confirms coverage — `okto_pulse_confirm_amendment_coverage`, the ONLY writer of the non-forgeable `coverage_confirmed` signal. Re-executable evidence is necessary but NOT sufficient without a real validator attestation bound to this amendment + artifact.
5. Only now move the bug forward (`move_card`) and close it with a conclusion. Confirm it travelled `coverage_pending` → `path_b_ready` (`okto_pulse_list_amendment_revisions` / `okto_pulse_get_amendment_revision`).

**KG checkpoints (part of Path B regression — not optional)**

- The amendment materializes in the WORKING partition while `draft`/incomplete and becomes canonical ONLY at `done` + complete lineage — verify there is no premature canonical leak.
- Re-run `okto_pulse_kg_health` after closure: no NEW canonical debt and no NEW DLQ entry attributable to this bug/amendment. If a rebuild ran, the amendment partition must reconcile (no `MATERIALIZED_LAYER_MISMATCH`).

**Do NOT close the bug while any of these hold (fail-closed):**

- The bug is `coverage_pending`: lineage may be eligible but the validator has not confirmed coverage.
- The amendment is `draft`/`review`/`cancelled`/`superseded`, or its lineage is incomplete (`blocked_amendment_status` / `incomplete_amendment_lineage`).
- The regression evidence is reused, stale, or missing a replayable class/pointer such as `test_file_path`+`test_function`, `replay_command`, `mcp_replay_manifest`, or `manual_checklist_ref` with the required `expected_output_snapshot`.
- The candidate scenario is cross-spec or unrelated with no formal amendment backing it (`missing_amendment_revision` / `unrelated_scenario` / `cross_spec_scenario`).
- A hotfix lane (Path C) is being treated as a substitute for amendment lineage — the lane only unblocks execution and never replaces Path B.
- KG health still shows related canonical debt or DLQ that you have not resolved.

There is no way to close a historical bug without validator-confirmed coverage and a clean KG. The error codes are in `reference/errors.md`, the amendment tools in `reference/tool-docs/card.md`, and the KG health contract in `reference/kg-health.md`.

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
