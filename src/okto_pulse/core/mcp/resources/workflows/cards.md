---
version: "1.2"
---

# Cards Workflow — Implementation, Bug & Test Execution

### Mandatory delivery handoff

Tasks/bugs record implementation, **not test results**. After an accepted Target
execution receipt and card completion, use `okto_pulse_record_delivery_evidence`
(`implementation`) to bind delivered code to the Spec obligations. Test cards must
execute their linked scenarios, record authenticated passing evidence and complete;
then record `test` with the implementation association IDs actually tested.
One record may cover multiple obligations. Read the current inventory first with
`okto_pulse_get_delivery_evidence`; never invent IDs, hashes or verification flags.
Details and human-only not-applicable handling:
`okto-pulse://reference/code-traceability`, section **Delivery evidence**.

Semantic guideline assessment follows
`okto-pulse://reference/policy-compliance`.

## 2.7b Architecture Design — Structural Artifacts (Summary)

Architecture Design is the first-class place for system structure. See the full reference in `okto-pulse://reference/tool-docs/architecture`. Use `okto_pulse_copy_architecture_to_card` before starting execution (`started`, then `in_progress` for normal cards).

## 2.8 Cards (Tasks)

### Card-Level Artifact Review (MANDATORY; KB attachment advisory)

> **A card must be self-contained for the artifacts its implementation
> requires.** Architecture and Mockup coverage is blocking. Knowledge Base
> context is advisory: copy only KBs that materially help execute or validate
> the card; never attach filler merely to satisfy this review.

**Attachment path:**

| Source of the artifact | Tool | When to use |
|---|---|---|
| Mockup / Architecture Design already exists on the parent spec | `okto_pulse_copy_mockups_to_card` / `okto_pulse_copy_architecture_to_card` | Blocking path when applicable. Pass `screen_ids` / `design_ids` to scope a subset; omit to copy all. |
| Relevant KB already exists on the parent spec | `okto_pulse_replace_card_knowledge_assignments` | Advisory path. Pass `knowledge_ids` to select only context that materially helps the card; omission never blocks completion. |

Card resources are read-only governed snapshots. Do not create, edit, annotate,
import, or delete Knowledge Base, Mockup, or Architecture resources directly on a
card; update the source ideation/refinement/spec resource and then run the
matching copy tool for Mockup/Architecture, or the Knowledge assignment tools, to refresh the card snapshot while preserving the source
identity used by the Resource Gate.

**Copy versus reference:** a governed copy made by the official copy tool is
valid resource coverage; the gate does NOT require a reference instead of that
copy. It matches resource identity/provenance, not the title, HTML, screenshot,
or a mention in the task description. Recreating identical content under an
unrelated ID does not cover the original resource. Two identical-looking mockups
with distinct origins may therefore be two separate coverage obligations.

If a gate reports an uncovered resource although a mockup is visible on a task,
inspect the reported `resource_id`, `unique_resource_id`, and `origin_evidence`
and compare them with the task snapshot's ID and source metadata. Use the
official copy tool for the required source and a non-cancelled task belonging
to that Spec, then re-read full Spec context and retry the readiness check.
Do not invent provenance, replace a valid copy with a textual reference, delete
duplicates, or mark a resource N/A merely to bypass coverage. If the reported
source cannot be selected by the tool, report the exact IDs as a lineage/copy
problem rather than claiming that copying is forbidden.

Read `okto-pulse://reference/knowledge-governance` before deciding that content
belongs in a KB. Creating a Card uses one Knowledge selection contract.
Omitting `knowledge_propagation` creates an `omitted` selection with no inherited
Knowledge and a fresh operation key.

### Selective Knowledge propagation

Select the intended state explicitly when providing an envelope:

| Input on `okto_pulse_create_card` | Meaning |
|---|---|
| `knowledge_propagation` absent | Create with no inherited Knowledge selection. Supply an envelope with a stable key when retry identity is needed. |
| `selection_state="omitted"`, no `mode`, no `knowledge_ids` | Record no inherited Knowledge selection. |
| `selection_state="explicit_empty"`, `mode="drop"`, empty `knowledge_ids` | Authoritative empty selection/drop-all. A non-empty `justification` is required. |
| `selection_state="explicit_ids"`, non-empty `knowledge_ids`, `mode="reference"`, `"snapshot"`, or `"drop"` | Propagate only those stable roots, or explicitly drop the named roots. A non-empty `justification` is required. |

Every envelope has `contract_version=2` and a non-empty,
caller-stable `idempotency_key`. A create accepts `expected_revision` omitted
or `0`; it rejects another value. Repeating the semantically identical request
with the same key returns the original durable result with `replayed=true`.
Never reuse that key for a different card payload, selection, actor, or parent.

For card creation, optional `relevance_links` explain why the selected Knowledge matters. Each item is
`{entity_type, entity_id}` and `entity_type` is exactly one of
`functional_requirement`, `acceptance_criterion`, or `test_scenario`. The
referenced FR, AC, or scenario must belong to the card's linked spec; the whole
operation fails before creation when any source or linkage is invalid.

After creation, use the assignment tools. Replace accepts relevance links in
its `linkage` field:

- `okto_pulse_replace_card_knowledge_assignments` atomically replaces the
  selection with `reference` or `snapshot` assignments.
- `okto_pulse_drop_card_knowledge_assignments` drops named roots; an empty
  `knowledge_ids` list is the explicit drop-all operation.
- `okto_pulse_refresh_card_knowledge_assignments` refreshes snapshot content by
  stable **root Knowledge ID**, never by assignment-row ID.
- `okto_pulse_get_card_knowledge_propagation` reads the technical selection,
  revision, assignments, stale state, and native assignment history.

Replace/drop/refresh require the current `expected_revision`. Read the
technical projection immediately before a mutation when the revision is not
already known. Each successful mutation increments `revision`. Retrying the
same request with the same `idempotency_key` returns the original receipt;
changing the request under that key is an idempotency conflict.

**Mandatory before moving the card to `in_progress`:** Complete these steps
before entering either execution state (`started`/`in_progress`); then follow
the exact transition edge advertised for the concrete card.

1. Run `okto_pulse_get_task_context(board_id, card_id, profile="full", context_scope="gate", include_knowledge=true, include_mockups=true, include_architecture=true, include_qa=true, include_comments=true)` and inspect Resource Gate state, validation/reviewer blockers, and the content manifest. Fetch `profile="detail"` plus its drilldowns when you need the attached bodies.
2. For each Mockup / Architecture Design the task needs, decide:
   - **Already on the card** → no action.
   - **On the parent spec, relevant to this task** → call the copy tool.
   - **Not yet captured anywhere** → add it to the source ideation/refinement/spec first, then call the copy tool.
3. Review effective KBs and copy only the entries that materially improve
   implementation or validation context. Missing or uncovered KBs are advisory
   and never block `entity_completion`, `spec_validation`, or `spec_done`.
4. When a blocking Architecture/Mockup genuinely does not apply, record N/A
   with a real justification. Do not create filler artifacts or mark a KB N/A
   solely to make a gate look complete.

### Governance Rules (enforced by the system)

1. **Every card must be linked to a spec** — `spec_id` is mandatory in `okto_pulse_create_card`.
2. **Spec status rules for card creation** — depend on `card_type`; the per-type matrix is in `okto-pulse://reference/card_types`.
3. **A spec cannot move to `done` without full test coverage** — every acceptance criterion must have at least one test scenario linked.
4. **A spec cannot move to `done` if it has pending tasks** — all linked non-bug, non-archived cards must be `done` or `cancelled` first.
5. **No card can advance to `started`/`in_progress` unless ALL test scenarios have linked task cards**.
6. **No card can advance unless ALL functional requirements have linked business rules**.
7. **Mandatory card execution pre-flight sequence** — `okto_pulse_get_task_context` → attach applicable artifacts → call `okto_pulse_get_allowed_transitions` → follow the advertised edge(s) → begin work. A normal card follows `not_started → started → in_progress`. A test/bug card may use direct `not_started → in_progress` only when the transition tool advertises that edge for the concrete card.

### There Are Three Types of Cards

1. **Implementation cards** (`card_type="normal"`) — implement functional/technical requirements from the spec.
2. **Test cards** (`card_type="test"`, requires non-empty `test_scenario_ids`) — cover test scenarios; the scenario-coverage gate counts **only** `card_type="test"` cards.
3. **Bug cards** (`card_type="bug"`) — track and fix bugs discovered during or after implementation.

Full per-type rules (spec-status matrix, `max_scenarios_per_card` cap, scenario evidence, gate interactions): `okto-pulse://reference/card_types`.

### Quality evidence while executing a card

The parent Spec's opt-in PageEnvelope may expose only lean
`quality_summaries`. When a task needs the actual rationale, read the current
receipt or page findings through the dedicated Quality tools and inspect
currentness; never treat a parent summary or newest head as a complete body.
The card executes linked test scenarios and implementation work—it does not
author a separate assessment or checklist template. A Test Scenario's
`manual_checklist_ref` belongs to its test-evidence method; it is not a curated
A3 result. Checklist accepts only its complete native result. See
`okto-pulse://reference/quality-assessments`.

### When Creating Cards from a Spec (MANDATORY ORDER)

1. **Get full spec context**: `okto_pulse_get_spec_context(board_id, spec_id, profile="full")` — returns the spec with all requirements, TRs, BRs, test scenarios, API contracts, KBs, and mockups. (`okto_pulse_get_task_context` needs an existing `card_id` — it belongs to the execution pre-flight in §2.8, not here.)
2. **Read test scenarios**: `okto_pulse_list_test_scenarios(board_id, spec_id)`.
3. **Read business rules and API contracts**: `okto_pulse_list_business_rules(board_id, spec_id)` and `okto_pulse_list_api_contracts(board_id, spec_id)`.
4. **Review conclusions of dependencies**: for every card this one will depend on, call `okto_pulse_get_task_conclusions(board_id, dep_card_id)`.
5. **Create test cards FIRST** — one per test scenario or per small group within `max_scenarios_per_card`, with `card_type="test"`, `test_scenario_ids`, and `spec_id`.
6. **IMMEDIATELY link each test card to its scenario(s)** via `okto_pulse_link_task(target_type="scenario", board_id, spec_id, target_id=<scenario_id>, card_id)`.
7. **Verify full linkage**: run `okto_pulse_list_test_scenarios` — every scenario must show at least one linked task.
8. **THEN create implementation cards** (`card_type="normal"`) — always pass `spec_id`.
9. **MANDATORY — Resolve blocking artifacts and review advisory Knowledge for
   every card.** Use `okto_pulse_copy_mockups_to_card`,
   `okto_pulse_copy_architecture_to_card`, and `okto_pulse_copy_qa_to_card`
   where applicable. For materially relevant Knowledge, either supply the
   `knowledge_propagation` envelope during card creation or use
   `okto_pulse_replace_card_knowledge_assignments` with mode, justification,
   expected revision and idempotency key. A KB omission is advisory and does
   not block the card.
10. **Write detailed card descriptions** including: what specifically needs to be built, which FRs/TRs/BRs this card addresses, which test scenarios this card should satisfy, which API contracts define the interfaces, and relevant technical constraints.

**Test card naming convention:** Prefix test cards with `[TEST]` to distinguish them.
Example: `[TEST] E2E — Valid OAuth2 token grants access`

### Anti-Patterns — DO NOT Do These

| Anti-pattern | Why it's bad | What to do instead |
|---|---|---|
| One big test card for all scenarios | No granular traceability | Create one test card per scenario or per small group of closely related scenarios |
| Test card without `okto_pulse_link_task(target_type="scenario", ...)` | Scenario shows "no tasks" — no way to know which card validates it | Always call `okto_pulse_link_task(target_type="scenario", ...)` after creating a test card |
| Starting work without `okto_pulse_get_task_context` | Implementing blind = guaranteed drift | ALWAYS call `okto_pulse_get_task_context` with `profile="full", context_scope="gate"` and all include flags BEFORE any work; use bounded detail/drilldowns for bodies |
| Card still `not_started` while writing code | Board is inaccurate | Query `okto_pulse_get_allowed_transitions`; move normal cards to `started` and then `in_progress` (or use a directly advertised test/bug edge) BEFORE first line of code |
| Expecting an omitted selection to copy every KB | Omission selects no inherited Knowledge | Send the stable Knowledge IDs and intended mode explicitly |
| Refreshing by assignment ID | Assignment rows are temporal and may be superseded | Pass stable root Knowledge IDs to `okto_pulse_refresh_card_knowledge_assignments` |
| Reusing an idempotency key after changing payload or selection | Replay identity no longer represents the same semantic request | Reuse a key only for an exact retry; generate a new key for new intent |

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
     1. **Create/associate the amendment** — `okto_pulse_create_amendment_revision` (binds to the bug's OWN **content-locked** spec — `done`/`validated`, OR `in_progress` still content-locked by an active passed validation (`current_validation_id` → outcome=success) — and starts as `draft`; an `in_progress` spec that is still editable is rejected, edit it directly instead), or `okto_pulse_associate_amendment_revision_artifacts` to attach regression artifacts onto an existing revision for this bug.
     2. **Complete the lineage + promote the revision** — declare the exact origin/affected-task membership (authoritative from the bug, not invented by the amendment), revision spec, and the declared regression scenario/test-task artifacts, then advance the revision with `okto_pulse_transition_amendment_revision` to `lineage_state=complete` and a non-blocking `status` (`approved`/`done`). Fail-closed: `lineage_state=complete` needs the declared scenario + test-task artifacts and the bug's origin task; `approved`/`done` need complete lineage; a `cancelled`/`superseded` revision is terminal (open a new revision). This promotion **never** confirms coverage — that is the validator's step below.
     3. **Register re-executable evidence** — the regression test card's scenario is `passed`/`automated` with a direct test pointer (`test_file_path`+`test_function`) or an explicit replayable `evidence_class` such as `mcp_replay_manifest` plus `expected_output_snapshot`.
     4. **Validator confirms coverage** — the validator calls `okto_pulse_confirm_amendment_coverage`, the ONLY writer of the non-forgeable `coverage_confirmed` signal (re-executable evidence is necessary but NOT sufficient — a real validator attestation bound to this amendment + artifact is required). BEFORE persisting, the tool runs a gate-consumability preflight: it applies the SAME eligibility predicate the bug regression gate uses, so a successful confirmation implies the attestation is consumable for this `(amendment_id, regression_test_task_id, regression_scenario_id)`. **Anti-pattern (fails closed):** reusing a same-spec happy-path/AC scenario that is NOT linked to the bug's origin/affected task and then calling confirm — the scenario routes through Path A and is `unrelated_scenario`, so the writer rejects it with `coverage_not_gate_consumable` and persists nothing. An amendment declaration does NOT convert an `unrelated_scenario` into valid Path B coverage; use a same-spec scenario tied to the bug lineage (Path A) or a genuinely cross-spec Path B artifact.
     5. **Gate allows the bug** — only after step 4. Until then the bug is `coverage_pending`: lineage may be eligible but the bug is NOT closure-ready. Inspect at any time with `okto_pulse_list_amendment_revisions` / `okto_pulse_get_amendment_revision`.
3. Create test task & link to bug (still started) — create a new post-bug `card_type="test"` card with the eligible `test_scenario_ids`, then link: `okto_pulse_update_card(card_id=bug_id, linked_test_task_ids="<test_task_id>")`.
4. Move to in_progress (BLOCKED until step 3 is done) — system validates linked test tasks, scenario existence, same-spec ownership, eligibility by origin/affected-task lineage, and that the test TASK (card) was created after the bug. The "after the bug" temporal applies to the test TASK, not the scenario — a pre-existing eligible scenario is valid regression coverage.
5. Fix the bug (in_progress) — implement the fix, run tests, update scenario statuses. On a `validated`/`done` spec, `okto_pulse_update_test_scenario_status` is still allowed for a scenario already linked to an executable test card, because this records operational evidence instead of editing semantic spec content.
6. Complete (done) — provide conclusion with what was fixed

> **Canonical sources (forward pointers):** eligible-scenario reuse and the Path B amendment-revision tools are defined in `reference/tool-docs/card.md`; content lock behavior is covered by **SpecLockedError** and the Path B error codes (`missing_amendment_revision`, `coverage_pending`, `gate_bypass_not_allowed`, …) in `reference/errors.md`. The steps above intentionally separate Path A (same-spec traceability reuse), Path B (amendment lineage + re-executable evidence + validator coverage confirmation).

**If you get an error moving a bug card:** see the "Common Errors and How to Fix Them" section in the error reference.

### Authored Learnings for a Bug

`board.settings.bug_learning_closeout` is human-controlled in Board Settings and
default Board configuration. Omission resolves to `advisory`: no Learning is
required for completion. In `blocking`, submit a valid durable capture with the
report or select one for task validation. Empty queues, historical presence and
provider failures are not proof. This policy does not replace legacy cognitive
holds or other completion gates, and `skip_cognitive_consolidation` cannot waive
it. Transition previews expose `valid_durable_learning_capture` as request input;
they do not materialize the graph or approve a capture.

The execution-report dialog can save a new Learning together with the report,
including an optional final Delivery batch. For MCP `okto_pulse_move_card`, use
`learning_submission` with a stable `capture_id`, the initial context's
`expected_source_digest`/`expected_source_version`, authored `content`, `context`,
`applicability`, and authenticated `scenario_ids`. All capture permissions and
`card.conclusion.write` are required in addition to the transition authority.
The server admits the exact new report into the capture basis and persists the
compound operation atomically. Keep the complete request unchanged for a retry;
changed content is a new intent. Retrying after a later reopen does not close
the Bug again or make the old lesson applicable to its new correction.

Record a useful lesson with its context, applicability and authenticated scenario
evidence. In the Bug modal, open **Validation → Execution report → Learnings**.
For MCP, read `okto_pulse_kg_get_learning_capture_context`, then submit the authored
content with `okto_pulse_kg_create_learning_capture`, using the returned source
digest/version and explicit scenario IDs. Keep the capture ID for an exact retry.
If the basis changes, preserve the text, refresh the evidence and review applicability
before submitting a new intent. Do not invent evidence or copy a previous lesson as
automatically applicable to a reopened correction.

Use `okto_pulse_kg_list_learning_captures` or **Saved Learnings** to resume from
durable authored history. Reading history requires the existing Learning-query
permission in addition to source access; creating content requires the KG creation
permissions. Historical presence is not execution approval, current applicability
or canonical graph materialization. A successful capture is acknowledged as pending
materialization and does not itself close the Bug or replace its existing completion
gates. No manual graph session is required for capture authorship.

When submitting the Bug's task validation, optionally select a current capture
with `learning_capture={learning_id, generation, fingerprint}`. The validation
writer rechecks its source and authenticated evidence and records the linkage
only if the existing completion gates allow Done. Source reads and the existing
Learning-query permission are required. This does not approve implementation or
materialize the graph. A changed execution report requires a capture on its new
basis; a legacy card without its executor report cannot attach an older capture
to a conclusion inferred from the review.

### Historical bug closure via Path B — operational checklist

Reprocessing or closing a bug that surfaced AFTER its spec was locked
(`done`/`validated`) runs through the SAME Path B gate as any other bug — there
is **no administrative shortcut and no `skip`/`override`/`force` path**. The
canonical sequence is **§2.9 step 2, Path B, steps 1-5 above** — follow it end to
end; it is re-executable by an agent and proven by `tests/test_path_b_e2e.py`.
The historical case adds only the differences below:

- **Pre-condition — authoritative lineage:** you have the bug's authoritative `origin_task_id` (and any real `affected_task_ids`). Membership is authoritative from the bug, never invented by the amendment. (The content-lock rule for the spec is in Path B step 1; an `in_progress` spec that is still editable needs no amendment — edit it directly.)
- **Integrity:** affected mutations remain subject to their existing KG integrity and evidence gates. Health reports component availability; it is not a mandatory inspection/repair ceremony before every product action.
- **FRESH evidence:** the re-executable evidence in Path B step 3 must come from a GREEN run and include a replayable `test_file_path` + `test_function` (or an explicitly supported replay manifest). Stale or reused evidence does not count.
- **Post-closure verification:** only after the validator calls `okto_pulse_confirm_amendment_coverage` for the formal `AmendmentHotfixRevision` and the attestation reports `coverage_confirmed` may you move the bug forward (`move_card`) and close it with a conclusion. Confirm it travelled `coverage_pending` → `path_b_ready` (`okto_pulse_list_amendment_revisions` / `okto_pulse_get_amendment_revision`).
- **Projection invariants:** the amendment materializes in the WORKING partition while `draft`/incomplete and becomes canonical ONLY at `done` + complete lineage — verify there is no premature canonical leak. The runtime preserves partition consistency and surfaces failures; the amendment must not introduce canonical debt or DLQ entries. An additional health call or maintenance step is not required to complete the product action.
- **Fail-closed — do NOT close while any of these hold:** the bug is `coverage_pending`; the amendment is `draft`/`review`/`cancelled`/`superseded` or its lineage incomplete (`blocked_amendment_status` / `incomplete_amendment_lineage`); the evidence is stale, reused, or missing a replayable class/pointer with the required `expected_output_snapshot`; the scenario is cross-spec/unrelated with no formal amendment backing it (`missing_amendment_revision` / `unrelated_scenario` / `cross_spec_scenario`); `okto_pulse_confirm_amendment_coverage` returned `coverage_not_gate_consumable` (fix the cause — never retry with a bypass); or KG health still shows unresolved related debt/DLQ.

There is no way to close a historical bug without validator-confirmed coverage and a clean KG. The error codes are in `reference/errors.md`, the amendment tools in `reference/tool-docs/card.md`, and the KG health contract in `reference/kg-health.md`.

## 2.11 Task Validation Workflow — Independent Quality Checkpoint

When the **Task Validation Gate** is enabled, cards must pass through an independent validation before moving to `done`.

- Gate is enabled when `validation_config.required == true` for the card
- Applies to `card_type: "normal"` and `card_type: "bug"`
- **Excluded:** `card_type: "test"` — test cards are validated by test scenario pass/fail status, not the gate

### Implementor Workflow

1. Retrieve context — `okto_pulse_get_task_context(board_id, card_id, profile="full", context_scope="gate")`. Check `validation_config.required`.
2. **MANDATORY for rework** — a failed admitted completion is status `rejected`. Read `current_rejection_summary` plus the referenced validation's `threshold_violations`, `confidence_justification`, `completeness_justification`, `drift_justification`, and `general_justification`. Editing implementation while Rejected is forbidden because it would erase the human handoff boundary. In blocking Code Traceability mode, the rejection's version bump can make an existing Target resolution outdated: investigation preflight and Target resolution renewal are the only allowed technical writes while Rejected; Target create/update, execution receipts, evidence dispositions, and implementation content remain frozen.
3. Start execution — query `okto_pulse_get_allowed_transitions`; from `not_started`, a normal card moves to `started` and then `in_progress`. A Rejected card has exactly one exit: move it to `in_progress` before changing implementation. That clears only the Current pointer; causal history remains append-only.
4. Implement the task.
5. Link artifacts — attach knowledge bases, mockups, or comments as work progresses.
6. Move to validation — `okto_pulse_move_card(status="validation", conclusion=..., completeness=..., completeness_justification=..., drift=..., drift_justification=...)`.
7. The card is ready for validation only after the move succeeds. `okto_pulse_submit_task_validation` requires status `validation` and fails closed otherwise — never request or hand off validation while the card is still `in_progress`.

### Validator Workflow

1. Confirm the card is in `validation` before analyzing. Find queued cards with `okto_pulse_list_cards_by_status(board_id, status="validation")`; if a candidate is not in that status, the implementor has not completed the handoff and validation must not start.
2. Get full gate context for each card — `okto_pulse_get_task_context(board_id, card_id, profile="full", context_scope="gate")`. Inspect `reviewer_separation` before acting; it is evaluated for the current agent against the card creator, assignee, and executor report author.
3. Analyze the work — review implementation against card description and spec requirements. When `reviewer_separation.mode="enforce"` and `allowed=false`, hand the validation to an independent principal instead of retrying.
4. Submit validation — `okto_pulse_submit_task_validation(board_id, card_id, expected_subject_version, idempotency_key, ...)` with:
   - `confidence` (0-100) + `confidence_justification`
   - `estimated_completeness` (0-100) + `completeness_justification`
   - `estimated_drift` (0-100) + `drift_justification`
   - `general_justification` + `recommendation` (`"approve"` or `"reject"`)
5. System routes automatically — `completion_outcome=completed` moves to `done`; `completion_outcome=rejected` moves Normal/Bug to `rejected` with one sealed Current cause. Test cards remain on their separate scenario workflow. Infrastructure/authorization/concurrency errors persist neither validation nor rejection.

### Reviewer Separation Modes

`board.settings.reviewer_separation_mode` governs task validation:

| Mode | Task-validation behavior |
|------|--------------------------|
| `enforce` | Creator, assignee, or executor conflicts fail closed with `reviewer_separation_required` and remediation `request_independent_task_validator`. No validation or status change is persisted. |
| `warn` | Submission continues; `reviewer_separation.warning=true`, conflicts, and source are persisted in the validation and returned to the caller. |
| `off` | Submission continues transparently, while the complete decision (including conflicts) is still persisted and returned. |

New boards (including the no-active-template fallback) and new default-board template versions materialize `enforce` unless `warn`/`off` is explicitly selected. An omitted setting uses `enforce` with `source="board_default"`. Invalid policy values are refused. Community bootstrap Boards explicitly select `off`.

### Deterministic Thresholds

| Threshold | Default | Rule |
|-----------|---------|------|
| `min_confidence` | 70 | `confidence < min_confidence` → **auto-fail** |
| `min_completeness` | 80 | `estimated_completeness < min_completeness` → **auto-fail** |
| `max_drift` | 50 | `estimated_drift > max_drift` → **auto-fail** |

## 2.12 Agent-mediated implementation targets

When Code Traceability is enabled, use this order before implementation:

1. Read `okto_pulse_get_task_context(profile="full", context_scope="gate")`.
2. Review the inherited `delivery_context`, effective `source_context`,
   relevant Code Evidence, and dispositions. Counts describe the complete
   effective set even when item drilldowns are bounded; read each item's explicit authored context.
3. Start the initial Card preflight request. The authenticated external agent
   checks real access and capabilities and investigates the source in its own
   environment; Pulse Core and Pulse Community never open or search it.
4. Submit a contextual V2 receipt, then create or adjust semantic Targets
   against its source head. Direct Card Evidence is AS-IS only. A path or
   symbol the Card plans to create is TO-BE Target intent, not Evidence.
5. Refetch the Card and Target revisions. Start and submit a new Target-bound
   Card preflight whose selector scope includes those exact Target revisions;
   submit a Resolution for every required Target from this second receipt.
6. Review overlaps. Create a dependency or a bounded acknowledgement when the
   policy permits it.
7. Read `okto_pulse_get_allowed_transitions` and follow only an advertised
   edge before starting execution.
8. After implementation, submit an Execution Disposition for every active
   required Target before `validation` or `done`.

The UI's **Technical Anchors** are these Implementation Targets. Create one
per independently resolvable action area with a concrete intent, role,
path/symbol hint, stable Spec links, and baseline Evidence when available. A
Target is mutable intent, not proof; the agent must still submit its current
Resolution and post-work Execution Disposition. In `advisory`, missing records
do not block the move, but Pulse cannot reconstruct them; later source,
selector, Target, or dependency drift can force the full investigation to be
repeated. Never resolve a newly created Target with the initial receipt whose
selector scope predates that Target.

For Greenfield/Hybrid work, an existing starter/base can be Evidence only as
`existing_scaffold` with an explicit `interpretation_limit`; source consulted
only as a pattern is `reference_pattern`. Neither proves the requested behavior
already exists. Every Evidence item requires authored context and defensible
baseline provenance. Earlier formats are rejected without conversion.

Run a new external preflight, re-evaluate the Targets, and submit a new receipt
when a dependency finishes, the observed workspace fingerprint changes, a
Target becomes stale, or the agent discovers additional code. A prior overlap
acknowledgement or resolution may become outdated when those identities change.
There is no `decoupled_mode` and no fallback repository reader in Community;
an unavailable agent capability produces a typed blocker or an explicit human
waiver according to board policy.

Canonical protocol: `okto-pulse://reference/code-traceability`.
