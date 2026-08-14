---
version: "1.1"
---

# Spec Validation Gate & Evaluation Gates

## Spec Status Transitions

Transitions table: see `okto-pulse://reference/transitions` (single source). Note the `in_progress` → `done` gate: all linked non-bug, non-archived cards must be `done` or `cancelled`, and when the spec has sprints, all sprints must be `closed` or `cancelled` (minimum 1 closed — see `okto-pulse://workflows/sprints`).

### Spec edition versus technical revision

Spec projections expose both counters:

- `edition` is the human-facing lifecycle counter shown as `vN`. Creation
  starts at `1`; it advances only when a successful lifecycle move enters
  `draft` from a non-draft status.
- `version` is the technical revision used for optimistic concurrency and KG
  source revision. Content and structured-entity writes may advance it without
  changing `edition`; those changes do not start a new human validation cycle.

Always pass `version`, never `edition`, to fields such as
`expected_spec_version` or `spec_version`. Reopening a terminal Spec may
advance both counters independently. Restoring an archived Spec is not a new
edition.

## Spec Validation Gate — `okto_pulse_submit_spec_validation`

When the board has `require_spec_validation=true`, advancing from `approved` to `validated` is gated.

**The canonical flow:**
1. Populate the spec in `draft` → move through `review` → `approved`.
2. Iterate coverage until ALL deterministic gates are green: AC coverage 100%, FR→BR coverage 100%, scenario→task linkage 100%, contract→task linkage 100%, TR→task linkage 100%.
3. Call `okto_pulse_submit_spec_validation` with the optimistic fences and all
   five canonical scores (`confidence`, `clarity`, `assertiveness`,
   `decidability`, `ambiguity`), one justification per score, optional
   metric-tagged pinpoints, and `recommendation=approve|reject`.
4. The gate runs coverage checks first. If any fail, you get a coverage error — fix the gap and retry.
5. If coverage passes, the gate computes `outcome` atomically:
   - `outcome=failed` if ANY threshold is violated OR `recommendation=reject`
   - `outcome=success` ONLY if all thresholds pass AND `recommendation=approve`
6. On `success`, the spec is atomically promoted to `validated` AND enters **content lock**.

**Thresholds (defaults 70/80/80/80/30):**

All scores are 0-100 integers, not 1-5. A 1-5-style score is treated literally
as 1/100 through 5/100 and will usually fail the configured thresholds.

| Dimension | Direction | Default threshold |
|---|---|---|
| `confidence` | Higher is better (min) | 70 |
| `clarity` | Higher is better (min) | 80 |
| `assertiveness` | Higher is better (min) | 80 |
| `decidability` | Higher is better (min) | 80 |
| `ambiguity` | LOWER is better (max) | 30 |

Thresholds decide the gate; they are not scoring anchors. Before evaluating,
read the effective values from `okto_pulse_get_board(board_id).settings` under
`min_spec_confidence`, `min_spec_clarity`, `min_spec_assertiveness`,
`min_spec_decidability`, and `max_spec_ambiguity`. Do not assume the defaults
when the board exposes configured values. Scores never compensate for one
another: a 95 in one dimension cannot cancel a failing dimension.

### Canonical Spec Validation scoring rubric

This is the single source of truth for translating evidence into the five
scores. Select the band first from the observed evidence. Then choose a value
inside that band only to express the extent and severity of the conditions in
that band. Never start from the configured threshold and work backwards toward
a passing number.

#### Universal anchors for higher-is-better dimensions

Use these bands for `confidence`, `clarity`, `assertiveness`, and
`decidability`; the dimension-specific anchors below determine what evidence
counts.

| Score | Meaning |
|---|---|
| `0-19` | Absent, fundamentally contradictory, or not assessable from the inspected evidence. |
| `20-39` | Systemic failure: most material areas do not satisfy the dimension. |
| `40-59` | Partially useful, but material gaps prevent reliable validation or execution. |
| `60-69` | Broad progress, with at least one blocking gap or an important evidence limitation. |
| `70-79` | Defensible and mostly adequate, but at least one material weakness remains. This band can satisfy the default Confidence threshold, but not the default `80` content thresholds. |
| `80-89` | Gate-ready: all material areas satisfy the dimension; only localized, non-blocking improvements remain. |
| `90-99` | Exemplary, precise, and comprehensively evidenced across the whole current Spec. |
| `100` | Exceptional and residue-free for this dimension. Do not use when any relevant weakness, inaccessible evidence, or unresolved question is known. |

Do not interpret adjacent numbers as false precision. For example, use `82`
instead of `88` when both are gate-ready but several minor residues remain;
use `88` when the evidence is almost exemplary. The justification must make
that choice reproducible.

#### `confidence` — reliability of the evaluation

**Question:** How reliable is this evaluator's assessment of the whole current
Spec edition?

Confidence describes the inspection, not the quality of the Spec. A deeply
flawed Spec may receive `confidence=95` when the rejection is supported by a
complete, current, traceable review. Conversely, a good-looking Spec must not
receive high confidence when important sections were not inspected.

| Score | Evidence anchor |
|---|---|
| `0-19` | Current Spec identity/edition is unknown, or the evaluator cannot establish what was assessed. |
| `20-39` | Only fragments or summaries were inspected; conclusions are mostly assumptions. |
| `40-59` | The main narrative and some requirements were inspected, but several material classes are missing. |
| `60-69` | Coverage is broad, but at least one material class, artifact, or conflicting fact remains uninspected or inaccessible. **This is the maximum when any material section was not inspected.** |
| `70-79` | Every material class was inspected for the current edition, with minor evidence limitations disclosed. |
| `80-89` | Full current context was inspected and cross-checked across requirements, criteria, rules, contracts, scenarios, Q&A, decisions, dependencies, and applicable artifacts. |
| `90-100` | Every conclusion is traceable to concrete current evidence or a stable anchor, conflicts were actively checked, and no relevant evidence is inaccessible. |

Required evidence includes the full current Spec context and every material
section applicable to that Spec. If edition/version cannot be confirmed, do
not submit. Reading only `description` and FRs caps confidence at `69`, even if
those fragments look excellent.

#### `clarity` — understandable intent without hidden context

**Question:** Can a reader understand the problem, intended solution, scope,
actors, terms, flows, states, and error behavior without relying on unwritten
shared context?

| Score | Evidence anchor |
|---|---|
| `0-19` | The core problem or intended outcome is absent or contradictory. |
| `20-39` | Most meaning depends on implicit context, unexplained terminology, or missing actors/scope. |
| `40-59` | The general intent is understandable, but important flows, states, terms, or boundaries must be inferred. |
| `60-69` | The happy path is clear, while multiple material edge/error/scope details remain unclear. |
| `70-79` | The Spec is mostly self-contained; a small number of localized material clarifications remain. |
| `80-89` | Problem, solution, requirements, terminology, flows, boundaries, and errors are self-contained and mutually consistent; only editorial improvements remain. |
| `90-100` | Complex behavior, states, failure modes, and domain terms are explained precisely and economically, with no material hidden context. |

Do not reduce Clarity merely because a requirement lacks a number when its
meaning is still unmistakable. Missing observable criteria belongs to
Assertiveness; missing implementation choices belongs to Decidability.

#### `assertiveness` — objective pass/fail verification

**Question:** Does every normative statement define an observable and
testable result, including units, limits, tolerances, and failure behavior when
they are relevant?

| Score | Evidence anchor |
|---|---|
| `0-19` | The text is almost entirely aspirational and cannot produce a pass/fail decision. |
| `20-39` | Most normative claims use subjective words such as “fast”, “appropriate”, “reasonable”, “if needed”, or “high” without objective criteria. |
| `40-59` | Some outcomes are testable, but key requirements still have no observable acceptance condition. |
| `60-69` | The happy path is testable; material boundaries, errors, or non-functional expectations remain subjective. |
| `70-79` | Most statements are testable, but one localized material group lacks a concrete oracle, unit, bound, or tolerance. |
| `80-89` | Every material normative statement has an observable oracle and measurable criteria where applicable; only minor test-detail improvements remain. |
| `90-100` | Verification is deterministic across happy paths, boundaries, failures, tolerances, and observability signals. |

Do not ask here whether an engineer knows which architecture to choose. That is
Decidability. Assertiveness asks whether a reviewer can prove pass or fail.

#### `decidability` — sufficient direction for material choices

**Question:** Does the Spec provide enough information to choose the material
implementation and operational actions, or deliberately delegate a choice
with explicit constraints and decision criteria?

| Score | Evidence anchor |
|---|---|
| `0-19` | The implementer must invent the core behavior, architecture, data, or operating model. |
| `20-39` | A direction exists, but multiple incompatible material choices remain open without criteria. |
| `40-59` | Some decisions are bounded, while important interfaces, topology, state, failure, security, or data choices remain open. |
| `60-69` | The main path is directed, but at least one blocking material choice still requires assumption or Q&A. |
| `70-79` | Only a few localized material choices lack a default, constraint, owner, or decision rule. |
| `80-89` | Every material choice is selected, constrained, or explicitly delegated with invariants and acceptance criteria; only non-material freedom remains. |
| `90-100` | Every material decision exposes the choice, constraints, defaults/fallbacks, ownership, and relevant trade-off rationale. |

Do not reward needless prescription. A Spec can be highly decidable while
leaving implementation freedom when that freedom is explicit and its
invariants, interfaces, risks, and acceptance criteria are bounded.

#### `ambiguity` — competing plausible interpretations (lower is better)

Ambiguity uses inverse anchors based on severity and dispersion, not a raw
sentence count.

| Score | Evidence anchor |
|---|---|
| `0-10` | No material statement has a competing plausible interpretation. |
| `11-20` | Rare, localized editorial ambiguity with no behavioral impact. |
| `21-30` | Minor, bounded interpretation residue; no unresolved interpretation changes material behavior. |
| `31-40` | At least one material alternative reading exists, or several localized residues can change implementation or validation. This fails the default maximum. |
| `41-60` | Multiple alternative readings materially change behavior, tests, data, or operations. |
| `61-80` | Undefined terms or contradictions are systemic; major parts of the solution admit incompatible readings. |
| `81-100` | The central intent, scope, or expected behavior is contradictory or cannot be determined reliably. |

Any unresolved alternative that changes behavior, security, data integrity, or
an external contract imposes a minimum ambiguity score of `31`. A contradiction
in central intent imposes a minimum of `61`. An omission counts as Ambiguity
only when it creates two or more plausible readings; otherwise score it under
Clarity or Decidability.

### Dimension boundaries — do not double-count automatically

Use this diagnostic sequence for each observation:

1. **Confidence:** “How solid and complete was my inspection?”
2. **Clarity:** “Can I understand the intended meaning?”
3. **Assertiveness:** “Can I objectively prove pass or fail?”
4. **Decidability:** “Do I know which material action or choice to take?”
5. **Ambiguity:** “Are two or more materially different readings plausible?”

One passage may genuinely affect more than one dimension, but each tag needs a
different defect and impact statement. Never mirror the same penalty across
Clarity, Assertiveness, Decidability, and Ambiguity merely because a sentence
is weak.

### Evidence, justifications, and pinpoint anchors

Each justification must identify:

1. the selected band and why its anchor applies;
2. the current-edition scope inspected;
3. concrete positive evidence;
4. material gaps or known evidence limitations; and
5. why the exact score fits inside the selected band.

Avoid justifications such as “looks good”, “meets the threshold”, or “score is
appropriate”. They do not make an evaluation reproducible.

Pinpoints use the closed shape
`{metric, anchor_type, anchor_ref?, detail}`:

| `anchor_type` | Use it for | `anchor_ref` |
|---|---|---|
| `structured_child` | A specific FR, BR, TR, AC, contract, scenario, Decision, IR, or OR returned by the current Spec context. Prefer this whenever a stable item exists. | The stable item ID, for example `ac_2ce4ec3` or `tr_availability`; never a list index or human ordinal such as `AC-1`. Use the ID itself, not a collection-qualified path. |
| `field` | A root Spec field such as `description` or `context`, or a named structured collection when no child ID represents the issue. | The exact current field name. |
| `qa` | A specific unresolved, conflicting, or decision-bearing Q&A item. | The stable Q&A ID. |
| `whole_artifact` | A systemic issue that cannot honestly be localized. Use sparingly. | Omit `anchor_ref`. |

The client submits only the selector. Pulse resolves it against the authorized
current Spec and seals the human-readable label/text plus stable ID into
history. The `detail` must state **observable defect + impact + concrete
remediation**; an ID alone is never sufficient. Reuse one anchor for multiple
metrics only when each pinpoint explains a distinct metric-specific impact.

Pinpoints remain optional in the transport contract, but this evaluator method
requires:

- at least one pinpoint for every metric that violates its effective threshold;
- at least one blocker pinpoint when `recommendation=reject` although every
  score passes; and
- pinpoints for the most material residual issues behind a borderline score.

### Calibrated examples

#### Example 1 — vague availability requirement

Before:

> “The application must have high availability.”

Assuming the rest of the Spec is current and was fully inspected, this passage
is **clear about the desired quality** (`clarity` can remain near `85`), but is
not objectively verifiable (`assertiveness` near `20`), does not direct
topology, scale, SLO, or recovery choices (`decidability` near `15`), and admits
materially different readings such as active-active versus active-passive
(`ambiguity` near `65`). Confidence is not scored from this sentence; it is
scored from the completeness of the whole evaluation.

After:

> “Run active-active across at least three availability zones. Keep the Auto
> Scaling Group between 3 and 8 instances. Provide a 99.99% monthly ingress
> availability SLO, excluding only declared maintenance. Scale out when p95 CPU
> exceeds 70% for five consecutive minutes and replace an unhealthy instance
> within two minutes.”

This can support `clarity`, `assertiveness`, and `decidability` in the `90-99`
band and `ambiguity` in `0-10`, provided contracts, scenarios, and related
requirements are consistent.

Example pinpoint for the “before” version:

```json
{
  "metric": "decidability",
  "anchor_type": "structured_child",
  "anchor_ref": "tr_availability",
  "detail": "The requirement names high availability but supplies no topology, SLO, scaling bounds, or recovery rule, so implementers must invent materially different operating models; specify those constraints and measurable defaults."
}
```

#### Example 2 — understandable but not assertive

Before:

> “When appropriate, notify the customer quickly after payment failure.”

The intended behavior is broadly understandable, so Clarity need not collapse.
However, “when appropriate” and “quickly” supply no oracle: Assertiveness is in
the `20-39` band. If the eligible failure classes and delivery channel are also
left open, Decidability is at most `59`; if two plausible teams would notify on
different events, Ambiguity is at least `31`.

After:

> “For payment failures with codes `insufficient_funds`, `expired_card`, or
> `do_not_honor`, enqueue one email notification within 30 seconds. Do not
> notify for client-cancelled payments. Retry delivery twice at 60-second
> intervals and emit `payment_notification_failed` after the final failure.”

The revised text defines scope, oracle, timing, exclusions, retry behavior, and
observability; it can support the `90-99` higher-is-better band and `0-10`
Ambiguity when consistent with its contract and scenarios.

#### Example 3 — confidence is independent of Spec quality

- Reading only `description` and FRs, without the current edition identity,
  Q&A, rules, contracts, scenarios, or dependencies: do not submit; if the
  edition is known but material sections remain unread, `confidence <= 69`.
- Reading the full current context, cross-checking every applicable section and
  anchoring each material conclusion: `confidence=80-95` is defensible even
  when the other metrics fail and the recommendation is `reject`.

### Recommendation semantics

Use `approve` only when every score satisfies the **effective** board threshold,
Confidence establishes a complete-enough inspection, no material Q&A is open,
and no blocker pinpoint remains. Use `reject` when any threshold fails, the
inspection is materially insufficient/inaccessible, or a blocker exists. If
all scores pass but the recommendation is `reject`, the justification must
identify the exceptional blocker and a pinpoint must locate it. `reject`
always wins; `approve` never overrides a threshold violation.

Each score requires its own evaluator justification. Pulse validates and stores
this externally supplied assessment but does not run an evaluator. Historical
`score`/`summary` and `completeness` records remain readable compatibility
evidence and do not define the canonical five-metric gate.

**Anti-pattern — GRAVE violation:** Re-submitting with higher numbers without actually improving the spec. The correct response to a failed dimension is to ADD content (new test scenarios, refined BRs, edge cases) until you genuinely believe the score is higher.

**FR coverage source:** the deterministic FR coverage gate is `Functional Requirement -> Business Rule`. It reads `business_rules[].linked_requirements`; direct task links on `functional_requirements[].linked_task_ids` are traceability only and do not close `fr_coverage_pct`.

**MCP tools for the gate:**
- `okto_pulse_submit_spec_validation(...)` — requires spec in `approved` status.
- `okto_pulse_list_spec_validations(board_id, spec_id)` — returns Current and Previous results by lifecycle edition; legacy SQL `NULL` editions are history-only under Previous.
- `okto_pulse_move_spec(board_id, spec_id, status="draft")` — the single-hop reopen path. It starts a new edition, clears `current_validation_id`, and preserves earlier results under Previous.

## Curated Spec Checklist Gate — `/specify/v1`

Every board resolves one versioned binding for target `spec`, phase
`spec_validation`, and mode `off`, `advisory`, or `blocking`. New boards
snapshot the mode selected by the active Global Default; absent and historical
default values resolve to `advisory`. Legacy boards without a binding resolve
effective `off` without creating a row. Only an authenticated human session
can change the board binding or the checklist mode on a Global Default.
Agents can read and execute the curated checklist but cannot mutate either of
those human-owned controls.

The execution flow is:

1. Read `okto_pulse_get_checklist_binding(board_id)` and the required full Spec
   context.
2. If mode is `advisory` or `blocking`, call
   `okto_pulse_start_checklist_execution` with the current binding digest and
   exact Spec edition plus its technical version.
3. Submit all ten immutable items exactly once with `pass`, `fail`, or allowed
   `not_applicable`; every result needs an anchor and N/A needs a rationale.
4. Read the submitted result with `okto_pulse_get_checklist_receipt` (the
   compatibility API name), then re-run full Spec context or
   `okto_pulse_get_allowed_transitions`. Canonical human readiness requires a
   passing Current result for the exact Spec edition. Frozen content, template,
   and binding identities remain technical audit evidence and do not create a
   second human-facing lifecycle state.

Mode behavior:

- `off`: start/submit is rejected before any execution row is created; the
  checklist does not block Spec Validation.
- `advisory`: results remain traceable but do not block.
- `blocking`: `approved` → `validated` and semantic Spec Validation both
  require a native, non-failing Current result for the same Spec edition.

The same pure readiness predicate drives
`okto_pulse_get_allowed_transitions`, `okto_pulse_move_spec`,
`okto_pulse_submit_spec_validation`, full Spec context, and REST/UI state.
Canonical blocking codes are `spec_checklist_gate_required` plus technical
compatibility reason codes such as `checklist_receipt_required`,
`checklist_receipt_stale`, `checklist_item_failed`, or
`manual_checklist_legacy_unverified`. The `receipt`/`stale` vocabulary in those
stable codes does not define the human UI state, which is Current or Previous.

MCP read/execution tools:

- `okto_pulse_get_checklist_binding`
- `okto_pulse_start_checklist_execution`
- `okto_pulse_submit_checklist_execution`
- `okto_pulse_get_checklist_receipt`

Exact arguments and result envelopes are documented in
`okto-pulse://reference/tool-docs/spec`.

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

The response separates `validation_outcome` from `completion_outcome`: a
quality review can pass while another governed completion gate rejects the
completion. An admitted rejection atomically stores its cause and moves a
Normal/Bug card to `rejected`; validators never move it back themselves. The
executor reads the Current cause, moves `rejected` → `in_progress`, records a
new conclusion, and hands off a new validation attempt. Every call carries
`expected_subject_version` and `idempotency_key`; exact retries resolve before
the mutable status check, while key reuse with a different payload fails.

The `resolved_from` field in `validation_config` tells you which level provided the active configuration (`"board"`, `"spec"`, or `"sprint"`).

**Independent reviewer policy:** `reviewer_separation_mode` is resolved from the board before any task-validation mutation. The full task context projects the current caller's `reviewer_separation` decision against card creator, assignee, and executor identities. `enforce` blocks with the action-required code `reviewer_separation_required`; `warn` and `off` proceed and persist the decision in the append-only validation. Legacy persisted boards with the setting absent resolve explicitly to `off` / `legacy_absent_compat`; new boards and new default-board template versions use `enforce` unless configured otherwise.

**Coverage summary tools:**
- `okto_pulse_list_spec_evaluations(board_id, spec_id)` — evaluations history.
- `okto_pulse_get_spec_evaluation(board_id, spec_id, evaluation_id)` — single evaluation detail.
- `okto_pulse_list_spec_validations(board_id, spec_id)` — validation gate history.
- `okto_pulse_list_task_validations(board_id, card_id)` — task-level validation history.

## Code Evidence disposition gate

When `code_traceability.mode="blocking"`, a Spec cannot leave authoring with
inherited Code Evidence still pending. The deterministic gate requires:

- every inherited Evidence item to resolve from the current Spec lineage;
- each item to have at least one current link to the applicable Spec, FR, TR,
  AC, BR, Decision, API Contract, IR, OR, or Test Scenario, or one explicit
  disposition for this Spec version;
- `evidence_disposition_coverage_pct = 100`;
- any receipt required by policy to be accepted, current, non-revoked, and
  conflict-free.

Evidence remains a factual historical snapshot. The Spec remains the normative
artifact, and linking or disposition does not rewrite either artifact. In
`advisory` mode the same findings are returned without independently blocking;
historical absent, `null`, or `off` settings resolve to this default Advisory
behavior. An authenticated external agent performs source access checks and
deterministic investigation before submission. Pulse Core validates the
receipt and Community persists/projects it; neither reads a repository to
satisfy the gate.

Read `okto-pulse://reference/code-traceability` before operating this domain.
