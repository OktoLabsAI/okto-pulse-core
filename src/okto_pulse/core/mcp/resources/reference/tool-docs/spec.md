---
version: "1.2"
---

# Tool docs — `spec`

Validation and curated-checklist gate rules:
`okto-pulse://reference/spec_gates`.

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_spec_dependency`

Add a directed operational prerequisite to a Spec. Both Specs must exist on the
same board, self-references and cycles are rejected, and an already active edge
cannot be added twice. If execution has started in the current Spec edition,
the new prerequisite must already be Done and not archived.

Args:
    board_id: Board ID shared by the dependent and prerequisite Specs
    spec_id: Dependent Spec whose execution will be gated
    prerequisite_spec_id: Spec that must be Done and not archived
    expected_spec_version: Exact current technical revision of the dependent Spec
    expected_spec_edition: Exact current lifecycle edition of the dependent Spec
    idempotency_key: Caller-stable key for exact retries of this add operation

Returns:
    `{success, dependency, spec_version, replayed}`. The successful mutation
    advances the dependent Spec's technical revision. An exact retry returns the
    original result with `replayed=true`; reusing the key for different input is
    a closed `spec_dependency_state_conflict` with
    `facts.conflict_kind=idempotency_key_reuse`.

Permissions:
    Requires `spec.entity.manage_dependencies` in the dependent Spec's current
    lifecycle state and read access to both endpoint Specs.

## `okto_pulse_remove_spec_dependency`

Remove an active outgoing prerequisite while preserving its immutable lifecycle
record. Removal is a tombstone operation; historical edges remain available
through `okto_pulse_list_spec_dependencies` with `active_state=removed|all`.

Args:
    board_id: Board ID
    spec_id: Dependent Spec that owns the outgoing edge
    dependency_id: Active dependency identity returned by the list/add tool
    reason: Required non-blank human explanation for removal
    expected_spec_version: Exact current technical revision of the dependent Spec
    expected_spec_edition: Exact current lifecycle edition of the dependent Spec
    idempotency_key: Caller-stable key for exact retries of this remove operation

Returns:
    `{success, dependency, spec_version, replayed}`. The successful mutation
    advances the dependent Spec's technical revision and returns the tombstone.

Permissions:
    Requires `spec.entity.manage_dependencies` in the dependent Spec's current
    lifecycle state and `spec.entity.read` for that dependent Spec.

## `okto_pulse_list_spec_dependencies`

List operational precedence in either direction with opaque keyset pagination.
The response embeds the authoritative readiness projection; there is no separate
readiness tool. A prerequisite is satisfied only while its current Spec is Done
and not archived.

Args:
    board_id: Board ID
    spec_id: Anchor Spec
    direction: `depends_on` for outgoing prerequisites (default), or
        `required_by` for incoming dependents
    cursor: Opaque `next_cursor` from the previous response; never decode or edit it
    limit: Page size from 1 through 100 (default 25)
    active_state: `active` (default), `removed`, or `all`
    satisfaction: `all` (default), `satisfied`, or `unmet`
    retrospective: Optional boolean filter for edges added after execution started
    related_statuses: Optional multi-value Spec-status filter
    lineage: `all` (default), `same_ideation`, or `cross_ideation`

Returns:
    `{items, direction, total, next_cursor, has_more, readiness}`. Each item
    includes current related-Spec state, satisfaction, lineage, retrospective
    state, and `{can_remove, remove_reason_code, can_navigate}` capabilities.
    `readiness.blocking_count`, `archived_blocking_count`, and
    `unfinished_blocking_count` are exact totals. `readiness.blockers[]` is a
    bounded diagnostic sample and `blockers_truncated` declares whether rows
    were omitted. Each sampled blocker includes `target_archived`, so a
    Done-but-archived prerequisite remains explicitly blocked and must be
    restored or removed even when its row falls outside the sample.

Permissions:
    Requires `spec.entity.read` for the anchor Spec.

## `okto_pulse_remove_spec_entity`

Consolidated spec-entity removal (R4). Dispatches on `target_type`.

Args:
    board_id: Board ID
    spec_id: Spec ID
    target_type: One of `business_rule` | `api_contract` | `decision`
    entity_id: The id of the entity to remove

Returns:
    For `business_rule`/`api_contract` (HARD remove): `{success, removed, remaining, …}`.
    For `decision` (SOFT-delete): `{success, revoked, decision}` — `status` becomes
    `revoked`, restorable via `okto_pulse_update_decision` with `status=active`.
    Unsupported `target_type` returns `{error:"unsupported_target_type", allowed:[…]}`
    (no mutation).

The legacy tools (`okto_pulse_remove_business_rule`, `okto_pulse_remove_api_contract`,
`okto_pulse_remove_decision`) remain as aliases and delegate to the same
implementation. Full family contract:
`okto-pulse://reference/tool-families/spec_entity_remove`.

## `okto_pulse_answer_spec_question`

Answer a question on a spec's Q&A board.
For text questions, provide answer. For choice questions, provide selected option IDs.

Args:
    board_id: Board ID
    spec_id: Spec ID (for context/validation)
    qa_id: Q&A item ID to answer
    answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
    selected: Option IDs for choice questions — multi-value; formats:
        okto-pulse://reference/multivalue.

Returns:
    JSON with updated Q&A item

## `okto_pulse_ask_spec_choice_question`

Ask a choice question (poll/form) on a spec's Q&A board. The respondent picks from predefined options.
Use this when you need a structured answer — e.g. "Which auth approach?" with options.

Args:
    board_id: Board ID
    spec_id: Spec ID
    question: The question text
    options: Option labels — multi-value; formats:
        okto-pulse://reference/multivalue.
    options_json: Preferred structured options. Pass a native array such as
        [{"label":"Safer path","recommended":true,"tradeoff":"More setup"}].
        A JSON-array string is accepted for compatibility. Each item requires
        label; recommended defaults to false and tradeoff to null. A non-empty
        options_json takes precedence over options.
    question_type: "choice" for single-select (default) or "multi_choice" for multi-select
    allow_free_text: "true" to also allow a free-text response alongside selections

Returns:
    JSON with Q&A item including choices

## `okto_pulse_ask_spec_question`

Ask a question on a spec's Q&A board. Use @Name to direct the question.
Both humans and agents can ask questions — this is for clarifying spec requirements
BEFORE work begins on tasks.

Args:
    board_id: Board ID
    spec_id: Spec ID
    question: Question text (use @Name to mention someone)

Returns:
    JSON with Q&A item details

## `okto_pulse_create_spec`

Create a new spec (specification) on the board. Specs define requirements that drive card/task creation.
AI agents can create specs to propose work, which can then be reviewed, approved, and derived into cards.

Args:
    board_id: Board ID
    title: Spec title
    description: High-level summary of what needs to be built (optional). Supports Markdown and Mermaid diagrams.
    context: Business context — why this spec exists, how it connects to the bigger picture (optional). Supports Markdown and Mermaid diagrams.
    functional_requirements: Pipe-separated list of functional requirements (e.g. "User can login|User can reset password")
    technical_requirements: Pipe-separated list of technical constraints (e.g. "Must use OAuth2|Response time < 200ms")
    acceptance_criteria: Pipe-separated list of acceptance criteria (e.g. "All tests pass|No console errors")
    status: Spec status — one of: draft, review, approved, in_progress, done, cancelled (default: draft)
    assignee_id: User/agent ID to assign (optional)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue.
    ideation_id: Optional parent ideation ID for traceability when creating a spec manually
    refinement_id: Optional parent refinement ID for traceability when creating a spec manually
    delivery_context: Required for a direct Spec without refinement lineage;
        one of brownfield, greenfield, or hybrid. A refinement-backed Spec
        inherits the frozen Refinement value when omitted.
    delivery_context_override_reason: Required when delivery_context differs
        from the inherited Refinement value; omit when no override is made.

Returns:
    JSON with created spec details

## `okto_pulse_delete_spec`

Delete a spec. Derived cards are unlinked but not deleted.

Args:
    board_id: Board ID
    spec_id: Spec ID

Returns:
    JSON with success status and a governed `takedown` receipt. Receipts for
    sprints deleted by the spec cascade are exposed under
    `takedown.descendant_deletions`.

## `okto_pulse_delete_spec_evaluation`

Delete your own evaluation. Only the author can delete their evaluation.

Args:
    board_id: Board ID
    spec_id: Spec ID
    evaluation_id: Evaluation ID to delete

Returns:
    JSON with success or error

## `okto_pulse_delete_spec_question`

Delete a Q&A item from a spec. Use this to invalidate outdated questions
or remove resolved clarifications that no longer apply.

Args:
    board_id: Board ID
    spec_id: Spec ID (for context/logging)
    qa_id: Q&A item ID to delete

Returns:
    JSON with success status

## `okto_pulse_derive_spec_from_ideation`

Create a spec draft from a DONE ideation. The ideation must be in 'done' status
(meaning it has been fully reviewed and snapshotted). The spec will have rich context
compiled from the ideation but structured fields (requirements, criteria) left empty
for deliberate analysis.

Artifacts (mockups, KBs, Architecture Designs) from the ideation are
automatically propagated to the spec. Use mockup_ids/kb_ids/
architecture_design_ids to select specific ones (default: all).

An Ideation has no inherited Code Evidence delivery context. Select the
explicit `delivery_context` for this Spec deliberately; do not infer it from
repository contents, source access, or an empty Evidence list.

Args:
    board_id: Board ID
    ideation_id: Ideation ID (must be in 'done' status)
    mockup_ids: Pipe-separated mockup IDs to propagate (optional, empty = all)
    kb_ids: Pipe-separated KB IDs to propagate (optional, empty = all)
    architecture_design_ids: Multi-value Architecture Design IDs to propagate (optional, empty = all)
    architecture_propagation_mode: one of copy, derive, reference_only, none.
        "snapshot" is not accepted; copy/derive are the snapshot-copy modes,
        while reference_only/none keep only parent linkage.
    delivery_context: Required implementation context — one of brownfield,
        greenfield, or hybrid.

Returns:
    JSON with the created spec details

## `okto_pulse_derive_spec_from_refinement`

Create a spec draft from a DONE refinement. The refinement must be in 'done' status.
Context is compiled from the refinement's scope, analysis, decisions, and Q&A.
The Spec inherits and pins the exact delivery-context provenance and effective
source-context manifest from the frozen Refinement snapshot. Later live
Evidence or human-classification changes do not silently rewrite this Spec.

Artifacts (mockups, KBs, Architecture Designs) from the refinement are
automatically propagated to the spec. Use mockup_ids/kb_ids/
architecture_design_ids to select specific ones (default: all) on the legacy
v1 path. Supplying `knowledge_propagation` opts only Knowledge propagation into
contract v2; mockup and Architecture Design parameters remain independent.

Args:
    board_id: Board ID
    refinement_id: Refinement ID (must be in 'done' status)
    mockup_ids: Pipe-separated mockup IDs to propagate (optional, empty = all)
    kb_ids: Pipe-separated KB IDs to propagate on legacy v1 (optional,
        empty = all). Mutually exclusive with knowledge_propagation.
    architecture_design_ids: Multi-value Architecture Design IDs to propagate (optional, empty = all)
    architecture_propagation_mode: one of copy, derive, reference_only, none.
        "snapshot" is not accepted; copy/derive are the snapshot-copy modes,
        while reference_only/none keep only parent linkage.
    knowledge_propagation: Optional contract-v2 envelope. Omit it to preserve
        the complete v1 derivation behavior. Supplying it — even with
        selection_state=omitted — selects v2. Fields:
        - contract_version: 2 (default)
        - selection_state: omitted | explicit_empty | explicit_ids
        - mode: absent for omitted; drop for explicit_empty; reference,
          snapshot, or drop for explicit_ids
        - knowledge_ids: empty for omitted/explicit_empty; non-empty stable
          source Knowledge IDs for explicit_ids
        - justification: required and non-empty unless selection_state=omitted
        - idempotency_key: required caller-stable key for exact retries
        - expected_revision: omit or pass 0 for creation

`kb_ids` plus `knowledge_propagation` is rejected with
`conflicting_propagation_parameters`; choose exactly one contract. The v2
preflight validates the done refinement, parent, selection, and source roots
before the deterministic spec is inserted.

Returns:
    Without knowledge_propagation: the unchanged v1 JSON with created spec
    details.

    With knowledge_propagation: `{success, contract_version, target_type,
    target_id, spec_id, operation_id, revision, replayed, selection_state,
    assignments}`. An exact retry with the same idempotency key returns the
    original durable result and `replayed=true`. MCP retries a
    `knowledge_creation_race` once in a fresh unit of work; if the error is
    still exposed, it carries `retryable=true` and the caller should retry the
    exact same request/key.

## `okto_pulse_get_spec`

Get full details of a spec including its derived cards.

Args:
    board_id: Board ID
    spec_id: Spec ID

Returns:
    JSON with spec details and linked cards

## `okto_pulse_get_spec_context`

Get the FULL consolidated context of a spec. Returns ALL structured data
needed to evaluate, validate, or review this spec before advancing it.

Includes: requirements, test scenarios, business rules, API contracts, IRs,
ORs, screen mockups, knowledge bases, Q&A, evaluations, cards, and sprints.

**Always call this before evaluating, moving, or creating cards from a spec.**

Args:
    board_id: Board ID
    spec_id: Spec ID
    include_knowledge: Include knowledge base entries (default "true")
    include_mockups: Include screen mockups (default "true")
    include_qa: Include Q&A items (default "true")
    include_architecture: Include Architecture Designs (default "true")
    include_superseded: When "false" (default), the `decisions` array
        returns only entries with status="active" — noise reduction for
        the common "what rules today?" path. Set to "true" to get the
        full history (active + superseded + revoked). A `decisions_stats`
        summary is always included so you can see what was filtered.
    profile: Response projection — one of: summary (default), detail, full,
        legacy. Use `summary` for exploration and `full` before evaluating,
        moving, or deriving cards. See okto-pulse://reference/projection-profiles.

Returns:
    JSON with complete spec context: all requirements + structured sections +
    artifacts + cards + sprints. `gate_readiness.spec_checklist` always projects
    the canonical curated-checklist mode, allowance, lifecycle state, technical
    compatibility reasons, and follow-up tool names. In `draft` this readiness remains visible
    without prematurely claiming an active transition gate; when an unsatisfied
    blocking checklist applies to `approved` → `validated`, the same predicate
    also appears in `gate_readiness.active_gates`.

## `okto_pulse_get_spec_evaluation`

Get full details of a specific evaluation including all dimensions and justifications.

Args:
    board_id: Board ID
    spec_id: Spec ID
    evaluation_id: Evaluation ID (e.g. eval_abc12345)

Returns:
    JSON with full evaluation details

## `okto_pulse_get_spec_history`

Get the detailed change history of a spec. Shows every modification with field-level diffs
(old value vs new value), who made the change, and when. Use this to understand how a spec
evolved over time and what exactly was modified at each step.

Args:
    board_id: Board ID
    spec_id: Spec ID
    limit: Maximum number of history entries to return (default 30)

Returns:
    JSON with list of history entries, newest first. Each entry includes:
    - action: what happened (created, updated, status_changed, cards_derived, etc.)
    - actor_name: who did it
    - changes: list of {field, old, new} diffs
    - summary: human-readable summary
    - version: spec version at that point
    - created_at: when it happened

## `okto_pulse_get_checklist_binding`

Read the effective curated checklist binding for a board. A legacy board with
no persisted binding is reported as effective `off`; this read never
materializes or changes a binding.

Args:
    board_id: Board ID

Returns:
    JSON containing the binding identity/digest, immutable version,
    `/specify/v1` template version, phase, and mode (`off`, `advisory`, or
    `blocking`).

Permissions:
    Requires both board read authority and `spec.checklist.read`.

## `okto_pulse_start_checklist_execution`

Start a server-issued execution frozen to the current Spec edition and version, semantic
content/input digests, template version/digest, and binding version/digest.
Mode `off` rejects the operation without creating an execution.

Args:
    board_id: Board ID
    spec_id: Spec ID
    binding_id: Current binding digest returned by
        `okto_pulse_get_checklist_binding`
    spec_edition: Exact current lifecycle edition
    expected_spec_version: Current Spec version
    idempotency_key: Stable key for exact retries

Returns:
    JSON with execution ID, frozen identities, revision, status, `replayed`,
    and the ten immutable template items in their normative order. Each item
    includes `item_id`, English and Portuguese title/description, and
    `allow_na`, so the subsequent submission is self-describing over MCP.

Permissions:
    Requires both Spec update authority and `spec.checklist.execute`.

## `okto_pulse_submit_checklist_execution`

Submit one outcome for every immutable `/specify/v1` item and seal a
server-authored result. The ten items must be present exactly once. Each
result requires a concrete Spec anchor; `not_applicable` also requires a
rationale and is accepted only on items whose template allows it.

Args:
    board_id: Board ID
    spec_id: Spec ID
    execution_id: Open execution ID
    expected_execution_revision: Current execution revision
    idempotency_key: Stable key for exact retries
    results: Array of exactly ten objects with `item_id`, `outcome`
        (`pass`, `fail`, or `not_applicable`), `anchor`, and optional
        `rationale`

Returns:
    JSON with a compatibility `receipt_id`, aggregate `outcome` (`pass` unless
    any item is `fail`), request digest, head revision, and `replayed`.

Permissions:
    Requires both Spec update authority and `spec.checklist.execute`.

## `okto_pulse_get_checklist_receipt`

Read an immutable checklist result and its ordered per-item evidence through
the compatibility receipt-named API. A result supports a blocking gate only
when it is Current for the exact Spec edition and every item is non-failing.

Args:
    board_id: Board ID
    receipt_id: Compatibility checklist result ID

Returns:
    JSON with the frozen identities, ordered results, source, head revision,
    predecessor, aggregate `outcome` (`pass` or `fail`), and
    `blocking_satisfied`.

Permissions:
    Requires both board read authority and `spec.checklist.read`.

## `okto_pulse_list_spec_evaluations`

List all qualitative evaluations for a spec, with stale indication.

Args:
    board_id: Board ID
    spec_id: Spec ID

Returns:
    JSON with evaluations list and summary

## `okto_pulse_list_spec_validations`

List Spec Validation Gate results in reverse chronological order, explicitly
separating Current from Previous by lifecycle edition.

Useful for understanding why a spec was validated (or failed). Canonical
records include five scores, per-score justifications, optional pinpoints,
outcome, threshold violations, and a resolved_thresholds snapshot of what was
in effect when the submit happened. Older records preserve their historical
score/summary or three-metric shape.
The result matching the active Spec edition is Current. Earlier-edition results
are Previous; legacy SQL `NULL` editions are `history_only` under Previous and
can never become Current.

Args:
    board_id: Board ID
    spec_id: Spec ID
    lifecycle_state: Optional `all|current|previous|history_only` filter

Returns:
    JSON with current_validation_id and validations list (reverse chronological)

## `okto_pulse_move_spec`

Change a spec's status (e.g. draft → review → approved → validated → in_progress → done).

Args:
    board_id: Board ID
    spec_id: Spec ID
    status: New status — one of: draft, review, approved, validated, in_progress, done, cancelled
    cancellation_reason: REQUIRED when status=cancelled; reopening clears it.

Returns:
    JSON with updated spec status

## `okto_pulse_submit_spec_evaluation`

Submit a qualitative evaluation for a spec in 'validated' status.
Multiple evaluators can submit independent evaluations.

Args:
    board_id: Board ID
    spec_id: Spec ID (must be in 'validated' status)
    breakdown_completeness: Score 0-100 — do tasks cover the spec scope?
    breakdown_justification: Why this score
    granularity: Score 0-100 — are tasks properly sized?
    granularity_justification: Why this score
    dependency_coherence: Score 0-100 — do task dependencies make sense?
    dependency_justification: Why this score
    test_coverage_quality: Score 0-100 — do tests cover happy path and edge cases?
    test_coverage_justification: Why this score
    overall_score: Overall score 0-100
    overall_justification: Overall assessment summary
    recommendation: approve | request_changes | reject

Returns:
    JSON with created evaluation details

## `okto_pulse_submit_spec_validation`

Submit a Spec Validation Gate record for a spec in 'approved' status.

This is the entry point for the Spec Validation Gate — a semantic quality
gate that runs AFTER the existing deterministic coverage gates
(AC/FR/TR/Contract/Code Evidence Matrix).
Use this AFTER the Spec is saturated on detail. Read
`okto-pulse://workflows/specs` for the authoring/evaluator loop and
`okto-pulse://reference/spec_gates` under **Canonical Spec Validation scoring rubric**
for the required bands, evidence caps, dimension boundaries, examples,
pinpoint anchors, and recommendation semantics. The abbreviated argument text
below is not sufficient to derive scores consistently.

The system runs coverage gates first; if any fails the submit is rejected
with the specific coverage violation. If coverage passes, it computes outcome:
- FAILED if any threshold violated OR recommendation=reject
- SUCCESS only if ALL thresholds pass AND recommendation=approve

On SUCCESS, the spec is atomically promoted from 'approved' to 'validated'
and enters the content lock (update_spec and related tools will raise
SpecLockedError). Same-edition lifecycle moves and Code
Investigation/Evidence/waiver events preserve Current. To edit after a success,
move the spec to draft; that opens a new edition, clears Current, and preserves
the full history. An explicit successor submission is the only other operation
that replaces Current.

ANTI-PATTERN WARNING: inflating scores to make the gate pass is a grave
violation of the detail saturation principle. If outcome=failed, iterate
on content (add scenarios, refine BRs, specify TRs) rather than just
raising the numbers.

Args:
    board_id: Board ID
    spec_id: Spec ID (must be in 'approved' status)
    expected_validation_edition: Current human lifecycle edition
    expected_spec_version: Current technical Spec version
    expected_head_revision: Current validation head revision
    confidence: Score 0-100 — reliability and evidence coverage of the evaluator's whole-current-edition assessment; NOT Spec quality
    confidence_justification: Why this confidence score (min 10 chars)
    clarity: Score 0-100 — how self-contained and understandable the problem, solution, scope, actors, terms, flows and errors are
    clarity_justification: Why this clarity score (min 10 chars)
    assertiveness: Score 0-100 — whether normative statements have objective, observable pass/fail criteria
    assertiveness_justification: Why this assertiveness score (min 10 chars)
    decidability: Score 0-100 — whether material implementation/operational choices are selected, constrained, or deliberately delegated with criteria
    decidability_justification: Why this decidability score (min 10 chars)
    ambiguity: Score 0-100 — severity and dispersion of materially competing plausible interpretations (LOWER IS BETTER)
    ambiguity_justification: Why this ambiguity score (min 10 chars)
    pinpoints: Optional closed list of metric-tagged semantic problem locations
    recommendation: One of: approve, reject

Scoring contract:
    Use a 0-100 integer scale, not 1-5. A value like 5 is treated as 5/100 and
    will usually fail. confidence/clarity/assertiveness/decidability are
    higher-is-better; ambiguity is lower-is-better. Default thresholds are
    70/80/80/80/max-30 respectively. Every score requires its own
    justification. A pinpoint has `{metric, anchor_type, anchor_ref?, detail}`;
    whole_artifact omits anchor_ref, while field/structured_child/qa require it.
    Prefer a stable structured-child or Q&A ID over a mutable ordinal/index.
    Select the rubric band from evidence before the exact score. Do not derive
    a score from the configured threshold, average dimensions, or mirror one
    defect across dimensions without a distinct metric-specific impact. Read
    the effective board thresholds before submitting. The evaluator method
    requires a pinpoint for each threshold violation and for an exceptional
    blocker behind reject when all scores pass.
    Pulse validates and stores the evaluator's result but does not perform the
    evaluation. Historical score/summary and completeness shapes remain
    readable history, but are not accepted for new submissions.

Coverage contract:
    FR coverage is computed from business_rules[].linked_requirements. Direct
    task links on functional_requirements[].linked_task_ids are traceability
    only and do not close fr_coverage_pct.
    Code Evidence Matrix coverage is 100% only when every active inherited
    Evidence item has a current Spec link or final disposition. It is enforced
    even when Code Traceability mode is advisory, unless an authorized human
    set `skip_code_evidence_coverage_global` for the Board or the audited
    `skip_code_evidence_coverage` flag on the Spec. Effective resolution is
    Board-global OR Spec-local. The governed agent workflow reads these flags
    and remediates coverage; skip changes remain human UI/REST decisions.

Returns:
    JSON with validation result, outcome, threshold violations, and resolved thresholds.
    On success, spec_status becomes "validated".

## `okto_pulse_update_spec`

Update a spec's fields. Content changes (description, context, requirements, criteria) bump the version.
Only non-empty fields are updated.

Args:
    board_id: Board ID
    spec_id: Spec ID
    title: New title (optional, empty = no change)
    description: New description (optional, empty = no change)
    context: New context (optional, empty = no change)
    functional_requirements: Pipe-separated list of functional requirements (optional, empty = no change)
    technical_requirements: Pipe-separated list of technical constraints (optional, empty = no change)
    acceptance_criteria: Pipe-separated list of acceptance criteria (optional, empty = no change)
    assignee_id: New assignee (optional, empty = no change)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue. (optional, empty = no change)

Returns:
    JSON with updated spec details

## `okto_pulse_update_spec_entity`

Polymorphic structured spec entity mutation tool for FR, BR, TR, Decision, AC, IR and OR.

API Contracts intentionally use okto_pulse_update_spec_api_contract so the richer
payload shape remains explicit while still delegating to StructuredSpecEntityService.

## Code Evidence links and dispositions

Read `delivery_context`, effective `source_context`, `source_context_items`,
and inherited Evidence from full Spec context. Summary role/classification
counts cover the complete effective set even when item collections are
bounded. Every item reports `context_origin` as `authored`,
`human_legacy_classification`, or `unclassified_legacy`; never infer a legacy
role from its path, type, or claim. Human legacy classification is an
append-only UI/REST action and has no MCP mutation.

Use
`okto_pulse_link_code_evidence` and `okto_pulse_unlink_code_evidence` for
version-fenced normative links, and `okto_pulse_set_code_evidence_disposition`
for an explicit non-applicable outcome. Every inherited item needs a current
link or final disposition before review in blocking mode. Evidence is an
immutable AS-IS factual snapshot and never becomes Spec text or a planned
TO-BE path.

A refinement-derived Spec stays bound to its frozen manifest and SHA-256. To
adopt a later Refinement snapshot, preview the governed Evidence rebase,
review context/classification/link/disposition deltas, and apply the exact
`preview_sha256`. Do not emulate rebase by copying Evidence or editing the
manifest. If the agent surface lacks preview/apply, surface the authorized
UI/REST action.

Canonical protocol: `okto-pulse://reference/code-traceability`.
