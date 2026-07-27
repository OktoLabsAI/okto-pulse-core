---
version: "1.0"
---

# Tool docs — `card`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_card_dependency`

Add a dependency: card_id cannot advance until depends_on_id is done/cancelled.
Repeated requests are idempotent and return the existing dependency. Invalid
graph shapes return typed errors: `dependency_self_reference` or
`dependency_cycle_detected`. A forward move with unfinished blockers returns
`dependencies_incomplete`.

Args:
    board_id: Board ID
    card_id: The card that will be blocked
    depends_on_id: The card it depends on

Returns:
    JSON with success and dependency_id, or a typed error envelope

## `okto_pulse_copy_qa_to_card`

Copy answered Q&A items from a spec to a card as a consolidated comment.
Only copies Q&As that have been answered — unanswered questions are skipped.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID

Returns:
    JSON with count of Q&A entries copied

## `okto_pulse_create_card`

Create a new card on the board. Every card MUST be linked to a spec.

Args:
    board_id: Board ID
    title: Card title
    spec_id: REQUIRED — Spec ID to link this card to. Normal/bug cards
        are allowed when the spec is approved, in_progress, or done.
        Test cards are allowed once the spec is approved/validated or
        later, including regression tests for a bug on a locked spec.
        For bug cards, this is auto-resolved from the origin task if not provided.
    description: Card description (optional). Supports Markdown and Mermaid diagrams (```mermaid code blocks).
    details: Card details/rich text (optional). Supports Markdown and Mermaid diagrams.
    status: Card status - one of: not_started, started, in_progress, validation, on_hold, done, cancelled
    priority: Card priority - one of: none, low, medium, high, very_high, critical (default: none)
    assignee_id: User ID to assign (optional)
    labels: Multi-value labels — formats: okto-pulse://reference/multivalue.
    test_scenario_ids: Multi-value test scenario IDs (e.g. ``["ts_abc", "ts_def"]``)
        — same input shapes as ``labels`` above. For test cards, this is MANDATORY.
        A single card is capped by the board's ``max_scenarios_per_card`` setting
        (default 3; some boards use 2). If the list exceeds the cap, creation
        fails with ``max_scenarios_per_card_exceeded`` and you must split the
        scenarios across separate test cards.
        When provided, automatically creates bidirectional links between the
        card and the scenarios. Linking to an existing scenario is a
        traceability update that leaves validated spec content unchanged, but
        for bug regression Path A the scenario must be
        eligible by lineage: same spec and linked to the bug origin task or an
        explicitly supplied affected task.
    card_type: Card type - "normal" (default), "test", or "bug".
        Test cards require test_scenario_ids and do not use
        submit_task_validation; complete them with move_card(..., done)
        plus conclusion/evidence. Bug regression coverage may reuse an
        eligible existing scenario after spec validation, but the linked test
        card itself must be created after the bug. On a validated/done spec,
        update the scenario status/evidence only after that test card is
        executable (`started`, `in_progress`, `validation`, or `done`) so the
        update is treated as operational evidence rather than semantic spec
        editing. If no eligible scenario
        exists or expected behavior changed, treat it as a semantic gap and
        remediate through a formal Path B AmendmentHotfixRevision —
        create/associate the revision, complete its lineage, register
        re-executable evidence, and have the validator confirm coverage —
        instead of editing the current spec. Refinement or spec-revision
        authoring may produce that revisional artifact, but on its own it does
        not satisfy the bug gate without amendment lineage and confirmed
        coverage. Bug cards require origin_task_id,
        severity, expected_behavior, and observed_behavior.
    origin_task_id: REQUIRED for bug cards — ID of the task that originated the bug. The spec is auto-resolved from this task.
    severity: REQUIRED for bug cards — one of: critical, major, minor
    expected_behavior: REQUIRED for bug cards — what should happen
    observed_behavior: REQUIRED for bug cards — what actually happens
    steps_to_reproduce: Steps to reproduce the bug (optional)
    action_plan: Plan for fixing the bug (optional)
    knowledge_propagation: Optional selective Knowledge contract-v2 envelope.
        Omit the whole field to preserve legacy v1 card creation and automatic
        Knowledge copy behavior. Supplying it selects v2, including when
        selection_state="omitted". Fields:
        - contract_version: 2 (default)
        - selection_state: omitted | explicit_empty | explicit_ids
        - mode: absent for omitted; drop for explicit_empty; reference,
          snapshot, or drop for explicit_ids
        - knowledge_ids: empty for omitted/explicit_empty; non-empty stable
          source Knowledge IDs for explicit_ids
        - justification: required and non-empty unless selection_state=omitted
        - idempotency_key: required caller-stable key for exact retries
        - expected_revision: omit or pass 0 for creation
        - relevance_links: optional list of {entity_type, entity_id}; the input
          alias linkage is also accepted. entity_type is
          functional_requirement, acceptance_criterion, or test_scenario, and
          every entity must belong to this card's linked spec.

Returns:
    Without knowledge_propagation: the unchanged v1 JSON with created card
    details.

    With knowledge_propagation: `{success, contract_version, card,
    operation_id, revision, replayed, selection_state, assignments}`. The
    `card` projection and operation result are rebuilt from the durable
    idempotency receipt. Replaying the exact same request/key returns the same
    result with `replayed=true`; reusing the key for a changed request fails.

Bug regression decision rule:
    Path A is traceability-only reuse. Use
    okto_pulse_resolve_bug_regression_scenarios before creating/linking the
    regression test card when eligibility is not obvious. The response exposes
    eligible_scenarios, rejected_scenarios, semantic_gap_required,
    spec_mutation_required, and next_action. Only eligible scenarios may satisfy
    require_test_task_for_bug.

    Path B is amendment-revision lineage remediation. Same-spec membership by
    itself, title similarity, cross-spec scenarios, scenario_not_found, or an
    unrelated scenario do not satisfy the bug gate. Escalate with
    next_action=escalate_semantic_gap, then remediate through a formal
    AmendmentHotfixRevision — create it with
    okto_pulse_create_amendment_revision, attach the regression
    artifacts/lineage with okto_pulse_associate_amendment_revision_artifacts and
    supply re-executable evidence, and have the validator record the
    (non-forgeable) coverage attestation with
    okto_pulse_confirm_amendment_coverage. Only then is the regression
    closure-ready; a generic authoring detour without amendment lineage and
    validator coverage confirmation does not satisfy the gate. Do not move a spec directly from in_progress to approved for the simple Path A reuse case.

    Path C is hotfix execution lane ONLY — it does NOT replace the Path B
    amendment lineage. When a done spec or closed origin sprint blocks bug
    execution, use the returned next_action (`assign_hotfix_lane` or
    `activate_hotfix_lane`) to put the bug and its regression test card on an
    active `lane_type="hotfix"` sprint. Cards are same-spec by default. The exact
    cross-spec Path B test task may join the original-spec lane only after its
    non-blocking complete amendment and persisted validator attestation bind the
    bug, task, scenario and revision spec. The lane only unblocks execution; the
    regression must still satisfy Path A reuse or the Path B amendment lineage
    above. Keep the original closed sprint unchanged.

## `okto_pulse_delete_card`

Delete a card from the board. This operation is permanent and cannot be undone.

## `okto_pulse_get_card`

Get detailed card information including attachments, Q&A, and comments.

## Card Knowledge assignments

The canonical contract for card Knowledge v2 replace, drop, refresh, read,
revision-CAS, and replay behavior lives at
`okto-pulse://reference/tool-docs/knowledge`.

## `okto_pulse_get_card_dependencies`

List cards that this card depends on and cards that depend on it.

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON with depends_on (blockers) and dependents (blocked by this card)

## `okto_pulse_list_blockers`

Triage view of everything stalling the funnel, with root-cause classification.

Every returned entry carries a `type` so the agent can act directly:

- `dependency_blocked` — card is active while at least one `depends_on`
  target is not DONE.
- `on_hold` — card is explicitly paused (status=on_hold).
- `stale` — card is started/in_progress/validation and hasn't been
  touched for more than `stale_hours`.
- `spec_pending_validation` — spec is approved but has no 'approve'
  evaluation yet, blocking promotion to in_progress.
- `spec_no_cards` — spec is validated/in_progress but has zero linked
  cards (implementation hasn't started).
- `uncovered_scenario` — test scenario has no linked test card, so the
  test-coverage gate will fail.

Args:
    board_id: Board ID
    stale_hours: Cards unchanged longer than this while active are flagged
        as stale (default 72, ≥1).
    filter_type: Optional — return only blockers of this type. Empty returns all.

Returns:
    JSON ``{summary: {<type>: count}, total: int, blockers: [...]}``

## `okto_pulse_list_cards_by_status`

List cards on the board with optional filters and pagination.

status: empty = all, or one of not_started/started/in_progress/validation/on_hold/done/cancelled/open.
Use 'open' for all cards NOT in done/cancelled. Max limit is 200.

## `okto_pulse_move_card`

Move a card to a different column/position on the board.

Moving to 'validation' or 'done' REQUIRES conclusion, completeness (0-100),
completeness_justification, drift (0-100), and drift_justification so the
reviewer can validate the claim. Use -1 for completeness/drift when no
execution report is required (e.g. moving to on_hold or started).

## `okto_pulse_remove_card_dependency`

Remove a dependency between two cards.

Args:
    board_id: Board ID
    card_id: The card that has the dependency
    depends_on_id: The card it depended on

Returns:
    JSON with success status

## `okto_pulse_update_card`

Update card details. Pass only the fields you want to change; omit the rest.

Multi-value fields (labels, test_scenario_ids, linked_test_task_ids) — formats:
okto-pulse://reference/multivalue. For bidirectional scenario linking, use
`okto_pulse_link_task(target_type="scenario", ...)`.

## Path B amendment revisions (cross-spec regression evidence)

These tools REMEDIATE the bug regression gate for Path B — they never skip or
override it (any `skip_gate`/`override_gate`/`bypass`/`force` field is rejected
with `gate_bypass_not_allowed`). They mirror the REST endpoints under
`/api/v1/boards/{board_id}/bugs/{bug_id}/amendment-revisions`. See the Path B
sequence in `workflows/cards.md` and the error codes in `reference/errors.md`.

## `okto_pulse_create_amendment_revision`

Create a Path B `AmendmentHotfixRevision` for a bug (REST twin: `POST
.../amendment-revisions`). The amendment binds to the bug's OWN **content-locked**
spec (`done`/`validated`, OR `in_progress` still content-locked by an active passed
validation — `current_validation_id` → outcome=success) and always starts as `draft`
— you cannot mint `approved`/`done` (`invalid_initial_status`) and you cannot inject
a coverage confirmation (that is the validator's job, non-forgeable). Args:
`board_id`, `bug_id`, optional `original_spec_id` (defaults to the bug's spec; a
mismatch is `bug_spec_mismatch`), `revision_spec_id`, `origin_task_ids`,
`affected_task_ids`, `regression_scenario_ids`, `regression_test_task_ids`,
`automated_regression_refs`. Rejects an `in_progress` spec that is NOT content-locked
— still editable, or with a `failed`/`stale`/`superseded` validation —
(`original_spec_not_done_or_locked`); edit the spec directly there.

## `okto_pulse_list_amendment_revisions`

List the bug's amendment revisions plus the bug-level Path B resolution (REST
twin: `GET .../amendment-revisions`). The `path_b_resolution` payload exposes
`coverage_state` (`coverage_pending` is NOT closure-ready), `missing_links`,
`safe_next_actions`, and rejected/eligible regression artifacts — enough to pick
the next safe action without reading raw errors. Read-only.

## `okto_pulse_get_amendment_revision`

Get one amendment revision scoped to the bug (REST twin: `GET
.../amendment-revisions/{amendment_id}`). A revision that does not belong to this
bug/board fails `amendment_bug_mismatch` — it never leaks as success. Read-only.

## `okto_pulse_associate_amendment_revision_artifacts`

Additively associate regression artifacts/evidence to an existing revision (REST
twin: `POST .../amendment-revisions/{amendment_id}/associate`). Args:
`board_id`, `bug_id`, `amendment_id`, and any of `regression_scenario_ids`,
`regression_test_task_ids`, `automated_regression_refs` (at least one, else
`no_artifacts_to_associate`). NEVER reparents `origin_bug_id`/`original_spec_id`;
audit-backed.

## `okto_pulse_transition_amendment_revision`

Promote an amendment's lifecycle for the bug (REST twin: `POST
.../amendment-revisions/{amendment_id}/lifecycle`) — the agent-facing step that
takes a created/associated revision to `approved`/`done` + `lineage_state=complete`
so the bug gate can reach `path_b_ready`. Args: `board_id`, `bug_id`,
`amendment_id`, and any of `status`, `lineage_state`. Fail-closed: unknown values
are rejected (`invalid_amendment_status`/`invalid_lineage_state`);
`lineage_state=complete` needs the declared regression scenario + test-task
artifacts and the bug's authoritative origin task (`incomplete_lineage_artifacts`);
`approved`/`done` need complete lineage (`cannot_promote_incomplete_lineage`).
A `cancelled`/`superseded` revision is permanently immutable
(`terminal_amendment_revision`): no later status, lineage, coverage, or artifact
association mutation is allowed. Only an exact retry of the current terminal
status is accepted as an effect-free idempotent operation; create a new revision
for further work. This tool has NO coverage parameter and **never** confirms
coverage — that stays validator-only via
`okto_pulse_confirm_amendment_coverage`, so on its own this tool leaves the bug
`coverage_pending` (it does not close it).

## `okto_pulse_confirm_amendment_coverage`

VALIDATOR-ONLY. Records the non-forgeable coverage attestation that lets the gate
treat a Path B regression artifact as closure-ready. Args: `board_id`,
`amendment_id`, `regression_test_task_id`, `regression_scenario_id` (the test task
+ scenario MUST be declared by the amendment). There is **no** `bug_id` argument —
the amendment already carries bug/board/original_spec. Fail-closed: the regression
test task must be `done` with its declared scenario `passed`/`automated` carrying
re-executable evidence (`test_file_path`+`test_function`, or an explicit replayable
evidence_class such as `mcp_replay_manifest` plus `expected_output_snapshot`) —
binding + validator authorization + reexecutable evidence are necessary but NOT
sufficient.

Gate-consumability preflight (BUG-01): BEFORE persisting, the tool runs the SAME
eligibility predicate the bug regression gate uses for this `(amendment_id,
regression_test_task_id, regression_scenario_id)`, so success implies the
attestation is persisted AND consumable by the gate. A **same-spec** scenario is
routed through **Path A** and is eligible ONLY when linked to the bug's
origin/affected-task lineage — an amendment declaration does NOT convert an
`unrelated_scenario` into valid Path B coverage. A **cross-spec** scenario is
routed through **Path B** and consumable only when the candidate attestation drives
`path_b_ready`. An inert tuple fails closed with `coverage_not_gate_consumable`
(see `reference/errors.md`), distinct from `coverage_pending`. Until a consumable
attestation is recorded, the bug stays `coverage_pending`.
