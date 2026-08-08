---
version: "1.0"
---

# Tool docs — `guideline`

Canonical versioned-guideline, compliance, waiver, cursor and authority rules
are linked once in the governed-policy section below.

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_create_guideline`

Create a guideline identity and immutable initial revision (`1.0.0`). If scope
is "global", it goes into the catalog; if scope is "inline", set a board_id.
Requires `guidelines.revisions.create`. This compatibility payload cannot
author executable rules.

Args:
    board_id: Board ID (used for authentication; also used as guideline board_id if scope is "inline")
    title: Guideline title
    content: Guideline content (Markdown supported)
    tags: Pipe-separated tags (e.g. "coding|architecture") — empty = no tags
    scope: "global" (catalog) or "inline" (board-specific)

Returns:
    JSON with created guideline

## `okto_pulse_delete_guideline`

Compatibility name for retiring a guideline. Retirement is terminal for new
adoptions, but immutable revisions, binding lineage and audit evidence remain.
Requires `guidelines.revisions.retire`.

Args:
    board_id: Board ID (used for authentication)
    guideline_id: Guideline ID to retire

Returns:
    JSON with success status

## `okto_pulse_get_board_guidelines`

Get all guidelines for a board, ordered by priority. This is the PRIMARY tool
for reading board guidelines — call it BEFORE doing any work on a board.

Returns linked global guidelines and inline board guidelines merged and sorted.

Args:
    board_id: Board ID

Returns:
    JSON with guidelines sorted by ascending priority (lower values first).

## `okto_pulse_link_guideline_to_board`

Deprecated direct-adoption shim. It checks `guidelines.adoption.manage` and
returns `guideline_impact_preview_required` without mutation. Use
`okto_pulse_preview_guideline_impact`, then
`okto_pulse_adopt_guideline_revision` with the exact receipt and digest.

Args:
    board_id: Board ID
    guideline_id: Guideline ID to link
    priority: Non-negative order; lower values are evaluated first (default 0).

Returns:
    Typed migration error with `next_action=preview_then_adopt`.

## `okto_pulse_list_guidelines`

List global guidelines from the catalog. Use this to browse available guidelines
that can be linked to boards.

Args:
    board_id: Board ID (used for authentication)
    offset: Pagination offset (default 0)
    limit: Max results (default 50)
    tag: Optional tag filter (empty = all)

Returns:
    JSON with list of global guidelines

## `okto_pulse_list_default_guideline_candidates`

List GLOBAL catalog guidelines with derived eligibility, exact head revision
authority and current default pin from the umbrella template. REST twin:
GET /guidelines/default-candidates.

Only global guidelines can become defaults; inline board guidelines are never
eligible.

Args:
    board_id: Board ID used for authentication.
    scope: Template scope (default `global`).
    template_id: Optional — inspect a specific template version; empty uses
        the active template.

Returns:
    JSON with candidate guidelines and current default selection state.

## `okto_pulse_update_default_guideline_refs`

Update a template's `guideline_default_refs` using only GLOBAL catalog
guidelines. REST twin: POST
/default-board-configurations/{template_id}/guidelines. Requires
`guidelines.adoption.manage`.

Inline/missing/non-global refs are rejected fail-closed (structured error).
An ACTIVE template is copy-on-write: a new version is created and activated;
a draft mutates in-place.

Args:
    board_id: Board ID used for authentication.
    template_id: Default board-configuration template ID.
    guideline_default_refs: List of exact immutable pins containing
        `guideline_id`, `revision_id`, `revision_number`, `semantic_version`,
        `revision_digest`, and optional `priority`. An empty list clears the
        defaults.

Returns:
    JSON with the EFFECTIVE template (including its default guideline refs).

## `okto_pulse_update_board_guideline_priority`

Deprecated direct-adoption shim. It checks `guidelines.adoption.manage` and
returns `guideline_impact_preview_required` without mutation. Preview the
proposed priority and then adopt using the exact receipt.

Args:
    board_id: Board ID
    guideline_id: Linked guideline ID
    priority: New non-negative order; lower values are evaluated first.

Returns:
    Typed migration error with `next_action=preview_then_adopt`.

## `okto_pulse_unlink_guideline_from_board`

Unlink a guideline from a board as an append-only terminal binding event.
Requires `guidelines.adoption.manage`; the guideline and history are retained.

Args:
    board_id: Board ID
    guideline_id: Guideline ID to unlink

Returns:
    JSON with success status

## `okto_pulse_update_guideline`

Compatibility façade that appends an immutable guideline revision when title,
content or tags change. Requires `guidelines.revisions.create`; a no-op keeps
the current head. Semantic metrics require the governed revision tool.

Args:
    board_id: Board ID (used for authentication)
    guideline_id: Guideline ID to update
    title: New title (empty = no change)
    content: New content (empty = no change)
    tags: New pipe-separated tags (empty = no change)

Returns:
    JSON with updated guideline

## Governed policy tool conventions

The normative lifecycle, authority, projection, cursor, currentness and error
rules live only in `okto-pulse://reference/policy-compliance`. The sections
below document bounded tool signatures and response shapes.

## `okto_pulse_list_guideline_revisions`

List immutable revisions for one guideline.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline head.
    limit: Page size from 1 through 200.
    cursor: Optional opaque `revision` cursor.
    profile: `summary` or `detail`.

Returns:
    Outcome with ordered revisions and optional `next_cursor`.

## `okto_pulse_get_guideline_revision`

Read one exact immutable guideline revision.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline head.
    revision_id: Exact revision identity.

Returns:
    Outcome with revision and current authority context.

## `okto_pulse_create_guideline_revision`

Create the next semantic revision from a closed patch. A patch that supplies
metrics also requires `guidelines.metrics.author`. Metric semantics and SemVer
rules live only in the canonical protocol linked above.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline head.
    idempotency_key: Retry-stable client key.
    patch: Closed semantic patch; no authoritative metadata.
    declared_semantic_version: Optional version assertion.

Returns:
    Outcome with created revision, semantic bump and replay state.

## `okto_pulse_retire_guideline`

Retire or supersede a guideline. `superseded` requires a successor;
`retired` forbids one.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline to retire.
    status: `retired` or `superseded`.
    reason: Auditable bounded rationale.
    idempotency_key: Retry-stable client key.
    superseded_by_guideline_id: Required only for `superseded`.

Returns:
    Outcome with server-issued retirement evidence.

## `okto_pulse_preview_guideline_impact`

Create immutable impact evidence before an adoption.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline to evaluate.
    proposed_priority: Non-negative proposed priority.
    proposed_enforcement: `advisory` or `blocking`.
    proposed_minimum_confidence: Board-effective confidence floor, 0 through 100.
    proposed_metric_threshold_overrides: Optional metric-code threshold map.
    idempotency_key: Retry-stable client key.
    to_revision_id: Optional exact target revision.

Returns:
    Outcome with receipt, digest and bounded impact summary.

## `okto_pulse_get_guideline_impact`

Read one immutable impact receipt.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline represented by the receipt.
    impact_receipt_id: Server-issued receipt identity.

Returns:
    Outcome with the immutable impact evidence.

## `okto_pulse_list_guideline_impact_items`

Page exact targets and artifacts captured by an impact receipt.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline represented by the receipt.
    impact_receipt_id: Server-issued receipt identity.
    limit: Page size from 1 through 200.
    cursor: Optional opaque `impact` cursor.
    entity_type: Optional closed entity-type filter.
    item_kind: Optional `binding`, `target`, `artifact`, or `waiver`.
    profile: `summary` or `detail`.

Returns:
    Outcome with impact items and optional `next_cursor`.

## `okto_pulse_adopt_guideline_revision`

Adopt exactly the revision proven by a server-issued impact receipt.

Args:
    board_id: Authenticated board scope.
    guideline_id: Guideline to adopt.
    impact_receipt_id: Server-issued impact receipt.
    impact_digest: SHA-256 digest returned with that receipt.
    idempotency_key: Retry-stable client key.

Returns:
    Outcome with the new exact-revision binding.

## `okto_pulse_record_semantic_guideline_assessment`

Record complete externally-produced semantic metric evidence. The normative
assessment and gate protocol lives only in the canonical protocol linked
above.

Args:
    board_id: Authenticated board scope.
    entity_type: Closed governed entity type.
    subject_id: Exact entity identity.
    expected_subject_version: Exact current entity version fence.
    binding_id: Exact adopted binding identity.
    expected_binding_revision: Exact binding revision fence.
    guideline_revision_id: Exact adopted guideline revision fence.
    idempotency_key: Retry-stable client key.
    confidence: Compulsory whole-assessment confidence, 0 through 100.
    metric_results: Complete score, rationale, evidence and pinpoint set.
    model_id: Optional assessor model identifier.

Returns:
    Outcome with one atomically sealed receipt and all metric results.

## `okto_pulse_list_semantic_guideline_assessments`

Page immutable semantic assessment receipts with derived currentness.

Args:
    board_id: Authenticated board scope.
    limit: Page size from 1 through 200.
    cursor: Optional signed `semantic_assessment` keyset cursor.
    entity_type, subject_id: Optional exact subject filters.
    guideline_id, binding_id: Optional exact policy filters.
    outcome: Optional `passed` or `metric_threshold_failed`.
    currentness: Optional `current` or `stale`; scanning remains keyset-safe.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with one homogeneous projection page and optional `next_cursor`.

## `okto_pulse_get_semantic_guideline_assessment`

Read one immutable semantic assessment receipt by identity.

Args:
    board_id: Authenticated board scope.
    receipt_id: Server-issued receipt identity.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with currentness derived against authoritative live fences.

## `okto_pulse_get_current_semantic_guideline_assessment`

Read the current receipt for one exact subject and adopted binding.

Args:
    board_id: Authenticated board scope.
    entity_type, subject_id: Exact governed subject.
    binding_id: Exact adopted binding.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with the current receipt or a canonical not-found error.

## `okto_pulse_list_semantic_guideline_findings`

Page independently addressable failed-metric findings.

Args:
    board_id: Authenticated board scope.
    limit: Page size from 1 through 200.
    cursor: Optional signed `semantic_finding` keyset cursor.
    receipt_id, guideline_id, binding_id, metric_id: Optional exact filters.
    entity_type, subject_id: Optional exact subject filters.
    outcome: Optional closed metric outcome.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with findings, honest currentness, and optional `next_cursor`.

## `okto_pulse_list_semantic_guideline_waivers`

Page semantic metric-waiver heads at one explicit evaluation time.

Args:
    board_id: Authenticated board scope.
    evaluated_at: Authoritative time for expiry/currentness projection.
    limit: Page size from 1 through 200.
    cursor: Optional signed `semantic_waiver` keyset cursor.
    finding_id, metric_result_id, receipt_id: Optional anchor filters.
    guideline_id, binding_id, metric_id: Optional policy filters.
    entity_type, subject_id: Optional exact subject filters.
    status: Optional closed lifecycle status.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with one homogeneous projection page and optional `next_cursor`.
    Status is effective as of `evaluated_at`: an approved immutable head past
    its deadline projects as `expired` without mutating its ledger history.
    The `full` projection includes the permanent assessor fence, sealed
    digests and the last event's idempotency key.

## `okto_pulse_get_semantic_guideline_waiver`

Read one semantic metric-waiver head at an explicit evaluation time.

Args:
    board_id: Authenticated board scope.
    waiver_id: Server-issued waiver identity.
    evaluated_at: Authoritative time for expiry/currentness projection. Reuse
        the collection snapshot when opening a listed waiver.
    profile: Closed `summary`, `detail`, or `full` projection.

Returns:
    Outcome with the effective lifecycle status as of `evaluated_at`, exact
    anchor/currentness and full fences when requested. Scheduled expiry is
    projected consistently with the waiver collection without mutating the
    immutable ledger head.
    The `full` projection includes the last event's idempotency key.

## `okto_pulse_list_semantic_guideline_waiver_events`

Read the append-only event history for one semantic metric waiver.

Args:
    board_id: Authenticated board scope.
    waiver_id: Server-issued waiver identity.

Returns:
    Outcome with ordered immutable events, including revalidation decisions.

## `okto_pulse_request_semantic_guideline_waiver`

Request a bounded waiver for one exact, current failed metric anchor.

Args:
    board_id: Authenticated board scope.
    metric_result_id, finding_id, receipt_id: Required exact anchor triple.
    justification: Auditable rationale.
    evidence_refs: Non-empty structured evidence references.
    expires_at: Optional requested bounded expiry.
    idempotency_key: Retry-stable client key.

Returns:
    Outcome with the requested waiver and append-only request event.

## `okto_pulse_review_semantic_guideline_waiver`

Approve or reject a requested semantic waiver independently.

Args:
    board_id: Authenticated board scope.
    waiver_id: Server-issued waiver identity.
    decision: `approve` or `reject`.
    expected_waiver_revision: Required compare-and-swap precondition.
    reason: Auditable rationale.
    evidence_refs: Non-empty structured evidence references.
    idempotency_key: Retry-stable client key.

Returns:
    Outcome with the updated waiver head and append-only review event.

## `okto_pulse_revoke_semantic_guideline_waiver`

Revoke an approved semantic waiver.

Args:
    board_id: Authenticated board scope.
    waiver_id: Server-issued waiver identity.
    expected_waiver_revision: Required compare-and-swap precondition.
    reason: Auditable rationale.
    evidence_refs: Non-empty structured evidence references.
    idempotency_key: Retry-stable client key.

Returns:
    Outcome with the revoked waiver head and append-only event.

## `okto_pulse_revalidate_semantic_guideline_waiver`

Append an independent currentness decision without rebinding or automatically
reactivating the waiver.

Args:
    board_id: Authenticated board scope.
    waiver_id: Server-issued waiver identity.
    expected_waiver_revision: Required compare-and-swap precondition.
    evaluated_at: Authoritative evaluation time.
    idempotency_key: Retry-stable client key.

Returns:
    Exact `{waiver_id, waiver_revision, status, current, reason_code,
    replayed}` where status is `approved`, `expired`, `anchor_stale`, or
    `revoked`. Detailed drift reasons remain in the append-only event/full
    projection.
