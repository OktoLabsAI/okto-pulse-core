---
version: "1.0"
---

# Quality Assessments

Quality assessments are immutable, edition-bound results for an Ideation,
Refinement, or Spec. Their score and pinpointed findings are evidence; the
applicable lifecycle gate remains the authority for whether an entity may
advance. `receipt_id` and tool names containing `receipt` are compatibility API
terms, not product concepts.

Operational pre-flight, currentness, projection, and gate rules:
`okto-pulse://reference/quality-assessments`.

Supported assessment kinds are:

- Ideation: `ambiguity`
- Refinement: `ambiguity`
- Spec: externally recorded `requirement_lint` at `approved`; read-only
  migrated `spec_validation` audit evidence

Each lifecycle edition has at most one current head. Product flows use
`lifecycle_state`: `current`, `previous`, or legacy `history_only`. Returning
the subject to `draft` starts a new edition and moves the old result into
Previous. A legacy row with SQL `NULL` edition is always `history_only`, stays
readable under Previous, is never backfilled, and can never become Current.
Technical currentness reasons remain audit metadata and do not make ordinary
row-version or policy drift look like a new human validation cycle.

Assessment list tools use opaque keyset cursors ordered by
`created_at DESC, id DESC`. Do not decode or synthesize a cursor. Pass the
returned `next_cursor` with the same board, subject, kind, and filters.

When a parent entity listing explicitly uses the PageEnvelope projection, its
optional `quality_summaries` map is lean and keyed by assessment kind. Each
summary contains `edition`, `state` (`not_started|current`), `previous_count`,
and `current_result`. The result is absent for `not_started` and contains only
`score` and `scale` for `current`; it never embeds findings, evidence,
questions, anchors, receipt IDs, or head mechanics. The field is omitted
entirely when the caller lacks that entity type's Quality-read permission.
Legacy array listing shapes remain unchanged.

## `okto_pulse_record_ambiguity_assessment`

Record one immutable ambiguity assessment for an Ideation or Refinement. The
server owns `assessment_kind`, the 1–5 lower-is-better scale, the semantic
input digests, subject/anchor identity, versions, origin, and technical result
IDs. It also authors the result justification and fixes
`blocking_eligible=false`; clients must not provide or derive those fields.

An Ideation accepts this write only in `evaluating`; a Refinement accepts it
only in `approved`. Perform clarification and semantic edits earlier, then use
the assessment to verify the stable candidate immediately before its `done`
gate.

The write is compare-and-swap protected. Read the entity's full context and
the current ambiguity head immediately before calling. Use
`expected_head_revision=0` when no Current ambiguity result exists. An exact retry
must reuse the same `idempotency_key` and payload; never reuse that key for a
changed assessment.

Each finding pinpoints ambiguity with a stable anchor:

- `whole_artifact`: omit `anchor_ref`
- `field`: use a stable semantic field name
- `structured_child`: use the child's stable ID
- `qa`: use the Q&A item's stable ID

Array positions such as `requirements[2]`, numeric path segments, and other
mutable indexes are rejected. `category_code` must be an exact category from
`ambiguity_taxonomy/v1`. Findings are explanatory evidence and are never
independently blocking; the current score and board gate policy decide the
transition.

Up to five proposed questions may accompany the assessment as immutable result
evidence. They never create or mutate the subject's Q&A board. Prefer
one focused question per ambiguity and use `question_type="choice"` only with
mutually exclusive choices.

Args:
    board_id: Board ID
    subject_type: `ideation` or `refinement`
    subject_id: Subject ID inside the board
    idempotency_key: Caller-stable key for exact retries
    expected_subject_version: Version from the latest full subject context
    expected_subject_edition: Lifecycle edition from that same context
    expected_head_revision: Current ambiguity-head revision, or 0 when absent
    score: Ambiguity score from 1 through 5; lower is better
    summary: Concise human-readable explanation of the result
    findings: Native array of pinpoint finding objects. Each object supplies
        `finding_key`, `category_code`, `severity`, `confidence`,
        `deterministic`, `title`, `detail`, optional `remediation`,
        optional `rule_code`, optional `evidence_refs`, and an `anchor`
        containing `anchor_type`, optional `anchor_ref`, and optional
        `excerpt_hash`. Each evidence reference contains `source_type`,
        `source_id`, `source_version`, and `content_hash`.
    proposed_questions: Native array of at most five objects with
        `client_key`, `question`, `question_type`, optional `choices`,
        `allow_free_text`, optional `category_code`, and optional
        `finding_keys`

Returns:
    Success envelope with technical `result_id`, `subject_edition`, and
    `status="accepted"`. Conflicts report a stable reason code and `retryable`;
    refresh context/head state before rebuilding a changed request.

Permissions:
    Requires subject write authority and
    `{subject_type}.quality.assess`. An agent cannot set the human-owned
    ambiguity-gate skip.

## `okto_pulse_record_requirement_lint`

Record one immutable Requirement Lint assessment produced by the external
agent for the current Spec edition. The Spec must be `approved`; Pulse Core and
Community do not inspect a local repository or run an internal analyzer.

First call `okto_pulse_get_requirement_lint_preflight`. Use its exact
optimistic fences, edition-pinned `ruleset_digest`, and anchors.
`expected_head_revision=0` means no result exists for this edition. Findings
use the same stable anchors and evidence shape as ambiguity findings. Proposed
questions are intentionally unsupported. Findings are advisory: neither their
presence, severity, nor count independently blocks validation. The accepted
Requirement Lint result itself is mandatory for the Current edition.

Args:
    board_id: Board ID
    spec_id: Spec ID inside the board
    idempotency_key: Caller-stable key for exact retries
    expected_subject_version: Version from the latest Spec context
    expected_subject_edition: Lifecycle edition from the preflight
    expected_head_revision: Current edition head revision, or 0 when absent
    ruleset_digest: Exact edition-pinned digest from the preflight
    score: External agent's bounded overall lint score
    summary: Concise human-readable explanation of the overall result
    evaluated_rule_count: Optional positive count of evaluated rules
    findings: Native array of pinpoint finding objects

Returns:
    Success envelope with technical `result_id`, `subject_edition`,
    `status="accepted"`, and `idempotent_replay`.

Permissions:
    Requires Spec write authority and `spec.quality.assess`.

## `okto_pulse_get_requirement_lint_preflight`

Read the approved Spec's edition-pinned Requirement Lint anchors, ruleset
identity, and submission fence. This tool never analyzes repository content
and never opens a writable unit of work. After a Current result exists, its
ruleset remains pinned until the Spec returns to Draft.

Args:
    board_id: Board ID
    spec_id: Spec ID inside the board

Returns:
    `assessment_kind`, `subject_edition`, `subject_status`, `ruleset_digest`,
    `requirement_anchors`, and `submission_fence` with
    `expected_subject_edition`, `expected_subject_version`, and
    `expected_head_revision`.

Permissions:
    Requires Board read authority and `spec.quality.read`.

## `okto_pulse_get_current_quality_assessment`

Read the Current result for one supported subject/kind in the active lifecycle
edition.

Args:
    board_id: Board ID
    subject_type: `ideation`, `refinement`, or `spec`
    subject_id: Subject ID inside the board
    assessment_kind: `ambiguity`, `spec_validation`, or `requirement_lint`,
        using the supported subject/kind matrix at the top of this resource

Returns:
    Success envelope with the immutable result and compatibility receipt/head
    fields. Product state is Current for the active edition; ordered
    `stale_reasons` are technical audit metadata only. Returns
    `assessment_current_not_found` when that subject/kind has no head.

Permissions:
    Requires subject read authority and `{subject_type}.quality.read`.

## `okto_pulse_get_quality_assessment_receipt`

Read one immutable result by its compatibility `receipt_id` inside its board scope. The detailed result
preserves traceability from findings to any Q&A materialized with the write.
A result from an earlier edition remains readable under Previous. A legacy
result with SQL `NULL` edition is history-only and can never become Current.

Args:
    board_id: Board ID
    receipt_id: Quality assessment receipt ID

Returns:
    Success envelope containing the result and its lifecycle projection.
    Findings are intentionally paged separately with
    `okto_pulse_list_quality_findings`; receipt bodies never smuggle an
    unbounded finding collection. The receipt carries its frozen subject
    version, score/scale, digests, versions, predecessor, outcome, and
    provenance.

Permissions:
    Requires board/subject read authority and the matching
    `{subject_type}.quality.read` permission resolved from the receipt.

## `okto_pulse_list_quality_assessments`

List immutable assessment results for one subject using real keyset
pagination. Human lifecycle state is derived from the subject edition.

Args:
    board_id: Board ID
    subject_type: `ideation`, `refinement`, or `spec`
    subject_id: Subject ID inside the board
    assessment_kind: Optional supported kind filter
    state: Compatibility filter: `current`, `stale`, or `superseded`.
        Product surfaces group every non-current row under Previous; use the
        returned lifecycle state for human decisions.
    limit: Maximum entries, from 1 through 200 (default 50)
    cursor: Opaque `next_cursor` returned by the preceding page
    offset: Non-negative compatibility input for the first page. Once a
        cursor is present its keyset position is authoritative and a
        simultaneous non-zero offset is rejected

Returns:
    Success envelope with `items`, `offset`, `limit`, `total_filtered`,
    `total_overall`, `has_more`, `next_cursor`, and ordering. Every item
    includes its compatibility receipt payload, `is_head`, technical `state`,
    and lifecycle projection.

Permissions:
    Requires subject read authority and `{subject_type}.quality.read`.

## `okto_pulse_list_quality_findings`

List pinpointed findings for one subject using real keyset pagination.
Findings are ordered by `created_at DESC, id DESC` and retain their stable
anchor and receipt provenance.

Args:
    board_id: Board ID
    subject_type: `ideation`, `refinement`, or `spec`
    subject_id: Subject ID inside the board
    receipt_id: Optional receipt filter
    assessment_kind: Optional supported kind filter
    category_code: Optional exact taxonomy category filter
    severity: Optional `info`, `low`, `medium`, `high`, or `critical`
    limit: Maximum entries, from 1 through 200 (default 50)
    cursor: Opaque `next_cursor` returned by the preceding page
    offset: Non-negative compatibility input for the first page. Once a
        cursor is present its keyset position is authoritative and a
        simultaneous non-zero offset is rejected

Returns:
    Success envelope with `items`, `offset`, `limit`, `total_filtered`,
    `total_overall`, `has_more`, `next_cursor`, and ordering. Each item
    includes immutable finding content, lifecycle, stable anchor, evidence
    references, receipt ID, and assessment kind.

Permissions:
    Requires subject read authority and `{subject_type}.quality.read`.
