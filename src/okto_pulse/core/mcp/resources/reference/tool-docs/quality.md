---
version: "1.0"
---

# Quality Assessments

Quality assessments are immutable, version-bound receipts for an Ideation,
Refinement, or Spec. Their score and pinpointed findings are evidence; the
applicable lifecycle gate remains the authority for whether an entity may
advance.

Operational pre-flight, currentness, projection, and gate rules:
`okto-pulse://reference/quality-assessments`.

Supported assessment kinds are:

- Ideation: `ambiguity`
- Refinement: `ambiguity`
- Spec: native automatic `requirement_lint`; read-only migrated
  `spec_validation` audit evidence

The current head is not necessarily current for the entity. Always inspect
`currentness.current` and its ordered `stale_reasons`. A non-head receipt has
state `superseded`; the head has state `current` or `stale`. Content, Q&A,
subject-version, ruleset, taxonomy, or policy changes can stale a receipt.
Re-run the owning assessment or semantic writer instead of treating an old
score as current. For a Spec gate, use the live Validation flow rather than a
migrated `spec_validation` Quality receipt.

Assessment list tools use opaque keyset cursors ordered by
`created_at DESC, id DESC`. Do not decode or synthesize a cursor. Pass the
returned `next_cursor` with the same board, subject, kind, and filters.

When a parent entity listing explicitly uses the PageEnvelope projection, its
optional `quality_summaries` map is lean and keyed by assessment kind. Each summary is
limited to `receipt_id`, `subject_version`, `currentness`, `score`, `scale`,
and `head_revision`; it never embeds findings, evidence, questions, anchors,
or receipt bodies. The field is omitted entirely when the caller lacks that
entity type's Quality-read permission. Legacy array listing shapes remain
unchanged.

## `okto_pulse_record_ambiguity_assessment`

Record one immutable ambiguity assessment for an Ideation or Refinement. The
server owns `assessment_kind`, the 1–5 lower-is-better scale, the semantic
input digests, subject/anchor identity, versions, origin, and receipt IDs.
It also authors the receipt justification and fixes
`blocking_eligible=false`; clients must not provide or derive those fields.

An Ideation accepts this write only in `evaluating`; a Refinement accepts it
only in `approved`. Perform clarification and semantic edits earlier, then use
the assessment to verify the stable candidate immediately before its `done`
gate.

The write is compare-and-swap protected. Read the entity's full context and
the current ambiguity head immediately before calling. Use
`expected_head_revision=0` when no ambiguity receipt exists. An exact retry
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

Up to five proposed questions may accompany the assessment. They are
materialized atomically on the subject's Q&A board and linked back to their
finding keys. Prefer one focused question per ambiguity and use
`question_type="choice"` only with mutually exclusive choices. A failure
creates neither a receipt nor partial Q&A.

Args:
    board_id: Board ID
    subject_type: `ideation` or `refinement`
    subject_id: Subject ID inside the board
    idempotency_key: Caller-stable key for exact retries
    expected_subject_version: Version from the latest full subject context
    expected_head_revision: Current ambiguity-head revision, or 0 when absent
    score: Ambiguity score from 1 through 5; lower is better
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
    Success envelope with `receipt_id`, `head_revision`, `qa_id_map`, and
    `replayed`. Conflicts report a stable reason code and `retryable`; refresh
    context/head state before rebuilding a changed request.

Permissions:
    Requires subject write authority and
    `{subject_type}.quality.assess`. Materializing proposed questions also
    requires the subject's Q&A ask permission. An agent cannot set the
    human-owned ambiguity-gate skip.

## `okto_pulse_get_current_quality_assessment`

Read the current head for one supported subject/kind and evaluate it against
the entity's current semantic content and Q&A. "Current" here is a computed
freshness result, not merely the newest receipt.

Args:
    board_id: Board ID
    subject_type: `ideation`, `refinement`, or `spec`
    subject_id: Subject ID inside the board
    assessment_kind: `ambiguity`, `spec_validation`, or `requirement_lint`,
        using the supported subject/kind matrix at the top of this resource

Returns:
    Success envelope with the immutable receipt, head identity/revision, and
    `currentness` (`current` plus ordered `stale_reasons`). Returns
    `assessment_current_not_found` when that subject/kind has no head.

Permissions:
    Requires subject read authority and `{subject_type}.quality.read`.

## `okto_pulse_get_quality_assessment_receipt`

Read one immutable receipt by ID inside its board scope. The detailed result
preserves traceability from findings to any Q&A materialized with the write.
A historical receipt remains readable even after it becomes stale or
superseded.

Args:
    board_id: Board ID
    receipt_id: Quality assessment receipt ID

Returns:
    Success envelope containing the receipt and its computed `currentness`.
    Findings are intentionally paged separately with
    `okto_pulse_list_quality_findings`; receipt bodies never smuggle an
    unbounded finding collection. The receipt carries its frozen subject
    version, score/scale, digests, versions, predecessor, outcome, and
    provenance.

Permissions:
    Requires board/subject read authority and the matching
    `{subject_type}.quality.read` permission resolved from the receipt.

## `okto_pulse_list_quality_assessments`

List immutable assessment receipts for one subject using real keyset
pagination. Currentness is evaluated against the subject's state at read time.

Args:
    board_id: Board ID
    subject_type: `ideation`, `refinement`, or `spec`
    subject_id: Subject ID inside the board
    assessment_kind: Optional supported kind filter
    state: Optional `current`, `stale`, or `superseded` filter
    limit: Maximum entries, from 1 through 200 (default 50)
    cursor: Opaque `next_cursor` returned by the preceding page
    offset: Non-negative compatibility input for the first page. Once a
        cursor is present its keyset position is authoritative and a
        simultaneous non-zero offset is rejected

Returns:
    Success envelope with `items`, `offset`, `limit`, `total_filtered`,
    `total_overall`, `has_more`, `next_cursor`, and ordering. Every item
    includes its receipt, `is_head`, `state`, and `currentness`.

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
