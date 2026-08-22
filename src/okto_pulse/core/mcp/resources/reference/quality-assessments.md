---
version: "1.0"
---

# Quality assessments, pinpointing, and lifecycle results

This resource is the operational contract for SK-A Quality reads and writes.
Read it before recording ambiguity, interpreting a quality summary, using a
Current result in a gate decision, or paging findings. `receipt_id` remains a
technical API identifier; product language is Current and Previous results.

For the canonical Spec status-to-action sequence and the boundary between the
read-only Quality surface and actionable Validation surface, use
`okto-pulse://workflows/specs` under **Spec Quality — Canonical Agent Flow**.
This resource intentionally does not repeat those lifecycle steps.
Executable guideline findings are a separate evidence family governed by
`okto-pulse://reference/policy-compliance`.

## Subject and assessment matrix

| Subject | Kind | Writer | Allowed lifecycle |
|---|---|---|---|
| Ideation | `ambiguity` | REST/MCP ambiguity command | `evaluating` |
| Refinement | `ambiguity` | REST/MCP ambiguity command | `approved` |
| Spec | `spec_validation` | System legacy import | One-shot migration; audit only |
| Spec | `requirement_lint` | REST/MCP external agent command | `approved` |

The live Spec Validation transaction remains authoritative and writes the
Spec's validation history. A `spec_validation` Quality result, when present,
is migrated audit evidence. Requirement Lint is independently recorded by the
agent at `approved`; Core and Community never inspect a local repository or
run internal cognition.

## Mandatory pre-flight for a write

This sequence applies to agent-submitted ambiguity and Requirement Lint writes.

1. Read the subject's current full context and retain its version and edition.
   For Requirement Lint, call `okto_pulse_get_requirement_lint_preflight` and
   retain its edition-pinned ruleset digest and anchors.
2. Read the current head for the assessment kind. Use head revision `0` when no
   Current result exists for the edition.
3. Check the existing domain-write permission and the matching
   `{subject}.quality.assess` leaf.
4. Generate a caller-stable idempotency key. Reuse it only for a byte-equivalent
   retry.
5. Submit `expected_subject_version`, `expected_subject_edition`, and
   `expected_head_revision`. A conflict is retryable only after refreshing all
   fences. Requirement Lint also submits the exact `ruleset_digest` returned by
   its preflight.

The server owns assessment kind, scale, origin/source/channel, digests,
authority digest, subject identity in anchors, technical result IDs,
justification, and
`blocking_eligible=false`. A client supplying or attempting to override those
fields is invalid.

## Pinpoint findings and questions

Findings explain a score; they never create a second gate. Each finding uses a
stable `whole_artifact`, `field`, `structured_child`, or `qa` anchor. Array
indexes and mutable numeric path segments are forbidden. Evidence references
carry only source type, stable ID, version, and SHA-256.

Requirement Lint findings are advisory individually and in aggregate: their
presence, severity, and count do not independently block validation. Recording
one accepted Requirement Lint result for the Current Spec edition is still a
mandatory lifecycle step; “advisory findings” does not mean “optional result.”

An ambiguity assessment may propose at most five questions as immutable result
evidence. They never materialize or mutate Q&A. Requirement Lint does not
accept proposed questions. Any failure leaves no partial result, finding,
head, history, event, or outbox mutation.

## Lifecycle state and technical audit

Human validity is edition-based. A result matching the subject's current
edition is `current`; returning the subject to `draft` increments the edition
and makes earlier results `previous`. Legacy rows whose edition is SQL `NULL`
are `history_only`: they remain readable under Previous, are never backfilled,
and can never become Current.

Version and semantic digest differences remain available as technical audit
metadata. They do not create human-facing staleness inside one edition.
Imported `spec_validation` results remain audit-only; use the live Spec
Validation record and gate for readiness.

## Reads, projections, and pagination

Quality reads require the existing subject-read authority AND the matching
`{subject}.quality.read` leaf. Receipt-global REST paths resolve their subject
without revealing cross-board existence.

An opt-in parent `PageEnvelope` may include only `quality_summaries`, keyed by
assessment kind. Each summary contains `edition`, `state`
(`not_started|current`), `previous_count`, and `current_result`. The
`current_result` is absent for `not_started` and contains only `score` and
`scale` for `current`; it never exposes receipt or head mechanics.
Permission denial omits the entire field; it is never represented by `{}`.
Legacy array lists remain byte-equivalent.

REST detail lists use offset PageEnvelope pagination and limits `25|50|100`.
Core/MCP accept `offset >= 0`, limit `1..200`, and return a real opaque keyset
cursor ordered by `created_at DESC, id DESC`. Once a cursor is supplied, its
boundary is authoritative and cannot be combined with a non-zero offset.
`total_filtered` and `total_overall` are exact and independent of the window.

## Gate behavior and error parity

Ideation and Refinement ambiguity gates consume only a Current ambiguity
result and the board's configured score threshold. Refinement evaluates
Ambiguity before Resource before Cognitive. A human-only Refinement skip may
bypass Ambiguity alone; there is no MCP skip and no agent permission for it.

REST and MCP expose the same semantic reason. Generic category codes remain
`forbidden`, `version_conflict`, and `validation_failed`. The two declared,
bounded contract conditions `invalid_pagination` and
`question_budget_exceeded` remain top-level public codes as well as
`details.reason_code`; other detailed safe reasons stay in
`details.reason_code`. Unsupported subject/kind pairs are rejected before any
read. Refresh on a retryable conflict, fix payload or authority on a
non-retryable error, and never retry by weakening a gate.

## Related resources

- `okto-pulse://workflows/preflight`
- `okto-pulse://workflows/ideations`
- `okto-pulse://workflows/refinements`
- `okto-pulse://workflows/specs`
- `okto-pulse://reference/spec_gates`
- `okto-pulse://reference/projection-profiles`
- `okto-pulse://reference/errors`
- `okto-pulse://reference/tool-docs/quality`
