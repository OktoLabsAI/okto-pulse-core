---
version: "1.0"
---

# Quality assessments, pinpointing, and currentness

This resource is the operational contract for SK-A Quality reads and writes.
Read it before recording ambiguity, interpreting a quality summary, using a
receipt in a gate decision, or paging findings.

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
| Spec | `requirement_lint` | Semantic Spec writer only | Same semantic write |

There is no independent Spec assessment mutation. The live Spec Validation
transaction remains authoritative and writes the Spec's validation history,
not a native Quality receipt. A `spec_validation` Quality receipt, when
present, is migrated audit evidence. Requirement lint inherits the authority
of the semantic Spec write.

## Mandatory pre-flight for a write

This sequence applies to agent-submitted Ideation/Refinement ambiguity writes.
Spec Quality has no agent write; its semantic writer owns requirement lint.

1. Read the subject's current full context and retain its version.
2. Read the current head for the assessment kind. Use head revision `0` when no
   receipt exists.
3. Check the existing domain-write permission and the matching
   `{subject}.quality.assess` leaf. Proposed questions additionally require the
   matching `{subject}.qa.ask`.
4. Generate a caller-stable idempotency key. Reuse it only for a byte-equivalent
   retry.
5. Submit `expected_subject_version` and `expected_head_revision`. A conflict is
   retryable only after refreshing both fences.

The server owns assessment kind, scale, origin/source/channel, digests,
authority digest, subject identity in anchors, receipt IDs, justification, and
`blocking_eligible=false`. A client supplying or attempting to override those
fields is invalid.

## Pinpoint findings and questions

Findings explain a score; they never create a second gate. Each finding uses a
stable `whole_artifact`, `field`, `structured_child`, or `qa` anchor. Array
indexes and mutable numeric path segments are forbidden. Evidence references
carry only source type, stable ID, version, and SHA-256.

An assessment may propose at most five questions. Automatic requirement
lint additionally deduplicates its proposals against the Spec's existing Q&A
by normalized question text (answered or not): re-issuing the advisory lint
receipt on every semantic write never re-materializes an already-asked
question. Lint questions embed the flagged item id in parentheses and the
requirement text itself, so a human can answer them from the Q&A surface
alone. Accepted questions and their
finding links are created atomically with the receipt, and the result maps each
caller `client_key` to the issued Q&A ID. Any failure leaves no partial receipt,
finding, question, head, history, event, or outbox mutation.

**After any semantic Spec write, review what the lint generated.** The write
that mutated the Spec also produced a fresh advisory `requirement_lint`
receipt. Before moving on, the writing agent MUST:

1. Fetch the current receipt
   (`okto_pulse_get_current_quality_assessment` with
   `assessment_kind="requirement_lint"`) and list its findings
   (`okto_pulse_list_quality_findings` filtered by the receipt id).
2. Triage every finding: either sharpen the flagged requirement text in the
   same working session, or leave the finding standing deliberately and say
   why in the conversation or the linked Q&A answer.
3. List the Spec's open Q&A (`okto_pulse_list_qa`) and account for every
   lint-proposed question: answer it with the decided criterion when the
   decision is yours to make, or leave it explicitly for the human — never
   ignore generated questions silently. Answering a question does not clear
   its finding; only editing the requirement text does.

## Currentness is computed, not inferred from head

The newest head can be stale. A receipt is current only when its subject version
and all five semantic input digests still match:

- content
- clarification/Q&A
- ruleset
- taxonomy
- policy identity

The closed stale-reason order is
`subject_version_changed`, `content_changed`, `clarification_changed`,
`ruleset_changed`, `taxonomy_changed`, `policy_changed`. Gate enablement and
threshold settings do not stale a receipt. Re-run the owning assessment or
semantic writer after fixing content or answering Q&A; never manually clear
staleness. Imported `spec_validation` receipts remain audit-only — use the live
Spec Validation record and gate for readiness.

## Reads, projections, and pagination

Quality reads require the existing subject-read authority AND the matching
`{subject}.quality.read` leaf. Receipt-global REST paths resolve their subject
without revealing cross-board existence.

An opt-in parent `PageEnvelope` may include only `quality_summaries`, keyed by
assessment kind. Each summary is restricted to `receipt_id`,
`subject_version`, `currentness`, `score`, `scale`, and `head_revision`.
Permission denial omits the entire field; it is never represented by `{}`.
Legacy array lists remain byte-equivalent.

REST detail lists use offset PageEnvelope pagination and limits `25|50|100`.
Core/MCP accept `offset >= 0`, limit `1..200`, and return a real opaque keyset
cursor ordered by `created_at DESC, id DESC`. Once a cursor is supplied, its
boundary is authoritative and cannot be combined with a non-zero offset.
`total_filtered` and `total_overall` are exact and independent of the window.

## Gate behavior and error parity

Ideation and Refinement ambiguity gates consume only a current ambiguity
receipt and the board's configured score threshold. Refinement evaluates
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
