---
version: "1.0"
contract: "policy-compliance-resource/v1"
---

# Versioned guidelines and policy compliance

This is the canonical agent protocol for executable board guidelines,
immutable guideline revisions, impact evidence, adoption/unlink, compliance
receipts and governed waivers. Read it before changing a guideline revision,
adopting policy, evaluating an SDLC entity, relying on a receipt at a
transition, or operating a waiver.

Guideline prose remains useful context. Only structured `policy/v1` rules are
executable. The relational policy authority is canonical; KG constraint nodes
are a derived, rebuildable projection.

## Authority and pre-flight

1. Resolve the exact board and read its guidelines.
2. Read the target entity's current context before evaluation or transition.
3. Use a caller-stable idempotency key for each mutation. Reuse it only for a
   byte-equivalent retry.
4. For a list, start with `profile="summary"` and no cursor. Follow the returned
   cursor with identical filters and profile.
5. Treat every receipt and impact preview as immutable evidence. Never edit,
   synthesize, or reinterpret its digests.

The server owns revision/event/receipt/waiver IDs, timestamps, actor identity,
head and binding revisions, authority digests, policy-set digests and
currentness computation. The client may supply only two evidence
preconditions:

- `impact_receipt_id` plus its exact `impact_digest` when adopting;
- `expected_waiver_revision` for waiver compare-and-swap.

These are optimistic preconditions, not client-authored authority. On conflict,
refresh and reconsider; do not silently replace them with newly read values
inside an idempotent replay.

## Revision lifecycle

- `okto_pulse_list_guideline_revisions` returns immutable history with
  `summary|detail` projections and keyset pagination.
- `okto_pulse_get_guideline_revision` returns one revision with the current
  guideline head and retirement context.
- `okto_pulse_create_guideline_revision` applies a partial patch. Omitted fields
  remain unchanged; empty `tags` or `rules` intentionally clears that
  collection. A no-op consumes the idempotency key without manufacturing a
  revision.
- A semantic version below the deterministic minimum fails with `under_bump`;
  use the returned minimum as guidance and submit a new intent.
- `okto_pulse_retire_guideline` records a terminal tombstone. `superseded`
  requires a different successor guideline; `retired` forbids one.

Do not use legacy hard deletion to model lifecycle. Retirement preserves
history and unlink preserves the board binding lineage.

## Rule authoring

A revision with `rules=[]` is valid context-only guidance. Do not invent an
executable rule merely to make a guideline adoptable. Add `policy/v1` rules
only when the requirement can be checked deterministically.

For each rule, keep `rule_id` stable across revisions and use `code` as its
readable audit key. Set explicit target entity types before choosing
predicates, because the closed fact catalog is target-aware. Presence
operators (`exists`, `not_exists`) take no value. Every other operator must
use the typed parameter shape documented by the guideline tools.

Use `policy_class="standard"` for ordinary rules. The protected classes
`coverage`, `permissions`, `reviewer_separation`, and `lineage` are
non-waivable. Enforcement belongs to each rule: `advisory` records a finding
without blocking, while `blocking` participates in supported transition
gates.

## Impact, adoption, and unlink

Always preview before adoption:

1. Call `okto_pulse_preview_guideline_impact` with priority, the reserved
   compatibility field `proposed_default_enforcement`, and an optional exact
   target revision. For a new binding pass `advisory`; for an existing binding
   preserve its current value. This field does not override rule-level
   enforcement.
2. Inspect the receipt and page
   `okto_pulse_list_guideline_impact_items` when any affected count is nonzero.
3. If the impact is acceptable, call `okto_pulse_adopt_guideline_revision`
   using the receipt ID and digest exactly as returned.
4. If the preview is stale, create a new preview; never patch old evidence.

Use `okto_pulse_unlink_guideline_from_board` to stop applying one board
guideline without deleting its identity or history. Adoption, unlink and
retirement emit immutable policy-change events. Their derived KG projection
ends superseded constraints instead of erasing provenance.

## Compliance evaluation and currentness

Executable targets are the closed set `ideation`, `refinement`, `spec`,
`sprint`, `card`, and `test_scenario`.

`okto_pulse_evaluate_policy_compliance` snapshots the current subject and
current board policy, evaluates only applicable structured rules, and records
one immutable receipt plus pinpoint findings. Use:

- `okto_pulse_get_current_policy_compliance_receipt` for the current receipt of
  one exact subject;
- `okto_pulse_get_policy_compliance_receipt` for historical evidence;
- `okto_pulse_list_policy_compliance_receipts` for filtered history;
- `okto_pulse_list_policy_compliance_findings` for rule-level diagnosis.

Currentness is computed against subject version and policy identity. A newer
head can still be stale. A stale receipt is audit evidence only and never
authorizes a transition. Fix the subject or policy, then evaluate again.

Blocking rules participate in the existing transition decision for their
target entity. Advisory findings remain visible but do not block. The policy
gate does not replace entity-specific ambiguity, Resource, checklist,
validation, evaluation, test or cognitive gates; all applicable gates must
pass.

## Waiver lifecycle and separation

A waiver is bound to one current, waivable finding:

1. Request with `okto_pulse_request_policy_waiver`, a justification, evidence
   references and an expiry.
2. An independent authorized reviewer uses
   `okto_pulse_review_policy_waiver` with `approve|reject`.
3. Read the head and append-only events before a later mutation.
4. Revoke with `okto_pulse_revoke_policy_waiver`, or revalidate against current
   source evidence with `okto_pulse_revalidate_policy_waiver`.

Approved is not synonymous with effective. A waiver is effective only while
approved, unexpired and still bound to current subject/guideline/rule evidence.
Source drift makes it ineffective and requires explicit revalidation.
Revalidation is privilege-granting too: the actor must still be independent
from the original requester. A requester cannot restore their own exception.
Protected policy classes are never waivable.

## Projections and keyset pagination

Paginated policy list tools accept only `summary|detail`, default `summary`,
with limits `1..200`. They use one shared signed opaque keyset cursor. There is
no offset fallback. The append-only waiver event history is a bounded
non-paginated read.

- Never decode, edit, persist as authority, or reuse a cursor with different
  filters/profile.
- `next_cursor=null` and `has_more=false` end the traversal.
- For waiver lists, repeat the same explicit `evaluated_at` on every page.
- `summary` omits bodies, reasons and evidence; request `detail` only for the
  bounded page that needs them.

## MCP outcome and errors

All governed tools return the native MCP outcome v2 envelope. Check `outcome`
before reading `data`. On error use `error_code`, `retryable`, `next_action`
and bounded `details`; do not parse prose.

Common actions:

- `permission_denied`: request the exact capability; retrying unchanged is not
  recovery.
- `policy_waiver_independent_reviewer_required`: assign an authorized actor
  other than the requester for review or revalidation.
- `invalid_cursor`: restart pagination without a cursor.
- `conflict`: refresh the exact authority/precondition and reconsider.
- `under_bump`: increase the declared semantic version.
- `guideline_impact_no_changes`: the exact revision and board configuration
  are already active; do not retry unless the intended revision or priority
  changes.
- `service_unavailable`: report or retry without changing the intent.

## Capabilities

Reads and mutations fail closed on their dedicated leaves:

| Area | Capabilities |
|---|---|
| Revisions | `guidelines.revisions.read`, `.create`, `.retire` |
| Blocking-rule authoring | `guidelines.rules.author_blocking` |
| Impact and adoption | `guidelines.impact.preview`, `guidelines.adoption.manage` |
| Compliance | `guidelines.compliance.read`, `.evaluate` |
| Waivers | `guidelines.waiver.read`, `.request`, `.review`, `.revoke`, `.revalidate` |

Capability denial occurs before the policy unit of work is entered. Presets and
Full Control receive introduced leaves through the versioned permission
manifest; absent or partial custom authority remains denied.

## KG projection and recovery

Policy-change delivery materializes deterministic active constraint nodes for
the exact adopted revision. Adopt activates the new rule set and ends replaced
constraints; unlink and retirement end the affected active set. Replays are
idempotent. Relational receipts, findings, revisions and event evidence remain
the source of truth. This projection lives only in the board graph and is not
published to the Global Discovery index.

If the graph projection is missing or inconsistent, diagnose KG health and use
the normal board rebuild workflow. Rebuild inventories canonical relational
policy state and must fail before reporting success if policy-constraint
projection fails. Never repair a constraint by editing the graph directly.

Related operational references:
`okto-pulse://reference/transitions`,
`okto-pulse://reference/errors`, and
`okto-pulse://workflows/kg`.
