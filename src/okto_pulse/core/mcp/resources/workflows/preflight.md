---
version: "1.3"
---

# Pre-Flight Checklist (READ FIRST)

Every time you start a session or pick up a new task, follow the matching sequence below.
Before authoring, assessing, or relying on semantic board guidelines, read
`okto-pulse://reference/policy-compliance`.

## Action map — choose one route, then read its required detail

| Action | Read/prepare | Act and verify | Stop condition |
|---|---|---|---|
| Explore a board | Session pre-flight; bounded lists/summary | Fetch only relevant detail and follow pagination | Missing access or incomplete required context |
| Author Ideation/Refinement/Spec | Matching workflow; current context, relevant source artifacts and required KG queries | Edit in an allowed state; read back IDs/version | Unresolved user intent, frozen content or conflicting provenance |
| Start/rework a card | Card execution pre-flight below | Choose a currently allowed edge; verify `in_progress` before work | Refusal, missing authority or required context |
| Validate/complete | Full gate context; current evidence/head/binding; independent reviewer where required | Submit the type-specific gate; re-read status and blockers | An accepted submission is not automatically an approved delivery |
| Handle error/background work | Outer V2 outcome and domain state; retry protocol below | Follow the owned handle/next action; verify terminal result | Unknown outcome, unavailable authority, repeated non-progress or exhausted budget |

These routes do not replace the mandatory domain resources. Reuse unchanged
instruction resources already read in this session; do not reload every domain
for an unrelated operation. Fetch artifact bodies only when needed for actual
implementation/review, using the manifest/drilldowns. Refresh mutable context
before a gated move and after a write that changes its version/head.

## Rule categories — authority and proof

| Category | Meaning | Where to verify |
|---|---|---|
| Server gate | Deterministically enforced when enabled for this actor/board/state | Full gate context, effective settings, allowed transitions, mutation response |
| Agent protocol | Required agent behavior, not certified by a successful server move | Audit trail and actual work: context reads, prescribed KG queries, Project Structure decision, meaningful progress comments |
| Advisory | Review for usefulness; absence alone does not block | Advisory fields in the current summary; KB context must not become filler |
| Human/independent authority | Only the authorized actor can take the action | Effective permissions and typed remediation; no automatic skip or self-approval |

Architecture/Mockup presence and coverage are server gates where applicable;
Project Structure applicability is an agent protocol. Code Traceability and
guideline enforcement depend on effective policy. A comment cannot waive a
server gate, and a green Resource Gate does not prove all protocol obligations.

### Session pre-flight — before any board work

```
1. okto_pulse_get_my_profile()             → know who you are
2. okto_pulse_list_my_boards()             → know what you have access to
3. okto_pulse_get_unseen_summary(board_id) → check mentions + recent activity
4. okto_pulse_get_board_guidelines(board_id) → read rules set by the board owner
```

### Entity context pre-flight — before moving or validating anything

Call the matching `okto_pulse_get_{ideation,refinement,spec,sprint}_context`
with `profile="full"` before any move/validation. For cards, call
`okto_pulse_get_task_context(profile="full", context_scope="gate")`: it is the
bounded full gate/readiness slice and includes a content manifest plus
drilldowns. The default `summary` profile is for cheap exploration, not for
status-changing work (see
`okto-pulse://reference/projection-profiles`).

### Card execution pre-flight — before implementation work

```
1. okto_pulse_get_task_context(board_id, card_id, profile="full", context_scope="gate", include_knowledge=true, include_mockups=true, include_architecture=true, include_qa=true, include_comments=true)
2. Attach applicable artifacts — follow the Card-Level Artifact Attachment path in §2.8 of okto-pulse://workflows/cards (the single source: copy tools per artifact, decide per KE/mockup/Architecture Design, and any skip requires a one-line justifying comment)
3. Read okto_pulse_get_allowed_transitions; enter in_progress only through an allowed edge for the current type, status, permissions and gates (use started first when required). If already in_progress, do not repeat a move merely to satisfy this checklist.
4. Verify the move succeeded and re-read the card state, then BEGIN WORK. A refusal is a blocker, not permission to start anyway.
```

**Never skip card execution steps 1 and 3.**

The transition read is a snapshot, not a durable authorization: the mutation
revalidates current state. For `rejected`, inspect its Current sealed cause and
use the sole rework exit to `in_progress`. Test cards have a separate lifecycle;
see `okto-pulse://reference/transitions` and `okto-pulse://reference/card_types`.

Use `profile="detail"` and follow its drilldowns when implementation/review
needs artifact or requirement bodies. Do not replace the gate-scope call with a
potentially oversized `profile="full", context_scope="all"` response. Mutation
services resolve and fingerprint complete context server-side; the client-side
gate slice is the bounded operational view.

This is an operational protocol rule: the MCP server does not prove that you
read context; your audit trail and artifact quality do.

### Quality Assessment pre-flight — before record/read/gate use

Read `okto-pulse://reference/quality-assessments` before operating on Quality
evidence. For an ambiguity write:

1. Read the subject's full current context.
2. Read its current ambiguity head and retain `head_revision` (use 0 when
   absent).
3. Confirm domain-write authority AND `{subject}.quality.assess`; proposed
   questions also require `{subject}.qa.ask`.
4. Submit a stable idempotency key with the exact
   `expected_subject_version` and `expected_head_revision`.
5. After success, re-read currentness. A head is not proof that the receipt is
   current.

Before executing the Spec checklist, read the full Spec context and
`okto_pulse_get_checklist_binding`; freeze the current Spec version and binding
identity when starting the execution. Submit every immutable item exactly once.
`manual_checklist_ref` is legacy evidence only and never satisfies A3.

### Resource Gate pre-flight — mandatory before completion

**Specs also require a Project Structure applicability decision before leaving
`draft`.** Inspect the current tree and Spec `context`: author a meaningful tree
or persist `Project Structure: not applicable` with a specific reason and the
edition/scope reviewed. An absent/empty tree alone is unresolved, not N/A.
Read `okto-pulse://reference/project-structure`. This is a separate agent
protocol obligation; it is not included in the Resource Gate summary below.
If omitted, warn and resolve it before proceeding; do not assume a green
Resource Gate means this decision was made.

Architecture, Mockup, and Knowledge Base are all tracked by Resource Gate, but
their authority differs: **Architecture and Mockup are blocking**;
**Knowledge Base is advisory**. A missing or uncovered KB remains visible in
the summary/workspace and never blocks entity completion, `spec_validation`, or
`spec_done`.

| Work item | Resource Gate `entity_type` |
|---|---|
| Ideation | `ideation` |
| Refinement | `refinement` |
| Spec | `spec` |
| Card, task, test, bug | `card` |

Before finalization call `okto_pulse_get_resource_gate_summary(board_id, entity_type, entity_id)` and resolve every entry in `missing_resources` by attaching the blocking Architecture/Mockup artifact or marking it N/A with `justification`. Review `advisory_missing_resources` for useful KB context, but do not create filler or mark KB N/A solely to satisfy a gate.

The blocking-resource playbook remains reversible and auditable: marking N/A with `justification` records intent, and `okto_pulse_clear_resource_not_applicable` restores the normal presence check when the resource becomes applicable.

### Design System pre-flight — before creating or editing mockups

A board can mandate a **Design System** for its screen mockups. The `MockupDesignSystemGate`
enforces it deterministically on `okto_pulse_add_screen_mockup` / `okto_pulse_update_screen_mockup`
(and the REST twin) **before persistence**. Discover the mandate from the board summary so you
reference the Design System from the start instead of learning it by being rejected:

```
1. okto_pulse_get_board(board_id) → read the `design_system` block:
     { "effective": {design_system_id, title, version, source} | null,
       "gate_mode": "off" | "advisory" | "blocking",
       "mandate": true when there is an effective Design System AND gate_mode == "blocking" }
2. okto_pulse_get_board_design_system(board_id) → the effective Design System identity (id/version/source)
3. okto_pulse_get_design_system(design_system_id) → the full Design System incl. payload (tokens, components, layout/accessibility rules) to actually consume it
```

When `mandate` is true (gate_mode=blocking + an effective Design System), a new/updated mockup MUST carry:
- `design_system_ref` = the board's REAL effective `design_system_id` (synthetic/wrong → rejected),
- `design_system_version` matching the effective version,
- `design_system_evidence` = non-empty proof the screen consumes the Design System.

Otherwise the gate rejects **before persisting** with an actionable, structured error:

| reason code | meaning |
|---|---|
| `design_system_required` | no `design_system_ref` provided |
| `design_system_not_found` | the ref is synthetic, non-existent, or dangling (does not resolve to the real effective Design System) |
| `design_system_version_mismatch` | the version does not match the effective Design System version |
| `design_system_evidence_missing` | no consumption evidence |

The error payload carries `expected_design_system_id` and `expected_design_system_version` — use
them to **self-correct** (set `design_system_ref` / `design_system_version` to the expected values,
add evidence) and retry.

- **advisory**: the mockup is persisted, but the response carries a `design_system_gate` warning and a queryable `DesignSystemGateAudit` row is written.
- **off**, or a board with **no effective Design System**: the gate never blocks.

> Caveat: `mandate=true` can still fail at the gate with `design_system_not_found` if the board's configured Design System is **dangling** (e.g. a board link to a deleted Design System — the resolver still reports it but `exists=false`). Read a `design_system_not_found` under `mandate=true` as "the board's Design System config is broken", not as your ref being wrong.

### Code Traceability pre-flight — agent mediated

Before a governed Refinement, Spec, or Card transition, follow this chain:

```text
explicit delivery_context + full subject context
→ external agent capability/access preflight
→ contextual V2 receipt
→ AS-IS evidence or target coverage
→ accepted-receipt freshness against subject/version and source head
→ overlap decision
→ allowed transition
```

A Refinement and a direct Spec require an explicit
`delivery_context=brownfield|greenfield|hybrid`; a derived Spec inherits the
value and provenance frozen in its Refinement snapshot. Never infer delivery
context from source access, repository contents, or an empty Evidence list.

The authenticated external agent performs the capability/access check and any
deterministic source investigation in its own environment. Pulse Core validates
and governs the bounded attestation. Pulse Community only persists and projects
the resulting opaque records. Neither Pulse surface clones, opens, searches,
resolves, or otherwise establishes truth from a repository, filesystem, code
provider, or language runtime.

For new work, submit `contract_version=2` with
`evidence_applicable`, `no_relevant_existing_implementation`, `partial`, or
`unavailable`. The no-existing outcome is complete Greenfield evidence and
requires full identity/workspace/capabilities with no omissions; it is not an
alias for unavailable access. Existing scaffold/base, constraints, and
references may still be recorded as AS-IS Evidence under
`existing_scaffold`, `existing_constraint`, or `reference_pattern`.
Scaffold/reference items require `interpretation_limit`.

Never submit TO-BE files or structures as Evidence. Put them in the Spec,
Architecture Design, mockup, or Implementation Target. V1 remains readable but
contextually unclassified and cannot be inferred into V2 authority. If the
live inbound surface exposes only V1, stop and report the missing V2
capability.

If `source_context_items` reports `unclassified_legacy`, inspect the exact IDs.
An authorized agent may append classification with
`okto_pulse_classify_legacy_code_evidence`; an authorized human may use UI/REST.
Both require `code_traceability.evidence.classify_legacy`, defensible provenance
and current CAS inputs. Without authority or evidence, report the blocker.
The original Evidence remains immutable; classification does not upgrade V1.
Read the effective `source_context` summary even when item collections are
bounded. Treat a derived Spec's source-context manifest as frozen until an
explicit, preview-fenced rebase.

Treat `partial` and `unavailable` as explicit outcomes. Follow the typed
blocker or human-waiver path advertised by policy; never invent
`decoupled_mode`, infer access from a previous run, or silently downgrade a
blocking gate.

Canonical protocol: `okto-pulse://reference/code-traceability`.

## Version and identity glossary

| Identity | Use | Not interchangeable with |
|---|---|---|
| Product release | Installed implementation/capabilities | Entity version or instruction revision |
| Resource/catalog identity or content hash | Cache official instruction content; when unavailable, invalidate on reconnect/restart | Mutable board guidelines/context |
| Entity edition | Lifecycle/review cycle | Content version within that edition |
| Subject/content version | CAS fence for the exact reviewed entity | Receipt ID or head revision |
| Head/assignment/classification revision | CAS fence for that particular ledger/scope | Another resource's revision |
| Receipt ID/digest and currentness | Evidence of a specific accepted operation | Permanent approval or automatic adoption by a frozen descendant |

Use exact server-returned fields; never substitute an edition for a version,
guess a digest, or reuse one stale fence across a sequence of mutations.

## Retry, uncertainty and background work

| Situation | Next action |
|---|---|
| Invalid payload/unsupported argument | Read the current schema and safe validation details; correct input before retrying |
| CAS/version/head conflict | Re-read current context/head, reconsider intent, submit changed input with a new idempotency key |
| Exact replay of the same operation | Reuse its key and identical payload only where the tool supports idempotency |
| Permission or independent-review requirement | Ask the authorized actor; `retryable=true` is not permission to bypass it |
| Timeout/disconnect with unknown write outcome | Read the receipt/entity/job status first; do not duplicate an unconfirmed mutation |
| `accepted`/`pending`/`running` | Keep the returned handle, poll its documented status tool, and inspect the domain result until terminal |
| Cancel requested | Verify terminal cancellation; request acceptance does not prove work stopped |
| Graph unavailable/recovery needed | Diagnose the component and use `okto-pulse://reference/kg-health`; do not infer a rebuild from generic overall state |

Follow server retry-after/backoff/deadline when provided. Otherwise use bounded
backoff for transient retries; do not busy-poll. Stop automatic mutation retries
after repeated unchanged failure and report the blocker. For monitoring, agree
a time budget (or follow the user's explicit monitoring instruction), give
progress updates and retain the handle across reconnects. No progress alone
does not authorize cancelling, restarting or launching a duplicate job.

When escalating, include board/entity ID, operation, error code, current state,
version/edition, safe blocker details, receipt/job ID, attempts and the exact
missing authority/action. Never include tokens, challenges or sensitive bodies.
