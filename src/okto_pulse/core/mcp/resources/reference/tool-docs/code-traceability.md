---
version: "1.3"
---

# Tool docs — Code Traceability

## `okto_pulse_get_delivery_evidence`

Inputs: `board_id`, `spec_id`. Requires `code_traceability.evidence.read` and board
access. Returns current `edition`, `version`, complete obligation rows with semantic
digests, implementation/test association IDs, separate waiver IDs, `allowed`,
`blockers`, rejected IDs, eligible receipt candidates, per-card obligations and
audit history. Implementation receipts are selectable before card completion;
test candidates currently require a completed Test Card. Read-only: no test
execution, implicit waiver, graph mutation or reopen.

Each `per_card` also carries its current `card_version` and a bounded `progress`
summary: at most 20 recent checkpoints, original authors, remaining work and
declared source state. `total`, `truncated` and shortened-text markers disclose
omissions. Revoked checkpoints remain labeled history. This is not a complete
resume manifest; `recovery_verified` is always false. Check your own access to
the declared workspace before claiming that earlier changes were recovered.

## `okto_pulse_record_delivery_evidence`

Inputs: `board_id`, `card_id`, `spec_id`, and closed object `evidence`:

```json
{
  "expected_spec_edition": 1,
  "expected_card_version": 7,
  "idempotency_key": "delivery-task-42-v1",
  "kind": "implementation",
  "bindings": [
    {"obligation_ref": "fr:fr_42", "contribution": "partial"},
    {"obligation_ref": "ac:ac_42", "contribution": "complete"}
  ],
  "execution_id": "accepted-target-execution-id",
  "justification": "The committed parser implements these input/output obligations."
}
```

- `implementation`: task/bug + accepted `execution_id`, with clean, immutable Git
  result revision and actual path. Record the binding before completing the card;
  final rollup credit still requires Done. A planned Target is insufficient.
  Use `bindings` to declare `partial` or `complete` separately for each obligation;
  omit `obligation_refs` with this form. A partial binding retains its accepted
  receipt but cannot satisfy the Card DoD, implementation rollup or a test join.
  Multiple partial records do not add up to completion. Complete is still an
  executor declaration, subject to the existing proof and review requirements.
  Exact replay cannot change a declaration; a later declaration is a new record.
  New records do not silently revoke or replace earlier records. Legacy clients
  may still use `obligation_refs` under the current compatibility contract;
  history without a declaration stays legacy, never relabelled complete.
  This extension does not adopt ARQ/VER, redefine assigned contribution scope
  or seal the final selection.
  For multiple Targets, supply a nonempty `execution_refs` set on **each** binding:
  `[{"execution_id":"execution-A"},{"execution_id":"execution-B"}]`.
  Omit envelope `execution_id`, `execution_submission` and `execution_client_ref`
  in that form. Each binding independently selects its receipts; no Cartesian
  Target×obligation expansion is inferred. Within a set, receipts must have the
  same observed source and immutable revision (`delivery_execution_base_conflict`
  otherwise); obtain compatible observations rather than assuming Git ancestry.
  In a batch, a set can use `{"client_ref":"earlier-proof"}` for an earlier
  single-execution implementation entry. Composite entries cannot act as a
  single-execution alias. Referenced execution sets count toward the aggregate
  200-link limit. A stale member invalidates only bindings using it; tests must
  name the exact implementation record and satisfy chronology for every receipt
  in the bindings they verify. No previous test transfers to a new set.
- `progress`: executing, unarchived normal/bug/Test card; requires
  `card.conclusion.write`. Use `justification` as the work summary and provide
  `progress` with `contract_version: "delivery-progress/v2"`, `remaining`,
  `material_change: "none"|"targets"|"source"|"unknown"`, and
  `source_state: {"workspace_state":"dirty","recoverability":"external_workspace"}`
  (or both values `unknown`). No execution/test receipt or commit is required;
  omit their fields. Optional `target_ids` must belong to this Card; a supplied
  opaque `source_ref` must be known to this board. `impact_delta` uses the existing
  closed ImpactEvidence contract and is a claim. Progress never counts as
  implementation/test proof, approval or completion. It does not start rework.
  When a delta has a known base, also provide `impact_base_revision` (full
  40/64-character hexadecimal revision), with the source and result revision in
  `source_state`. This declares a base→result edge, not authenticated ancestry.
  Omit the base when unknown; do not fabricate one. The per-card read exposes
  `accumulated_impact`: it composes explicit revision chains, keeps create→delete
  out of the net result and preserves the complete work history. Divergent,
  disconnected, missing or conflicting declarations return bounded
  `needs_reconciliation` items. Repo labels and source identities remain distinct.
  This preview is claim-only; `composed` does not mean current, verified or ready.
  It does not yet replace the impact block submitted with the execution report.
  Validation/rejected/done/on_hold/not_started cards cannot accept a new checkpoint.
  Record significant results or a deliberate pause; no fixed time/command cadence.
  `none` is a context note without a material delta. `targets` requires exact
  affected `target_ids`; `source` requires `source_ref` and no Target list.
  `unknown` narrows by declared Targets/source when available, otherwise this
  Card's work is uncertain. Target/source combinations must agree. An affected
  execution stops proving the current result until its accepted receipt observes
  the work strictly after the checkpoint. Rebinding an old receipt, appending a
  clean note or comparing commit hashes cannot restore it. Independent Targets
  stay eligible; tests never transfer to a new implementation record.
  Legacy v1 payloads/replay digests remain intact: dirty state or material impact
  is treated conservatively in its declared scope; a context-only legacy note
  does not invalidate proof solely by time. The full active checkpoint population
  governs currentness even when the resume summary is capped at 20 records.
- `test`: done TEST `card_id`, linked passed `scenario_id`, nonempty
  `implementation_ids` returned from implementation associations. Uses the current
  authenticated scenario receipt; clients cannot supply `verified` or hashes. Only
  select records this run actually tested. Multiple tests may jointly cover code.
- `revoke`: authorized human only, `record_id`, empty `obligation_refs`,
  justification. Revokes a record of this card. Appends a tombstone; cannot erase
  or restore revoked history.

Waivers remain on the human-only Spec REST surface, using `phase` =
`implementation` or `test`, exact obligation refs and justification. That surface
also revokes legacy records. Neither operation creates a passing test.

Every card write requires Spec edition/card version and a nonempty explanation. At most
1,000 refs/implementation IDs, 20,000 explanation characters; unknown fields and
duplicate refs fail validation. Same actor/key/payload replays `{id,replayed:true}`;
changing that payload yields `delivery_idempotency_conflict`. Normal acceptance
returns `{id,replayed:false}`. Refresh after acceptance; read `allowed` separately.
Progress has a 128 KiB aggregate request limit, at most 200 obligation refs,
100 Target IDs and 8,000 remaining-work characters. Reuse the same key/content
after uncertain failure; do not create another key simply because of a timeout.

For one or several entries, use the same endpoint/tool with this envelope:

```json
{
  "contract_version": "card-delivery-batch/v1",
  "expected_card_version": 7,
  "expected_spec_edition": 1,
  "expected_delivery_revision": 0,
  "idempotency_key": "parser-checkpoints-1",
  "entries": [{
    "client_ref": "parser",
    "kind": "progress",
    "justification": "Parser changed; normalization remains.",
    "progress": {
      "contract_version": "delivery-progress/v2",
      "material_change": "unknown",
      "source_state": {"workspace_state": "dirty", "recoverability": "external_workspace"},
      "remaining": "Implement normalization and run scenarios."
    }
  }]
}
```

Read `per_card.delivery_revision`; zero is valid only for an empty ledger.
The revision includes every record in this Card/Spec/edition, including legacy
appends and revocations. A batch has 1–50 entries, unique `client_ref` (80 ASCII
letters/digits/underscore/hyphen), at most 200 reference uses across entries and
128 KiB serialized bytes. Each entry uses its existing progress/implementation/test
contract and permission; waiver/revoke remain separate. Implementation/test
admission still requires the existing authenticated source records. Partial/complete
declarations are not yet supported.

Within a batch, an implementation may use `execution_client_ref: "earlier-proof"`
instead of execution_id/execution_submission. This references the accepted execution
of an earlier implementation entry; it does not create another execution or grant
attestation authority. Missing, forward, cyclic and wrong-kind references are rejected.
The response includes the canonical execution_id for inline and alias entries.

Any progress/implementation/test entry may cite `progress_refs` with either
`{"record_id": "saved-progress"}` or, in a batch, `{"client_ref": "earlier-progress"}`.
Each reference has exactly one identity. Persisted progress must belong to the same
Card/Spec/edition and board. Local references resolve only to earlier progress in
this request. The ledger stores canonical IDs. These are historical references:
they neither supersede/revoke progress nor declare its remaining work complete.
They grant no proof credit, even when the cited historical progress was revoked.
All references count against the aggregate budget and share the batch rollback.

An implementation entry may replace `execution_id` with `execution_submission`:

```json
{
  "client_ref": "parser-proof",
  "kind": "implementation",
  "obligation_refs": ["fr:parser"],
  "justification": "Implemented the parser and observed the committed result.",
  "execution_submission": {
    "target_id": "target-parser",
    "result_investigation_receipt_id": "accepted-result-receipt",
    "disposition": "touched",
    "actual_relative_path": "src/parser.py"
  }
}
```

The same variant is accepted as a single entry without `client_ref`. Supply
exactly one of execution_id/execution_submission. Board/Card/actor, idempotency
and justification come from the enclosing request. The origin Target service
validates disposition, current Target scope, ownership, trust, freshness and
committed proof; its receipt, event and Delivery binding share one transaction.
The accepted execution ID is returned with the binding ID, including on replay.
An inline execution does not create an investigation attestation or consume a new
challenge: obtain the accepted result-state investigation first, covering the final
Target revisions. Tests continue to reference results authenticated by their runtime.

The server returns `entries: [{client_ref,id}]`, the accepted `delivery_revision`
and `replayed`. IDs are server-owned. Every entry succeeds or none persists,
including when a later proof fails. The first immutable entry holds the envelope
receipt; retry the complete unchanged envelope. Adding/removing/reordering entries
under that key conflicts. A stale revision requires reading/reviewing the current
ledger. An entry error includes `entry_index`, `client_ref` and a bounded cause
code. These identifiers do not grant access to another Card or another actor's proof.

Implementation writes require `code_traceability.target.execution_submit`; test
associations require `spec.tests.execute` (QA need not have implementation-write
privileges). Human waiver/revoke require `code_traceability.waiver.create`/`.clear`.
The MCP routing policy admits board readers, but the shared use case always checks
the kind-specific mutation permission before touching persistence.
Agents must ask the human rather than forge an actor or authorization receipt.

Errors: `delivery_version_conflict`/`delivery_edition_conflict` → reread and review;
`delivery_obligation_not_found` → use the current inventory;
`delivery_accepted_committed_task_execution_required` → obtain a valid committed execution receipt;
`delivery_current_verified_test_and_implementation_required` → fix ownership or
rerun against the current implementation; `delivery_evidence_incomplete` on Done
→ address the projection's missing rows. Existing `delivery_evidence_gate`
advisory/blocking policy and the authorized `skip_delivery_evidence` override
affect transitions, never the factual coverage verdict. Corrected requests need
a new idempotency key.

REST read: GET `/api/v1/boards/{board_id}/specs/{spec_id}/delivery-evidence`.
Card write: POST
`/api/v1/boards/{board_id}/cards/{card_id}/specs/{spec_id}/delivery-evidence`;
the body is exactly `evidence`. The old Spec POST accepts only human-authorized
`waiver`/`revoke`, with `expected_edition`/`expected_version`. An old
`implementation`/`test` body returns 422 `delivery_card_scope_required`; use the
card route and its current fences. Historical records remain readable/revocable.

These tools accept bounded observations from an authenticated external agent.
Pulse never clones, opens, searches, or resolves a repository. Read
`okto-pulse://reference/code-traceability` first for the normative operational
sequence, complete symbol/link/Target examples, security rules, currentness,
waivers, completion criteria, and advisory-mode risk.

Every command has a closed, operation-specific input schema.

## Technical Evidence versus Technical Anchors

| UI concept | Canonical record | Use it for |
|---|---|---|
| Technical Evidence | Code Evidence | An immutable, agent-attested fact observed in an exact Refinement, Spec, or Card source snapshot |
| Technical Anchor | Implementation Target | Mutable Card intent describing the path/symbol action to resolve and execute |

Evidence says what exists or was observed. A Target says where and why a Card
should act. Never create only a receipt: a receipt proves bounded access and
capability, but does not communicate a source finding or implementation intent.

Set the subject's `delivery_context` (`brownfield`, `greenfield`, or `hybrid`)
before investigation. New Evidence is contextual V2 and strictly AS-IS. An
existing Greenfield scaffold/base may be `existing_scaffold`; an existing
constraint may be `existing_constraint`; source consulted only as a model is
`reference_pattern`. A planned file or structure is TO-BE and belongs in the
Spec, Architecture Design, mockup, or Implementation Target, never Evidence.

## Mandatory operation and fence order

1. Fetch the full current subject context and its technical `version`. Confirm
   the explicit delivery context and provenance; never infer it from source
   availability.
2. Start the exact-version preflight with
   `okto_pulse_start_code_investigation`.
3. Inspect source only in the authenticated agent environment.
4. Submit the single-use challenge as contextual V2 with
   `okto_pulse_submit_code_investigation_receipt`.
5. For a Refinement/Spec/Card AS-IS fact, submit contextual V2 Code Evidence.
   For Card action
   intent, use the initial Card receipt/source head to create or adjust the
   Implementation Targets.
6. When linking Evidence to a Spec, use the current `expected_spec_version`.
   Every successful link or disposition returns a new `spec_version`; carry it
   into the next mutation or refetch before continuing.
7. Refetch the Card/Target revisions, run a new Target-bound Card preflight,
   submit its receipt, and resolve every required Target. A receipt whose
   selector scope predates a Target cannot resolve that Target.
8. Refetch gate context, resolve overlaps, and follow only an advertised
   lifecycle transition.
9. After Card execution, start a new result-state preflight and submit one
   Execution Disposition for every active required Target.
10. Read `source_context` for the effective role/origin summary. A bounded
    item list does not bound its complete counts. Treat a derived Spec's
    source-context manifest as frozen until an explicit preview/apply rebase.

Use an `idempotency_key` again only for an exact retry of the same payload.
Entity version, source head/workspace identity, selector, Target revision, or
dependency drift can invalidate reuse and require a new preflight.

## `okto_pulse_start_code_investigation`

Start a bounded request for the exact current subject/version. It returns the
server-bound scope, required capabilities, profiles, TTL, opaque source
identity, and single-use challenge; it does not contact a source provider.

Args:
    board_id: Board ID.
    subject_type: `refinement`, `spec`, or `card`.
    subject_id: Current subject ID.
    expected_subject_version: Technical version from the latest full context.
    idempotency_key: Caller-stable key for an exact retry.
    source_ref: Optional opaque identity only when already known. Never pass a
        path, URL, checkout directory, or credential.

## `okto_pulse_submit_code_investigation_receipt`

Submit the external agent's contextual V2 result. Actor, delivery context,
source scope, subject/version, head, and trust are server-owned.

Args:
    request_id/challenge_token: Exact single-use pair returned by start.
    contract_version: `2` for new governed work.
    outcome: `evidence_applicable`,
        `no_relevant_existing_implementation`, `partial`, or `unavailable`.
        The no-existing outcome is complete success, valid only for a
        Greenfield subject with full identity/workspace/capabilities and no
        omissions. It may coexist with scaffold/constraint/reference Evidence
        but never with `current_implementation` Evidence in the same scope.
    capabilities: Only capabilities actually exercised. Either complete V2
        outcome requires every `required_capability` returned by start;
        outcome; otherwise use `partial` plus omissions. Include additional
        exercised capabilities required by the intended record, such as
        `symbol_resolution` for symbol Evidence. Never claim a capability
        merely to pass the fence.
    source_identity_digest/declared_revision/workspace_state: Reproducible
        observed identity. A usable receipt needs the workspace fingerprint;
        all three must be absent for `unavailable`.
    omission_manifest: Required for `partial` and `unavailable`; each item has
        a bounded reason, affected-scope digest, and count. It must be empty for
        either complete V2 outcome.
    tooling: `tool_id`, `tool_version`, and deterministic `method_id`.
    observed_at: Agent observation time; server receipt time owns freshness.
    idempotency_key: Exact-retry key.

Do not stop after this call when access exists. The accepted receipt is the
attestation fence for the Code Evidence or Target Resolution that communicates
the investigation result.

V1 `accessible|partial|unavailable` receipts remain readable only. They do not
prove contextual applicability. If the live MCP schema does not advertise the
V2 discriminator/outcomes, stop and surface the missing capability rather than
using V1 for a new write.

## `okto_pulse_get_code_investigation_receipt`

Return bounded receipt metadata and computed currentness; never an operational
workspace locator or source excerpt. Use it to distinguish `current` from
outdated, conflicted, expired, or revoked state before reuse.

Args:
    board_id: Board ID.
    receipt_id: Accepted receipt ID.

## `okto_pulse_submit_code_evidence`

Submit one immutable factual observation bound to an accepted current agent
receipt. Prefer one record per independently reusable claim.

Args:
    contract_version: `2` for new Evidence.
    investigation_receipt_id: Accepted receipt for this exact parent/version.
    parent_type/parent_id: `refinement`, `spec`, or `card` and its ID.
    evidence_type: `behavior`, `structure`, `contract`, `test`,
        `configuration`, `data_model`, `migration`, `dependency`, or
        `runtime_observation`.
    claim: Standalone human assertion explaining what was observed. “See file”
        and an ID alone are not useful Evidence.
    source_role: `current_implementation`, `existing_scaffold`,
        `existing_constraint`, or `reference_pattern`. Never author
        `uncategorized_legacy`.
    relevance_summary/scope_relation/source_origin: Required bounded context
        that makes the observation understandable to a clean-context consumer.
    interpretation_limit: Required for `existing_scaffold` and
        `reference_pattern`; explain what the observation does not prove.
    baseline_provenance: Required `presence`, matching `workspace_state_id`,
        and optional provenance note. `preexisting_worktree` requires a note.
        Post-baseline or planned source is forbidden.
    selector_kind: `symbol`, `file`, `span`, `configuration_key`,
        `schema_object`, `endpoint`, or `test_case`.
    relative_path: Normalized repository-relative path when applicable.
    language/symbol_kind/qualified_symbol/symbol_signature: Stable semantic
        selector details. `symbol` requires `qualified_symbol`.
    line_start/line_end: Optional paired snapshot coordinates; never durable
        identity and never valid without a relative path.
    declared_source_content_sha256: Required digest of exact observed source
        content under the preflight canonicalization profile.
    excerpt/excerpt_sha256: Optional exact safe excerpt and its own SHA-256.
        Omit both when board policy uses `receipt_content="metadata_only"`.
    declared_file_blob_sha256: Optional whole-file blob digest.
    idempotency_key: Exact-retry key.

Minimal symbol-shaped call:

```text
okto_pulse_submit_code_evidence(
  board_id=<board_id>, investigation_receipt_id=<accepted_receipt_id>,
  contract_version=2,
  parent_type="refinement", parent_id=<refinement_id>,
  evidence_type="behavior",
  claim="OrderService.submit persists the key after the provider call.",
  source_role="current_implementation",
  relevance_summary="Defines the current submission baseline.",
  scope_relation="Directly implements the in-scope flow.",
  source_origin="Observed in the accepted service snapshot.",
  interpretation_limit=null,
  baseline_provenance={
    presence:"committed_snapshot",
    workspace_state_id:<opaque_workspace_state_id>,
    provenance_note:null
  },
  selector_kind="symbol", relative_path="src/orders/service.py",
  language="python", symbol_kind="method",
  qualified_symbol="OrderService.submit", line_start=118, line_end=146,
  declared_source_content_sha256=<sha256>,
  idempotency_key="ct-evidence-order-submit-1"
)
```

These V2-only fields must be present in the live inbound schema. A legacy MCP
shape is not permission to omit them; stop rather than create ambiguous V1
Evidence.

## Effective context and explicit actor classification

`okto_pulse_get_code_evidence` and `okto_pulse_list_code_evidence` expose the
immutable Evidence record. The entity context's `source_context` and
`source_context_items` expose effective contextual meaning, including
`context_origin=authored|human_legacy_classification|unclassified_legacy`,
complete role counts, and classification state. The middle origin is a
compatibility label for an actor-authored overlay. Summary/gate projections
omit classifier identity; detail/full may expose it for authorized audit.

An authorized human may use the UI/REST batch and an authorized agent may use
`okto_pulse_classify_legacy_code_evidence`. Both are governed by
`code_traceability.evidence.classify_legacy`; each correction appends a new
revision over the immutable Evidence payload. Agents must consume the exact
server-authored classification inputs and must request human input when the
available context does not support a defensible classification.

A derived Spec keeps the source-context manifest frozen at its exact
Refinement snapshot. Live Evidence or human-classification changes do not
rewrite it. Adopt a later snapshot only through the governed preview/apply
rebase and its exact `preview_sha256`; when that surface is not available over
MCP, request the authorized UI/REST action.

## `okto_pulse_classify_legacy_code_evidence`

Append one atomic classification batch over legacy Evidence.

Args:
    board_id: Board owning every Evidence item.
    items: Complete classification items using the exact projected
        `expected_evidence_payload_sha256`, `expected_classification_revision`,
        and `baseline_provenance`, plus explicit source role, relevance, scope,
        origin, and any required interpretation limit.
    justification: Plain-language reason supporting the batch decision.
    idempotency_key: Reuse only for a byte-for-byte retry.

The tool never edits the original Evidence. A stale payload or classification
revision fails closed; refresh the current Refinement context before retrying.

## `okto_pulse_get_code_evidence`

Read one immutable Evidence projection.

Args:
    board_id/evidence_id: Exact Evidence identity.
    profile: `summary`, `detail` (default), or `full`. Use `full` only for a
        bounded single-item audit requiring the complete accepted record.

## `okto_pulse_list_code_evidence`

List board-scoped Evidence with typed parent/status/attestation filters and an
opaque cursor. Use the returned cursor unchanged; do not decode or synthesize
it. Filter by `parent_type` and `parent_id` when building one subject's record.

## `okto_pulse_supersede_code_evidence`

Create an immutable correction and mark its predecessor superseded. Supply a
new accepted V2 receipt, complete contextual replacement Evidence fields,
`supersedes_evidence_id`, and a human `supersession_reason`. Do not edit or
silently contradict the predecessor.

## `okto_pulse_link_code_evidence`

Link active Evidence to one current normative Spec entity under a Spec-version
fence. `entity_type` supports `spec`, `functional_requirement`,
`technical_requirement`, `acceptance_criterion`, `business_rule`,
`api_contract`, `integration_requirement`, `observability_requirement`,
`decision`, and `test_scenario`. Relations are `supports`, `constrains`,
`motivates`, `implements`, `tests`, or `contradicts`.

Use a stable item ID and a rationale that explains the semantic relationship.
The result returns the next `spec_version`; use it for the next link or refetch.

```text
okto_pulse_link_code_evidence(
  board_id=<board_id>, spec_id=<spec_id>, evidence_id=<evidence_id>,
  entity_type="technical_requirement", entity_id=<tr_id>,
  relation_type="constrains",
  rationale="The observed write order constrains TR-2 recovery behavior.",
  expected_spec_version=<current_spec_version>
)
```

## `okto_pulse_unlink_code_evidence`

Remove one Evidence link under the current Spec-version fence. Pass the link
ID, not the Evidence ID. The successful result advances `spec_version`.

## `okto_pulse_set_code_evidence_disposition`

Record explicit treatment of inherited Evidence for the current Spec.
Dispositions are `not_relevant`, `superseded`, or `deferred`, with a required
human justification. `deferred` is allowed only while the Spec is Draft and
does not satisfy final coverage. The successful result advances
`spec_version`; refetch/carry the returned fence before another mutation.

## `okto_pulse_create_implementation_target`

Create semantic Card intent; Pulse does not discover source targets. This is
the canonical record shown as a Technical Anchor. An accepted initial Card
preflight/source head must exist before creation. After creating or updating
Targets, run a new Card preflight so its selector scope binds the current
Target IDs and revisions before submitting Resolutions.

Args:
    board_id/card_id: Target-owning Card.
    source_ref: Opaque source identity from preflight, never a locator.
    selector_kind: `symbol`, `file`, `glob`, `semantic`, or `new_file`.
    relative_path_hint/language/symbol_kind/qualified_symbol/symbol_signature:
        Strongest known semantic hint. `symbol` requires a qualified symbol;
        `file` and `new_file` require a relative path.
    role: `read`, `modify`, `extend`, `create`, `delete`, `test`, or
        `validate`.
    intent: Concrete human-readable action and reason.
    required: Whether the Card must resolve/execute this Target.
    expected_spec_version: Current technical version of the Card's Spec.
    baseline_evidence_id: Most direct immutable starting observation.
    spec_links: Stable normative item IDs driving the action.
    evidence_links: Evidence relations `derived_from`, `validates`, or
        `replaces`.

```text
okto_pulse_create_implementation_target(
  board_id=<board_id>, card_id=<card_id>, source_ref=<opaque_source_ref>,
  selector_kind="symbol", relative_path_hint="src/orders/service.py",
  language="python", symbol_kind="method",
  qualified_symbol="OrderService.submit", role="modify",
  intent="Move idempotency persistence before the provider call.",
  required=true, expected_spec_version=<current_spec_version>,
  baseline_evidence_id=<evidence_id>,
  spec_links=[{entity_type:"technical_requirement", entity_id:<tr_id>}],
  evidence_links=[{evidence_id:<evidence_id>, relation_type:"derived_from"}]
)
```

## `okto_pulse_update_implementation_target`

Update mutable Target intent under `expected_revision` optimistic control and
provide `change_reason`. Omitted fields remain unchanged; a changed value or
link set needs the current Target revision. Never mutate Evidence through this
tool.

## `okto_pulse_list_implementation_targets`

List persisted Target intent and current lifecycle metadata. Filter by Card,
opaque source, lifecycle status, or role; continue only with the returned
opaque cursor.

## `okto_pulse_submit_implementation_target_resolution`

Submit an external-agent resolution bound to the current Target revision,
Card version, request selector scope, and current source head.

This resolution renewal and its investigation preflight are permitted while a
Card is `rejected`, because rejection advances the Card version and blocking
mode needs a Current resolution before the only exit to `in_progress`. This is
not permission to change implementation: Target create/update and execution
receipt submission remain frozen until the rework handoff is accepted.

- `resolved`/`moved`: relative path required, confidence `>=0.95`, no more
  than one candidate.
- `stale`: reason and path/candidate required, confidence `0.80-<0.95`.
- `ambiguous`: at least two candidates whose top scores differ by at most
  `0.05`; omit one resolved path and top-level confidence.
- `missing`/`unavailable`: reason required; omit resolved path, candidates,
  and confidence.

Include the exact accepted Card `investigation_receipt_id`, deterministic
tooling identity, agent observation time, and a new idempotency key. Do not
inflate confidence to force an executable state. The receipt must come from a
preflight started after the current Target ID/revision entered the Card
selector scope; never reuse the initial pre-Target receipt.

## `okto_pulse_get_implementation_overlaps`

Return overlaps derived only from persisted current Target resolutions for one
Card. Review this after all required Targets resolve and again when any
resolution changes.

## `okto_pulse_acknowledge_implementation_overlap`

Record a bounded decision for one exact Target/Resolution pair using
`ordered_by_dependency`, `accepted_parallel`, `merged_targets`, or
`false_positive`, plus a human justification. Any changed resolution makes the
acknowledgement stale.

## `okto_pulse_submit_implementation_target_execution_receipt`

Record the external agent's final Target disposition after a new result-state
preflight. Dispositions are `touched`, `not_touched`, `replaced`, `created`,
`deleted`, or `superseded`. Always explain the outcome; include the actual
relative path/symbol when known. `replaced` requires a distinct
`replacement_target_id`.

## `okto_pulse_mark_code_traceability_not_applicable`

Record an explicit scoped human waiver, separate from agent attestation. Use
this only when Code Traceability is genuinely not applicable or the governed
human policy explicitly accepts the bounded exception. Never fabricate an
agent receipt to avoid a warning.

## `okto_pulse_clear_code_traceability_not_applicable`

Clear one active waiver while preserving audit history.

## Advisory outcome

In `advisory`, unmet Code Traceability conditions do not block a lifecycle
transition, but they remain traceability debt. Pulse cannot recreate omitted
source observations or intent. Later entity/source/selector/Target drift can
make the earlier investigation unusable and force a new preflight,
investigation, receipt, Evidence submission, links, and Resolution. Record the
work while its evidence is current; do not treat advisory as an implicit
waiver.

These are separate closed schemas. Do not collapse them into a heterogeneous
`target_type + payload` command.
