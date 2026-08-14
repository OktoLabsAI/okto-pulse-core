---
version: "1.1"
---

# Tool docs — Code Traceability

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

## Mandatory operation and fence order

1. Fetch the full current subject context and its technical `version`.
2. Start the exact-version preflight with
   `okto_pulse_start_code_investigation`.
3. Inspect source only in the authenticated agent environment.
4. Submit the single-use challenge with
   `okto_pulse_submit_code_investigation_receipt`.
5. For a Refinement/Spec/Card fact, submit Code Evidence. For Card action
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

Submit the external agent's bounded `accessible`, `partial`, or `unavailable`
claim. Actor, source scope, subject/version, head, and trust are server-owned.

Args:
    request_id/challenge_token: Exact single-use pair returned by start.
    outcome: `accessible`, `partial`, or `unavailable`.
    capabilities: Only capabilities actually exercised. `accessible` requires
        every `required_capability` returned by start; otherwise use `partial`
        plus omissions. Include additional exercised capabilities required by
        the intended record, such as `symbol_resolution` for symbol Evidence.
        Never claim a capability merely to pass the fence.
    source_identity_digest/declared_revision/workspace_state: Reproducible
        observed identity. A usable receipt needs the workspace fingerprint;
        all three must be absent for `unavailable`.
    omission_manifest: Required for `partial` and `unavailable`; each item has
        a bounded reason, affected-scope digest, and count. It must be empty for
        `accessible`.
    tooling: `tool_id`, `tool_version`, and deterministic `method_id`.
    observed_at: Agent observation time; server receipt time owns freshness.
    idempotency_key: Exact-retry key.

Do not stop after this call when access exists. The accepted receipt is the
attestation fence for the Code Evidence or Target Resolution that communicates
the investigation result.

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
    investigation_receipt_id: Accepted receipt for this exact parent/version.
    parent_type/parent_id: `refinement`, `spec`, or `card` and its ID.
    evidence_type: `behavior`, `structure`, `contract`, `test`,
        `configuration`, `data_model`, `migration`, `dependency`, or
        `runtime_observation`.
    claim: Standalone human assertion explaining what was observed. “See file”
        and an ID alone are not useful Evidence.
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
  parent_type="refinement", parent_id=<refinement_id>,
  evidence_type="behavior",
  claim="OrderService.submit persists the key after the provider call.",
  selector_kind="symbol", relative_path="src/orders/service.py",
  language="python", symbol_kind="method",
  qualified_symbol="OrderService.submit", line_start=118, line_end=146,
  declared_source_content_sha256=<sha256>,
  idempotency_key="ct-evidence-order-submit-1"
)
```

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
new accepted receipt, complete replacement Evidence fields,
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
