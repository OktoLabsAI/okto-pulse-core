---
version: "1.0"
---

# Tool docs — Code Traceability

These tools accept bounded observations from an authenticated external agent.
Pulse never clones, opens, searches, or resolves a repository. Read
`okto-pulse://reference/code-traceability` first for the normative evidence,
security, currentness, waiver, and remediation protocol.

Every command has a closed, operation-specific input schema.

## `okto_pulse_start_code_investigation`

Starts a bounded request for the exact current subject/version. It returns the
server-bound scope, required capabilities, profiles, TTL, and single-use
challenge; it does not contact a source provider.

## `okto_pulse_submit_code_investigation_receipt`

Accepts the external agent's bounded `accessible`, `partial`, or `unavailable`
claim. Actor, source scope, subject/version, head, and trust are server-owned.

## `okto_pulse_get_code_investigation_receipt`

Returns bounded receipt metadata and computed currentness; never an operational
workspace locator or source excerpt.

## `okto_pulse_submit_code_evidence`

Submits one immutable factual snapshot bound to an accepted agent receipt.

## `okto_pulse_get_code_evidence`

Reads one bounded Evidence projection (`summary`, `detail`, or `full`).

## `okto_pulse_list_code_evidence`

Lists board-scoped Evidence with typed filters and an opaque cursor.

## `okto_pulse_supersede_code_evidence`

Creates an immutable correction and marks its predecessor superseded.

## `okto_pulse_link_code_evidence`

Links Evidence to one current normative Spec entity under a Spec-version fence.

## `okto_pulse_unlink_code_evidence`

Removes one Evidence link under the same Spec-version fence.

## `okto_pulse_set_code_evidence_disposition`

Records the explicit treatment of inherited Evidence for the current Spec.

## `okto_pulse_create_implementation_target`

Creates semantic implementation intent supplied by a human or external agent;
it does not ask Pulse to discover a target.

## `okto_pulse_update_implementation_target`

Updates semantic Target intent under optimistic revision control.

## `okto_pulse_list_implementation_targets`

Lists persisted Target intent and current lifecycle metadata.

## `okto_pulse_submit_implementation_target_resolution`

Accepts an external agent resolution bound to Target revision, Card version,
request selector scope, and current source head.

## `okto_pulse_get_implementation_overlaps`

Returns overlap derived exclusively from persisted current resolutions.

## `okto_pulse_acknowledge_implementation_overlap`

Records a bounded decision for one exact resolution pair; any changed
resolution makes it stale.

## `okto_pulse_submit_implementation_target_execution_receipt`

Records the authenticated external agent's final Target disposition.

## `okto_pulse_mark_code_traceability_not_applicable`

Records an explicit scoped human waiver, separate from agent attestation.

## `okto_pulse_clear_code_traceability_not_applicable`

Clears one active waiver while preserving audit history.

These are separate closed schemas. Do not collapse them into a heterogeneous
`target_type + payload` command.
