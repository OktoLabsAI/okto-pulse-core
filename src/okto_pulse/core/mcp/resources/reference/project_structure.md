---
version: "1.1"
---

# Project structure for Specs

Project structure is a human-readable tree owned by a Spec. The tree is not
required where it adds no meaningful structure, but **the applicability decision
is mandatory for every Spec**. It describes items relevant to that Spec; it is
not Code Evidence and need not enumerate the whole repository.

## Mandatory decision before leaving Draft

1. Read the full current Spec context, its edition/version and delivery context.
   Inspect the relevant baseline or planned deliverables. Read any existing
   Project Structure and prior applicability statement before changing either.
2. If files, folders, modules, schemas, configuration or other artifacts locate
   the intended work meaningfully, populate their relevant tree while in Draft.
   Greenfield work is applicable when a useful TO-BE structure can be described;
   absence of existing code is not by itself an N/A reason.
3. Otherwise, explicitly record `Project Structure: not applicable` in the
   Spec's `context`, with a concrete reason, scope examined and current edition.
   Use the existing `okto_pulse_update_spec` context field, preserve all unrelated
   context, and read the Spec back to verify persistence. Coordinate with other
   authors: this tool replaces the context string, it is not an atomic append.
4. Before any forward move or approval, check that the tree or N/A reason still
   matches the edition and material scope. A change of scope requires reassessment.
   Warn explicitly and resolve a missing decision before the agent proceeds.

No structure (`null`), an empty structure (`[]`), a chat-only statement, lack of
time/access/investigation, or a generic "not needed" is not an N/A justification.
Do not create fake nodes, revoke useful existing structure, manufacture a Decision
or link a dummy Task to satisfy this rule. Existing approved/terminal Specs are
not automatically reopened or mutated; surface the gap for governed revision.

This is an **agent protocol obligation**, not a newly implemented server gate.
The persisted context statement is auditable text, not a machine-validated
applicability field. Do not call the Resource Gate N/A tool with
`resource_type="project_structure"`: that type is not supported. A successful
transition/coverage result does not certify this separate protocol check.

Example context section (illustrative; write an honest reason for the real scope):

```text
## Project Structure applicability
Project Structure: not applicable
Edition reviewed: 2
Scope reviewed: wording-only update to an existing operating policy.
Justification: this edition changes policy text only; it introduces no file,
module, schema, configuration or deliverable-structure decision. The policy
itself and its acceptance criteria are already identified in the Spec.
```

Keep the existing context before/after this section intact. Do not copy the
example's edition or justification into unrelated Specs. The usual Draft-only
content rules and version bump apply to changing this context.

## Truthful classification

Classify every node from the information already available; do not ask a human
to choose when the answer is established:

- `as_is`: an item known to exist in the accepted baseline.
- `to_be`: a planned item or planned shape. It must never link Code Evidence.
- `reference_scaffold`: an existing scaffold, base, template, or external
  reference that informs the design without claiming delivered behavior. It
  requires a non-blank `interpretation_limit` explaining what may and may not
  be inferred. This is not a second human-facing field: author that boundary
  as the node's single `note` / **Note / Description**, and send the same
  normalized text in `interpretation_limit` for the governed transport fence.

`state` is optional and, when useful, is one of `existing`, `planned`,
`modified`, or `removed`. `kind` is exactly `folder`, `file`, or `artifact`.
Each node has exactly one human-editable `note`; keep it concise and useful to
a human reader. Never create a separate "Reference boundary" comment in the
human UI or export.

## Canonical write path

Use `okto_pulse_update_spec_entity` with
`entity_type="project_structure_node"`. Supply the current
`expected_spec_version`, current `expected_structure_revision`, and a fresh,
stable `idempotency_key`. Prefer
`operation="batch"` with `payload_json={"operations": [...]}` when a user
intent requires more than one edit. The batch is all-or-nothing and checks the
permission leaf for every contained operation.

Operations are `create`, `update`, `revoke`, `restore`, `reorder`,
`link_task`, `unlink_task`, `link_test`, `unlink_test`, `link_evidence`, and
`unlink_evidence`. Never write `project_structure` through whole-Spec update.

Limits are 500 active nodes, depth 20, name 255 characters after trimming, and
note/interpretation limit 4,000 characters after trimming. Sibling positions
are zero-based and contiguous. A parent must be one active `folder`; files and
artifacts cannot have children. Revoking a non-empty folder is rejected with a
bounded impact report: explicitly move or revoke its children first. Pulse
never performs a silent cascade.

Tree shape, name, note, classification, state, Evidence links, revoke/restore,
and ordering are semantic content mutations and remain Draft-only. A single
Task/Test `link_*` or `unlink_*`, or a batch containing only those relational
operations, is traceability-only and may run while the Spec is `draft`,
`approved`, `validated`, `in_progress`, or `done`. It advances the structure
revision but preserves `Spec.version`, so it does not reopen or stale the
human validation result. A mixed batch is semantic and therefore Draft-only.
Both fences are still required so concurrent traceability edits cannot be lost.

Task links use roles `create`, `modify`, `read`, or `remove`. Test links use
roles `target`, `test_file`, `fixture`, or `integration_point`. References must
belong to the same Spec. Task and Test views contain only direct references and
the ancestors needed for context; Bugs do not receive a Project structure
projection in this release.

## Reads and exports

Full Spec context preserves `project_structure=null` (not authored) versus
`project_structure=[]` (authored and empty), together with revision and digest.
Task/Test projections report direct versus context-only nodes and affected
references after revocation or reclassification. When the whole Spec is
exported to HTML or Markdown, its active tree and node notes are included in
deterministic preorder under **Note / Description**. For a legacy
`reference_scaffold` whose `note` and `interpretation_limit` differ, readers
show the interpretation limit as the single description because it carries
the stricter non-inference boundary; the next edit must converge both fields.
UI collapse state is presentation-only and is never stored or exported.

Code Evidence remains immutable AS-IS observation. A Project structure node is
normative/contextual information; linking Evidence does not turn the node into
Evidence, and `to_be` nodes cannot carry Evidence.

## Complete authoring examples

Read `okto_pulse_get_spec_context(profile="full")` first. Replace the example
board/spec IDs and fences with current values; do not copy version 4/revision 0
blindly. Each JSON block below is the argument object for
`okto_pulse_update_spec_entity`. Each starts from an unauthored tree. For an
existing tree, reuse its IDs and sibling positions instead of duplicating roots.
Keep one stable idempotency key for exact retries; after a conflict, re-read and
replan the intent with fresh fences and a new key. Verify the returned tree.

### Brownfield: existing module to modify

<!-- tested-example: brownfield -->
```json
{
  "board_id": "BOARD_ID", "spec_id": "SPEC_ID",
  "entity_type": "project_structure_node", "operation": "batch",
  "expected_spec_version": 4, "expected_structure_revision": 0,
  "idempotency_key": "structure-brownfield-intent-1",
  "payload_json": {"operations": [
    {"operation": "create", "payload": {"id": "psn_src", "parent_id": null, "position": 0, "kind": "folder", "name": "src", "classification": "as_is", "state": "existing", "note": "Existing source directory inspected in the accepted baseline."}},
    {"operation": "create", "payload": {"id": "psn_service", "parent_id": "psn_src", "position": 0, "kind": "file", "name": "service.py", "classification": "as_is", "state": "modified", "note": "Existing module in scope for this change; this note is not implementation evidence."}}
  ]}
}
```

### Greenfield: planned source and module

<!-- tested-example: greenfield -->
```json
{
  "board_id": "BOARD_ID", "spec_id": "SPEC_ID",
  "entity_type": "project_structure_node", "operation": "batch",
  "expected_spec_version": 4, "expected_structure_revision": 0,
  "idempotency_key": "structure-greenfield-intent-1",
  "payload_json": {"operations": [
    {"operation": "create", "payload": {"id": "psn_src", "parent_id": null, "position": 0, "kind": "folder", "name": "src", "classification": "to_be", "state": "planned", "note": "Planned source directory, not an observed baseline."}},
    {"operation": "create", "payload": {"id": "psn_service", "parent_id": "psn_src", "position": 0, "kind": "file", "name": "service.py", "classification": "to_be", "state": "planned", "note": "Planned module implementing the behavior defined by this Spec."}}
  ]}
}
```

### Scaffold: existing reference, not delivered functionality

<!-- tested-example: scaffold -->
```json
{
  "board_id": "BOARD_ID", "spec_id": "SPEC_ID",
  "entity_type": "project_structure_node", "operation": "batch",
  "expected_spec_version": 4, "expected_structure_revision": 0,
  "idempotency_key": "structure-scaffold-intent-1",
  "payload_json": {"operations": [
    {"operation": "create", "payload": {"id": "psn_template", "parent_id": null, "position": 0, "kind": "folder", "name": "template", "classification": "reference_scaffold", "state": "existing", "note": "Existing scaffold informs layout only; it does not prove the Spec behavior is implemented.", "interpretation_limit": "Existing scaffold informs layout only; it does not prove the Spec behavior is implemented."}}
  ]}
}
```

After Tasks/Tests exist, link relevant nodes with the documented roles; do not
invent card IDs before those cards exist. Mixed AS-IS/TO-BE projects can combine
the truthful classifications in one batch, without reclassifying planned files
as evidence merely because they are children of an existing folder.
