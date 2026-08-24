---
version: "1.0"
---

# Project structure for Specs

Project structure is an optional, human-readable tree owned by a Spec. It
describes the project items relevant to that Spec; it is not Code Evidence and
it is not required for projects without a useful conventional structure.

## Truthful classification

Classify every node from the information already available; do not ask a human
to choose when the answer is established:

- `as_is`: an item known to exist in the accepted baseline.
- `to_be`: a planned item or planned shape. It must never link Code Evidence.
- `reference_scaffold`: an existing scaffold, base, template, or external
  reference that informs the design without claiming delivered behavior. It
  requires a non-blank `interpretation_limit` explaining what may and may not
  be inferred.

`state` is optional and, when useful, is one of `existing`, `planned`,
`modified`, or `removed`. `kind` is exactly `folder`, `file`, or `artifact`.
Each node has one editable `note`; keep it concise and useful to a human reader.

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
deterministic preorder. UI collapse state is presentation-only and is never
stored or exported.

Code Evidence remains immutable AS-IS observation. A Project structure node is
normative/contextual information; linking Evidence does not turn the node into
Evidence, and `to_be` nodes cannot carry Evidence.
