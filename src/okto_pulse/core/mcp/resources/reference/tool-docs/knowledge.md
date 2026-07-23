---
version: "1.0"
---

# Tool docs — `knowledge`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

Read `okto-pulse://reference/knowledge-governance` before authoring a KB or
promoting a finding. KB bodies are advisory/untrusted; first-class SDLC
artifacts remain authoritative. Legacy calls still use `legacy_all`: an
omitted ID filter copies all resources selected by the existing path.
Selective propagation v2 is opt-in through a complete versioned envelope or
the dedicated card-assignment tools below. An omitted v2 envelope stays on the
legacy path; a supplied envelope is authoritative and never implies legacy
copy-all.

## `okto_pulse_add_card_knowledge`

Deprecated / blocked. Card Knowledge Base resources are read-only governed
snapshots. Create or update the Knowledge Base on the source ideation,
refinement, or spec, then call `okto_pulse_copy_knowledge_to_card`.

Args:
    board_id: Board ID
    card_id: Card ID
    title: KE title
    content: KE content (Markdown by default)
    description: Short summary (optional)
    mime_type: Content MIME type (default text/markdown)
    source: Free-form provenance hint (e.g. "manual", "copied_from_spec:<spec_id>:<kb_id>")

Returns:
    JSON error `card_resource_read_only`

## `okto_pulse_add_ideation_knowledge`

Add a knowledge base item to an ideation.

Provide exactly ONE of content or content_reference. Ideation KBs are
propagated to refinements/specs by default when those artifacts are derived
or created from the ideation.

Args:
    board_id: Board ID
    ideation_id: Ideation ID
    title: Title of the knowledge base item
    content: Inline text content (use for small snippets)
    content_reference: Runtime-specific reference resolved by the active edition
    description: Short description of what this document contains (optional)
    mime_type: Content type, default "text/markdown"
    governance_metadata: Optional v1 object or JSON string; follow the complete
        closed contract at okto-pulse://reference/knowledge-governance

## `okto_pulse_add_refinement_knowledge`

Add a knowledge base item to a refinement. Use this to attach reference documents,
design docs, analysis notes, or any context that helps agents understand the refinement.

Provide exactly ONE of: content or content_reference.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    title: Title of the knowledge base item
    content: Inline text content (use for small snippets)
    content_reference: Runtime-specific reference resolved by the active edition
    description: Short description of what this document contains (optional)
    mime_type: Content type, default "text/markdown"
    governance_metadata: Optional v1 object or JSON string; follow the complete
        closed contract at okto-pulse://reference/knowledge-governance

Returns:
    JSON with created knowledge base item

## `okto_pulse_add_spec_knowledge`

Add a knowledge base item to a spec. Use this to attach reference documents,
design docs, API specs, or any context that helps agents understand the spec.

Provide exactly ONE of: content or content_reference.

Args:
    board_id: Board ID
    spec_id: Spec ID
    title: Title of the knowledge base item
    content: Inline text content (use for small snippets)
    content_reference: Runtime-specific reference resolved by the active edition
    description: Short description of what this document contains (optional)
    mime_type: Content type, default "text/markdown"
    governance_metadata: Optional v1 object or JSON string; follow the complete
        closed contract at okto-pulse://reference/knowledge-governance

Returns:
    JSON with created knowledge base item

## `okto_pulse_copy_knowledge_to_card`

Copy knowledge base entries from a spec to a card as inline card KEs.
Each copied entry is stored in Card.knowledge_bases with stable provenance.
Complete governance metadata is copied unchanged. An existing managed Spec
snapshot may be refreshed at the same id/source when the source changes;
semantically identical metadata is a no-op. Metadata never changes selection,
fan-out, Resource Gate, or lineage.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID
    knowledge_ids: Multi-value knowledge base IDs to copy (empty = copy ALL) —
        formats: okto-pulse://reference/multivalue.

Returns:
    JSON with count of knowledge entries copied and total card KEs

## `okto_pulse_replace_card_knowledge_assignments`

Atomically replace a card's authoritative v2 Knowledge selection. This is a
complete replacement, not an additive patch. Every source and relevance link
is validated before any assignment is written. Relevance links may target only
FR, AC, or test-scenario IDs on the card's linked spec.

Args:
    board_id: Board ID
    card_id: Card ID
    request:
        contract_version: Must be 2
        knowledge_ids: Non-empty unique stable Knowledge root IDs
        mode: `reference` or `snapshot`
        linkage: Optional list of `{entity_type, entity_id}` relevance links
        justification: Required non-empty reason
        idempotency_key: Required non-empty replay key
        expected_revision: Current non-negative selection revision (CAS)

Returns:
    JSON with `success`, `contract_version`, `operation_id`, `revision`,
    `replayed`, `selection_state`, and the effective assignments. A stale
    `expected_revision` fails closed without a partial write.

## `okto_pulse_drop_card_knowledge_assignments`

Authoritatively remove selected card Knowledge assignments. IDs are stable
Knowledge roots, never assignment-row IDs. An empty `knowledge_ids` list means
drop all assignments; use this operation rather than encoding removal through
replace or refresh.

Args:
    board_id: Board ID
    card_id: Card ID
    request:
        contract_version: Must be 2
        knowledge_ids: Unique stable roots; empty means all
        justification: Required non-empty reason
        idempotency_key: Required non-empty replay key
        expected_revision: Current non-negative selection revision (CAS)

Returns:
    The same versioned mutation projection as replace. Revision conflicts and
    invalid roots fail closed.

## `okto_pulse_refresh_card_knowledge_assignments`

Refresh existing `snapshot` assignments from their current source revisions.
This operation does not change selection and accepts stable Knowledge root IDs,
never assignment-row IDs. Reference-mode assignments are not refresh targets.

Args:
    board_id: Board ID
    card_id: Card ID
    request:
        contract_version: Must be 2
        knowledge_ids: Non-empty unique stable Knowledge root IDs
        idempotency_key: Required non-empty replay key
        expected_revision: Current non-negative selection revision (CAS)

Returns:
    JSON with `success`, `contract_version`, `operation_id`, `revision`,
    `replayed`, and `refreshed` source revision/hash records.

## `okto_pulse_get_card_knowledge_propagation`

Read the card's technical v2 Knowledge state without mutating it. Use the
returned revision as `expected_revision` for the next replace, drop, or refresh
operation.

Args:
    board_id: Board ID
    card_id: Card ID

Returns:
    JSON with `contract_version`, `revision`, `selection_state`, and assignments
    projected as stable root ID, mode, origin class, state, and stale flag.

### v2 concurrency and replay rules

- Existing-card mutations require the exact current revision returned by
  `okto_pulse_get_card_knowledge_propagation`.
- `knowledge_propagation_revision_conflict` is not permission to overwrite:
  read again, confirm the desired intent, then submit it with a new
  idempotency key and current revision.
- Reuse an `idempotency_key` only for an identical retry. Exact replays return
  the original operation/result with `replayed=true`; changed intent returns
  `knowledge_propagation_idempotency_conflict`.
- V2 creation uses `expected_revision=0`. If a bounded creation race remains
  retryable, repeat the exact request with the same key so the durable result
  can be recovered.

## `okto_pulse_delete_card_knowledge`

Deprecated / blocked. Card Knowledge Base resources are read-only governed
snapshots. Delete or update the source Knowledge Base, then refresh card context
with `okto_pulse_copy_knowledge_to_card`.

## `okto_pulse_delete_ideation_knowledge`

Delete a knowledge base item from an ideation.

## `okto_pulse_delete_refinement_knowledge`

Delete a knowledge base item from a refinement.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    knowledge_id: Knowledge base item ID

Returns:
    JSON with success status

## `okto_pulse_delete_spec_knowledge`

Delete a knowledge base item from a spec.

Args:
    board_id: Board ID
    spec_id: Spec ID
    knowledge_id: Knowledge base item ID

Returns:
    JSON with success status

## `okto_pulse_get_card_knowledge`

Get a single KE by id from a card's inline knowledge_bases array.

## `okto_pulse_get_ideation_knowledge`

Get the full content of an ideation knowledge base item.

## `okto_pulse_get_refinement_knowledge`

Get the full content of a refinement knowledge base item.

Args:
    board_id: Board ID
    refinement_id: Refinement ID
    knowledge_id: Knowledge base item ID

Returns:
    JSON with full knowledge base content

## `okto_pulse_get_spec_knowledge`

Get the full content of a knowledge base item.

Args:
    board_id: Board ID
    spec_id: Spec ID
    knowledge_id: Knowledge base item ID

Returns:
    JSON with full knowledge base content

## `okto_pulse_list_knowledge`

List knowledge base items for a spec, ideation, refinement, or card.

    Consolidates: list_spec_knowledge, list_ideation_knowledge,
    list_refinement_knowledge, list_card_knowledge.

    Args:
        board_id: Board ID
        entity_type: One of: spec, ideation, refinement, card
        entity_id: ID of the entity
        filters: Optional filter dict OR JSON string.
            mime_type: filter by MIME type

    Returns:
        JSON {knowledge_bases: [...], count: int, entity_type: str} or structured error

## `okto_pulse_update_card_knowledge`

Deprecated / blocked. Card Knowledge Base resources are read-only governed
snapshots. Update the source Knowledge Base, then refresh card context with
`okto_pulse_copy_knowledge_to_card`.
