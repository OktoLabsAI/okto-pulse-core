---
version: "1.0"
---

# Tool docs — `knowledge`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

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

Returns:
    JSON with created knowledge base item

## `okto_pulse_copy_knowledge_to_card`

Copy knowledge base entries from a spec to a card as inline card KEs.
Each copied entry is stored in Card.knowledge_bases with stable provenance.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID
    knowledge_ids: Multi-value knowledge base IDs to copy (empty = copy ALL) —
        formats: okto-pulse://reference/multivalue.

Returns:
    JSON with count of knowledge entries copied and total card KEs

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
