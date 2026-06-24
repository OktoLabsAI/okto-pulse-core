---
version: "1.0"
---

# Tool docs — `architecture`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_architecture_design`

Create an Architecture Design on an ideation, refinement, spec, or card.

Use this whenever the artifact benefits from explicit architecture: services,
modules, applications, databases, queues, topics, events, external integrations,
runtime boundaries, API contracts, data flows, or task ownership boundaries.

For non-trivial payloads, call okto_pulse_get_architecture_design_schema
first, then okto_pulse_validate_architecture_design_payload. Persist only
after the dry-run returns valid=true and you have reviewed warnings. Warnings
are not blockers, but they usually mark details a downstream implementer or
validator would otherwise have to guess.

The server critiques the full payload before accepting it. Rejections include
contextual paths such as entities[0].name, interfaces[1].participants[0] when legacy participants are supplied, or
diagrams[0].adapter_payload.elements[2].linkedEntityId. Fix the cited field
and retry; do not move invalid architecture into prose fields to bypass the
structured artifact.

Args:
    board_id: Board ID
    parent_type: One of ideation, refinement, spec, card
    parent_id: Parent entity ID
    title: Design title
    global_description: Required global architecture description
    entities: JSON array or native list of entity descriptions. Use concrete
        names and categorical types, for example:
        [
          {
            "id": "entity-web-app",
            "name": "Customer Portal",
            "entity_type": "web_app",
            "responsibility": "Collects checkout input and displays order status.",
            "technologies": ["React", "Vite"]
          },
          {
            "id": "entity-checkout-api",
            "name": "Checkout API",
            "entity_type": "api",
            "responsibility": "Validates checkout commands and orchestrates payment authorization.",
            "technologies": ["FastAPI", "SQLAlchemy"]
          },
          {
            "id": "entity-orders-db",
            "name": "Orders DB",
            "entity_type": "database",
            "responsibility": "Persists orders, payment state, and idempotency keys.",
            "technologies": ["PostgreSQL"]
          }
        ]
        Do not use entity name == entity_type, such as name="API" and
        entity_type="api"; the API rejects that because ownership and task
        boundaries become ambiguous.
    interfaces: JSON array or native list of interface/contract descriptions.
        endpoint is optional but recommended for API paths, RPC methods,
        event names, queue names, or operations. Interfaces do not own
        source/target; diagram connections define endpoint entities through
        sourceElementId and targetElementId. direction must be one of
        source_to_target, target_to_source, bidirectional, none. Example:
        [
          {
            "id": "interface-create-order",
            "name": "Create order",
            "endpoint": "POST /orders",
            "description": "Customer Portal sends a checkout request to Checkout API.",
            "direction": "source_to_target",
            "protocol": "REST",
            "contract_type": "OpenAPI",
            "request_schema": {"type": "object", "required": ["cart_id"]},
            "response_schema": {"type": "object", "required": ["order_id"]}
          }
        ]
    diagrams: JSON array or native list of diagrams; adapter_payload is stored
        separately. Only format="excalidraw_json" is accepted. Mermaid,
        PlantUML, C4, SVG, and raw snippets may be included only as
        descriptive text in entity responsibility, boundaries, notes, or
        global_description, not as diagrams[].format. Excalidraw payloads should link elements using
        linkedEntityId and linkedInterfaceIds when possible. For Excalidraw
        edges, use sourceElementId, targetElementId, linkedInterfaceIds,
        and connectionType. One connector can carry several interface
        contracts/endpoints, for example
        linkedInterfaceIds=["interface-create-order", "interface-get-order"].
        linkedInterfaceId remains accepted for legacy single-contract
        edges. connectionType accepts only "direct" and "elbow"; do not
        send "curved". Example:
        [
          {
            "id": "diagram-runtime-context",
            "title": "Runtime context",
            "diagram_type": "context",
            "format": "excalidraw_json",
            "adapter_payload": {"type": "excalidraw", "version": 2, "elements": [], "appState": {}, "files": {}}
          }
        ]

Returns:
    JSON with the created Architecture Design.

## `okto_pulse_copy_architecture_to_card`

Copy Architecture Designs from a spec to a card/task as deep-copy snapshots.

Args:
    board_id: Board ID
    spec_id: Source spec ID
    card_id: Target card ID
    design_ids: Optional multi-value design IDs to copy; empty copies all
    profile: Response projection profile — `summary` (default) | `full` | `legacy`.

Returns:
    Response shape depends on `profile` (R2.3 projection):
    - `summary` (default): copy metadata only — `{copied, design_ids,
      total_on_card, projection}`. It does NOT return `architecture_designs`
      bodies. `projection` is the canonical R5 envelope (`profile`, `outcome`,
      `payload_bytes`, `truncated`, `omitted_count`, `deduped_count`,
      `follow_up[{rel, target_ref}]`). The full bodies are persisted on the card
      regardless of profile — read them with `okto_pulse_get_task_context(profile=full)`
      or re-call here with `profile=full`.
    - `full` / `legacy`: the prior payload with complete bodies —
      `{success, copied, architecture_designs:[...]}`, no projection envelope.
    - Unsupported profile (e.g. `detail`): structured error `unsupported_projection`
      with `supported_profiles=[summary, full, legacy]` (no silent fallback, no copy
      performed).

## `okto_pulse_delete_architecture_design`

Delete an Architecture Design.

Args:
    board_id: Board ID
    design_id: Architecture Design ID

Returns:
    JSON with success true when deleted.

## `okto_pulse_dump_architecture_diagram`

Load and dump a diagram payload through its ArchitectureDiagramAdapter.

Args:
    board_id: Board ID
    design_id: Architecture Design ID
    diagram_id: Diagram ID inside the design

Returns:
    JSON with raw payload and adapter dump string.

## `okto_pulse_get_architecture_design`

Get one Architecture Design by ID.

Args:
    board_id: Board ID
    design_id: Architecture Design ID
    include_payloads: Include heavy diagram adapter payloads (default false)

Returns:
    JSON with the architecture design envelope.

## `okto_pulse_get_architecture_design_schema`

Return the machine-readable Architecture Design payload schema.

Call this before authoring a non-trivial Architecture Design payload. The
schema includes allowed enums, entity/interface contracts, Excalidraw
adapter payload rules, bad examples, good examples, the complete minimal
payload example AND a `semantic_node_registry` section (spec cc497a0d) that
defines the canonical mapping from entity_type to
{displayType, architectureKind, iconName} plus the icon allowlist.

MANDATORY validation flow for MCP agents:
    1. okto_pulse_get_architecture_design_schema(board_id) — read the registry.
    2. Build payload: prefer letting the registry normalize linked nodes by setting
       entity.entity_type + diagram element.linkedEntityId only (text/displayType/
       architectureKind/iconName auto-filled at persist time).
    3. okto_pulse_validate_architecture_design_payload(...) — confirm valid=true and
       surface warnings (semantic_metadata_normalized) to the human.
    4. okto_pulse_add_architecture_design / update_architecture_design — backend
       re-applies normalization + rejects ambiguous payloads (FR3 of spec cc497a0d).

Do NOT invent iconName/displayType/architectureKind outside the registry. Linked
nodes (linkedEntityId set) must either provide all four metadata fields explicitly
or rely on the registry to fill them deterministically; otherwise the payload is
rejected with suggested_fixes.

Args:
    board_id: Board ID

Returns:
    JSON with success=true and schema (includes semantic_node_registry).

## `okto_pulse_import_excalidraw_architecture_diagram`

Import an Excalidraw JSON scene into an Architecture Design.

Args:
    board_id: Board ID
    design_id: Architecture Design ID
    title: Diagram title
    payload_json: Excalidraw JSON object or JSON string
    diagram_type: context/container/component/sequence/deployment/data_flow/other
    replace_diagram_id: Existing diagram ID to replace; empty appends a new diagram
    description: Optional diagram description
    order_index: Diagram order
    change_summary: Optional version summary

Returns:
    JSON with the updated Architecture Design.

## `okto_pulse_list_architecture_designs`

List Architecture Designs for an ideation, refinement, spec, or card.

Args:
    board_id: Board ID
    parent_type: One of ideation, refinement, spec, card
    parent_id: Parent entity ID
    include_payloads: Include heavy diagram adapter payloads (default false)

Returns:
    JSON with architecture designs. Payloads are omitted by default.

## `okto_pulse_update_architecture_design`

Update an Architecture Design. Omitted fields are left unchanged.

For large or generated updates, call okto_pulse_get_architecture_design_schema
once per session and okto_pulse_validate_architecture_design_payload before
this tool. The validator merges omitted update fields from the existing
design and reports blocking issues plus warnings without creating a new
Architecture Design version.

The update is critiqued against the complete resulting design before it is
saved. Use this to keep architecture current as an ideation, refinement, or
spec changes. Prefer replacing entities/interfaces/diagrams with the complete
intended arrays so links remain deterministic.

Contextual validation examples:
- entities[0].name duplicates entity_type "api" -> use a concrete name such
  as "Checkout API" and keep entity_type as "api".
- interfaces[0].participants[1] references an unknown entity -> remove
  legacy participants or correct the participant id/name.
- interfaces[0].direction must be one of source_to_target,
  target_to_source, bidirectional, none.
- diagrams[0].adapter_payload.elements[2].linkedInterfaceIds must reference
  one or more interface ids/names in interfaces.
- diagrams[0].adapter_payload.elements[2] links an interface but the
  connected nodes do not expose linkedEntityId endpoint entities.
- diagrams[0].adapter_payload.elements[2].connectionType must be "direct"
  or "elbow"; use "elbow" for routed/orthogonal connections.

Args:
    board_id: Board ID
    design_id: Architecture Design ID
    title: Optional new title
    global_description: Optional new global description
    entities: Optional JSON array/native list
    interfaces: Optional JSON array/native list
    diagrams: Optional JSON array/native list
    change_summary: Optional version summary

Returns:
    JSON with the updated Architecture Design.

## `okto_pulse_validate_architecture_design_payload`

Dry-run critique for an Architecture Design payload without persisting it.

Use this before okto_pulse_add_architecture_design or
okto_pulse_update_architecture_design. For creates, pass parent_type and
parent_id. For updates, pass design_id and only the fields you intend to
change; omitted fields are merged from the existing design before critique.

The response includes:
- valid: false when the payload would be rejected by create/update.
- issues: blocking contextual errors with JSON paths.
- warnings: non-blocking gaps that reduce implementation clarity.
- suggested_fixes: concrete corrections for common architecture mistakes.
- summary: counts of entities, interfaces, diagrams, elements, and links.

Typical catches:
- entities where name duplicates entity_type after normalization.
- interfaces with invalid legacy participants, invalid direction, or missing
  protocol/contract metadata for schema payloads.
- diagrams with any format other than excalidraw_json. Mermaid, PlantUML,
  C4, SVG, and raw snippets are allowed only as descriptive text in entity
  responsibility, boundaries, notes, or global_description.
- diagrams with invalid linkedEntityId, linkedInterfaceIds, endpoint/entity
  connection mismatches, or unsupported connectionType. Excalidraw
  connectionType accepts only "direct" or "elbow".

Args:
    board_id: Board ID
    parent_type: Create mode parent type: ideation, refinement, spec, card
    parent_id: Create mode parent ID
    design_id: Update mode Architecture Design ID
    title: Candidate title
    global_description: Candidate global description
    entities: Candidate JSON array/native list, or omitted to keep existing in update mode
    interfaces: Candidate JSON array/native list, or omitted to keep existing in update mode
    diagrams: Candidate JSON array/native list, or omitted to keep existing in update mode

Returns:
    JSON dry-run critique; this tool does not write anything.


## `architecture_warning_acknowledgement` is audit-only — NOT a propagation bypass

`architecture_warning_acknowledgement` belongs to the AUTHORING path only: it lets you SAVE
a warning-bearing Architecture Design. The warnings are still recorded as active findings,
and the acknowledgement is stored as audit-only evidence (in `ArchitectureWarningAcknowledgement`).
It does NOT clear the findings and it does NOT authorize copying or propagating that design.
An active finding blocks BOTH Done and copy/propagation (`architecture_propagation_blocked`),
regardless of how many acknowledgements exist, and the Resource Gate fails closed instead of
auto-marking the inherited architecture N/A. To propagate, fix the SOURCE design until the
backend critic resolves the findings, then retry the copy.


## `okto_pulse_list_architecture_propagation_legacy`

Read-only, forward-only diagnostic. Lists Architecture Design snapshots that were copied
before the propagation-eligibility rule and whose SOURCE is now ineligible. It NEVER
backfills, resolves findings, mutates snapshots, or changes any SDLC status.

Args:
    board_id: Board ID
    limit: Max items per page (1..200; default 100)
    offset: Pagination offset (default 0)
    include_clean: "true" to also list snapshots whose source is currently eligible (default "false")
    parent_type_filter: Optional parent_type filter (ideation | refinement | spec | card)

Returns:
    JSON: `{ success, board_id, items: [{ target_design_id, target_parent{type,id},
    source_design_id, source_ref, source_version, legacy_status
    (source_blocked | verdict_missing | source_unavailable), verdict_status, finding_keys,
    remediation, mutation_performed: false }], scanned_total, limit, offset,
    mutation_performed: false }`. No mutation is ever performed.
