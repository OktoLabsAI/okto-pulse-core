---
version: "1.0"
---

# Destructive Operations — Read Before Calling

Some MCP tools are **irreversible** at the storage layer. Calling them by mistake is one of the most costly failure modes an agent can hit because there is no undo and no confirmation prompt.

## Hard Delete — Row is Physically Removed, Cannot Be Recovered

| Tool | What it destroys |
|---|---|
| `DELETE /api/v1/boards/{board_id}` | The board, every source entity, relational KG/KB history, uploaded attachment, board graph, rebuild/audit/quarantine artifact, and every physical Global Discovery generation that could retain the board. Because Global Discovery is a derived cross-board store, the delete invalidates it for all boards; rebuild/recovery must rematerialize it from the remaining relational sources. |
| `okto_pulse_delete_card` | The card and all its Q&A, comments, attachments, validations, conclusions. |
| `okto_pulse_delete_spec` | The spec and its derived sprints. Cards survive as orphans with `spec_id=null`. |
| `okto_pulse_delete_ideation` / `okto_pulse_delete_refinement` | The ideation/refinement and every derived child (refinements, specs). |
| `okto_pulse_delete_attachment` | The file blob. |
| `okto_pulse_delete_comment` / `okto_pulse_delete_question` | The comment or Q&A item. |
| `okto_pulse_delete_guideline` | The guideline (globally, if it's a global guideline). |
| `okto_pulse_delete_spec_knowledge` | The attached knowledge base content. |
| `okto_pulse_delete_screen_mockup` | The mockup HTML. |
| `okto_pulse_remove_business_rule` / `okto_pulse_remove_api_contract` | The BR / contract. Linked tasks remain but the coverage gate may now fail. |
| `okto_pulse_remove_spec_entity` | Consolidated removal for `business_rule` / `api_contract` / `decision`. **Asymmetry:** `business_rule` and `api_contract` are HARD removals; `decision` is a soft-delete (`status="revoked"`, restorable). See `okto-pulse://reference/tool-families/spec_entity_remove`. |
| `okto_pulse_delete_spec_evaluation` / `okto_pulse_delete_sprint_evaluation` | The evaluation entry (audit trail is lost). |
| `okto_pulse_delete_topic` | The Topic. Only allowed when it has NO associated Stories, including archived ones (`topic_not_empty` otherwise). |
| `okto_pulse_delete_test_scenario` | The scenario, AND its id is dropped from every card's `test_scenario_ids` in atomic CASCADE. Does not block on existing links — coverage gates may start failing. |
| `okto_pulse_delete_architecture_design` | The Architecture Design. |
| `okto_pulse_delete_design_system` | The Design System (admin write, `SPECS_UPDATE`). |
| `okto_pulse_delete_ideation_knowledge` / `okto_pulse_delete_refinement_knowledge` | The knowledge base item on the ideation/refinement. (`okto_pulse_delete_card_knowledge` is deprecated — card KB resources are read-only governed snapshots.) |
| `okto_pulse_delete_ideation_question` / `okto_pulse_delete_refinement_question` / `okto_pulse_delete_spec_question` / `okto_pulse_delete_sprint_question` | The Q&A item on that entity, including any recorded answer. |
| `okto_pulse_remove_card_dependency` | The dependency link between two cards (the cards survive; re-add to undo). |

## Soft-Delete — Entity Stays but Becomes Unreachable Through Normal Queries

| Tool | Effect |
|---|---|
| `okto_pulse_remove_decision` | Sets `status="revoked"`. Decision stays in `spec.decisions[]` for audit. Reversible via `okto_pulse_update_decision(status="active")`. |
| `okto_pulse_archive_tree` | Sets `archived=true` on the whole sub-tree. Fully reversible via `okto_pulse_restore_tree`. |
| `okto_pulse_archive_story` / `okto_pulse_archive_topic` | Archives the story/topic. Reversible via `okto_pulse_restore_story` / `okto_pulse_restore_topic`. |

## KG Destructive Operations

| Operation | Effect | Safeguard |
|---|---|---|
| `okto_pulse_kg_rebuild_preflight` → `okto_pulse_kg_rebuild_confirm` → `okto_pulse_kg_rebuild_run` | Rebuild **discards the board's current graph generation** and re-derives it from board sources. | Three-step hash/token/lock gate. It handles board `graph_state=recovery_needed`; if the board graph is healthy and only discovery failed, it refuses with `board_rebuild_wrong_recovery_scope`. Generic `overall_state` is not a rebuild signal. |
| `okto_pulse_kg_global_discovery_recovery_preflight` → `okto_pulse_kg_global_discovery_recovery_confirm` → `okto_pulse_kg_global_discovery_recovery_run` | Replaces the derived global discovery cache, never authoritative board graphs. | Admitted only for healthy board graphs + concrete discovery recovery signal. Before dispatch, `run` persists integrity-bound worker inputs and a durable SQL control row, then returns `accepted`; use the status/cancel/resume tools for the owned background attempt. A fully materialized generation is validated before one hashed pointer switch; legacy bytes and all sidecars remain intact, and failed readback rolls the pointer back. One durable cross-process fence excludes recovery, outbox, schema, search-WAL and GC writers. |
| `okto_pulse_kg_quarantine_restore` | With `apply=true`, swaps the board's live graph files for a quarantined snapshot. | `apply=false` (default) returns an auditable plan with NO mutation. `apply=true` first moves the live files to a NEW quarantine with manifest (`backup_quarantine_id` in the result), so the swap is reversible; a `partial_restore` error records exact state for rollback. |
| KG dedup hard-delete (`kg_dedup_hard_delete`) | Physical node delete + bulk edge re-point. | **FORBIDDEN** at every surface by the curation policy — this is the mutation class behind the KGD-01 corruption. Dedup merges (`kg_dedup_entities`) and `kg_unmerge` are propose-only and REVERSIBLE via the equivalence ledger; prefer unmerge, never hard delete. |

## Session-Level

| Tool | Effect |
|---|---|
| `okto_pulse_kg_abort_consolidation` | Drops an in-flight consolidation session. Candidates added so far are lost but nothing persisted is affected. Safe. |

## Rules of Engagement

1. **Prefer soft-delete** (`okto_pulse_archive_tree`, `okto_pulse_remove_decision`) whenever the intent is "hide this from normal views". Only use hard delete when the entity must not exist at all (e.g. GDPR erasure, deleting truly broken test cards).
2. **Before any hard delete, post a one-line rationale with an @mention.** When
   the target or its parent supports comments, post it there. When that artifact
   family has no comment surface, post it on a board-scoped audit card and name
   the exact target type + id. The audit card is the governed fallback; never
   invent an unsupported comment call or skip the rationale silently.
3. **Never delete as a shortcut to fix a validation error.** If the system is rejecting a move because an entity exists, fix the entity, don't delete it.
4. **`okto_pulse_remove_business_rule` / `okto_pulse_remove_api_contract` break coverage** — the spec that depended on them will now fail `okto_pulse_submit_spec_validation`. Use them only when you're replacing the BR/contract with another one in the same action.
5. **`okto_pulse_delete_ideation` / `okto_pulse_delete_refinement` cascade.** You're deleting the entire sub-tree, not just the ideation. Confirm the blast radius.
6. **Retain every takedown handle.** Governed entity deletes return a root
   `takedown` receipt. Cascade responses additionally expose recursive
   `descendant_deletions`; each receipt can be followed independently with
   `okto_pulse_kg_takedown_status`.
7. **Delivered graph tombstones contain identity, not deleted content.** A
   governed hard delete converges deterministic graph nodes to
   `revocation_reason=source_deleted`, zero relevance, and an erased semantic
   payload. Even an administrative raw Cypher read with
   `include_working=true` must not recover the former title/body/context,
   source quote, or justification. The Community graph adapter also replaces
   the indexed node without its embedding.
