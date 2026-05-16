---
version: "1.0"
---

# Destructive Operations — Read Before Calling

Some MCP tools are **irreversible** at the storage layer. Calling them by mistake is one of the most costly failure modes an agent can hit because there is no undo and no confirmation prompt.

## Hard Delete — Row is Physically Removed, Cannot Be Recovered

| Tool | What it destroys |
|---|---|
| `okto_pulse_delete_card` | The card and all its Q&A, comments, attachments, validations, conclusions. |
| `okto_pulse_delete_spec` | The spec. Cards that referenced it become orphaned (spec_id preserved but `okto_pulse_get_spec` fails). |
| `okto_pulse_delete_ideation` / `okto_pulse_delete_refinement` | The ideation/refinement and every derived child (refinements, specs). |
| `okto_pulse_delete_attachment` | The file blob. |
| `okto_pulse_delete_comment` / `okto_pulse_delete_question` | The comment or Q&A item. |
| `okto_pulse_delete_guideline` | The guideline (globally, if it's a global guideline). |
| `okto_pulse_delete_spec_knowledge` | The attached knowledge base content. |
| `okto_pulse_delete_screen_mockup` | The mockup HTML. |
| `okto_pulse_remove_business_rule` / `okto_pulse_remove_api_contract` | The BR / contract. Linked tasks remain but the coverage gate may now fail. |
| `okto_pulse_delete_spec_evaluation` / `okto_pulse_delete_sprint_evaluation` | The evaluation entry (audit trail is lost). |

## Soft-Delete — Entity Stays but Becomes Unreachable Through Normal Queries

| Tool | Effect |
|---|---|
| `okto_pulse_remove_decision` | Sets `status="revoked"`. Decision stays in `spec.decisions[]` for audit. Reversible via `okto_pulse_update_decision(status="active")`. |
| `okto_pulse_archive_tree` | Sets `archived=true` on the whole sub-tree. Fully reversible via `okto_pulse_restore_tree`. |

## Session-Level

| Tool | Effect |
|---|---|
| `okto_pulse_kg_abort_consolidation` | Drops an in-flight consolidation session. Candidates added so far are lost but nothing persisted is affected. Safe. |

## Rules of Engagement

1. **Prefer soft-delete** (`okto_pulse_archive_tree`, `okto_pulse_remove_decision`) whenever the intent is "hide this from normal views". Only use hard delete when the entity must not exist at all (e.g. GDPR erasure, deleting truly broken test cards).
2. **Before any hard delete, post a comment** on the parent entity with a one-line rationale and @mention the user. If the user objects, you can still recover.
3. **Never delete as a shortcut to fix a validation error.** If the system is rejecting a move because an entity exists, fix the entity, don't delete it.
4. **`okto_pulse_remove_business_rule` / `okto_pulse_remove_api_contract` break coverage** — the spec that depended on them will now fail `okto_pulse_submit_spec_validation`. Use them only when you're replacing the BR/contract with another one in the same action.
5. **`okto_pulse_delete_ideation` / `okto_pulse_delete_refinement` cascade.** You're deleting the entire sub-tree, not just the ideation. Confirm the blast radius.
