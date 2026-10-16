---
version: "1.0"
---

# Tool docs — `decision`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_decision`

Add a formalized Decision to a spec.

A Decision records a contextual CHOICE — the reasoning behind picking one
path over alternatives. Different from BusinessRule (which is a NORM, a
prescriptive "DEVE" statement): use a Decision to capture design
intent, tradeoffs, or team consensus. The KG extracts Decisions into
queryable nodes. Every active Decision needs a complete verification plan:
selected active obligations and/or an inspection condition with bounded scope.
Task links provide context, not proof. There is no minimum number of Decisions;
do not create a Task or Test Card solely to cover a Decision.

Planning and delivery rules: `okto-pulse://reference/spec_gates` (Decisions
Coverage Gate). Inspection and current proof: `okto-pulse://reference/tool-docs/code-traceability`
(`okto_pulse_get_decision_reviews`, `okto_pulse_record_decision_reviews`).

Args:
    board_id: Board ID
    spec_id: Spec ID
    title: Decision title (e.g. "Use an embedded graph database over Neo4j")
    rationale: Why this choice was made
    context: When/where this applies (optional)
    alternatives_considered: Native string array of alternatives
    supersedes_decision_id: id of another Decision this one replaces; it auto-moves to status=superseded
    linked_requirements: Native string array of requirement refs. Accepted forms:
        FR index/fr_id/text and structured TR id/text. Persisted values are
        canonical ids when the write path resolves them.
    notes: Additional notes
    verification: Native object with `obligation_refs` and/or `inspection`.
        Obligation refs identify active same-Spec FR/TR/BR/IR/OR/AC/API items.
        Inspection has a non-empty `condition` and bounded `scope_refs`.
        Both selected obligation proof and inspection must pass when declared.

Returns:
    JSON with created decision and spec coverage snapshot

Example verification plan for a scope choice (replace the Spec ID):

```json
{
  "verification": {
    "obligation_refs": [],
    "inspection": {
      "condition": "Confirm that the release scope excludes recurring bookings and that its requirements and acceptance criteria do not promise them.",
      "scope_refs": [{"kind": "spec", "id": "<spec_id>"}]
    }
  }
}
```

This is a partial `okto_pulse_add_decision` payload, not an approval. At delivery,
the authorized reviewer records an observed inspection through the review tool;
reviewer separation, currentness and historical conflicts still apply.


## `okto_pulse_remove_spec_entity` (`target_type="decision"`)

Use `okto_pulse_remove_spec_entity` with `target_type="decision"` and `entity_id` naming the target.
See `okto-pulse://reference/tool-families/spec_entity_remove` for parameters, authorization and results.

## `okto_pulse_update_spec_entity` — decision

Use `entity_type="decision", entity_id`, `operation="update"` and an object `payload_json` containing the fields to change. Omit unchanged fields; use JSON null to clear optional fields. Use exact same-Spec IDs for links.
Use the explicit `restore`, `revoke` or `supersede` operation for lifecycle changes. Existing granular permissions and impact checks apply.
