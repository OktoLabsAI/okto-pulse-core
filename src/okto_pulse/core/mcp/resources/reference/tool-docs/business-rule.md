---
version: "1.0"
---

# Tool docs — `business-rule`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_add_business_rule`

Add a business rule to a spec. Business rules define system behavior constraints
using When/Then format.

Args:
    board_id: Board ID
    spec_id: Spec ID
    title: Rule title (e.g. "Discount cap for non-premium users")
    rule: The business rule statement
    when: Condition that triggers the rule
    then: Expected behavior / outcome
    linked_requirements: Pipe-separated functional requirement refs. Accepted forms:
        0-based indices ("0|2|5"), canonical fr_... ids, or exact FR text.
        Human labels such as "FR-1" are not accepted because they are display
        labels, not stable identifiers.
    notes: Additional notes (optional)

Returns:
    JSON with the created business rule including resolved requirement text

## `okto_pulse_list_business_rules`

List all business rules for a spec with linked functional requirements resolved as text.

Args:
    board_id: Board ID
    spec_id: Spec ID

Returns:
    JSON array of business rules with resolved linked requirements

## `okto_pulse_remove_spec_entity` (`target_type="business_rule"`)

Use `okto_pulse_remove_spec_entity` with `target_type="business_rule"` and `entity_id` naming the target.
See `okto-pulse://reference/tool-families/spec_entity_remove` for parameters, authorization and results.

## `okto_pulse_update_spec_entity` — business_rule

Use `entity_type="business_rule", entity_id`, `operation="update"` and an object `payload_json` containing the fields to change. Omit unchanged fields; use JSON null to clear optional fields. Use exact same-Spec IDs for links.
