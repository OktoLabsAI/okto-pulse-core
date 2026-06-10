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
    linked_requirements: Pipe-separated INDICES (0-based) of functional requirements this rule relates to.
        Example: "0|2|5" links to the 1st, 3rd, and 6th functional requirement.
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

## `okto_pulse_remove_business_rule`

Remove a business rule from a spec.

Args:
    board_id: Board ID
    spec_id: Spec ID
    rule_id: Business rule ID to remove

Returns:
    JSON confirmation

## `okto_pulse_update_business_rule`

Update an existing business rule on a spec.

Args:
    board_id: Board ID
    spec_id: Spec ID
    rule_id: Business rule ID (e.g. "br_abc12345")
    title: New title (optional, empty = no change)
    rule: New rule statement (optional)
    when: New condition (optional)
    then: New outcome (optional)
    linked_requirements: Pipe-separated INDICES (0-based) of functional requirements.
        Pass "CLEAR" to remove all links. Empty = no change.
    notes: New notes (optional, "CLEAR" to remove)

Returns:
    JSON with the updated business rule
