"""Public ownership rules for deterministic Spec relationship projections.

Adapters perform storage operations; this contract decides which relationships
belong to the projection. A similar prefix or a matching pair is insufficient.
"""

SCENARIO_CRITERIA_NAMESPACE = "scenario_criteria"
SCENARIO_CRITERIA_RULES = frozenset({"tests/ac_match@v2.0", "tests/ac_match@v2.1"})


def is_spec_child_reference(reference: str, *, owner_id: str, section: str) -> bool:
    if type(reference) is not str or type(owner_id) is not str or not owner_id:
        return False
    parts = reference.split(":")
    return (len(parts) == 4 and parts[:3] == ["spec", owner_id, section]
            and bool(parts[3]) and parts[3].strip() == parts[3])


def is_scenario_criterion_writer(*, rule_id: str, layer: str, created_by: str) -> bool:
    return (rule_id in SCENARIO_CRITERIA_RULES
            and layer == "deterministic" and created_by == "worker_layer1")


def owns_scenario_criterion_edge(*, owner_id: str, source_ref: str, target_ref: str,
                                rule_id: str, layer: str, created_by: str) -> bool:
    return (is_spec_child_reference(source_ref, owner_id=owner_id, section="test_scenario")
            and is_spec_child_reference(target_ref, owner_id=owner_id, section="ac")
            and is_scenario_criterion_writer(rule_id=rule_id, layer=layer, created_by=created_by))
