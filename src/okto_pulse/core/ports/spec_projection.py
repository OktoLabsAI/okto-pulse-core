"""Public ownership rules for deterministic Spec relationship projections.

Adapters perform storage operations; this contract decides which relationships
belong to the projection. A similar prefix or a matching pair is insufficient.
"""

from dataclasses import dataclass

SCENARIO_CRITERIA_NAMESPACE = "scenario_criteria"
SCENARIO_CRITERIA_RULES = frozenset({"tests/ac_match@v2.0", "tests/ac_match@v2.1"})


@dataclass(frozen=True, slots=True)
class SpecRelationshipFamily:
    namespace: str
    edge_type: str
    source_type: str
    source_sections: tuple[str, ...]
    target_sections: tuple[tuple[str, str], ...]
    rules: frozenset[str]

    def owns_endpoints(self, *, owner_id, source_type, target_type, source_ref, target_ref):
        return (source_type == self.source_type
            and any(is_spec_child_reference(source_ref, owner_id=owner_id, section=section)
                for section in self.source_sections)
            and any(target_type == kind and is_spec_child_reference(target_ref, owner_id=owner_id, section=section)
                for kind, section in self.target_sections))

    def owns_writer(self, *, rule_id, layer, created_by):
        return rule_id in self.rules and layer == 'deterministic' and created_by == 'worker_layer1'


_FAMILIES = {
    SCENARIO_CRITERIA_NAMESPACE: SpecRelationshipFamily(SCENARIO_CRITERIA_NAMESPACE, 'tests',
        'TestScenario', ('test_scenario',), (('Criterion', 'ac'),), SCENARIO_CRITERIA_RULES),
    'decision_requirements': SpecRelationshipFamily('decision_requirements', 'derives_from',
        'Decision', ('decision', 'decision_legacy'), (('Requirement', 'fr'), ('Constraint', 'tr')),
        frozenset({'derives_from/cooccurrence@v2.0', 'derives_from/explicit_link@v2.0',
                   'derives_from/explicit_link@v2.1'})),
}


def spec_relationship_family(namespace: str) -> SpecRelationshipFamily:
    """Closed families only; callers cannot supply their own cleanup grammar."""
    try:
        return _FAMILIES[namespace]
    except (KeyError, TypeError):
        raise ValueError('spec_relationship_namespace_invalid') from None


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
