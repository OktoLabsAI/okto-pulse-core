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

    def matches_rule_family(self, rule_id: str) -> bool:
        return any(rule_id.startswith(rule.split('@', 1)[0] + '@') for rule in self.rules)


_FAMILIES = {
    SCENARIO_CRITERIA_NAMESPACE: SpecRelationshipFamily(SCENARIO_CRITERIA_NAMESPACE, 'tests',
        'TestScenario', ('test_scenario',), (('Criterion', 'ac'),), SCENARIO_CRITERIA_RULES),
    'decision_requirements': SpecRelationshipFamily('decision_requirements', 'derives_from',
        'Decision', ('decision', 'decision_legacy'), (('Requirement', 'fr'), ('Constraint', 'tr')),
        frozenset({'derives_from/cooccurrence@v2.0', 'derives_from/explicit_link@v2.0',
                   'derives_from/explicit_link@v2.1'})),
    'business_rule_requirements': SpecRelationshipFamily('business_rule_requirements', 'derives_from',
        'Constraint', ('business_rule',), (('Requirement', 'fr'),),
        frozenset({'derives_from/br_requirement@v2.1'})),
    'integration_requirements': SpecRelationshipFamily('integration_requirements', 'derives_from',
        'Requirement', ('integration_requirement',), (('Requirement', 'fr'), ('Constraint', 'tr')),
        frozenset({'derives_from/ir_requirement@v2.1'})),
    'observability_requirements': SpecRelationshipFamily('observability_requirements', 'derives_from',
        'Constraint', ('observability_requirement',), (('Requirement', 'fr'), ('Constraint', 'tr')),
        frozenset({'derives_from/or_requirement@v2.1'})),
    'observability_integrations': SpecRelationshipFamily('observability_integrations', 'derives_from',
        'Constraint', ('observability_requirement',), (('Requirement', 'integration_requirement'),),
        frozenset({'derives_from/or_integration@v2.1'})),
    'api_business_rules': SpecRelationshipFamily('api_business_rules', 'implements',
        'APIContract', ('api_contract',), (('Constraint', 'business_rule'),),
        frozenset({'implements/api_business_rule@v2.1'})),
}

SPEC_RELATIONSHIP_NAMESPACES = frozenset(_FAMILIES)


def is_spec_relationship_writer(*, edge_type, source_type, target_type, rule_id, layer, created_by):
    return any(family.edge_type == edge_type and family.source_type == source_type
        and any(kind == target_type for kind, _section in family.target_sections)
        and family.owns_writer(rule_id=rule_id, layer=layer, created_by=created_by)
        for family in _FAMILIES.values())


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
