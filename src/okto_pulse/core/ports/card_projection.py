"""Closed ownership and provenance of observed Card-to-Spec-child links.

These rules record the relational references that were observed. Even a
reciprocal link is neither an execution result nor coverage accepted by a gate.
"""
from dataclasses import dataclass

from okto_pulse.core.domain.delivery_inventory import COLLECTIONS


@dataclass(frozen=True, slots=True)
class CardChildFamily:
    field: str
    section: str
    target_type: str

    @property
    def namespace(self):
        return 'card_child_' + self.section

    @property
    def rule(self):
        return 'supports/' + self.namespace + '@v2.1'

    def owns_endpoints(self, *, owner_id, source_type, target_type, source_ref, target_ref):
        if type(target_ref) is not str:
            return False
        parts = target_ref.split(':')
        return (source_type in {'Entity', 'Bug'} and source_ref == f'card:{owner_id}'
            and target_type == self.target_type and len(parts) == 4
            and parts[0] == 'spec' and parts[2] == self.section
            and all(part and part.strip() == part for part in parts))

    def owns_writer(self, *, rule_id, layer, created_by):
        return rule_id == self.rule and layer == 'deterministic' and created_by == 'worker_layer1'


_CHILD_TYPES = {
    'fr': ('fr', 'Requirement'), 'tr': ('tr', 'Constraint'),
    'ac': ('ac', 'Criterion'), 'br': ('business_rule', 'Constraint'),
    'api': ('api_contract', 'APIContract'), 'ir': ('integration_requirement', 'Requirement'),
    'or': ('observability_requirement', 'Constraint'), 'decision': ('decision', 'Decision'),
}
CARD_CHILD_FAMILIES = tuple(CardChildFamily(field, *_CHILD_TYPES[prefix]) for prefix, field in COLLECTIONS)
CARD_CHILD_NAMESPACES = frozenset(family.namespace for family in CARD_CHILD_FAMILIES)
CARD_PROJECTION_FIELDS = frozenset(family.field for family in CARD_CHILD_FAMILIES) | {'test_scenarios'}


def card_child_family(namespace):
    for family in CARD_CHILD_FAMILIES:
        if family.namespace == namespace:
            return family
    raise ValueError('card_child_namespace_invalid')


def is_card_child_writer(*, edge_type, source_type, target_type, rule_id, layer, created_by):
    return (edge_type == 'supports' and source_type in {'Entity', 'Bug'}
        and any(family.target_type == target_type and family.owns_writer(
            rule_id=rule_id, layer=layer, created_by=created_by) for family in CARD_CHILD_FAMILIES))


def is_card_projection_writer(*, edge_type, source_type, target_type, rule_id, layer, created_by):
    if source_type not in {'Entity', 'Bug'}:
        return False
    if edge_type == 'precedes' and target_type in {'Entity', 'Bug'}:
        return is_card_dependency_writer(rule_id=rule_id, layer=layer, created_by=created_by)
    if edge_type == 'belongs_to' and target_type == 'Entity':
        return is_card_parent_writer(rule_id=rule_id, layer=layer, created_by=created_by)
    if edge_type == 'supports' and target_type == 'TestScenario':
        return is_card_scenario_writer(rule_id=rule_id, layer=layer, created_by=created_by)
    return is_card_child_writer(edge_type=edge_type, source_type=source_type, target_type=target_type,
        rule_id=rule_id, layer=layer, created_by=created_by)


def spec_linked_card_ids(before, after):
    """Old/new consumers across the authoritative child collection registry."""
    return scenario_linked_card_ids(*(values.get(field) for values in (before, after)
                                      for field in CARD_PROJECTION_FIELDS))


CARD_SCENARIO_NAMESPACE = 'card_scenarios'
CARD_PARENT_NAMESPACE = 'card_parent'
CARD_PARENT_RULE = 'belongs_to/card_to_spec@v2.0'
CARD_DEPENDENCY_NAMESPACE = 'card_dependencies'
CARD_DEPENDENCY_RULE_PREFIX = 'precedes/card_dependency/'


def card_dependency_rule(dependency_id):
    if (type(dependency_id) is not str or not dependency_id or dependency_id.strip() != dependency_id
            or any(character in dependency_id for character in '/@:')):
        raise ValueError('card_dependency_identity_invalid')
    return CARD_DEPENDENCY_RULE_PREFIX + dependency_id + '@v2.1'


def is_card_dependency_writer(*, rule_id, layer, created_by):
    if type(rule_id) is not str or not rule_id.startswith(CARD_DEPENDENCY_RULE_PREFIX) or not rule_id.endswith('@v2.1'):
        return False
    identity = rule_id[len(CARD_DEPENDENCY_RULE_PREFIX):-len('@v2.1')]
    try:
        return card_dependency_rule(identity) == rule_id and layer == 'deterministic' and created_by == 'worker_layer1'
    except ValueError:
        return False


def owns_card_dependency_endpoints(*, owner_id, source_type, target_type, source_ref, target_ref):
    parts = source_ref.split(':') if type(source_ref) is str else ()
    return (source_type in {'Entity', 'Bug'} and target_type in {'Entity', 'Bug'}
        and target_ref == f'card:{owner_id}' and len(parts) == 2 and parts[0] == 'card'
        and bool(parts[1]) and parts[1].strip() == parts[1] and parts[1] != owner_id)


CARD_SCENARIO_RULE_PREFIX = 'supports/card_scenario_observed_'
CARD_SCENARIO_RULES = frozenset(
    CARD_SCENARIO_RULE_PREFIX + origin + '@v2.1'
    for origin in ('card', 'spec', 'reciprocal')
)


def scenario_linked_card_ids(*collections):
    """Bounded identifiers retained from before/after scenario collections.

    This is invalidation metadata, never validation or permission to adopt a
    link. Domain validators keep authority over malformed references.
    """
    return sorted({identity for collection in collections
                   if isinstance(collection, (list, tuple))
                   for item in collection if isinstance(item, dict)
                   if isinstance(item.get('linked_task_ids'), (list, tuple))
                   for identity in (item.get('linked_task_ids') or [])
                   if type(identity) is str and identity.strip() == identity and identity})


def card_scenario_rule(*, card_reference: bool, spec_reference: bool) -> str:
    if type(card_reference) is not bool or type(spec_reference) is not bool or not (card_reference or spec_reference):
        raise ValueError('card_scenario_origin_invalid')
    origin = 'reciprocal' if card_reference and spec_reference else 'card' if card_reference else 'spec'
    return CARD_SCENARIO_RULE_PREFIX + origin + '@v2.1'


def is_scenario_source_reference(reference):
    if type(reference) is not str:
        return False
    parts = reference.split(':')
    return (len(parts) == 4 and parts[0] == 'spec' and parts[2] == 'test_scenario'
            and all(part and part.strip() == part for part in parts))


def owns_card_scenario_endpoints(*, owner_id, source_type, target_type, source_ref, target_ref):
    return (source_type in {'Entity', 'Bug'} and target_type == 'TestScenario'
            and source_ref == f'card:{owner_id}' and is_scenario_source_reference(target_ref))


def is_card_scenario_writer(*, rule_id, layer, created_by):
    return rule_id in CARD_SCENARIO_RULES and layer == 'deterministic' and created_by == 'worker_layer1'


def is_spec_source_reference(reference):
    if type(reference) is not str:
        return False
    parts = reference.split(':')
    return len(parts) == 2 and parts[0] == 'spec' and bool(parts[1]) and parts[1].strip() == parts[1]


def owns_card_parent_endpoints(*, owner_id, source_type, target_type, source_ref, target_ref):
    return (source_type in {'Entity', 'Bug'} and target_type == 'Entity'
            and source_ref == f'card:{owner_id}' and is_spec_source_reference(target_ref))


def is_card_parent_writer(*, rule_id, layer, created_by):
    return rule_id == CARD_PARENT_RULE and layer == 'deterministic' and created_by == 'worker_layer1'
