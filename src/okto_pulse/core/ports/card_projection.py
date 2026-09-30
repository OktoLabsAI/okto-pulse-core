"""Closed ownership and provenance of observed Card-to-scenario links.

These rules record the relational references that were observed. Even a
reciprocal link is neither an execution result nor coverage accepted by a gate.
"""
CARD_SCENARIO_NAMESPACE = 'card_scenarios'
CARD_PARENT_NAMESPACE = 'card_parent'
CARD_PARENT_RULE = 'belongs_to/card_to_spec@v2.0'
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
