"""Evaluate complete relational reference sources without consulting the graph."""
from collections import Counter
from dataclasses import dataclass
import hashlib
import json

from okto_pulse.core.ports.card_projection import CARD_SCENARIO_NAMESPACE, card_scenario_rule
from okto_pulse.core.ports.projection_findings import ProjectionFindingSnapshot, ProjectionReferenceFinding


def reference_ids(value, field):
    if value is None:
        return set()
    if (not isinstance(value, (list, tuple))
            or any(type(item) is not str or not item or item.strip() != item or ':' in item for item in value)):
        raise ValueError('card_scenario_source_invalid:' + field)
    return set(value)


@dataclass(frozen=True, slots=True)
class CardScenarioReferenceAnalysis:
    links: tuple[tuple[str, str], ...]
    snapshot: ProjectionFindingSnapshot


def analyze_card_scenario_references(*, board_id, card_id, spec_id, card_links,
                                     parent_exists: bool, scenarios):
    """Absence must be established by an authoritative read, not an exception.

    ``scenarios=None`` is valid only for a known-absent parent. Provider errors
    must propagate before this function, never become ``parent_exists=False``.
    An existing parent with an empty collection is a complete, different source.
    """
    card_ids = reference_ids(card_links, 'test_scenario_ids')
    if type(parent_exists) is not bool:
        raise ValueError('card_scenario_parent_state_invalid')
    if parent_exists:
        if not spec_id or not isinstance(scenarios, list) or any(type(item) is not dict for item in scenarios):
            raise ValueError('card_scenario_source_invalid:test_scenarios')
    elif scenarios is not None:
        raise ValueError('card_scenario_parent_state_invalid')
    normalized = []
    for item in scenarios or []:
        identity = item.get('id')
        if type(identity) is not str or not identity or identity.strip() != identity or ':' in identity:
            raise ValueError('card_scenario_source_invalid:scenario_id')
        normalized.append((identity, sorted(reference_ids(item.get('linked_task_ids'), 'linked_task_ids'))))
    counts = Counter(identity for identity, _ in normalized)
    observed = {identity: card_id in linked for identity, linked in normalized}
    selected, findings = [], []
    for identity in sorted(card_ids | {identity for identity, linked in normalized if card_id in linked}):
        reason = ('parent_absent' if not parent_exists else 'target_absent' if identity not in counts
                  else 'target_ambiguous' if counts[identity] != 1 else None)
        if reason:
            # Without parent context the intended value remains its literal
            # source token. It is not a fabricated, globally resolved graph ref.
            target = f'spec:{spec_id}:test_scenario:{identity}' if spec_id else identity
            selector = (f'card:{card_id}:test_scenario_ids' if identity in card_ids
                        else f'spec:{spec_id}:test_scenario:{identity}:linked_task_ids')
            findings.append(ProjectionReferenceFinding(board_id, 'card', card_id, CARD_SCENARIO_NAMESPACE,
                selector, target, reason))
        else:
            selected.append((identity, card_scenario_rule(card_reference=identity in card_ids,
                                                         spec_reference=observed[identity])))
            if (identity in card_ids) != observed[identity]:
                selector = (f'card:{card_id}:test_scenario_ids' if identity in card_ids
                            else f'spec:{spec_id}:test_scenario:{identity}:linked_task_ids')
                findings.append(ProjectionReferenceFinding(
                    board_id, 'card', card_id, CARD_SCENARIO_NAMESPACE, selector,
                    f'spec:{spec_id}:test_scenario:{identity}', 'source_disagreement'))

    fingerprint = hashlib.sha256(json.dumps(
        ['card-scenario-source/v1', board_id, card_id, spec_id, parent_exists,
         sorted(card_ids), sorted(normalized)], separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    snapshot = ProjectionFindingSnapshot(board_id, 'card', card_id, CARD_SCENARIO_NAMESPACE,
                                         fingerprint, tuple(findings))
    return CardScenarioReferenceAnalysis(tuple(selected), snapshot)
