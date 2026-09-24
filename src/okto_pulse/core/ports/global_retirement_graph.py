"""Global cache retirement derives ownership and counts from the Board graph.

Digest node_type, short-id prefixes and global row counts are not authority.
No physical graph operation or new permission is defined here.
"""

from dataclasses import dataclass, replace
import re

from okto_pulse.core.kg.logical_transfer import LOGICAL_NULL
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
from okto_pulse.core.ports.retirement_graph import GraphRetirementPlan, _measure, verify_graph_retirement_selection

_LIMIT = 100_000
_MAX_BOARDS = 256


@dataclass(frozen=True, slots=True)
class GlobalRetirementSourceFacts:
    board_plan: GraphRetirementPlan
    removed_node_ids: tuple[str, ...]
    before_count: int
    after_count: int

    def __post_init__(self):
        if (not isinstance(self.board_plan, GraphRetirementPlan)
                or self.removed_node_ids != tuple(sorted({key for _, key in self.board_plan.node_keys}))
                or any(type(value) is not int or not 0 <= value <= _LIMIT for value in (self.before_count, self.after_count))
                or self.after_count > self.before_count
                or self.before_count - self.after_count > len(self.removed_node_ids)):
            raise ValueError("global_retirement_source_facts_invalid")


def global_retirement_source_facts(snapshot, plan: GraphRetirementPlan) -> GlobalRetirementSourceFacts:
    """Derive counts using the existing outbox's publishable-source predicate.

    The fixed original snapshot is checked against both fingerprints first.
    This is source absence planning; cached digest type is deliberately unused.
    """
    verify_graph_retirement_selection(snapshot, plan)
    removed_keys = frozenset(plan.node_keys)
    removed_ids = {key for _, key in removed_keys}
    surviving_ids, publishable, retained_publishable = set(), set(), set()
    for batch in snapshot.iter_nodes(batch_size=500):
        for node in batch:
            removed = (node.type_name, node.key) in removed_keys
            if not removed:
                surviving_ids.add(node.key)
            # Mirrors GlobalOutboxProcessor._read_board_digestable_node_types:
            # embedding present AND neither revocation nor supersession.
            if (node.type_name in VECTOR_INDEX_TYPES and node.properties.get("embedding", LOGICAL_NULL) != LOGICAL_NULL
                    and node.properties.get("revocation_reason", LOGICAL_NULL) == LOGICAL_NULL
                    and node.properties.get("superseded_by", LOGICAL_NULL) == LOGICAL_NULL):
                if node.key in publishable:
                    raise ValueError("global_retirement_source_identity_ambiguous")
                publishable.add(node.key)
                if not removed:
                    retained_publishable.add(node.key)
    if removed_ids & surviving_ids:
        raise ValueError("global_retirement_source_identity_ambiguous")
    return GlobalRetirementSourceFacts(plan, tuple(sorted(removed_ids)), len(publishable), len(retained_publishable))


@dataclass(frozen=True, slots=True)
class GlobalGraphRetirementPlan:
    sources: tuple[GlobalRetirementSourceFacts, ...]
    before_sha256: str
    after_sha256: str
    digest_ids: tuple[str, ...]
    removed_relations: int
    board_counts: tuple[tuple[str, int, int], ...]

    def __post_init__(self):
        if (type(self.sources) is not tuple or len(self.sources) > _MAX_BOARDS
                or any(not isinstance(item, GlobalRetirementSourceFacts) for item in self.sources)
                or sum(len(item.board_plan.node_keys) for item in self.sources) > _LIMIT
                or any(type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
                    for value in (self.before_sha256, self.after_sha256))
                or type(self.digest_ids) is not tuple or len(self.digest_ids) > _LIMIT
                or any(type(key) is not str or not 1 <= len(key) <= 256 for key in self.digest_ids)
                or self.digest_ids != tuple(sorted(set(self.digest_ids)))
                or type(self.removed_relations) is not int or not 0 <= self.removed_relations <= _LIMIT
                or (not self.digest_ids and self.removed_relations != 0)
                or type(self.board_counts) is not tuple):
            raise ValueError("global_retirement_plan_invalid")
        owners = tuple(item.board_plan.board_id for item in self.sources)
        if owners != tuple(sorted(set(owners))):
            raise ValueError("global_retirement_plan_invalid")
        facts = {item.board_plan.board_id: item for item in self.sources}
        if (len(self.board_counts) > len(facts)
                or any(type(item) is not tuple or len(item) != 3 or item[0] not in facts
                    or type(item[1]) is not int or type(item[2]) is not int
                    or (item[1], item[2]) != (facts[item[0]].before_count, facts[item[0]].after_count)
                    or item[1] == item[2] for item in self.board_counts)
                or tuple(item[0] for item in self.board_counts) != tuple(sorted({item[0] for item in self.board_counts}))
                or bool(self.digest_ids or self.board_counts) != (self.before_sha256 != self.after_sha256)):
            raise ValueError("global_retirement_plan_invalid")


def plan_global_graph_retirement(snapshot, sources: tuple[GlobalRetirementSourceFacts, ...]) -> GlobalGraphRetirementPlan:
    if (type(sources) is not tuple or len(sources) > _MAX_BOARDS
            or any(not isinstance(item, GlobalRetirementSourceFacts) for item in sources)
            or sum(len(item.board_plan.node_keys) for item in sources) > _LIMIT):
        raise ValueError("global_retirement_source_facts_invalid")
    ordered = tuple(sorted(sources, key=lambda item: item.board_plan.board_id))
    facts = {item.board_plan.board_id: item for item in ordered}
    if len(facts) != len(ordered):
        raise ValueError("global_retirement_duplicate_board")
    # A Board whose plan removes no source is outside this operation.
    targets = {owner: frozenset(item.removed_node_ids) for owner, item in facts.items() if item.removed_node_ids}
    boards, selected_owners, counts = set(), set(), []

    def should_remove(node):
        if node.type_name != "DecisionDigest":
            return False
        owner = node.properties.get("board_id")
        if owner in targets and node.properties.get("original_node_id") in targets[owner]:
            selected_owners.add(owner)
            return True
        return False

    def transform(node):
        if node.type_name != "Board" or node.key not in targets:
            return node
        boards.add(node.key)
        source = facts[node.key]
        if node.properties.get("decision_count") != source.before_count:
            raise ValueError("global_retirement_board_count_mismatch")
        if source.before_count == source.after_count:
            return node
        counts.append((node.key, source.before_count, source.after_count))
        return replace(node, properties={**node.properties, "decision_count": source.after_count})

    before, after, keys, relations = _measure(snapshot, should_remove, scope="global_discovery", transform=transform)
    if selected_owners - boards:
        raise ValueError("global_retirement_board_summary_missing")
    return GlobalGraphRetirementPlan(ordered, before, after, tuple(key for _, key in keys), relations, tuple(sorted(counts)))


def verify_global_retirement_selection(snapshot, plan: GlobalGraphRetirementPlan) -> None:
    if not isinstance(plan, GlobalGraphRetirementPlan) or plan_global_graph_retirement(snapshot, plan.sources) != plan:
        raise ValueError("global_retirement_selection_mismatch")
