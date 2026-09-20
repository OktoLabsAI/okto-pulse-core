"""Pure, bounded selection of historical Sprint-owned graph projections.

This migration contract does not authorize a runtime writer or a rebuild.
Session ownership and neighboring nodes never imply source ownership.
"""

from dataclasses import dataclass
import re

from okto_pulse.core.kg.connectivity_guard import WriterClass, classify_writer_path
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalFingerprintAccumulator, LogicalSchemaIndex,
    canonical_bytes, encode_value,
)

_MAX_RECORDS = 100_000
_MAX_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class GraphRetirementPlan:
    board_id: str
    before_sha256: str
    after_sha256: str
    node_keys: tuple[tuple[str, str], ...]
    removed_relations: int

    def __post_init__(self):
        if (type(self.board_id) is not str or not 1 <= len(self.board_id) <= 256
                or any(type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
                    for value in (self.before_sha256, self.after_sha256))
                or type(self.node_keys) is not tuple or len(self.node_keys) > _MAX_RECORDS
                or any(type(key) is not tuple or len(key) != 2 or key[0] not in {"Entity", "Criterion"}
                    or type(key[1]) is not str or not 1 <= len(key[1]) <= 256 for key in self.node_keys)
                or self.node_keys != tuple(sorted(set(self.node_keys)))
                or (bool(self.node_keys) != (self.before_sha256 != self.after_sha256))
                or (not self.node_keys and self.removed_relations != 0)
                or type(self.removed_relations) is not int or not 0 <= self.removed_relations <= _MAX_RECORDS):
            raise ValueError("retirement_graph_plan_invalid")


def _measure(snapshot, select, require_incident=None, *, scope="board", transform=None):
    schema = snapshot.schema()
    if schema.scope != scope:
        raise ValueError("retirement_graph_scope_required")
    declared = snapshot.counts()
    if declared.nodes + declared.relations > _MAX_RECORDS:
        raise ValueError("retirement_graph_record_limit")
    index = LogicalSchemaIndex.build(schema)
    before = LogicalFingerprintAccumulator.for_schema(schema)
    after = LogicalFingerprintAccumulator.for_schema(schema)
    keys, removed = set(), set()
    consumed = size = removed_relations = 0

    def charge(record):
        nonlocal consumed, size
        consumed += 1
        size += len(canonical_bytes({name: encode_value(value) for name, value in record.properties.items()}))
        if consumed > _MAX_RECORDS or size > _MAX_BYTES:
            raise ValueError("retirement_graph_record_limit")

    for batch in snapshot.iter_nodes(batch_size=500):
        if len(batch) > 500:
            raise ValueError("retirement_graph_batch_limit")
        for node in batch:
            charge(node)
            index.validate_node(node)
            identity = (node.type_name, node.key)
            if identity in keys:
                raise ValueError("retirement_graph_duplicate_node")
            keys.add(identity)
            before.add_node(node)
            if select(node):
                removed.add(identity)
            else:
                survivor = transform(node) if transform is not None else node
                index.validate_node(survivor)
                if (survivor.type_name, survivor.key) != identity:
                    raise ValueError("retirement_graph_survivor_identity_changed")
                after.add_node(survivor)
    for batch in snapshot.iter_relations(batch_size=500):
        if len(batch) > 500:
            raise ValueError("retirement_graph_batch_limit")
        for relation in batch:
            charge(relation)
            index.validate_relation(relation)
            source = (relation.source_type, relation.source_key)
            target = (relation.target_type, relation.target_key)
            if source not in keys or target not in keys:
                raise ValueError("retirement_graph_missing_endpoint")
            before.add_relation(relation)
            if source in removed or target in removed:
                if require_incident is not None:
                    require_incident(relation)
                removed_relations += 1
            else:
                after.add_relation(relation)
    if before.counts() != declared:
        raise ValueError("retirement_graph_census_mismatch")
    return before.digest(), after.digest(), tuple(sorted(removed)), removed_relations


def plan_sprint_graph_retirement(snapshot, *, board_id: str, archived_origin_ids: frozenset[str]) -> GraphRetirementPlan:
    """Select only the deterministic root/outcome owned by archived origins.

    A cognitive/unknown producer, derived source ref or unexpected node type
    needs investigation. Neither content text nor a session name is a selector.
    The caller proves the archive's Board and owns the fixed snapshot lifetime.
    """
    if (type(archived_origin_ids) is not frozenset or len(archived_origin_ids) > _MAX_RECORDS
            or any(type(value) is not str or not 1 <= len(value) <= 128 or ":" in value
                or value.strip() != value for value in archived_origin_ids)):
        raise ValueError("retirement_graph_origins_invalid")

    def select(node):
        ref = node.properties.get("source_artifact_ref", LOGICAL_NULL)
        if type(ref) is not str or not ref.strip().lower().startswith("sprint:"):
            return False
        writer = node.properties.get("created_by_agent", "")
        if (not ref.startswith("sprint:") or ref[7:] not in archived_origin_ids or node.type_name not in {"Entity", "Criterion"}
                or type(writer) is not str or classify_writer_path(writer) != WriterClass.DETERMINISTIC):
            raise ValueError("retirement_graph_source_requires_disposition")
        return True

    def require_incident(relation):
        rule = relation.properties.get("rule_id", "")
        if (relation.layout_name != "belongs_to" or relation.properties.get("layer") != "deterministic"
                or type(rule) is not str or rule.partition("@")[0] not in {
                    "belongs_to/sprint_outcome", "belongs_to/sprint_to_spec",
                    "belongs_to/card_to_sprint", "belongs_to/sprint_to_board"}):
            raise ValueError("retirement_graph_relation_requires_disposition")

    before, after, keys, relations = _measure(snapshot, select, require_incident)
    return GraphRetirementPlan(board_id, before, after, keys, relations)


def graph_retirement_fingerprint(snapshot, *, scope="board") -> str:
    return _measure(snapshot, lambda node: False, scope=scope)[0]


def verify_graph_retirement_selection(snapshot, plan: GraphRetirementPlan) -> None:
    if not isinstance(plan, GraphRetirementPlan):
        raise ValueError("retirement_graph_plan_invalid")
    keys = frozenset(plan.node_keys)
    measured = _measure(snapshot, lambda node: (node.type_name, node.key) in keys)
    if measured != (plan.before_sha256, plan.after_sha256, plan.node_keys, plan.removed_relations):
        raise ValueError("retirement_graph_selection_mismatch")
