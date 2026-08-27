"""The logical fingerprint: golden digest, order independence, discrimination."""

from __future__ import annotations

import pytest

from logical_transfer_testing import (
    SAMPLE_MICROS,
    sample_nodes,
    sample_relations,
    sample_schema,
)
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL,
    LogicalFingerprintAccumulator,
    LogicalNode,
    LogicalNodeType,
    LogicalPropertyDef,
    LogicalRelation,
    LogicalRelationLayout,
    LogicalSchema,
    LogicalTimestamp,
    LogicalVector,
    fingerprint_graph,
    schema_digest,
)


GOLDEN_FINGERPRINT = (
    "7ff81024c2891a235839ecfbff2d21ec5a7601d59d415cd5523d32cf8652837a"
)


def fingerprint(nodes=None, relations=None, schema=None) -> str:
    return fingerprint_graph(
        schema if schema is not None else sample_schema(),
        sample_nodes() if nodes is None else nodes,
        sample_relations() if relations is None else relations,
    )


class TestGoldenDigest:
    def test_the_fingerprint_is_frozen(self) -> None:
        assert fingerprint() == GOLDEN_FINGERPRINT

    def test_the_accumulator_agrees_with_the_whole_graph_helper(self) -> None:
        accumulator = LogicalFingerprintAccumulator.for_schema(sample_schema())
        for node in sample_nodes():
            accumulator.add_node(node)
        for relation in sample_relations():
            accumulator.add_relation(relation)
        assert accumulator.digest() == GOLDEN_FINGERPRINT


class TestOrderIndependence:
    def test_reversing_the_nodes_does_not_change_the_fingerprint(self) -> None:
        assert fingerprint(nodes=tuple(reversed(sample_nodes()))) == GOLDEN_FINGERPRINT

    def test_reversing_the_relations_does_not_change_the_fingerprint(self) -> None:
        reversed_relations = tuple(reversed(sample_relations()))
        assert fingerprint(relations=reversed_relations) == GOLDEN_FINGERPRINT


class TestMultiplicity:
    def test_dropping_one_of_two_identical_parallels_changes_the_digest(self) -> None:
        relations = sample_relations()
        assert relations[0] == relations[1]
        assert fingerprint(relations=relations[1:]) != GOLDEN_FINGERPRINT

    def test_identical_items_do_not_cancel(self) -> None:
        # A XOR-combined multiset hash would return the empty digest here. That
        # is the exact failure this format cannot tolerate, so it is pinned.
        single = (sample_relations()[0],)
        doubled = (sample_relations()[0], sample_relations()[0])
        empty: tuple[LogicalRelation, ...] = ()
        digests = {
            fingerprint(relations=empty),
            fingerprint(relations=single),
            fingerprint(relations=doubled),
        }
        assert len(digests) == 3

    def test_duplicating_a_node_changes_the_digest(self) -> None:
        nodes = sample_nodes()
        assert fingerprint(nodes=(*nodes, nodes[0])) != GOLDEN_FINGERPRINT


class TestMutationDiscrimination:
    """Every mutation below must move the digest; a silent one is data loss."""

    def test_changing_a_property_value_is_visible(self) -> None:
        nodes = list(sample_nodes())
        nodes[0] = LogicalNode(
            "Card", "c1", {**dict(nodes[0].properties), "rank": 8}
        )
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_dropping_a_property_is_visible(self) -> None:
        nodes = list(sample_nodes())
        remaining = dict(nodes[0].properties)
        del remaining["rank"]
        nodes[0] = LogicalNode("Card", "c1", remaining)
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_null_is_not_the_same_as_absent(self) -> None:
        base = LogicalNode("Card", "cx", {"id": "cx"})
        explicit = LogicalNode("Card", "cx", {"id": "cx", "title": LOGICAL_NULL})
        assert fingerprint(nodes=(base,)) != fingerprint(nodes=(explicit,))

    def test_null_is_not_the_same_as_the_empty_string(self) -> None:
        null_side = LogicalNode("Card", "cx", {"id": "cx", "title": LOGICAL_NULL})
        empty_side = LogicalNode("Card", "cx", {"id": "cx", "title": ""})
        assert fingerprint(nodes=(null_side,)) != fingerprint(nodes=(empty_side,))

    def test_changing_a_node_key_is_visible(self) -> None:
        nodes = list(sample_nodes())
        nodes[1] = LogicalNode("Card", "c9", dict(nodes[1].properties))
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_reversing_a_relation_direction_is_visible(self) -> None:
        relations = list(sample_relations())
        relations[0] = LogicalRelation("blocks", "c2", "c1", {"note": "x"})
        assert fingerprint(relations=tuple(relations)) != GOLDEN_FINGERPRINT

    def test_changing_a_relation_property_is_visible(self) -> None:
        relations = list(sample_relations())
        relations[2] = LogicalRelation("blocks", "c1", "c1", {"note": "y"})
        assert fingerprint(relations=tuple(relations)) != GOLDEN_FINGERPRINT

    def test_moving_a_vector_component_by_one_bit_is_visible(self) -> None:
        nodes = list(sample_nodes())
        properties = dict(nodes[0].properties)
        properties["card_embedding"] = LogicalVector(
            "card_embedding", "float32", (1.0000000000000002, 2.5, -0.25)
        )
        nodes[0] = LogicalNode("Card", "c1", properties)
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_renaming_a_vector_space_is_visible(self) -> None:
        nodes = list(sample_nodes())
        properties = dict(nodes[0].properties)
        properties["card_embedding"] = LogicalVector(
            "other_space", "float32", (1.0, 2.5, -0.25)
        )
        nodes[0] = LogicalNode("Card", "c1", properties)
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_moving_a_timestamp_by_one_microsecond_is_visible(self) -> None:
        nodes = list(sample_nodes())
        properties = dict(nodes[0].properties)
        properties["created_at"] = LogicalTimestamp(SAMPLE_MICROS + 1)
        nodes[0] = LogicalNode("Card", "c1", properties)
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_a_schema_change_alone_moves_the_fingerprint(self) -> None:
        altered = LogicalSchema(
            scope="board",
            node_types=(
                LogicalNodeType(
                    name="Card",
                    key="id",
                    properties=(
                        LogicalPropertyDef("id", "string", nullable=False),
                        LogicalPropertyDef("title", "string"),
                        LogicalPropertyDef("rank", "int64"),
                        LogicalPropertyDef("score", "float64"),
                        LogicalPropertyDef("done", "bool"),
                        LogicalPropertyDef("created_at", "timestamp_us"),
                        LogicalPropertyDef("card_embedding", "vector"),
                        LogicalPropertyDef("extra", "string"),
                    ),
                ),
            ),
            relation_layouts=(
                LogicalRelationLayout(
                    name="blocks",
                    source_type="Card",
                    target_type="Card",
                    properties=(LogicalPropertyDef("note", "string"),),
                ),
            ),
            vector_spaces=(sample_schema().vector_spaces[0],),
        )
        assert fingerprint(schema=altered) != GOLDEN_FINGERPRINT
        assert schema_digest(altered) != schema_digest(sample_schema())


class TestSectionSeparation:
    def test_nodes_and_relations_live_in_different_sections(self) -> None:
        # Without per-section domain separation a node and a relation that
        # canonicalize alike could trade places unnoticed.
        schema = LogicalSchema(
            scope="board",
            node_types=(
                LogicalNodeType(
                    "T", "id", (LogicalPropertyDef("id", "string", nullable=False),)
                ),
            ),
            relation_layouts=(LogicalRelationLayout("T", "T", "T", ()),),
        )
        node_only = fingerprint_graph(schema, (LogicalNode("T", "a", {"id": "a"}),), ())
        relation_only = fingerprint_graph(schema, (), (LogicalRelation("T", "a", "a"),))
        assert node_only != relation_only

    @pytest.mark.parametrize("scope", ["board", "global_discovery"])
    def test_the_scope_is_part_of_the_schema_digest(self, scope: str) -> None:
        schema = LogicalSchema(
            scope=scope,
            node_types=(
                LogicalNodeType(
                    "T", "id", (LogicalPropertyDef("id", "string", nullable=False),)
                ),
            ),
        )
        other = LogicalSchema(
            scope="board" if scope == "global_discovery" else "global_discovery",
            node_types=schema.node_types,
        )
        assert schema_digest(schema) != schema_digest(other)
