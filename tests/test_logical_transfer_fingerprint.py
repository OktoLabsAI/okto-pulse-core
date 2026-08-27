"""The logical fingerprint: golden digest, order independence, discrimination."""

from __future__ import annotations

import pytest

from logical_transfer_testing import (
    SAMPLE_MICROS,
    EMBEDDING_STORAGE_DTYPE,
    board_schema,
    embedding_space,
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
    LogicalVectorSpace,
    fingerprint_graph,
    schema_digest,
)


GOLDEN_FINGERPRINT = (
    "5c60feb1131600144121a744e586ddc7a8dac1bf637ef523ef9db820a23b3dfb"
)


def fingerprint(nodes=None, relations=None, schema=None) -> str:
    return fingerprint_graph(
        schema if schema is not None else sample_schema(),
        sample_nodes() if nodes is None else nodes,
        sample_relations() if relations is None else relations,
    )


def replace_property(node: LogicalNode, name: str, value) -> LogicalNode:
    properties = dict(node.properties)
    properties[name] = value
    return LogicalNode(node.type_name, node.key, properties)


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
        # A XOR-combined multiset hash would return the empty digest for the
        # doubled case. That is the exact failure this format cannot tolerate.
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
        nodes[0] = replace_property(nodes[0], "rank", 8)
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
        nodes[1] = LogicalNode("Card", "c9", {"id": "c9", "title": LOGICAL_NULL})
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_reversing_a_relation_direction_is_visible(self) -> None:
        relations = list(sample_relations())
        relations[0] = LogicalRelation(
            "blocks", "Card", "Card", "c2", "c1", {"note": "x"}
        )
        assert fingerprint(relations=tuple(relations)) != GOLDEN_FINGERPRINT

    def test_changing_a_relation_property_is_visible(self) -> None:
        relations = list(sample_relations())
        relations[2] = LogicalRelation(
            "blocks", "Card", "Card", "c1", "c1", {"note": "y"}
        )
        assert fingerprint(relations=tuple(relations)) != GOLDEN_FINGERPRINT

    def test_moving_a_vector_component_by_one_bit_is_visible(self) -> None:
        nodes = list(sample_nodes())
        nodes[0] = replace_property(
            nodes[0],
            "embedding",
            LogicalVector(
                "card_embedding_idx",
                EMBEDDING_STORAGE_DTYPE,
                (1.0000000000000002, 2.5, -0.25),
            ),
        )
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_renaming_a_vector_space_is_visible(self) -> None:
        nodes = list(sample_nodes())
        nodes[0] = replace_property(
            nodes[0],
            "embedding",
            LogicalVector("other_idx", EMBEDDING_STORAGE_DTYPE, (1.0, 2.5, -0.25)),
        )
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT

    def test_moving_a_timestamp_by_one_microsecond_is_visible(self) -> None:
        nodes = list(sample_nodes())
        nodes[0] = replace_property(
            nodes[0], "created_at", LogicalTimestamp(SAMPLE_MICROS + 1)
        )
        assert fingerprint(nodes=tuple(nodes)) != GOLDEN_FINGERPRINT


class TestVectorSemanticsAreInTheDigest:
    """Geometry travels: a space recreated with different search meaning refuses."""

    def swapped(self, **overrides) -> LogicalSchema:
        base = sample_schema().vector_spaces[0]
        fields = {
            "name": base.name,
            "storage_dtype": base.storage_dtype,
            "dimension": base.dimension,
            "metric": base.metric,
            "normalized": base.normalized,
        }
        fields.update(overrides)
        return LogicalSchema(
            scope="board",
            node_types=sample_schema().node_types,
            relation_layouts=sample_schema().relation_layouts,
            vector_spaces=(LogicalVectorSpace(**fields),),
        )

    @pytest.mark.parametrize(
        "overrides",
        [
            {"metric": "l2"},
            {"normalized": True},
            {"storage_dtype": "float32"},
            {"dimension": 4},
        ],
    )
    def test_changing_space_semantics_moves_the_schema_digest(
        self, overrides: dict
    ) -> None:
        altered = self.swapped(**overrides)
        assert schema_digest(altered) != schema_digest(sample_schema())

    @pytest.mark.parametrize(
        "overrides", [{"metric": "l2"}, {"normalized": True}]
    )
    def test_changing_space_semantics_moves_the_fingerprint(
        self, overrides: dict
    ) -> None:
        # Names, counts and dimensions would all still agree; only carrying the
        # geometry makes this transfer refusable.
        assert fingerprint(schema=self.swapped(**overrides)) != GOLDEN_FINGERPRINT


class TestLayoutIdentityIsTheTriple:
    """One name can host several layouts; the digest must tell them apart."""

    def board_relation(self, name: str, node_type: str) -> LogicalRelation:
        return LogicalRelation(name, node_type, node_type, "k1", "k2")

    def test_same_name_different_endpoints_are_different_relations(self) -> None:
        schema = board_schema()
        decision_side = fingerprint_graph(
            schema, (), (self.board_relation("supersedes", "Decision"),)
        )
        alternative_side = fingerprint_graph(
            schema, (), (self.board_relation("supersedes", "Alternative"),)
        )
        assert decision_side != alternative_side

    def test_both_layouts_of_one_name_are_kept_by_the_schema(self) -> None:
        schema = board_schema()
        assert len(schema.relation_layouts) == 2
        assert {layout.identity for layout in schema.relation_layouts} == {
            ("supersedes", "Decision", "Decision"),
            ("supersedes", "Alternative", "Alternative"),
        }

    def test_a_layout_is_looked_up_by_its_whole_identity(self) -> None:
        schema = board_schema()
        found = schema.relation_layout("supersedes", "Alternative", "Alternative")
        assert found.source_type == "Alternative"


class TestSectionSeparation:
    def test_nodes_and_relations_live_in_different_sections(self) -> None:
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
        relation_only = fingerprint_graph(
            schema, (), (LogicalRelation("T", "T", "T", "a", "a"),)
        )
        assert node_only != relation_only

    @pytest.mark.parametrize("scope", ["board", "global_discovery"])
    def test_the_scope_is_part_of_the_schema_digest(self, scope: str) -> None:
        node_types = (
            LogicalNodeType(
                "T", "id", (LogicalPropertyDef("id", "string", nullable=False),)
            ),
        )
        schema = LogicalSchema(scope=scope, node_types=node_types)
        other = LogicalSchema(
            scope="board" if scope == "global_discovery" else "global_discovery",
            node_types=node_types,
        )
        assert schema_digest(schema) != schema_digest(other)

    def test_the_property_to_space_mapping_is_in_the_digest(self) -> None:
        # Two schemas identical except for which space a property points at.
        def build(space: str) -> LogicalSchema:
            return LogicalSchema(
                scope="board",
                node_types=(
                    LogicalNodeType(
                        "T",
                        "id",
                        (
                            LogicalPropertyDef("id", "string", nullable=False),
                            LogicalPropertyDef(
                                "embedding", "vector", vector_space=space
                            ),
                        ),
                    ),
                ),
                vector_spaces=(embedding_space("a"), embedding_space("b")),
            )

        assert schema_digest(build("a")) != schema_digest(build("b"))
