"""The format must represent the two real scopes, not a convenient simplification.

The Board schema is where a name-derived vector space breaks: all eleven node
types carry a property literally called ``embedding``, and each belongs to a
different space.  Global Discovery repeats the collision between ``Entity`` and
``DecisionDigest``.  And Board's relation layouts share names across different
endpoint pairs, so a layout keyed by name alone loses all but one of them.

These fixtures are reduced -- they pin the mappings and the attributes, not all
967 property definitions, whose counts the Community adapter owns.
"""

from __future__ import annotations

import pytest

from logical_transfer_testing import (
    BOARD_SPACE_BY_TYPE,
    GLOBAL_KEY_BY_TYPE,
    EMBEDDING_DIMENSION,
    EMBEDDING_METRIC,
    EMBEDDING_NORMALIZED,
    EMBEDDING_STORAGE_DTYPE,
    GLOBAL_LAYOUTS,
    GLOBAL_SPACE_BY_PROPERTY,
    board_schema,
    global_schema,
)
from okto_pulse.core.kg.logical_transfer import (
    LogicalNodeType,
    LogicalPropertyDef,
    LogicalRelation,
    LogicalSchema,
    LogicalSchemaError,
    decode_schema,
    encode_schema,
    fingerprint_graph,
    schema_digest,
)


class TestBoardScope:
    def test_all_eleven_node_types_are_represented(self) -> None:
        assert len(board_schema().node_types) == 11

    def test_all_eleven_spaces_are_represented(self) -> None:
        assert len(board_schema().vector_spaces) == 11

    def test_every_type_calls_its_property_embedding(self) -> None:
        # The exact collision that a name-derived space cannot survive.
        for node_type in board_schema().node_types:
            assert "embedding" in node_type.property_names()

    def test_each_embedding_points_at_its_own_space(self) -> None:
        mapping = {
            node_type.name: node_type.property_def("embedding").vector_space
            for node_type in board_schema().node_types
        }
        assert mapping == BOARD_SPACE_BY_TYPE
        assert len(set(mapping.values())) == 11

    @pytest.mark.parametrize("space_name", sorted(BOARD_SPACE_BY_TYPE.values()))
    def test_every_space_carries_the_real_geometry(self, space_name: str) -> None:
        space = board_schema().vector_space(space_name)
        assert space.dimension == EMBEDDING_DIMENSION
        assert space.metric == EMBEDDING_METRIC
        assert space.normalized is EMBEDDING_NORMALIZED
        assert space.storage_dtype == EMBEDDING_STORAGE_DTYPE

    def test_one_layout_name_hosts_two_distinct_layouts(self) -> None:
        identities = {layout.identity for layout in board_schema().relation_layouts}
        assert identities == {
            ("supersedes", "Decision", "Decision"),
            ("supersedes", "Alternative", "Alternative"),
        }

    def test_the_two_supersedes_layouts_are_told_apart(self) -> None:
        schema = board_schema()
        decision = schema.relation_layout("supersedes", "Decision", "Decision")
        alternative = schema.relation_layout(
            "supersedes", "Alternative", "Alternative"
        )
        assert decision is not alternative

    def test_occurrences_with_equal_keys_stay_distinct(self) -> None:
        # Same layout name, same key values, different endpoint types. Without
        # the types on the occurrence these two would be indistinguishable.
        schema = board_schema()
        decision_side = LogicalRelation(
            "supersedes", "Decision", "Decision", "k1", "k2"
        )
        alternative_side = LogicalRelation(
            "supersedes", "Alternative", "Alternative", "k1", "k2"
        )
        assert decision_side != alternative_side
        assert fingerprint_graph(schema, (), (decision_side,)) != fingerprint_graph(
            schema, (), (alternative_side,)
        )


class TestGlobalDiscoveryScope:
    def test_the_four_node_types_are_represented(self) -> None:
        names = {node_type.name for node_type in global_schema().node_types}
        assert names == set(GLOBAL_SPACE_BY_PROPERTY)
        assert len(names) == 4

    def test_the_four_spaces_are_represented(self) -> None:
        assert len(global_schema().vector_spaces) == 4

    def test_the_seven_layouts_are_represented(self) -> None:
        identities = {layout.identity for layout in global_schema().relation_layouts}
        assert identities == set(GLOBAL_LAYOUTS)
        assert len(identities) == 7

    def test_entity_and_decision_digest_share_a_property_name(self) -> None:
        schema = global_schema()
        entity = schema.node_type("Entity").property_def("embedding")
        digest = schema.node_type("DecisionDigest").property_def("embedding")
        assert entity.name == digest.name == "embedding"
        assert entity.vector_space == "entity_embedding_idx"
        assert digest.vector_space == "digest_embedding_idx"

    def test_each_type_maps_to_its_declared_space(self) -> None:
        schema = global_schema()
        for node_type, (prop, space) in GLOBAL_SPACE_BY_PROPERTY.items():
            assert schema.node_type(node_type).property_def(prop).vector_space == space

    def test_each_type_is_keyed_the_way_the_real_schema_keys_it(self) -> None:
        schema = global_schema()
        for node_type, key in GLOBAL_KEY_BY_TYPE.items():
            assert schema.node_type(node_type).key == key

    def test_the_board_type_is_keyed_by_board_id(self) -> None:
        # Not "id": keying every Global type alike would hide a key-name bug.
        board = global_schema().node_type("Board")
        assert board.key == "board_id"
        assert "board_id" in board.property_names()
        assert "id" not in board.property_names()

    def test_only_the_self_relations_carry_weight(self) -> None:
        weighted = {
            layout.identity
            for layout in global_schema().relation_layouts
            if "weight" in layout.property_names()
        }
        assert weighted == {
            ("TOPIC_RELATES_TO", "Topic", "Topic"),
            ("ENTITY_RELATES_TO", "Entity", "Entity"),
        }

    def test_self_relations_are_representable(self) -> None:
        schema = global_schema()
        loop = LogicalRelation(
            "TOPIC_RELATES_TO", "Topic", "Topic", "t1", "t1", {"weight": 0.5}
        )
        assert loop.source_key == loop.target_key
        assert schema.relation_layout(*loop.layout_identity) is not None


class TestBothScopesRoundTrip:
    @pytest.mark.parametrize(
        "builder", [board_schema, global_schema], ids=["board", "global"]
    )
    def test_a_schema_survives_the_wire_exactly(self, builder) -> None:
        schema = builder()
        assert decode_schema(encode_schema(schema)) == schema

    @pytest.mark.parametrize(
        "builder", [board_schema, global_schema], ids=["board", "global"]
    )
    def test_a_schema_digest_is_stable_across_a_round_trip(self, builder) -> None:
        schema = builder()
        assert schema_digest(decode_schema(encode_schema(schema))) == schema_digest(
            schema
        )

    def test_the_two_scopes_do_not_share_a_digest(self) -> None:
        assert schema_digest(board_schema()) != schema_digest(global_schema())


class TestTheSchemaStillRefusesRealMistakes:
    def test_a_vector_property_naming_no_space_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError):
            LogicalPropertyDef("embedding", "vector")

    def test_a_vector_property_naming_an_undeclared_space_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            LogicalSchema(
                scope="board",
                node_types=(
                    LogicalNodeType(
                        "T",
                        "id",
                        (
                            LogicalPropertyDef("id", "string", nullable=False),
                            LogicalPropertyDef(
                                "embedding", "vector", vector_space="missing_idx"
                            ),
                        ),
                    ),
                ),
            )
        assert "undeclared vector space" in str(caught.value)

    def test_a_non_vector_property_may_not_name_a_space(self) -> None:
        with pytest.raises(LogicalSchemaError):
            LogicalPropertyDef("title", "string", vector_space="somewhere_idx")

    def test_a_truly_duplicated_layout_is_still_refused(self) -> None:
        # Same name AND same endpoints twice is a real duplicate, unlike two
        # layouts that merely share a name.
        from okto_pulse.core.kg.logical_transfer import LogicalRelationLayout

        with pytest.raises(LogicalSchemaError) as caught:
            LogicalSchema(
                scope="board",
                node_types=(
                    LogicalNodeType(
                        "T", "id", (LogicalPropertyDef("id", "string", nullable=False),)
                    ),
                ),
                relation_layouts=(
                    LogicalRelationLayout("rel", "T", "T"),
                    LogicalRelationLayout("rel", "T", "T"),
                ),
            )
        assert "duplicate" in str(caught.value)
