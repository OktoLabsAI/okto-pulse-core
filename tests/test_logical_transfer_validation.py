"""Records are checked against the schema they claim to belong to.

Every case here would otherwise produce an artifact that is internally
consistent -- matching counts, matching checksum, matching fingerprint -- and
semantically invalid, because all three describe the bytes produced rather than
whether they mean anything under the declared schema.
"""

from __future__ import annotations

import pytest

from logical_transfer_testing import (
    SAMPLE_MICROS,
    EMBEDDING_STORAGE_DTYPE,
    board_schema,
    sample_counts,
    sample_nodes,
    sample_relations,
    sample_schema,
)
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL,
    LogicalNode,
    LogicalRelation,
    LogicalSchemaError,
    LogicalSchemaIndex,
    LogicalTimestamp,
    LogicalVector,
    canonical_json,
    decode_artifact,
    encode_artifact,
)
from okto_pulse.core.kg.logical_transfer.canonical import encode_node


def index():
    return LogicalSchemaIndex.build(sample_schema())


def node(**properties) -> LogicalNode:
    base = {"id": "c1"}
    base.update(properties)
    return LogicalNode("Card", "c1", base)


class TestUnknownStructure:
    def test_an_undeclared_node_type_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(LogicalNode("Ghost", "g1", {"id": "g1"}))
        assert "type" in str(caught.value)

    def test_an_undeclared_relation_layout_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError):
            index().validate_relation(
                LogicalRelation("haunts", "Card", "Card", "c1", "c2")
            )

    def test_a_known_name_between_wrong_endpoints_is_refused(self) -> None:
        # The name exists; this triple does not. Looking layouts up by name
        # alone would have accepted it.
        with pytest.raises(LogicalSchemaError):
            index().validate_relation(
                LogicalRelation("blocks", "Card", "Ghost", "c1", "c2")
            )

    def test_an_undeclared_node_property_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(node(nickname="x"))
        assert "nickname" in str(caught.value)

    def test_an_undeclared_relation_property_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError):
            index().validate_relation(
                LogicalRelation(
                    "blocks", "Card", "Card", "c1", "c2", {"weight": 1.0}
                )
            )


class TestKeyIntegrity:
    def test_a_node_whose_key_property_is_missing_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(LogicalNode("Card", "c1", {"title": "t"}))
        assert "key" in str(caught.value)

    def test_a_node_whose_key_property_disagrees_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(LogicalNode("Card", "c1", {"id": "c2"}))
        assert "key" in str(caught.value)


class TestValueTypes:
    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("title", 1),
            ("title", True),
            ("rank", "7"),
            ("rank", 1.0),
            ("rank", True),
            ("score", 1),
            ("score", "0.1"),
            ("done", 1),
            ("done", "true"),
            ("created_at", 1717171717000000),
            ("created_at", "2026-01-01"),
            ("embedding", (1.0, 2.0, 3.0)),
        ],
    )
    def test_a_value_of_the_wrong_type_is_refused(
        self, name: str, value: object
    ) -> None:
        with pytest.raises(LogicalSchemaError):
            index().validate_node(node(**{name: value}))

    def test_a_bool_is_not_an_int64(self) -> None:
        # bool is a subclass of int, so an int64 column that accepted True
        # would round-trip it as 1.
        with pytest.raises(LogicalSchemaError):
            index().validate_node(node(rank=True))

    def test_an_int_out_of_int64_range_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError):
            index().validate_node(node(rank=2**63))

    def test_declared_types_are_accepted(self) -> None:
        index().validate_node(
            node(
                title="t",
                rank=7,
                score=0.5,
                done=False,
                created_at=LogicalTimestamp(SAMPLE_MICROS),
                embedding=LogicalVector(
                    "card_embedding_idx", EMBEDDING_STORAGE_DTYPE, (1.0, 2.0, 3.0)
                ),
            )
        )


class TestNullability:
    def test_a_null_in_a_non_nullable_property_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(LogicalNode("Card", "c1", {"id": LOGICAL_NULL}))
        assert "non-nullable" in str(caught.value)

    def test_a_null_in_a_nullable_property_is_accepted(self) -> None:
        index().validate_node(node(title=LOGICAL_NULL))


class TestVectorAgreement:
    def vector(self, space: str, dtype: str, width: int) -> LogicalVector:
        return LogicalVector(space, dtype, tuple(float(i) for i in range(width)))

    def test_a_vector_in_the_wrong_space_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(
                node(embedding=self.vector("other_idx", EMBEDDING_STORAGE_DTYPE, 3))
            )
        assert "space" in str(caught.value)

    def test_a_vector_with_the_wrong_dtype_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(
                node(embedding=self.vector("card_embedding_idx", "float32", 3))
            )
        assert "dtype" in str(caught.value)

    def test_a_vector_of_the_wrong_width_is_refused(self) -> None:
        with pytest.raises(LogicalSchemaError) as caught:
            index().validate_node(
                node(
                    embedding=self.vector(
                        "card_embedding_idx", EMBEDDING_STORAGE_DTYPE, 4
                    )
                )
            )
        assert "width" in str(caught.value) or "dimension" in str(caught.value)


class TestBothCodecDirectionsValidate:
    def test_the_encoder_refuses_to_write_an_invalid_record(self) -> None:
        bad = LogicalNode("Ghost", "g1", {"id": "g1"})
        with pytest.raises(LogicalSchemaError):
            list(
                encode_artifact(
                    sample_schema(),
                    (*sample_nodes(), bad),
                    sample_relations(),
                    counts=sample_counts(),
                )
            )

    def test_the_decoder_refuses_to_read_an_invalid_record(self) -> None:
        # Hand-built, because a conforming encoder cannot produce this. The
        # record is perfectly canonical; only the schema says it is wrong.
        lines = list(
            encode_artifact(
                sample_schema(),
                sample_nodes(),
                sample_relations(),
                counts=sample_counts(),
            )
        )
        ghost = LogicalNode("Ghost", "g1", {"id": "g1"})
        lines.insert(3, canonical_json({"record": "node", **encode_node(ghost)}))
        with pytest.raises(LogicalSchemaError) as caught:
            decode_artifact(lines)
        assert "Ghost" in str(caught.value)


class TestValidationIsBounded:
    def test_the_index_is_built_once_and_answers_by_lookup(self) -> None:
        schema = board_schema()
        built = LogicalSchemaIndex.build(schema)
        assert len(built.node_types) == 11
        assert len(built.layouts) == 2
        assert len(built.spaces) == 11
        # Every lookup key is the layout's full identity, not its name.
        assert ("supersedes", "Decision", "Decision") in built.layouts
        assert ("supersedes", "Alternative", "Alternative") in built.layouts
