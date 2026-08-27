"""The ``okto-pulse-logical-graph/1`` codec: golden bytes, distinctions, refusals."""

from __future__ import annotations

import json
import math

import pytest

from logical_transfer_testing import (
    SAMPLE_MICROS,
    EMBEDDING_STORAGE_DTYPE,
    sample_counts,
    sample_nodes,
    sample_relations,
    sample_schema,
)
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_GRAPH_FORMAT,
    LOGICAL_NULL,
    ArtifactIntegrityError,
    ArtifactMalformedError,
    ArtifactSequenceError,
    ArtifactTrailingDataError,
    ArtifactTruncatedError,
    LogicalTimestamp,
    LogicalValueError,
    LogicalVector,
    UnsupportedFeatureError,
    UnsupportedFormatVersionError,
    decode_artifact,
    decode_records,
    decode_value,
    encode_artifact,
    encode_value,
    schema_digest,
)


GOLDEN_SCHEMA_DIGEST = (
    "c288bf0306d4f67f5517a051c7f322ed29293426d5323a006ba95b888acd3945"
)
GOLDEN_FINGERPRINT = (
    "5c60feb1131600144121a744e586ddc7a8dac1bf637ef523ef9db820a23b3dfb"
)
GOLDEN_STREAM_CHECKSUM = (
    "434242f5b71634aae5d7aa89fa6ae660ffa1eb37ad706fe4448ec149b7d2861f"
)
GOLDEN_NODE_LINE = (
    '{"key":"c1","properties":{"created_at":["timestamp_us","1717171717000000"],'
    '"done":["bool",true],"embedding":["vector",{"components":'
    '["0x1.0000000000000p+0","0x1.4000000000000p+1","-0x1.0000000000000p-2"],'
    '"dtype":"float64","space_name":"card_embedding_idx"}],"id":["string","c1"],'
    '"rank":["int64","7"],"score":["float64","0x1.999999999999ap-4"],'
    '"title":["string",""]},"record":"node","type":"Card"}'
)
GOLDEN_RELATION_LINE = (
    '{"layout":"blocks","properties":{"note":["string","x"]},"record":"relation",'
    '"source":"c1","source_type":"Card","target":"c2","target_type":"Card"}'
)


def encoded() -> list[str]:
    return list(
        encode_artifact(
            sample_schema(),
            sample_nodes(),
            sample_relations(),
            counts=sample_counts(),
        )
    )


class TestGolden:
    def test_the_wire_bytes_of_a_node_are_frozen(self) -> None:
        assert encoded()[1] == GOLDEN_NODE_LINE

    def test_the_wire_bytes_of_a_relation_are_frozen(self) -> None:
        assert encoded()[3] == GOLDEN_RELATION_LINE

    def test_the_schema_digest_is_frozen(self) -> None:
        assert schema_digest(sample_schema()) == GOLDEN_SCHEMA_DIGEST

    def test_the_manifest_carries_the_frozen_checksums(self) -> None:
        manifest = json.loads(encoded()[-1])
        assert manifest["record"] == "manifest"
        assert manifest["fingerprint"] == GOLDEN_FINGERPRINT
        assert manifest["stream_checksum"] == GOLDEN_STREAM_CHECKSUM

    def test_encoding_is_deterministic(self) -> None:
        assert encoded() == encoded()

    def test_the_header_names_the_exact_format(self) -> None:
        header = json.loads(encoded()[0])
        assert header["format"] == LOGICAL_GRAPH_FORMAT
        assert header["features"]["required"] == ["vectors"]


class TestRoundTrip:
    def test_a_graph_survives_a_round_trip_exactly(self) -> None:
        artifact = decode_artifact(encoded())
        assert artifact.nodes == sample_nodes()
        assert artifact.relations == sample_relations()
        assert artifact.header.schema == sample_schema()
        assert artifact.manifest is not None
        assert artifact.manifest.counts == sample_counts()

    def test_absent_and_null_stay_different(self) -> None:
        properties = decode_artifact(encoded()).nodes[1].properties
        assert properties["title"] is LOGICAL_NULL
        assert "rank" not in properties

    def test_the_empty_string_is_not_null(self) -> None:
        properties = decode_artifact(encoded()).nodes[0].properties
        assert properties["title"] == ""
        assert properties["title"] is not LOGICAL_NULL

    def test_a_timestamp_keeps_its_microseconds(self) -> None:
        stamp = decode_artifact(encoded()).nodes[0].properties["created_at"]
        assert stamp == LogicalTimestamp(SAMPLE_MICROS)

    def test_a_vector_travels_by_logical_space_name(self) -> None:
        vector = decode_artifact(encoded()).nodes[0].properties["embedding"]
        assert isinstance(vector, LogicalVector)
        assert vector.space_name == "card_embedding_idx"
        assert vector.dtype == EMBEDDING_STORAGE_DTYPE
        assert vector.components == (1.0, 2.5, -0.25)

    def test_identical_parallel_relations_both_survive(self) -> None:
        relations = decode_artifact(encoded()).relations
        parallels = [
            relation
            for relation in relations
            if relation.source_key == "c1" and relation.target_key == "c2"
        ]
        assert len(parallels) == 2
        assert parallels[0] == parallels[1]

    def test_direction_and_self_loops_are_preserved(self) -> None:
        relations = decode_artifact(encoded()).relations
        loops = [r for r in relations if r.source_key == r.target_key]
        assert len(loops) == 1
        assert loops[0].source_key == "c1"
        assert (relations[0].source_key, relations[0].target_key) == ("c1", "c2")

    def test_a_relation_carries_its_endpoint_types(self) -> None:
        relation = decode_artifact(encoded()).relations[0]
        assert relation.source_type == "Card"
        assert relation.target_type == "Card"
        assert relation.layout_identity == ("blocks", "Card", "Card")


class TestVersionAndFeatures:
    def test_an_unknown_format_identifier_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["format"] = "okto-pulse-logical-graph/2"
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(UnsupportedFormatVersionError):
            decode_artifact(lines)

    def test_an_unknown_required_feature_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["features"]["required"] = ["vectors", "time_travel"]
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(UnsupportedFeatureError) as caught:
            decode_artifact(lines)
        assert "time_travel" in str(caught.value)

    def test_an_unknown_optional_feature_is_ignored(self) -> None:
        lines = list(
            encode_artifact(
                sample_schema(),
                sample_nodes(),
                sample_relations(),
                counts=sample_counts(),
                optional_features=("annotations",),
            )
        )
        assert "annotations" in decode_artifact(lines).header.optional_features

    def test_a_vector_schema_that_omits_its_feature_is_refused(self) -> None:
        # Otherwise a reader with no vector support would accept the artifact
        # and silently import embeddings it cannot represent.
        lines = encoded()
        header = json.loads(lines[0])
        header["features"]["required"] = []
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError) as caught:
            decode_artifact(lines)
        assert "vectors" in str(caught.value)

    def test_a_repeated_feature_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["features"]["required"] = ["vectors", "vectors"]
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)


class TestStrictShapes:
    """The wire shape is frozen: unknown, missing and repeated fields refuse."""

    def test_an_unknown_header_field_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["extra"] = 1
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_a_missing_header_field_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        del header["schema_digest"]
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_an_unknown_features_field_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["features"]["maybe"] = []
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_an_unknown_manifest_field_is_refused(self) -> None:
        lines = encoded()
        manifest = json.loads(lines[-1])
        manifest["extra"] = "x"
        lines[-1] = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_an_unknown_relation_field_is_refused(self) -> None:
        lines = encoded()
        payload = json.loads(lines[3])
        payload["weight"] = 1
        lines[3] = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_a_duplicate_key_is_refused(self) -> None:
        # json keeps the last one silently, so a record could carry two values
        # for one property and decode to whichever was written second.
        lines = encoded()
        lines[2] = lines[2].replace('{"id"', '{"id":["string","ghost"],"id"', 1)
        with pytest.raises(ArtifactMalformedError) as caught:
            decode_artifact(lines)
        assert "duplicate" in str(caught.value)


class TestCanonicalBytes:
    """A record must be in THE canonical form, not merely parse to the same thing."""

    def test_reordered_keys_are_refused(self) -> None:
        lines = encoded()
        payload = json.loads(lines[3])
        reordered = dict(reversed(list(payload.items())))
        lines[3] = json.dumps(reordered, separators=(",", ":"), sort_keys=False)
        assert json.loads(lines[3]) == payload
        with pytest.raises(ArtifactMalformedError) as caught:
            decode_artifact(lines)
        assert "canonical" in str(caught.value)

    def test_added_whitespace_is_refused(self) -> None:
        lines = encoded()
        lines[3] = json.dumps(json.loads(lines[3]), sort_keys=True)  # spaced
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_a_padded_line_is_refused(self) -> None:
        # Stripping first would accept bytes the checksum never covered.
        lines = encoded()
        lines[3] = "  " + lines[3] + "  "
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_an_empty_line_is_refused(self) -> None:
        lines = encoded()
        lines.insert(3, "")
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_a_blank_line_after_the_manifest_is_trailing_data(self) -> None:
        with pytest.raises(ArtifactTrailingDataError):
            decode_artifact([*encoded(), ""])

    def test_a_non_canonical_float_spelling_is_refused(self) -> None:
        lines = encoded()
        payload = json.loads(lines[1])
        payload["properties"]["score"] = ["float64", "0x3.333333333334p-5"]
        lines[1] = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactMalformedError) as caught:
            decode_artifact(lines)
        assert "canonical" in str(caught.value)


class TestStructuralRefusals:
    def test_a_missing_manifest_is_truncation(self) -> None:
        with pytest.raises(ArtifactTruncatedError):
            decode_artifact(encoded()[:-1])

    def test_data_after_the_manifest_is_refused(self) -> None:
        with pytest.raises(ArtifactTrailingDataError):
            decode_artifact([*encoded(), encoded()[3]])

    def test_a_node_after_the_relation_section_is_out_of_sequence(self) -> None:
        lines = encoded()
        reordered = [
            lines[0],
            lines[1],
            lines[3],
            lines[2],
            lines[4],
            lines[5],
            lines[6],
        ]
        with pytest.raises(ArtifactSequenceError):
            decode_artifact(reordered)

    def test_a_record_before_the_header_is_out_of_sequence(self) -> None:
        lines = encoded()
        with pytest.raises(ArtifactSequenceError):
            decode_artifact([lines[1], *lines])

    def test_a_second_header_is_out_of_sequence(self) -> None:
        lines = encoded()
        with pytest.raises(ArtifactSequenceError):
            decode_artifact([lines[0], lines[0], *lines[1:]])

    def test_an_empty_stream_is_truncation(self) -> None:
        with pytest.raises(ArtifactTruncatedError):
            decode_artifact([])

    def test_a_non_json_line_is_malformed(self) -> None:
        lines = encoded()
        lines[2] = "{not json"
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)

    def test_an_unknown_record_kind_is_malformed(self) -> None:
        lines = encoded()
        lines.insert(2, json.dumps({"record": "footnote"}, separators=(",", ":")))
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)


class TestIntegrityRefusals:
    def test_a_manifest_count_that_disagrees_is_refused(self) -> None:
        lines = encoded()
        manifest = json.loads(lines[-1])
        manifest["counts"]["nodes"] = 99
        lines[-1] = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_header_count_that_disagrees_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["counts"]["relations"] = 99
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_dropped_record_is_refused(self) -> None:
        lines = encoded()
        del lines[4]
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_tampered_manifest_fingerprint_is_refused(self) -> None:
        lines = encoded()
        manifest = json.loads(lines[-1])
        manifest["fingerprint"] = "0" * 64
        lines[-1] = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_tampered_schema_digest_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["schema_digest"] = "0" * 64
        lines[0] = json.dumps(header, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)


class TestValueCodec:
    @pytest.mark.parametrize(
        "value",
        [
            LOGICAL_NULL,
            True,
            False,
            0,
            -1,
            2**63 - 1,
            -(2**63),
            0.0,
            -0.5,
            "",
            "text",
            LogicalTimestamp(0),
            LogicalTimestamp(SAMPLE_MICROS),
            LogicalVector("space", "float64", (0.0, -1.5)),
        ],
    )
    def test_every_supported_value_round_trips_exactly(self, value: object) -> None:
        decoded = decode_value(encode_value(value))
        assert decoded == value
        assert type(decoded) is type(value)

    def test_bool_is_not_encoded_as_an_integer(self) -> None:
        assert encode_value(True)[0] == "bool"
        assert encode_value(1)[0] == "int64"
        assert decode_value(encode_value(True)) is True
        assert decode_value(encode_value(1)) == 1

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
    def test_nan_and_infinity_are_refused(self, value: float) -> None:
        with pytest.raises(LogicalValueError):
            encode_value(value)

    @pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
    def test_int64_overflow_is_refused(self, value: int) -> None:
        with pytest.raises(LogicalValueError):
            encode_value(value)

    def test_python_none_is_refused_in_favour_of_an_explicit_null(self) -> None:
        with pytest.raises(LogicalValueError):
            encode_value(None)

    @pytest.mark.parametrize("value", [b"bytes", [1, 2], {"a": 1}, object()])
    def test_unsupported_types_are_refused(self, value: object) -> None:
        with pytest.raises(LogicalValueError):
            encode_value(value)

    def test_a_float_keeps_every_bit(self) -> None:
        value = 0.1 + 0.2
        assert decode_value(encode_value(value)) == value

    def test_an_unknown_value_tag_is_malformed(self) -> None:
        with pytest.raises(ArtifactMalformedError):
            decode_value(["decimal", "1.5"])

    def test_a_non_canonical_integer_is_malformed(self) -> None:
        with pytest.raises(ArtifactMalformedError):
            decode_value(["int64", "007"])

    def test_a_non_canonical_float_is_malformed(self) -> None:
        assert float.fromhex("0x2.0p+0") == 2.0
        with pytest.raises(ArtifactMalformedError):
            decode_value(["float64", "0x2.0p+0"])

    def test_an_extreme_float_exponent_is_typed_not_leaked(self) -> None:
        # float.fromhex raises OverflowError here, not ValueError; letting it
        # escape would put an untyped builtin on a typed boundary.
        with pytest.raises(ArtifactMalformedError):
            decode_value(["float64", "0x1.0p+99999"])


class TestStreaming:
    def test_decoding_yields_records_before_the_stream_ends(self) -> None:
        stream = decode_records(encoded())
        assert next(stream).kind == "header"
        second = next(stream)
        assert second.kind == "node"
        assert second.node is not None
        assert second.node.key == "c1"

    def test_a_reader_that_stops_early_verifies_nothing(self) -> None:
        stream = decode_records(encoded()[:-1])
        assert next(stream).kind == "header"
        with pytest.raises(ArtifactTruncatedError):
            list(stream)
