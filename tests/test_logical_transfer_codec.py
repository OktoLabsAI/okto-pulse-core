"""The ``okto-pulse-logical-graph/1`` codec: golden bytes, distinctions, refusals."""

from __future__ import annotations

import json
import math

import pytest

from logical_transfer_testing import (
    SAMPLE_MICROS,
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
    "2a68eba0f42d43a0f6e25a18f9f7e4bc1c4b28d531534f3cb1ac1f954f6c15dd"
)
GOLDEN_FINGERPRINT = (
    "7ff81024c2891a235839ecfbff2d21ec5a7601d59d415cd5523d32cf8652837a"
)
GOLDEN_STREAM_CHECKSUM = (
    "ac72548120ddbe1f2f152877b92246ea66838795adc5a94fe63338672311b9f3"
)
GOLDEN_NODE_LINE = (
    '{"key":"c1","properties":{"card_embedding":["vector",{"components":'
    '["0x1.0000000000000p+0","0x1.4000000000000p+1","-0x1.0000000000000p-2"],'
    '"dtype":"float32","space_name":"card_embedding"}],"created_at":'
    '["timestamp_us","1717171717000000"],"done":["bool",true],"id":'
    '["string","c1"],"rank":["int64","7"],"score":'
    '["float64","0x1.999999999999ap-4"],"title":["string",""]},'
    '"record":"node","type":"Card"}'
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
    def test_the_wire_bytes_of_a_record_are_frozen(self) -> None:
        assert encoded()[1] == GOLDEN_NODE_LINE

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
        artifact = decode_artifact(encoded())
        absent_side = artifact.nodes[1].properties
        assert absent_side["title"] is LOGICAL_NULL
        assert "rank" not in absent_side

    def test_the_empty_string_is_not_null(self) -> None:
        artifact = decode_artifact(encoded())
        assert artifact.nodes[0].properties["title"] == ""
        assert artifact.nodes[0].properties["title"] is not LOGICAL_NULL

    def test_a_timestamp_keeps_its_microseconds(self) -> None:
        artifact = decode_artifact(encoded())
        stamp = artifact.nodes[0].properties["created_at"]
        assert stamp == LogicalTimestamp(SAMPLE_MICROS)

    def test_a_vector_travels_by_logical_space_name(self) -> None:
        artifact = decode_artifact(encoded())
        vector = artifact.nodes[0].properties["card_embedding"]
        assert isinstance(vector, LogicalVector)
        assert vector.space_name == "card_embedding"
        assert vector.dtype == "float32"
        assert vector.components == (1.0, 2.5, -0.25)

    def test_identical_parallel_relations_both_survive(self) -> None:
        artifact = decode_artifact(encoded())
        parallels = [
            relation
            for relation in artifact.relations
            if relation.source_key == "c1" and relation.target_key == "c2"
        ]
        assert len(parallels) == 2
        assert parallels[0] == parallels[1]

    def test_direction_and_self_loops_are_preserved(self) -> None:
        artifact = decode_artifact(encoded())
        loops = [
            relation
            for relation in artifact.relations
            if relation.source_key == relation.target_key
        ]
        assert len(loops) == 1
        assert loops[0].source_key == "c1"
        directed = artifact.relations[0]
        assert (directed.source_key, directed.target_key) == ("c1", "c2")


class TestVersionAndFeatures:
    def test_an_unknown_format_identifier_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["format"] = "okto-pulse-logical-graph/2"
        lines[0] = json.dumps(header)
        with pytest.raises(UnsupportedFormatVersionError):
            decode_artifact(lines)

    def test_an_unknown_required_feature_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["features"]["required"] = ["vectors", "time_travel"]
        lines[0] = json.dumps(header)
        with pytest.raises(UnsupportedFeatureError) as caught:
            decode_artifact(lines)
        assert "time_travel" in str(caught.value)

    def test_an_unknown_optional_feature_is_ignored(self) -> None:
        # Declared through the encoder rather than patched into the header: the
        # checksum covers the header, so a hand-edited one would be refused as
        # corruption before the feature rule was ever reached.
        lines = list(
            encode_artifact(
                sample_schema(),
                sample_nodes(),
                sample_relations(),
                counts=sample_counts(),
                optional_features=("annotations",),
            )
        )
        artifact = decode_artifact(lines)
        assert "annotations" in artifact.header.optional_features


class TestStructuralRefusals:
    def test_a_missing_manifest_is_truncation(self) -> None:
        with pytest.raises(ArtifactTruncatedError):
            decode_artifact(encoded()[:-1])

    def test_data_after_the_manifest_is_refused(self) -> None:
        lines = [*encoded(), encoded()[3]]
        with pytest.raises(ArtifactTrailingDataError):
            decode_artifact(lines)

    def test_a_node_after_the_relation_section_is_out_of_sequence(self) -> None:
        lines = encoded()
        reordered = [lines[0], lines[1], lines[3], lines[2], lines[4], lines[5], lines[6]]
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
        lines.insert(2, json.dumps({"record": "footnote"}))
        with pytest.raises(ArtifactMalformedError):
            decode_artifact(lines)


class TestIntegrityRefusals:
    def test_a_manifest_count_that_disagrees_is_refused(self) -> None:
        lines = encoded()
        manifest = json.loads(lines[-1])
        manifest["counts"]["nodes"] = 99
        lines[-1] = json.dumps(manifest)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_header_count_that_disagrees_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["counts"]["relations"] = 99
        lines[0] = json.dumps(header)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_dropped_record_is_refused(self) -> None:
        lines = encoded()
        del lines[4]
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_tampered_value_breaks_the_checksum(self) -> None:
        lines = encoded()
        payload = json.loads(lines[2])
        payload["properties"]["id"] = ["string", "tampered"]
        lines[2] = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_tampered_manifest_fingerprint_is_refused(self) -> None:
        lines = encoded()
        manifest = json.loads(lines[-1])
        manifest["fingerprint"] = "0" * 64
        lines[-1] = json.dumps(manifest)
        with pytest.raises(ArtifactIntegrityError):
            decode_artifact(lines)

    def test_a_tampered_schema_digest_is_refused(self) -> None:
        lines = encoded()
        header = json.loads(lines[0])
        header["schema_digest"] = "0" * 64
        lines[0] = json.dumps(header)
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
            LogicalVector("space", "float32", (0.0, -1.5)),
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


class TestStreaming:
    def test_decoding_yields_records_before_the_stream_ends(self) -> None:
        stream = decode_records(encoded())
        first = next(stream)
        assert first.kind == "header"
        second = next(stream)
        assert second.kind == "node"
        assert second.node is not None
        assert second.node.key == "c1"

    def test_a_reader_that_stops_early_verifies_nothing(self) -> None:
        # Truncation is only observable at the end, so an abandoned read must
        # not be mistaken for a validated one.
        stream = decode_records(encoded()[:-1])
        assert next(stream).kind == "header"
        with pytest.raises(ArtifactTruncatedError):
            list(stream)
