from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from okto_pulse.core.kg.query_contract import (
    COGNITIVE_OUTCOME_TYPES,
    GRAPH_LAYER_VALUES,
    KGEdgeType,
    KGNodeType,
    RELATED_CONTEXT_DEPTHS,
    RELATED_CONTEXT_DIRECTIONS,
    edge_type_values,
    query_contract_document,
    validate_related_context_artifact_ref,
)
from okto_pulse.core.kg.rebuild_audit import CognitivePendingOutcomeType
from okto_pulse.core.kg.schemas import (
    KGEdgeType as PrimitiveEdgeType,
    KGNodeType as PrimitiveNodeType,
)
from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    get_schema_info,
    validate_cypher_read_only,
)
from okto_pulse.core.kg.tool_schemas import (
    AlternativeResult,
    ContextHop,
    SimilarDecisionResult,
)


def test_runtime_models_share_the_canonical_node_edge_and_outcome_enums():
    contract = query_contract_document()

    assert PrimitiveNodeType is KGNodeType
    assert PrimitiveEdgeType is KGEdgeType
    assert CognitivePendingOutcomeType.__members__ == {
        member.name: member for member in CognitivePendingOutcomeType
    }
    assert [member.value for member in CognitivePendingOutcomeType] == list(
        COGNITIVE_OUTCOME_TYPES
    )
    assert [member.value for member in KGNodeType] == contract["node_types"]
    assert [member.value for member in KGEdgeType] == list(edge_type_values())
    assert "Alternative" in contract["node_types"]


def test_query_contract_covers_layers_related_context_and_edge_endpoints():
    contract = query_contract_document()

    assert contract["version"] == "1.0"
    assert contract["graph_layers"] == list(GRAPH_LAYER_VALUES)
    assert contract["related_context"] == {
        "directions": list(RELATED_CONTEXT_DIRECTIONS),
        "max_depths": list(RELATED_CONTEXT_DEPTHS),
    }
    assert contract["similarity"] == {"minimum": 0.0, "maximum": 1.0}
    assert {"from": "Decision", "to": "Alternative"} in contract[
        "edge_endpoints"
    ]["relates_to"]


def test_schema_info_exposes_the_same_machine_readable_contract():
    result = get_schema_info("contract-board")

    assert result["query_contract"] == query_contract_document()
    assert set(result["query_contract"]["edge_types"]) == {
        row["name"] for row in result["stable_rel_types"]
    }


def test_related_context_id_is_typed_without_breaking_legacy_refs():
    raw_uuid = "11111111-1111-1111-1111-111111111111"

    assert validate_related_context_artifact_ref(f"spec:{raw_uuid}") is None
    assert validate_related_context_artifact_ref(f"card:{raw_uuid}") is None
    assert validate_related_context_artifact_ref("legacy-reference") is None
    assert "ambiguous" in validate_related_context_artifact_ref(raw_uuid)
    assert "typed reference" in validate_related_context_artifact_ref(
        f"refinement:{raw_uuid}"
    )


def test_wire_contract_bounds_edge_types_and_similarity():
    hop = ContextHop(
        center_id="c",
        center_title="center",
        rel1_type="relates_to",
    )
    assert hop.rel1_type == KGEdgeType.RELATES_TO
    AlternativeResult(id="a", title="discarded")
    SimilarDecisionResult(id="d", title="valid", similarity=1.0)

    with pytest.raises(ValidationError):
        ContextHop(center_id="c", center_title="center", rel1_type="invented")
    with pytest.raises(ValidationError):
        SimilarDecisionResult(id="d", title="invalid", similarity=1.01)


def test_cypher_subset_distinguishes_unsupported_from_unsafe_operations():
    validate_cypher_read_only("MATCH (n) RETURN n")

    with pytest.raises(TierPowerError) as unsupported:
        validate_cypher_read_only("EXPLAIN MATCH (n) RETURN n")
    assert unsupported.value.code == "unsupported_operation"
    assert unsupported.value.details["query_contract_version"] == "1.0"

    with pytest.raises(TierPowerError) as unsafe:
        validate_cypher_read_only("CREATE (n:Decision)")
    assert unsafe.value.code == "unsafe_cypher"


def test_resource_documents_the_same_contract_and_real_reflective_loop():
    text = (
        Path(__file__).parents[1]
        / "src/okto_pulse/core/mcp/resources/reference/tool-docs/kg.md"
    ).read_text(encoding="utf-8")

    assert 'query_contract.version == "1.0"' in text
    assert "retrieve → critic → corrective action" in text
    assert "v1_stub_no_critic_wired" not in text
    for terminal_reason in (
        "malformed_critic_output",
        "no_progress",
        "retrieval_error",
        "critic_error",
    ):
        assert terminal_reason in text
