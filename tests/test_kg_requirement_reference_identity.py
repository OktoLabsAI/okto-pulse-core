"""Persisted requirement references use IDs, never text or positions."""
import pytest

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


@pytest.mark.parametrize("family", ["api_contracts", "business_rules",
    "integration_requirements", "observability_requirements"])
@pytest.mark.parametrize("reference", ["Same requirement", "0", 0, True])
def test_requirement_projection_does_not_resolve_text_or_position(family, reference):
    source = {
        "id": "spec-native-refs", "board_id": "board-native-refs", "status": "done",
        "functional_requirements": [{"id": "fr_native", "text": "Same requirement"}],
        family: [{"id": "source-native", "title": "Declared relationship",
                  "linked_requirements": [reference]}],
    }
    result = DeterministicWorker().process_spec(source)
    assert not [edge for edge in result.edges
                if edge.edge_type in {"implements", "derives_from"}]


@pytest.mark.parametrize("family", ["api_contracts", "business_rules",
    "integration_requirements", "observability_requirements"])
def test_requirement_projection_keeps_exact_native_id(family):
    source = {
        "id": "spec-native-refs", "board_id": "board-native-refs", "status": "done",
        "functional_requirements": [{"id": "fr_native", "text": "Same requirement"}],
        family: [{"id": "source-native", "title": "Declared relationship",
                  "linked_requirements": ["fr_native"]}],
    }
    result = DeterministicWorker().process_spec(source)
    refs = {node.candidate_id: node.source_artifact_ref for node in result.nodes}
    edges = [edge for edge in result.edges
             if edge.edge_type in {"implements", "derives_from"}]
    assert len(edges) == 1
    assert refs[edges[0].to_candidate_id] == "spec:spec-native-refs:fr:fr_native"
