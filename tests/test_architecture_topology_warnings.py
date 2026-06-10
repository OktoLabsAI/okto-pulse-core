"""Architecture topology warning engine unit tests."""

from __future__ import annotations

import pytest

from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitectureWarningRecord,
    TOPOLOGY_SUGGESTED_FIXES,
    TopologyWarningEngine,
)


def _entities() -> list[dict]:
    return [
        {"id": "entity-web", "name": "Web App", "entity_type": "web_app"},
        {"id": "entity-api", "name": "Pulse API", "entity_type": "api"},
        {"id": "entity-db", "name": "Database", "entity_type": "database"},
    ]


def _diagram(elements: list[dict], *, diagram_id: str = "diagram-runtime", diagram_type: str = "runtime") -> dict:
    return {
        "id": diagram_id,
        "title": "Runtime",
        "diagram_type": diagram_type,
        "format": "excalidraw_json",
        "adapter_payload": {
            "type": "excalidraw",
            "version": 2,
            "elements": elements,
            "appState": {},
            "files": {},
        },
    }


def _node(element_id: str, entity_id: str, text: str | None = None) -> dict:
    return {
        "id": element_id,
        "type": "rectangle",
        "linkedEntityId": entity_id,
        "text": text or entity_id,
    }


def _edge(element_id: str, source: str | None, target: str | None) -> dict:
    edge = {"id": element_id, "type": "arrow"}
    if source is not None:
        edge["sourceElementId"] = source
    if target is not None:
        edge["targetElementId"] = target
    return edge


def _warning_dicts(elements: list[dict], *, entities: list[dict] | None = None) -> list[dict]:
    result = TopologyWarningEngine().evaluate(
        entities=_entities() if entities is None else entities,
        diagrams=[_diagram(elements)],
    )
    return result.to_dict()["warnings"]


def _evaluation_dict(
    elements: list[dict],
    *,
    entities: list[dict] | None = None,
    diagram: dict | None = None,
) -> dict:
    result = TopologyWarningEngine().evaluate(
        entities=_entities() if entities is None else entities,
        diagrams=[diagram if diagram is not None else _diagram(elements)],
    )
    return result.to_dict()


def test_architecture_warning_record_rejects_unknown_code_and_multiple_targets() -> None:
    with pytest.raises(ValueError, match="unknown architecture warning code"):
        ArchitectureWarningRecord(
            code="not_a_warning",
            message="bad",
            path="diagrams[0]",
            suggested_fix="fix",
        )

    with pytest.raises(ValueError, match="at most one primary target"):
        ArchitectureWarningRecord(
            code="isolated_entity_node",
            message="bad",
            path="diagrams[0]",
            suggested_fix="fix",
            element_id="node-a",
            entity_id="entity-a",
        )


def test_payload_schema_exposes_topology_warning_contract() -> None:
    from okto_pulse.core.services.architecture import architecture_design_payload_schema

    schema = architecture_design_payload_schema()
    assert schema["allowed_values"]["topology_warning.code"] == [
        "conceptual_justification_invalid",
        "dangling_connector",
        "disconnected_subgraph",
        "entity_without_diagram",
        "isolated_entity_node",
    ]
    assert schema["topology_warning_contract"]["record_shape"]["target_fields"] == ["element_id", "entity_id", "node_ref"]
    assert schema["topology_warning_contract"]["connectivity_justifications"]["location"] == "diagram.connectivity_justifications"
    assert schema["topology_warning_contract"]["connectivity_justifications"]["suppressible_codes"] == [
        "disconnected_subgraph",
        "isolated_entity_node",
    ]
    assert schema["topology_warning_contract"]["suggested_fix_templates"] == TOPOLOGY_SUGGESTED_FIXES


def test_connected_architecture_diagram_emits_no_topology_or_coverage_warnings() -> None:
    warnings = _warning_dicts(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _node("node-db", "entity-db", "Database"),
            _edge("edge-web-api", "node-web", "node-api"),
            _edge("edge-api-db", "node-api", "node-db"),
        ]
    )

    assert warnings == []


def test_payload_critic_summary_exposes_safe_warning_counts_by_code_and_diagram_type() -> None:
    payload = {
        "title": "Runtime topology",
        "global_description": "Runtime topology with one intentionally incomplete relationship.",
        "entities": _entities()[:2],
        "interfaces": [],
        "diagrams": [
            _diagram(
                [
                    _node("node-web", "entity-web", "Web App"),
                    _node("node-api", "entity-api", "Pulse API"),
                    _edge("edge-loop", "node-web", "node-web"),
                ],
                diagram_type="runtime",
            )
        ],
    }

    critique = ArchitectureDesignRepository(None).critique_payload(payload)  # type: ignore[arg-type]
    summary = critique["summary"]

    assert summary["structured_warning_counts_by_code"] == {"isolated_entity_node": 1}
    assert summary["structured_warning_counts_by_code_and_diagram_type"] == [
        {"code": "isolated_entity_node", "diagram_type": "runtime", "count": 1}
    ]
    assert "Pulse API" not in str(summary["structured_warning_counts_by_code_and_diagram_type"])


def test_isolated_linked_entity_node_returns_structured_warning() -> None:
    warnings = _warning_dicts(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _edge("edge-loop", "node-web", "node-web"),
        ],
        entities=_entities()[:2],
    )

    assert [warning["code"] for warning in warnings] == ["isolated_entity_node"]
    warning = warnings[0]
    assert warning["severity"] == "warning"
    assert warning["diagram_id"] == "diagram-runtime"
    assert warning["diagram_type"] == "runtime"
    assert warning["element_id"] == "node-api"
    assert warning["path"] == "diagrams[0].adapter_payload.elements[1]"
    assert warning["suggested_fix"] == TOPOLOGY_SUGGESTED_FIXES["isolated_entity_node"].format(label="Pulse API")


def test_disconnected_connected_components_return_warning_without_double_counting_isolated_nodes() -> None:
    warnings = _warning_dicts(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            {"id": "node-audit", "type": "rectangle", "text": "Audit Sink"},
            {"id": "node-worker", "type": "rectangle", "text": "Worker"},
            _edge("edge-web-api", "node-web", "node-api"),
            _edge("edge-audit-worker", "node-audit", "node-worker"),
        ],
        entities=_entities()[:2],
    )

    assert [warning["code"] for warning in warnings] == ["disconnected_subgraph"]
    assert warnings[0]["element_id"] == "node-audit"
    assert warnings[0]["suggested_fix"] == TOPOLOGY_SUGGESTED_FIXES["disconnected_subgraph"]


def test_recoverable_dangling_connector_returns_warning() -> None:
    warnings = _warning_dicts(
        [
            {"id": "node-web", "type": "rectangle", "text": "Web App"},
            {"id": "node-api", "type": "rectangle", "text": "Pulse API"},
            _edge("edge-dangling", "node-web", None),
        ],
        entities=[],
    )

    assert [warning["code"] for warning in warnings] == ["dangling_connector"]
    dangling = warnings[0]
    assert dangling["element_id"] == "edge-dangling"
    assert dangling["path"] == "diagrams[0].adapter_payload.elements[2]"
    assert dangling["suggested_fix"] == TOPOLOGY_SUGGESTED_FIXES["dangling_connector"]


def test_declared_entity_without_any_diagram_coverage_returns_entity_warning() -> None:
    warnings = _warning_dicts(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _edge("edge-web-api", "node-web", "node-api"),
        ]
    )

    assert [warning["code"] for warning in warnings] == ["entity_without_diagram"]
    warning = warnings[0]
    assert warning["entity_id"] == "entity-db"
    assert warning["path"] == "entities[2]"
    assert warning["suggested_fix"] == TOPOLOGY_SUGGESTED_FIXES["entity_without_diagram"].format(entity_name="Database")


def test_entity_coverage_accepts_linked_entity_name_as_canonical_reference() -> None:
    warnings = _warning_dicts(
        [
            _node("node-web", "Web App", "Web App"),
            _node("node-api", "Pulse API", "Pulse API"),
            _edge("edge-web-api", "node-web", "node-api"),
        ],
        entities=_entities()[:2],
    )

    assert warnings == []


def test_conceptual_valid_isolated_node_justification_suppresses_and_records_reason() -> None:
    diagram = _diagram(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _edge("edge-loop", "node-web", "node-web"),
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {
        "node-api": "Pulse API is intentionally shown as an external conceptual boundary.",
    }

    result = _evaluation_dict([], entities=_entities()[:2], diagram=diagram)

    assert result["warnings"] == []
    assert len(result["suppressed_warnings"]) == 1
    suppressed = result["suppressed_warnings"][0]
    assert suppressed["code"] == "isolated_entity_node"
    assert suppressed["element_id"] == "node-api"
    assert suppressed["justification"] == "Pulse API is intentionally shown as an external conceptual boundary."


def test_conceptual_valid_disconnected_subgraph_justification_suppresses_component_warning() -> None:
    diagram = _diagram(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            {"id": "node-audit", "type": "rectangle", "text": "Audit Sink"},
            {"id": "node-worker", "type": "rectangle", "text": "Worker"},
            _edge("edge-web-api", "node-web", "node-api"),
            _edge("edge-audit-worker", "node-audit", "node-worker"),
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {
        "node-audit": "Audit flow is intentionally separated to explain a later async path.",
    }

    result = _evaluation_dict([], entities=_entities()[:2], diagram=diagram)

    assert result["warnings"] == []
    assert len(result["suppressed_warnings"]) == 1
    assert result["suppressed_warnings"][0]["code"] == "disconnected_subgraph"
    assert result["suppressed_warnings"][0]["element_id"] == "node-audit"


def test_conceptual_placeholder_justification_is_reported_and_does_not_suppress() -> None:
    diagram = _diagram(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _edge("edge-loop", "node-web", "node-web"),
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {"node-api": "TODO connect later"}

    result = _evaluation_dict([], entities=_entities()[:2], diagram=diagram)

    assert [warning["code"] for warning in result["warnings"]] == [
        "conceptual_justification_invalid",
        "isolated_entity_node",
    ]
    assert result["warnings"][0]["element_id"] == "node-api"
    assert result["suppressed_warnings"] == []


def test_conceptual_short_justification_is_reported_and_does_not_suppress() -> None:
    diagram = _diagram(
        [
            _node("node-web", "entity-web", "Web App"),
            _node("node-api", "entity-api", "Pulse API"),
            _edge("edge-loop", "node-web", "node-web"),
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {"node-api": "too short"}

    result = _evaluation_dict([], entities=_entities()[:2], diagram=diagram)

    assert [warning["code"] for warning in result["warnings"]] == [
        "conceptual_justification_invalid",
        "isolated_entity_node",
    ]
    assert "at least 12 characters" in result["warnings"][0]["message"]
    assert result["suppressed_warnings"] == []


def test_conceptual_justification_allows_node_ref_fallback_for_legacy_nodes_without_stable_id() -> None:
    diagram = _diagram(
        [
            {
                "type": "rectangle",
                "text": "Legacy Imported Node",
                "linkedEntityId": "entity-web",
                "customData": {"node_ref": "legacy-node-ref"},
            },
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {
        "legacy-node-ref": "Legacy imported diagram lacks stable element ids but is intentionally isolated.",
    }

    result = _evaluation_dict([], entities=_entities()[:1], diagram=diagram)

    assert result["warnings"] == []
    assert len(result["suppressed_warnings"]) == 1
    suppressed = result["suppressed_warnings"][0]
    assert suppressed["code"] == "isolated_entity_node"
    assert suppressed["node_ref"] == "legacy-node-ref"
    assert "element_id" not in suppressed


def test_conceptual_justification_does_not_suppress_dangling_connector_warning() -> None:
    diagram = _diagram(
        [
            {"id": "node-web", "type": "rectangle", "text": "Web App"},
            {"id": "node-api", "type": "rectangle", "text": "Pulse API"},
            _edge("edge-dangling", "node-web", None),
        ],
        diagram_type="conceptual",
    )
    diagram["connectivity_justifications"] = {
        "edge-dangling": "Connector is intentionally incomplete while discussing the conceptual path.",
    }

    result = _evaluation_dict([], entities=[], diagram=diagram)

    assert [warning["code"] for warning in result["warnings"]] == ["dangling_connector"]
    assert result["warnings"][0]["element_id"] == "edge-dangling"
    assert result["suppressed_warnings"] == []
