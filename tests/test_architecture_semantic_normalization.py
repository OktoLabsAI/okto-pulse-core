"""Spec cc497a0d — Semantic node normalization & rejection for Excalidraw payloads.

Coverage:
  - ts_semantic_normalize_safe — Safe linkedEntityId-only node is normalized before persistence.
  - ts_semantic_reject_ambiguous — Divergent/invalid metadata is rejected with suggested_fixes.
  - ts_semantic_mcp_agent_contract — MCP schema exposes semantic_node_registry to agents.
  - ts_semantic_legacy_lazy — Legacy payload migrates only when deterministic.
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Ideation
from okto_pulse.core.services.architecture import (
    ALLOWED_NODE_ICON_NAMES,
    REQUIRED_LINKED_NODE_FIELDS,
    ArchitectureDesignRepository,
    ArchitecturePayloadValidationError,
    architecture_design_payload_schema,
    normalize_excalidraw_semantics,
)


USER_ID = "semantic-norm-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed(db_factory) -> tuple[str, str]:
    board_id = _id("sem-board")
    ideation_id = _id("sem-ideation")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Semantic Normalization Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Semantic ideation",
                description="seed",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, ideation_id


def _base_payload(extra_node: dict | None = None) -> dict:
    payload = {
        "title": "Semantic test architecture",
        "global_description": "Customer Portal calls Checkout API which persists Orders.",
        "entities": [
            {
                "id": "entity-portal",
                "name": "Customer Portal",
                "entity_type": "web_app",
                "responsibility": "Collects checkout input from customers.",
            },
            {
                "id": "entity-checkout",
                "name": "Checkout API",
                "entity_type": "api",
                "responsibility": "Validates checkout and persists orders.",
            },
            {
                "id": "entity-orders",
                "name": "Orders DB",
                "entity_type": "database",
                "responsibility": "Persists order state.",
            },
        ],
        "interfaces": [],
        "diagrams": [
            {
                "id": "diagram-main",
                "title": "Main flow",
                "diagram_type": "context",
                "format": "excalidraw_json",
                "adapter_payload": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [
                        {"id": "node-checkout", "type": "rectangle", "linkedEntityId": "entity-checkout"},
                    ],
                    "appState": {},
                    "files": {},
                },
            }
        ],
    }
    if extra_node is not None:
        payload["diagrams"][0]["adapter_payload"]["elements"].append(extra_node)
    return payload


# ---------------------------------------------------------------------------
# ts_semantic_normalize_safe — safe linkedEntityId-only node gets filled
# ---------------------------------------------------------------------------


def test_normalize_excalidraw_semantics_fills_safe_metadata_for_linked_node() -> None:
    payload = _base_payload()
    entities = payload["entities"]
    diagram_payload = payload["diagrams"][0]["adapter_payload"]

    normalized, warnings, issues = normalize_excalidraw_semantics(diagram_payload, entities)

    assert issues == []
    assert len(warnings) == 1
    assert "semantic_metadata_normalized" in warnings[0]
    node = normalized["elements"][0]
    assert node["text"] == "Checkout API"
    assert node["displayType"] == "API"
    assert node["architectureKind"] == "service"
    assert node["iconName"] == "server"
    # All required fields present after normalization
    for field in REQUIRED_LINKED_NODE_FIELDS:
        assert node.get(field), f"expected field {field!r} to be populated"


@pytest.mark.asyncio
async def test_critique_payload_emits_warning_for_normalized_node(db_factory) -> None:
    _, _ = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        critique = repo.critique_payload(_base_payload())
        assert critique["valid"] is True, critique["issues"]
        assert any(
            "semantic_metadata_normalized" in warning for warning in critique["warnings"]
        ), critique["warnings"]


@pytest.mark.asyncio
async def test_create_persists_normalized_metadata(db_factory) -> None:
    _, ideation_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload = _base_payload()
        payload["architecture_warning_acknowledgement"] = {
            "accepted": True,
            "statement": "Known unmapped entities are acceptable for this normalization fixture.",
        }
        design = await repo.create("ideation", ideation_id, payload, USER_ID)
        await db.commit()
        # Re-fetch in same session and load payload via diagram store.
        loaded = await repo.get(design.id)
        await repo._attach_payloads(loaded)  # type: ignore[attr-defined]
        node = loaded.diagrams[0]["adapter_payload"]["elements"][0]
        assert node["displayType"] == "API"
        assert node["architectureKind"] == "service"
        assert node["iconName"] == "server"
        assert node["text"] == "Checkout API"


# ---------------------------------------------------------------------------
# ts_semantic_reject_ambiguous — divergence/invalid icon/unknown type rejected
# ---------------------------------------------------------------------------


def test_normalize_rejects_diverging_display_type() -> None:
    payload = _base_payload()
    payload["diagrams"][0]["adapter_payload"]["elements"][0]["displayType"] = "Database"
    diagram_payload = payload["diagrams"][0]["adapter_payload"]
    _, _, issues = normalize_excalidraw_semantics(diagram_payload, payload["entities"])
    assert any("diverges" in issue and "Database" in issue for issue in issues), issues


def test_normalize_rejects_invalid_icon_name() -> None:
    payload = _base_payload()
    payload["diagrams"][0]["adapter_payload"]["elements"][0]["iconName"] = "not-a-real-icon"
    diagram_payload = payload["diagrams"][0]["adapter_payload"]
    _, _, issues = normalize_excalidraw_semantics(diagram_payload, payload["entities"])
    assert any("not in the allowed icon set" in issue for issue in issues), issues


def test_normalize_rejects_linked_node_when_entity_type_missing() -> None:
    payload = _base_payload()
    payload["entities"][1]["entity_type"] = ""  # checkout api without entity_type
    _, _, issues = normalize_excalidraw_semantics(
        payload["diagrams"][0]["adapter_payload"], payload["entities"]
    )
    assert any("without entity_type" in issue for issue in issues), issues


@pytest.mark.asyncio
async def test_create_rejects_diverging_display_type_with_suggested_fix(db_factory) -> None:
    _, ideation_id = await _seed(db_factory)
    payload = _base_payload()
    payload["diagrams"][0]["adapter_payload"]["elements"][0]["displayType"] = "Database"
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        critique = repo.critique_payload(payload)
        assert critique["valid"] is False
        assert any("diverges" in issue for issue in critique["issues"])
        assert any(
            "registry" in fix.lower() or "entity_type" in fix.lower()
            for fix in critique["suggested_fixes"]
        ), critique["suggested_fixes"]

        with pytest.raises(ArchitecturePayloadValidationError) as excinfo:
            await repo.create("ideation", ideation_id, payload, USER_ID)
        assert any("diverges" in issue for issue in excinfo.value.issues)


# ---------------------------------------------------------------------------
# ts_semantic_mcp_agent_contract — payload schema exposes the registry
# ---------------------------------------------------------------------------


def test_architecture_design_payload_schema_exposes_semantic_registry() -> None:
    schema = architecture_design_payload_schema()
    registry_section = schema.get("semantic_node_registry")
    assert registry_section is not None, "semantic_node_registry missing from schema"
    assert registry_section["required_node_fields_for_linked"] == list(REQUIRED_LINKED_NODE_FIELDS)
    assert set(registry_section["allowed_icon_names"]) == ALLOWED_NODE_ICON_NAMES
    assert "api" in registry_section["mappings"], "core entity_types must be exposed"
    assert "database" in registry_section["mappings"]
    # Validation flow must instruct agents to call schema -> validate -> persist
    flow = registry_section["validation_flow_for_agents"]
    assert any("get_architecture_design_schema" in step for step in flow)
    assert any("validate" in step for step in flow)


def test_semantic_registry_covers_spec_entity_type_examples() -> None:
    schema = architecture_design_payload_schema()
    examples = schema.get("entity_type_examples") or []
    # Every example in the spec contract must resolve to either a direct mapping
    # or a family fallback (database_table -> database, etc.).
    from okto_pulse.core.services.architecture import _resolve_semantic_canonical, _semantic_registry_key

    for entity_type in examples:
        key = _semantic_registry_key(entity_type)
        canonical = _resolve_semantic_canonical(key)
        assert canonical is not None, (
            f"entity_type {entity_type!r} from spec contract has no canonical mapping; "
            "extend SEMANTIC_NODE_REGISTRY."
        )


# ---------------------------------------------------------------------------
# ts_semantic_legacy_lazy — legacy payloads migrate only when deterministic
# ---------------------------------------------------------------------------


def test_legacy_lazy_migration_safe_inference() -> None:
    """Legacy linkedEntityId-only node with mapped entity_type -> auto-filled with warning."""
    payload = _base_payload()
    diagram_payload = payload["diagrams"][0]["adapter_payload"]
    normalized, warnings, issues = normalize_excalidraw_semantics(diagram_payload, payload["entities"])
    assert issues == []
    assert any("semantic_metadata_normalized" in w for w in warnings)
    assert normalized["elements"][0]["displayType"] == "API"


def test_legacy_lazy_migration_blocks_when_unmapped_type_and_no_metadata() -> None:
    """Legacy linkedEntityId-only node whose entity_type cannot be resolved -> blocked."""
    payload = _base_payload()
    # Add an entity with an exotic type that the registry cannot resolve.
    payload["entities"].append({
        "id": "entity-exotic",
        "name": "Exotic Component",
        "entity_type": "totally_unknown_xyz",
        "responsibility": "Demonstrates ambiguous case.",
    })
    payload["diagrams"][0]["adapter_payload"]["elements"].append(
        {"id": "node-exotic", "type": "rectangle", "linkedEntityId": "entity-exotic"}
    )
    _, _, issues = normalize_excalidraw_semantics(
        payload["diagrams"][0]["adapter_payload"], payload["entities"]
    )
    assert any("no canonical mapping" in issue for issue in issues), issues


def test_legacy_lazy_migration_accepts_explicit_metadata_for_unmapped_type() -> None:
    """Legacy node with unmapped entity_type BUT explicit metadata -> accepted, no auto-fill."""
    payload = _base_payload()
    payload["entities"].append({
        "id": "entity-exotic",
        "name": "Exotic",
        "entity_type": "exotic_proprietary_type",
        "responsibility": "Custom non-registry type.",
    })
    payload["diagrams"][0]["adapter_payload"]["elements"].append({
        "id": "node-exotic",
        "type": "rectangle",
        "linkedEntityId": "entity-exotic",
        "text": "Exotic",
        "displayType": "Custom Component",
        "architectureKind": "custom",
        "iconName": "boxes",
    })
    _, _, issues = normalize_excalidraw_semantics(
        payload["diagrams"][0]["adapter_payload"], payload["entities"]
    )
    # The base node still gets normalized; the exotic node must NOT raise.
    assert not any("totally_unknown_xyz" in issue for issue in issues)
    assert not any("exotic_proprietary_type" in issue for issue in issues), issues
