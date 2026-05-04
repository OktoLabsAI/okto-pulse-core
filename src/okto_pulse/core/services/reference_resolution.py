"""Reference resolution helpers for MCP contexts and traceability reports.

These helpers keep the existing entity payloads intact while adding an explicit
view of direct and inherited references. Agents and markdown exports can then
consume one predictable structure instead of guessing where a referenced
artifact lives.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.services.analytics_service import (
    resolve_linked_criteria_to_indices,
    resolve_linked_fr_indices,
)


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _source_meta(
    *,
    source_type: str,
    source_id: str | None,
    source_title: str | None,
    reference_type: str,
) -> dict[str, Any]:
    return {
        "source_type": source_type,
        "source_id": source_id,
        "source_title": source_title,
        "reference_type": reference_type,
    }


def _serialize_kb(kb: Any, *, include_content: bool) -> dict[str, Any]:
    if isinstance(kb, dict):
        data = {
            "id": kb.get("id"),
            "title": kb.get("title") or kb.get("name"),
            "description": kb.get("description"),
            "mime_type": kb.get("mime_type") or kb.get("content_type") or "text/markdown",
        }
        if include_content:
            data["content"] = kb.get("content")
        for attr in ("source", "source_type", "source_id", "created_by", "created_at", "updated_at"):
            if kb.get(attr) is not None:
                data[attr] = kb.get(attr)
        return data

    data: dict[str, Any] = {
        "id": getattr(kb, "id", None),
        "title": getattr(kb, "title", None),
        "description": getattr(kb, "description", None),
        "mime_type": getattr(kb, "mime_type", "text/markdown"),
    }
    if include_content:
        data["content"] = getattr(kb, "content", None)
    for attr in ("created_by", "created_at", "updated_at"):
        value = getattr(kb, attr, None)
        if value is not None:
            data[attr] = value.isoformat() if hasattr(value, "isoformat") else value
    return data


def _serialize_mockup(mockup: Any) -> dict[str, Any] | None:
    if not isinstance(mockup, dict):
        return None
    data = {
        "id": mockup.get("id"),
        "title": mockup.get("title") or mockup.get("name"),
        "description": mockup.get("description"),
        "screen_type": mockup.get("screen_type"),
        "origin_id": mockup.get("origin_id"),
    }
    for attr in ("source", "source_id", "html_content", "annotations", "order"):
        if mockup.get(attr) is not None:
            data[attr] = mockup.get(attr)
    return data


def _serialize_architecture_design(design: Any) -> dict[str, Any] | None:
    if isinstance(design, dict):
        data = {
            "id": design.get("id"),
            "title": design.get("title"),
            "parent_type": design.get("parent_type"),
            "parent_id": design.get("parent_id"),
            "version": design.get("version"),
            "source_design_id": design.get("source_design_id"),
            "source_ref": design.get("source_ref"),
            "source_version": design.get("source_version"),
        }
        for attr in ("global_description", "entities", "interfaces", "diagrams"):
            if design.get(attr) is not None:
                data[attr] = design.get(attr)
        return data

    if design is None:
        return None
    return {
        "id": getattr(design, "id", None),
        "title": getattr(design, "title", None),
        "parent_type": getattr(design, "parent_type", None),
        "parent_id": (
            getattr(design, "ideation_id", None)
            or getattr(design, "refinement_id", None)
            or getattr(design, "spec_id", None)
            or getattr(design, "card_id", None)
        ),
        "version": getattr(design, "version", None),
        "source_design_id": getattr(design, "source_design_id", None),
        "source_ref": getattr(design, "source_ref", None),
        "source_version": getattr(design, "source_version", None),
    }


def resolve_artifact_references(
    entity: Any,
    *,
    source_type: str,
    source_id: str | None,
    source_title: str | None,
    reference_type: str = "direct",
    include_content: bool = True,
    architecture_designs: list[Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return normalized KB, mockup and architecture references for an entity."""

    meta = _source_meta(
        source_type=source_type,
        source_id=source_id,
        source_title=source_title,
        reference_type=reference_type,
    )
    mockups = [
        {**serialized, **meta}
        for item in (getattr(entity, "screen_mockups", None) or [])
        if (serialized := _serialize_mockup(item))
    ]
    knowledge_bases = [
        {**_serialize_kb(kb, include_content=include_content), **meta}
        for kb in (getattr(entity, "knowledge_bases", None) or [])
    ]
    designs_source = (
        architecture_designs
        if architecture_designs is not None
        else (getattr(entity, "architecture_designs", None) or [])
    )
    architecture = [
        {**serialized, **meta}
        for design in designs_source
        if (serialized := _serialize_architecture_design(design))
    ]
    return {
        "knowledge_bases": knowledge_bases,
        "screen_mockups": mockups,
        "architecture_designs": architecture,
    }


def merge_reference_groups(*groups: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for group in groups:
        for key, values in group.items():
            merged.setdefault(key, []).extend(values or [])
    return merged


def _is_linked_to_card(item: Any, card_id: str | None) -> bool:
    if not card_id or not isinstance(item, dict):
        return False
    return card_id in (item.get("linked_task_ids") or [])


def _requirement_item(
    value: Any,
    *,
    index: int,
    source_type: str,
    source_id: str | None,
    source_title: str | None,
    reference_type: str,
    referenced_by_task: bool = False,
) -> dict[str, Any]:
    data = dict(value) if isinstance(value, dict) else {"text": value}
    data.setdefault("id", str(index))
    data["index"] = index
    data.update(
        _source_meta(
            source_type=source_type,
            source_id=source_id,
            source_title=source_title,
            reference_type=reference_type,
        )
    )
    data["referenced_by_task"] = referenced_by_task
    return data


def _structured_item(
    item: Any,
    *,
    source_type: str,
    source_id: str | None,
    source_title: str | None,
    default_reference_type: str,
    linked_reference_type: str,
    linked: bool,
) -> dict[str, Any]:
    data = dict(item) if isinstance(item, dict) else {"value": item}
    data.update(
        _source_meta(
            source_type=source_type,
            source_id=source_id,
            source_title=source_title,
            reference_type=linked_reference_type if linked else default_reference_type,
        )
    )
    data["referenced_by_task"] = linked
    return data


def resolve_spec_references(
    spec: Any,
    *,
    card: Any | None = None,
    include_superseded: bool = False,
    include_content: bool = True,
    artifact_reference_type: str = "direct",
    structured_reference_type: str = "direct",
    architecture_designs: list[Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Resolve structured spec data and artifacts into a normalized reference map."""

    spec_id = getattr(spec, "id", None)
    spec_title = getattr(spec, "title", None)
    card_id = getattr(card, "id", None)
    frs = list(getattr(spec, "functional_requirements", None) or [])
    trs = list(getattr(spec, "technical_requirements", None) or [])
    criteria = list(getattr(spec, "acceptance_criteria", None) or [])
    scenarios = list(getattr(spec, "test_scenarios", None) or [])
    rules = list(getattr(spec, "business_rules", None) or [])
    contracts = list(getattr(spec, "api_contracts", None) or [])
    decisions = [
        item for item in (getattr(spec, "decisions", None) or [])
        if include_superseded or not isinstance(item, dict) or item.get("status", "active") == "active"
    ]

    linked_scenarios = {
        item.get("id")
        for item in scenarios
        if isinstance(item, dict)
        and card_id
        and (card_id in (item.get("linked_task_ids") or []) or item.get("id") in (getattr(card, "test_scenario_ids", None) or []))
    }
    linked_ac_indices: set[int] = set()
    for item in scenarios:
        if isinstance(item, dict) and item.get("id") in linked_scenarios:
            linked_ac_indices |= resolve_linked_criteria_to_indices(
                item.get("linked_criteria") or [], criteria
            )

    linked_fr_indices: set[int] = set()
    for collection in (rules, contracts, decisions):
        for item in collection:
            if isinstance(item, dict) and _is_linked_to_card(item, card_id):
                linked_fr_indices |= resolve_linked_fr_indices(
                    item.get("linked_requirements") or [], frs
                )

    result: dict[str, list[dict[str, Any]]] = {
        "functional_requirements": [
            _requirement_item(
                item,
                index=i,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                reference_type=structured_reference_type,
                referenced_by_task=i in linked_fr_indices,
            )
            for i, item in enumerate(frs)
        ],
        "technical_requirements": [
            _requirement_item(
                item,
                index=i,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                reference_type=(
                    "linked_task" if isinstance(item, dict) and _is_linked_to_card(item, card_id)
                    else structured_reference_type
                ),
                referenced_by_task=isinstance(item, dict) and _is_linked_to_card(item, card_id),
            )
            for i, item in enumerate(trs)
        ],
        "acceptance_criteria": [
            _requirement_item(
                item,
                index=i,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                reference_type="linked_task" if i in linked_ac_indices else structured_reference_type,
                referenced_by_task=i in linked_ac_indices,
            )
            for i, item in enumerate(criteria)
        ],
        "test_scenarios": [
            _structured_item(
                item,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                default_reference_type=structured_reference_type,
                linked_reference_type="linked_task",
                linked=isinstance(item, dict) and item.get("id") in linked_scenarios,
            )
            for item in scenarios
        ],
        "business_rules": [
            _structured_item(
                item,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                default_reference_type=structured_reference_type,
                linked_reference_type="linked_task",
                linked=_is_linked_to_card(item, card_id),
            )
            for item in rules
        ],
        "api_contracts": [
            _structured_item(
                item,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                default_reference_type=structured_reference_type,
                linked_reference_type="linked_task",
                linked=_is_linked_to_card(item, card_id),
            )
            for item in contracts
        ],
        "decisions": [
            _structured_item(
                item,
                source_type="spec",
                source_id=spec_id,
                source_title=spec_title,
                default_reference_type=structured_reference_type,
                linked_reference_type="linked_task",
                linked=_is_linked_to_card(item, card_id),
            )
            for item in decisions
        ],
    }
    return merge_reference_groups(
        result,
        resolve_artifact_references(
            spec,
            source_type="spec",
            source_id=spec_id,
            source_title=spec_title,
            reference_type=artifact_reference_type,
            include_content=include_content,
            architecture_designs=architecture_designs,
        ),
    )


def resolve_entity_context_references(
    entity: Any,
    *,
    source_type: str,
    include_content: bool = True,
    architecture_designs: list[Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Resolve direct artifacts for ideation/refinement-like context payloads."""

    return resolve_artifact_references(
        entity,
        source_type=source_type,
        source_id=getattr(entity, "id", None),
        source_title=getattr(entity, "title", None),
        reference_type="direct",
        include_content=include_content,
        architecture_designs=architecture_designs,
    )


def resolve_task_context_references(
    card: Any,
    spec: Any | None,
    *,
    include_superseded: bool = False,
    include_content: bool = True,
    card_architecture_designs: list[Any] | None = None,
    spec_architecture_designs: list[Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Resolve everything a task references directly or through its parent spec."""

    card_refs = resolve_artifact_references(
        card,
        source_type="card",
        source_id=getattr(card, "id", None),
        source_title=getattr(card, "title", None),
        reference_type="direct",
        include_content=include_content,
        architecture_designs=card_architecture_designs,
    )
    if not spec:
        return card_refs
    spec_refs = resolve_spec_references(
        spec,
        card=card,
        include_superseded=include_superseded,
        include_content=include_content,
        artifact_reference_type="parent_spec",
        structured_reference_type="parent_spec",
        architecture_designs=spec_architecture_designs,
    )
    return merge_reference_groups(card_refs, spec_refs)

