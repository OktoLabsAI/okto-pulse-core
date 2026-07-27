"""Canonical effective Knowledge reads for application-facing surfaces."""

from __future__ import annotations

from copy import copy
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeTargetKey,
    KnowledgeTargetType,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)


def _detached_mapping(item: Any) -> dict[str, Any] | None:
    """Copy one persistence-neutral Knowledge payload into a detached mapping."""

    if isinstance(item, Mapping):
        return dict(item)
    values = getattr(item, "values", None)
    if isinstance(values, Mapping):
        return dict(values)
    model_dump = getattr(item, "model_dump", None)
    if callable(model_dump):
        payload = model_dump(mode="python")
        return dict(payload) if isinstance(payload, Mapping) else None
    raw = getattr(item, "__dict__", None)
    if isinstance(raw, Mapping):
        return {
            str(key): value
            for key, value in raw.items()
            if not str(key).startswith("_")
        }
    return None


async def load_effective_knowledge(
    services: Any,
    entity: Any,
    *,
    target_type: KnowledgeTargetType | str,
) -> list[dict[str, Any]]:
    """Return the one authoritative Knowledge projection for a Spec or Card.

    Once v2 is active, every REST/MCP consumer reads the same ResourceLineage
    projection as Resource Gate.  Historical physical JSON stays available
    through the technical/history projection and cannot leak into this result.
    """

    resolved_target_type = KnowledgeTargetType(target_type)
    if resolved_target_type not in {
        KnowledgeTargetType.SPEC,
        KnowledgeTargetType.CARD,
    }:
        raise ValueError("knowledge_effective_read_target_type_unsupported")
    target = KnowledgeTargetKey(
        board_id=str(entity.board_id),
        target_type=resolved_target_type,
        target_id=str(entity.id),
    )
    read = await services.knowledge_propagation.read(target)
    if not read.v2_active:
        return list(
            filter(
                None,
                (
                    _detached_mapping(item)
                    for item in (getattr(entity, "knowledge_bases", None) or ())
                ),
            )
        )

    projection = await services.resource_gate.get_effective_resources(
        str(entity.board_id),
        resolved_target_type.value,
        str(entity.id),
    )
    effective: list[dict[str, Any]] = []
    resources = projection.get("resources")
    knowledge = (
        resources.get("knowledge_base")
        if isinstance(resources, Mapping)
        else None
    )
    if not isinstance(knowledge, list):
        raise KnowledgePropagationServiceError(
            "knowledge_propagation_effective_read_failed",
            "Resource Gate returned an invalid effective Knowledge projection",
            details={
                "board_id": str(entity.board_id),
                "target_type": resolved_target_type.value,
                "target_id": str(entity.id),
            },
        )
    for item in knowledge:
        if not isinstance(item, Mapping):
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_effective_read_failed",
                "Resource Gate returned an invalid effective Knowledge item",
                details={
                    "board_id": str(entity.board_id),
                    "target_type": resolved_target_type.value,
                    "target_id": str(entity.id),
                },
            )
        payload = item.get("resource")
        if not item.get("hydrated") or not isinstance(payload, Mapping):
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_effective_read_failed",
                "an effective Knowledge assignment could not be hydrated",
                details={
                    "board_id": str(entity.board_id),
                    "target_type": resolved_target_type.value,
                    "target_id": str(entity.id),
                    "resource_id": item.get("id"),
                    "hydration_error": item.get("hydration_error"),
                },
            )
        resolved = dict(payload)
        if resolved_target_type is KnowledgeTargetType.SPEC:
            resolved["spec_id"] = str(entity.id)
        ref = item.get("ref")
        if isinstance(ref, Mapping) and ref.get("knowledge_assignment_id"):
            resolved["knowledge_assignment"] = {
                "assignment_id": ref.get("knowledge_assignment_id"),
                "mode": ref.get("knowledge_assignment_mode"),
                "state": ref.get("knowledge_assignment_state"),
                "stale": bool(ref.get("knowledge_assignment_stale")),
                "origin_class": ref.get("origin_class"),
            }
        effective.append(resolved)
    return effective


async def load_effective_card_knowledge(
    services: Any,
    card: Any,
) -> list[dict[str, Any]]:
    return await load_effective_knowledge(
        services,
        card,
        target_type=KnowledgeTargetType.CARD,
    )


async def load_effective_spec_knowledge(
    services: Any,
    spec: Any,
) -> list[dict[str, Any]]:
    return await load_effective_knowledge(
        services,
        spec,
        target_type=KnowledgeTargetType.SPEC,
    )


def attach_effective_knowledge(
    entity: Any,
    knowledge_bases: list[dict[str, Any]],
) -> Any:
    """Attach a read projection without marking a detached record dirty."""

    payload = [dict(item) for item in knowledge_bases]
    attach = getattr(entity, "attach", None)
    if callable(attach):
        attach("knowledge_bases", payload)
        return entity
    if isinstance(entity, Mapping):
        projected = dict(entity)
        projected["knowledge_bases"] = payload
        return projected
    projected = copy(entity)
    setattr(projected, "knowledge_bases", payload)
    return projected


async def project_effective_knowledge(
    services: Any,
    entity: Any,
    *,
    target_type: KnowledgeTargetType | str,
) -> Any:
    return attach_effective_knowledge(
        entity,
        await load_effective_knowledge(
            services,
            entity,
            target_type=target_type,
        ),
    )


__all__ = [
    "attach_effective_knowledge",
    "load_effective_card_knowledge",
    "load_effective_knowledge",
    "load_effective_spec_knowledge",
    "project_effective_knowledge",
]
