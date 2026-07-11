"""Application rules for propagating governed artifacts between aggregates."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.ports.application_persistence import (
    ApplicationRecord,
    get_application_persistence_port,
)

_PROPAGATED_KB_PREFIX = "[propagated from parent]"
_KB_ENTITY_BY_TYPE_NAME = {
    "IdeationKnowledgeBase": "ideation_knowledge_base",
    "RefinementKnowledgeBase": "refinement_knowledge_base",
    "SpecKnowledgeBase": "spec_knowledge_base",
}


def _kb_entity_name(identifier: str | type[Any]) -> str:
    """Normalize the legacy model-class identifier without depending on an ORM."""
    if isinstance(identifier, str):
        return identifier
    try:
        return _KB_ENTITY_BY_TYPE_NAME[identifier.__name__]
    except KeyError as exc:
        raise ValueError(
            f"unsupported_knowledge_base_entity:{identifier.__name__}"
        ) from exc


def _filter_mockups(
    mockups: list[dict] | None,
    mockup_ids: list[str] | None,
) -> list[dict]:
    """Filter and copy mockups, adding origin identity for traceability."""
    if not mockups:
        return []
    source = (
        mockups
        if mockup_ids is None
        else [item for item in mockups if item.get("id") in mockup_ids]
    )
    copied: list[dict] = []
    for item in source:
        new_item = dict(item)
        new_item["origin_id"] = item.get("id")
        origin_token = f"{item.get('id')}{id(new_item)}"
        new_item["id"] = f"sm_{hashlib.md5(origin_token.encode()).hexdigest()[:8]}"
        copied.append(new_item)
    return copied


def _propagated_kb_description(description: str | None) -> str:
    """Apply the human-readable propagation marker at most once."""
    body = (description or "").strip()
    if body.startswith(_PROPAGATED_KB_PREFIX):
        return body
    return f"{_PROPAGATED_KB_PREFIX} {body}".strip()


async def propagate_artifacts(
    db: Any,
    source_mockups: list[dict] | None,
    source_qa_items: list | None,
    source_knowledge_bases: list | None,
    target_entity: ApplicationRecord,
    target_kb_entity: str | type[Any] | None,
    user_id: str,
    mockup_ids: list[str] | None = None,
    kb_ids: list[str] | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    source_title: str | None = None,
    source_version: int | None = None,
) -> None:
    """Apply additive mockup, knowledge and answered-Q&A propagation rules."""
    persistence = get_application_persistence_port()

    copied_mockups = _filter_mockups(source_mockups, mockup_ids)
    if copied_mockups:
        existing = list(target_entity.screen_mockups or [])
        propagated = existing + copied_mockups
        from okto_pulse.core.services.design_system import gate_entity_screen_mockups

        target_entity.screen_mockups = existing
        await gate_entity_screen_mockups(
            db,
            target_entity,
            propagated,
            entity_type=target_entity.entity,
        )
        target_entity.screen_mockups = propagated

    if target_kb_entity and source_knowledge_bases:
        target_kb_entity_name = _kb_entity_name(target_kb_entity)
        knowledge_bases = (
            source_knowledge_bases
            if kb_ids is None
            else [
                item
                for item in source_knowledge_bases
                if (
                    item.get("id")
                    if isinstance(item, dict)
                    else getattr(item, "id", None)
                )
                in kb_ids
            ]
        )
        target_id_field = {
            "spec_knowledge_base": "spec_id",
            "refinement_knowledge_base": "refinement_id",
            "ideation_knowledge_base": "ideation_id",
        }.get(target_kb_entity_name)
        if target_id_field:
            for item in knowledge_bases:
                get_value = (
                    (lambda key: item.get(key))
                    if isinstance(item, dict)
                    else (lambda key: getattr(item, key, None))
                )
                parent_kb_id = get_value("id")
                parent_root = get_value("root_source_kb_id")
                payload = {
                    target_id_field: target_entity.id,
                    "title": get_value("title"),
                    "description": _propagated_kb_description(
                        get_value("description")
                    ),
                    "content": get_value("content"),
                    "mime_type": get_value("mime_type") or "text/markdown",
                    "created_by": user_id,
                    "source_type": source_type,
                    "source_id": source_id,
                    "source_title": source_title,
                    "source_version": source_version,
                    "source_kb_id": parent_kb_id,
                    "immediate_parent_kb_id": parent_kb_id,
                    "root_source_kb_id": parent_root or parent_kb_id,
                }
                await persistence.add(
                    db,
                    ApplicationRecord(
                        entity=target_kb_entity_name,
                        values={
                            key: value
                            for key, value in payload.items()
                            if value is not None
                        },
                    ),
                )
            await persistence.flush(db)

    if source_qa_items:
        target_qa = {
            "spec": ("spec_qa_item", "spec_id"),
            "refinement": ("refinement_qa_item", "refinement_id"),
        }.get(target_entity.entity)
        if target_qa:
            target_qa_entity, target_fk_field = target_qa
            for item in source_qa_items:
                get_value = (
                    (lambda key: item.get(key))
                    if isinstance(item, dict)
                    else (lambda key: getattr(item, key, None))
                )
                answer = get_value("answer")
                selected = get_value("selected")
                if not answer and not selected:
                    continue
                payload: dict[str, Any] = {
                    target_fk_field: target_entity.id,
                    "question": get_value("question") or "",
                    "question_type": get_value("question_type") or "text",
                    "choices": get_value("choices"),
                    "allow_free_text": get_value("allow_free_text") or False,
                    "answer": answer,
                    "selected": selected,
                    "asked_by": get_value("asked_by") or user_id,
                    "answered_by": get_value("answered_by"),
                    "answered_at": (
                        get_value("answered_at")
                        or get_value("created_at")
                        or datetime.now(timezone.utc)
                    ),
                }
                if get_value("created_at") is not None:
                    payload["created_at"] = get_value("created_at")
                await persistence.add(
                    db,
                    ApplicationRecord(entity=target_qa_entity, values=payload),
                )
            await persistence.flush(db)


__all__ = ["propagate_artifacts"]
