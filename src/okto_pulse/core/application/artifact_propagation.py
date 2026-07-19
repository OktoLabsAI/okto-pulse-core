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


class ArtifactResourceSelectionError(ValueError):
    """An explicit mockup/KB token did not resolve on the source artifact."""

    code = "resource_selection_invalid"

    def __init__(
        self,
        *,
        resource_type: str,
        source_type: str | None,
        source_id: str | None,
        requested: list[str],
        matched: list[str],
        missing: list[str],
    ) -> None:
        self.resource_type = resource_type
        self.source_type = source_type
        self.source_id = source_id
        self.requested = tuple(requested)
        self.matched = tuple(matched)
        self.missing = tuple(missing)
        super().__init__(self.code)

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "detail": (
                f"One or more {self.resource_type} selection tokens do not "
                "resolve on the requested source artifact."
            ),
            "resource_type": self.resource_type,
            "source_parent_type": self.source_type,
            "source_parent_id": self.source_id,
            "requested": list(self.requested),
            "matched": list(self.matched),
            "missing": list(self.missing),
            "retryable": False,
        }


def _value(item: Any, key: str) -> Any:
    return item.get(key) if isinstance(item, dict) else getattr(item, key, None)


def artifact_identity_values(item: Any, resource_type: str) -> set[str]:
    values: set[str] = set()

    def add(value: Any) -> None:
        if value in (None, ""):
            return
        token = str(value).strip()
        if not token:
            return
        values.add(token)
        tail = token
        for separator in (":", "/", "\\"):
            if separator in tail:
                tail = tail.rsplit(separator, 1)[-1]
        if tail:
            values.add(tail)
            values.add(f"{resource_type}:{tail}")
            if resource_type == "knowledge_base":
                values.add(f"kb:{tail}")

    for key in (
        "id",
        "root_source_mockup_id",
        "root_source_kb_id",
        "origin_id",
        "source_mockup_id",
        "source_kb_id",
        "immediate_parent_kb_id",
        "source_ref",
        "origin_ref",
        "source",
    ):
        add(_value(item, key))
    return values


def validate_artifact_selections(
    *,
    source_mockups: list[Any] | None,
    source_knowledge_bases: list[Any] | None,
    mockup_ids: list[str] | None,
    kb_ids: list[str] | None,
    source_type: str | None,
    source_id: str | None,
) -> None:
    for resource_type, items, requested_values in (
        ("mockup", source_mockups or [], mockup_ids),
        ("knowledge_base", source_knowledge_bases or [], kb_ids),
    ):
        if requested_values is None:
            continue
        requested = list(
            dict.fromkeys(
                str(item).strip()
                for item in requested_values
                if str(item).strip()
            )
        )
        identities = [artifact_identity_values(item, resource_type) for item in items]
        matched = [
            token for token in requested if any(token in identity for identity in identities)
        ]
        missing = [token for token in requested if token not in matched]
        if missing:
            raise ArtifactResourceSelectionError(
                resource_type=resource_type,
                source_type=source_type,
                source_id=source_id,
                requested=requested,
                matched=matched,
                missing=missing,
            )


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
        else [
            item
            for item in mockups
            if artifact_identity_values(item, "mockup") & set(mockup_ids)
        ]
    )
    copied: list[dict] = []
    for item in source:
        new_item = dict(item)
        # Preserve the canonical root across every snapshot hop.  The direct
        # parent remains available separately for audit/debugging.
        new_item["origin_id"] = (
            item.get("origin_id")
            or item.get("source_mockup_id")
            or item.get("id")
        )
        new_item["source_mockup_id"] = item.get("id")
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
) -> dict[str, int]:
    """Apply additive mockup, knowledge and answered-Q&A propagation rules."""
    persistence = get_application_persistence_port()
    copied_kb_count = 0
    copied_qa_count = 0

    validate_artifact_selections(
        source_mockups=source_mockups,
        source_knowledge_bases=source_knowledge_bases,
        mockup_ids=mockup_ids,
        kb_ids=kb_ids,
        source_type=source_type,
        source_id=source_id,
    )

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
                if artifact_identity_values(item, "knowledge_base") & set(kb_ids)
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
                copied_kb_count += 1
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
                copied_qa_count += 1
            await persistence.flush(db)

    return {
        "mockup": len(copied_mockups),
        "knowledge_base": copied_kb_count,
        "qa": copied_qa_count,
    }


__all__ = [
    "ArtifactResourceSelectionError",
    "artifact_identity_values",
    "propagate_artifacts",
    "validate_artifact_selections",
]
