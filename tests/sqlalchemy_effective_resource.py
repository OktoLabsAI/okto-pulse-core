"""Test-only SQLAlchemy effective resource persistence adapter."""

from typing import Any

from sqlalchemy import select

from sqlalchemy_test_models import (
    Ideation,
    IdeationKnowledgeBase,
    Refinement,
    RefinementKnowledgeBase,
    Spec,
    SpecKnowledgeBase,
)
from okto_pulse.core.domain.knowledge_fingerprint import (
    resolve_knowledge_content_sha256,
)


class TestSqlAlchemyEffectiveResourcePersistence:
    __test__ = False

    async def load_knowledge_bases(
        self,
        context,
        *,
        source_entity_type: str,
        source_entity_id: str,
    ) -> list[dict[str, Any]]:
        model_and_fk = {
            "ideation": (IdeationKnowledgeBase, IdeationKnowledgeBase.ideation_id),
            "refinement": (
                RefinementKnowledgeBase,
                RefinementKnowledgeBase.refinement_id,
            ),
            "spec": (SpecKnowledgeBase, SpecKnowledgeBase.spec_id),
        }.get(source_entity_type)
        if model_and_fk is None:
            return []
        model, foreign_key = model_and_fk
        rows = (
            await context.execute(
                select(model)
                .where(foreign_key == source_entity_id)
                .order_by(model.created_at.asc())
            )
        ).scalars().all()
        return [
            {
                "id": row.id,
                "title": row.title,
                "description": getattr(row, "description", None),
                "content": row.content,
                "mime_type": getattr(row, "mime_type", None) or "text/markdown",
                "source_version": getattr(row, "source_version", None),
                "source_kb_id": getattr(row, "source_kb_id", None),
                "root_source_kb_id": getattr(row, "root_source_kb_id", None),
                "immediate_parent_kb_id": getattr(
                    row, "immediate_parent_kb_id", None
                ),
                "content_hash": resolve_knowledge_content_sha256(row),
            }
            for row in rows
        ]

    async def load_mockups(
        self,
        context,
        *,
        source_entity_type: str,
        source_entity_id: str,
    ) -> list[dict[str, Any]]:
        model = {"ideation": Ideation, "refinement": Refinement, "spec": Spec}.get(
            source_entity_type
        )
        if model is None:
            return []
        entity = await context.get(model, source_entity_id)
        if entity is None:
            return []
        return [
            dict(item)
            for item in entity.screen_mockups or ()
            if isinstance(item, dict)
        ]


__all__ = ["TestSqlAlchemyEffectiveResourcePersistence"]
