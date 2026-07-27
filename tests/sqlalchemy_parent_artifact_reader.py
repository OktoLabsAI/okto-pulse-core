"""Test-only SQLAlchemy parent artifact lookup adapter."""

from sqlalchemy import select

from sqlalchemy_test_models import Card, Spec, Sprint
from okto_pulse.core.ports.parent_artifact import ParentArtifactRecord


class TestSqlAlchemyParentArtifactReader:
    __test__ = False

    async def read_many(
        self,
        context,
        *,
        artifact_type: str,
        ids: frozenset[str],
    ) -> tuple[ParentArtifactRecord, ...]:
        models = {"spec": Spec, "sprint": Sprint, "card": Card}
        model = models[artifact_type]
        result = await context.execute(
            select(model.id, model.title, model.status).where(model.id.in_(ids))
        )
        return tuple(
            ParentArtifactRecord(
                artifact_type=artifact_type,
                id=str(row_id),
                title=str(row_title or ""),
                status=str(getattr(row_status, "value", row_status)),
            )
            for row_id, row_title, row_status in result.all()
        )


__all__ = ["TestSqlAlchemyParentArtifactReader"]
