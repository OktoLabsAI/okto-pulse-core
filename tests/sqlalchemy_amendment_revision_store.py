"""Test-only SQLAlchemy amendment revision store."""

from sqlalchemy import select

from sqlalchemy_test_models import ActivityLog, AmendmentHotfixRevision
from okto_pulse.core.ports.amendment_revision import (
    AmendmentAuditRecord,
    AmendmentRevisionRecord,
)


def _record(row):  # noqa: ANN001, ANN201
    values = {
        name: getattr(row, name)
        for name in AmendmentRevisionRecord.__dataclass_fields__
    }
    for name in (
        "origin_task_ids",
        "affected_task_ids",
        "regression_scenario_ids",
        "regression_test_task_ids",
        "automated_regression_refs",
    ):
        values[name] = list(values[name] or [])
    if values["validation_metadata"]:
        values["validation_metadata"] = dict(values["validation_metadata"])
    return AmendmentRevisionRecord(**values)


class TestSqlAlchemyAmendmentRevisionStore:
    __test__ = False

    async def get(self, context, *, amendment_id: str):  # noqa: ANN001, ANN201
        row = await context.get(AmendmentHotfixRevision, amendment_id)
        return _record(row) if row is not None else None

    async def list_for_bug(
        self,
        context,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
    ):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(AmendmentHotfixRevision).where(
                    AmendmentHotfixRevision.board_id == board_id,
                    AmendmentHotfixRevision.original_spec_id == original_spec_id,
                    AmendmentHotfixRevision.origin_bug_id == origin_bug_id,
                )
            )
        ).scalars().all()
        return tuple(_record(row) for row in rows)

    async def save(
        self,
        context,
        record,
        *,
        audit: AmendmentAuditRecord,
    ):  # noqa: ANN001, ANN201
        row = await context.get(AmendmentHotfixRevision, record.id)
        if row is None:
            row = AmendmentHotfixRevision(id=record.id)
            context.add(row)
        for field_name in AmendmentRevisionRecord.__dataclass_fields__:
            value = getattr(record, field_name)
            if isinstance(value, list):
                value = list(value)
            elif isinstance(value, dict):
                value = dict(value)
            setattr(row, field_name, value)
        context.add(
            ActivityLog(
                board_id=record.board_id,
                card_id=record.origin_bug_id,
                action=audit.action,
                actor_type="agent",
                actor_id=audit.actor,
                actor_name=audit.actor,
                details=dict(audit.details),
            )
        )
        await context.flush()
        return record


__all__ = ["TestSqlAlchemyAmendmentRevisionStore"]
