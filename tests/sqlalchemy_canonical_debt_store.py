"""Test-only SQLAlchemy canonical-debt store."""

from collections.abc import Sequence

from sqlalchemy import and_, func, select

from sqlalchemy_test_models import CanonicalDebt
from okto_pulse.core.ports.canonical_debt import CanonicalDebtRecord
from okto_pulse.core.domain.code_traceability_kg import (
    CODE_TRACEABILITY_KG_SUBTYPES,
)


def _record(row: CanonicalDebt) -> CanonicalDebtRecord:
    values = {
        name: getattr(row, name)
        for name in CanonicalDebtRecord.__dataclass_fields__
    }
    values["retry_count"] = int(values["retry_count"] or 0)
    return CanonicalDebtRecord(**values)


def _apply(row: CanonicalDebt, record: CanonicalDebtRecord) -> None:
    for field_name in CanonicalDebtRecord.__dataclass_fields__:
        setattr(row, field_name, getattr(record, field_name))


class TestSqlAlchemyCanonicalDebtStore:
    __test__ = False

    async def counts_by_state(
        self,
        context,
        *,
        board_id: str,
        include_code_traceability: bool = True,
    ):  # noqa: ANN001, ANN201
        predicates = [CanonicalDebt.board_id == board_id]
        if not include_code_traceability:
            predicates.append(
                CanonicalDebt.artifact_type.not_in(
                    CODE_TRACEABILITY_KG_SUBTYPES
                )
            )
        rows = (
            await context.execute(
                select(CanonicalDebt.canonical_state, func.count())
                .where(*predicates)
                .group_by(CanonicalDebt.canonical_state)
            )
        ).all()
        return {str(state): int(count) for state, count in rows}

    async def list_records(
        self,
        context,
        *,
        board_id,
        artifact_type,
        state,
        limit,
        offset,
        include_code_traceability=True,
    ):  # noqa: ANN001, ANN201
        predicates = [CanonicalDebt.board_id == board_id]
        if artifact_type:
            predicates.append(CanonicalDebt.artifact_type == artifact_type)
        if state:
            predicates.append(CanonicalDebt.canonical_state == state)
        if not include_code_traceability:
            predicates.append(
                CanonicalDebt.artifact_type.not_in(
                    CODE_TRACEABILITY_KG_SUBTYPES
                )
            )
        where = and_(*predicates)
        total = int(
            await context.scalar(
                select(func.count()).select_from(CanonicalDebt).where(where)
            )
            or 0
        )
        rows = (
            await context.execute(
                select(CanonicalDebt)
                .where(where)
                .order_by(CanonicalDebt.updated_at.desc(), CanonicalDebt.id.asc())
                .offset(offset)
                .limit(limit)
            )
        ).scalars().all()
        return total, tuple(_record(row) for row in rows)

    async def find_by_identity(
        self,
        context,
        *,
        board_id,
        artifact_type,
        artifact_id,
        target_status,
        content_hash,
    ):  # noqa: ANN001, ANN201
        row = (
            await context.execute(
                select(CanonicalDebt).where(
                    CanonicalDebt.board_id == board_id,
                    CanonicalDebt.artifact_type == artifact_type,
                    CanonicalDebt.artifact_id == artifact_id,
                    CanonicalDebt.target_status == target_status,
                    CanonicalDebt.content_hash == content_hash,
                )
            )
        ).scalar_one_or_none()
        return _record(row) if row is not None else None

    async def get(
        self,
        context,
        *,
        debt_id: str,
        include_code_traceability: bool = True,
    ):  # noqa: ANN001, ANN201
        if include_code_traceability:
            row = await context.get(CanonicalDebt, debt_id)
        else:
            row = (
                await context.execute(
                    select(CanonicalDebt).where(
                        CanonicalDebt.id == debt_id,
                        CanonicalDebt.artifact_type.not_in(
                            CODE_TRACEABILITY_KG_SUBTYPES
                        ),
                    )
                )
            ).scalar_one_or_none()
        return _record(row) if row is not None else None

    async def find_open_by_evidence(
        self,
        context,
        *,
        board_id,
        source_ref,
        content_hash,
        open_states: Sequence[str],
    ):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(CanonicalDebt).where(
                    CanonicalDebt.board_id == board_id,
                    CanonicalDebt.source_ref == source_ref,
                    CanonicalDebt.content_hash == content_hash,
                    CanonicalDebt.canonical_state.in_(open_states),
                )
            )
        ).scalars().all()
        return tuple(_record(row) for row in rows)

    async def find_open_for_artifact(
        self,
        context,
        *,
        board_id,
        artifact_type,
        artifact_id,
        target_status,
        open_states: Sequence[str],
    ):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(CanonicalDebt).where(
                    CanonicalDebt.board_id == board_id,
                    CanonicalDebt.artifact_type == artifact_type,
                    CanonicalDebt.artifact_id == artifact_id,
                    CanonicalDebt.target_status == target_status,
                    CanonicalDebt.canonical_state.in_(open_states),
                )
            )
        ).scalars().all()
        return tuple(_record(row) for row in rows)

    async def save(
        self, context, record, *, commit: bool = False
    ):  # noqa: ANN001, ANN201
        row = await context.get(CanonicalDebt, record.id)
        if row is None:
            row = CanonicalDebt(id=record.id)
            context.add(row)
        _apply(row, record)
        await context.flush()
        if commit:
            await context.commit()
        return record


__all__ = ["TestSqlAlchemyCanonicalDebtStore"]
