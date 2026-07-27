"""Test-only persistence adapter for legacy materialization integration tests."""

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.application.spec_materialization import (
    materialize_legacy_fr_ac_board as materialize_with_store,
)
from sqlalchemy_test_models import Spec


class TestSqlAlchemySpecMaterializationStore:
    __test__ = False

    def __init__(self, session) -> None:  # noqa: ANN001
        self._session = session

    async def list_specs(self, board_id: str):  # noqa: ANN201
        result = await self._session.execute(
            select(Spec).where(Spec.board_id == board_id)
        )
        return list(result.scalars().all())

    async def apply(self, plan) -> None:  # noqa: ANN001
        for change in plan.changes:
            for field_name, canonical in change.fields:
                setattr(change.spec, field_name, canonical)
                flag_modified(change.spec, field_name)
        await self._session.commit()


async def materialize_legacy_fr_ac_board(
    session,
    board_id: str,
    dry_run: bool = True,
):  # noqa: ANN001, ANN201
    return await materialize_with_store(
        TestSqlAlchemySpecMaterializationStore(session),
        board_id,
        dry_run=dry_run,
    )


__all__ = [
    "TestSqlAlchemySpecMaterializationStore",
    "materialize_legacy_fr_ac_board",
]
