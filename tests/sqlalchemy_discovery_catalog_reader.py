"""Test-only Discovery catalog read adapter."""

from sqlalchemy import select

from sqlalchemy_test_models import (
    Board,
    BoardShare,
    DiscoveryIntent,
    DiscoverySavedSearch,
    DiscoverySearchHistory,
)
from okto_pulse.core.ports.discovery_catalog import (
    DiscoveryIntentRecord,
    DiscoverySavedSearchRecord,
    DiscoverySearchHistoryRecord,
)


def _intent(row):  # noqa: ANN001, ANN201
    return DiscoveryIntentRecord(
        **{
            name: getattr(row, name)
            for name in DiscoveryIntentRecord.__dataclass_fields__
        }
    )


class TestSqlAlchemyDiscoveryCatalogReader:
    __test__ = False

    async def list_active_intents(self, context):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(DiscoveryIntent)
                .where(DiscoveryIntent.active.is_(True))
                .order_by(DiscoveryIntent.category, DiscoveryIntent.label)
            )
        ).scalars().all()
        return tuple(_intent(row) for row in rows)

    async def list_saved_searches(
        self, context, *, board_id: str
    ):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(DiscoverySavedSearch)
                .where(DiscoverySavedSearch.board_id == board_id)
                .order_by(DiscoverySavedSearch.created_at.desc())
            )
        ).scalars().all()
        return tuple(
            DiscoverySavedSearchRecord(
                **{
                    name: getattr(row, name)
                    for name in DiscoverySavedSearchRecord.__dataclass_fields__
                }
            )
            for row in rows
        )

    async def list_search_history(
        self, context, *, board_id: str, user_id: str, limit: int
    ):  # noqa: ANN001, ANN201
        rows = (
            await context.execute(
                select(DiscoverySearchHistory)
                .where(
                    DiscoverySearchHistory.board_id == board_id,
                    DiscoverySearchHistory.user_id == user_id,
                )
                .order_by(DiscoverySearchHistory.searched_at.desc())
                .limit(limit)
            )
        ).scalars().all()
        return tuple(
            DiscoverySearchHistoryRecord(
                **{
                    name: getattr(row, name)
                    for name in DiscoverySearchHistoryRecord.__dataclass_fields__
                }
            )
            for row in rows
        )

    async def get_intent(
        self, context, *, intent_id: str
    ):  # noqa: ANN001, ANN201
        row = await context.get(DiscoveryIntent, intent_id)
        return _intent(row) if row is not None else None

    async def can_read_board(
        self, context, *, board_id: str, user_id: str
    ):  # noqa: ANN001, ANN201
        board = await context.get(Board, board_id)
        if board is None:
            return False
        if board.owner_id == user_id:
            return True
        share = (
            await context.execute(
                select(BoardShare.id).where(
                    BoardShare.board_id == board_id,
                    BoardShare.user_id == user_id,
                )
            )
        ).scalar_one_or_none()
        return share is not None


__all__ = ["TestSqlAlchemyDiscoveryCatalogReader"]
