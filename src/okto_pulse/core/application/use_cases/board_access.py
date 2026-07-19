"""Shared board-access preflight for transport-neutral use cases."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


async def load_accessible_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    allowed_share_permissions: Collection[str] | None = None,
) -> Any | None:
    """Return the board only when ``actor`` owns it or has a board share.

    ``None`` deliberately covers both a missing board and a denied board so callers
    can preserve non-enumerable not-found envelopes at their inbound boundary.
    """
    if actor.board_id is not None and actor.board_id != board_id:
        return None

    board = await uow.boards.get(board_id)
    if board is None:
        return None
    # Community actors that predate realm propagation are explicitly local.
    # Likewise a legacy board with no stored realm is local-only compatibility,
    # never a wildcard into tenant realms.
    actor_realm = actor.realm_id or LOCAL_REALM_ID
    board_realm = getattr(board, "realm_id", None) or LOCAL_REALM_ID
    if board_realm != actor_realm:
        return None
    # MCP authentication resolves an agent's board grant before constructing a
    # board-bound ActorContext.  Agent grants do not live in the user-share
    # repository, so repeating the REST owner/share lookup would reject every
    # legitimate MCP call.  The binding remains fail-closed above and a missing
    # board still has the same non-enumerable outcome.
    if actor.source == "mcp" and actor.board_id == board_id:
        return board
    if getattr(board, "owner_id", None) == actor.actor_id:
        return board

    permission = await uow.services.shares.get_user_permission(
        board_id,
        actor.actor_id,
    )
    if permission is None:
        return None
    if (
        allowed_share_permissions is not None
        and permission not in allowed_share_permissions
    ):
        return None
    return board


async def load_accessible_card(
    uow: PulseUnitOfWork,
    card_id: str,
    actor: ActorContext,
    *,
    expected_board_id: str | None = None,
    allowed_share_permissions: Collection[str] | None = None,
) -> Any | None:
    """Resolve ``card -> board -> actor`` without exposing global child IDs."""

    card = await uow.services.cards.get_card(card_id)
    if card is None:
        return None
    board_id = getattr(card, "board_id", None)
    if board_id is None or (
        expected_board_id is not None and board_id != expected_board_id
    ):
        return None
    if await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=allowed_share_permissions,
    ) is None:
        return None
    return card


__all__ = ["load_accessible_board", "load_accessible_card"]
