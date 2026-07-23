"""Fail-closed guard for legacy physical Card knowledge writes.

Selective Knowledge propagation v2 is authoritative once its target scope is
active.  Legacy writers must therefore consult the edition-owned propagation
port before touching ``Card.knowledge_bases``.  The guard remains in Core and
depends only on the public propagation contract.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.knowledge_selection import KnowledgeTargetType
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationPort,
    KnowledgePropagationPortError,
    KnowledgePropagationScope,
    KnowledgeScopeLookup,
    KnowledgeTargetKey,
    get_knowledge_propagation_port,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)


LEGACY_KNOWLEDGE_WRITE_FORBIDDEN = (
    "knowledge_propagation_legacy_write_forbidden"
)


def legacy_knowledge_write_forbidden_error(
    *,
    board_id: str,
    card_id: str,
) -> KnowledgePropagationServiceError:
    """Build the stable transport-neutral rejection for a v2 Card target."""

    return KnowledgePropagationServiceError(
        LEGACY_KNOWLEDGE_WRITE_FORBIDDEN,
        (
            "legacy physical knowledge writes are forbidden while Knowledge "
            "propagation v2 is active for the card"
        ),
        details={
            "board_id": board_id,
            "target_type": KnowledgeTargetType.CARD.value,
            "target_id": card_id,
        },
    )


async def load_card_knowledge_scope(
    context: Any,
    *,
    board_id: str,
    card_id: str,
    port: KnowledgePropagationPort | None = None,
) -> KnowledgePropagationScope:
    """Load and validate the authoritative Card scope before any legacy write."""

    target = KnowledgeTargetKey(
        board_id=board_id,
        target_type=KnowledgeTargetType.CARD,
        target_id=card_id,
    )
    try:
        resolved_port = port if port is not None else get_knowledge_propagation_port()
    except RuntimeError as exc:
        if str(exc) != "knowledge_propagation_port_not_configured":
            raise
        raise KnowledgePropagationServiceError(
            "knowledge_propagation_port_not_configured",
            "the Knowledge propagation persistence port is not configured",
            details={"target": target.to_dict()},
        ) from exc
    scope = await resolved_port.load_scope(
        context,
        KnowledgeScopeLookup(target=target),
    )
    if scope.target != target:
        raise KnowledgePropagationPortError(
            "knowledge_propagation_scope_target_mismatch",
            "the loaded Knowledge propagation scope does not match the card target",
            details={
                "expected": target.to_dict(),
                "actual": scope.target.to_dict(),
            },
        )
    return scope


async def require_legacy_card_knowledge_write_allowed(
    context: Any,
    *,
    board_id: str,
    card_id: str,
    port: KnowledgePropagationPort | None = None,
) -> KnowledgePropagationScope:
    """Reject a physical legacy KB write when the target has activated v2."""

    scope = await load_card_knowledge_scope(
        context,
        board_id=board_id,
        card_id=card_id,
        port=port,
    )
    if scope.v2_active:
        raise legacy_knowledge_write_forbidden_error(
            board_id=board_id,
            card_id=card_id,
        )
    return scope


__all__ = [
    "LEGACY_KNOWLEDGE_WRITE_FORBIDDEN",
    "legacy_knowledge_write_forbidden_error",
    "load_card_knowledge_scope",
    "require_legacy_card_knowledge_write_allowed",
]
