"""Explicit trusted-port fixtures for tests of unrelated lifecycle gates.

Production Community projections and receipt verification are covered by the
paired integration suite; these fixtures do not register a production bypass.
"""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

from test_delivery_evidence_domain import SNAPSHOT


def install_complete_delivery_port(monkeypatch):
    from okto_pulse.core.services import delivery_evidence

    async def load(scope):
        return replace(
            SNAPSHOT,
            scope=scope,
            implementations=tuple(
                replace(i, scope=scope) for i in SNAPSHOT.implementations
            ),
            tests=tuple(replace(t, scope=scope) for t in SNAPSHOT.tests),
        )

    store = SimpleNamespace(
        load_snapshot=AsyncMock(side_effect=load), lock_scope=AsyncMock()
    )
    monkeypatch.setattr(delivery_evidence, "delivery_store", lambda _session: store)
    return store


def install_complete_card_delivery_port(monkeypatch):
    """Provide accepted Card proof explicitly for unrelated lifecycle tests."""
    from okto_pulse.core.domain.delivery_evidence import DeliveryScope
    from okto_pulse.core.services import delivery_evidence

    async def load(scope, *, prospective_report=None):
        assert prospective_report is None or "delivery_manifest" not in prospective_report
        delivery_scope = DeliveryScope(scope.board_id, scope.spec_id, scope.spec_edition)
        return replace(SNAPSHOT, scope=delivery_scope, implementations=tuple(
            replace(row, scope=delivery_scope, card_id=scope.card_id) for row in SNAPSHOT.implementations
        ), tests=())

    store = SimpleNamespace(load_card_snapshot=AsyncMock(side_effect=load))
    monkeypatch.setattr(delivery_evidence, "card_delivery_store", lambda _session: store)
    return store
