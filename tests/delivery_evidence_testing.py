"""Explicit trusted-port fixtures for tests of unrelated lifecycle gates.

Production Community projections and receipt verification are covered by the
paired integration suite; these fixtures do not register a production bypass.
"""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock



def install_complete_delivery_port(monkeypatch):
    from okto_pulse.core.services import delivery_evidence
    from test_execution_contract import adopted_snapshot

    snapshot = adopted_snapshot()

    async def load(scope):
        implementations = tuple(replace(row, fact=replace(row.fact, scope=scope))
                                for row in snapshot.effective_context.implementations)
        tests = tuple(replace(row, fact=replace(row.fact, scope=scope))
                      for row in snapshot.effective_context.tests)
        return replace(
            snapshot,
            scope=scope,
            implementations=tuple(row.fact for row in implementations),
            tests=tuple(row.fact for row in tests),
            effective_context=replace(snapshot.effective_context,
                                      implementations=implementations, tests=tests),
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

    from test_execution_contract import adopted_snapshot

    snapshot = adopted_snapshot()
    context = snapshot.effective_context

    async def load(scope, *, prospective_report=None):
        assert prospective_report is None or "delivery_manifest" not in prospective_report
        delivery_scope = DeliveryScope(scope.board_id, scope.spec_id, scope.spec_edition)
        # A native Card projection includes its effective responsibility scope.
        contribution = replace(context.inventory.rows[0].contributions[0], card_id=scope.card_id)
        inventory = replace(context.inventory, rows=(
            replace(context.inventory.rows[0], contributions=(contribution,)),
        ))
        implementation = replace(context.implementations[0], fact=replace(
            context.implementations[0].fact, scope=delivery_scope, card_id=scope.card_id,
        ))
        return replace(
            snapshot, scope=delivery_scope, implementations=(implementation.fact,), tests=(),
            effective_context=replace(
                context, inventory=inventory, implementations=(implementation,), tests=(),
            ),
        )

    store = SimpleNamespace(load_card_snapshot=AsyncMock(side_effect=load))
    monkeypatch.setattr(delivery_evidence, "card_delivery_store", lambda _session: store)
    return store
