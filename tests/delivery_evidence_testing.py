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
    from okto_pulse.core.services import delivery_evidence
    from sqlalchemy import select
    from sqlalchemy_test_models import Card

    from test_delivery_obligation_assessment import snapshot as complete_snapshot
    from test_effective_delivery_coverage import planned_scenarios

    snapshot = complete_snapshot()
    context = snapshot.effective_context
    handle = SimpleNamespace(load_snapshot=None)

    def factory(session):
        async def load(scope, *, prospective_card_id=None, prospective_report=None):
            assert prospective_report is None or "delivery_manifest" not in prospective_report
            cards = (await session.execute(
                select(Card.id).where(Card.board_id == scope.board_id,
                                      Card.spec_id == scope.spec_id)
            )).scalars().all()
            card_ids = tuple(sorted(set(cards) | ({prospective_card_id} if prospective_card_id else set())))
            contribution_template = context.inventory.rows[0].contributions[0]
            implementation_template = context.implementations[0]
            test_template = context.tests[0]
            contributions = tuple(replace(contribution_template, card_id=card_id)
                                  for card_id in card_ids)
            inventory = replace(context.inventory, rows=(
                replace(context.inventory.rows[0], contributions=contributions),
            ))
            implementations = tuple(replace(implementation_template, fact=replace(
                implementation_template.fact, id=card_id, scope=scope, card_id=card_id,
            )) for card_id in card_ids)
            tests = (replace(test_template, fact=replace(
                test_template.fact, id="test-all", scope=scope,
                scenario_id="scenario-all", receipt_id="receipt-all",
                verified_implementation_ids=card_ids,
            )),)
            run_template = context.native_verification.runs[0]
            native_verification = replace(context.native_verification, runs=tuple(
                replace(run_template, scope=scope, scenario_id=row.fact.scenario_id,
                        receipt_id=row.fact.receipt_id)
                for row in tests
            ))
            return replace(
                snapshot, scope=scope,
                implementations=tuple(row.fact for row in implementations),
                tests=tuple(row.fact for row in tests),
                effective_context=replace(
                    context, inventory=inventory, implementations=implementations,
                    tests=tests, required_scenarios=planned_scenarios(tests),
                    native_verification=native_verification,
                ),
            )

        return SimpleNamespace(load_snapshot=AsyncMock(side_effect=load))

    stores = {}

    def get_store(session):
        key = id(session)
        if key not in stores:
            stores[key] = factory(session)
        handle.load_snapshot = stores[key].load_snapshot
        return stores[key]

    # The gate reads the canonical Spec snapshot for a Card's contribution.
    monkeypatch.setattr(delivery_evidence, "delivery_store", get_store)
    # Expose the actual canonical read for the wiring assertion.
    return handle
