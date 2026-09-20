"""Card completion may ignore test phase, never unknown structural coverage."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.delivery_evidence import DeliveryContribution
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
from okto_pulse.core.services import delivery_evidence as service
from test_delivery_evidence_domain import BINDING, SNAPSHOT
from test_effective_delivery_coverage import case


def ready_snapshot():
    inventory, implementations, _ = case()
    inventory = replace(inventory, rows=(replace(inventory.rows[0], contributions=inventory.rows[0].contributions[:1]),))
    scoped = replace(implementations[0], fact=replace(implementations[0].fact, card_status="in_progress"))
    return replace(SNAPSHOT, implementations=(scoped.fact,), tests=(),
        effective_context=EffectiveDeliveryContext(inventory, (scoped,), (), frozenset({"automated_test"})))


async def require(snapshot, monkeypatch, *, mode="blocking"):
    monkeypatch.setattr(service, "card_delivery_store", lambda _: SimpleNamespace(load_card_snapshot=AsyncMock(return_value=snapshot)))
    card = SimpleNamespace(id="ui", board_id="board", spec_id="spec", card_type="normal")
    spec = SimpleNamespace(id="spec", edition=2)
    await service.require_card_delivery(None, card, spec, board=SimpleNamespace(settings={"delivery_evidence_gate": mode}))


@pytest.mark.asyncio
async def test_current_complete_implementation_can_finish_before_done_or_passing_tests(monkeypatch):
    await require(ready_snapshot(), monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["resolution_limit", "ambiguous_identity", "missing_context", "unknown_method_registry"])
async def test_structural_unknown_cannot_be_mistaken_for_no_missing_obligations(monkeypatch, mutation):
    snapshot = ready_snapshot()
    context = snapshot.effective_context
    if mutation == "resolution_limit":
        implementations = tuple(replace(context.implementations[0], fact=replace(context.implementations[0].fact, id=str(i))) for i in range(10001))
        snapshot = replace(snapshot, implementations=tuple(row.fact for row in implementations),
                           effective_context=replace(context, implementations=implementations))
    elif mutation == "ambiguous_identity":
        snapshot = replace(snapshot, implementations=snapshot.implementations * 2,
                           effective_context=replace(context, implementations=context.implementations * 2))
    elif mutation == "missing_context":
        snapshot = replace(snapshot, effective_context=False)
    else:
        snapshot = replace(snapshot, effective_context=replace(context, admitted_methods=None))
    with pytest.raises(ValueError, match="delivery_evidence_incomplete"):
        await require(snapshot, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["partial", "scope_changed", "proof_stale"])
async def test_known_but_unready_contribution_still_blocks_card(monkeypatch, mutation):
    snapshot = ready_snapshot()
    context = snapshot.effective_context
    scoped = context.implementations[0]
    if mutation == "partial":
        scoped = replace(scoped, fact=replace(scoped.fact, contributions=(DeliveryContribution(BINDING, "partial"),)))
    elif mutation == "scope_changed":
        scoped = replace(scoped, scopes=(replace(scoped.scopes[0], scope_sha256="e" * 64),))
    else:
        scoped = replace(scoped, fact=replace(scoped.fact, current_accepted_execution=False))
    snapshot = replace(snapshot, implementations=(scoped.fact,), effective_context=replace(context, implementations=(scoped,)))
    with pytest.raises(ValueError, match="delivery_evidence_incomplete"):
        await require(snapshot, monkeypatch)


@pytest.mark.asyncio
async def test_existing_advisory_policy_is_not_reinterpreted_as_credit(monkeypatch):
    await require(replace(ready_snapshot(), complete=False, effective_context=False), monkeypatch, mode="advisory")
