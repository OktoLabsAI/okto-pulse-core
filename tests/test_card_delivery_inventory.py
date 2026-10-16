"""Current Card allocation; no title-only inventory API remains."""

from test_effective_delivery_inventory import inventory
from test_implementation_responsibility import card


def test_unlinked_card_receives_one_current_scope_obligation():
    result, _ = inventory(cards=[card(), card("loose")])
    obligations = result.card_obligations("loose")
    assert len(obligations) == 1
    assert obligations[0].binding.obligation_ref == "card:loose"
    assert len(obligations[0].binding.semantic_sha256) == 64
    assert obligations[0].contributions[0].scope == "whole_card"


def test_cards_see_only_their_allocated_obligations():
    result, _ = inventory(cards=[card(), card("loose")])
    owner_refs = {row.binding.obligation_ref for row in result.card_obligations("owner")}
    loose_refs = {row.binding.obligation_ref for row in result.card_obligations("loose")}
    assert owner_refs
    assert owner_refs.isdisjoint(loose_refs)
    assert loose_refs == {"card:loose"}
