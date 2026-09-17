"""Card-scoped obligation derivation (FR-2 of the per-task re-anchoring).

The join key is ``linked_task_ids`` on each structured spec entity; digests
must be identical to the spec inventory so the migration verdict-equivalence
gate compares like with like, and a card without links receives exactly the
``card:<id>`` fallback obligation — never vacuous coverage.
"""

from types import SimpleNamespace

from okto_pulse.core.services.delivery_evidence import (
    card_delivery_inventory,
    delivery_inventory,
)

SPEC = SimpleNamespace(
    id="spec-1",
    title="Delivery Evidence por Task",
    description=None,
    context=None,
    functional_requirements=[
        {
            "id": "fr-1",
            "title": "Ledger card-scoped",
            "linked_task_ids": ["task-1", "task-2"],
        },
        {
            "id": "fr-2",
            "title": "Obrigação isolada",
            "linked_task_ids": ["task-2"],
        },
        {
            "id": "fr-3",
            "title": "Cancelada",
            "status": "cancelled",
            "linked_task_ids": ["task-1"],
        },
    ],
    technical_requirements=[
        {"id": "tr-1", "title": "Hexagonal", "linked_task_ids": ["task-1"]},
    ],
    business_rules=[],
    acceptance_criteria=[],
    api_contracts=[],
    integration_requirements=[],
    observability_requirements=[],
    decisions=[],
)


def _refs(obligations):
    return [item.binding.obligation_ref for item in obligations]


def test_card_inventory_derives_from_links_and_skips_inactive_entities():
    card = SimpleNamespace(id="task-1", title="Card-scoped delivery ledger")
    obligations = card_delivery_inventory(SPEC, card)
    assert _refs(obligations) == ["fr:fr-1", "tr:tr-1"]


def test_card_inventory_digest_parity_with_spec_inventory():
    card = SimpleNamespace(id="task-1", title="Card-scoped delivery ledger")
    card_items = {
        item.binding.obligation_ref: item
        for item in card_delivery_inventory(SPEC, card)
    }
    spec_items = {
        item.binding.obligation_ref: item for item in delivery_inventory(SPEC)
    }
    assert set(card_items) <= set(spec_items)
    for ref, obligation in card_items.items():
        assert (
            obligation.binding.semantic_sha256
            == spec_items[ref].binding.semantic_sha256
        )
        assert obligation.title == spec_items[ref].title


def test_unlinked_card_receives_exactly_the_fallback_obligation():
    card = SimpleNamespace(id="task-9", title="Sem links")
    obligations = card_delivery_inventory(SPEC, card)
    assert _refs(obligations) == ["card:task-9"]
    assert obligations[0].title == "Sem links"
    assert len(obligations[0].binding.semantic_sha256) == 64


def test_cards_see_only_their_linked_obligations():
    first = card_delivery_inventory(SPEC, SimpleNamespace(id="task-1", title="A"))
    second = card_delivery_inventory(SPEC, SimpleNamespace(id="task-2", title="B"))
    assert _refs(second) == ["fr:fr-1", "fr:fr-2"]
    assert set(_refs(first)) & set(_refs(second)) == {"fr:fr-1"}
