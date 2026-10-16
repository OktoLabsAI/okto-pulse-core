"""Validation and coverage must admit the same qualified BR responsibility."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from okto_pulse.core.services.analytics_service import spec_coverage_summary
from okto_pulse.core.services.business_rule_coverage import inherited_business_rule_task_ids
from okto_pulse.core.services.coverage_traceability_read_model import _derived_links
from okto_pulse.core.ports.coverage_traceability import CoverageObligationType
from okto_pulse.core.services import main
from test_implementation_responsibility import split_population, card


def fixture():
    spec = SimpleNamespace(
        id="spec", board_id="board", title="Qualified BR", edition=1,
        test_scenarios=[], api_contracts=[], decisions=[],
        skip_test_coverage=False, skip_rules_coverage=False,
        **split_population(),
    )
    cards = [SimpleNamespace(**card("ui")), SimpleNamespace(**card("authorization"))]
    return spec, cards


@pytest.mark.asyncio
async def test_validation_summary_and_canonical_projection_share_inherited_scope(monkeypatch):
    spec, cards = fixture()
    before = deepcopy(vars(spec))

    async def read(*args, **kwargs):
        assert kwargs["limit"] == 5001
        return cards

    monkeypatch.setattr(main, "_application_list", read)
    await main.CardService(object()).check_rules_coverage(spec, SimpleNamespace(settings={}))
    assert inherited_business_rule_task_ids(spec, cards) == {"br": {"authorization"}}
    assert spec_coverage_summary(spec, cards=cards)["br_task_linkage_pct"] == 100
    assert _derived_links(spec, cards)[(CoverageObligationType.BUSINESS_RULE, 0)] == {"authorization"}
    assert vars(spec) == before  # No materialized links and no delivery proof.


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["cancelled", "archived", "foreign_board", "foreign_spec", "test", "missing", "ambiguous", "unqualified", "truncated"])
async def test_invalid_inheritance_never_satisfies_validation_or_coverage(monkeypatch, damage):
    spec, cards = fixture()
    owner = cards[1]
    if damage == "cancelled": owner.status = "cancelled"
    elif damage == "archived": owner.archived = True
    elif damage == "foreign_board": owner.board_id = "other"
    elif damage == "foreign_spec": owner.spec_id = "other"
    elif damage == "test": owner.card_type = "test"
    elif damage == "missing": cards.pop()
    elif damage == "truncated": cards = cards + [SimpleNamespace(**card(str(i))) for i in range(4999)]
    elif damage == "unqualified": spec.functional_requirements[0].pop("verification")
    elif damage == "ambiguous":
        spec.functional_requirements[0]["implementation_plan"]["contributions"][0].update(
            scope="whole_requirement", criterion_ids=[],
        )

    async def read(*args, **kwargs): return cards
    monkeypatch.setattr(main, "_application_list", read)
    with pytest.raises(ValueError, match="complete inherited responsibility"):
        await main.CardService(object()).check_rules_coverage(spec, SimpleNamespace(settings={}))
    assert inherited_business_rule_task_ids(spec, cards) == {}
    assert spec_coverage_summary(spec, cards=cards)["br_task_linkage_pct"] == 0
    assert (CoverageObligationType.BUSINESS_RULE, 0) not in _derived_links(spec, cards)


def test_missing_card_population_does_not_invent_inherited_credit():
    spec, _ = fixture()
    assert inherited_business_rule_task_ids(spec, None) == {}
    assert spec_coverage_summary(spec)["br_task_linkage_pct"] == 0


@pytest.mark.asyncio
async def test_direct_links_and_explicit_skip_keep_their_existing_contract(monkeypatch):
    spec, cards = fixture()
    spec.business_rules[0]["linked_task_ids"] = ["authorization"]
    async def unexpected(*args, **kwargs): raise AssertionError("No inherited population needed")
    monkeypatch.setattr(main, "_application_list", unexpected)
    await main.CardService(object()).check_rules_coverage(spec, SimpleNamespace(settings={}))
    assert spec_coverage_summary(spec, cards=cards)["br_task_linkage_pct"] == 100
    spec.business_rules[0]["linked_task_ids"] = []
    await main.CardService(object()).check_rules_coverage(spec, SimpleNamespace(settings={"skip_rules_coverage_global":True}))
