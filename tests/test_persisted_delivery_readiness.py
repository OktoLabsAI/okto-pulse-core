"""The durable read model must preserve native evaluation semantics."""

from dataclasses import replace

from okto_pulse.core.domain.delivery_assessment import aggregate_delivery_metrics
from okto_pulse.core.domain.effective_delivery_coverage import EvaluatedDeliverySnapshot
from okto_pulse.core.services.persisted_delivery_readiness import build_readiness_generation
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_readiness_access import batch


def _source(value=None):
    source = batch(value or snapshot())
    return replace(source, cards=tuple(replace(card,
        scenario_ids=('scenario-ui', 'scenario-authorization')) if card.card_type == 'test' else card
        for card in source.cards))


def test_generation_matches_canonical_spec_and_each_card():
    source = _source()
    projected = build_readiness_generation(source, captured_revision=7)
    canonical = EvaluatedDeliverySnapshot(source.facts)
    by_card = {summary.card_id: summary for summary in projected.summaries}
    for card in (None, *source.cards):
        assessment = (canonical.obligations() if card is None else
            canonical.scenarios(card_id=card.id, scenario_ids=card.scenario_ids) if card.card_type == 'test'
            else canonical.obligations(card_id=card.id, card_type=card.card_type))
        metrics = aggregate_delivery_metrics(assessment)
        row = by_card[card.id if card else None]
        assert row.implementation.percent == metrics.implementation.percent
        assert row.verification.percent == metrics.verification.percent
        assert row.implementation.total == metrics.implementation.total
        assert row.verification.total == metrics.verification.total
    assert by_card['test-card-1'].implementation.percent is None
    assert projected.captured_revision == 7
    assert projected.manifest_sha256 == build_readiness_generation(source, captured_revision=7).manifest_sha256
    assert projected.manifest_sha256 != build_readiness_generation(source, captured_revision=8).manifest_sha256


def test_generation_preserves_independent_local_progress_and_unknowns():
    value = snapshot()
    source = _source(with_facts(value, implementations=value.effective_context.implementations[:1]))
    by_card = {row.card_id: row for row in build_readiness_generation(source,
        captured_revision=1).summaries}
    assert by_card[None].implementation.percent == 0
    assert by_card['ui'].implementation.percent == 100
    assert by_card['authorization'].implementation.percent == 0

    unavailable = replace(source, unavailable_reason_codes=('readiness_population_limit',))
    result = build_readiness_generation(unavailable, captured_revision=2)
    for row in result.summaries:
        assert row.verification.percent is None
        assert row.verification.status == 'unknown'
    assert result.units == ()
