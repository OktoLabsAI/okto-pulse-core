"""The durable read model must preserve native evaluation semantics."""

from dataclasses import asdict, replace
from datetime import timedelta
import orjson
import pytest

from okto_pulse.core.services import persisted_delivery_readiness as projection
from okto_pulse.core.domain.delivery_assessment import aggregate_delivery_metrics
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.domain.effective_delivery_coverage import EvaluatedDeliverySnapshot
from okto_pulse.core.services.persisted_delivery_readiness import build_readiness_generation
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_readiness_access import batch
from test_delivery_evidence_domain import BINDING


def _source(value=None):
    source = batch(value or snapshot())
    return replace(source, cards=tuple(replace(card,
        scenario_ids=('scenario-ui', 'scenario-authorization')) if card.card_type == 'test' else card
        for card in source.cards))


def test_generation_matches_canonical_spec_and_each_card():
    source = _source()
    projected = build_readiness_generation(source, captured_revision=7)
    canonical = EvaluatedDeliverySnapshot(source.facts)
    canonical_contexts = EvaluatedDeliveryReadiness(source, actor_id=source.access.actor_id)
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
        assert row.context == canonical_contexts.context(card.id if card else None).model_dump(mode='json')
    assert by_card['test-card-1'].implementation.percent is None
    assert projected.captured_revision == 7
    assert projected.manifest_sha256 == build_readiness_generation(source, captured_revision=7).manifest_sha256
    assert projected.manifest_sha256 != build_readiness_generation(source, captured_revision=8).manifest_sha256


def test_stored_canonical_rows_have_the_generation_manifest():
    generation = build_readiness_generation(_source(), captured_revision=7)
    def encoded(payload):
        return orjson.dumps(payload, option=orjson.OPT_SORT_KEYS).decode()
    rows = ([(0, row.card_id or '', '', '', '', encoded(row.payload()))
        for row in generation.summaries]
        + [(1, row.card_id or '', '', '', '', encoded(row.payload))
            for row in generation.diagnostics]
        + [(2, row.card_id or '', row.kind, row.ref, row.semantic_sha256,
            encoded(asdict(row))) for row in generation.units])
    rows.sort(key=lambda row: row[:5])
    args = dict(board_id=generation.board_id, spec_id=generation.spec_id,
        edition=generation.spec_edition, captured_revision=generation.captured_revision,
        source_snapshot_id=generation.source_snapshot_id,
        evaluator_version=generation.evaluator_version)
    assert projection.readiness_manifest_digest_stored(**args, rows=rows) == generation.manifest_sha256
    changed = [*rows]
    head = next(index for index, row in enumerate(changed) if row[0] == 0)
    changed[head] = (*changed[head][:5], changed[head][5].replace('"card_id":', '"other_id":', 1))
    assert projection.readiness_manifest_digest_stored(**args, rows=changed) != generation.manifest_sha256


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


def test_incremental_generation_reuses_unchanged_card_with_exact_manifest():
    previous_source = _source()
    predecessor = build_readiness_generation(previous_source, captured_revision=7)
    changed_facts = with_facts(previous_source.facts,
        implementations=previous_source.facts.effective_context.implementations[:1])
    source = replace(previous_source, facts=changed_facts,
        snapshot_id='1' * 64,
        evaluated_at=previous_source.evaluated_at + timedelta(seconds=1))
    full = build_readiness_generation(source, captured_revision=8)
    incremental = build_readiness_generation(source, captured_revision=8,
        predecessor=predecessor,
        affected_card_ids=frozenset({None, 'authorization', 'test-card-1'}))
    assert incremental.summaries == full.summaries
    assert incremental.diagnostics == full.diagnostics
    assert incremental.units == full.units
    assert incremental.manifest_sha256 == full.manifest_sha256


def test_incremental_generation_refuses_unverified_or_nonadjacent_predecessor():
    source = _source()
    predecessor = build_readiness_generation(source, captured_revision=7)
    next_source = replace(source, snapshot_id='1' * 64,
        evaluated_at=source.evaluated_at + timedelta(seconds=1))
    affected = frozenset({None, *(card.id for card in source.cards)})
    with pytest.raises(ValueError, match="readiness_incremental_predecessor_ineligible"):
        build_readiness_generation(next_source, captured_revision=9,
            predecessor=predecessor, affected_card_ids=affected)
    with pytest.raises(ValueError, match="readiness_incremental_predecessor_manifest_mismatch"):
        build_readiness_generation(next_source, captured_revision=8,
            predecessor=replace(predecessor, manifest_sha256='0' * 64),
            affected_card_ids=affected)


def test_generation_checks_deadline_before_and_during_evaluation(monkeypatch):
    source = _source()
    with pytest.raises(TimeoutError, match='readiness_calculation_budget_exceeded'):
        build_readiness_generation(source, captured_revision=1, deadline=-1)

    clock = iter((0.0, 2.0))
    monkeypatch.setattr(projection, 'monotonic', lambda: next(clock))
    with pytest.raises(TimeoutError, match='readiness_calculation_budget_exceeded'):
        build_readiness_generation(source, captured_revision=1, deadline=1.0)


@pytest.mark.parametrize('variant', ['historical_proof', 'missing_verification',
    'waiver', 'historical_done_without_verification', 'unknown_verifier'])
def test_generation_units_and_percentages_follow_canonical_cases(variant):
    value = snapshot()
    if variant in {'missing_verification', 'waiver', 'historical_done_without_verification'}:
        value = with_facts(value, tests=())
    if variant == 'waiver':
        value = replace(value, waivers=(DeliveryWaiverFact('waiver', value.scope,
            BINDING, DeliveryPhase.TEST, 'Human decision', 'human', 'authority',
            True, 'risk_accepted'),))
    if variant == 'unknown_verifier':
        value = replace(value, complete=False, facts_complete=True,
            effective_context=replace(value.effective_context, admitted_methods=None))
    source = _source(value)
    if variant == 'historical_done_without_verification':
        source = replace(source, spec_status='done', cards=tuple(replace(card,
            status='done') for card in source.cards))
    projection = build_readiness_generation(source, captured_revision=11)
    native = EvaluatedDeliverySnapshot(source.facts).obligations()
    metrics = aggregate_delivery_metrics(native)
    summary = next(row for row in projection.summaries if row.card_id is None)
    assert summary.implementation.percent == metrics.implementation.percent
    assert summary.verification.percent == metrics.verification.percent
    assert summary.implementation.total == metrics.implementation.total
    assert summary.verification.total == metrics.verification.total
    units = [row for row in projection.units if row.card_id is None]
    assert [(row.ref, row.semantic_sha256, row.implementation_state,
        row.verification_state) for row in units] == [
            (row.binding.obligation_ref, row.binding.semantic_sha256,
             row.implementation.state.value, row.verification.state.value)
            for row in native.obligations]
    if variant == 'historical_proof':
        assert summary.implementation.percent == summary.verification.percent == 100
    if variant == 'historical_done_without_verification':
        assert summary.verification.percent == 0
    if variant == 'unknown_verifier':
        assert summary.implementation.percent == 100
        assert summary.verification.percent is None
