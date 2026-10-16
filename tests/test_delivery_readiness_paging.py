"""P05 response paging does not page proof, lose detail or manufacture scores."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_diagnostics import build_diagnostic_graph, cause_identity
from okto_pulse.core.models.delivery_readiness import (
    DeliveryReadinessContext, DeliveryReadinessDetail, ReadinessEvidenceRef, ReadinessFact,
    ReadinessPinpoint, ReadinessReadQuery,
)
from okto_pulse.core.ports.delivery_readiness import ReadinessReadError
from okto_pulse.core.services.delivery_readiness import PreparedDeliveryReadiness, serialize_readiness_response
from okto_pulse.core.services.delivery_readiness_diagnostics import DiagnosticDetail, NativeDeliveryDiagnostics
from test_delivery_readiness_diagnostics import SCOPE, stale, scenario, unit
from test_delivery_readiness_contracts import metric, pinpoint


class Cursor:
    """Port double: actual HMAC and cross-process tests live in Community."""
    def __init__(self):
        self.tokens = {}

    def encode(self, binding, offset):
        key = f'token-{len(self.tokens)}'
        self.tokens[key] = (binding, offset)
        return key

    def decode(self, token, binding):
        expected, offset = self.tokens[token]
        if expected != binding:
            raise ReadinessReadError('readiness_cursor_stale')
        return offset


def context(graph, **changes):
    values = dict(scope=dict(board_id='board', spec_id='spec', card_id=None, card_type=None,
        spec_edition=2, spec_version=3, card_version=None), snapshot_id='a' * 64,
        evaluated_at=datetime(2026, 10, 14, tzinfo=timezone.utc), scope_complete=True, evaluation_complete=True,
        implementation=metric(), verification=metric(), delivery_state='verification_pending', proof_complete=False,
        delivery_gate=dict(status='blocked', mode='blocking', exception_applied=False, exception_refs=[]),
        lifecycle=dict(stored_status='in_progress', check_status='not_evaluated',
            other_blocker_count=None, qualification_is_current=None),
        summary=dict(message_code='pending', message='Pending native evidence', unresolved_units=2,
            root_causes=len(graph.causes), lifecycle_pending=None, exceptions=0))
    return DeliveryReadinessContext.model_validate(values | changes)


def diagnostics(count=1, *, text='', units=1):
    graph = build_diagnostic_graph(SCOPE, tuple(stale(f'execution-{i}',
        units=tuple(unit(f'fr-{i}-{j}') for j in range(units))) for i in range(count)))
    details = tuple(DiagnosticDetail(c.id, (), (ReadinessFact(text=text),) if text else (),
        (), (), (), ()) for c in graph.causes)
    return NativeDeliveryDiagnostics(graph, details, SCOPE)


def pages(prepared, query):
    result = []
    while True:
        response = prepared.page(query, actor_id='actor')
        result.append(response)
        if not response.page.has_more:
            return result
        query = query.model_copy(update={'cursor': response.page.next_cursor})


def test_S06_paging_many_causes_preserves_whole_projection_and_nulls(monkeypatch):
    data = diagnostics(47)
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    def forbidden(*args, **kwargs):
        pytest.fail('paging must not reevaluate proof')
    monkeypatch.setattr('okto_pulse.core.domain.effective_delivery_coverage.evaluate_effective_facts', forbidden)
    result = pages(prepared, ReadinessReadQuery(board_id='board', spec_id='spec', limit=7))
    assert [r.page.returned for r in result] == [7, 7, 7, 7, 7, 7, 5]
    assert [i.id for r in result for i in r.issues] == [c.id for c in data.graph.causes]
    for response in result:
        assert response.page.total == 47
        assert response.implementation.percent == response.verification.percent == 75
        assert response.scope_complete and response.evaluation_complete
        assert b'"qualification_is_current":null' in serialize_readiness_response(response)
        assert len(serialize_readiness_response(response)) <= 32768


def test_S07_all_details_remain_recoverable_beyond_each_summary_array():
    data = diagnostics(units=250)
    old = data.details[0]
    detail = replace(old,
        expected=tuple(ReadinessFact(criterion_id=f'ac-{i}', text='expected') for i in range(32)),
        observed=tuple(ReadinessFact(observation_id=f'obs-{i}', text='observed') for i in range(35)),
        evidence_refs=tuple(ReadinessEvidenceRef(kind='receipt', id=f'receipt-{i}', source_ref=None,
            revision=None) for i in range(45)),
        pinpoints=tuple(ReadinessPinpoint.model_validate(pinpoint() | {'entity_id': f'ac-{i}'}) for i in range(27)))
    data = replace(data, details=(detail,))
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    summary = prepared.page(ReadinessReadQuery(board_id='board', spec_id='spec'), actor_id='actor')
    issue = summary.issues[0]
    assert issue.affected_unit_count == 250 and len(issue.affected_units) == 10
    assert issue.expected_count == 32 and issue.observed_count == 35
    assert summary.page.response_truncated and not summary.page.has_more
    result = pages(prepared, ReadinessReadQuery.model_validate(issue.detail_query.model_dump()))
    entries = [e for response in result for e in response.entries]
    assert len({e.id for e in entries}) == len(entries) == result[0].page.total
    assert sum(e.type == 'affected_unit' for e in entries) == 250
    for field in ('expected', 'observed', 'evidence_refs', 'pinpoints'):
        assert {e.value.model_dump_json() for e in entries if e.section == field} == {
            v.model_dump_json() for v in getattr(detail, field)}
    for response in result:
        assert response.entry_counts == result[0].entry_counts
        assert response.implementation == summary.implementation
        assert response.verification == summary.verification
        assert response.page.returned <= 20 and len(serialize_readiness_response(response)) <= 131072
        assert DeliveryReadinessDetail.model_validate_json(serialize_readiness_response(response)) == response


def test_summary_and_detail_obey_real_utf8_byte_budgets():
    data = diagnostics(24, text='á🧪' * 800)
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    result = pages(prepared, ReadinessReadQuery(board_id='board', spec_id='spec'))
    assert result[0].page.returned < 20
    assert sum(r.page.returned for r in result) == 24
    assert all(len(serialize_readiness_response(r)) <= 32768 for r in result)
    large = diagnostics()
    large = replace(large, details=(replace(large.details[0], observed=tuple(
        ReadinessFact(observation_id=f'obs-{i}', text='á🧪' * 6000) for i in range(8))),))
    prepared = PreparedDeliveryReadiness(context(large.graph), large, cursor=Cursor())
    result = pages(prepared, ReadinessReadQuery(board_id='board', spec_id='spec',
        snapshot_id='a' * 64, issue_id=large.graph.causes[0].id))
    assert len(result) > 1 and any(r.page.returned < 20 and r.page.has_more for r in result)
    assert all(len(serialize_readiness_response(r)) <= 131072 for r in result)
    assert sum(e.section == 'observed' for r in result for e in r.entries) == 8


@pytest.mark.parametrize('detail', [False, True])
def test_single_unrepresentable_item_is_explicit_limit_error_with_authorized_identity(detail):
    data = diagnostics(text='🧪' * 40000)
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    query = ReadinessReadQuery(board_id='board', spec_id='spec',
        **({'issue_id': data.graph.causes[0].id, 'snapshot_id': 'a' * 64} if detail else {}))
    with pytest.raises(ReadinessReadError) as error:
        # Earlier small detail entries may fit. Continue until the huge fact.
        pages(prepared, query)
    assert error.value.code == 'readiness_limit_exceeded'
    assert error.value.identity == data.graph.causes[0].id


def test_final_page_can_fit_when_one_item_plus_cursor_does_not(monkeypatch):
    import okto_pulse.core.services.delivery_readiness as service
    data = diagnostics(2)
    class LargeCursor(Cursor):
        def encode(self, binding, offset):
            return 'x' * 8000
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=LargeCursor())
    query = ReadinessReadQuery(board_id='board', spec_id='spec')
    complete = prepared.page(query, actor_id='actor')
    monkeypatch.setattr(service, 'SUMMARY_MAX_BYTES', len(serialize_readiness_response(complete)))
    result = prepared.page(query, actor_id='actor')
    assert result.page.returned == 2 and result.page.next_cursor is None


def test_filter_changes_issues_not_global_metrics_or_gates():
    graph = build_diagnostic_graph(SCOPE, (stale(), scenario('scenario')))
    data = NativeDeliveryDiagnostics(graph, tuple(DiagnosticDetail(c.id, (), (), (), (), (), ()) for c in graph.causes), SCOPE)
    prepared = PreparedDeliveryReadiness(context(graph), data, cursor=Cursor())
    all_rows = prepared.page(ReadinessReadQuery(board_id='board', spec_id='spec'), actor_id='actor')
    for phase in ('implementation', 'verification', 'lifecycle'):
        filtered = prepared.page(ReadinessReadQuery(board_id='board', spec_id='spec', phase=phase), actor_id='actor')
        assert filtered.implementation == all_rows.implementation
        assert filtered.verification == all_rows.verification
        assert filtered.delivery_gate == all_rows.delivery_gate
        assert all(i.phase == phase and i.detail_query.phase == phase for i in filtered.issues)
        assert filtered.page.total == (0 if phase == 'lifecycle' else 1)


def test_cycle_edges_and_all_actions_are_recoverable_in_detail():
    a, b = stale('a'), stale('b')
    aid, bid = (cause_identity(SCOPE, item) for item in (a, b))
    graph = build_diagnostic_graph(SCOPE, (a, b), dependency_edges=((aid, bid), (bid, aid)))
    data = NativeDeliveryDiagnostics(graph, tuple(DiagnosticDetail(c.id, (), (), (), (), (), ()) for c in graph.causes), SCOPE)
    prepared = PreparedDeliveryReadiness(context(graph), data, cursor=Cursor())
    found = []
    for cause in graph.causes:
        found.extend(e for response in pages(prepared, ReadinessReadQuery(board_id='board', spec_id='spec',
            issue_id=cause.id, snapshot_id='a' * 64, limit=2)) for e in response.entries)
    for kind, edges in (('cause_cycle', graph.cycle_edges), ('action_cycle', graph.action_cycle_edges)):
        assert {(e.value.source_id, e.value.target_id) for e in found
            if e.section == 'relations' and e.value.kind == kind} == set(edges)
    assert {e.value.id for e in found if e.type == 'action'} == {a.id for a in graph.actions}


def test_projection_identity_changes_with_visibility_but_not_observed_time():
    data = diagnostics(2)
    codec = Cursor()
    base = context(data.graph)
    prepared = PreparedDeliveryReadiness(base, data, cursor=codec)
    query = ReadinessReadQuery(board_id='board', spec_id='spec', limit=1)
    token = prepared.page(query, actor_id='actor').page.next_cursor
    later = PreparedDeliveryReadiness(base.model_copy(update={'evaluated_at': base.evaluated_at + timedelta(hours=1)}), data, cursor=codec)
    assert later.projection_id == prepared.projection_id
    assert later.page(query.model_copy(update={'cursor': token}), actor_id='actor').page.returned == 1
    changed = replace(data, details=tuple(replace(d, owner='owner', owner_state='known') for d in data.details))
    restricted = PreparedDeliveryReadiness(base, changed, cursor=codec)
    assert restricted.projection_id != prepared.projection_id
    with pytest.raises(ReadinessReadError, match='readiness_cursor_stale'):
        restricted.page(query.model_copy(update={'cursor': token}), actor_id='actor')


@pytest.mark.parametrize('changes', [{'board_id': 'other'}, {'card_id': 'foreign'}, {'spec_id': 'other'}])
def test_scope_cannot_be_replaced_by_query(changes):
    data = diagnostics()
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    with pytest.raises(ReadinessReadError, match='readiness_scope_mismatch'):
        prepared.page(ReadinessReadQuery.model_validate(dict(board_id='board', spec_id='spec') | changes), actor_id='actor')


def test_detail_requires_current_snapshot_and_closed_query():
    for changes in ({'issue_id': 'cause'}, {'limit': True}, {'limit': 21}, {'phase': 'other'}, {'edition': 1}, {'record_id': 'r'}):
        with pytest.raises(ValidationError):
            ReadinessReadQuery.model_validate(dict(board_id='board', spec_id='spec') | changes)


def test_detail_transport_rejects_type_confusion_and_unknown_fields():
    data = diagnostics()
    prepared = PreparedDeliveryReadiness(context(data.graph), data, cursor=Cursor())
    result = prepared.page(ReadinessReadQuery(board_id='board', spec_id='spec',
        snapshot_id='a' * 64, issue_id=data.graph.causes[0].id), actor_id='actor')
    original = result.model_dump(mode='json')
    damaged = original | {'entries': [original['entries'][0] | {'type': 'pinpoint'}]}
    with pytest.raises(ValidationError):
        DeliveryReadinessDetail.model_validate(damaged)
    with pytest.raises(ValidationError):
        DeliveryReadinessDetail.model_validate(original | {'unknown': 'field'})
    with pytest.raises(ValidationError):
        result.entries[0].id = 'changed'
    with pytest.raises(ReadinessReadError, match='readiness_cursor_stale'):
        prepared.page(ReadinessReadQuery(board_id='board', spec_id='spec', snapshot_id='b' * 64), actor_id='actor')


@pytest.mark.parametrize('changes', [{'board_id': 'foreign'}, {'spec_id': 'foreign'}, {'edition': 3}])
def test_cannot_assemble_header_and_diagnostics_from_different_scope(changes):
    from okto_pulse.core.domain.delivery_diagnostics import DiagnosticIntegrityError
    data = diagnostics()
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_scope_invalid'):
        PreparedDeliveryReadiness(context(data.graph), replace(data, scope=replace(SCOPE, **changes)), cursor=Cursor())
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_scope_invalid'):
        PreparedDeliveryReadiness(context(data.graph), replace(data, card_id='different-card'), cursor=Cursor())
