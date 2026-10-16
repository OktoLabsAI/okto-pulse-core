"""P07 compact mutation errors reuse assessment facts without a second read."""
import json

import pytest

from okto_pulse.core.domain.delivery_assessment import assess_delivery_gate, assess_obligation_delivery
from okto_pulse.core.domain.delivery_diagnostics import DiagnosticIntegrityError
from okto_pulse.core.inbound.delivery_readiness_error import project_readiness_error
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.mcp.code_traceability_tools import _error_outcome
from okto_pulse.core.models.delivery_evidence import DeliveryEvidenceReadQuery
from okto_pulse.core.models.delivery_report import DeliveryReportRejected
from okto_pulse.core.ports.delivery_readiness import ReadinessAccessDenied, ReadinessReadError
from okto_pulse.core.application.delivery_errors import DeliveryGateBlocked
from test_delivery_obligation_assessment import snapshot, with_facts


def test_edition_error_contracts_are_public_without_exposing_service_internals():
    from okto_pulse.core.application.boundary.public_contract_manifest import is_public_core_contract
    assert is_public_core_contract(DeliveryGateBlocked.__module__ + '.DeliveryGateBlocked')
    assert is_public_core_contract(project_readiness_error.__module__ + '.project_readiness_error')
    assert is_public_core_contract(project_readiness_error.__module__ + '.READINESS_ERRORS')
    assert not is_public_core_contract('okto_pulse.core.services.delivery_evidence.delivery_store')


@pytest.mark.parametrize('card_id', [None, 'ui'])
def test_compact_error_identifies_actual_assessment_and_valid_follow_up_without_dumping_evidence(card_id):
    assessment = assess_obligation_delivery(with_facts(snapshot(), implementations=(), tests=()),
        **({'card_id': card_id, 'card_type': 'normal'} if card_id else {}))
    error = DeliveryGateBlocked(assessment, assess_delivery_gate(assessment))
    envelope = error.to_error_dict()
    summary = envelope['details']['readiness_summary']
    assert summary['implementation']['percent'] == summary['verification']['percent'] == 0
    assert summary['delivery_gate_status'] == 'blocked' and summary['snapshot_refresh_required']
    assert len(summary['assessment_snapshot_id']) == 64
    assert error.to_error_dict() == envelope
    query = DeliveryEvidenceReadQuery(**envelope['details']['readiness_query'])
    assert query.view == 'readiness' and query.card_id == card_id
    assert query.snapshot_id is None  # A gate fingerprint never masquerades as a read cursor base.
    assert len(json.dumps(envelope).encode()) < 8192
    assert 'obligations' not in envelope['details'] and 'evidence_refs' not in envelope['details']
    assert json.loads(MCPAdapterContract.error(error)) == {'error': error.code, **envelope}
    assert DeliveryReportRejected(error).to_error_dict() == envelope
    outcome = _error_outcome(error)
    assert outcome.is_error and outcome.code == error.code
    assert outcome.details['readiness_summary'] == summary


@pytest.mark.parametrize('code,retryable', [('readiness_query_invalid', False),
    ('readiness_scope_mismatch', False), ('readiness_cursor_stale', True),
    ('readiness_snapshot_changed', True), ('readiness_limit_exceeded', False),
    ('readiness_assessment_unavailable', True)])
def test_typed_read_errors_have_distinct_codes_and_safe_guidance(code, retryable):
    error = ReadinessReadError(code, native_code='native_source_unavailable', identity='private-location')
    envelope = project_readiness_error(error)
    outcome = _error_outcome(error)
    assert outcome.is_error and outcome.code == code and outcome.retryable is retryable
    assert envelope['details'] == {'native_code': 'native_source_unavailable'}
    assert 'private-location' not in json.dumps(envelope)


def test_access_and_integrity_errors_do_not_become_empty_success_or_leak_error_prose():
    assert _error_outcome(ReadinessAccessDenied('spec.entity.read')).code == 'readiness_permission_denied'
    error = DiagnosticIntegrityError('private source text /absolute/path')
    assert project_readiness_error(error)['details'] == {}
    assert _error_outcome(error).code == 'readiness_diagnostic_integrity'
