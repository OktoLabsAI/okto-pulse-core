from dataclasses import replace

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityContractError,
    SourceContextCurrentReceiptV2,
    aggregate_current_contextual_investigation_outcome_v2,
)
from okto_pulse.core.models.code_traceability import CodeInvestigationReceiptView
from test_code_traceability_contracts import receipt


def test_receipt_projects_only_current_outcome_and_refuses_null_context():
    current = receipt()
    payload = CodeInvestigationReceiptView.model_validate(current).model_dump(mode='json')
    assert payload['contextual_outcome'] == 'evidence_applicable'
    assert 'outcome' not in payload
    assert 'effective_outcome' not in payload
    with pytest.raises(CodeTraceabilityContractError):
        replace(current, contextual_outcome=None)
    with pytest.raises(CodeTraceabilityContractError):
        replace(current, delivery_context=None)


def test_frozen_receipt_requires_authored_context_without_inference():
    current = receipt()
    frozen = SourceContextCurrentReceiptV2(
        receipt_id=current.id, source_ref=current.source_ref, generation=1,
        head_revision=1, payload_sha256=current.payload_sha256,
        delivery_context=current.delivery_context,
        contextual_outcome=current.contextual_outcome, context_contract_version=2,
    )
    assert frozen.as_dict()['contextual_outcome'] == 'evidence_applicable'
    with pytest.raises(CodeTraceabilityContractError):
        replace(frozen, contextual_outcome=None, delivery_context=None, context_contract_version=None)
    assert aggregate_current_contextual_investigation_outcome_v2(()) is None
    with pytest.raises(CodeTraceabilityContractError):
        aggregate_current_contextual_investigation_outcome_v2((None,))
