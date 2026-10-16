"""The fresh-install contract rejects pre-context writes before persistence."""
from datetime import datetime, timezone
from types import SimpleNamespace
from dataclasses import replace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityContractError,
    ObservedWorkspaceStateRef,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
    CodeInvestigationReceiptSubmission,
)
from okto_pulse.core.services.code_evidence import CodeEvidenceService
from okto_pulse.core.mcp.code_traceability_tools import _investigation_receipt_command

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def evidence_payload():
    return dict(
        contract_version=2, board_id='board', investigation_receipt_id='receipt',
        parent_type='spec', parent_id='spec', evidence_type='structure',
        claim='Existing implementation handles the governed operation.',
        selector=dict(kind='file', relative_path='src/service.py'),
        declared_source_content_sha256='a'*64, idempotency_key='evidence',
        source_role='current_implementation', relevance_summary='Existing operation',
        scope_relation='Directly in scope', source_origin='Committed baseline',
        baseline_provenance=dict(presence='committed_snapshot', workspace_state_id='ws'),
    )


@pytest.mark.parametrize('version', [None, 1, 3])
def test_evidence_refuses_missing_or_incompatible_version(version):
    payload = evidence_payload()
    if version is None:
        payload.pop('contract_version')
    else:
        payload['contract_version'] = version
    with pytest.raises(ValidationError):
        CodeEvidenceSubmission.model_validate(payload)


def test_native_evidence_context_survives_materialization_and_cannot_be_downgraded():
    command = CodeEvidenceSubmission.model_validate(evidence_payload())
    workspace = ObservedWorkspaceStateRef(
        declared_revision='revision', workspace_state_id='ws', declared_dirty=False,
        observed_at=NOW, reproducibility_claim='committed',
        fingerprint_algorithm='agent-manifest-v1', manifest_digest='b'*64,
        manifest_entry_count=1,
    )
    accepted = SimpleNamespace(receipt=SimpleNamespace(
        id='receipt', source_ref='source', subject_version=1, workspace_state=workspace,
    ))
    evidence = CodeEvidenceService(clock=lambda: NOW, id_factory=lambda _: 'evidence')._materialize(
        command, accepted=accepted, actor_id='agent', payload_sha256='c'*64,
        supersedes_evidence_id=None,
    )
    assert evidence.context_contract_version == 2
    assert evidence.relevance_summary == command.relevance_summary
    assert evidence.baseline_provenance == command.baseline_provenance
    with pytest.raises(CodeTraceabilityContractError):
        replace(evidence, context_contract_version=None)
    with pytest.raises(CodeTraceabilityContractError):
        replace(evidence, source_role='uncategorized_legacy')


def test_supersession_requires_complete_authored_context():
    payload = dict(evidence_payload(), supersedes_evidence_id='previous', supersession_reason='Corrected observation')
    assert CodeEvidenceSupersessionSubmission.model_validate(payload).supersedes_evidence_id == 'previous'
    payload.pop('baseline_provenance')
    with pytest.raises(ValidationError):
        CodeEvidenceSupersessionSubmission.model_validate(payload)


@pytest.mark.parametrize('version,outcome', [(1, 'accessible'), (2, 'accessible'), (1, 'evidence_applicable')])
def test_mcp_receipt_refuses_old_version_or_outcome(version, outcome):
    with pytest.raises(ValidationError):
        _investigation_receipt_command(
            contract_version=version, board_id='board', request_id='request',
            challenge_token='secret', outcome=outcome, capabilities=[],
            tooling=dict(tool_id='agent', tool_version='1', method_id='check'),
            observed_at=NOW, idempotency_key='receipt',
        )


def test_receipt_context_remains_server_owned():
    payload = dict(
        contract_version=2, board_id='board', request_id='request',
        challenge_token='secret', outcome='evidence_applicable', capabilities=[],
        tooling=dict(tool_id='agent', tool_version='1', method_id='check'),
        observed_at=NOW, idempotency_key='receipt',
    )
    assert CodeInvestigationReceiptSubmission.model_validate(payload).contract_version == 2
    with pytest.raises(ValidationError):
        CodeInvestigationReceiptSubmission.model_validate(dict(payload, delivery_context='greenfield'))
