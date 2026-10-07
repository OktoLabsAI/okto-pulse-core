"""Test views of the native live worker; no retained migration plan."""
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from okto_pulse.core.ports.consolidation import ConsolidationProjectionInputs
from okto_pulse.core.application.processors.consolidation import _prepare_deterministic_projection


def spec(**values):
    return SimpleNamespace(**({'id': 'spec-one', 'board_id': 'board', 'title': 'Expected Spec',
        'description': 'Retain structured requirements.', 'context': '', 'status': 'done',
        'functional_requirements': [{'id': 'fr-one', 'title': 'Retain the source'}],
        'technical_requirements': [], 'acceptance_criteria': [], 'business_rules': [],
        'test_scenarios': [], 'api_contracts': [], 'decisions': [], 'architecture_designs': [],
        'integration_requirements': [], 'observability_requirements': []} | values))


def persistence(artifact):
    return SimpleNamespace(load_artifact=AsyncMock(return_value=artifact),
        load_projection_inputs=AsyncMock(return_value=ConsolidationProjectionInputs()),
        list_artifacts=AsyncMock(return_value=()))


async def prepare_projection(context, port, board_id, artifact_type, artifact_id):
    entry = SimpleNamespace(board_id=board_id, artifact_type=artifact_type, artifact_id=artifact_id)
    result = await _prepare_deterministic_projection(context, entry, persistence=port)
    assert result is not True and result is not False
    worker, _source = result
    view = asdict(worker)
    if worker.reference_findings is not None:
        view['reference_findings'] = worker.reference_findings.to_payload()
    return view
