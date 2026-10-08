"""KG56: missing-link suggestions never adopt frozen source requirements."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.processors.consolidation import _resolve_missing_link_candidates
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.services.analytics_service import spec_coverage_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["validated", "in_progress", "done"])
async def test_frozen_source_suggestions_do_not_emit_links_or_change_coverage(status):
    source = {
        "id": "spec-frozen", "board_id": "board", "title": "Frozen source",
        "status": status, "edition": 1, "version": 4,
        "current_validation_id": "validation",
        "functional_requirements": [{"id": "fr", "text": "Current requirement"}],
        "technical_requirements": [], "acceptance_criteria": [{"id": "ac", "text": "Condition"}],
        "test_scenarios": [{"id": "scenario", "title": "Unlinked scenario",
                            "linked_criteria": [], "linked_task_ids": []}],
        "api_contracts": [{"id": "api", "method": "GET", "path": "/items",
                           "linked_requirements": [], "linked_task_ids": []}],
        "business_rules": [], "decisions": [], "integration_requirements": [],
        "observability_requirements": [],
    }
    before = deepcopy(source)
    coverage = spec_coverage_summary(SimpleNamespace(**source), cards=[])
    result = DeterministicWorker().process_spec(source)
    suggestions = [c for c in result.missing_link_candidates if c.suggested_candidates]
    assert {c.edge_type for c in suggestions} >= {"tests", "implements"}
    edges_before = deepcopy(result.edges)
    candidates_before = deepcopy(result.missing_link_candidates)
    persistence = SimpleNamespace(list_artifacts=AsyncMock(
        side_effect=AssertionError("Semantic suggestions must not trigger artifact resolution")))
    resolved = await _resolve_missing_link_candidates(
        None, "board", result, persistence=persistence)
    assert resolved.edges == edges_before
    assert resolved.missing_link_candidates == candidates_before
    assert not any(edge.edge_type in {"tests", "implements"} for edge in resolved.edges)
    assert not any(edge.layer == "fallback" for edge in resolved.edges)
    persistence.list_artifacts.assert_not_called()
    assert source == before
    assert spec_coverage_summary(SimpleNamespace(**source), cards=[]) == coverage
    assert coverage["ac_coverage_pct"] == 0
