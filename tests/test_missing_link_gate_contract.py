"""KG28: semantic reference policy never infers optional associations."""
from copy import deepcopy

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.missing_link_gate import (
    SPEC_LINK_FIELDS, MissingLinkSourceUnavailable, missing_link_mode, spec_semantic_link_findings,
)
from okto_pulse.core.models.schemas import BoardSettings


def source():
    return {name: [] for origin, _, targets in SPEC_LINK_FIELDS for name in (origin, *targets)}


def test_default_is_advisory_without_consulting_graph_state():
    assert missing_link_mode(None) == BoardSettings().missing_link_gate == "advisory"
    assert missing_link_mode({"skip_cognitive_consolidation": False}) == "advisory"
    assert not spec_semantic_link_findings(spec_id="spec", collections=source())


@pytest.mark.parametrize("value", [None, "off", "BLOCKING", True, {}, 0])
def test_invalid_policy_does_not_become_an_implicit_waiver(value):
    with pytest.raises(ValueError, match="missing_link_gate_policy_invalid"):
        missing_link_mode({"missing_link_gate": value})
    with pytest.raises(ValidationError):
        BoardSettings(missing_link_gate=value)


@pytest.mark.parametrize("origin,field,targets", SPEC_LINK_FIELDS)
def test_current_declared_target_is_required_but_optional_omission_is_valid(origin, field, targets):
    values = source()
    values[origin] = [{"id": "owner"}]
    assert not spec_semantic_link_findings(spec_id="spec", collections=values)
    values[origin][0][field] = "target" if field == "supersedes_decision_id" else ["target"]
    before = deepcopy(values)
    findings = spec_semantic_link_findings(spec_id="spec", collections=values)
    assert len(findings) == 1
    assert findings[0].field == field and findings[0].target_ref == "target"
    assert findings[0].reason == "target_absent"
    assert values == before
    values[targets[0]].append({"id": "target"})
    assert not spec_semantic_link_findings(spec_id="spec", collections=values)
    values[targets[0]].append({"id": "target"})
    assert spec_semantic_link_findings(spec_id="spec", collections=values)[0].reason == "target_ambiguous"


def test_incomplete_source_is_unavailable_not_zero_findings():
    values = source()
    del values["technical_requirements"]
    with pytest.raises(MissingLinkSourceUnavailable):
        spec_semantic_link_findings(spec_id="spec", collections=values)


def test_resolved_source_replaces_stale_diagnostics_without_ledger_input():
    values = source()
    values["decisions"] = [{"id": "choice", "linked_requirements": ["gone"]}]
    assert spec_semantic_link_findings(spec_id="spec", collections=values)
    values["decisions"][0]["linked_requirements"] = []
    assert not spec_semantic_link_findings(spec_id="spec", collections=values)
