"""R2: execute published resource examples against native argument contracts."""
from __future__ import annotations

import copy
import inspect
import json
from pathlib import Path
import re
from typing import get_type_hints
from types import SimpleNamespace

import pytest
from pydantic import ConfigDict, ValidationError, create_model

from okto_pulse.core.mcp import server
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceInput
from okto_pulse.core.models.quality_assessment import QualityFindingInput
from okto_pulse.core.domain.quality_assessment import QualityAssessmentContractError
from okto_pulse.core.domain.quality_taxonomy import require_ambiguity_category
from okto_pulse.core.services.ambiguity_assessment import AMBIGUITY_SCALE
from okto_pulse.core.application.use_cases.code_traceability import _require_spec_entity
from okto_pulse.core.domain.code_traceability import CodeEvidenceLinkInvalid, SpecEntityType
from okto_pulse.core.domain.enums import TestScenarioStatus as ScenarioStatus
from okto_pulse.core.models.schemas import TestScenario as Scenario


DOCS = Path(__file__).parents[1] / "src/okto_pulse/core/mcp/resources/reference/tool-docs"
PATTERN = re.compile(r"<!-- (contract|model)-example: ([a-zA-Z0-9_]+) -->\s*```json\s*(.*?)\s*```", re.S)
EXAMPLES = [(kind, name, json.loads(body)) for filename in
            ("qa.md", "quality.md", "code-traceability.md", "test-scenario.md")
            for kind, name, body in PATTERN.findall((DOCS / filename).read_text(encoding="utf-8"))]


def _argument_model(name):
    tool = next(tool for tool in server.mcp.iter_tools() if tool.name == name)
    function = inspect.unwrap(tool.fn)
    hints = get_type_hints(function, include_extras=True)
    fields = {key: (hints[key], ... if parameter.default is inspect.Parameter.empty else parameter.default)
              for key, parameter in inspect.signature(function).parameters.items()}
    return create_model("PublishedExample", __config__=ConfigDict(extra="forbid"), **fields)


@pytest.mark.parametrize("kind,name,payload", EXAMPLES, ids=[name for _, name, _ in EXAMPLES])
def test_published_examples_match_native_argument_models(kind, name, payload):
    model = CardDeliveryEvidenceInput if kind == "model" else _argument_model(name)
    parsed = model.model_validate_json(json.dumps(payload))
    if name == "okto_pulse_record_ambiguity_assessment":
        AMBIGUITY_SCALE.validate_score(parsed.score)
        for finding in parsed.findings:
            require_ambiguity_category(finding.category_code)
            finding.to_domain()
    if name == "okto_pulse_ask_spec_choice_question":
        assert sum(option["recommended"] for option in payload["options"]) == 1
        assert all(option["tradeoff"] for option in payload["options"])


def test_published_contract_examples_cover_every_changed_workflow():
    assert {name for kind, name, _ in EXAMPLES} == {
        "okto_pulse_ask_spec_choice_question", "okto_pulse_record_ambiguity_assessment",
        "okto_pulse_record_delivery_evidence", "CardDeliveryEvidenceInput",
        "okto_pulse_start_code_investigation", "okto_pulse_submit_implementation_target_resolution",
        "okto_pulse_add_test_scenario", "okto_pulse_update_test_scenario",
    }


def test_progress_example_cannot_move_source_ref_outside_source_state():
    payload = copy.deepcopy(next(payload for _, name, payload in EXAMPLES
                                 if name == "okto_pulse_record_delivery_evidence"))
    progress = payload["evidence"]["progress"]
    progress["source_ref"] = progress["source_state"].pop("source_ref")
    with pytest.raises(ValidationError):
        _argument_model("okto_pulse_record_delivery_evidence").model_validate_json(json.dumps(payload))


def test_quality_examples_use_distinct_native_score_scales():
    assert AMBIGUITY_SCALE.validate_score(0) == 0
    assert AMBIGUITY_SCALE.validate_score(5) == 5
    with pytest.raises(QualityAssessmentContractError):
        AMBIGUITY_SCALE.validate_score(6)
    finding = copy.deepcopy(next(payload for _, name, payload in EXAMPLES
                                 if name == "okto_pulse_record_ambiguity_assessment")["findings"][0])
    finding["confidence"] = 95
    with pytest.raises(ValidationError):
        QualityFindingInput.model_validate(finding)


@pytest.mark.parametrize("status", list(ScenarioStatus))
@pytest.mark.parametrize("representation", ["native", "json"])
def test_same_spec_scenario_links_accept_every_native_operational_state(status, representation):
    scenario = Scenario(id="ts-native", title="Native scenario", status=status)
    if representation == "json":
        scenario = scenario.model_dump(mode="json")
    spec = SimpleNamespace(id="spec-owned", test_scenarios=[scenario])
    _require_spec_entity(spec, SpecEntityType.TEST_SCENARIO, "ts-native")


@pytest.mark.parametrize("scenarios", [
    [], ["ts-native"], [{"id": "ts-foreign", "status": "passed"}],
    [{"id": "ts-native"}], [{"id": "ts-native", "status": None}],
    [{"id": "ts-native", "status": "active"}],
    [{"id": "ts-native", "status": "unknown"}],
    [{"id": "ts-native", "status": "PASSED"}],
])
def test_scenario_links_reject_missing_foreign_or_malformed_native_state(scenarios):
    spec = SimpleNamespace(id="spec-owned", test_scenarios=scenarios, test_scenario_ids=["ts-native"])
    with pytest.raises(CodeEvidenceLinkInvalid):
        _require_spec_entity(spec, SpecEntityType.TEST_SCENARIO, "ts-native")


def test_scenario_lifecycle_does_not_relax_other_normative_entity_statuses():
    spec = SimpleNamespace(functional_requirements=[{"id": "fr-1", "status": "failed"}])
    with pytest.raises(CodeEvidenceLinkInvalid):
        _require_spec_entity(spec, SpecEntityType.FUNCTIONAL_REQUIREMENT, "fr-1")
