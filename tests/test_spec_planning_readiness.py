"""Native planning judgments remain independent in the integrated projection."""
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.spec_evaluation import evaluate_spec_decomposition
from okto_pulse.core.domain.spec_planning_readiness import (
    PLANNING_COMPONENTS, SpecPlanningComponent, SpecPlanningReadiness,
)
from okto_pulse.core.services.spec_planning_readiness import (
    PLANNING_COVERAGE_CHECKS, read_spec_planning_readiness, require_planning_coverage,
)


def rig():
    spec = NS(id="spec", board_id="board", edition=1, current_validation_id="quality",
              validations=[{"id": "quality", "edition": 1, "validation_edition": 1, "outcome": "success"}],
              evaluations=[{"id": "review", "spec_edition": 1, "recommendation": "approve", "overall_score": 90}],
              decisions=[])
    board = NS(settings={})
    cards = NS(**{method: AsyncMock() for _key, method, _skip, _field in PLANNING_COVERAGE_CHECKS})
    resources = NS(
        is_spec_architecture_required_for_validation=lambda _board: False,
        is_spec_resource_task_coverage_required=lambda _board: True,
        validate_or_raise_spec_architecture_validation_resource=AsyncMock(),
        validate_or_raise_spec_resource_task_coverage=AsyncMock(),
    )
    specs = NS(
        spec_decomposition_verdict=lambda subject, config: evaluate_spec_decomposition(
            subject.evaluations, edition=subject.edition,
            required=not config.settings.get("auto_validate", False), threshold=70,
        ),
        require_planning_lint=AsyncMock(return_value=(NS(id="lint"), None)),
    )
    return dict(
        spec=spec, board=board, specs=specs, cards=cards, resources=resources,
        checklist=AsyncMock(return_value=(NS(allowed=True, mode=NS(value="blocking"), reason="passed"), "checklist")),
        policy=AsyncMock(return_value=None), can_read=lambda _permission: True,
        can_act=lambda _permission: True,
    )


def by_key(result):
    return {item.key: item for item in result.components}


@pytest.mark.asyncio
async def test_complete_readiness_keeps_native_ids_and_has_no_combined_score():
    result = await read_spec_planning_readiness(**rig())
    assert result.state == "ready", result.to_dict()
    assert tuple(by_key(result)) == PLANNING_COMPONENTS
    assert by_key(result)["spec_validation"].result_ids == ("quality",)
    assert by_key(result)["decomposition"].result_ids == ("review",)
    assert by_key(result)["requirement_lint"].result_ids == ("lint",)
    assert by_key(result)["curated_checklist"].result_ids == ("checklist",)
    assert by_key(result)["decision_verification"].native_result == "not_applicable"
    assert "score" not in result.to_dict()
    assert not result.to_dict()["blocked_actions"]


@pytest.mark.asyncio
@pytest.mark.parametrize("quality,review,score,blocker", [
    ("failed", "approve", 100, "spec_validation"),
    ("success", "reject", 100, "decomposition"),
    ("success", "approve", 40, "decomposition"),
    ("success", "request_changes", 100, "decomposition"),
])
async def test_native_failure_cannot_be_compensated_by_another_review(quality, review, score, blocker):
    inputs = rig()
    inputs["spec"].validations[0]["outcome"] = quality
    inputs["spec"].evaluations[0].update(recommendation=review, overall_score=score)
    result = await read_spec_planning_readiness(**inputs)
    assert result.state == "blocked"
    assert by_key(result)[blocker].state == "blocked"
    assert result.to_dict()["blocked_actions"] == ["promote_to_validated"]


@pytest.mark.asyncio
async def test_hidden_component_does_not_run_reader_or_expose_receipt_or_actions():
    inputs = rig()
    inputs["can_read"] = lambda permission: permission != "spec.checklist.read"
    result = await read_spec_planning_readiness(**inputs)
    inputs["checklist"].assert_not_awaited()
    hidden = by_key(result)["curated_checklist"]
    assert result.state == hidden.state == "unavailable"
    assert hidden.result_ids == () and hidden.next_action is None and hidden.native_result is None


@pytest.mark.asyncio
async def test_technical_failure_is_unavailable_without_protected_exception_text():
    inputs = rig()
    inputs["specs"].require_planning_lint.side_effect = RuntimeError("private storage diagnostic")
    result = await read_spec_planning_readiness(**inputs)
    assert result.state == "unavailable"
    assert by_key(result)["requirement_lint"].state == "unavailable"
    assert "private storage" not in str(result.to_dict())


@pytest.mark.asyncio
async def test_disabled_quality_does_not_require_its_exclusive_prerequisites():
    inputs = rig()
    inputs["board"].settings = {"require_spec_validation": False, "auto_validate": True}
    inputs["spec"].validations = inputs["spec"].evaluations = []
    result = await read_spec_planning_readiness(**inputs)
    assert result.state == "ready"
    assert by_key(result)["spec_validation"].native_result == "off"
    inputs["specs"].require_planning_lint.assert_not_awaited()
    inputs["cards"].check_ac_scenario_coverage.assert_not_awaited()
    inputs["resources"].validate_or_raise_spec_resource_task_coverage.assert_not_awaited()
    inputs["checklist"].assert_awaited_once()


@pytest.mark.asyncio
async def test_advisory_findings_remain_advisory_and_actions_require_write_authority():
    inputs = rig()
    inputs["checklist"].return_value = (NS(allowed=True, mode=NS(value="advisory"), reason="missing_result"), None)
    inputs["spec"].evaluations = []
    inputs["can_act"] = lambda _permission: False
    result = await read_spec_planning_readiness(**inputs)
    assert by_key(result)["curated_checklist"].state == "ready"
    assert by_key(result)["curated_checklist"].mode == "advisory"
    assert by_key(result)["decomposition"].state == "blocked"
    assert by_key(result)["decomposition"].next_action is None


@pytest.mark.asyncio
async def test_shared_coverage_writer_keeps_native_error_and_order():
    inputs = rig()
    error = ValueError("actual native coverage error")
    inputs["cards"].check_rules_coverage.side_effect = error
    with pytest.raises(ValueError) as raised:
        await require_planning_coverage(inputs["cards"], inputs["spec"], inputs["board"], quality_enabled=True)
    assert raised.value is error
    inputs["cards"].check_ac_scenario_coverage.assert_awaited_once()
    inputs["cards"].check_test_coverage.assert_awaited_once()
    inputs["cards"].check_trs_coverage.assert_not_awaited()


def test_aggregate_refuses_an_incomplete_component_census():
    with pytest.raises(ValueError, match="census_incomplete"):
        SpecPlanningReadiness((SpecPlanningComponent("spec_validation", "ready", "blocking", True),))


@pytest.mark.asyncio
@pytest.mark.parametrize("case,state,mode", [
    ("blocking_pass", "ready", "blocking"),
    ("blocking_fail", "blocked", "blocking"),
    ("advisory_fail", "ready", "advisory"),
    ("missing", "blocked", "blocking"),
    ("unavailable", "unavailable", None),
    ("off", "ready", "off"),
])
async def test_policy_projection_preserves_native_judgment(case, state, mode):
    from test_skb3_semantic_policy_transition_gate import _binding_snapshot, _transition_snapshot
    from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement
    from okto_pulse.core.domain.guideline_semantic_transition import evaluate_policy_transition

    binding = _binding_snapshot(
        score=40 if case.endswith("fail") else 90,
        enforcement=GuidelineEnforcement.ADVISORY if case == "advisory_fail" else GuidelineEnforcement.BLOCKING,
        include_receipt=case not in {"missing", "unavailable"},
        assessment_available=case != "unavailable",
        assessment_error_code="assessment_service_unavailable" if case == "unavailable" else None,
    )
    native = evaluate_policy_transition(
        _transition_snapshot((binding,)), "review" if case == "off" else "validated",
    )
    inputs = rig()
    inputs["policy"].return_value = native
    result = await read_spec_planning_readiness(**inputs)
    component = by_key(result)["policy_compliance"]
    assert component.state == state, component
    assert component.mode == mode
    assert component.native_result == native.reason_code.value
    if case != "off":
        assert component.result_ids == native.receipt_ids
        assert component.reason_codes == tuple(reason.value for reason in native.reason_codes)


@pytest.mark.asyncio
async def test_disabled_quality_and_decomposition_do_not_require_read_permissions():
    inputs = rig()
    inputs["board"].settings = {"require_spec_validation": False, "auto_validate": True}
    inputs["can_read"] = lambda permission: permission not in {"spec.validation.read", "spec.evaluations.read", "spec.quality.read"}
    result = await read_spec_planning_readiness(**inputs)
    assert result.state == "ready"


@pytest.mark.asyncio
async def test_malformed_quality_edition_is_unavailable_not_authored_rejection():
    inputs = rig()
    inputs["spec"].validations[0]["validation_edition"] = 2
    result = await read_spec_planning_readiness(**inputs)
    component = by_key(result)["spec_validation"]
    assert component.state == "unavailable"
    assert component.native_result is None and component.result_ids == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["missing_outcome", "unknown_outcome", "duplicate_head", "dangling_head"])
async def test_invalid_quality_head_is_unavailable_not_a_reviewer_verdict(corruption):
    inputs = rig()
    record = inputs["spec"].validations[0]
    if corruption == "missing_outcome":
        record.pop("outcome")
    elif corruption == "unknown_outcome":
        record["outcome"] = "not_a_native_outcome"
    elif corruption == "duplicate_head":
        inputs["spec"].validations.append(dict(record))
    else:
        inputs["spec"].current_validation_id = "absent"
    result = await read_spec_planning_readiness(**inputs)
    component = by_key(result)["spec_validation"]
    assert result.state == component.state == "unavailable"
    assert component.native_result is None and component.result_ids == ()
    assert component.next_action is None  # A new authored review does not repair authority.


@pytest.mark.asyncio
@pytest.mark.parametrize("field,permission,key,method", [
    ("integration_requirements", "spec.integration_requirements.read", "integration_coverage", "check_ir_coverage"),
    ("observability_requirements", "spec.observability_requirements.read", "observability_coverage", "check_or_coverage"),
    ("api_contracts", "spec.contracts.read", "contract_coverage", "check_contract_coverage"),
    ("test_scenarios", "spec.tests.read", "test_card_coverage", "check_test_coverage"),
])
async def test_coverage_hides_native_diagnostics_without_component_read(field, permission, key, method):
    inputs = rig()
    setattr(inputs["spec"], field, [{"id": "protected", "title": "Confidential detail"}])
    reader = getattr(inputs["cards"], method)
    reader.side_effect = ValueError("Confidential detail has no linked card")
    inputs["can_read"] = lambda leaf: leaf != permission
    component = by_key(await read_spec_planning_readiness(**inputs))[key]
    reader.assert_not_awaited()
    assert component.state == "unavailable" and component.message is None
