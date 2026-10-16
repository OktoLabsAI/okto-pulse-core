"""Resolve a Test Card's observation plan from its current Spec, without copies.

This read model describes work, never authenticates evidence or grants credit.
Callers own authorization and supply only the same-Board, same-Spec population.
"""

from collections import defaultdict
from collections.abc import Mapping

from pydantic import ValidationError

from okto_pulse.core.domain.criterion_verification import CriterionVerification
from okto_pulse.core.domain.test_scenarios import VALID_VERIFICATION_METHODS


def build_test_card_verification_context(
    *, scenario_ids, scenarios, criteria, admitted_methods
):
    issues = []

    def index(values, kind):
        grouped = defaultdict(list)
        if not isinstance(values, (list, tuple)):
            issues.append({"code": f"{kind}_population_unavailable"})
            return grouped
        for value in values:
            if isinstance(value, Mapping) and isinstance(value.get("id"), str):
                grouped[value["id"]].append(value)
        return grouped

    scenario_index = index(scenarios, "scenario")
    criterion_index = index(criteria, "criterion")
    items = []
    if not isinstance(scenario_ids, (list, tuple)) or not scenario_ids:
        issues.append({"code": "test_card_scenarios_required"})
        scenario_ids = []
    if any(not isinstance(value, str) or not value for value in scenario_ids):
        issues.append({"code": "test_card_scenario_identity_invalid"})
    for scenario_id in dict.fromkeys(
        value for value in scenario_ids if isinstance(value, str)
    ):
        matches = scenario_index.get(scenario_id, [])
        if len(matches) != 1:
            issues.append({"code": "scenario_unresolved", "scenario_id": scenario_id})
            continue
        scenario = matches[0]
        method = scenario.get("verification_method")
        method_admitted = isinstance(method, str) and method in (admitted_methods or ())
        if method not in VALID_VERIFICATION_METHODS or not method_admitted:
            issues.append(
                {"code": "verification_method_unavailable", "scenario_id": scenario_id}
            )
        resolved = []
        links = scenario.get("linked_criteria")
        if (
            not isinstance(links, list)
            or not links
            or any(not isinstance(v, str) for v in links)
        ):
            issues.append(
                {"code": "scenario_criteria_required", "scenario_id": scenario_id}
            )
            links = []
        for criterion_id in dict.fromkeys(links):
            candidates = criterion_index.get(criterion_id, [])
            if len(candidates) != 1 or candidates[0].get("status") not in (
                None,
                "active",
            ):
                issues.append(
                    {
                        "code": "criterion_unresolved",
                        "scenario_id": scenario_id,
                        "criterion_id": criterion_id,
                    }
                )
                continue
            criterion = candidates[0]
            try:
                metadata = CriterionVerification.model_validate(
                    {
                        key: criterion[key]
                        for key in ("verification_profile", "requirement_links")
                        if key in criterion
                    }
                )
            except ValidationError:
                issues.append(
                    {"code": "criterion_metadata_invalid", "criterion_id": criterion_id}
                )
                continue
            if not metadata.verification_profile or not metadata.requirement_links:
                issues.append(
                    {"code": "criterion_plan_incomplete", "criterion_id": criterion_id}
                )
            if (
                not isinstance(criterion.get("text"), str)
                or not criterion["text"].strip()
            ):
                issues.append(
                    {
                        "code": "criterion_condition_missing",
                        "criterion_id": criterion_id,
                    }
                )
            resolved.append(
                {
                    "criterion_id": criterion_id,
                    "title": criterion.get("title"),
                    "condition": criterion.get("text"),
                    **metadata.model_dump(mode="json"),
                }
            )
        items.append(
            {
                "scenario_id": scenario_id,
                "title": scenario.get("title"),
                "given": scenario.get("given"),
                "when": scenario.get("when"),
                "expected_observation": scenario.get("then"),
                "verification_method": method,
                "method_admitted": method_admitted,
                "criteria": resolved,
                "evidence_requirement": (
                    "Authenticated Evidence V2 result on the current scenario and implementation base."
                    if method == "automated_test"
                    else "Authenticated verification-report/v1 with expected, observed and outcome for every linked criterion."
                    if method in VALID_VERIFICATION_METHODS
                    else None
                ),
            }
        )
    return {
        "content_complete": True,
        "source_resolution_complete": not issues,
        "items": items,
        "issues": issues,
        "delivery_evaluated": False,
        "guidance": {
            "resource": "okto-pulse://reference/tool-docs/test-scenario",
            "record_result_tool": "okto_pulse_update_test_scenario_status",
            "admit_report_tool": "okto_pulse_admit_test_verification_report",
            "associate_delivery_tool": "okto_pulse_record_delivery_evidence",
            "instruction": "Use the current authenticated result; associate its receipt through the Test Card ledger. Planning is not execution credit.",
        },
    }
