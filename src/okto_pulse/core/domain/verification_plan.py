"""Declared observation/work planning over canonical links; never execution credit.

All authored scenarios linked to a required criterion count. A supported method
on one scenario cannot conceal another unsupported method or missing Test Card.
Implementation contributions, dependencies and semantic approval remain separate.
"""

from collections import defaultdict
from collections.abc import Mapping

from okto_pulse.core.domain.enums import CardStatus, TestScenarioStatus
from okto_pulse.core.domain.requirement_verification_resolution import INACTIVE_STATES
from okto_pulse.core.domain.test_scenarios import (
    ADMITTED_VERIFICATION_METHODS,
    VALID_SCENARIO_TYPES,
    VALID_VERIFICATION_METHODS,
)

MAX_PLAN_NODES = 5000
MAX_PLAN_LINKS = 4096


def _identity(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 255


def resolve_verification_plan(
    *,
    qualification,
    board_id,
    spec_id,
    scenarios,
    cards,
    criteria,
    admitted_methods: frozenset[str] | None,
):
    """Compose a complete snapshot before paging, without reading results/proofs.

    ``admitted_methods`` comes from the public verifier port, never request data.
    Missing/oversized/malformed populations fail closed. Card rows must already
    be authorized and scoped; validate that scope again at the domain boundary.
    """
    issues = []
    issue_count = 0
    complete = True

    def issue(code, **facts):
        nonlocal issue_count
        issue_count += 1
        if len(issues) < 100:
            issues.append({"code": code, **facts})

    def population(values, kind):
        nonlocal complete
        if not isinstance(values, (list, tuple)) or len(values) > MAX_PLAN_NODES:
            complete = False
            issue("verification_plan_population_unavailable", population=kind)
            return {}
        grouped = defaultdict(list)
        for row in values:
            if not isinstance(row, Mapping) or not _identity(row.get("id")):
                complete = False
                issue("verification_plan_identity_invalid", population=kind)
                continue
            grouped[row["id"]].append(row)
        result = {}
        for identity, rows in grouped.items():
            if len(rows) != 1:
                complete = False
                issue("verification_plan_identity_ambiguous", population=kind)
            else:
                result[identity] = rows[0]
        return result

    scenario_rows = population(scenarios, "scenarios")
    criterion_rows = population(criteria, "criteria")
    card_rows = population(cards, "test_cards")
    by_scenario = defaultdict(list)
    card_links = 0
    for identity, card in sorted(card_rows.items()):
        if card.get("board_id") != board_id or card.get("spec_id") != spec_id:
            complete = False
            issue("verification_plan_scope_invalid", population="test_cards")
            continue
        if type(card.get("archived")) is not bool or card.get("status") not in tuple(
            s.value for s in CardStatus
        ):
            complete = False
            issue("verification_card_state_invalid", card_id=identity)
            continue
        if card["archived"] or card["status"] == "cancelled":
            continue
        if card.get("card_type") != "test":
            continue
        links = card.get("test_scenario_ids")
        if (
            not isinstance(links, list)
            or not links
            or any(not _identity(v) for v in links)
        ):
            issue("verification_card_scenario_links_invalid", card_id=identity)
            continue
        card_links += len(links)
        if card_links > MAX_PLAN_LINKS:
            complete = False
            issue("verification_plan_resolution_limit")
            break
        for scenario_id in sorted(set(links)):
            if scenario_id not in scenario_rows:
                issue("verification_card_scenario_unresolved", card_id=identity)
                continue
            by_scenario[scenario_id].append(identity)

    supported = (
        admitted_methods & ADMITTED_VERIFICATION_METHODS
        if isinstance(admitted_methods, frozenset)
        else None
    )
    if supported is None:
        issue("verification_method_capability_unavailable")
    by_criterion = defaultdict(list)
    scenario_links = 0
    for identity, scenario in sorted(scenario_rows.items()):
        links = scenario.get("linked_criteria")
        if not isinstance(links, list) or any(not _identity(v) for v in links):
            issue("verification_scenario_criteria_invalid", scenario_id=identity)
            continue
        scenario_links += len(links)
        if scenario_links > MAX_PLAN_LINKS:
            complete = False
            issue("verification_plan_resolution_limit")
            break
        blockers = []
        method = scenario.get("verification_method")
        if method is None:
            blockers.append("verification_method_required")
        elif not isinstance(method, str) or method not in VALID_VERIFICATION_METHODS:
            blockers.append("verification_method_invalid")
        elif supported is None or method not in supported:
            blockers.append("verification_method_unsupported")
        if scenario.get("scenario_type") not in VALID_SCENARIO_TYPES:
            blockers.append("verification_scenario_type_invalid")
        if scenario.get("status") not in tuple(s.value for s in TestScenarioStatus):
            blockers.append("verification_scenario_state_invalid")
        if any(
            not isinstance(scenario.get(field), str) or not scenario[field].strip()
            for field in ("given", "when", "then")
        ):
            blockers.append("verification_observation_required")
        planned = {
            "scenario_id": identity,
            "method": method if isinstance(method, str) and len(method) <= 64 else None,
            "method_admitted": isinstance(method, str)
            and supported is not None
            and method in supported,
            "method_plan_complete": not blockers,
            "test_card_ids": by_scenario[identity],
            "blockers": blockers,
            "work_blockers": []
            if by_scenario[identity]
            else ["verification_test_card_required"],
        }
        for criterion_id in sorted(set(links)):
            criterion = criterion_rows.get(criterion_id)
            # The active-obligation population governs planning. Keeping a
            # retired criterion/scenario for history must not create new work.
            if criterion is not None and criterion.get("status") in INACTIVE_STATES:
                continue
            if criterion is None or criterion.get("status") not in (
                None,
                "active",
                "not_applicable",
            ):
                issue(
                    "verification_scenario_criterion_unresolved", scenario_id=identity
                )
                continue
            by_criterion[criterion_id].append(planned)

    rows = []
    expansion = 0
    for row in qualification["requirements"]:
        method_ready = bool(row["qualification_resolved"])
        work_ready = method_ready
        paths = []
        for path in row["criteria_paths"]:
            plans = by_criterion[path["criterion_id"]]
            expansion += len(plans)
            if expansion > MAX_PLAN_LINKS:
                complete = False
                plans = []
            path_methods = bool(plans) and all(p["method_plan_complete"] for p in plans)
            path_work = path_methods and all(p["test_card_ids"] for p in plans)
            method_ready = method_ready and path_methods
            work_ready = work_ready and path_work
            paths.append(
                {
                    **path,
                    "scenario_plans": plans,
                    "planning_blockers": []
                    if plans
                    else ["verification_scenario_required"],
                    "method_plan_complete": path_methods,
                    "verification_work_complete": bool(path_work),
                }
            )
        rows.append(
            {
                **row,
                "criteria_paths": paths,
                "method_plan_complete": method_ready,
                "verification_work_complete": work_ready,
            }
        )
    if expansion > MAX_PLAN_LINKS:
        issue("verification_plan_resolution_limit")
    population_complete = complete and qualification["population_complete"]
    method_ready = (
        population_complete
        and qualification["criteria_resolution_complete"]
        and not issue_count
        and all(r["method_plan_complete"] for r in rows)
    )
    return {
        **qualification,
        "requirements": rows,
        "methods_evaluated": True,
        "verification_work_evaluated": True,
        "method_plan_complete": bool(method_ready),
        "verification_work_complete": bool(
            method_ready and all(r["verification_work_complete"] for r in rows)
        ),
        "planning_population_complete": population_complete,
        "planning_issues": issues,
        "planning_issue_count": issue_count,
        "planning_issues_truncated": issue_count > len(issues),
    }
