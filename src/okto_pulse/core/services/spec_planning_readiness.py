"""Read planning gates through their native predicates without writing opinions."""
from collections.abc import Awaitable, Callable

from okto_pulse.core.domain.spec_planning_readiness import (
    PLANNING_COMPONENTS, SpecPlanningComponent, SpecPlanningReadiness,
)
from okto_pulse.core.domain.spec_evaluation import spec_evaluation_is_current
from okto_pulse.core.domain.spec_validation import require_spec_validation_edition
from okto_pulse.core.domain.code_traceability import CodeTraceabilityContractError


# Shared enumeration for the authoritative coverage writers and this projection.
# AC coverage belongs to the enabled quality-validation prerequisite set.
PLANNING_COVERAGE_CHECKS = (
    ("acceptance_coverage", "check_ac_scenario_coverage", "skip_test_coverage", "acceptance_criteria"),
    ("test_card_coverage", "check_test_coverage", "skip_test_coverage", "test_scenarios"),
    ("business_coverage", "check_rules_coverage", "skip_rules_coverage", "functional_requirements"),
    ("technical_coverage", "check_trs_coverage", "skip_trs_coverage", "technical_requirements"),
    ("contract_coverage", "check_contract_coverage", "skip_contract_coverage", "api_contracts"),
    ("integration_coverage", "check_ir_coverage", "skip_ir_coverage", "integration_requirements"),
    ("observability_coverage", "check_or_coverage", "skip_or_coverage", "observability_requirements"),
    ("task_requirement_links", "check_task_requirement_links_for_spec", None, None),
    ("decision_verification", "check_decisions_coverage", None, "decisions"),
    ("code_evidence", "check_code_evidence_coverage", "skip_code_evidence_coverage", None),
)

_COVERAGE_READ_PERMISSIONS = {
    "acceptance_coverage": ("spec.tests.read",),
    "test_card_coverage": ("spec.tests.read", "card.entity.read"),
    "business_coverage": ("spec.rules.read", "card.entity.read"),
    "technical_coverage": ("card.entity.read",),
    "contract_coverage": ("spec.contracts.read", "card.entity.read"),
    "integration_coverage": ("spec.integration_requirements.read", "card.entity.read"),
    "observability_coverage": ("spec.observability_requirements.read", "card.entity.read"),
    "task_requirement_links": ("card.entity.read",),
    "decision_verification": (),
    "code_evidence": ("code_traceability.evidence.read", "code_traceability.investigation.read"),
}


async def require_planning_coverage(cards, spec, board, *, quality_enabled: bool) -> None:
    """Execute the existing predicates in their native order and retain errors."""
    for key, method, _skip, _field in PLANNING_COVERAGE_CHECKS:
        if key == "acceptance_coverage" and not quality_enabled:
            continue
        await getattr(cards, method)(spec, board)


async def read_spec_planning_readiness(
    *, spec, board, specs, cards, resources, checklist: Callable[[], Awaitable],
    policy: Callable[[], Awaitable], can_read: Callable[[str], bool],
    can_act: Callable[[str], bool],
) -> SpecPlanningReadiness:
    """No metadata polling: full admission reads run on explicit workspace load.

    Caller owns the consistent read transaction and subject authorization.
    Hidden components never run their reader or disclose native result IDs.
    Runtime failures remain unavailable; they are not negative authored reviews.
    """
    settings = (getattr(board, "settings", None) or {}) if board else {}
    quality_enabled = bool(settings.get("require_spec_validation", True))
    results: dict[str, SpecPlanningComponent] = {}

    def action(permission: str, name: str) -> str | None:
        return name if can_act(permission) else None

    async def observe(key, permission, callback, *, enabled=True, applicable=True,
                      next_action=None, interpret=None):
        if not enabled or not applicable:
            results[key] = SpecPlanningComponent(
                key, "ready", "off" if not enabled else "blocking", applicable,
                native_result="off" if not enabled else "not_applicable",
            )
            return
        permissions = (permission,) if isinstance(permission, str) else permission
        if not all(can_read(item) for item in permissions):
            results[key] = SpecPlanningComponent.unreadable(key)
            return
        try:
            value = await callback()
            results[key] = (interpret(value) if interpret else SpecPlanningComponent(
                key, "ready", "blocking", True, native_result="passed",
            ))
        except ValueError as exc:
            projection_unavailable = isinstance(exc, CodeTraceabilityContractError) and exc.code in {
                "code_investigation_currentness_unknown", "code_traceability_projection_incomplete",
            }
            if projection_unavailable or key in {"spec_validation", "decomposition", "policy_compliance", "curated_checklist"}:
                results[key] = SpecPlanningComponent(
                    key, "unavailable", None, None,
                    reason_codes=(exc.code if projection_unavailable else key + "_authority_invalid",),
                )
                return
            results[key] = SpecPlanningComponent(
                key, "blocked", "blocking", True, native_result="failed",
                reason_codes=(str(getattr(exc, "code", key + "_required")),),
                message=str(exc), next_action=next_action,
            )
        except Exception:
            # No infrastructure message, protected data or guessed gate verdict.
            results[key] = SpecPlanningComponent(
                key, "unavailable", None, None,
                reason_codes=(key + "_unavailable",),
            )

    async def quality_result():
        pointer = getattr(spec, "current_validation_id", None)
        if pointer is None:
            return None
        if not isinstance(pointer, str) or not pointer.strip():
            raise ValueError("spec_validation_head_invalid")
        rows = [row for row in (spec.validations or []) if row.get("id") == pointer]
        # Missing/ambiguous authority is not a reviewer's negative opinion, nor
        # may a duplicate head silently choose the last apparently positive row.
        if len(rows) != 1:
            raise ValueError("spec_validation_head_invalid")
        row = rows[0]
        if row.get("outcome") not in {"success", "failed"}:
            raise ValueError("spec_validation_outcome_invalid")
        return row if require_spec_validation_edition(row) == spec.edition else None

    def interpret_quality(row):
        passed = row is not None and row.get("outcome") == "success"
        return SpecPlanningComponent(
            "spec_validation", "ready" if passed else "blocked", "blocking", True,
            native_result=row.get("outcome") if row else "missing",
            result_ids=(row["id"],) if row else (),
            reason_codes=() if passed else ("spec_validation_required" if row is None else "spec_validation_failed",),
            next_action=None if passed else action("spec.validation.submit", "submit_spec_validation"),
        )

    await observe("spec_validation", "spec.validation.read", quality_result,
                  enabled=quality_enabled, interpret=interpret_quality)

    async def decomposition_result():
        return specs.spec_decomposition_verdict(spec, board)

    def interpret_decomposition(verdict):
        # Off does not discard independently recorded reviews from history.
        return SpecPlanningComponent(
            "decomposition", "ready" if verdict.allowed else "blocked",
            "blocking" if verdict.required else "off", True,
            native_result=(verdict.blocker or ("passed" if verdict.required else "off")),
            result_ids=tuple(row["id"] for row in (spec.evaluations or [])
                             if spec_evaluation_is_current(row, spec.edition)),
            reason_codes=("decomposition_" + verdict.blocker,) if verdict.blocker else (),
            next_action=None if verdict.allowed else action("spec.evaluations.submit", "submit_spec_evaluation"),
        )

    await observe("decomposition", "spec.evaluations.read", decomposition_result,
                  enabled=not settings.get("auto_validate", False) and not getattr(spec, "skip_qualitative_validation", False),
                  interpret=interpret_decomposition)
    await observe("requirement_lint", "spec.quality.read",
                  lambda: specs.require_planning_lint(spec), enabled=quality_enabled,
                  interpret=lambda current: SpecPlanningComponent(
                      "requirement_lint", "ready", "blocking", True,
                      native_result="accepted", result_ids=(current[0].id,),
                  ),
                  next_action=action("spec.quality.assess", "record_requirement_lint"))

    def interpret_checklist(value):
        decision, receipt_id = value
        return SpecPlanningComponent(
            "curated_checklist", "ready" if decision.allowed else "blocked",
            decision.mode.value, True, native_result=decision.reason,
            result_ids=(receipt_id,) if receipt_id else (),
            reason_codes=() if decision.allowed else (decision.reason,),
            next_action=None if decision.allowed else action("spec.checklist.execute", "complete_curated_checklist"),
        )

    await observe("curated_checklist", "spec.checklist.read", checklist, interpret=interpret_checklist)

    def interpret_policy(decision):
        if decision is None:
            return SpecPlanningComponent("policy_compliance", "ready", "off", False, native_result="not_applicable")
        unavailable = any(reason.value == "policy_assessment_unavailable" for reason in decision.reason_codes)
        if (not decision.policy_compliance_required
                or decision.reason_code.value == "policy_compliance_not_applicable"):
            return SpecPlanningComponent(
                "policy_compliance", "ready", "off", False,
                native_result=decision.reason_code.value,
            )
        return SpecPlanningComponent(
            "policy_compliance", "unavailable" if unavailable else "ready" if decision.allowed else "blocked",
            None if unavailable else "blocking" if not decision.allowed or decision.applicable_blocking_metric_count else "advisory", True,
            native_result=decision.reason_code.value, result_ids=tuple(decision.receipt_ids),
            reason_codes=tuple(reason.value for reason in decision.reason_codes),
            next_action=None if decision.allowed else action("guidelines.assessments.record", "complete_policy_compliance"),
        )

    await observe("policy_compliance", "guidelines.assessments.read", policy, interpret=interpret_policy)

    def interpret_code_evidence(evaluation):
        # The native predicate checks projection integrity even when the human
        # coverage skip applies. Only its successful result may report Off.
        skipped = getattr(evaluation, "evidence_coverage_skipped", False) is True
        return SpecPlanningComponent(
            "code_evidence", "ready", "off" if skipped else "blocking", True,
            native_result="off" if skipped else "passed",
        )

    for key, method, skip, field in PLANNING_COVERAGE_CHECKS:
        enabled = not (skip and (getattr(spec, skip, False) or settings.get(skip + "_global", False)))
        if key == "acceptance_coverage":
            enabled = enabled and quality_enabled
        await observe(
            key, ("spec.entity.read", *_COVERAGE_READ_PERMISSIONS[key]),
            lambda method=method: getattr(cards, method)(spec, board),
            enabled=enabled or key == "code_evidence", applicable=bool(getattr(spec, field, None)) if field else True,
            next_action=action("spec.entity.edit_fields", "review_planning_content"),
            interpret=interpret_code_evidence if key == "code_evidence" else None,
        )
    await observe(
        "architecture", ("spec.entity.read", "spec.architecture.read"),
        lambda: resources.validate_or_raise_spec_architecture_validation_resource(
            spec.board_id, spec.id, board=board, phase="spec_validation"),
        enabled=resources.is_spec_architecture_required_for_validation(board),
        next_action=action("spec.entity.edit_fields", "review_architecture"),
    )
    await observe(
        "resource_coverage", "spec.entity.read",
        lambda: resources.validate_or_raise_spec_resource_task_coverage(
            spec.board_id, spec.id, phase="spec_validation", enabled=True),
        enabled=quality_enabled and resources.is_spec_resource_task_coverage_required(board),
        next_action=action("spec.entity.edit_fields", "review_resource_task_coverage"),
    )
    return SpecPlanningReadiness(tuple(results[key] for key in PLANNING_COMPONENTS))
