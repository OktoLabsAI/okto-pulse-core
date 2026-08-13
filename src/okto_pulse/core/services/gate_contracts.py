"""R4-IMP1 — normalized operational gate / transition contracts.

A single predictable envelope for gate blocks across MCP / REST / UI, so callers
(agents, the IDE, the SPA) get machine-readable ``gate_type`` / ``required_tool`` /
``blocked_transition`` / ``next_action`` instead of having to parse free text.

R4 does NOT change the state machine: these gates BLOCK exactly the same
transitions they blocked before (no auto-promotion). They only enrich the error
contract. ``GateContractError`` subclasses ``ValueError`` so existing
``except ValueError`` / ``pytest.raises(ValueError)`` call sites keep working, and
the human-readable ``message`` preserves the legacy copy.

Envelope (codex-approved shape):
    {
      "code": "<machine code>",
      "message": "<human, actionable, legacy-compatible>",
      "details": {
        "gate_type", "entity_type", "entity_id", "current_status",
        "blocked_transition", "required_status", "required_tool",
        "follow_up_tool", "operator_action", "next_action",
        "enforcement_mode", "enforcement_active", "would_block_done"
      }
    }
Not-applicable fields are present as ``null`` for a predictable shape; the
applicable ones (``gate_type`` always; ``required_tool`` / ``blocked_transition``
when relevant) are always populated.
"""

from __future__ import annotations

from typing import Any, Callable

# gate_type vocabulary (structured contract field, NOT a persisted enum).
GATE_SPEC_VALIDATION = "spec_validation"
GATE_SPEC_CHECKLIST = "spec_checklist"
GATE_SPEC_QUALITATIVE_EVALUATION = "spec_qualitative_evaluation"
GATE_TEST_CARD_COMPLETION = "test_card_completion"
GATE_RESOURCE = "resource_gate"
GATE_COGNITIVE_READINESS = "cognitive_readiness"
GATE_STATE_TRANSITION = "state_transition"
GATE_SPEC_DEPENDENCIES = "spec_dependencies"

_DETAIL_KEYS = (
    "gate_type",
    "entity_type",
    "entity_id",
    "current_status",
    "blocked_transition",
    "required_status",
    "required_tool",
    "follow_up_tool",
    "operator_action",
    "next_action",
    "enforcement_mode",
    "enforcement_active",
    "would_block_done",
)


class GateContractError(ValueError):
    """A gate block carrying the normalized operational contract. Subclasses
    ``ValueError`` so legacy call sites and tests still treat it as one; the
    structured envelope is available via :meth:`to_dict`."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        gate_type: str,
        entity_type: str | None = None,
        entity_id: str | None = None,
        current_status: str | None = None,
        blocked_transition: str | None = None,
        required_status: str | None = None,
        required_tool: str | None = None,
        follow_up_tool: str | None = None,
        operator_action: str | None = None,
        next_action: dict[str, Any] | None = None,
        enforcement_mode: str | None = None,
        enforcement_active: bool | None = None,
        would_block_done: bool | None = None,
        extra_details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.gate_type = gate_type
        self._details: dict[str, Any] = {
            "gate_type": gate_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "current_status": current_status,
            "blocked_transition": blocked_transition,
            "required_status": required_status,
            "required_tool": required_tool,
            "follow_up_tool": follow_up_tool,
            "operator_action": operator_action,
            "next_action": next_action,
            "enforcement_mode": enforcement_mode,
            "enforcement_active": enforcement_active,
            "would_block_done": would_block_done,
        }
        if extra_details:
            self._details.update(extra_details)

    @property
    def details(self) -> dict[str, Any]:
        return dict(self._details)

    def to_dict(self) -> dict[str, Any]:
        """The normalized envelope. ``error`` mirrors ``code`` so callers that key
        on ``error`` (legacy MCP shape) keep working."""
        return {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "details": dict(self._details),
        }


# ---------------------------------------------------------------------------
# Builders — one per R4-IMP1 gate point (keep the legacy human message verbatim
# so text-matching call sites/tests stay green).
# ---------------------------------------------------------------------------


def spec_validation_gate_error(*, spec_id: str, current_status: str) -> GateContractError:
    """move_spec approved->validated direct is blocked when the Spec Validation
    Gate is on; the only path to validated is the semantic gate tool."""
    return GateContractError(
        code="spec_validation_gate_required",
        message=(
            "Spec Validation Gate is enabled on this board. Direct "
            "approved→validated is blocked — submit a spec validation "
            "via okto_pulse_submit_spec_validation (or the IDE Validate "
            "button) to go through the semantic quality gate."
        ),
        gate_type=GATE_SPEC_VALIDATION,
        entity_type="spec",
        entity_id=spec_id,
        current_status=current_status,
        blocked_transition=f"{current_status}->validated",
        required_status="validated",
        required_tool="okto_pulse_submit_spec_validation",
        follow_up_tool="okto_pulse_move_spec",
        operator_action=(
            "Submit a spec validation to run the semantic quality gate; the gate "
            "promotes the spec to validated. Direct move_spec to validated is blocked."
        ),
        next_action={
            "tool": "okto_pulse_submit_spec_validation",
            "params": {"spec_id": spec_id},
            "hint": "Run the semantic validation gate instead of moving directly.",
        },
        enforcement_mode="enforced",
        enforcement_active=True,
    )


def spec_checklist_gate_error(
    *,
    spec_id: str,
    current_status: str,
    reason: str,
    mode: str,
    stale_reasons: tuple[str, ...] = (),
) -> GateContractError:
    """Block Spec validation when the effective A3 binding is unsatisfied."""

    return GateContractError(
        code="spec_checklist_gate_required",
        message=(
            "The blocking Spec checklist is not satisfied for the current "
            "Spec version. Complete and pass the current /specify checklist "
            "before validating the Spec."
        ),
        gate_type=GATE_SPEC_CHECKLIST,
        entity_type="spec",
        entity_id=spec_id,
        current_status=current_status,
        blocked_transition=f"{current_status}->validated",
        required_status="validated",
        required_tool="okto_pulse_submit_checklist_execution",
        follow_up_tool="okto_pulse_submit_spec_validation",
        operator_action=(
            "Resolve the current checklist binding, start an execution, submit "
            "all ten item results, and retry Spec validation."
        ),
        next_action={
            "tool": "okto_pulse_get_checklist_binding",
            "params": {"spec_id": spec_id},
            "hint": (
                "Inspect the effective binding and current receipt before "
                "starting a new checklist execution."
            ),
        },
        enforcement_mode=mode,
        enforcement_active=True,
        extra_details={
            "checklist_reason": reason,
            "stale_reasons": list(stale_reasons),
        },
    )


def _update_test_scenario_next_action(
    *, board_id: str | None, spec_id: str | None, card_id: str,
    pending_scenario_ids: list[str],
) -> dict[str, Any]:
    """Shared next_action for the test-card completion flow (R4-IMP3 contract).

    ``params_template`` mirrors the REAL okto_pulse_update_test_scenario_status
    signature (board_id, spec_id, scenario_id SINGULAR, status, evidence) — one call
    per pending scenario — so an agent never trial-and-errors invalid params.
    ``follow_up`` is the real okto_pulse_move_card(board_id, card_id, status). The
    pending list stays a SEPARATE ``scenario_ids`` metadatum, NEVER inside the tool
    params (no card_id / no plural scenario_ids in the update tool params)."""
    return {
        "tool": "okto_pulse_update_test_scenario_status",
        "params_template": {
            "board_id": board_id,
            "spec_id": spec_id,
            "scenario_id": "<pending_scenario_id>",
            "status": "passed",
            "evidence": "<required when marking passed/failed/automated>",
        },
        "scenario_ids": list(pending_scenario_ids),
        "follow_up": {
            "tool": "okto_pulse_move_card",
            "params": {"board_id": board_id, "card_id": card_id, "status": "done"},
        },
        "hint": (
            "Call okto_pulse_update_test_scenario_status once per pending scenario_id "
            "(status='passed'/'automated' with evidence), then "
            "okto_pulse_move_card(status='done')."
        ),
    }


def _link_test_scenario_next_action(
    *,
    board_id: str | None,
    spec_id: str | None,
    card_id: str,
) -> dict[str, Any]:
    """Recovery action for an empty or dangling test-scenario reference set."""

    return {
        "tool": "okto_pulse_link_task",
        "params_template": {
            "board_id": board_id,
            "target_type": "scenario",
            "target_id": "<existing_scenario_id>",
            "card_id": card_id,
            "spec_id": spec_id,
        },
        "follow_up": {
            "tool": "okto_pulse_get_task_context",
            "params": {"board_id": board_id, "card_id": card_id},
        },
        "hint": (
            "Link at least one existing scenario to the test card, then re-read "
            "task context and execute every linked scenario before completion."
        ),
    }


def task_validation_unsupported_for_test_card_error(
    *, card_id: str, board_id: str | None = None, spec_id: str | None = None,
) -> GateContractError:
    """submit_task_validation on a test card: the normal task-validation gate does
    not apply to test cards -- update scenario statuses and move the card to done."""
    return GateContractError(
        code="test_card_not_subject_to_task_validation",
        message=(
            "Card type 'test' is not subject to the task validation gate. Update "
            "its linked scenario statuses with okto_pulse_update_test_scenario_status, "
            "then complete it with okto_pulse_move_card(status='done')."
        ),
        gate_type=GATE_TEST_CARD_COMPLETION,
        entity_type="card",
        entity_id=card_id,
        required_tool="okto_pulse_update_test_scenario_status",
        follow_up_tool="okto_pulse_move_card",
        operator_action=(
            "Test cards bypass task validation: update scenario statuses, then move "
            "the card to done."
        ),
        next_action=_update_test_scenario_next_action(
            board_id=board_id, spec_id=spec_id, card_id=card_id, pending_scenario_ids=[],
        ),
    )


def incomplete_test_card_completion_error(
    *,
    card_id: str,
    current_status: str | None,
    pending_scenarios: list[dict[str, Any]],
    board_id: str | None = None,
    spec_id: str | None = None,
) -> GateContractError:
    """move_card to done on a test card with linked scenarios still draft/ready.

    ``pending_scenarios`` is a list of ``{"id", "title", "status"}`` so the contract
    is operationally actionable (which scenarios, on which spec), not text-only."""
    total = len(pending_scenarios)
    titles = [str(s.get("title") or s.get("id")) for s in pending_scenarios]
    scenario_ids = [str(s.get("id")) for s in pending_scenarios if s.get("id")]
    missing = [
        s for s in pending_scenarios if str(s.get("status") or "") == "missing"
    ]
    shown = ", ".join(f'"{t}"' for t in titles[:3])
    suffix = f" and {total - 3} more" if total > 3 else ""
    if missing:
        message = (
            "Cannot complete this test card: its scenario linkage is empty or "
            f"contains {len(missing)} unresolved reference(s) ({shown}{suffix}). "
            "Link existing scenarios and execute them before completing the card."
        )
        required_tool = "okto_pulse_link_task"
        operator_action = (
            "Repair the missing/unresolved scenario linkage, execute every linked "
            "scenario with evidence, then retry the done transition."
        )
        next_action = _link_test_scenario_next_action(
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
        )
    else:
        message = (
            f"Cannot complete this test card: {total} linked scenario(s) are not "
            f"completion-ready ({shown}{suffix}). Update scenario statuses to "
            "'automated' or 'passed' using okto_pulse_update_test_scenario_status "
            "before completing the card."
        )
        required_tool = "okto_pulse_update_test_scenario_status"
        operator_action = (
            f"Move {total} pending scenario(s) to automated/passed with evidence, "
            "then move the test card to done."
        )
        next_action = _update_test_scenario_next_action(
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
            pending_scenario_ids=scenario_ids,
        )
    return GateContractError(
        code="test_card_completion_blocked",
        message=message,
        gate_type=GATE_TEST_CARD_COMPLETION,
        entity_type="card",
        entity_id=card_id,
        current_status=current_status,
        blocked_transition=(f"{current_status}->done" if current_status else None),
        required_status="done",
        required_tool=required_tool,
        follow_up_tool="okto_pulse_move_card",
        operator_action=operator_action,
        next_action=next_action,
        enforcement_mode="enforced",
        enforcement_active=True,
        would_block_done=True,
        extra_details={
            "unready_scenario_count": total,
            "pending_scenarios": [
                {"id": s.get("id"), "title": s.get("title"), "status": s.get("status")}
                for s in pending_scenarios
            ],
        },
    )


# Linked-scenario statuses that still BLOCK a test card from done (mirror of the
# move_card test_card_completion gate); automated/passed are complete.
_BLOCKING_SCENARIO_STATUSES = ("draft", "ready")
_SCENARIO_EVIDENCE_FIELDS = (
    "test_run_id", "output_snippet", "last_run_at", "test_file_path", "test_function",
)


def _scenario_evidence_present(
    scenario: dict[str, Any],
    *,
    evidence_validator: Callable[[dict[str, Any]], bool] | None = None,
) -> bool:
    # Keep the proactive task context aligned with the actual card/sprint gate:
    # a non-empty object is not proof. Canonical Evidence V2 must pass the same
    # semantic verifier used by every mutation path.
    from okto_pulse.core.services.test_scenario_lifecycle import (
        GATED_STATUSES,
        scenario_has_required_evidence,
    )

    status = str(scenario.get("status") or "")
    if status in GATED_STATUSES:
        candidate = scenario
        if not isinstance(scenario.get("evidence"), dict):
            legacy_top_level = {
                field: scenario.get(field)
                for field in _SCENARIO_EVIDENCE_FIELDS
                if scenario.get(field)
            }
            candidate = {**scenario, "evidence": legacy_top_level or None}
        evidence = candidate.get("evidence") or candidate.get("latest_evidence")
        claims_v2 = bool(
            isinstance(evidence, dict)
            and (
                evidence.get("manifest_ref") is not None
                or evidence.get("execution_attestation") is not None
                or evidence.get("execution_receipt") is not None
            )
        )
        if evidence_validator is not None:
            return evidence_validator(candidate)
        if claims_v2:
            return False
        return scenario_has_required_evidence(candidate)
    return False


def operational_flow_for_test_card(
    *,
    card_id: str,
    board_id: str | None,
    spec_id: str | None,
    current_status: str | None,
    linked_scenarios: list[dict[str, Any]],
    expected_scenario_ids: list[str] | None = None,
    evidence_validator: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any]:
    """R4-IMP3 — read-only PROACTIVE operational-flow block for a test card in
    get_task_context. Reuses the R4-IMP1 test_card_completion contract fields so the
    operator/agent sees the path (update scenarios -> move done) BEFORE hitting the
    gate. Not a state-machine change; ``mutation_allowed=false``."""
    scenarios = [
        {
            "id": s.get("id"),
            "title": s.get("title"),
            "status": s.get("status"),
            "evidence_present": _scenario_evidence_present(
                s, evidence_validator=evidence_validator
            ),
        }
        for s in linked_scenarios
    ]
    pending = [
        {"id": s["id"], "title": s["title"], "status": s["status"]}
        for s in scenarios
        if (
            str(s["status"] or "") in _BLOCKING_SCENARIO_STATUSES
            or (
                str(s["status"] or "") in {"automated", "passed", "failed"}
                and not s["evidence_present"]
            )
        )
    ]
    # A caller may provide the persisted reference set so partial resolution
    # (one valid scenario plus one dangling id) is also detectable.  The
    # parameter is optional for backward compatibility; an entirely empty
    # resolved set still always fails closed.
    resolved_ids = {
        str(scenario["id"])
        for scenario in scenarios
        if scenario.get("id") is not None
    }
    expected_ids = (
        [str(value) for value in expected_scenario_ids]
        if expected_scenario_ids is not None
        else None
    )
    unresolved_ids = (
        [value for value in expected_ids if value not in resolved_ids]
        if expected_ids is not None
        else []
    )
    if unresolved_ids:
        pending.extend(
            {
                "id": scenario_id,
                "title": f"Unresolved test scenario {scenario_id}",
                "status": "missing",
            }
            for scenario_id in unresolved_ids
        )
    elif not scenarios:
        pending = [
            {
                "id": None,
                "title": "No linked test scenario could be resolved",
                "status": "missing",
            }
        ]
    already_done = str(current_status or "").strip().lower() == "done"
    would_block_done = bool(pending) and not already_done
    follow_up_tool = None if already_done else "okto_pulse_move_card"
    if already_done:
        operator_action = (
            "The test card is already done; no completion transition is required."
        )
        required_tool = None
        next_action = None
    elif would_block_done:
        has_linkage_failure = any(
            str(item.get("status") or "") == "missing" for item in pending
        )
        if has_linkage_failure:
            operator_action = (
                "No linked scenario could be resolved. Repair the scenario "
                "linkage and execute the linked scenarios before completion."
            )
            required_tool = "okto_pulse_link_task"
            next_action = _link_test_scenario_next_action(
                board_id=board_id,
                spec_id=spec_id,
                card_id=card_id,
            )
        else:
            operator_action = (
                f"Update {len(pending)} pending scenario(s) to automated/passed "
                "with evidence, then move the test card to done."
            )
            required_tool = "okto_pulse_update_test_scenario_status"
            # R4-IMP3: shared test-card next_action (params_template fiel à
            # assinatura real do tool singular + follow_up move_card).
            next_action = _update_test_scenario_next_action(
                board_id=board_id,
                spec_id=spec_id,
                card_id=card_id,
                pending_scenario_ids=[
                    str(p["id"]) for p in pending if p.get("id")
                ],
            )
    else:
        operator_action = (
            "The resolved linked scenarios are automated/passed; move the test "
            "card to done."
        )
        required_tool = "okto_pulse_update_test_scenario_status"
        next_action = {
            "tool": "okto_pulse_move_card",
            "params": {"board_id": board_id, "card_id": card_id, "status": "done"},
            "hint": "Linked scenarios are complete; move the test card to done.",
        }
    return {
        "card_type": "test",
        # Test cards are NOT subject to the task-validation gate (R4-IMP1); the
        # completion path is scenario status + move_card(done).
        "applies_to_task_validation": False,
        "gate_type": GATE_TEST_CARD_COMPLETION,
        "current_status": current_status,
        "would_block_done": would_block_done,
        "linked_scenarios": scenarios,
        "pending_scenarios": pending,
        "required_tool": required_tool,
        "follow_up_tool": follow_up_tool,
        "operator_action": operator_action,
        "next_action": next_action,
        "mutation_allowed": False,
    }


# ---------------------------------------------------------------------------
# R4-IMP4 — read-only gate/readiness CONTEXT block (get_spec_context /
# get_task_context). Pure assembly over already-derived primitives so the
# context surfaces the SAME gate fields the gates enforce (parity by
# construction), with an explicit, auditable ``consistency`` self-check. These
# builders are read-only and NEVER include a skip/no_action tool — a context
# must not induce an agent to bypass a gate (codex restriction).
# ---------------------------------------------------------------------------

GATE_READINESS_SOURCE = "gate_contracts+cognitive_readiness"


def _consistency_ok(source: str = GATE_READINESS_SOURCE) -> dict[str, Any]:
    """Enxuto quando não há divergência (codex Q2)."""
    return {"checked": True, "mismatch": False, "source": source}


def _consistency_mismatch(
    *, expected: Any, observed: Any, reason: str, source: str = GATE_READINESS_SOURCE,
) -> dict[str, Any]:
    """Auditável: ``expected``/``observed``/``reason`` quando contexto e gate divergem."""
    return {
        "checked": True,
        "mismatch": True,
        "source": source,
        "expected": expected,
        "observed": observed,
        "reason": reason,
    }


def spec_gate_readiness(
    *,
    spec_id: str,
    spec_status: str,
    require_spec_validation: bool,
    cognitive_enforcement_active: bool,
    checklist_mode: str | None = None,
    checklist_allowed: bool | None = None,
    checklist_reason: str | None = None,
    checklist_current: bool | None = None,
    checklist_stale_reasons: tuple[str, ...] = (),
    dependency_readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read-only gate/readiness summary for a spec context (R4-IMP4).

    Spec context does NOT aggregate per-card cognitive verdicts (codex Q1):
    cognitive readiness is per-artifact/card and lives in ``get_task_context``.
    Here we expose only the board's cognitive enforcement posture + the spec's
    OWN transition gate (``spec_validation`` when ``approved`` and the board
    requires validation), derived from the SAME builder ``move_spec`` raises.
    """
    active_gates: list[dict[str, Any]] = []
    dependency_readiness = dict(dependency_readiness or {})
    dependency_gate_applicable = (
        spec_status == "validated"
        and int(dependency_readiness.get("blocking_count", 0) or 0) > 0
    )
    if dependency_gate_applicable:
        archived_blocking_count = int(
            dependency_readiness.get("archived_blocking_count", 0) or 0
        )
        unfinished_blocking_count = int(
            dependency_readiness.get("unfinished_blocking_count", 0) or 0
        )
        operator_action = "Complete every unfinished prerequisite Spec."
        if archived_blocking_count > 0:
            operator_action = (
                "Restore archived prerequisite Specs (and complete them when "
                "needed), complete unfinished prerequisite Specs, or remove "
                "the blocking dependencies."
            )
        active_gates.append(
            {
                "gate_type": GATE_SPEC_DEPENDENCIES,
                "blocked_transition": "validated_to_in_progress",
                "required_status": "done",
                "required_tool": "okto_pulse_list_spec_dependencies",
                "follow_up_tool": "okto_pulse_get_spec_context",
                "operator_action": operator_action,
                "enforcement_mode": "blocking",
                "enforcement_active": True,
                "blocking_count": int(dependency_readiness.get("blocking_count", 0)),
                "archived_blocking_count": archived_blocking_count,
                "unfinished_blocking_count": unfinished_blocking_count,
                "blockers_truncated": bool(
                    dependency_readiness.get("blockers_truncated", False)
                ),
                "blockers": list(dependency_readiness.get("blockers", ())),
            }
        )
    spec_validation_applicable = (
        spec_status == "approved" and bool(require_spec_validation)
    )
    if spec_validation_applicable:
        details = spec_validation_gate_error(
            spec_id=spec_id, current_status=spec_status,
        ).details
        active_gates.append(
            {
                "gate_type": details["gate_type"],
                "blocked_transition": details["blocked_transition"],
                "required_status": details["required_status"],
                "required_tool": details["required_tool"],
                "follow_up_tool": details["follow_up_tool"],
                "operator_action": details["operator_action"],
                "enforcement_mode": details["enforcement_mode"],
                "enforcement_active": details["enforcement_active"],
            }
        )

    checklist_stale_reasons = tuple(checklist_stale_reasons)
    checklist_gate_applicable = (
        spec_status == "approved"
        and checklist_mode == "blocking"
        and checklist_allowed is False
    )
    if checklist_gate_applicable:
        assert checklist_reason is not None
        details = spec_checklist_gate_error(
            spec_id=spec_id,
            current_status=spec_status,
            reason=checklist_reason,
            mode=checklist_mode,
            stale_reasons=checklist_stale_reasons,
        ).details
        active_gates.append(
            {
                "gate_type": details["gate_type"],
                "blocked_transition": details["blocked_transition"],
                "required_status": details["required_status"],
                "required_tool": details["required_tool"],
                "follow_up_tool": details["follow_up_tool"],
                "operator_action": details["operator_action"],
                "next_action": details["next_action"],
                "enforcement_mode": details["enforcement_mode"],
                "enforcement_active": details["enforcement_active"],
                "checklist_reason": details["checklist_reason"],
                "stale_reasons": details["stale_reasons"],
            }
        )

    # Consistency: surfaced gate presence MUST equal both independently
    # recomputed predicates. A future refactor that diverges from either
    # canonical gate surfaces the mismatch instead of misleading the operator.
    observed = {
        "spec_validation_gate": any(
            gate["gate_type"] == GATE_SPEC_VALIDATION for gate in active_gates
        ),
        "spec_checklist_gate": any(
            gate["gate_type"] == GATE_SPEC_CHECKLIST for gate in active_gates
        ),
        "spec_dependency_gate": any(
            gate["gate_type"] == GATE_SPEC_DEPENDENCIES for gate in active_gates
        ),
    }
    expected = {
        "spec_validation_gate": spec_validation_applicable,
        "spec_checklist_gate": checklist_gate_applicable,
        "spec_dependency_gate": dependency_gate_applicable,
    }
    if observed != expected:
        consistency = _consistency_mismatch(
            expected=expected,
            observed=observed,
            reason=(
                "Spec gate presence diverged from the canonical validation "
                "and curated-checklist predicates."
            ),
            source="gate_contracts",
        )
    else:
        consistency = _consistency_ok(source="gate_contracts")

    readiness = {
        "cognitive_enforcement_active": bool(cognitive_enforcement_active),
        "cognitive_enforcement_mode": (
            "enforced" if cognitive_enforcement_active else "advisory"
        ),
        "active_gates": active_gates,
        "note": (
            "Cognitive readiness is per-card; call okto_pulse_get_task_context for "
            "each card's verdict. This block does NOT aggregate card-level cognitive "
            "blocks."
        ),
        "mutation_allowed": False,
        "consistency": consistency,
        "spec_dependencies": dependency_readiness,
    }
    if checklist_mode is not None:
        readiness["spec_checklist"] = {
            "mode": checklist_mode,
            "allowed": bool(checklist_allowed),
            "reason": checklist_reason,
            "current": checklist_current,
            "stale_reasons": list(checklist_stale_reasons),
            "enforcement_active": checklist_mode == "blocking",
            "binding_tool": "okto_pulse_get_checklist_binding",
            "execution_tool": "okto_pulse_start_checklist_execution",
            "receipt_tool": "okto_pulse_get_checklist_receipt",
        }
    return readiness


def task_gate_readiness(
    *,
    card_status: str,
    cognitive_enforcement_active: bool,
    cognitive_verdict: dict[str, Any] | None,
    operational_flow: dict[str, Any] | None,
) -> dict[str, Any]:
    """Read-only gate/readiness summary for a task context (R4-IMP4).

    Surfaces the card's OWN cognitive verdict (``would_block_done`` is
    enforcement-aware — ``blocking`` alone never blocks the done-gate) plus the
    single transition gate currently relevant to completing the card:

    * test card  -> reuse ``operational_flow_for_test_card`` (parity w/ R4-IMP3);
    * else, if the cognitive verdict would block done under the active policy ->
      a ``cognitive_readiness`` gate pointing at the EVALUATE tool (never a skip).

    ``mutation_allowed`` is False; this block never induces an agent to skip a
    gate.
    """
    active_gate: dict[str, Any] | None = None
    if operational_flow is not None:
        # Single source: the already-computed test-card operational flow. The blocked
        # transition is derived from the flow's own current_status ("<status>->done")
        # so the context exposes the actionable transition, not just the gate type.
        flow_status = operational_flow.get("current_status")
        terminal = any(
            str(value or "").strip().lower() == "done"
            for value in (card_status, flow_status)
        )
        if not terminal:
            active_gate = {
                "gate_type": operational_flow.get("gate_type"),
                "blocked_transition": f"{flow_status}->done" if flow_status else None,
                "required_status": "done",
                "required_tool": operational_flow.get("required_tool"),
                "follow_up_tool": operational_flow.get("follow_up_tool"),
                "would_block_done": bool(operational_flow.get("would_block_done")),
            }
    elif cognitive_verdict and cognitive_verdict.get("would_block_done"):
        active_gate = {
            "gate_type": GATE_COGNITIVE_READINESS,
            "blocked_transition": f"{card_status}->done",
            "required_status": "done",
            "required_tool": "okto_pulse_kg_evaluate_cognitive_readiness",
            "follow_up_tool": "okto_pulse_move_card",
            "would_block_done": True,
        }

    # consistency self-checks (genuine invariants; not tautological):
    mismatch: dict[str, Any] | None = None
    # inv1: would_block_done is enforcement-aware — a blocking verdict with the
    # board policy OFF must NOT block done (advisory != blocking).
    if (
        cognitive_verdict
        and cognitive_verdict.get("would_block_done")
        and not cognitive_enforcement_active
    ):
        mismatch = _consistency_mismatch(
            expected={"would_block_done": False, "when": "enforcement_active=false"},
            observed={"would_block_done": True},
            reason="cognitive would_block_done must be enforcement-aware, not blocking-only",
        )
    # inv2: a cognitive gate is surfaced IFF the verdict would block done.
    elif (
        active_gate
        and active_gate["gate_type"] == GATE_COGNITIVE_READINESS
        and not (cognitive_verdict and cognitive_verdict.get("would_block_done"))
    ):
        mismatch = _consistency_mismatch(
            expected={"cognitive_gate": False},
            observed={"cognitive_gate": True},
            reason="cognitive_readiness gate surfaced without a would_block_done verdict",
        )
    # inv3 (test card): the gate's would_block_done MUST equal the single-source
    # operational flow it was derived from.
    elif (
        operational_flow is not None
        and active_gate is not None
        and active_gate["would_block_done"] != bool(operational_flow.get("would_block_done"))
    ):
        mismatch = _consistency_mismatch(
            expected={"would_block_done": bool(operational_flow.get("would_block_done"))},
            observed={"would_block_done": active_gate["would_block_done"]},
            reason="test_card gate would_block_done diverged from operational_flow",
        )

    return {
        "cognitive_enforcement_active": bool(cognitive_enforcement_active),
        "cognitive_enforcement_mode": (
            "enforced" if cognitive_enforcement_active else "advisory"
        ),
        "cognitive_readiness": cognitive_verdict,
        "active_gate": active_gate,
        "mutation_allowed": False,
        "consistency": mismatch if mismatch is not None else _consistency_ok(),
    }


# ---------------------------------------------------------------------------
# R5-IMP1 — human-only control for skip / no_action mutations.
#
# Applying or clearing a skip / no_action (or toggling the ambiguity-gate skip)
# is a HUMAN decision. The agent-facing MCP surface must never decide or apply
# it: the skip tools fail closed with this envelope BEFORE any write-path call,
# so no ledger / skip_ambiguity_gate / override is touched (state_changed=false,
# mutation_allowed=false). The deeper service guard (``_assert_human_for_r7_hold``
# + ``actor_is_human``) stays the single authorization path for the human REST
# surface — this is the agent-boundary enforcement, not a second auth path.
# ---------------------------------------------------------------------------

GATE_HUMAN_CONTROL_REQUIRED = "human_control_required"


def human_control_required_envelope(
    *, blocked_tool: str, blocked_action: str, target_ref: str | None = None,
) -> dict[str, Any]:
    """Read-only refusal payload for an agent-facing skip / no_action mutation.

    The message asks for a HUMAN decision via the UI / human REST control and
    NEVER instructs the agent to apply the skip itself."""
    return {
        "error": GATE_HUMAN_CONTROL_REQUIRED,
        "code": GATE_HUMAN_CONTROL_REQUIRED,
        "message": (
            "This is a human-only control. Applying or clearing a skip / "
            "no_action (or changing the ambiguity-gate skip) requires an explicit "
            "human decision through the UI or the human REST surface — an agent "
            "must not decide or apply it. The gate stays in place; escalate to a "
            "human operator."
        ),
        "details": {
            "mutation_allowed": False,
            "state_changed": False,
            "required_surface": "ui|human_rest",
            "required_actor": "human",
            "blocked_tool": blocked_tool,
            "blocked_action": blocked_action,
            "target_ref": target_ref,
        },
    }


def spec_evaluation_success_envelope(
    *, spec_id: str, status_before: str, status_after: str, evaluation: dict[str, Any],
) -> dict[str, Any]:
    """submit_spec_evaluation is append-only and does NOT change the spec status.
    The success body reports the independently-captured before/after status (so a
    future regression that changed status surfaces as state_changed=true) and points
    at the operator's next step (move_spec to in_progress) -- never an auto-transition."""
    return {
        "success": True,
        "evaluation": evaluation,
        "state_changed": status_before != status_after,
        "status_before": status_before,
        "status_after": status_after,
        "next_action": {
            "tool": "okto_pulse_move_spec",
            "params": {"spec_id": spec_id, "status": "in_progress"},
            "hint": (
                "Evaluation recorded (append-only). Move the spec to in_progress when "
                "ready to begin execution."
            ),
        },
    }


__all__ = [
    "GATE_COGNITIVE_READINESS",
    "GATE_RESOURCE",
    "GATE_SPEC_CHECKLIST",
    "GATE_SPEC_DEPENDENCIES",
    "GATE_SPEC_QUALITATIVE_EVALUATION",
    "GATE_SPEC_VALIDATION",
    "GATE_STATE_TRANSITION",
    "GATE_TEST_CARD_COMPLETION",
    "GATE_HUMAN_CONTROL_REQUIRED",
    "GATE_READINESS_SOURCE",
    "GateContractError",
    "human_control_required_envelope",
    "incomplete_test_card_completion_error",
    "operational_flow_for_test_card",
    "spec_evaluation_success_envelope",
    "spec_checklist_gate_error",
    "spec_gate_readiness",
    "spec_validation_gate_error",
    "task_gate_readiness",
    "task_validation_unsupported_for_test_card_error",
]
