"""Allowed transition read model for lifecycle UI/action surfaces.

The transition authority stays in the services that enforce state moves. This
module projects those same programmatic tables into a read model for UI and MCP
consumers, so display affordances cannot drift into a parallel lifecycle map.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Mapping, Sequence

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
)
from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    lifecycle_definition,
    transition_contracts,
)
from okto_pulse.core.domain.card_transition import (
    CardTransitionFacts,
    PendingScenario,
    bug_regression_gate_applies,
    evaluate_card_transition,
)
from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    SpecStatus,
    SprintLaneType,
    SprintStatus,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog

ALLOWED_TRANSITIONS_SOURCE = "core_sdlc_registry_v1"
ALLOWED_TRANSITIONS_DRIFT_METRIC = "allowed_transitions_contract_drift_total"


@dataclass(frozen=True)
class AllowedTransition:
    to_status: str
    label: str
    gate: str
    blocked_reason: str | None = None
    preconditions: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    effects: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "to_status": self.to_status,
            "label": self.label,
            "gate": self.gate,
            "blocked_reason": self.blocked_reason,
            "preconditions": list(self.preconditions),
            "capabilities": list(self.capabilities),
            "effects": list(self.effects),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class AllowedTransitionsReadModel:
    board_id: str
    entity_type: str
    entity_id: str | None
    current_status: str
    allowed_transitions: list[AllowedTransition]
    source: str = ALLOWED_TRANSITIONS_SOURCE

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "current_status": self.current_status,
            "allowed_transitions": [item.to_dict() for item in self.allowed_transitions],
            "source": self.source,
        }


@dataclass(frozen=True)
class AllowedTransitionDriftReport:
    metric_name: str
    drift_total: int
    missing_edges: list[tuple[str, str, str]]
    extra_edges: list[tuple[str, str, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "drift_total": self.drift_total,
            "missing_edges": list(self.missing_edges),
            "extra_edges": list(self.extra_edges),
        }


class ListAllowedTransitionsCommand:
    __slots__ = ("board_id", "entity_type", "entity_id", "current_status")

    def __init__(
        self,
        board_id: str,
        entity_type: str,
        *,
        entity_id: str | None = None,
        current_status: str | None = None,
    ) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.current_status = current_status


class ListAllowedTransitionsResult:
    __slots__ = ("read_model",)

    def __init__(self, read_model: AllowedTransitionsReadModel) -> None:
        self.read_model = read_model


def _authority_for(entity_type: str):
    normalized = (entity_type or "").strip().lower()
    try:
        return lifecycle_definition(normalized)
    except ValueError as exc:
        allowed = ", ".join(sorted(SDLC_REGISTRY))
        raise CommandValidationError(
            f"Invalid entity_type. Must be one of: {allowed}"
        ) from exc


def _parse_status(entity_type: str, status: str) -> Enum:
    authority = _authority_for(entity_type)
    try:
        return authority.status_enum(status)
    except ValueError as exc:
        allowed = [item.value for item in authority.status_enum]
        raise CommandValidationError(
            f"Invalid status for {entity_type}. Must be one of: {allowed}"
        ) from exc


def _label_for(status: Enum) -> str:
    return str(status.value).replace("_", " ").title()


def _gate_for(entity_type: str, from_status: Enum, to_status: Enum) -> str:
    from_value = str(from_status.value)
    to_value = str(to_status.value)
    if entity_type == "spec":
        if from_value == "approved" and to_value == "validated":
            return "spec_validation"
        if from_value == "validated" and to_value == "in_progress":
            return "spec_evaluation"
        if from_value == "in_progress" and to_value == "done":
            return "coverage_and_tasks"
        if from_value in {"validated", "in_progress", "done"} and to_value in {
            "draft",
            "review",
            "approved",
        }:
            return "unlock_content"
    if entity_type == "ideation" and from_value == "evaluating" and to_value == "done":
        return "ambiguity_resource_cognitive"
    if entity_type == "refinement" and to_value == "done":
        return "resource_cognitive"
    return "none"


def allowed_transitions_for_status(
    entity_type: str,
    current_status: str,
    *,
    card_type: str | None = None,
) -> list[AllowedTransition]:
    """Project the same canonical contracts consumed by mutation services."""

    normalized = (entity_type or "").strip().lower()
    _parse_status(normalized, current_status)
    return [
        AllowedTransition(
            to_status=edge.to_status,
            label=edge.label,
            gate=edge.gate,
            preconditions=edge.preconditions,
            capabilities=edge.capabilities,
            effects=edge.effects,
            reason_codes=edge.reason_codes,
        )
        for edge in transition_contracts(normalized, current_status)
        if not edge.card_types or card_type in edge.card_types
    ]


def allowed_transition_edges() -> dict[str, dict[str, list[str]]]:
    """Return the current programmatic authority as comparable string edges."""

    edges: dict[str, dict[str, list[str]]] = {}
    for entity_type, authority in SDLC_REGISTRY.items():
        edges[entity_type] = {
            from_status: [edge.to_status for edge in to_statuses]
            for from_status, to_statuses in authority.transitions.items()
        }
    return edges


def calculate_allowed_transition_drift(
    candidate_edges: Mapping[str, Mapping[str, Sequence[str]]],
) -> AllowedTransitionDriftReport:
    """Compare a proposed/docs-derived edge set with the runtime authority."""

    expected = allowed_transition_edges()
    missing: list[tuple[str, str, str]] = []
    extra: list[tuple[str, str, str]] = []
    for entity_type, status_edges in expected.items():
        candidate_status_edges = candidate_edges.get(entity_type, {})
        for from_status, to_statuses in status_edges.items():
            expected_set = set(to_statuses)
            candidate_set = set(candidate_status_edges.get(from_status, []))
            missing.extend(
                (entity_type, from_status, to_status)
                for to_status in sorted(expected_set - candidate_set)
            )
            extra.extend(
                (entity_type, from_status, to_status)
                for to_status in sorted(candidate_set - expected_set)
            )
    for entity_type, status_edges in candidate_edges.items():
        if entity_type in expected:
            continue
        for from_status, to_statuses in status_edges.items():
            extra.extend((entity_type, from_status, to_status) for to_status in to_statuses)
    return AllowedTransitionDriftReport(
        metric_name=ALLOWED_TRANSITIONS_DRIFT_METRIC,
        drift_total=len(missing) + len(extra),
        missing_edges=missing,
        extra_edges=extra,
    )


class ListAllowedTransitionsUseCase:
    """Read allowed transitions for an entity or explicit status."""

    async def execute(
        self,
        command: ListAllowedTransitionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListAllowedTransitionsResult:
        entity_type = (command.entity_type or "").strip().lower()
        _authority_for(entity_type)
        board = await load_accessible_board(uow, command.board_id, actor)
        if not board:
            raise EntityNotFoundError("board", command.board_id)

        entity_id = (command.entity_id or "").strip() or None
        card_type: str | None = None
        if entity_id:
            entity = await self._load_entity(uow.services, entity_type, entity_id)
            if not entity or getattr(entity, "board_id", None) != command.board_id:
                raise EntityNotFoundError(entity_type, entity_id)
            current_status = str(entity.status.value)
            if entity_type == "card":
                raw_card_type = getattr(entity, "card_type", None)
                card_type = str(getattr(raw_card_type, "value", raw_card_type or "normal"))
        else:
            if not command.current_status:
                raise CommandValidationError(
                    "current_status is required when entity_id is not provided"
                )
            current_status = command.current_status
            _parse_status(entity_type, current_status)

        transitions = allowed_transitions_for_status(
            entity_type, current_status, card_type=card_type
        )
        if entity_id:
            transitions = [
                replace(
                    transition,
                    blocked_reason=await self._blocked_reason(
                        uow.services,
                        entity_type,
                        entity,
                        transition,
                    ),
                )
                for transition in transitions
            ]

        read_model = AllowedTransitionsReadModel(
            board_id=command.board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            current_status=current_status,
            allowed_transitions=transitions,
        )
        return ListAllowedTransitionsResult(read_model)

    @staticmethod
    def _exception_reason(exc: ValueError) -> str:
        code = getattr(exc, "code", None)
        detail = str(exc)
        return f"{code}: {detail}" if code else detail

    async def _blocked_reason(
        self,
        services: ApplicationServiceCatalog,
        entity_type: str,
        entity: Any,
        transition: AllowedTransition,
    ) -> str | None:
        """Preview state-dependent admission gates without mutating the entity.

        Request-local requirements such as a cancellation reason or execution
        report remain in ``preconditions``; ``blocked_reason`` is reserved for
        facts that are already false for the loaded entity.
        """

        if bool(getattr(entity, "archived", False)):
            return "Entity is archived. Restore it before changing status."

        try:
            if entity_type == "ideation" and transition.to_status == "done":
                await services.ideations._enforce_ambiguity_gate(entity)
                await services.resource_gate.validate_or_raise_entity_completion(
                    entity.board_id,
                    "ideation",
                    entity.id,
                    phase="ideation_done",
                )
            elif entity_type == "refinement":
                if (
                    transition.to_status == "review"
                    and not any(
                        isinstance(item, str) and item.strip()
                        for item in (getattr(entity, "in_scope", None) or [])
                    )
                ):
                    return (
                        "refinement_scope_required: add at least one non-empty "
                        "in_scope item before moving to review."
                    )
                if transition.to_status == "done":
                    await services.refinements._validate_cognitive_done(
                        entity,
                        read_only_preview=True,
                    )
                    await services.resource_gate.validate_or_raise_entity_completion(
                        entity.board_id,
                        "refinement",
                        entity.id,
                        phase="refinement_done",
                    )
            elif entity_type == "spec":
                return await self._spec_blocked_reason(
                    services, entity, transition.to_status
                )
            elif entity_type == "sprint":
                return await self._sprint_blocked_reason(
                    services, entity, transition.to_status
                )
            elif entity_type == "card":
                return await self._card_blocked_reason(
                    services, entity, transition.to_status
                )
        except ValueError as exc:
            return self._exception_reason(exc)
        return None

    async def _spec_blocked_reason(
        self,
        services: ApplicationServiceCatalog,
        spec: Any,
        target_status: str,
    ) -> str | None:
        board = await services.boards.get_board(spec.board_id)
        board_settings = (getattr(board, "settings", None) or {}) if board else {}

        if target_status in {"validated", "in_progress"}:
            card_service = services.cards
            checks = (
                card_service.check_test_coverage,
                card_service.check_rules_coverage,
                card_service.check_trs_coverage,
                card_service.check_contract_coverage,
                card_service.check_ir_coverage,
                card_service.check_or_coverage,
                card_service.check_task_requirement_links_for_spec,
            )
            try:
                for check in checks:
                    await check(spec, board)
                await card_service.check_decision_presence(spec)
                await card_service.check_decisions_coverage(spec, board)
            except ValueError as exc:
                return self._exception_reason(exc)

        if target_status == "validated":
            if (
                spec.status == SpecStatus.APPROVED
                and board_settings.get("require_spec_validation", True)
            ):
                return (
                    "spec_validation_required: submit the Spec Validation Gate; "
                    "direct approved→validated moves are not admitted."
                )
            try:
                await services.resource_gate.validate_or_raise_spec_architecture_validation_resource(
                    spec.board_id,
                    spec.id,
                    board=board,
                    phase="spec_validation",
                )
            except ValueError as exc:
                return self._exception_reason(exc)

        if target_status == "in_progress":
            auto_validate = bool(board_settings.get("auto_validate", False))
            skip_qualitative = bool(
                getattr(spec, "skip_qualitative_validation", False)
            )
            if not auto_validate and not skip_qualitative:
                evaluations = [
                    item
                    for item in (getattr(spec, "evaluations", None) or [])
                    if not item.get("stale")
                ]
                rejections = [
                    item
                    for item in evaluations
                    if item.get("recommendation") == "reject"
                ]
                if rejections:
                    return (
                        "spec_evaluation_rejected: replace the rejecting "
                        "evaluation before starting the spec."
                    )
                approvals = [
                    item
                    for item in evaluations
                    if item.get("recommendation") == "approve"
                ]
                if not approvals:
                    return (
                        "spec_evaluation_required: submit at least one approving "
                        "Spec Evaluation before starting the spec."
                    )
                threshold = (
                    getattr(spec, "validation_threshold", None)
                    or board_settings.get("validation_threshold_global", 70)
                )
                average = sum(
                    item.get("overall_score", 0) for item in approvals
                ) / len(approvals)
                if average < threshold:
                    return (
                        "spec_evaluation_below_threshold: average approval "
                        f"score {average:.0f} is below {threshold}."
                    )

        if target_status == "done":
            skip_global = bool(
                board_settings.get("skip_test_coverage_global", False)
            )
            if (
                not bool(getattr(spec, "skip_test_coverage", False))
                and not skip_global
            ):
                from okto_pulse.core.services.main import (
                    _structured_ref_text,
                    resolve_linked_criteria_to_indices,
                )

                criteria = list(getattr(spec, "acceptance_criteria", None) or [])
                scenarios = list(getattr(spec, "test_scenarios", None) or [])
                covered_indices: set[int] = set()
                for scenario in scenarios:
                    covered_indices |= resolve_linked_criteria_to_indices(
                        scenario.get("linked_criteria"),
                        criteria,
                    )
                uncovered = [
                    f"[{index}] {_structured_ref_text(criterion)[:80]}..."
                    for index, criterion in enumerate(criteria)
                    if index not in covered_indices
                ]
                if uncovered:
                    return (
                        "coverage_incomplete: "
                        f"{len(uncovered)} acceptance criteria lack test scenarios "
                        f"({'; '.join(uncovered[:5])})."
                    )
            sprints = await services.sprints.list_sprints(spec.id)
            pending_sprints = [
                sprint
                for sprint in sprints
                if sprint.status not in (SprintStatus.CLOSED, SprintStatus.CANCELLED)
            ]
            if pending_sprints:
                return (
                    "sprints_incomplete: close or cancel every sprint before "
                    "completing the spec."
                )
            if sprints and not any(
                sprint.status == SprintStatus.CLOSED for sprint in sprints
            ):
                return (
                    "sprints_incomplete: at least one sprint must be closed; "
                    "all current sprints are cancelled."
                )
            pending_cards = [
                card
                for card in (getattr(spec, "cards", None) or [])
                if not bool(getattr(card, "archived", False))
                and getattr(card, "card_type", CardType.NORMAL) == CardType.NORMAL
                and card.status not in (CardStatus.DONE, CardStatus.CANCELLED)
            ]
            if pending_cards:
                return (
                    "cards_incomplete: complete or cancel every normal card "
                    "before completing the spec."
                )
            try:
                await services.specs._validate_cognitive_done(
                    spec,
                    board,
                    read_only_preview=True,
                )
                gate = services.resource_gate
                await gate.validate_or_raise_spec_architecture_validation_resource(
                    spec.board_id,
                    spec.id,
                    board=board,
                    phase="spec_done",
                )
                await gate.validate_or_raise_spec_resource_task_coverage(
                    spec.board_id,
                    spec.id,
                    phase="spec_done",
                    enabled=gate.is_spec_resource_task_coverage_required(board),
                )
                await gate.validate_or_raise_architecture_findings(
                    spec.board_id,
                    "spec",
                    spec.id,
                    phase="spec_done",
                )
            except ValueError as exc:
                return self._exception_reason(exc)
        return None

    async def _sprint_blocked_reason(
        self,
        services: ApplicationServiceCatalog,
        sprint: Any,
        target_status: str,
    ) -> str | None:
        list_assigned_cards = getattr(
            services.sprints, "list_assigned_cards", None
        )
        if callable(list_assigned_cards):
            cards = list(await list_assigned_cards(sprint.id))
        else:
            # Compatibility for persistence-neutral test/service catalogs. Real
            # runtime catalogs expose list_assigned_cards and never trust an ORM
            # relationship that may predate a re-assignment in this transaction.
            cards = [
                card
                for card in (getattr(sprint, "cards", None) or [])
                if not bool(getattr(card, "archived", False))
            ]
        if target_status == "active":
            if not cards:
                return "sprint_empty: assign at least one card before activation."
            if getattr(sprint, "lane_type", None) == SprintLaneType.HOTFIX:
                bug_ids = {
                    card.id for card in cards if card.card_type == CardType.BUG
                }
                test_ids = {
                    card.id for card in cards if card.card_type == CardType.TEST
                }
                origin_bug = next(
                    (
                        card
                        for card in cards
                        if card.id == sprint.origin_bug_id
                        and card.card_type == CardType.BUG
                    ),
                    None,
                )
                linked = set(
                    getattr(origin_bug, "linked_test_task_ids", None) or []
                )
                if (
                    not sprint.origin_bug_id
                    or sprint.origin_bug_id not in bug_ids
                    or not linked.intersection(test_ids)
                ):
                    return (
                        "hotfix_regression_lineage_required: assign the origin "
                        "bug and one explicitly linked regression test card."
                    )

        if target_status == "review":
            spec = await services.specs.get_spec(sprint.spec_id)
            board = await services.boards.get_board(sprint.board_id)
            skip_coverage = bool(
                getattr(sprint, "skip_test_coverage", False)
                or (
                    (getattr(board, "settings", None) or {}).get(
                        "skip_test_coverage_global", False
                    )
                    if board
                    else False
                )
            )
            if spec and not skip_coverage:
                from okto_pulse.core.services.sprint_scope import SprintScopeResolver

                scope = SprintScopeResolver.resolve(
                    sprint=sprint,
                    spec=spec,
                    cards=cards,
                )
                pending = [
                    scenario
                    for scenario in scope.items.get("test_scenarios", ())
                    if scenario.get("status") != "passed"
                ]
                if pending:
                    return (
                        "scoped_tests_incomplete: every scoped test scenario "
                        "must pass before review."
                    )

        if target_status == "closed":
            if any(
                card.status not in (CardStatus.DONE, CardStatus.CANCELLED)
                for card in cards
            ):
                return (
                    "sprint_has_incomplete_cards: complete or cancel every "
                    "assigned card before closing."
                )
            spec = await services.specs.get_spec(sprint.spec_id)
            board = await services.boards.get_board(sprint.board_id)
            if spec is not None:
                from okto_pulse.core.services.main import (
                    scenario_has_authenticated_required_evidence,
                )
                from okto_pulse.core.services.sprint_scope import (
                    SprintScopeResolver,
                    completion_blockers,
                )

                settings = (
                    (getattr(board, "settings", None) or {}) if board else {}
                )
                skip_evidence = bool(
                    settings.get("skip_test_evidence_global", False)
                )
                scope = SprintScopeResolver.resolve(
                    sprint=sprint,
                    spec=spec,
                    cards=cards,
                )
                scope_blockers = completion_blockers(
                    scope,
                    skip_test_coverage=bool(
                        getattr(sprint, "skip_test_coverage", False)
                        or settings.get("skip_test_coverage_global", False)
                    ),
                    skip_test_evidence=skip_evidence,
                    skip_rules_coverage=bool(
                        getattr(sprint, "skip_rules_coverage", False)
                        or settings.get("skip_rules_coverage_global", False)
                    ),
                    evidence_validator=lambda scenario: (
                        scenario_has_authenticated_required_evidence(
                            board_id=sprint.board_id,
                            spec_id=spec.id,
                            scenario=scenario,
                            acceptance_criteria=list(
                                getattr(spec, "acceptance_criteria", None) or []
                            ),
                        )
                    ),
                )
                if scope_blockers:
                    codes = ", ".join(
                        blocker.code for blocker in scope_blockers[:5]
                    )
                    return (
                        "sprint_scope_gate_blocked: resolve scoped coverage, "
                        f"evidence, and rules blockers ({codes})."
                    )
            if not bool(getattr(sprint, "skip_qualitative_validation", False)):
                evaluations = [
                    item
                    for item in (getattr(sprint, "evaluations", None) or [])
                    if not item.get("stale")
                ]
                if any(
                    item.get("recommendation") == "reject"
                    for item in evaluations
                ):
                    return (
                        "sprint_evaluation_rejected: replace the rejecting "
                        "evaluation before closing."
                    )
                approvals = [
                    item
                    for item in evaluations
                    if item.get("recommendation") == "approve"
                ]
                if not approvals:
                    return (
                        "sprint_evaluation_required: submit an approving sprint "
                        "evaluation before closing."
                    )
                threshold = (
                    getattr(sprint, "validation_threshold", None)
                    or (
                        (getattr(board, "settings", None) or {}).get(
                            "validation_threshold_global",
                            70,
                        )
                        if board
                        else 70
                    )
                )
                average = sum(
                    item.get("overall_score", 0) for item in approvals
                ) / len(approvals)
                if average < threshold:
                    return (
                        "sprint_evaluation_below_threshold: average approval "
                        f"score {average:.0f} is below {threshold}."
                    )
        return None

    async def _bug_regression_blocked_reason(
        self,
        services: ApplicationServiceCatalog,
        card: Any,
        target: CardStatus,
        *,
        board_settings: Mapping[str, Any],
        spec: Any | None,
    ) -> tuple[bool, str | None]:
        """Read-only projection of the deep bug-regression mutation gate."""

        raw_type = getattr(card, "card_type", CardType.NORMAL)
        card_type = (
            raw_type
            if isinstance(raw_type, CardType)
            else CardType(getattr(raw_type, "value", raw_type or "normal"))
        )
        severity = str(
            getattr(getattr(card, "severity", None), "value", None) or "minor"
        )
        facts = CardTransitionFacts(
            card_id=card.id,
            old_status=card.status,
            new_status=target,
            card_type=card_type,
            spec_id=getattr(card, "spec_id", None),
            require_test_task_for_bug=bool(
                board_settings.get("require_test_task_for_bug", True)
            ),
            bug_test_gate_min_severity=str(
                board_settings.get("bug_test_gate_min_severity", "minor")
            ),
            severity=severity,
        )
        if not bug_regression_gate_applies(facts):
            return False, None

        from okto_pulse.core.services.amendment_revision import (
            AmendmentRevisionService,
        )
        from okto_pulse.core.services.bug_regression_scenarios import (
            AmendmentLineageFact,
            BugRegressionGateValidator,
            BugRegressionScenarioEligibilityResolver,
        )
        from okto_pulse.core.services.main import (
            _amendment_regression_test_task_ids,
        )

        amendment_rows = (
            await AmendmentRevisionService(services.cards.db).list_for_bug(
                board_id=card.board_id,
                original_spec_id=card.spec_id,
                origin_bug_id=card.id,
            )
            if getattr(card, "spec_id", None)
            else []
        )
        amendment_facts = [
            AmendmentLineageFact.from_row(row) for row in amendment_rows
        ]
        effective_test_ids = list(
            getattr(card, "linked_test_task_ids", None) or []
        ) or _amendment_regression_test_task_ids(amendment_rows)
        if not effective_test_ids:
            origin_task = (
                await services.cards.get_card(card.origin_task_id)
                if getattr(card, "origin_task_id", None)
                else None
            )
            eligibility = (
                BugRegressionScenarioEligibilityResolver().resolve(
                    bug_card=card,
                    spec=spec,
                    origin_task=origin_task,
                    amendment_facts=amendment_facts,
                )
                if spec is not None and origin_task is not None
                else None
            )
            eligible_count = (
                len(eligibility.eligible_scenarios) if eligibility else 0
            )
            if eligible_count == 0:
                return (
                    True,
                    "missing_regression_test_task: zero eligible existing "
                    "scenarios; use Path B to resolve the semantic gap before "
                    "starting this bug.",
                )
            return (
                True,
                "missing_regression_test_task: create and link a fresh "
                "regression test card using one of the "
                f"{eligible_count} eligible existing scenario(s) before "
                "starting this bug.",
            )

        all_scenarios = {
            str(item["id"]): item
            for item in (getattr(spec, "test_scenarios", None) or [])
            if isinstance(item, dict) and item.get("id") is not None
        }
        validated_tests: list[Any] = []
        candidate_scenario_ids: list[str] = []
        bug_created = getattr(card, "created_at", None)
        for test_id in effective_test_ids:
            test_card = await services.cards.get_card(test_id)
            if test_card is None:
                return (
                    True,
                    f"regression_test_not_found: linked test task '{test_id}' "
                    "does not exist.",
                )
            test_type = getattr(test_card, "card_type", CardType.NORMAL)
            test_type = getattr(test_type, "value", test_type)
            if test_type != CardType.TEST.value:
                return (
                    True,
                    f"regression_test_type_invalid: linked card '{test_id}' "
                    "is not a test card.",
                )
            scenario_ids = list(
                getattr(test_card, "test_scenario_ids", None) or []
            )
            if not scenario_ids:
                return (
                    True,
                    f"regression_test_scenarios_missing: linked test '{test_id}' "
                    "has no test scenarios.",
                )
            if (
                getattr(test_card, "spec_id", None)
                != getattr(card, "spec_id", None)
                and not amendment_facts
            ):
                return (
                    True,
                    f"regression_test_cross_spec: linked test '{test_id}' "
                    "belongs to another spec without amendment lineage.",
                )
            test_created = getattr(test_card, "created_at", None)
            if (
                bug_created is not None
                and test_created is not None
                and test_created.isoformat() < bug_created.isoformat()
            ):
                return (
                    True,
                    f"regression_test_stale: linked test '{test_id}' predates "
                    "the bug.",
                )
            validated_tests.append(test_card)
            candidate_scenario_ids.extend(str(item) for item in scenario_ids)

        missing_ids = {
            scenario_id
            for scenario_id in candidate_scenario_ids
            if scenario_id not in all_scenarios
        }
        candidate_spec_ids: dict[str, str] = {}
        if missing_ids:
            for other_spec in await services.specs.list_specs(
                card.board_id,
                include_archived=True,
            ):
                if other_spec.id == getattr(card, "spec_id", None):
                    continue
                for scenario in getattr(other_spec, "test_scenarios", None) or []:
                    if not isinstance(scenario, dict) or scenario.get("id") is None:
                        continue
                    scenario_id = str(scenario["id"])
                    if scenario_id in missing_ids:
                        candidate_spec_ids.setdefault(scenario_id, other_spec.id)
            absent = sorted(missing_ids - set(candidate_spec_ids))
            if absent:
                return (
                    True,
                    "scenario_not_found: linked regression scenario(s) do not "
                    f"exist ({', '.join(absent[:5])}).",
                )

        origin_task = (
            await services.cards.get_card(card.origin_task_id)
            if getattr(card, "origin_task_id", None)
            else None
        )
        if origin_task is None:
            return (
                True,
                "origin_task_missing: set a valid origin_task_id before "
                "starting this bug.",
            )

        result = BugRegressionGateValidator().validate_linked_test_tasks(
            bug_card=card,
            linked_test_tasks=validated_tests,
            spec=spec,
            origin_task=origin_task,
            candidate_spec_ids_by_scenario_id=candidate_spec_ids,
            amendment_facts=amendment_facts,
        )
        if not result.allowed:
            decision = getattr(result.decision, "value", str(result.decision))
            rejected = ", ".join(
                f"{item.scenario_id}:{item.reason.value}"
                for item in result.eligibility.rejected_scenarios[:5]
            )
            return (
                True,
                f"{decision}: bug regression eligibility blocked"
                + (f" ({rejected})" if rejected else "."),
            )
        return True, None

    async def _card_blocked_reason(
        self,
        services: ApplicationServiceCatalog,
        card: Any,
        target_status: str,
    ) -> str | None:
        target = CardStatus(target_status)
        old_status = card.status
        raw_type = getattr(card, "card_type", CardType.NORMAL)
        card_type = (
            raw_type
            if isinstance(raw_type, CardType)
            else CardType(getattr(raw_type, "value", raw_type or "normal"))
        )
        board = await services.boards.get_board(card.board_id)
        board_settings = (getattr(board, "settings", None) or {}) if board else {}
        spec = (
            await services.specs.get_spec(card.spec_id)
            if getattr(card, "spec_id", None)
            else None
        )
        sprints = (
            await services.sprints.list_sprints(card.spec_id)
            if getattr(card, "spec_id", None)
            else []
        )
        sprint = (
            await services.sprints.get_sprint(card.sprint_id)
            if getattr(card, "sprint_id", None)
            else None
        )
        pending_scenarios: list[PendingScenario] = []
        if (
            target == CardStatus.DONE
            and card_type == CardType.TEST
            and not bool(board_settings.get("skip_test_coverage_global", False))
        ):
            from okto_pulse.core.services.main import (
                scenario_has_authenticated_required_evidence,
            )

            scenario_ids = [
                str(value)
                for value in (getattr(card, "test_scenario_ids", None) or [])
            ]
            if not scenario_ids:
                pending_scenarios.append(
                    PendingScenario(
                        scenario_id="__unlinked__",
                        title="No test scenario is linked to this test card",
                        status="missing",
                    )
                )
            elif spec is None:
                pending_scenarios.extend(
                    PendingScenario(
                        scenario_id=scenario_id,
                        title=f"Unresolved test scenario {scenario_id}",
                        status="missing",
                    )
                    for scenario_id in scenario_ids
                )
            else:
                by_id = {
                    str(item.get("id")): item
                    for item in (getattr(spec, "test_scenarios", None) or [])
                    if isinstance(item, dict) and item.get("id") is not None
                }
                for scenario_id in scenario_ids:
                    scenario = by_id.get(scenario_id)
                    if scenario is None:
                        pending_scenarios.append(
                            PendingScenario(
                                scenario_id=scenario_id,
                                title=f"Unresolved test scenario {scenario_id}",
                                status="missing",
                            )
                        )
                        continue
                    if not (
                        scenario.get("status") in {"draft", "ready"}
                        or not scenario_has_authenticated_required_evidence(
                            board_id=card.board_id,
                            spec_id=spec.id,
                            scenario=scenario,
                            acceptance_criteria=list(
                                getattr(spec, "acceptance_criteria", None) or []
                            ),
                        )
                    ):
                        continue
                    pending_scenarios.append(
                        PendingScenario(
                            scenario_id=scenario_id,
                            title=scenario.get("title") or scenario_id,
                            status=scenario.get("status") or "draft",
                        )
                    )
        validation_required = False
        if target == CardStatus.DONE:
            config = services.cards._resolve_validation_config(
                card,
                spec,
                sprint,
                board_settings,
            )
            validation_required = bool(config["required"])
        bug_gate_applies_now, bug_blocked_reason = (
            await self._bug_regression_blocked_reason(
                services,
                card,
                target,
                board_settings=board_settings,
                spec=spec,
            )
        )
        if bug_blocked_reason is not None:
            return bug_blocked_reason
        facts = CardTransitionFacts(
            card_id=card.id,
            old_status=old_status,
            new_status=target,
            card_type=card_type,
            archived=bool(getattr(card, "archived", False)),
            spec_id=getattr(card, "spec_id", None),
            spec_title=getattr(spec, "title", None),
            spec_status=getattr(spec, "status", None),
            sprint_count=len(sprints),
            sprint_id=getattr(card, "sprint_id", None),
            sprint_exists=sprint is not None if card.sprint_id else True,
            sprint_status=getattr(sprint, "status", None),
            sprint_title=getattr(sprint, "title", None),
            sprint_is_hotfix=bool(
                sprint and sprint.lane_type == SprintLaneType.HOTFIX
            ),
            hotfix_count=sum(
                1 for item in sprints if item.lane_type == SprintLaneType.HOTFIX
            ),
            validation_required=validation_required,
            pending_scenarios=tuple(pending_scenarios),
            require_test_task_for_bug=bool(
                board_settings.get("require_test_task_for_bug", True)
            ),
            bug_test_gate_min_severity=str(
                board_settings.get("bug_test_gate_min_severity", "minor")
            ),
            severity=str(
                getattr(getattr(card, "severity", None), "value", None) or "minor"
            ),
            has_regression_test_evidence=bool(
                getattr(card, "linked_test_task_ids", None)
            )
            or bug_gate_applies_now,
        )
        decision = evaluate_card_transition(facts)
        if not decision.allowed and decision.block:
            if (
                decision.block.code == "test_scenarios_pending"
                and any(
                    scenario.status == "missing"
                    for scenario in pending_scenarios
                )
            ):
                missing_ids = [
                    scenario.scenario_id
                    for scenario in pending_scenarios
                    if scenario.status == "missing"
                    and scenario.scenario_id != "__unlinked__"
                ]
                detail = (
                    "Test-card scenario linkage is empty or contains missing/"
                    "unresolved scenario ids"
                )
                if missing_ids:
                    detail += f" ({', '.join(missing_ids[:5])})"
                return f"{decision.block.code}: {detail}."
            return f"{decision.block.code}: {decision.block.detail}"

        old_level = services.cards._STATUS_ORDER.get(old_status, 0)
        new_level = services.cards._STATUS_ORDER.get(target, 0)
        if new_level > old_level and target != CardStatus.CANCELLED:
            if card_type == CardType.NORMAL and old_level < services.cards._STATUS_ORDER[
                CardStatus.IN_PROGRESS
            ]:
                try:
                    await services.cards.check_card_requirement_link_gate(
                        card, spec, board
                    )
                except ValueError as exc:
                    return self._exception_reason(exc)
            dependencies_met, blocking = await services.cards.check_dependencies_met(
                card.id
            )
            if not dependencies_met:
                return (
                    "dependencies_incomplete: complete blocking dependencies "
                    f"first ({', '.join(blocking)})."
                )
        if target == CardStatus.DONE:
            try:
                await services.cards._validate_cognitive_done(
                    card,
                    board,
                    read_only_preview=True,
                )
                await services.resource_gate.validate_or_raise_entity_completion(
                    card.board_id,
                    "card",
                    card.id,
                    phase="card_done",
                )
            except ValueError as exc:
                return self._exception_reason(exc)
        return None

    async def _load_entity(
        self,
        services: ApplicationServiceCatalog,
        entity_type: str,
        entity_id: str,
    ) -> Any:
        if entity_type == "ideation":
            return await services.ideations.get_ideation(entity_id)
        if entity_type == "refinement":
            return await services.refinements.get_refinement(entity_id)
        if entity_type == "spec":
            return await services.specs.get_spec(entity_id)
        if entity_type == "story":
            return await services.stories.get_story(entity_id)
        if entity_type == "card":
            return await services.cards.get_card(entity_id)
        if entity_type == "sprint":
            return await services.sprints.get_sprint(entity_id)
        raise CommandValidationError(f"Invalid entity_type: {entity_type}")
