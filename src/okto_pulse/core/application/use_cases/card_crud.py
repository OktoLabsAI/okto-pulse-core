"""Card CRUD use cases (SaaS Refactor spec R01A REST-FU4-S1).

Transport-free reimplementations of the three thin ``api/cards.py`` CRUD
endpoints that wrap ``CardService`` — get / update / delete. Each delegates to
the existing service so payload, ownership/404 and the read-only/operation gate
errors (``CardOperationError``/``CardResourceReadOnlyError`` propagate for the
adapter to map to 409) are unchanged. Reads: no commit; writes: ``commit(uow)``
after the service mutation, then a re-fetch so the adapter returns the updated
card with its relationships loaded — exactly as the legacy endpoint did.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    ConflictError,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import CardService


# --- get --------------------------------------------------------------------


class GetCardCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class GetCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class GetCardUseCase:
    """Fetch a card with all its relationships (read). 404 when missing."""

    async def execute(
        self, command: GetCardCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardResult:
        card = await CardService(session_of(uow)).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        return GetCardResult(card)


# --- update -----------------------------------------------------------------


class UpdateCardCommand:
    __slots__ = ("card_id", "data")

    def __init__(self, card_id: str, data: Any) -> None:
        self.card_id = card_id
        self.data = data


class UpdateCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class UpdateCardUseCase:
    """Update a card (write). The gate errors raised by ``update_card``
    (``CardOperationError``/``CardResourceReadOnlyError``) propagate for the
    adapter to map (both 409); a missing card is ``EntityNotFoundError`` (404).
    Commits, then re-fetches via ``get_card`` so the result carries the updated
    card with its relationships loaded."""

    async def execute(
        self, command: UpdateCardCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateCardResult:
        service = CardService(session_of(uow))
        card = await service.update_card(command.card_id, actor.actor_id, command.data)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        await commit(uow)
        return UpdateCardResult(await service.get_card(command.card_id))


# --- delete -----------------------------------------------------------------


class DeleteCardCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class DeleteCardResult:
    __slots__ = ()


class DeleteCardUseCase:
    """Delete a card, cascade-cleaning orphan references (write). 404 when
    missing."""

    async def execute(
        self, command: DeleteCardCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteCardResult:
        service = CardService(session_of(uow))
        deleted = await service.delete_card(command.card_id, actor.actor_id)
        if not deleted:
            raise EntityNotFoundError("card", command.card_id)
        await commit(uow)
        return DeleteCardResult()


# ===========================================================================
# R01A REST-FU4-S2: card lifecycle / move / dependencies / task-validation.
# Each use case wraps the EXISTING ``CardService`` method — the move state
# machine, the dependency-cycle guard, the SQL inline and the validation
# routing all stay in the service; only the transport envelope (lookup →
# not-found → mutate → commit / re-fetch) moves here. Service gate errors
# (``CardOperationError``/``GateContractError``/``ResourceGateError``/
# ``ValueError``) propagate uncaught so the adapter maps them with the legacy
# status + detail, preserving the exact except-order (subclasses first).
# ===========================================================================


# --- move (state machine) ---------------------------------------------------


class MoveCardCommand:
    __slots__ = ("card_id", "data")

    def __init__(self, card_id: str, data: Any) -> None:
        self.card_id = card_id
        self.data = data


class MoveCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class MoveCardUseCase:
    """Move a card to a different column/position (write). Delegates to
    ``CardService.move_card`` (the 3600-LOC state machine stays untouched). The
    gate errors it raises (``CardOperationError``/``GateContractError``/
    ``ResourceGateError``/``ValueError`` — all 409) propagate for the adapter to
    map in that exact order; a missing card is ``EntityNotFoundError`` (404).
    Commits, then re-fetches via ``get_card`` so the result carries the moved
    card with its relationships loaded — exactly as the legacy endpoint did."""

    async def execute(
        self, command: MoveCardCommand, *, actor: ActorContext, uow: Any
    ) -> MoveCardResult:
        service = CardService(session_of(uow))
        card = await service.move_card(command.card_id, actor.actor_id, command.data)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        await commit(uow)
        return MoveCardResult(await service.get_card(command.card_id))


# --- dependencies (read) ----------------------------------------------------


class GetCardDependenciesCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class GetCardDependenciesResult:
    __slots__ = ("dependencies",)

    def __init__(self, dependencies: list[Any]) -> None:
        self.dependencies = dependencies


class GetCardDependenciesUseCase:
    """List the cards this card depends on (read, no commit). The adapter shapes
    each ``Card`` into the legacy ``{id, title, status}`` projection."""

    async def execute(
        self, command: GetCardDependenciesCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardDependenciesResult:
        deps = await CardService(session_of(uow)).get_dependencies(command.card_id)
        return GetCardDependenciesResult(deps)


class GetCardDependentsCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class GetCardDependentsResult:
    __slots__ = ("dependents",)

    def __init__(self, dependents: list[Any]) -> None:
        self.dependents = dependents


class GetCardDependentsUseCase:
    """List the cards that depend on this card (read, no commit). The adapter
    shapes each ``Card`` into the legacy ``{id, title, status}`` projection."""

    async def execute(
        self, command: GetCardDependentsCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardDependentsResult:
        deps = await CardService(session_of(uow)).get_dependents(command.card_id)
        return GetCardDependentsResult(deps)


# --- dependencies (write) ---------------------------------------------------


class AddCardDependencyCommand:
    __slots__ = ("card_id", "depends_on_id")

    def __init__(self, card_id: str, depends_on_id: str) -> None:
        self.card_id = card_id
        self.depends_on_id = depends_on_id


class AddCardDependencyResult:
    __slots__ = ("dependency_id",)

    def __init__(self, dependency_id: str) -> None:
        self.dependency_id = dependency_id


class AddCardDependencyUseCase:
    """Add ``card_id`` depends-on ``depends_on_id`` (write). ``CardService``
    returns ``None`` on a self-reference or a cycle — that becomes
    ``ConflictError`` the adapter maps to the legacy 409 detail. Captures the
    generated id before the commit (the Python-side default is set at
    construction) and returns it for the adapter envelope."""

    async def execute(
        self, command: AddCardDependencyCommand, *, actor: ActorContext, uow: Any
    ) -> AddCardDependencyResult:
        service = CardService(session_of(uow))
        dep = await service.add_dependency(command.card_id, command.depends_on_id)
        if not dep:
            raise ConflictError("card_dependency", command.card_id)
        dependency_id = dep.id
        await commit(uow)
        return AddCardDependencyResult(dependency_id)


class RemoveCardDependencyCommand:
    __slots__ = ("card_id", "depends_on_id")

    def __init__(self, card_id: str, depends_on_id: str) -> None:
        self.card_id = card_id
        self.depends_on_id = depends_on_id


class RemoveCardDependencyResult:
    __slots__ = ()


class RemoveCardDependencyUseCase:
    """Remove the ``card_id`` → ``depends_on_id`` dependency (write). When no row
    matched, ``CardService.remove_dependency`` returns ``False`` — that becomes
    ``EntityNotFoundError`` the adapter maps to the legacy 404 ("Dependency not
    found")."""

    async def execute(
        self, command: RemoveCardDependencyCommand, *, actor: ActorContext, uow: Any
    ) -> RemoveCardDependencyResult:
        service = CardService(session_of(uow))
        removed = await service.remove_dependency(command.card_id, command.depends_on_id)
        if not removed:
            raise EntityNotFoundError("dependency", command.card_id)
        await commit(uow)
        return RemoveCardDependencyResult()


# --- task validations -------------------------------------------------------


class SubmitTaskValidationCommand:
    __slots__ = ("card_id", "data")

    def __init__(self, card_id: str, data: Any) -> None:
        self.card_id = card_id
        self.data = data


class SubmitTaskValidationResult:
    __slots__ = ("validation",)

    def __init__(self, validation: Any) -> None:
        self.validation = validation


class SubmitTaskValidationUseCase:
    """Submit a task validation for a card in 'validation' status (write).

    Validates the required fields and the recommendation enum upfront, raising
    ``CommandValidationError`` (adapter → 400) with the exact legacy messages.
    Resolves the reviewer display name via ``resolve_actor_name`` with the legacy
    fallback to the actor id on any error, then delegates to
    ``CardService.submit_task_validation`` (threshold check + persistence +
    card routing stay in the service). ``GateContractError`` (→ 409),
    ``ResourceGateError`` (→ 409) and ``ValueError`` (→ 422) propagate for the
    adapter to map; a missing card is ``EntityNotFoundError`` (→ 404). Commits
    after the service mutation, exactly as the legacy endpoint did."""

    _REQUIRED = (
        "confidence",
        "confidence_justification",
        "estimated_completeness",
        "completeness_justification",
        "estimated_drift",
        "drift_justification",
        "general_justification",
        "recommendation",
    )

    async def execute(
        self, command: SubmitTaskValidationCommand, *, actor: ActorContext, uow: Any
    ) -> SubmitTaskValidationResult:
        from okto_pulse.core.services.main import resolve_actor_name

        data = command.data
        missing = [f for f in self._REQUIRED if f not in data or data[f] is None]
        if missing:
            raise CommandValidationError(
                f"Missing required fields: {', '.join(missing)}"
            )
        if data.get("recommendation") not in ("approve", "reject"):
            raise CommandValidationError(
                "recommendation must be 'approve' or 'reject'"
            )

        session = session_of(uow)
        service = CardService(session)
        card = await service.get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        if actor.actor_name:
            reviewer_name = actor.actor_name
        else:
            try:
                reviewer_name = await resolve_actor_name(session, actor.actor_id, card.board_id)
            except Exception:
                reviewer_name = actor.actor_id

        result = await service.submit_task_validation(
            card_id=command.card_id,
            reviewer_id=actor.actor_id,
            reviewer_name=reviewer_name,
            data=data,
        )
        await commit(uow)
        return SubmitTaskValidationResult(result)


class ListTaskValidationsCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class ListTaskValidationsResult:
    __slots__ = ("validations",)

    def __init__(self, validations: list[Any]) -> None:
        self.validations = validations


class ListTaskValidationsUseCase:
    """List a card's validations, reverse-chronological (read, no commit).
    ``CardService.list_task_validations`` raises ``ValueError`` for a missing card
    — the adapter maps it to the same 404 as before. The adapter wraps the list in
    the legacy ``{card_id, total, validations}`` envelope."""

    async def execute(
        self, command: ListTaskValidationsCommand, *, actor: ActorContext, uow: Any
    ) -> ListTaskValidationsResult:
        validations = await CardService(session_of(uow)).list_task_validations(
            command.card_id
        )
        return ListTaskValidationsResult(validations)


class GetTaskValidationCommand:
    __slots__ = ("card_id", "validation_id")

    def __init__(self, card_id: str, validation_id: str) -> None:
        self.card_id = card_id
        self.validation_id = validation_id


class GetTaskValidationResult:
    __slots__ = ("validation",)

    def __init__(self, validation: Any) -> None:
        self.validation = validation


class GetTaskValidationUseCase:
    """Fetch a single validation by id (read, no commit).
    ``CardService.get_task_validation`` raises ``ValueError`` for a missing card
    (adapter → 404 with its message); an unknown validation id returns ``None``
    → ``EntityNotFoundError`` the adapter maps to the legacy 404 ("Validation not
    found")."""

    async def execute(
        self, command: GetTaskValidationCommand, *, actor: ActorContext, uow: Any
    ) -> GetTaskValidationResult:
        validation = await CardService(session_of(uow)).get_task_validation(
            command.card_id, command.validation_id
        )
        if not validation:
            raise EntityNotFoundError("task_validation", command.validation_id)
        return GetTaskValidationResult(validation)


class DeleteTaskValidationCommand:
    __slots__ = ("card_id", "validation_id")

    def __init__(self, card_id: str, validation_id: str) -> None:
        self.card_id = card_id
        self.validation_id = validation_id


class DeleteTaskValidationResult:
    __slots__ = ()


class DeleteTaskValidationUseCase:
    """Delete a validation entry (write). ``CardService.delete_task_validation``
    raises ``ValueError`` for a missing card (adapter → 404 with its message) and
    returns ``False`` for an unknown validation id → ``EntityNotFoundError`` the
    adapter maps to the legacy 404 ("Validation not found"). The service mutates
    in place without committing, so the use case commits — exactly as the legacy
    endpoint did."""

    async def execute(
        self, command: DeleteTaskValidationCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteTaskValidationResult:
        service = CardService(session_of(uow))
        deleted = await service.delete_task_validation(
            command.card_id, command.validation_id, actor.actor_id
        )
        if not deleted:
            raise EntityNotFoundError("task_validation", command.validation_id)
        await commit(uow)
        return DeleteTaskValidationResult()


# ===========================================================================
# R01A REST-FU4-S3: bug card / test-task linking.
# Three endpoints that the legacy ``api/cards.py`` drove directly off the
# request session: the regression-scenario preview (read), and the
# link/unlink of test-task ids onto a bug card's ``linked_test_task_ids``
# JSONB (writes). The preview delegates to the existing
# ``BugRegressionScenarioPreviewService``; the link/unlink keep the EXACT
# legacy validation gates + status codes + ``flag_modified`` + conditional
# commit, only the persistence handle moves from ``db`` to ``session_of(uow)``.
# ===========================================================================


# --- regression scenario candidates (read) ----------------------------------


class GetBugRegressionScenarioCandidatesCommand:
    __slots__ = ("card_id", "board_id", "affected_task_ids", "candidate_scenario_ids")

    def __init__(
        self,
        card_id: str,
        board_id: str,
        affected_task_ids: list[str] | None = None,
        candidate_scenario_ids: list[str] | None = None,
    ) -> None:
        self.card_id = card_id
        self.board_id = board_id
        self.affected_task_ids = affected_task_ids
        self.candidate_scenario_ids = candidate_scenario_ids


class GetBugRegressionScenarioCandidatesResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class GetBugRegressionScenarioCandidatesUseCase:
    """Preview reusable regression scenarios for a bug without mutating spec/card
    state (read, no commit). Delegates to ``BugRegressionScenarioPreviewService``
    with ``surface="rest"`` — the eligibility resolution, cross-spec
    classification and observability all stay in the service. Its typed
    ``BugRegressionScenarioPreviewError`` propagates uncaught so the adapter maps
    it with the carried ``status_code`` + ``to_dict()`` payload, exactly as the
    legacy endpoint did."""

    async def execute(
        self,
        command: GetBugRegressionScenarioCandidatesCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> GetBugRegressionScenarioCandidatesResult:
        from okto_pulse.core.services.bug_regression_preview import (
            BugRegressionScenarioPreviewService,
        )

        payload = await BugRegressionScenarioPreviewService(session_of(uow)).resolve(
            board_id=command.board_id,
            bug_id=command.card_id,
            affected_task_ids=command.affected_task_ids,
            candidate_scenario_ids=command.candidate_scenario_ids,
            surface="rest",
        )
        return GetBugRegressionScenarioCandidatesResult(payload)


# --- link test task to bug (write) ------------------------------------------


class LinkTestTaskToBugCommand:
    __slots__ = ("card_id", "test_task_id")

    def __init__(self, card_id: str, test_task_id: Any) -> None:
        self.card_id = card_id
        self.test_task_id = test_task_id


class LinkTestTaskToBugResult:
    __slots__ = ("is_unblocked",)

    def __init__(self, is_unblocked: bool) -> None:
        self.is_unblocked = is_unblocked


class LinkTestTaskToBugUseCase:
    """Link a test task onto a bug card's ``linked_test_task_ids`` (write).

    Replicates the legacy endpoint's validation gates and their exact status
    mapping, raising transport-neutral errors the adapter maps 1:1:

    * missing ``test_task_id`` → ``CommandValidationError`` (→ 400,
      "test_task_id is required")
    * bug card not found → ``EntityNotFoundError("card", …)`` (→ 404,
      "Card not found")
    * card is not a bug → ``CommandValidationError`` (→ 400,
      "Card is not a bug card")
    * test task not found → ``EntityNotFoundError("test_task", …)`` (→ 404,
      "Test task not found")
    * spec mismatch / created-before-bug / scenario-not-on-spec → ``ValueError``
      (→ 422) with the exact legacy detail strings.

    The two ``get_card`` lookups stay on ``CardService``; the spec re-fetch uses
    ``session_of(uow)`` (the legacy ``db.get(Spec, …)`` inline). The JSONB
    mutation uses ``flag_modified`` and commits ONLY when the id was actually
    appended — exactly as the legacy endpoint did. ``is_unblocked`` mirrors the
    legacy ``len(linked) >= 1``."""

    async def execute(
        self, command: LinkTestTaskToBugCommand, *, actor: ActorContext, uow: Any
    ) -> LinkTestTaskToBugResult:
        from okto_pulse.core.services import SpecService
        from okto_pulse.core.services.persistence_mutation import (
            mark_mutable_field_modified,
        )

        if not command.test_task_id:
            raise CommandValidationError("test_task_id is required")

        session = session_of(uow)
        service = CardService(session)
        bug_card = await service.get_card(command.card_id)
        if not bug_card:
            raise EntityNotFoundError("card", command.card_id)

        if getattr(bug_card, "card_type", "normal") != "bug":
            raise CommandValidationError("Card is not a bug card")

        test_task = await service.get_card(command.test_task_id)
        if not test_task:
            raise EntityNotFoundError("test_task", command.test_task_id)

        # Validate same spec.
        if test_task.spec_id != bug_card.spec_id:
            raise ValueError("Test task does not belong to the same spec as the bug")

        # Validate the regression test task was created after the bug. The task
        # may reference an existing scenario on a validated/locked spec.
        if bug_card.created_at and test_task.created_at:
            if test_task.created_at.isoformat() < bug_card.created_at.isoformat():
                raise ValueError(
                    "Test task was created before the bug card — link a regression "
                    "test task created after the bug"
                )

        # Validate test task references scenarios that still exist on the spec.
        if bug_card.spec_id and test_task.test_scenario_ids:
            spec = await SpecService(session).get_spec(bug_card.spec_id)
            if spec:
                all_scenarios = {s["id"]: s for s in (spec.test_scenarios or [])}
                for sid in test_task.test_scenario_ids:
                    if sid not in all_scenarios:
                        raise ValueError(
                            f"Test task references scenario '{sid}' that does not "
                            "exist on the bug spec"
                        )

        # Add test task to linked_test_task_ids.
        linked = list(bug_card.linked_test_task_ids or [])
        if command.test_task_id not in linked:
            linked.append(command.test_task_id)
            bug_card.linked_test_task_ids = linked
            mark_mutable_field_modified(bug_card, "linked_test_task_ids")
            await commit(uow)

        return LinkTestTaskToBugResult(is_unblocked=len(linked) >= 1)


# --- unlink test task from bug (write) --------------------------------------


class UnlinkTestTaskFromBugCommand:
    __slots__ = ("card_id", "test_task_id")

    def __init__(self, card_id: str, test_task_id: str) -> None:
        self.card_id = card_id
        self.test_task_id = test_task_id


class UnlinkTestTaskFromBugResult:
    __slots__ = ()


class UnlinkTestTaskFromBugUseCase:
    """Unlink a test task from a bug card's ``linked_test_task_ids`` (write). A
    missing bug card is ``EntityNotFoundError("card", …)`` (adapter → 404,
    "Card not found"). The JSONB mutation uses ``flag_modified`` and commits ONLY
    when the id was actually present and removed — exactly as the legacy endpoint
    did (an absent id is a no-op 204)."""

    async def execute(
        self, command: UnlinkTestTaskFromBugCommand, *, actor: ActorContext, uow: Any
    ) -> UnlinkTestTaskFromBugResult:
        from okto_pulse.core.services.persistence_mutation import (
            mark_mutable_field_modified,
        )

        service = CardService(session_of(uow))
        bug_card = await service.get_card(command.card_id)
        if not bug_card:
            raise EntityNotFoundError("card", command.card_id)

        linked = list(bug_card.linked_test_task_ids or [])
        if command.test_task_id in linked:
            linked.remove(command.test_task_id)
            bug_card.linked_test_task_ids = linked
            mark_mutable_field_modified(bug_card, "linked_test_task_ids")
            await commit(uow)
        return UnlinkTestTaskFromBugResult()


# ===========================================================================
# R01A REST-FU4-S4: card activity / seen-status / knowledge (read-only).
# Closes ``api/cards.py`` — after this card no endpoint there binds ``get_db``.
# The activity + seen SQL the legacy endpoints ran inline on the request session
# moves to the transport-free readers ``compute_card_activity`` /
# ``compute_card_seen_status`` in ``services/main.py`` (the ``compute_*`` pattern);
# the use cases only CALL the reader so this layer never touches ``select``/ORM
# (the relational ratchet gate). The knowledge reads delegate to
# ``CardService.get_card`` and read the card's ``knowledge_bases`` JSONB snapshot.
# All read-only — no commit. The create/update/delete knowledge endpoints stay
# blocked 409 (read-only governed snapshots) and carry NO use case — the adapter
# raises directly; only their ``db -> uow`` signature changes to zero out cards.py.
# ===========================================================================


# --- activity (read) --------------------------------------------------------


class GetCardActivityCommand:
    __slots__ = ("card_id", "limit")

    def __init__(self, card_id: str, *, limit: int = 50) -> None:
        self.card_id = card_id
        self.limit = limit


class GetCardActivityResult:
    __slots__ = ("activity",)

    def __init__(self, activity: list[Any]) -> None:
        self.activity = activity


class GetCardActivityUseCase:
    """Activity log for a card, newest first (read, no commit). Delegates to the
    transport-free ``compute_card_activity`` reader — the ``ActivityLog`` query and
    the ``activity_log_*`` projection stay in the service layer. An unknown card id
    yields an empty list, exactly as the legacy endpoint did (no 404)."""

    async def execute(
        self, command: GetCardActivityCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardActivityResult:
        from okto_pulse.core.services.main import compute_card_activity

        activity = await compute_card_activity(
            session_of(uow), command.card_id, limit=command.limit
        )
        return GetCardActivityResult(activity)


# --- seen status (read) -----------------------------------------------------


class GetCardSeenStatusCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class GetCardSeenStatusResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class GetCardSeenStatusUseCase:
    """Per-item seen status (comments + QA) for a card (read, no commit). Delegates
    to the transport-free ``compute_card_seen_status`` reader — the three inline
    queries (comment/QA ids + the ``AgentSeenItem``→agent-name join) and the grouping
    stay in the service layer. Returns ``{"items": {...}}`` (``{}`` when the card has
    no comment/QA items), exactly as the legacy endpoint did (no 404)."""

    async def execute(
        self, command: GetCardSeenStatusCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardSeenStatusResult:
        from okto_pulse.core.services.main import compute_card_seen_status

        data = await compute_card_seen_status(session_of(uow), command.card_id)
        return GetCardSeenStatusResult(data)


# --- card knowledge (read) --------------------------------------------------


class ListCardKnowledgeCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class ListCardKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: list[Any]) -> None:
        self.knowledge = knowledge


class ListCardKnowledgeUseCase:
    """List the KE snapshots attached to a card (read, no commit). Delegates to
    ``CardService.get_card`` and reads the card's ``knowledge_bases`` JSONB; a
    missing card is ``EntityNotFoundError("card", …)`` (adapter → 404 "Card not
    found"). The adapter wraps the list in the legacy ``{card_id, knowledge}``
    envelope."""

    async def execute(
        self, command: ListCardKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> ListCardKnowledgeResult:
        card = await CardService(session_of(uow)).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        return ListCardKnowledgeResult(list(card.knowledge_bases or []))


class GetCardKnowledgeCommand:
    __slots__ = ("card_id", "kb_id")

    def __init__(self, card_id: str, kb_id: str) -> None:
        self.card_id = card_id
        self.kb_id = kb_id


class GetCardKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: dict[str, Any]) -> None:
        self.knowledge = knowledge


class GetCardKnowledgeUseCase:
    """Fetch a single KE snapshot from a card's ``knowledge_bases`` by id (read, no
    commit). Drives BOTH the JSON ``get`` endpoint and the markdown ``download`` (the
    adapter builds the file Response from the returned entry). A missing card is
    ``EntityNotFoundError("card", …)`` (→ 404 "Card not found"); an unknown kb id is
    ``EntityNotFoundError("card_knowledge", …)`` (→ 404 "Knowledge entry not found").
    The ``entity_type`` drives the adapter's two distinct 404 details — exactly as
    the legacy endpoint did."""

    async def execute(
        self, command: GetCardKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> GetCardKnowledgeResult:
        card = await CardService(session_of(uow)).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)
        for kb in card.knowledge_bases or []:
            if kb.get("id") == command.kb_id:
                return GetCardKnowledgeResult(kb)
        raise EntityNotFoundError("card_knowledge", command.kb_id)
