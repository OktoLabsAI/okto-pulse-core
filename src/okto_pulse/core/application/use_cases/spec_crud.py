"""Spec CRUD + history use cases (SaaS Refactor spec R01A REST-FU3a).

Transport-free reimplementations of the six clean ``api/specs.py`` CRUD endpoints
that wrap ``SpecService``/``BoardService`` — create / list / get / move / delete /
history. Each delegates to the existing service so payload, ownership/404, the
status-transition gate errors (GateContractError/ResourceGateError/ValueError
propagate for the adapter to map) and the commit are unchanged. ``update_spec``
(permission + deprecation) is handled separately. Reads: no commit; writes:
commit(uow) after the service mutation.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
    session_of,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.services.application_schemas import SpecUpdate
from okto_pulse.core.services import BoardService, SpecService


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


# --- create -----------------------------------------------------------------


class CreateSpecCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class CreateSpecUseCase:
    """Create a spec (write). ``EntityNotFoundError("board")`` when the board is
    missing/not owned — the adapter maps to the legacy 404."""

    async def execute(
        self, command: CreateSpecCommand, *, actor: ActorContext, uow: Any
    ) -> CreateSpecResult:
        service = SpecService(session_of(uow))
        spec = await service.create_spec(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not spec:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return CreateSpecResult(await service.get_spec(spec.id))


# --- list -------------------------------------------------------------------


class ListSpecsCommand:
    __slots__ = ("board_id", "status_filter", "include_archived")

    def __init__(
        self, board_id: str, *, status_filter: str | None = None, include_archived: bool = False
    ) -> None:
        self.board_id = board_id
        self.status_filter = status_filter
        self.include_archived = include_archived


class ListSpecsResult:
    __slots__ = ("specs",)

    def __init__(self, specs: list[Any]) -> None:
        self.specs = specs


class ListSpecsUseCase:
    """List a board's specs (read). 404 when the board is missing/not owned."""

    async def execute(
        self, command: ListSpecsCommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecsResult:
        session = session_of(uow)
        board = await BoardService(session).get_board(
            command.board_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
        )
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        specs = await SpecService(session).list_specs(
            command.board_id, command.status_filter, include_archived=command.include_archived
        )
        return ListSpecsResult(specs)


# --- get --------------------------------------------------------------------


class GetSpecCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class GetSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class GetSpecUseCase:
    """Fetch a spec with its derived cards (read). 404 when missing."""

    async def execute(
        self, command: GetSpecCommand, *, actor: ActorContext, uow: Any
    ) -> GetSpecResult:
        spec = await SpecService(session_of(uow)).get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        return GetSpecResult(spec)


# --- move (status transition) -----------------------------------------------


class MoveSpecCommand:
    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Any) -> None:
        self.spec_id = spec_id
        self.data = data


class MoveSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class MoveSpecUseCase:
    """Change a spec's status (write). The gate errors
    (``GateContractError``/``ResourceGateError``/``ValueError``) raised by
    ``move_spec`` propagate for the adapter to map (409/409/400); a missing spec
    is ``EntityNotFoundError`` (404)."""

    async def execute(
        self, command: MoveSpecCommand, *, actor: ActorContext, uow: Any
    ) -> MoveSpecResult:
        service = SpecService(session_of(uow))
        spec = await service.move_spec(command.spec_id, actor.actor_id, command.data)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return MoveSpecResult(await service.get_spec(command.spec_id))


# --- delete -----------------------------------------------------------------


class DeleteSpecCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class DeleteSpecResult:
    __slots__ = ()


class DeleteSpecUseCase:
    """Delete a spec, unlinking derived cards (write). 404 when missing."""

    async def execute(
        self, command: DeleteSpecCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteSpecResult:
        service = SpecService(session_of(uow))
        deleted = await service.delete_spec(command.spec_id, actor.actor_id)
        if not deleted:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return DeleteSpecResult()


# --- history ----------------------------------------------------------------


class ListSpecHistoryCommand:
    __slots__ = ("spec_id", "limit")

    def __init__(self, spec_id: str, *, limit: int = 50) -> None:
        self.spec_id = spec_id
        self.limit = limit


class ListSpecHistoryResult:
    __slots__ = ("history",)

    def __init__(self, history: list[Any]) -> None:
        self.history = history


class ListSpecHistoryUseCase:
    """Read a spec's change history (read, no commit)."""

    async def execute(
        self, command: ListSpecHistoryCommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecHistoryResult:
        history = await SpecService(session_of(uow)).list_history(
            command.spec_id, command.limit
        )
        return ListSpecHistoryResult(history)


# ===========================================================================
# R01A REST-FU3a: update_spec permission-requirements helper chain (moved here
# from api/specs.py; pure, isolated to update_spec) + the use case.
# ===========================================================================


def _schema_item(value) -> dict:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return dict(value)
    return {"value": value}

def _items_by_id(items) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for index, item in enumerate(items or []):
        data = _schema_item(item)
        key = str(data.get("id") or f"__index_{index}")
        indexed[key] = data
    return indexed

def _without_linked_tasks(item: dict) -> dict:
    data = dict(item)
    data.pop("linked_task_ids", None)
    return data

def _canonical_requirement_for_edit(item: dict) -> dict:
    data = _without_linked_tasks(item)
    neutral_defaults = {
        "description": "",
        "status": "active",
        "integration_type": "api",
        "signal_type": "metric",
    }
    for key, default in neutral_defaults.items():
        if data.get(key) == default:
            data.pop(key, None)
    return {
        key: value
        for key, value in data.items()
        if value not in (None, [], {})
    }

def _linked_task_ids(item: dict) -> list[str]:
    return sorted(str(value) for value in (item.get("linked_task_ids") or []))

def _requirement_change_permissions(*, current_items, next_items, prefix: str) -> set[str]:
    current = _items_by_id(current_items)
    next_by_key = _items_by_id(next_items)
    current_keys = set(current)
    next_keys = set(next_by_key)

    required: set[str] = set()
    if next_keys - current_keys:
        required.add(f"{prefix}.create")
        if any(_linked_task_ids(next_by_key[key]) for key in next_keys - current_keys):
            required.add(f"{prefix}.link_task")
    if current_keys - next_keys:
        required.add(f"{prefix}.delete")

    for key in current_keys & next_keys:
        before = current[key]
        after = next_by_key[key]
        if _linked_task_ids(before) != _linked_task_ids(after):
            required.add(f"{prefix}.link_task")
        if _canonical_requirement_for_edit(before) != _canonical_requirement_for_edit(after):
            required.add(f"{prefix}.edit")

    return required

def _spec_update_permission_requirements(spec, data: SpecUpdate) -> set[str]:
    fields_set = set(
        getattr(data, "model_fields_set", None)
        or getattr(data, "__fields_set__", set())
    )
    required: set[str] = set()

    if "integration_requirements" in fields_set:
        required.update(
            _requirement_change_permissions(
                current_items=getattr(spec, "integration_requirements", None) or [],
                next_items=data.integration_requirements or [],
                prefix="spec.integration_requirements",
            )
        )
        if "spec.integration_requirements.link_task" in required:
            required.add("card.link_to.ir")
    if "observability_requirements" in fields_set:
        required.update(
            _requirement_change_permissions(
                current_items=getattr(spec, "observability_requirements", None) or [],
                next_items=data.observability_requirements or [],
                prefix="spec.observability_requirements",
            )
        )
        if "spec.observability_requirements.link_task" in required:
            required.add("card.link_to.or")
    if {"skip_ir_coverage", "skip_or_coverage"} & fields_set:
        required.add("spec.entity.edit_coverage_flags")

    return required

# --- update (permission-gated; spec_update_permission_requirements chain moved
# here from api/specs.py — pure, used only by update_spec) -------------------


class UpdateSpecCommand:
    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Any) -> None:
        self.spec_id = spec_id
        self.data = data


class UpdateSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class UpdateSpecUseCase:
    """Update a spec (write). Resolves the actor's permissions and raises
    ``PermissionDeniedError`` (adapter -> 403) when a required permission is
    missing; ``EntityNotFoundError`` (404) when the spec is missing; the
    ``ValueError`` from ``update_spec`` (orphan linked refs) propagates for the
    adapter to map to 422. The deprecation header stays in the adapter."""

    async def execute(
        self, command: UpdateSpecCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateSpecResult:
        from okto_pulse.core.services.permission_policy import check_permission
        from okto_pulse.core.services.main import resolve_user_permissions

        session = session_of(uow)
        service = SpecService(session)
        existing = await service.get_spec(command.spec_id)
        if not existing:
            raise EntityNotFoundError("spec", command.spec_id)

        required = _spec_update_permission_requirements(existing, command.data)
        if required:
            permission_set = await resolve_user_permissions(
                session, actor.actor_id, existing.board_id
            )
            for permission in sorted(required):
                error = check_permission(permission_set, permission)
                if error:
                    raise PermissionDeniedError(error)

        spec = await service.update_spec(command.spec_id, actor.actor_id, command.data)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return UpdateSpecResult(await service.get_spec(command.spec_id))



# --- structured spec entities (REST-FU3b-S1) --------------------------------


class RunStructuredSpecEntityCommand:
    __slots__ = (
        "spec_id", "entity_type", "operation", "payload", "entity_id",
        "expected_spec_version", "task_id", "ack_token", "preview_only",
    )

    def __init__(
        self, spec_id: str, entity_type: str, operation: str, *,
        payload: dict[str, Any] | None = None, entity_id: str | None = None,
        expected_spec_version: int | None = None, task_id: str | None = None,
        ack_token: str | None = None, preview_only: bool = False,
    ) -> None:
        self.spec_id = spec_id
        self.entity_type = entity_type
        self.operation = operation
        self.payload = payload
        self.entity_id = entity_id
        self.expected_spec_version = expected_spec_version
        self.task_id = task_id
        self.ack_token = ack_token
        self.preview_only = preview_only


class RunStructuredSpecEntityResult:
    __slots__ = ("structured_result",)

    def __init__(self, structured_result: Any) -> None:
        self.structured_result = structured_result


class RunStructuredSpecEntityUseCase:
    """Apply a structured spec child-entity mutation (write). Raises
    ``EntityNotFoundError("spec")`` upfront; otherwise returns the
    ``StructuredSpecEntityResult`` for the adapter to map (success → body;
    failure → ``error_code`` → HTTP status). Commits only when the service
    reports changed fields; rolls back on failure — preserving the legacy
    ``_run_structured_spec_entity_command`` semantics exactly."""

    async def execute(
        self, command: RunStructuredSpecEntityCommand, *, actor: ActorContext, uow: Any
    ) -> RunStructuredSpecEntityResult:
        from okto_pulse.core.services.main import resolve_user_permissions
        from okto_pulse.core.services.spec_structured_entities import (
            StructuredSpecEntityCommand,
            StructuredSpecEntityService,
        )

        session = session_of(uow)
        spec = await SpecService(session).get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        permission_set = await resolve_user_permissions(session, actor.actor_id, spec.board_id)
        result = await StructuredSpecEntityService(session).apply(
            StructuredSpecEntityCommand(
                board_id=spec.board_id,
                spec_id=command.spec_id,
                actor_id=actor.actor_id,
                entity_type=command.entity_type,
                entity_id=command.entity_id,
                operation=command.operation,
                payload=command.payload or {},
                expected_spec_version=command.expected_spec_version,
                task_id=command.task_id,
                ack_token=command.ack_token,
                preview_only=command.preview_only,
                permission_set=permission_set,
            )
        )
        if not result.success:
            await session.rollback()
        elif result.changed_fields:
            await commit(uow)
        return RunStructuredSpecEntityResult(result)


# --- spec validation list (REST-FU3b-S1) ------------------------------------


class ListSpecValidationsCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class ListSpecValidationsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListSpecValidationsUseCase:
    """List a spec's validation-gate records (read). ``SpecService`` raises
    ``ValueError`` when the spec is missing — the adapter maps it to the same 404
    as before."""

    async def execute(
        self, command: ListSpecValidationsCommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecValidationsResult:
        result = await SpecService(session_of(uow)).list_spec_validations(command.spec_id)
        return ListSpecValidationsResult(result)


# ===========================================================================
# R01A REST-FU3c-S2: spec card / scenario linking. Each use case wraps the
# EXISTING SpecService/CardService methods — the SQL inline in those methods
# stays in the service; only the transport envelope (lookup → not-found →
# mutate → commit) moves here. Not-found becomes ``EntityNotFoundError`` so the
# adapter reproduces the legacy 404 detail; ``ValueError`` (orphan refs) and
# ``CardOperationError`` (card workflow, a ValueError subclass) propagate for
# the adapter to map (422 / 409). ``SetTestScenarioStatusUseCase`` does NOT
# commit — ``SpecService.set_test_scenario_status`` persists+commits narrowly
# on its own, exactly as the legacy endpoint relied on.
# ===========================================================================


# --- link card to spec ------------------------------------------------------


class LinkCardToSpecCommand:
    __slots__ = ("spec_id", "card_id")

    def __init__(self, spec_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.card_id = card_id


class LinkCardToSpecResult:
    __slots__ = ("spec_id", "card_id")

    def __init__(self, spec_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.card_id = card_id


class LinkCardToSpecUseCase:
    """Link an existing card to a spec (write). ``EntityNotFoundError`` when the
    spec/card is missing or they belong to different boards — the adapter maps it
    to the legacy 404. ``SpecService.link_card`` raises ``ValueError`` for a
    non-linkable spec status; that propagates unchanged (the legacy endpoint did
    not catch it)."""

    async def execute(
        self, command: LinkCardToSpecCommand, *, actor: ActorContext, uow: Any
    ) -> LinkCardToSpecResult:
        service = SpecService(session_of(uow))
        linked = await service.link_card(
            command.spec_id, command.card_id, user_id=actor.actor_id
        )
        if not linked:
            raise EntityNotFoundError("spec_or_card", command.spec_id)
        await commit(uow)
        return LinkCardToSpecResult(command.spec_id, command.card_id)


# --- unlink card from spec --------------------------------------------------


class UnlinkCardFromSpecCommand:
    __slots__ = ("spec_id", "card_id")

    def __init__(self, spec_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.card_id = card_id


class UnlinkCardFromSpecResult:
    __slots__ = ("spec_id", "card_id")

    def __init__(self, spec_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.card_id = card_id


class UnlinkCardFromSpecUseCase:
    """Unlink a card from its spec (write). ``EntityNotFoundError("card")`` when
    the card is missing or not linked — the adapter maps it to the legacy 404."""

    async def execute(
        self, command: UnlinkCardFromSpecCommand, *, actor: ActorContext, uow: Any
    ) -> UnlinkCardFromSpecResult:
        service = SpecService(session_of(uow))
        unlinked = await service.unlink_card(command.card_id, user_id=actor.actor_id)
        if not unlinked:
            raise EntityNotFoundError("card", command.card_id)
        await commit(uow)
        return UnlinkCardFromSpecResult(command.spec_id, command.card_id)


# --- link task to scenario --------------------------------------------------


class LinkTaskToScenarioCommand:
    __slots__ = ("spec_id", "scenario_id", "card_id")

    def __init__(self, spec_id: str, scenario_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.card_id = card_id


class LinkTaskToScenarioResult:
    __slots__ = ("spec_id", "scenario_id", "card_id")

    def __init__(self, spec_id: str, scenario_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.card_id = card_id


class LinkTaskToScenarioUseCase:
    """Bidirectionally link a task card to a test scenario (write). Validates
    spec, card and scenario existence upfront — each raises a typed
    ``EntityNotFoundError`` (entity_type ``spec`` / ``card`` / ``scenario``) the
    adapter maps to the matching legacy 404 detail. ``update_spec`` may raise
    ``ValueError`` (orphan refs → 422) and ``update_card`` may raise
    ``CardOperationError`` (→ 409); both propagate before the commit, so a
    failure leaves the request transaction uncommitted, exactly as before."""

    async def execute(
        self, command: LinkTaskToScenarioCommand, *, actor: ActorContext, uow: Any
    ) -> LinkTaskToScenarioResult:
        from okto_pulse.core.services.application_schemas import CardUpdate
        from okto_pulse.core.services import CardService

        session = session_of(uow)
        spec_service = SpecService(session)
        spec = await spec_service.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)

        card_service = CardService(session)
        card = await card_service.get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        scenarios = list(spec.test_scenarios or [])
        found = False
        for scenario in scenarios:
            if scenario.get("id") == command.scenario_id:
                task_ids = list(scenario.get("linked_task_ids") or [])
                if command.card_id not in task_ids:
                    task_ids.append(command.card_id)
                scenario["linked_task_ids"] = task_ids
                found = True
                break
        if not found:
            raise EntityNotFoundError("scenario", command.scenario_id)

        await spec_service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(test_scenarios=scenarios)
        )

        existing = list(card.test_scenario_ids or [])
        if command.scenario_id not in existing:
            existing.append(command.scenario_id)
        await card_service.update_card(
            command.card_id, actor.actor_id, CardUpdate(test_scenario_ids=existing)
        )

        await commit(uow)
        return LinkTaskToScenarioResult(
            command.spec_id, command.scenario_id, command.card_id
        )


# --- unlink task from scenario ----------------------------------------------


class UnlinkTaskFromScenarioCommand:
    __slots__ = ("spec_id", "scenario_id", "card_id")

    def __init__(self, spec_id: str, scenario_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.card_id = card_id


class UnlinkTaskFromScenarioResult:
    __slots__ = ("spec_id", "scenario_id", "card_id")

    def __init__(self, spec_id: str, scenario_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.card_id = card_id


class UnlinkTaskFromScenarioUseCase:
    """Bidirectionally unlink a task card from a test scenario (write). Raises
    ``EntityNotFoundError`` (entity_type ``spec`` / ``scenario``) the adapter
    maps to the legacy 404 details. Mirrors the legacy endpoint: the card side is
    best-effort (skipped when the card is gone) and ``update_spec`` is not
    error-wrapped, so any ``ValueError`` propagates unchanged."""

    async def execute(
        self, command: UnlinkTaskFromScenarioCommand, *, actor: ActorContext, uow: Any
    ) -> UnlinkTaskFromScenarioResult:
        from okto_pulse.core.services.application_schemas import CardUpdate
        from okto_pulse.core.services import CardService

        session = session_of(uow)
        spec_service = SpecService(session)
        spec = await spec_service.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)

        scenarios = list(spec.test_scenarios or [])
        found = False
        for scenario in scenarios:
            if scenario.get("id") == command.scenario_id:
                task_ids = list(scenario.get("linked_task_ids") or [])
                if command.card_id in task_ids:
                    task_ids.remove(command.card_id)
                scenario["linked_task_ids"] = task_ids
                found = True
                break
        if not found:
            raise EntityNotFoundError("scenario", command.scenario_id)

        await spec_service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(test_scenarios=scenarios)
        )

        card_service = CardService(session)
        card = await card_service.get_card(command.card_id)
        if card:
            existing = list(card.test_scenario_ids or [])
            if command.scenario_id in existing:
                existing.remove(command.scenario_id)
            await card_service.update_card(
                command.card_id, actor.actor_id, CardUpdate(test_scenario_ids=existing)
            )

        await commit(uow)
        return UnlinkTaskFromScenarioResult(
            command.spec_id, command.scenario_id, command.card_id
        )


# --- scoped test-scenario status mutation -----------------------------------


class SetTestScenarioStatusCommand:
    __slots__ = ("spec_id", "scenario_id", "status", "evidence")

    def __init__(
        self,
        spec_id: str,
        scenario_id: str,
        status: str,
        evidence: dict | None = None,
    ) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.status = status
        self.evidence = evidence


class SetTestScenarioStatusResult:
    __slots__ = ("result",)

    def __init__(self, result: dict) -> None:
        self.result = result


class SetTestScenarioStatusUseCase:
    """Scoped operational status mutation for ONE test scenario (write). Delegates
    to ``SpecService.set_test_scenario_status`` (SQL inline + narrow
    persist+commit live there) and does NOT commit through the UoW — the service
    commits its narrow update itself, exactly as the legacy endpoint relied on.
    ``StatusNotMutableError`` (→ 409) and ``ValueError``
    (``scenario_not_found`` → 404, otherwise → 422) propagate for the adapter."""

    async def execute(
        self, command: SetTestScenarioStatusCommand, *, actor: ActorContext, uow: Any
    ) -> SetTestScenarioStatusResult:
        result = await SpecService(session_of(uow)).set_test_scenario_status(
            command.spec_id,
            actor.actor_id,
            command.scenario_id,
            command.status,
            command.evidence,
        )
        return SetTestScenarioStatusResult(result)


# ===========================================================================
# R01A REST-FU3e-S3: spec integration / observability requirement task-linking.
# Each use case wraps the EXISTING SpecService method — the SQL inline stays in
# the service; only the transport envelope (lookup → permission → not-found →
# mutate → commit) moves here. Mirrors the legacy endpoints EXACTLY: the
# permission pair the legacy ``_require_permissions`` enforced is resolved INSIDE
# the use case (in the same order, first failure → ``PermissionDeniedError`` →
# 403), the card is looked up ONLY to validate existence (these endpoints persist
# solely via ``update_spec`` — there is NO card-side mutation), the requirement
# is matched by ``id`` (missing → typed ``EntityNotFoundError`` →
# entity_type ``integration_requirement`` / ``observability_requirement`` so the
# adapter reproduces the per-entity 404 detail), and ``update_spec``'s
# ``ValueError`` (orphan refs → 422) propagates before the commit so a failure
# leaves the request transaction uncommitted, exactly as before.
# ===========================================================================


async def _check_requirement_link_permissions(
    session: Any, actor_id: str, board_id: str, permissions: tuple[str, ...]
) -> None:
    """Resolve the actor's permission set and enforce ``permissions`` in order,
    raising ``PermissionDeniedError`` on the first failure — the transport-free
    equivalent of the legacy ``_require_permissions`` guard."""
    from okto_pulse.core.services.permission_policy import check_permission
    from okto_pulse.core.services.main import resolve_user_permissions

    permission_set = await resolve_user_permissions(session, actor_id, board_id)
    for permission in permissions:
        error = check_permission(permission_set, permission)
        if error:
            raise PermissionDeniedError(error)


# --- link task to integration requirement -----------------------------------


class LinkTaskToIntegrationRequirementCommand:
    __slots__ = ("spec_id", "requirement_id", "card_id")

    def __init__(self, spec_id: str, requirement_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.requirement_id = requirement_id
        self.card_id = card_id


class LinkTaskToIntegrationRequirementResult:
    __slots__ = ("spec_id", "requirement_id", "card_id")

    def __init__(self, spec_id: str, requirement_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.requirement_id = requirement_id
        self.card_id = card_id


class LinkTaskToIntegrationRequirementUseCase:
    """Link a task card to a spec integration requirement (write). Resolves the
    ``spec.integration_requirements.link_task`` + ``card.link_to.ir`` pair inside
    the use case (legacy ``_require_permissions`` order) → ``PermissionDeniedError``
    (403); ``EntityNotFoundError`` for the missing spec / card / requirement (the
    adapter maps each to its legacy 404 detail); persists via
    ``SpecService.update_spec`` whose ``ValueError`` (orphan refs → 422)
    propagates before the commit. No card-side mutation — exactly as the legacy
    endpoint."""

    async def execute(
        self,
        command: LinkTaskToIntegrationRequirementCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> LinkTaskToIntegrationRequirementResult:
        from okto_pulse.core.services import CardService

        session = session_of(uow)
        spec_service = SpecService(session)
        spec = await spec_service.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)

        await _check_requirement_link_permissions(
            session,
            actor.actor_id,
            spec.board_id,
            ("spec.integration_requirements.link_task", "card.link_to.ir"),
        )

        card = await CardService(session).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        requirements = list(spec.integration_requirements or [])
        target = next(
            (item for item in requirements if item.get("id") == command.requirement_id),
            None,
        )
        if target is None:
            raise EntityNotFoundError("integration_requirement", command.requirement_id)

        task_ids = list(target.get("linked_task_ids") or [])
        if command.card_id not in task_ids:
            task_ids.append(command.card_id)
        target["linked_task_ids"] = task_ids

        await spec_service.update_spec(
            command.spec_id,
            actor.actor_id,
            SpecUpdate(integration_requirements=requirements),
        )
        await commit(uow)
        return LinkTaskToIntegrationRequirementResult(
            command.spec_id, command.requirement_id, command.card_id
        )


# --- link task to observability requirement ---------------------------------


class LinkTaskToObservabilityRequirementCommand:
    __slots__ = ("spec_id", "requirement_id", "card_id")

    def __init__(self, spec_id: str, requirement_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.requirement_id = requirement_id
        self.card_id = card_id


class LinkTaskToObservabilityRequirementResult:
    __slots__ = ("spec_id", "requirement_id", "card_id")

    def __init__(self, spec_id: str, requirement_id: str, card_id: str) -> None:
        self.spec_id = spec_id
        self.requirement_id = requirement_id
        self.card_id = card_id


class LinkTaskToObservabilityRequirementUseCase:
    """Link a task card to a spec observability requirement (write). Resolves the
    ``spec.observability_requirements.link_task`` + ``card.link_to.or`` pair inside
    the use case (legacy ``_require_permissions`` order) → ``PermissionDeniedError``
    (403); ``EntityNotFoundError`` for the missing spec / card / requirement (the
    adapter maps each to its legacy 404 detail); persists via
    ``SpecService.update_spec`` whose ``ValueError`` (orphan refs → 422)
    propagates before the commit. No card-side mutation — exactly as the legacy
    endpoint."""

    async def execute(
        self,
        command: LinkTaskToObservabilityRequirementCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> LinkTaskToObservabilityRequirementResult:
        from okto_pulse.core.services import CardService

        session = session_of(uow)
        spec_service = SpecService(session)
        spec = await spec_service.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)

        await _check_requirement_link_permissions(
            session,
            actor.actor_id,
            spec.board_id,
            ("spec.observability_requirements.link_task", "card.link_to.or"),
        )

        card = await CardService(session).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        requirements = list(spec.observability_requirements or [])
        target = next(
            (item for item in requirements if item.get("id") == command.requirement_id),
            None,
        )
        if target is None:
            raise EntityNotFoundError("observability_requirement", command.requirement_id)

        task_ids = list(target.get("linked_task_ids") or [])
        if command.card_id not in task_ids:
            task_ids.append(command.card_id)
        target["linked_task_ids"] = task_ids

        await spec_service.update_spec(
            command.spec_id,
            actor.actor_id,
            SpecUpdate(observability_requirements=requirements),
        )
        await commit(uow)
        return LinkTaskToObservabilityRequirementResult(
            command.spec_id, command.requirement_id, command.card_id
        )


# ===========================================================================
# R01A REST-FU3d-S4: spec knowledge base / Q&A / evaluation. This closes
# api/specs.py (0 endpoints left on get_db). Each use case wraps the EXISTING
# SpecKnowledgeService / SpecQAService / SpecService method — the SQL inline in
# those readers (``list_knowledge`` / ``list_qa`` / ``list_spec_evaluations``)
# stays in the service; only the transport envelope (lookup → not-found →
# mutate → commit) moves here. The per-endpoint 404 detail strings live in the
# adapter, keyed off the typed ``EntityNotFoundError`` entity_type
# (``spec`` → "Spec not found", ``spec_knowledge`` → "Knowledge base item not
# found", ``spec_qa`` → "Q&A item not found"). Reads do NOT commit; writes
# commit(uow) after the service mutation, exactly as the legacy endpoints did.
# ===========================================================================


# --- spec knowledge: list ---------------------------------------------------


class ListSpecKnowledgeCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class ListSpecKnowledgeResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListSpecKnowledgeUseCase:
    """List a spec's knowledge base items without content (read, no commit).
    Mirrors the legacy endpoint exactly: no existence check — an unknown spec
    simply yields an empty list."""

    async def execute(
        self, command: ListSpecKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecKnowledgeResult:
        from okto_pulse.core.services import SpecKnowledgeService

        items = await SpecKnowledgeService(session_of(uow)).list_knowledge(command.spec_id)
        return ListSpecKnowledgeResult(items)


# --- spec knowledge: get ----------------------------------------------------


class GetSpecKnowledgeCommand:
    __slots__ = ("spec_id", "knowledge_id")

    def __init__(self, spec_id: str, knowledge_id: str) -> None:
        self.spec_id = spec_id
        self.knowledge_id = knowledge_id


class GetSpecKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class GetSpecKnowledgeUseCase:
    """Fetch one knowledge base item with full content (read, no commit).
    ``EntityNotFoundError("spec_knowledge")`` when the item is missing OR belongs
    to a different spec — the adapter maps it to the legacy 404 detail."""

    async def execute(
        self, command: GetSpecKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> GetSpecKnowledgeResult:
        from okto_pulse.core.services import SpecKnowledgeService

        kb = await SpecKnowledgeService(session_of(uow)).get_knowledge(command.knowledge_id)
        if not kb or kb.spec_id != command.spec_id:
            raise EntityNotFoundError("spec_knowledge", command.knowledge_id)
        return GetSpecKnowledgeResult(kb)


# --- spec knowledge: create -------------------------------------------------


class CreateSpecKnowledgeCommand:
    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Any) -> None:
        self.spec_id = spec_id
        self.data = data


class CreateSpecKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class CreateSpecKnowledgeUseCase:
    """Add a knowledge base item to a spec (write). ``create_knowledge`` returns
    ``None`` when the spec is missing → ``EntityNotFoundError("spec")`` (adapter →
    404 "Spec not found"); commits after the service mutation."""

    async def execute(
        self, command: CreateSpecKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> CreateSpecKnowledgeResult:
        from okto_pulse.core.services import SpecKnowledgeService

        kb = await SpecKnowledgeService(session_of(uow)).create_knowledge(
            command.spec_id, actor.actor_id, command.data
        )
        if not kb:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return CreateSpecKnowledgeResult(kb)


# --- spec knowledge: delete -------------------------------------------------


class DeleteSpecKnowledgeCommand:
    __slots__ = ("spec_id", "knowledge_id")

    def __init__(self, spec_id: str, knowledge_id: str) -> None:
        self.spec_id = spec_id
        self.knowledge_id = knowledge_id


class DeleteSpecKnowledgeResult:
    __slots__ = ()


class DeleteSpecKnowledgeUseCase:
    """Delete a knowledge base item (write). ``EntityNotFoundError("spec_knowledge")``
    when the item is missing OR belongs to a different spec (same upfront check as
    the legacy endpoint); commits after the delete."""

    async def execute(
        self, command: DeleteSpecKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteSpecKnowledgeResult:
        from okto_pulse.core.services import SpecKnowledgeService

        service = SpecKnowledgeService(session_of(uow))
        kb = await service.get_knowledge(command.knowledge_id)
        if not kb or kb.spec_id != command.spec_id:
            raise EntityNotFoundError("spec_knowledge", command.knowledge_id)
        await service.delete_knowledge(command.knowledge_id)
        await commit(uow)
        return DeleteSpecKnowledgeResult()


# --- spec Q&A: list ---------------------------------------------------------


class ListSpecQACommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class ListSpecQAResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListSpecQAUseCase:
    """List a spec's Q&A items (read, no commit). Mirrors the legacy endpoint: no
    existence check — an unknown spec simply yields an empty list."""

    async def execute(
        self, command: ListSpecQACommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecQAResult:
        from okto_pulse.core.services import SpecQAService

        items = await SpecQAService(session_of(uow)).list_qa(command.spec_id)
        return ListSpecQAResult(items)


# --- spec Q&A: create question ----------------------------------------------


class CreateSpecQuestionCommand:
    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Any) -> None:
        self.spec_id = spec_id
        self.data = data


class CreateSpecQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class CreateSpecQuestionUseCase:
    """Ask a question on a spec (write). ``create_question`` returns ``None`` when
    the spec is missing → ``EntityNotFoundError("spec")`` (adapter → 404 "Spec not
    found"); commits after the service mutation."""

    async def execute(
        self, command: CreateSpecQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> CreateSpecQuestionResult:
        from okto_pulse.core.services import SpecQAService

        qa = await SpecQAService(session_of(uow)).create_question(
            command.spec_id, actor.actor_id, command.data
        )
        if not qa:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return CreateSpecQuestionResult(qa)


# --- spec Q&A: answer question ----------------------------------------------


class AnswerSpecQuestionCommand:
    __slots__ = ("qa_id", "data")

    def __init__(self, qa_id: str, data: Any) -> None:
        self.qa_id = qa_id
        self.data = data


class AnswerSpecQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class AnswerSpecQuestionUseCase:
    """Answer a spec Q&A question (write). Calls ``answer_question`` with the REST
    surface/actor_type. Preserves the legacy self-answer semantics EXACTLY: on
    ``QASelfAnsweringNotAllowedError`` the transaction is COMMITTED (the
    authorization audit side-effect persists) and the error re-raised for the
    adapter to map to 403 with its ``{reason, message}`` detail; ``None`` (no such
    Q&A or nothing persisted) → ``EntityNotFoundError("spec_qa")`` (404 "Q&A item
    not found"); a successful answer commits."""

    async def execute(
        self, command: AnswerSpecQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> AnswerSpecQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError, SpecQAService

        service = SpecQAService(session_of(uow))
        try:
            qa = await service.answer_question(
                command.qa_id, actor.actor_id, command.data,
                actor_type="user", surface="rest",
            )
        except QASelfAnsweringNotAllowedError:
            await commit(uow)
            raise
        if not qa:
            raise EntityNotFoundError("spec_qa", command.qa_id)
        await commit(uow)
        return AnswerSpecQuestionResult(qa)


# --- spec Q&A: delete question ----------------------------------------------


class DeleteSpecQuestionCommand:
    __slots__ = ("qa_id",)

    def __init__(self, qa_id: str) -> None:
        self.qa_id = qa_id


class DeleteSpecQuestionResult:
    __slots__ = ()


class DeleteSpecQuestionUseCase:
    """Delete a spec Q&A item (write). ``delete_question`` returns ``False`` when
    the item is missing → ``EntityNotFoundError("spec_qa")`` (404 "Q&A item not
    found"); commits after the delete."""

    async def execute(
        self, command: DeleteSpecQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteSpecQuestionResult:
        from okto_pulse.core.services import SpecQAService

        deleted = await SpecQAService(session_of(uow)).delete_question(command.qa_id)
        if not deleted:
            raise EntityNotFoundError("spec_qa", command.qa_id)
        await commit(uow)
        return DeleteSpecQuestionResult()


# --- spec evaluation: submit ------------------------------------------------


class SubmitSpecEvaluationCommand:
    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: dict) -> None:
        self.spec_id = spec_id
        self.data = data


class SubmitSpecEvaluationResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict) -> None:
        self.payload = payload


class SubmitSpecEvaluationUseCase:
    """Submit a qualitative evaluation for a spec in 'validated' status (write).
    ``EntityNotFoundError("spec")`` when the spec is missing (adapter → 404);
    resolves the evaluator display name best-effort (any failure falls back to the
    actor id, exactly as the legacy endpoint); ``submit_spec_evaluation``'s
    ``GateContractError`` (→ 409 ``to_dict``) and ``ValueError`` (→ 409 ``str``)
    propagate for the adapter to map. Captures spec status before/after
    independently (append-only evaluation) and returns the same success envelope
    as the MCP twin."""

    async def execute(
        self, command: SubmitSpecEvaluationCommand, *, actor: ActorContext, uow: Any
    ) -> SubmitSpecEvaluationResult:
        from okto_pulse.core.services.gate_contracts import (
            spec_evaluation_success_envelope,
        )
        from okto_pulse.core.services.main import resolve_actor_name

        session = session_of(uow)
        service = SpecService(session)
        spec = await service.get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)

        try:
            evaluator_name = await resolve_actor_name(session, actor.actor_id, spec.board_id)
        except Exception:
            evaluator_name = actor.actor_id

        status_before = spec.status.value
        evaluation = await service.submit_spec_evaluation(
            command.spec_id,
            actor.actor_id,
            evaluator_name,
            command.data,
            actor_type="user",
            surface="api",
        )
        spec_after = await service.get_spec(command.spec_id)
        status_after = spec_after.status.value if spec_after else status_before
        await commit(uow)
        return SubmitSpecEvaluationResult(
            spec_evaluation_success_envelope(
                spec_id=command.spec_id,
                status_before=status_before,
                status_after=status_after,
                evaluation=evaluation,
            )
        )


# --- spec evaluation: list --------------------------------------------------


class ListSpecEvaluationsCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class ListSpecEvaluationsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListSpecEvaluationsUseCase:
    """List a spec's evaluations newest-first (read, no commit). ``SpecService``
    raises ``ValueError`` when the spec is missing — the adapter maps it to the
    same 404 as the legacy endpoint."""

    async def execute(
        self, command: ListSpecEvaluationsCommand, *, actor: ActorContext, uow: Any
    ) -> ListSpecEvaluationsResult:
        result = await SpecService(session_of(uow)).list_spec_evaluations(command.spec_id)
        return ListSpecEvaluationsResult(result)
