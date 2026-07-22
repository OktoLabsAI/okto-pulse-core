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

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.services.application_schemas import SpecUpdate


_SPEC_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}


def _query_scope_for_actor(
    actor: ActorContext, *, board_id: str | None = None
) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        allowed_board_ids={board_id} if board_id else set(),
        require_ownership=False,
    )


async def _require_actor_board_spec(
    scope: PulseUnitOfWork | ApplicationServiceCatalog,
    spec_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> Any:
    """Resolve ``Spec -> Board -> Actor`` before any child or side effect.

    REST callers must provide the UoW so owner/share and realm checks can be
    performed.  The services-only compatibility form is deliberately limited
    to an already-authenticated, board-bound MCP actor.
    """
    uow = scope if hasattr(scope, "services") else None
    services = uow.services if uow is not None else scope
    spec = await services.specs.get_spec(spec_id)
    if spec is None:
        raise EntityNotFoundError("spec", spec_id)

    # MCP authentication has already resolved the agent grant and bound the
    # actor to exactly one board.  That authenticated binding is the sole MCP
    # authority here; never reinterpret an MCP actor as a REST owner/share.
    if actor.source == "mcp":
        if actor.board_id == spec.board_id:
            return spec
        raise EntityNotFoundError("spec", spec_id)

    if uow is None:
        raise EntityNotFoundError("spec", spec_id)

    board = await load_accessible_board(
        uow,
        spec.board_id,
        actor,
        allowed_share_permissions=(
            _SPEC_WRITE_SHARE_PERMISSIONS if write else None
        ),
    )
    if board is None:
        raise EntityNotFoundError("spec", spec_id)
    return spec


async def _require_spec_board_card(
    services: ApplicationServiceCatalog, card_id: str, spec: Any
) -> Any:
    """Load a Card without disclosing cards outside the resolved Spec board."""
    card = await services.cards.get_card(card_id)
    if card is None or card.board_id != spec.board_id:
        raise EntityNotFoundError("card", card_id)
    return card


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
    missing/not owned — the adapter maps to the legacy 404. Explicit ideation/
    refinement parents are resolved before the dependent Spec FK write; canonical
    lineage failures propagate without creating a Spec."""

    async def execute(
        self, command: CreateSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateSpecResult:
        service = uow.services.specs
        if await load_accessible_board(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions=_SPEC_WRITE_SHARE_PERMISSIONS,
        ) is None:
            raise EntityNotFoundError("board", command.board_id)
        ideation_id = getattr(command.data, "ideation_id", None)
        refinement_id = getattr(command.data, "refinement_id", None)
        if ideation_id or refinement_id:
            await uow.services.resolve_effective_spec_parent_lineage(
                board_id=command.board_id,
                ideation_id=ideation_id,
                refinement_id=refinement_id,
            )

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
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        include_archived: bool = False,
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

    async def preflight(
        self, command: ListSpecsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> None:
        """Authorize the board without materializing its legacy specs."""
        board = await load_accessible_board(
            uow,
            command.board_id,
            actor,
        )
        if not board:
            raise EntityNotFoundError("board", command.board_id)

    async def execute(
        self, command: ListSpecsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListSpecsResult:
        await self.preflight(command, actor=actor, uow=uow)
        specs = await uow.services.specs.list_specs(
            command.board_id,
            command.status_filter,
            include_archived=command.include_archived,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
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
    """Fetch a board-scoped spec with its derived cards (read)."""

    async def execute(
        self, command: GetSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetSpecResult:
        spec = await _require_actor_board_spec(uow, command.spec_id, actor)
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
        self, command: MoveSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MoveSpecResult:
        service = uow.services.specs
        await _require_actor_board_spec(uow, command.spec_id, actor, write=True)
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
        self, command: DeleteSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteSpecResult:
        service = uow.services.specs
        await _require_actor_board_spec(uow, command.spec_id, actor, write=True)
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
        self,
        command: ListSpecHistoryCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSpecHistoryResult:
        await _require_actor_board_spec(uow, command.spec_id, actor)
        history = await uow.services.specs.list_history(command.spec_id, command.limit)
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
    return {key: value for key, value in data.items() if value not in (None, [], {})}


def _linked_task_ids(item: dict) -> list[str]:
    return sorted(str(value) for value in (item.get("linked_task_ids") or []))


def _requirement_change_permissions(
    *, current_items, next_items, prefix: str
) -> set[str]:
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
        if _canonical_requirement_for_edit(before) != _canonical_requirement_for_edit(
            after
        ):
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
        self, command: UpdateSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateSpecResult:
        from okto_pulse.core.services.permission_policy import check_permission

        service = uow.services.specs
        existing = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )

        required = _spec_update_permission_requirements(existing, command.data)
        if required:
            permission_set = await uow.services.resolve_user_permissions(
                actor.actor_id,
                existing.board_id,
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
        "spec_id",
        "entity_type",
        "operation",
        "payload",
        "entity_id",
        "expected_spec_version",
        "task_id",
        "ack_token",
        "preview_only",
    )

    def __init__(
        self,
        spec_id: str,
        entity_type: str,
        operation: str,
        *,
        payload: dict[str, Any] | None = None,
        entity_id: str | None = None,
        expected_spec_version: int | None = None,
        task_id: str | None = None,
        ack_token: str | None = None,
        preview_only: bool = False,
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
        self,
        command: RunStructuredSpecEntityCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RunStructuredSpecEntityResult:
        from okto_pulse.core.services.spec_structured_entities import (
            StructuredSpecEntityCommand,
        )

        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        permission_set = await uow.services.resolve_user_permissions(
            actor.actor_id,
            spec.board_id,
        )
        result = await uow.services.structured_specs.apply(
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
            await uow.rollback()
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
    """List a board-scoped spec's validation-gate records (read).

    ``ValueError`` preserves the existing adapter contract for missing specs while
    the explicit preflight also hides specs outside the actor's board.
    """

    async def execute(
        self,
        command: ListSpecValidationsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSpecValidationsResult:
        try:
            await _require_actor_board_spec(uow, command.spec_id, actor)
        except EntityNotFoundError as exc:
            raise ValueError("Spec not found") from exc
        result = await uow.services.specs.list_spec_validations(command.spec_id)
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
        self,
        command: LinkCardToSpecCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> LinkCardToSpecResult:
        service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        await _require_spec_board_card(uow.services, command.card_id, spec)
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
        self,
        command: UnlinkCardFromSpecCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UnlinkCardFromSpecResult:
        service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        card = await _require_spec_board_card(uow.services, command.card_id, spec)
        if card.spec_id != command.spec_id:
            raise EntityNotFoundError("card", command.card_id)
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
    __slots__ = ("spec_id", "scenario_id", "card_id", "coverage")

    def __init__(
        self,
        spec_id: str,
        scenario_id: str,
        card_id: str,
        *,
        coverage: dict | None = None,
    ) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.card_id = card_id
        self.coverage = coverage or {}


class LinkTaskToScenarioUseCase:
    """Bidirectionally link a task card to a test scenario (write). Validates
    spec, card and scenario existence upfront — each raises a typed
    ``EntityNotFoundError`` (entity_type ``spec`` / ``card`` / ``scenario``) the
    adapter maps to the matching legacy 404 detail. ``update_spec`` may raise
    ``ValueError`` (orphan refs → 422) and ``update_card`` may raise
    ``CardOperationError`` (→ 409); both propagate before the commit, so a
    failure leaves the request transaction uncommitted, exactly as before."""

    async def execute(
        self,
        command: LinkTaskToScenarioCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> LinkTaskToScenarioResult:
        from okto_pulse.core.application.use_cases.card_traceability import (
            LinkCardTraceabilityCommand,
            LinkCardTraceabilityUseCase,
        )

        from okto_pulse.core.services.card_traceability import (
            TraceabilityTargetNotFoundError,
        )

        try:
            await LinkCardTraceabilityUseCase().execute(
                LinkCardTraceabilityCommand(
                    command.spec_id,
                    command.card_id,
                    [("scenario", command.scenario_id)],
                ),
                actor=actor,
                uow=uow,
            )
        except TraceabilityTargetNotFoundError as exc:
            raise EntityNotFoundError("scenario", command.scenario_id) from exc
        spec = await uow.services.specs.get_spec(command.spec_id)
        scenarios = list(spec.test_scenarios or []) if spec else []

        from okto_pulse.core.services.analytics_service import spec_coverage_summary

        coverage = spec_coverage_summary(spec, scenarios=scenarios)
        return LinkTaskToScenarioResult(
            command.spec_id,
            command.scenario_id,
            command.card_id,
            coverage=coverage,
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
    ``EntityNotFoundError`` (entity_type ``spec`` / ``card`` / ``scenario``) the
    adapter maps to a governed not-found envelope. Spec and card must both belong
    to the authenticated board before either side is mutated; ``update_spec`` is
    not error-wrapped, so any ``ValueError`` propagates unchanged."""

    async def execute(
        self,
        command: UnlinkTaskFromScenarioCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UnlinkTaskFromScenarioResult:
        from okto_pulse.core.services.application_schemas import CardUpdate

        spec_service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )

        card_service = uow.services.cards
        card = await _require_spec_board_card(uow.services, command.card_id, spec)

        scenarios = [dict(item) for item in (spec.test_scenarios or [])]
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
        self,
        command: SetTestScenarioStatusCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SetTestScenarioStatusResult:
        service = uow.services.specs
        try:
            await _require_actor_board_spec(
                uow, command.spec_id, actor, write=True
            )
        except EntityNotFoundError as exc:
            raise ValueError("scenario_not_found: spec not found") from exc
        result = await service.set_test_scenario_status(
            command.spec_id,
            actor.actor_id,
            command.scenario_id,
            command.status,
            command.evidence,
        )
        return SetTestScenarioStatusResult(result)


class ExecuteTestScenarioEvidenceCommand:
    __slots__ = (
        "spec_id",
        "scenario_id",
        "status",
        "manifest_ref",
        "inline_replay",
    )

    def __init__(
        self,
        spec_id: str,
        scenario_id: str,
        status: str,
        manifest_ref: str = "",
        inline_replay: object | None = None,
    ) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.status = status
        self.manifest_ref = manifest_ref
        self.inline_replay = inline_replay


class ExecuteTestScenarioEvidenceResult:
    __slots__ = ("evidence",)

    def __init__(self, evidence: dict[str, Any]) -> None:
        self.evidence = evidence


class ExecuteTestScenarioEvidenceUseCase:
    """Run an edition-owned replay and return an authenticated V2 receipt.

    The use case does not persist scenario state. Callers must pass the returned
    evidence unchanged to the scoped status mutation, which authenticates it
    again at the final write boundary.
    """

    async def execute(
        self,
        command: ExecuteTestScenarioEvidenceCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ExecuteTestScenarioEvidenceResult:
        from okto_pulse.core.ports.test_evidence import (
            TestEvidenceExecutionRequest,
            resolve_test_evidence_execution_issuer,
            resolve_test_evidence_write_verifier,
        )
        from okto_pulse.core.services.test_scenario_lifecycle import (
            GATED_STATUSES,
            compute_test_scenario_semantic_sha256,
        )

        # Resolve authorization before validating replay inputs.  A caller that
        # cannot write this Spec must observe the same not-found boundary for
        # every otherwise well-formed command, and must never reach a trusted
        # evidence issuer/verifier (or a scenario child lookup).
        spec = await _require_actor_board_spec(
            uow,
            command.spec_id,
            actor,
            write=True,
        )
        if command.status not in GATED_STATUSES:
            raise ValueError(
                "evidence_execution_status_invalid: expected automated, passed, or failed"
            )
        manifest_ref = (
            command.manifest_ref.strip()
            if isinstance(command.manifest_ref, str) and command.manifest_ref.strip()
            else None
        )
        inline_replay = command.inline_replay
        if isinstance(inline_replay, str) and not inline_replay.strip():
            inline_replay = None
        if manifest_ref is None and inline_replay is None:
            raise ValueError("evidence_v2.replay_source_required")
        if manifest_ref is not None and inline_replay is not None:
            raise ValueError("evidence_v2.replay_sources_mutually_exclusive")
        scenarios = [
            item for item in (spec.test_scenarios or []) if isinstance(item, dict)
        ]
        scenario = next(
            (item for item in scenarios if item.get("id") == command.scenario_id),
            None,
        )
        if scenario is None:
            raise EntityNotFoundError("scenario", command.scenario_id)
        scenario_sha256 = compute_test_scenario_semantic_sha256(
            board_id=spec.board_id,
            spec_id=command.spec_id,
            scenario=scenario,
            acceptance_criteria=list(getattr(spec, "acceptance_criteria", None) or []),
        )

        issuer = resolve_test_evidence_execution_issuer()
        verifier = resolve_test_evidence_write_verifier()
        if issuer is None or verifier is None:
            raise ValueError("evidence_v2.trusted_runtime_not_configured")
        issued = await issuer.execute(
            TestEvidenceExecutionRequest(
                board_id=spec.board_id,
                spec_id=command.spec_id,
                scenario_id=command.scenario_id,
                status=command.status,
                manifest_ref=manifest_ref,
                actor_id=actor.actor_id,
                scenario_sha256=scenario_sha256,
                inline_replay=inline_replay,
            )
        )
        evidence = dict(issued.evidence)
        verification = verifier.verify(
            board_id=spec.board_id,
            spec_id=command.spec_id,
            scenario_id=command.scenario_id,
            scenario_sha256=scenario_sha256,
            status=command.status,
            actor_id=actor.actor_id,
            evidence=evidence,
        )
        if not verification.verified:
            raise ValueError(
                "evidence_unverified: " + ", ".join(verification.reason_codes)
            )
        return ExecuteTestScenarioEvidenceResult(evidence)


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
    services: ApplicationServiceCatalog,
    actor_id: str,
    board_id: str,
    permissions: tuple[str, ...],
) -> None:
    """Resolve the actor's permission set and enforce ``permissions`` in order,
    raising ``PermissionDeniedError`` on the first failure — the transport-free
    equivalent of the legacy ``_require_permissions`` guard."""
    from okto_pulse.core.services.permission_policy import check_permission

    permission_set = await services.resolve_user_permissions(actor_id, board_id)
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
        uow: PulseUnitOfWork,
    ) -> LinkTaskToIntegrationRequirementResult:

        spec_service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        await _require_spec_board_card(uow.services, command.card_id, spec)

        await _check_requirement_link_permissions(
            uow.services,
            actor.actor_id,
            spec.board_id,
            ("spec.integration_requirements.link_task", "card.link_to.ir"),
        )

        requirements = [dict(item) for item in (spec.integration_requirements or [])]
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
        uow: PulseUnitOfWork,
    ) -> LinkTaskToObservabilityRequirementResult:

        spec_service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        await _require_spec_board_card(uow.services, command.card_id, spec)

        await _check_requirement_link_permissions(
            uow.services,
            actor.actor_id,
            spec.board_id,
            ("spec.observability_requirements.link_task", "card.link_to.or"),
        )

        requirements = [
            dict(item) for item in (spec.observability_requirements or [])
        ]
        target = next(
            (item for item in requirements if item.get("id") == command.requirement_id),
            None,
        )
        if target is None:
            raise EntityNotFoundError(
                "observability_requirement", command.requirement_id
            )

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


async def _require_actor_board_spec_knowledge(
    scope: PulseUnitOfWork | ApplicationServiceCatalog,
    spec_id: str,
    knowledge_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> Any:
    """Resolve both parents before exposing or mutating Spec Knowledge."""
    services = scope.services if hasattr(scope, "services") else scope
    spec = await _require_actor_board_spec(scope, spec_id, actor, write=write)
    kb = await services.spec_knowledge.get_knowledge(knowledge_id)
    if kb is None or kb.spec_id != spec.id:
        raise EntityNotFoundError("spec_knowledge", knowledge_id)
    return kb


async def _require_actor_board_spec_qa(
    scope: PulseUnitOfWork | ApplicationServiceCatalog,
    qa_id: str,
    actor: ActorContext,
    *,
    spec_id: str | None = None,
    write: bool = False,
) -> Any:
    """Resolve path Spec before reading a globally-addressable Q&A child."""
    if spec_id is None:
        raise EntityNotFoundError("spec_qa", qa_id)
    services = scope.services if hasattr(scope, "services") else scope
    await _require_actor_board_spec(scope, spec_id, actor, write=write)
    qa = await services.spec_qa.get_question(qa_id)
    if qa is None or qa.spec_id != spec_id:
        raise EntityNotFoundError("spec_qa", qa_id)
    return qa


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
    """List a board-scoped spec's knowledge items without content."""

    async def execute(
        self,
        command: ListSpecKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSpecKnowledgeResult:
        await _require_actor_board_spec(uow, command.spec_id, actor)
        items = await uow.services.spec_knowledge.list_knowledge(command.spec_id)
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
        self,
        command: GetSpecKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetSpecKnowledgeResult:
        kb = await _require_actor_board_spec_knowledge(
            uow,
            command.spec_id,
            command.knowledge_id,
            actor,
        )
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
        self,
        command: CreateSpecKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CreateSpecKnowledgeResult:
        await _require_actor_board_spec(uow, command.spec_id, actor, write=True)
        kb = await uow.services.spec_knowledge.create_knowledge(
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
        self,
        command: DeleteSpecKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeleteSpecKnowledgeResult:
        service = uow.services.spec_knowledge
        await _require_actor_board_spec_knowledge(
            uow,
            command.spec_id,
            command.knowledge_id,
            actor,
            write=True,
        )
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
    """List a board-scoped spec's Q&A items."""

    async def execute(
        self, command: ListSpecQACommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListSpecQAResult:

        await _require_actor_board_spec(uow, command.spec_id, actor)
        items = await uow.services.spec_qa.list_qa(command.spec_id)
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
        self,
        command: CreateSpecQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CreateSpecQuestionResult:
        await _require_actor_board_spec(uow, command.spec_id, actor, write=True)
        qa = await uow.services.spec_qa.create_question(
            command.spec_id, actor.actor_id, command.data
        )
        if not qa:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return CreateSpecQuestionResult(qa)


# --- spec Q&A: answer question ----------------------------------------------


class AnswerSpecQuestionCommand:
    __slots__ = ("qa_id", "data", "spec_id")

    def __init__(self, qa_id: str, data: Any, *, spec_id: str | None = None) -> None:
        self.qa_id = qa_id
        self.data = data
        self.spec_id = spec_id


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
        self,
        command: AnswerSpecQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AnswerSpecQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        service = uow.services.spec_qa
        await _require_actor_board_spec_qa(
            uow,
            command.qa_id,
            actor,
            spec_id=command.spec_id,
            write=True,
        )
        try:
            qa = await service.answer_question(
                command.qa_id,
                actor.actor_id,
                command.data,
                actor_type="user",
                surface="rest",
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
    __slots__ = ("qa_id", "spec_id")

    def __init__(self, qa_id: str, *, spec_id: str | None = None) -> None:
        self.qa_id = qa_id
        self.spec_id = spec_id


class DeleteSpecQuestionResult:
    __slots__ = ()


class DeleteSpecQuestionUseCase:
    """Delete a spec Q&A item (write). ``delete_question`` returns ``False`` when
    the item is missing → ``EntityNotFoundError("spec_qa")`` (404 "Q&A item not
    found"); commits after the delete."""

    async def execute(
        self,
        command: DeleteSpecQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeleteSpecQuestionResult:
        await _require_actor_board_spec_qa(
            uow,
            command.qa_id,
            actor,
            spec_id=command.spec_id,
            write=True,
        )
        deleted = await uow.services.spec_qa.delete_question(command.qa_id)
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
        self,
        command: SubmitSpecEvaluationCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SubmitSpecEvaluationResult:
        from okto_pulse.core.services.gate_contracts import (
            spec_evaluation_success_envelope,
        )

        service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )

        try:
            evaluator_name = await uow.services.resolve_actor_name(
                actor.actor_id,
                spec.board_id,
            )
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
        self,
        command: ListSpecEvaluationsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListSpecEvaluationsResult:
        try:
            await _require_actor_board_spec(uow, command.spec_id, actor)
        except EntityNotFoundError as exc:
            raise ValueError("Spec not found") from exc
        result = await uow.services.specs.list_spec_evaluations(command.spec_id)
        return ListSpecEvaluationsResult(result)
