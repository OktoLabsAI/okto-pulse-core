"""Architecture parent list/create use cases (SaaS Refactor spec R01A FU5-S1A).

Transport-free reimplementations of the eight ``api/architecture.py`` parent
list/create endpoints (ideation / refinement / spec / card × list / create) that
the legacy code drove off the request session through the shared
``_list_architecture`` / ``_create_architecture`` helpers. Each use case wraps the
EXISTING ``ArchitectureDesignRepository`` (the ``select``/ORM lives there) plus the
existing ``IdeationService`` / ``RefinementService`` / ``SpecService`` /
``CardService`` ``get_*`` readers for the parent-existence and spec-lock gates —
this layer never touches ``select``/``AsyncSession``/ORM directly (the relational
ratchet gate).

The legacy mechanism is preserved EXACTLY:

* ``_ensure_parent`` (a bare ``db.get`` PK lookup → 404 ``"{parent_type} not
  found"``) becomes a service ``get_*`` existence check raising
  ``EntityNotFoundError(parent_type, …)``; the adapter maps it to the same
  ``"{parent_type} not found"`` detail.
* For ``create`` on a spec parent the spec-architecture lock gate
  (``_ensure_spec_architecture_unlocked``) is replicated against the same fetched
  spec — a passed validation raises ``ConflictError`` the adapter maps to the
  legacy 409 locked detail; a missing spec is the same ``EntityNotFoundError``.
* ``repo.create``'s ``ValueError`` family (``CardArchitectureReadOnlyError`` →
  409, ``ArchitectureWarningAcknowledgementRequired`` → 409,
  ``ArchitecturePropagationBlocked`` → 422, ``ArchitecturePayloadValidationError``
  → 422, generic "not found" → 404) propagates UNCAUGHT so the adapter maps it
  through the existing ``_http_error_from_value`` helper — exactly as the legacy
  endpoint did.

Reads (list) do NOT commit; writes (create) ``commit(uow)`` after the repository
mutation, then return the projected ``to_response`` — exactly as the legacy
``_create_architecture`` (project then commit).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    ConflictError,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.models import ArchitectureDesignCreate
from okto_pulse.core.services.application_schemas import (
    ArchitectureDesignUpdate,
    ArchitectureDiagramPayloadResponse,
)
from okto_pulse.core.services.architecture import (
    stable_architecture_finding_key,
)
from okto_pulse.core.domain.human_validation_cycle import require_draft_mutation


def _require_architecture_parent_draft(parent: Any, parent_type: str) -> None:
    """Architecture is editable only inside a lifecycle subject's Draft."""

    if parent_type in {"ideation", "refinement", "spec"}:
        require_draft_mutation(parent, subject_type=parent_type)


async def _resolve_parent(
    services: ApplicationServiceCatalog,
    parent_type: str,
    parent_id: str,
) -> Any:
    """Fetch the architecture parent via the existing service ``get_*`` reader,
    raising ``EntityNotFoundError(parent_type, …)`` when it is missing — the
    transport-free equivalent of the legacy ``_ensure_parent`` (a bare
    ``db.get(model, id)`` → 404 ``"{parent_type} not found"``). Returns the parent
    so the spec-lock gate can reuse it without a second fetch."""
    if parent_type == "ideation":
        parent = await services.ideations.get_ideation(parent_id)
    elif parent_type == "refinement":
        parent = await services.refinements.get_refinement(parent_id)
    elif parent_type == "spec":
        parent = await services.specs.get_spec(parent_id)
    elif parent_type == "card":
        parent = await services.cards.get_card(parent_id)
    else:  # pragma: no cover - parent_type is a fixed literal per endpoint
        raise ValueError(f"unsupported architecture parent type: {parent_type}")
    if parent is None:
        raise EntityNotFoundError(parent_type, parent_id)
    return parent


async def _require_board_access(
    uow: PulseUnitOfWork,
    actor: ActorContext,
    board_id: str | None,
    *,
    entity_type: str,
    entity_id: str,
    expected_board_id: str | None = None,
) -> None:
    """Fail closed when an architecture record is outside the actor's board.

    MCP actors arrive after credential-to-board resolution and therefore carry a
    trusted ``actor.board_id``. REST actors do not, so their owner/share access is
    resolved through :func:`load_accessible_board`. Missing boards and denied
    boards deliberately raise the same entity-scoped not-found error.
    """

    if not board_id or (expected_board_id and board_id != expected_board_id):
        raise EntityNotFoundError(entity_type, entity_id)
    if actor.board_id is not None:
        if actor.board_id != board_id:
            raise EntityNotFoundError(entity_type, entity_id)
        return
    if await load_accessible_board(uow, board_id, actor) is None:
        raise EntityNotFoundError(entity_type, entity_id)


async def _resolve_accessible_parent(
    uow: PulseUnitOfWork,
    actor: ActorContext,
    parent_type: str,
    parent_id: str,
    *,
    board_id: str | None = None,
) -> Any:
    """Resolve a parent envelope, then authorize its board before child access."""

    parent = await _resolve_parent(uow.services, parent_type, parent_id)
    await _require_board_access(
        uow,
        actor,
        getattr(parent, "board_id", None),
        entity_type=parent_type,
        entity_id=parent_id,
        expected_board_id=board_id,
    )
    return parent


async def _resolve_accessible_design(
    uow: PulseUnitOfWork,
    actor: ActorContext,
    design_id: str,
    *,
    board_id: str | None = None,
    include_payloads: bool = False,
) -> Any:
    """Authorize a design envelope before parent, payload, or version reads.

    The first repository read is intentionally metadata-only. After board access
    succeeds, the declared parent is resolved and required to belong to the same
    board. Payload hydration, when requested, happens only after both checks.
    """

    repo = uow.services.architecture_designs
    design = await repo.get(design_id)
    if design is None:
        raise EntityNotFoundError("Architecture design", design_id)

    await _require_board_access(
        uow,
        actor,
        getattr(design, "board_id", None),
        entity_type="Architecture design",
        entity_id=design_id,
        expected_board_id=board_id,
    )
    try:
        parent_id = repo.parent_id_for(design)
        parent = await _resolve_parent(uow.services, design.parent_type, parent_id)
    except (AttributeError, EntityNotFoundError, ValueError) as exc:
        raise EntityNotFoundError("Architecture design", design_id) from exc
    if getattr(parent, "board_id", None) != design.board_id:
        raise EntityNotFoundError("Architecture design", design_id)

    if not include_payloads:
        return design

    loaded = await repo.get(design_id, include_payloads=True)
    try:
        loaded_parent_id = repo.parent_id_for(loaded) if loaded is not None else None
    except (AttributeError, ValueError) as exc:
        raise EntityNotFoundError("Architecture design", design_id) from exc
    if (
        loaded is None
        or loaded.board_id != design.board_id
        or loaded.parent_type != design.parent_type
        or loaded_parent_id != parent_id
    ):
        raise EntityNotFoundError("Architecture design", design_id)
    return loaded


def _spec_architecture_locked(spec: Any) -> bool:
    """Replicate the legacy ``_ensure_spec_architecture_unlocked`` predicate: a spec
    whose current validation outcome is ``success`` is locked for architecture
    edits."""
    current_id = getattr(spec, "current_validation_id", None)
    validations = getattr(spec, "validations", None) or []
    current = next((item for item in validations if item.get("id") == current_id), None)
    return bool(current_id and current and current.get("outcome") == "success")


class SpecArchitectureLockedError(ConflictError):
    """Architecture mutation rejected by a spec's successful validation."""

    def __init__(self, spec: Any) -> None:
        super().__init__("spec_architecture_locked", str(spec.id))
        self.current_validation_id = getattr(spec, "current_validation_id", None)


# --- list -------------------------------------------------------------------


class ListArchitectureCommand:
    __slots__ = ("parent_type", "parent_id", "include_payloads", "board_id")

    def __init__(
        self,
        parent_type: str,
        parent_id: str,
        include_payloads: bool = False,
        board_id: str | None = None,
    ) -> None:
        self.parent_type = parent_type
        self.parent_id = parent_id
        self.include_payloads = include_payloads
        self.board_id = board_id


class ListArchitectureResult:
    __slots__ = ("summaries",)

    def __init__(self, summaries: list[Any]) -> None:
        self.summaries = summaries


class ListArchitectureUseCase:
    """List the Architecture Designs of a parent (read, no commit). Validates the
    parent exists (``EntityNotFoundError`` → adapter 404 ``"{parent_type} not
    found"``) then delegates to ``ArchitectureDesignRepository.list`` +
    ``to_summary`` — the ``select`` and the projection stay in the repository."""

    async def execute(
        self, command: ListArchitectureCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListArchitectureResult:
        parent = await _resolve_accessible_parent(
            uow,
            actor,
            command.parent_type,
            command.parent_id,
            board_id=command.board_id,
        )
        repo = uow.services.architecture_designs
        designs = await repo.list(
            command.parent_type,
            command.parent_id,
            include_payloads=False,
        )
        designs = [
            design
            for design in designs
            if getattr(design, "board_id", None) == parent.board_id
        ]
        if command.include_payloads:
            hydrated: list[Any] = []
            for design in designs:
                scoped = await _resolve_accessible_design(
                    uow,
                    actor,
                    design.id,
                    board_id=parent.board_id,
                    include_payloads=True,
                )
                hydrated.append(repo.to_response(scoped))
            return ListArchitectureResult(hydrated)
        return ListArchitectureResult([repo.to_summary(design) for design in designs])


# --- create -----------------------------------------------------------------


class CreateArchitectureCommand:
    __slots__ = ("parent_type", "parent_id", "data", "board_id")

    def __init__(
        self,
        parent_type: str,
        parent_id: str,
        data: Any,
        board_id: str | None = None,
    ) -> None:
        self.parent_type = parent_type
        self.parent_id = parent_id
        self.data = data
        self.board_id = board_id


class CreateArchitectureResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class CreateArchitectureUseCase:
    """Create an Architecture Design under a parent (write). Validates the parent
    exists (``EntityNotFoundError`` → adapter 404 ``"{parent_type} not found"``);
    for a spec parent enforces the architecture lock gate against the same fetched
    spec (``ConflictError`` → adapter 409 locked detail). Delegates persistence to
    ``ArchitectureDesignRepository.create`` whose ``ValueError`` family propagates
    UNCAUGHT for the adapter's ``_http_error_from_value`` mapping. Projects via
    ``to_response`` then ``commit(uow)`` — the legacy project-then-commit order."""

    async def execute(
        self, command: CreateArchitectureCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateArchitectureResult:
        parent = await _resolve_accessible_parent(
            uow,
            actor,
            command.parent_type,
            command.parent_id,
            board_id=command.board_id,
        )
        _require_architecture_parent_draft(parent, command.parent_type)
        if command.parent_type == "spec" and _spec_architecture_locked(parent):
            raise SpecArchitectureLockedError(parent)
        repo = uow.services.architecture_designs
        design = await repo.create(
            command.parent_type, command.parent_id, command.data, actor.actor_id
        )
        response = repo.to_response(design)
        await commit(uow)
        return CreateArchitectureResult(response)


# === FU5-S1B: design CRUD + validation + propagation report ==================
#
# Transport-free reimplementations of five further ``api/architecture.py``
# endpoints that drove off the request session: ``get`` / ``update`` / ``delete``
# of a single Architecture Design, the dry-run payload ``validate``, and the
# read-only ``propagation-legacy-report``. The legacy mechanism is preserved
# EXACTLY — the ``select``/``db.get``/ORM lives in ``ArchitectureDesignRepository``
# (get/update/delete/critique/to_response) and in
# ``build_propagation_legacy_report``; this layer only reproduces the
# lookup → gate → mutate → commit envelope.


async def _resolve_mutable_design(
    uow: PulseUnitOfWork,
    actor: ActorContext,
    design_id: str,
    board_id: str | None = None,
) -> Any:
    """Transport-free twin of the legacy ``_ensure_design_mutable`` gate.

    Loads the Architecture Design via ``ArchitectureDesignRepository.get`` (the
    ``db.get`` stays in the repository): a missing design raises
    ``EntityNotFoundError("Architecture design", …)`` (adapter → 404 "Architecture
    design not found"). A ``card`` parent is read-only →
    ``ConflictError("card_architecture_readonly", …)`` (adapter → 409
    ``CARD_ARCHITECTURE_READ_ONLY_MESSAGE``). A ``spec`` parent enforces the same
    architecture lock as create against the spec fetched through the existing
    ``SpecService.get_spec`` reader: a missing spec raises
    ``EntityNotFoundError("Spec", …)`` (adapter → 404 "Spec not found"); a locked
    spec raises ``ConflictError("spec_architecture_locked", …)`` (adapter → 409
    locked detail). Returns the design so callers reuse it without a re-fetch."""
    design = await _resolve_accessible_design(
        uow,
        actor,
        design_id,
        board_id=board_id,
    )
    if design.parent_type == "card":
        raise ConflictError("card_architecture_readonly", design_id)
    parent_id = uow.services.architecture_designs.parent_id_for(design)
    parent = await _resolve_parent(uow.services, design.parent_type, parent_id)
    _require_architecture_parent_draft(parent, design.parent_type)
    if design.parent_type == "spec" and _spec_architecture_locked(parent):
        raise SpecArchitectureLockedError(parent)
    return design


# --- get --------------------------------------------------------------------


class GetArchitectureDesignCommand:
    __slots__ = ("design_id", "include_payloads", "board_id")

    def __init__(
        self,
        design_id: str,
        include_payloads: bool = False,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.include_payloads = include_payloads
        self.board_id = board_id


class GetArchitectureDesignResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class GetArchitectureDesignUseCase:
    """Fetch a single Architecture Design (read, no commit). Delegates to
    ``ArchitectureDesignRepository.get`` (the ``db.get`` + payload hydration stay
    there); a missing design raises ``EntityNotFoundError("Architecture design",
    …)`` (adapter → 404 "Architecture design not found"). Projects via
    ``to_response`` — exactly as the legacy endpoint did."""

    async def execute(
        self, command: GetArchitectureDesignCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetArchitectureDesignResult:
        repo = uow.services.architecture_designs
        design = await _resolve_accessible_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
            include_payloads=command.include_payloads,
        )
        return GetArchitectureDesignResult(repo.to_response(design))


# --- update -----------------------------------------------------------------


class UpdateArchitectureDesignCommand:
    __slots__ = ("design_id", "data", "board_id")

    def __init__(
        self,
        design_id: str,
        data: Any,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.data = data
        self.board_id = board_id


class UpdateArchitectureDesignResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class UpdateArchitectureDesignUseCase:
    """Update an Architecture Design (write). Applies the mutability gate
    (``_resolve_mutable_design`` → ``EntityNotFoundError`` / ``ConflictError`` the
    adapter maps to the legacy 404/409), then delegates persistence to
    ``ArchitectureDesignRepository.update`` whose ``ValueError`` family
    (acknowledgement-required / propagation-blocked / payload-invalid / not-found)
    propagates UNCAUGHT for the adapter's ``_http_error_from_value`` mapping.
    Projects via ``to_response`` then ``commit(uow)`` — the legacy
    project-then-commit order."""

    async def execute(
        self, command: UpdateArchitectureDesignCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateArchitectureDesignResult:
        await _resolve_mutable_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        repo = uow.services.architecture_designs
        design = await repo.update(command.design_id, command.data, actor.actor_id)
        response = repo.to_response(design)
        await commit(uow)
        return UpdateArchitectureDesignResult(response)


# --- delete -----------------------------------------------------------------


class DeleteArchitectureDesignCommand:
    __slots__ = ("design_id", "board_id")

    def __init__(self, design_id: str, board_id: str | None = None) -> None:
        self.design_id = design_id
        self.board_id = board_id


class DeleteArchitectureDesignResult:
    __slots__ = ()


class DeleteArchitectureDesignUseCase:
    """Delete an Architecture Design (write). Applies the mutability gate
    (``_resolve_mutable_design`` → ``EntityNotFoundError`` / ``ConflictError``),
    then delegates to ``ArchitectureDesignRepository.delete``; a ``False`` return
    (already gone) becomes ``EntityNotFoundError("Architecture design", …)``
    (adapter → 404 "Architecture design not found"). Commits after the delete —
    exactly as the legacy endpoint did."""

    async def execute(
        self, command: DeleteArchitectureDesignCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteArchitectureDesignResult:
        await _resolve_mutable_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        repo = uow.services.architecture_designs
        deleted = await repo.delete(command.design_id, actor.actor_id)
        if not deleted:
            raise EntityNotFoundError("Architecture design", command.design_id)
        await commit(uow)
        return DeleteArchitectureDesignResult()


# --- validate payload (dry-run, no persistence) -----------------------------


class ValidateArchitecturePayloadCommand:
    __slots__ = ("payload", "design_id")

    def __init__(self, payload: dict[str, Any], design_id: str | None = None) -> None:
        self.payload = payload
        self.design_id = design_id


class ValidateArchitecturePayloadResult:
    __slots__ = ("critique",)

    def __init__(self, critique: dict[str, Any]) -> None:
        self.critique = critique


class ValidateArchitecturePayloadUseCase:
    """Critique an Architecture Design payload without persisting it (read, no
    commit). Delegates to ``ArchitectureDesignRepository.critique_payload`` (the
    semantic validation stays in the repository); when a ``design_id`` is supplied
    the structured warnings are enriched with the stable finding key via
    ``stable_architecture_finding_key`` — exactly as the legacy endpoint did. The
    transport-side ``model_dump``/``design_id`` pop stays in the adapter."""

    async def execute(
        self,
        command: ValidateArchitecturePayloadCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ValidateArchitecturePayloadResult:
        repo = uow.services.architecture_designs
        critique = repo.critique_payload(command.payload)
        if command.design_id:
            critique["structured_warnings"] = [
                {
                    **warning,
                    "finding_key": stable_architecture_finding_key(
                        command.design_id, warning
                    ),
                }
                for warning in (critique.get("structured_warnings") or [])
            ]
        return ValidateArchitecturePayloadResult(critique)


class McpValidateArchitecturePayloadCommand:
    __slots__ = (
        "board_id",
        "parent_type",
        "parent_id",
        "design_id",
        "title",
        "global_description",
        "parsed_fields",
        "architecture_warning_acknowledgement",
        "commit_requested",
        "include_design",
    )

    def __init__(
        self,
        *,
        board_id: str,
        parent_type: str,
        parent_id: str,
        design_id: str,
        title: str,
        global_description: str,
        parsed_fields: dict[str, Any],
        architecture_warning_acknowledgement: Any,
        commit_requested: bool,
        include_design: bool,
    ) -> None:
        self.board_id = board_id
        self.parent_type = parent_type
        self.parent_id = parent_id
        self.design_id = design_id
        self.title = title
        self.global_description = global_description
        self.parsed_fields = parsed_fields
        self.architecture_warning_acknowledgement = architecture_warning_acknowledgement
        self.commit_requested = commit_requested
        self.include_design = include_design


class McpValidateArchitecturePayloadResult:
    __slots__ = ("payload", "parent_type")

    def __init__(self, payload: dict[str, Any], parent_type: str) -> None:
        self.payload = payload
        self.parent_type = parent_type


class McpValidateArchitecturePayloadUseCase:
    """MCP architecture payload validation, including the legacy commit=true path.

    The wrapper owns permission checks and JSON parsing. This use case owns the
    parent/design lookup, candidate merge, critique, optional create/update and
    commit through the MCP UnitOfWork.
    """

    async def execute(
        self,
        command: McpValidateArchitecturePayloadCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpValidateArchitecturePayloadResult:
        repo = uow.services.architecture_designs
        mode = "update" if command.design_id else "create"

        if command.design_id:
            if command.commit_requested:
                design = await _resolve_mutable_design(
                    uow,
                    actor,
                    command.design_id,
                    board_id=command.board_id,
                )
            else:
                design = await _resolve_accessible_design(
                    uow,
                    actor,
                    command.design_id,
                    board_id=command.board_id,
                )
            loaded = await _resolve_accessible_design(
                uow,
                actor,
                command.design_id,
                board_id=command.board_id,
                include_payloads=True,
            )
            candidate = {
                "title": command.title or loaded.title,
                "global_description": command.global_description or loaded.global_description,
                "entities": loaded.entities or [],
                "interfaces": loaded.interfaces or [],
                "diagrams": loaded.diagrams or [],
            }
            candidate.update(command.parsed_fields)
            parent_type = design.parent_type
        else:
            if not command.parent_type or not command.parent_id:
                raise ValueError(
                    "parent_type and parent_id are required when design_id is omitted"
                )
            parent = await _resolve_accessible_parent(
                uow,
                actor,
                command.parent_type,
                command.parent_id,
                board_id=command.board_id,
            )
            if command.commit_requested:
                _require_architecture_parent_draft(parent, command.parent_type)
            if (
                command.commit_requested
                and command.parent_type == "spec"
                and _spec_architecture_locked(parent)
            ):
                raise SpecArchitectureLockedError(parent)
            candidate = {
                "title": command.title,
                "global_description": command.global_description,
                "entities": command.parsed_fields.get("entities", []),
                "interfaces": command.parsed_fields.get("interfaces", []),
                "diagrams": command.parsed_fields.get("diagrams", []),
            }
            parent_type = command.parent_type

        critique = repo.critique_payload(candidate)
        if not command.commit_requested or not critique.get("valid"):
            await commit(uow)
            return McpValidateArchitecturePayloadResult(
                {"success": True, "mode": mode, **critique}, parent_type
            )

        if mode == "create":
            design = await repo.create(
                command.parent_type,
                command.parent_id,
                ArchitectureDesignCreate(
                    title=candidate["title"],
                    global_description=candidate["global_description"],
                    entities=candidate.get("entities") or [],
                    interfaces=candidate.get("interfaces") or [],
                    diagrams=candidate.get("diagrams") or [],
                    architecture_warning_acknowledgement=(
                        command.architecture_warning_acknowledgement
                    ),
                ),
                actor.actor_id,
            )
        else:
            patch_payload = ArchitectureDesignUpdate(
                **{
                    key: candidate[key]
                    for key in (
                        "title",
                        "global_description",
                        "entities",
                        "interfaces",
                        "diagrams",
                    )
                    if candidate.get(key) is not None
                }
            )
            patch_payload.architecture_warning_acknowledgement = (
                command.architecture_warning_acknowledgement
            )
            design = await repo.update(command.design_id, patch_payload, actor.actor_id)
        await commit(uow)

        warnings = list(critique.get("warnings") or [])
        structured_warnings = list(critique.get("structured_warnings") or [])
        suppressed_warnings = list(critique.get("suppressed_warnings") or [])
        envelope: dict[str, Any] = {
            "success": True,
            "mode": mode,
            "committed": True,
            "id": design.id,
            "version": design.version,
            "warnings_count": len(warnings),
            "structured_warnings_count": len(structured_warnings),
            "suppressed_warnings_count": len(suppressed_warnings),
            "normalized": bool(warnings),
        }
        if command.include_design:
            response = repo.to_response(design)
            envelope["architecture_design"] = (
                response.model_dump(mode="json")
                if hasattr(response, "model_dump")
                else response
            )
        return McpValidateArchitecturePayloadResult(envelope, parent_type)


# --- propagation legacy report (read-only diagnostic) -----------------------


class ArchitecturePropagationLegacyReportCommand:
    __slots__ = (
        "board_id",
        "limit",
        "offset",
        "include_clean",
        "parent_type_filter",
        "surface",
    )

    def __init__(
        self,
        board_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        include_clean: bool = False,
        parent_type_filter: str = "",
        surface: str = "rest",
    ) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset
        self.include_clean = include_clean
        self.parent_type_filter = parent_type_filter
        self.surface = surface


class ArchitecturePropagationLegacyReportResult:
    __slots__ = ("report",)

    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report


class ArchitecturePropagationLegacyReportUseCase:
    """Build the bounded, read-only legacy propagation diagnostic (read, no
    commit). Delegates wholesale to ``build_propagation_legacy_report`` (the scan +
    eligibility classification + observability stay in the service), forwarding
    ``surface="rest"`` and normalizing ``parent_type_filter`` to ``None`` exactly as
    the legacy endpoint did. Never mutates anything."""

    async def execute(
        self,
        command: ArchitecturePropagationLegacyReportCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ArchitecturePropagationLegacyReportResult:
        await _require_board_access(
            uow,
            actor,
            command.board_id,
            entity_type="Board",
            entity_id=command.board_id,
            expected_board_id=command.board_id,
        )
        report = await uow.services.build_propagation_legacy_report(
            board_id=command.board_id,
            limit=command.limit,
            offset=command.offset,
            include_clean=command.include_clean,
            parent_type_filter=command.parent_type_filter or None,
            surface=command.surface,
        )
        return ArchitecturePropagationLegacyReportResult(report)


# === FU5-S1C: diagram payload get/update + excalidraw import + diff + copy ====
#
# Transport-free reimplementations of the final five ``api/architecture.py``
# endpoints that drove off the request session: the per-diagram payload ``get``
# and ``put``, the Excalidraw ``import``, the version ``diff``, and the
# spec→card ``copy``. The legacy mechanism is preserved EXACTLY — the
# ``select``/``db.get``/ORM lives in ``ArchitectureDesignRepository`` (get /
# update / to_response / diff), in ``ArchitectureDiagramStore`` (load_payload /
# stat), and in ``ArchitecturePropagationService.copy_effective_spec_to_card``;
# this layer only reproduces the lookup → gate → mutate → commit envelope plus
# the pure-Python diagram-list editing the legacy endpoints did inline.


# --- diagram payload get (read, no commit) ----------------------------------


class GetArchitectureDiagramPayloadCommand:
    __slots__ = ("design_id", "diagram_id", "board_id")

    def __init__(
        self,
        design_id: str,
        diagram_id: str,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.diagram_id = diagram_id
        self.board_id = board_id


class GetArchitectureDiagramPayloadResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class GetArchitectureDiagramPayloadUseCase:
    """Load a single diagram's externalized payload (read, no commit). Fetches the
    design through ``ArchitectureDesignRepository.get`` (the ``db.get`` stays in the
    repository): a missing design raises ``EntityNotFoundError("Architecture
    design", …)`` (adapter → 404 "Architecture design not found"). A missing
    diagram, a diagram without an ``adapter_payload_ref``, or a payload the
    ``ArchitectureDiagramStore`` cannot resolve (``KeyError``) all raise
    ``EntityNotFoundError("Diagram payload", …)`` (adapter → 404 "Diagram payload
    not found") — exactly the two distinct 404 details the legacy endpoint
    produced. Projects the legacy ``ArchitectureDiagramPayloadResponse`` from the
    store's ``load_payload`` + ``stat``."""

    async def execute(
        self,
        command: GetArchitectureDiagramPayloadCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetArchitectureDiagramPayloadResult:
        design = await _resolve_accessible_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        diagram = next(
            (
                item
                for item in design.diagrams or []
                if item.get("id") == command.diagram_id
            ),
            None,
        )
        if not diagram or not diagram.get("adapter_payload_ref"):
            raise EntityNotFoundError("Diagram payload", command.diagram_id)
        store = uow.services.architecture_diagrams
        try:
            payload = await store.load_payload(diagram["adapter_payload_ref"])
            stat_info = await store.stat(diagram["adapter_payload_ref"])
        except KeyError as exc:
            raise EntityNotFoundError("Diagram payload", command.diagram_id) from exc
        return GetArchitectureDiagramPayloadResult(
            ArchitectureDiagramPayloadResponse(
                design_id=command.design_id,
                diagram_id=command.diagram_id,
                format=stat_info["format"],
                content_hash=stat_info["content_hash"],
                size_bytes=stat_info["size_bytes"],
                payload=payload,
            )
        )


# --- diagram payload update (write) -----------------------------------------


class UpdateArchitectureDiagramPayloadCommand:
    __slots__ = (
        "design_id",
        "diagram_id",
        "format",
        "payload",
        "change_summary",
        "architecture_warning_acknowledgement",
        "board_id",
    )

    def __init__(
        self,
        design_id: str,
        diagram_id: str,
        format: Any,
        payload: Any,
        change_summary: str | None,
        architecture_warning_acknowledgement: Any,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.diagram_id = diagram_id
        self.format = format
        self.payload = payload
        self.change_summary = change_summary
        self.architecture_warning_acknowledgement = architecture_warning_acknowledgement
        self.board_id = board_id


class UpdateArchitectureDiagramPayloadResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class UpdateArchitectureDiagramPayloadUseCase:
    """Replace one diagram's payload (write). Applies the mutability gate
    (``_resolve_mutable_design`` → ``EntityNotFoundError`` / ``ConflictError`` the
    adapter maps to the legacy 404/409); a missing diagram raises
    ``EntityNotFoundError("Diagram", …)`` (adapter → 404 "Diagram not found"). The
    pure-Python diagram-list editing (clone, set ``format``/``adapter_payload``)
    matches the legacy endpoint exactly, then delegates persistence to
    ``ArchitectureDesignRepository.update`` whose ``ValueError`` family propagates
    UNCAUGHT for the adapter's ``_http_error_from_value`` mapping. Projects via
    ``to_response`` then ``commit(uow)`` — the legacy project-then-commit order."""

    async def execute(
        self,
        command: UpdateArchitectureDiagramPayloadCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> UpdateArchitectureDiagramPayloadResult:
        design = await _resolve_mutable_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        diagrams = [dict(item) for item in design.diagrams or []]
        target = next(
            (item for item in diagrams if item.get("id") == command.diagram_id), None
        )
        if target is None:
            raise EntityNotFoundError("Diagram", command.diagram_id)
        target["format"] = command.format or target.get("format") or "raw"
        target["adapter_payload"] = command.payload
        repo = uow.services.architecture_designs
        updated = await repo.update(
            command.design_id,
            ArchitectureDesignUpdate(
                diagrams=diagrams,
                change_summary=command.change_summary
                or f"Updated diagram payload {command.diagram_id}",
                architecture_warning_acknowledgement=command.architecture_warning_acknowledgement,
            ),
            actor.actor_id,
        )
        response = repo.to_response(updated)
        await commit(uow)
        return UpdateArchitectureDiagramPayloadResult(response)


# --- excalidraw import (write) ----------------------------------------------


class ImportExcalidrawArchitectureDiagramCommand:
    __slots__ = (
        "design_id",
        "title",
        "payload",
        "diagram_type",
        "description",
        "order_index",
        "replace_diagram_id",
        "change_summary",
        "architecture_warning_acknowledgement",
        "board_id",
    )

    def __init__(
        self,
        design_id: str,
        title: str,
        payload: Any,
        diagram_type: Any,
        description: str | None,
        order_index: int,
        replace_diagram_id: str | None,
        change_summary: str | None,
        architecture_warning_acknowledgement: Any,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.title = title
        self.payload = payload
        self.diagram_type = diagram_type
        self.description = description
        self.order_index = order_index
        self.replace_diagram_id = replace_diagram_id
        self.change_summary = change_summary
        self.architecture_warning_acknowledgement = architecture_warning_acknowledgement
        self.board_id = board_id


class ImportExcalidrawArchitectureDiagramResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class ImportExcalidrawArchitectureDiagramUseCase:
    """Import an Excalidraw scene as a new diagram, or replace an existing one
    (write). Applies the mutability gate (``_resolve_mutable_design`` →
    ``EntityNotFoundError`` / ``ConflictError``); when ``replace_diagram_id`` is set
    but absent from the design a missing diagram raises
    ``EntityNotFoundError("Diagram", …)`` (adapter → 404 "Diagram not found"). The
    pure-Python build/replace/append of the diagram list matches the legacy
    endpoint exactly, then delegates persistence to
    ``ArchitectureDesignRepository.update`` whose ``ValueError`` family propagates
    UNCAUGHT for the adapter's ``_http_error_from_value`` mapping. Projects via
    ``to_response`` then ``commit(uow)`` — the legacy project-then-commit order."""

    async def execute(
        self,
        command: ImportExcalidrawArchitectureDiagramCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ImportExcalidrawArchitectureDiagramResult:
        design = await _resolve_mutable_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        diagrams = [dict(item) for item in design.diagrams or []]
        imported = {
            "id": command.replace_diagram_id or None,
            "title": command.title,
            "diagram_type": command.diagram_type,
            "format": "excalidraw_json",
            "description": command.description,
            "order_index": command.order_index,
            "adapter_payload": command.payload,
        }
        if command.replace_diagram_id:
            index = next(
                (
                    idx
                    for idx, item in enumerate(diagrams)
                    if item.get("id") == command.replace_diagram_id
                ),
                -1,
            )
            if index < 0:
                raise EntityNotFoundError("Diagram", command.replace_diagram_id)
            diagrams[index] = {
                **diagrams[index],
                **imported,
                "id": command.replace_diagram_id,
            }
        else:
            imported.pop("id")
            diagrams.append(imported)
        repo = uow.services.architecture_designs
        updated = await repo.update(
            command.design_id,
            ArchitectureDesignUpdate(
                diagrams=diagrams,
                change_summary=command.change_summary or "Imported Excalidraw diagram",
                architecture_warning_acknowledgement=command.architecture_warning_acknowledgement,
            ),
            actor.actor_id,
        )
        response = repo.to_response(updated)
        await commit(uow)
        return ImportExcalidrawArchitectureDiagramResult(response)


# --- version diff (read, no commit) -----------------------------------------


class GetArchitectureDiffCommand:
    __slots__ = ("design_id", "from_version", "to_version", "board_id")

    def __init__(
        self,
        design_id: str,
        from_version: int,
        to_version: int,
        board_id: str | None = None,
    ) -> None:
        self.design_id = design_id
        self.from_version = from_version
        self.to_version = to_version
        self.board_id = board_id


class GetArchitectureDiffResult:
    __slots__ = ("response",)

    def __init__(self, response: Any) -> None:
        self.response = response


class GetArchitectureDiffUseCase:
    """Diff two Architecture Design versions (read, no commit). Delegates wholesale
    to ``ArchitectureDesignRepository.diff`` (the ``select`` over the version table
    and the field-comparison stay in the repository); a missing version raises a
    ``ValueError("architecture design version not found")`` that propagates UNCAUGHT
    for the adapter's ``_http_error_from_value`` mapping (→ 404) — exactly as the
    legacy endpoint did. Never mutates anything."""

    async def execute(
        self, command: GetArchitectureDiffCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetArchitectureDiffResult:
        await _resolve_accessible_design(
            uow,
            actor,
            command.design_id,
            board_id=command.board_id,
        )
        repo = uow.services.architecture_designs
        diff = await repo.diff(
            command.design_id, command.from_version, command.to_version
        )
        return GetArchitectureDiffResult(diff)


# --- copy spec architecture to card (write) ---------------------------------


class CopyArchitectureFromSpecToCardCommand:
    __slots__ = (
        "card_id",
        "spec_id",
        "design_ids",
        "architecture_warning_acknowledgement",
        "board_id",
    )

    def __init__(
        self,
        card_id: str,
        spec_id: str,
        design_ids: list[str] | None,
        architecture_warning_acknowledgement: Any,
        board_id: str | None = None,
    ) -> None:
        self.card_id = card_id
        self.spec_id = spec_id
        self.design_ids = design_ids
        self.architecture_warning_acknowledgement = architecture_warning_acknowledgement
        self.board_id = board_id


class CopyArchitectureFromSpecToCardResult:
    __slots__ = ("responses",)

    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses


class CopyArchitectureFromSpecToCardUseCase:
    """Copy every effective spec Architecture obligation onto a card (write).
    Resolves the destination card through the existing ``CardService.get_card``
    reader (the transport-free twin of the legacy ``_ensure_parent(db, "card",
    …)``): a missing card raises ``EntityNotFoundError("card", …)`` (adapter → 404
    "card not found"), and its ``board_id`` feeds the copy exactly as the legacy
    endpoint did. Delegates wholesale to
    ``ArchitecturePropagationService.copy_effective_spec_to_card`` whose
    ``ResourceLineageResolutionError`` / ``ResourcePropagationError`` /
    ``ArchitecturePropagationBlocked`` / ``ValueError`` family propagates UNCAUGHT
    (raised before ``commit`` → no partial write) for the adapter's 422/structured
    mapping. Projects each copied design via ``to_response`` then ``commit(uow)`` —
    the legacy project-then-commit order."""

    async def execute(
        self,
        command: CopyArchitectureFromSpecToCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CopyArchitectureFromSpecToCardResult:
        card = await _resolve_accessible_parent(
            uow,
            actor,
            "card",
            command.card_id,
            board_id=command.board_id,
        )
        spec = await _resolve_accessible_parent(
            uow,
            actor,
            "spec",
            command.spec_id,
            board_id=command.board_id or card.board_id,
        )
        if spec.board_id != card.board_id or getattr(card, "spec_id", None) != spec.id:
            raise EntityNotFoundError("spec", command.spec_id)
        service = uow.services.architecture_propagation
        designs, _plan = await service.copy_effective_spec_to_card(
            board_id=command.board_id or card.board_id,
            spec_id=command.spec_id,
            card_id=command.card_id,
            actor_id=actor.actor_id,
            design_ids=command.design_ids,
            architecture_warning_acknowledgement=command.architecture_warning_acknowledgement,
        )
        repo = uow.services.architecture_designs
        responses = [repo.to_response(design) for design in designs]
        await commit(uow)
        return CopyArchitectureFromSpecToCardResult(responses)
