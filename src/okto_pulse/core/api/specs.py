"""Spec API endpoints."""

from collections.abc import Iterable
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.infra.permissions import (
    PermissionSet,
    check_permission,
    map_legacy_permissions,
    resolve_permissions,
)
from okto_pulse.core.models.db import Agent, PermissionPreset
from okto_pulse.core.models.schemas import (
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeResponse,
    SpecKnowledgeSummary,
    SpecMove,
    SpecResponse,
    SpecSummary,
    SpecUpdate,
)
from okto_pulse.core.models.schemas import SpecHistoryResponse, SpecQAAnswer, SpecQACreate, SpecQAResponse
from okto_pulse.core.services import (
    BoardService,
    QASelfAnsweringNotAllowedError,
    ResourceGateError,
    SpecKnowledgeService,
    SpecQAService,
    SpecService,
)
from okto_pulse.core.services.gate_contracts import (
    GateContractError,
    spec_evaluation_success_envelope,
)
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
    StructuredSpecEntityErrorCode,
    StructuredSpecEntityService,
)
from okto_pulse.core.services.test_scenario_lifecycle import StatusNotMutableError

router = APIRouter()


class ScenarioStatusUpdate(BaseModel):
    """Request body for the scoped test-scenario status endpoint."""

    status: str
    evidence: dict | None = None

STRUCTURED_SPEC_ENTITY_DEPRECATION_WARNING = (
    "Spec child entity edits should use /api/v1/specs/{spec_id}/structured-entities/"
    "{entity_type}; legacy whole-spec child list updates are compatibility-only."
)

_STRUCTURED_SPEC_ENTITY_UPDATE_FIELDS = {
    "functional_requirements",
    "acceptance_criteria",
    "technical_requirements",
    "business_rules",
    "api_contracts",
    "integration_requirements",
    "observability_requirements",
    "decisions",
}


class StructuredSpecEntityMutationRequest(BaseModel):
    operation: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    expected_spec_version: int | None = None
    task_id: str | None = None
    ack_token: str | None = None


def _structured_entity_status_code(error_code: str | None) -> int:
    return {
        StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED: status.HTTP_403_FORBIDDEN,
        StructuredSpecEntityErrorCode.VERSION_CONFLICT: status.HTTP_409_CONFLICT,
        StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED: status.HTTP_409_CONFLICT,
        StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID: status.HTTP_409_CONFLICT,
        StructuredSpecEntityErrorCode.ENTITY_NOT_FOUND: status.HTTP_404_NOT_FOUND,
        StructuredSpecEntityErrorCode.SPEC_NOT_FOUND: status.HTTP_404_NOT_FOUND,
        StructuredSpecEntityErrorCode.SPEC_LOCKED: status.HTTP_409_CONFLICT,
        StructuredSpecEntityErrorCode.UNSUPPORTED_ENTITY_TYPE: status.HTTP_422_UNPROCESSABLE_CONTENT,
        StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION: status.HTTP_422_UNPROCESSABLE_CONTENT,
        StructuredSpecEntityErrorCode.VALIDATION_FAILED: status.HTTP_422_UNPROCESSABLE_CONTENT,
        StructuredSpecEntityErrorCode.LINK_TARGET_INVALID: status.HTTP_422_UNPROCESSABLE_CONTENT,
    }.get(error_code, status.HTTP_400_BAD_REQUEST)


def _resource_gate_detail(exc: ResourceGateError) -> dict:
    return {
        "error": exc.code,
        "message": str(exc),
        "details": exc.details,
    }


def _raise_permission_denied(error: str) -> None:
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error)


async def _resolve_user_permissions(db: AsyncSession, user_id: str, board_id: str) -> PermissionSet:
    """Resolve the same best-effort permission model exposed by /me/permissions."""
    result = await db.execute(select(Agent).where(Agent.created_by == user_id).limit(1))
    agent = result.scalar_one_or_none()

    agent_flags: dict | None = None
    preset_flags: dict | None = None

    if agent is not None:
        if isinstance(agent.permission_flags, dict) and agent.permission_flags:
            agent_flags = agent.permission_flags
        elif isinstance(agent.permissions, list) and agent.permissions:
            agent_flags = map_legacy_permissions(agent.permissions)

        if agent.preset_id:
            preset = await db.get(PermissionPreset, agent.preset_id)
            if preset and preset.flags:
                preset_flags = preset.flags

    return resolve_permissions(agent_flags, preset_flags, None)


async def _require_permissions(
    db: AsyncSession,
    user_id: str,
    board_id: str,
    permissions: str | Iterable[str | None],
) -> None:
    permission_set = await _resolve_user_permissions(db, user_id, board_id)
    permission_names = [permissions] if isinstance(permissions, str) else list(permissions)
    for permission in permission_names:
        if not permission:
            continue
        error = check_permission(permission_set, permission)
        if error:
            _raise_permission_denied(error)


async def _run_structured_spec_entity_command(
    *,
    db: AsyncSession,
    user_id: str,
    spec_id: str,
    entity_type: str,
    operation: str,
    payload: dict[str, Any] | None = None,
    entity_id: str | None = None,
    expected_spec_version: int | None = None,
    task_id: str | None = None,
    ack_token: str | None = None,
    preview_only: bool = False,
) -> dict[str, Any]:
    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    permission_set = await _resolve_user_permissions(db, user_id, spec.board_id)
    service = StructuredSpecEntityService(db)
    result = await service.apply(
        StructuredSpecEntityCommand(
            board_id=spec.board_id,
            spec_id=spec_id,
            actor_id=user_id,
            entity_type=entity_type,
            entity_id=entity_id,
            operation=operation,
            payload=payload or {},
            expected_spec_version=expected_spec_version,
            task_id=task_id,
            ack_token=ack_token,
            preview_only=preview_only,
            permission_set=permission_set,
        )
    )
    body = result.as_dict()
    if not result.success:
        await db.rollback()
        raise HTTPException(
            status_code=_structured_entity_status_code(result.error_code),
            detail=body,
        )
    if result.changed_fields:
        await db.commit()
    return body


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


@router.post(
    "/boards/{board_id}/specs",
    response_model=SpecResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_spec(
    board_id: str,
    data: SpecCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new spec in a board."""
    service = SpecService(db)
    spec = await service.create_spec(board_id, user_id, data)
    if not spec:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Board not found or not owned by user",
        )
    await db.commit()
    spec = await service.get_spec(spec.id)
    return spec


@router.get("/boards/{board_id}/specs", response_model=list[SpecSummary])
async def list_specs(
    board_id: str,
    status_filter: str | None = Query(None, alias="status"),
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List specs for a board, optionally filtered by status."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = SpecService(db)
    return await service.list_specs(board_id, status_filter, include_archived=include_archived)


@router.get("/specs/{spec_id}", response_model=SpecResponse)
async def get_spec(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a spec by ID with its derived cards."""
    service = SpecService(db)
    spec = await service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    return spec


@router.patch("/specs/{spec_id}", response_model=SpecResponse)
async def update_spec(
    spec_id: str,
    data: SpecUpdate,
    response: Response,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a spec. Bumps version when content fields change.
    Rejects orphan `linked_*` references with 422 — see
    `_validate_spec_linked_refs` in services/main.py for the exact rules.
    """
    service = SpecService(db)
    existing = await service.get_spec(spec_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    fields_set = set(
        getattr(data, "model_fields_set", None)
        or getattr(data, "__fields_set__", set())
    )
    if fields_set & _STRUCTURED_SPEC_ENTITY_UPDATE_FIELDS:
        response.headers["X-Okto-Pulse-Deprecation"] = (
            STRUCTURED_SPEC_ENTITY_DEPRECATION_WARNING
        )
    required_permissions = _spec_update_permission_requirements(existing, data)
    if required_permissions:
        await _require_permissions(db, user_id, existing.board_id, sorted(required_permissions))
    try:
        spec = await service.update_spec(spec_id, user_id, data)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e),
        )
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    spec = await service.get_spec(spec_id)
    return spec


@router.post("/specs/{spec_id}/structured-entities/{entity_type}")
async def create_structured_spec_entity(
    spec_id: str,
    entity_type: str,
    data: StructuredSpecEntityMutationRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create one structured spec child entity through StructuredSpecEntityService."""
    return await _run_structured_spec_entity_command(
        db=db,
        user_id=user_id,
        spec_id=spec_id,
        entity_type=entity_type,
        operation="create",
        payload=data.payload,
        expected_spec_version=data.expected_spec_version,
        task_id=data.task_id,
        ack_token=data.ack_token,
    )


@router.patch("/specs/{spec_id}/structured-entities/{entity_type}/{entity_id}")
async def update_structured_spec_entity(
    spec_id: str,
    entity_type: str,
    entity_id: str,
    data: StructuredSpecEntityMutationRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update one structured spec child entity."""
    return await _run_structured_spec_entity_command(
        db=db,
        user_id=user_id,
        spec_id=spec_id,
        entity_type=entity_type,
        entity_id=entity_id,
        operation=data.operation or "update",
        payload=data.payload,
        expected_spec_version=data.expected_spec_version,
        task_id=data.task_id,
        ack_token=data.ack_token,
    )


@router.post("/specs/{spec_id}/structured-entities/{entity_type}/{entity_id}")
async def operate_structured_spec_entity(
    spec_id: str,
    entity_type: str,
    entity_id: str,
    data: StructuredSpecEntityMutationRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Run a structured operation such as revoke, supersede, restore, reorder, link_task or unlink_task."""
    if not data.operation:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="operation is required.",
        )
    return await _run_structured_spec_entity_command(
        db=db,
        user_id=user_id,
        spec_id=spec_id,
        entity_type=entity_type,
        entity_id=entity_id,
        operation=data.operation,
        payload=data.payload,
        expected_spec_version=data.expected_spec_version,
        task_id=data.task_id,
        ack_token=data.ack_token,
    )


@router.post("/specs/{spec_id}/structured-entities/{entity_type}/{entity_id}/impact-preview")
async def preview_structured_spec_entity_impact(
    spec_id: str,
    entity_type: str,
    entity_id: str,
    data: StructuredSpecEntityMutationRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Preview impact for destructive-like structured operations without mutating."""
    if not data.operation:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="operation is required.",
        )
    return await _run_structured_spec_entity_command(
        db=db,
        user_id=user_id,
        spec_id=spec_id,
        entity_type=entity_type,
        entity_id=entity_id,
        operation=data.operation,
        payload=data.payload,
        expected_spec_version=data.expected_spec_version,
        task_id=data.task_id,
        ack_token=data.ack_token,
        preview_only=True,
    )


@router.post("/specs/{spec_id}/move", response_model=SpecResponse)
async def move_spec(
    spec_id: str,
    data: SpecMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Change spec status."""
    service = SpecService(db)
    try:
        spec = await service.move_spec(spec_id, user_id, data)
    except GateContractError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=e.to_dict())
    except ResourceGateError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_resource_gate_detail(e),
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    spec = await service.get_spec(spec_id)
    return spec


@router.delete("/specs/{spec_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a spec. Unlinks derived cards but doesn't delete them."""
    service = SpecService(db)
    deleted = await service.delete_spec(spec_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()


@router.get("/specs/{spec_id}/history", response_model=list[SpecHistoryResponse])
async def list_spec_history(
    spec_id: str,
    limit: int = Query(50, ge=1, le=200),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed change history for a spec."""
    service = SpecService(db)
    return await service.list_history(spec_id, limit)


@router.post("/specs/{spec_id}/link-card/{card_id}", status_code=status.HTTP_200_OK)
async def link_card_to_spec(
    spec_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link an existing card to a spec."""
    service = SpecService(db)
    linked = await service.link_card(spec_id, card_id, user_id=user_id)
    if not linked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Spec or card not found, or they belong to different boards",
        )
    await db.commit()
    return {"success": True, "spec_id": spec_id, "card_id": card_id}


@router.post("/specs/{spec_id}/unlink-card/{card_id}", status_code=status.HTTP_200_OK)
async def unlink_card_from_spec(
    spec_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a card from a spec."""
    service = SpecService(db)
    unlinked = await service.unlink_card(card_id, user_id=user_id)
    if not unlinked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Card not found or not linked to any spec",
        )
    await db.commit()
    return {"success": True, "spec_id": spec_id, "card_id": card_id}


# ==================== LINK TASK TO SCENARIO ====================


@router.post("/specs/{spec_id}/scenarios/{scenario_id}/link-task/{card_id}", status_code=status.HTTP_200_OK)
async def link_task_to_scenario(
    spec_id: str,
    scenario_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a task card to a test scenario (bidirectional).
    Validates upfront that both scenario and card exist before mutating
    either side, so a typo in card_id no longer leaves an orphan link.
    """
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Card '{card_id}' not found — cannot link a non-existent card.",
        )

    scenarios = list(spec.test_scenarios or [])
    found = False
    for s in scenarios:
        if s.get("id") == scenario_id:
            task_ids = list(s.get("linked_task_ids") or [])
            if card_id not in task_ids:
                task_ids.append(card_id)
            s["linked_task_ids"] = task_ids
            found = True
            break

    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found in spec.",
        )

    try:
        await spec_service.update_spec(spec_id, user_id, SpecUpdate(test_scenarios=scenarios))
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e),
        )

    existing = list(card.test_scenario_ids or [])
    if scenario_id not in existing:
        existing.append(scenario_id)
    from okto_pulse.core.models.schemas import CardUpdate as CU
    await card_service.update_card(card_id, user_id, CU(test_scenario_ids=existing))

    await db.commit()
    return {"success": True, "spec_id": spec_id, "scenario_id": scenario_id, "card_id": card_id}


@router.post("/specs/{spec_id}/scenarios/{scenario_id}/unlink-task/{card_id}", status_code=status.HTTP_200_OK)
async def unlink_task_from_scenario(
    spec_id: str,
    scenario_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a task card from a test scenario (bidirectional)."""
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    scenarios = list(spec.test_scenarios or [])
    found = False
    for s in scenarios:
        if s.get("id") == scenario_id:
            task_ids = list(s.get("linked_task_ids") or [])
            if card_id in task_ids:
                task_ids.remove(card_id)
            s["linked_task_ids"] = task_ids
            found = True
            break

    if not found:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scenario not found")

    await spec_service.update_spec(spec_id, user_id, SpecUpdate(test_scenarios=scenarios))

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if card:
        existing = list(card.test_scenario_ids or [])
        if scenario_id in existing:
            existing.remove(scenario_id)
        from okto_pulse.core.models.schemas import CardUpdate as CU
        await card_service.update_card(card_id, user_id, CU(test_scenario_ids=existing))

    await db.commit()
    return {"success": True, "spec_id": spec_id, "scenario_id": scenario_id, "card_id": card_id}


@router.patch(
    "/specs/{spec_id}/scenarios/{scenario_id}/status",
    status_code=status.HTTP_200_OK,
)
async def update_test_scenario_status(
    spec_id: str,
    scenario_id: str,
    body: ScenarioStatusUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Scoped status mutation for a single test scenario (spec 6f1e75bf, FR6).

    Applies the same leaf helpers as the MCP status tool
    (require_test_scenario_status_mutable + validate_test_scenario_evidence via
    SpecService.set_test_scenario_status) and mutates ONLY the target scenario —
    it does NOT use the full-list update_spec path and does NOT trigger the
    content-lock, so the other scenarios are preserved. Rejects gated status
    without evidence (422) and arbitrary status changes on validated/done specs
    (409). Post-lock regression evidence remains allowed when the target
    scenario is already linked to an executable test card; this is operational
    evidence, not a semantic spec edit.
    """
    service = SpecService(db)
    try:
        result = await service.set_test_scenario_status(
            spec_id, user_id, scenario_id, body.status, body.evidence
        )
    except StatusNotMutableError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("scenario_not_found"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=msg
        )
    return {
        "id": spec_id,
        "scenario": {"id": result["scenario_id"], "status": result["new_status"]},
        "result": result,
    }


@router.post(
    "/specs/{spec_id}/integration-requirements/{requirement_id}/link-task/{card_id}",
    status_code=status.HTTP_200_OK,
)
async def link_task_to_integration_requirement(
    spec_id: str,
    requirement_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a task card to an integration requirement."""
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await _require_permissions(
        db,
        user_id,
        spec.board_id,
        ["spec.integration_requirements.link_task", "card.link_to.ir"],
    )

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Card '{card_id}' not found — cannot link a non-existent card.",
        )

    requirements = list(spec.integration_requirements or [])
    target = next((item for item in requirements if item.get("id") == requirement_id), None)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Integration requirement '{requirement_id}' not found in spec.",
        )

    task_ids = list(target.get("linked_task_ids") or [])
    if card_id not in task_ids:
        task_ids.append(card_id)
    target["linked_task_ids"] = task_ids

    try:
        await spec_service.update_spec(
            spec_id,
            user_id,
            SpecUpdate(integration_requirements=requirements),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e),
        )

    await db.commit()
    return {
        "success": True,
        "spec_id": spec_id,
        "requirement_id": requirement_id,
        "card_id": card_id,
    }


@router.post(
    "/specs/{spec_id}/observability-requirements/{requirement_id}/link-task/{card_id}",
    status_code=status.HTTP_200_OK,
)
async def link_task_to_observability_requirement(
    spec_id: str,
    requirement_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a task card to an observability requirement."""
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await _require_permissions(
        db,
        user_id,
        spec.board_id,
        ["spec.observability_requirements.link_task", "card.link_to.or"],
    )

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Card '{card_id}' not found — cannot link a non-existent card.",
        )

    requirements = list(spec.observability_requirements or [])
    target = next((item for item in requirements if item.get("id") == requirement_id), None)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Observability requirement '{requirement_id}' not found in spec.",
        )

    task_ids = list(target.get("linked_task_ids") or [])
    if card_id not in task_ids:
        task_ids.append(card_id)
    target["linked_task_ids"] = task_ids

    try:
        await spec_service.update_spec(
            spec_id,
            user_id,
            SpecUpdate(observability_requirements=requirements),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e),
        )

    await db.commit()
    return {
        "success": True,
        "spec_id": spec_id,
        "requirement_id": requirement_id,
        "card_id": card_id,
    }


# ==================== SPEC KNOWLEDGE BASE ====================


@router.get("/specs/{spec_id}/knowledge", response_model=list[SpecKnowledgeSummary])
async def list_spec_knowledge(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all knowledge base items for a spec (without content)."""
    service = SpecKnowledgeService(db)
    return await service.list_knowledge(spec_id)


@router.get("/specs/{spec_id}/knowledge/{knowledge_id}", response_model=SpecKnowledgeResponse)
async def get_spec_knowledge(
    spec_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a knowledge base item with full content."""
    service = SpecKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.spec_id != spec_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    return kb


@router.post("/specs/{spec_id}/knowledge", response_model=SpecKnowledgeResponse, status_code=status.HTTP_201_CREATED)
async def create_spec_knowledge(
    spec_id: str,
    data: SpecKnowledgeCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a knowledge base item to a spec."""
    service = SpecKnowledgeService(db)
    kb = await service.create_knowledge(spec_id, user_id, data)
    if not kb:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    return kb


@router.delete("/specs/{spec_id}/knowledge/{knowledge_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec_knowledge(
    spec_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a knowledge base item."""
    service = SpecKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.spec_id != spec_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    await service.delete_knowledge(knowledge_id)
    await db.commit()


# ==================== SPEC Q&A ====================


@router.get("/specs/{spec_id}/qa", response_model=list[SpecQAResponse])
async def list_spec_qa(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Q&A items for a spec."""
    service = SpecQAService(db)
    return await service.list_qa(spec_id)


@router.post("/specs/{spec_id}/qa", response_model=SpecQAResponse, status_code=status.HTTP_201_CREATED)
async def create_spec_question(
    spec_id: str,
    data: SpecQACreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Ask a question on a spec."""
    service = SpecQAService(db)
    qa = await service.create_question(spec_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    return qa


@router.post("/specs/{spec_id}/qa/{qa_id}/answer", response_model=SpecQAResponse)
async def answer_spec_question(
    spec_id: str,
    qa_id: str,
    data: SpecQAAnswer,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Answer a spec Q&A question."""
    service = SpecQAService(db)
    try:
        qa = await service.answer_question(
            qa_id, user_id, data, actor_type="user", surface="rest"
        )
    except QASelfAnsweringNotAllowedError as exc:
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"reason": exc.reason, "message": str(exc)},
        ) from exc
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()
    return qa


@router.delete("/specs/{spec_id}/qa/{qa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec_question(
    spec_id: str,
    qa_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a spec Q&A item."""
    service = SpecQAService(db)
    deleted = await service.delete_question(qa_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()


# ---- Spec Validation Gate Endpoints ----


@router.post("/specs/{spec_id}/validation", status_code=status.HTTP_201_CREATED)
async def submit_spec_validation(
    spec_id: str,
    data: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a Spec Validation Gate record for a spec in 'approved' status.

    Runs deterministic coverage gates as pre-requisite. If they pass, computes
    outcome from thresholds + recommendation: failed if any threshold violated
    or recommendation=reject; success only if all thresholds OK and approve.
    On success, atomically promotes spec.status to validated.
    """
    # Validate required fields (mirror SpecValidationSubmit schema)
    required = [
        "completeness", "completeness_justification",
        "assertiveness", "assertiveness_justification",
        "ambiguity", "ambiguity_justification",
        "general_justification", "recommendation",
    ]
    missing = [f for f in required if f not in data or data[f] is None]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required fields: {', '.join(missing)}",
        )
    if data.get("recommendation") not in ("approve", "reject"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="recommendation must be 'approve' or 'reject'",
        )
    # Min length checks (schema-level guarantee, but fail fast here too)
    for dim in ("completeness", "assertiveness", "ambiguity"):
        jf = data.get(f"{dim}_justification", "")
        if not isinstance(jf, str) or len(jf.strip()) < 10:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{dim}_justification must be at least 10 characters",
            )
    if len((data.get("general_justification") or "").strip()) < 20:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="general_justification must be at least 20 characters",
        )

    service = SpecService(db)
    spec = await service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    # Resolve reviewer name
    try:
        from okto_pulse.core.services.main import resolve_actor_name
        reviewer_name = await resolve_actor_name(db, user_id, spec.board_id)
    except Exception:
        reviewer_name = user_id

    try:
        result = await service.submit_spec_validation(
            spec_id=spec_id,
            reviewer_id=user_id,
            reviewer_name=reviewer_name,
            data=data,
        )
    except ResourceGateError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_resource_gate_detail(e),
        )
    except ValueError as e:
        # Could be: state guard, opt-in guard, coverage gate failure, or input validation
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    await db.commit()
    return result


@router.get("/specs/{spec_id}/validations")
async def list_spec_validations(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Spec Validation Gate records in reverse chronological order.

    Returns current_validation_id and the validations array with an 'active'
    flag on each record indicating if it's the currently-active pointer.
    """
    service = SpecService(db)
    try:
        result = await service.list_spec_validations(spec_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return {"spec_id": spec_id, **result}


class SpecEvaluationSubmit(BaseModel):
    """Avaliação qualitativa de uma spec validated — gêmeo REST do MCP tool
    ``okto_pulse_submit_spec_evaluation`` (paridade de superfícies)."""

    breakdown_completeness: int = Field(..., ge=0, le=100)
    breakdown_justification: str = Field(..., min_length=10)
    granularity: int = Field(..., ge=0, le=100)
    granularity_justification: str = Field(..., min_length=10)
    dependency_coherence: int = Field(..., ge=0, le=100)
    dependency_justification: str = Field(..., min_length=10)
    test_coverage_quality: int = Field(..., ge=0, le=100)
    test_coverage_justification: str = Field(..., min_length=10)
    overall_score: int = Field(..., ge=0, le=100)
    overall_justification: str = Field(..., min_length=10)
    recommendation: Literal["approve", "request_changes", "reject"]


@router.post("/specs/{spec_id}/evaluations", status_code=status.HTTP_201_CREATED)
async def submit_spec_evaluation(
    spec_id: str,
    data: SpecEvaluationSubmit,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a qualitative evaluation for a spec in 'validated' status.

    Gap fechado (paridade REST/MCP): este gate é pré-requisito de
    ``move_spec(validated→in_progress)``, mas só existia como MCP tool —
    usuários UI/REST ficavam presos em ``validated`` sem caminho de escrita.
    Mesma semântica do tool: múltiplos avaliadores, append-only, spec
    precisa estar em 'validated'.
    """
    service = SpecService(db)
    spec = await service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    try:
        from okto_pulse.core.services.main import resolve_actor_name
        evaluator_name = await resolve_actor_name(db, user_id, spec.board_id)
    except Exception:
        evaluator_name = user_id

    try:
        evaluation = await service.submit_spec_evaluation(
            spec_id,
            user_id,
            evaluator_name,
            data.model_dump(),
            actor_type="user",
            surface="api",
        )
    except GateContractError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=e.to_dict())
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    # R4-IMP1: evaluation is append-only; capture status BEFORE (the `spec` fetched
    # above) and AFTER independently so a future auto-transition surfaces as
    # state_changed. Same envelope as the MCP twin.
    status_before = spec.status.value
    spec_after = await service.get_spec(spec_id)
    status_after = spec_after.status.value if spec_after else status_before
    await db.commit()
    return spec_evaluation_success_envelope(
        spec_id=spec_id, status_before=status_before, status_after=status_after,
        evaluation=evaluation,
    )


@router.get("/specs/{spec_id}/evaluations")
async def list_spec_evaluations(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List spec evaluations (newest first) — gêmeo REST de
    ``okto_pulse_list_spec_evaluations``."""
    service = SpecService(db)
    try:
        return await service.list_spec_evaluations(spec_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
