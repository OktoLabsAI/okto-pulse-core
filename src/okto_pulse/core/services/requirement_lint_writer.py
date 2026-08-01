"""Shared orchestration boundary used by every semantic Spec writer."""

from __future__ import annotations

import copy
from typing import Any

from okto_pulse.core.domain.quality_canonicalization import (
    SEMANTIC_FIELD_MANIFEST_V1,
)
from okto_pulse.core.ports.requirement_lint import (
    RequirementLintExecutionFailed,
    RequirementLintWriteCommand,
    RequirementLintWriteResult,
    RequirementLintWriter,
    get_requirement_lint_writer_hook,
)
from okto_pulse.core.services.spec_entity_canonicalization import (
    SPEC_REQUIREMENT_FIELDS,
    canonicalize_spec_requirement_fields,
)


def spec_requirement_lint_payload(spec: object) -> dict[str, Any]:
    """Snapshot the final semantic Spec state before the UoW can commit.

    Legacy requirement strings are canonicalized only in this defensive read
    projection. This keeps the lint contract closed without turning an
    unrelated partial update into a hidden persistence migration.
    """

    payload: dict[str, Any] = {
        field_name: copy.deepcopy(getattr(spec, field_name, None))
        for field_name in SEMANTIC_FIELD_MANIFEST_V1["spec"]
    }
    payload.update(
        {
            "id": getattr(spec, "id", None),
            "board_id": getattr(spec, "board_id", None),
            "version": getattr(spec, "version", None),
        }
    )
    requirement_fields = {
        field_name: payload.get(field_name)
        for field_name, _ in SPEC_REQUIREMENT_FIELDS
    }
    payload.update(
        canonicalize_spec_requirement_fields(
            requirement_fields,
            existing_fields=requirement_fields,
        )
    )
    return payload


async def stage_spec_requirement_lint(
    context: object,
    spec: object,
    *,
    actor_id: str,
    writer: RequirementLintWriter,
    changed_fields: tuple[str, ...] | list[str],
) -> RequirementLintWriteResult:
    """Stage the lint receipt/findings/head through the current transaction.

    The concrete hook is forbidden from committing. Any analyzer, adapter,
    CAS, audit or outbox failure is normalized to the single writer-facing
    error required by SK-A; the caller's UoW then rolls everything back.
    """

    try:
        status = getattr(spec, "status", "")
        command = RequirementLintWriteCommand(
            board_id=str(getattr(spec, "board_id", "")),
            spec_id=str(getattr(spec, "id", "")),
            spec_version=getattr(spec, "version", None),
            actor_id=actor_id,
            writer=writer,
            spec_status=str(getattr(status, "value", status)),
            spec_archived=bool(getattr(spec, "archived", False)),
            changed_fields=tuple(changed_fields),
            spec_payload=spec_requirement_lint_payload(spec),
        )
        result = await get_requirement_lint_writer_hook().stage_requirement_lint(
            context,
            command,
        )
        if not isinstance(result, RequirementLintWriteResult):
            raise TypeError("requirement_lint_writer_hook_result_invalid")
        return result
    except RequirementLintExecutionFailed:
        raise
    except Exception as exc:
        raise RequirementLintExecutionFailed(
            stage="writer_hook",
            detail=type(exc).__name__,
        ) from exc


__all__ = [
    "spec_requirement_lint_payload",
    "stage_spec_requirement_lint",
]
