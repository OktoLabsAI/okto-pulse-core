"""Edition-neutral commands and results for explicit requirement-lint assessments."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from okto_pulse.core.services.spec_entity_canonicalization import (
    SPEC_REQUIREMENT_FIELDS,
    spec_child_id,
)


class RequirementLintWriter(str, Enum):
    BULK_CREATE = "bulk_create"
    BULK_UPDATE = "bulk_update"
    DERIVE_IDEATION = "derive_ideation"
    DERIVE_REFINEMENT = "derive_refinement"
    STRUCTURED_CRUD = "structured_crud"
    SCENARIO_BODY_UPDATE = "scenario_body_update"
    SCENARIO_DELETE = "scenario_delete"
    SEED = "seed"


class RequirementLintWriterContractError(ValueError):
    """The semantic writer supplied an invalid post-mutation snapshot."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class RequirementLintWriteCommand:
    """Complete semantic input for an explicitly prepared lint assessment."""

    board_id: str
    spec_id: str
    spec_version: int
    actor_id: str
    writer: RequirementLintWriter
    spec_status: str
    spec_archived: bool
    changed_fields: tuple[str, ...]
    spec_payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "actor_id", "spec_status"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise RequirementLintWriterContractError(
                    f"requirement_lint_{field_name}_required"
                )
            object.__setattr__(self, field_name, value.strip())
        if (
            not isinstance(self.spec_version, int)
            or isinstance(self.spec_version, bool)
            or self.spec_version < 1
        ):
            raise RequirementLintWriterContractError(
                "requirement_lint_spec_version_invalid"
            )
        if not isinstance(self.writer, RequirementLintWriter):
            raise RequirementLintWriterContractError(
                "requirement_lint_writer_invalid"
            )
        if not isinstance(self.spec_archived, bool):
            raise RequirementLintWriterContractError(
                "requirement_lint_spec_archived_invalid"
            )
        if not isinstance(self.changed_fields, tuple | list):
            raise RequirementLintWriterContractError(
                "requirement_lint_changed_fields_invalid"
            )
        changed = tuple(
            str(value).strip()
            for value in self.changed_fields
            if isinstance(value, str) and value.strip()
        )
        if not changed or len(changed) != len(self.changed_fields):
            raise RequirementLintWriterContractError(
                "requirement_lint_changed_fields_invalid"
            )
        object.__setattr__(self, "changed_fields", changed)
        if not isinstance(self.spec_payload, Mapping):
            raise RequirementLintWriterContractError(
                "requirement_lint_spec_payload_invalid"
            )
        payload = dict(self.spec_payload)
        if payload.get("id") != self.spec_id or payload.get("board_id") != self.board_id:
            raise RequirementLintWriterContractError(
                "requirement_lint_spec_identity_mismatch"
            )
        if payload.get("version") != self.spec_version:
            raise RequirementLintWriterContractError(
                "requirement_lint_spec_version_mismatch"
            )
        _validate_canonical_requirement_collections(payload)
        object.__setattr__(self, "spec_payload", payload)


def _validate_canonical_requirement_collections(payload: Mapping[str, Any]) -> None:
    from okto_pulse.core.domain.requirement_lint import RequirementLocale

    seen_ids: set[str] = set()
    for field_name, _ in SPEC_REQUIREMENT_FIELDS:
        values = payload.get(field_name)
        if values is None:
            continue
        if not isinstance(values, Sequence) or isinstance(
            values, str | bytes | bytearray
        ):
            raise RequirementLintWriterContractError(
                "requirement_lint_requirement_collection_invalid"
            )
        for item in values:
            if not isinstance(item, Mapping):
                raise RequirementLintWriterContractError(
                    "requirement_lint_requirement_not_canonical"
                )
            child_id = spec_child_id(item)
            text = item.get("text")
            status = item.get("status")
            if (
                child_id is None
                or not isinstance(text, str)
                or not isinstance(status, str)
                or not status.strip()
            ):
                raise RequirementLintWriterContractError(
                    "requirement_lint_requirement_not_canonical"
                )
            raw_locale = item.get("locale")
            if raw_locale is not None:
                try:
                    RequirementLocale(raw_locale)
                except (TypeError, ValueError) as exc:
                    raise RequirementLintWriterContractError(
                        "requirement_lint_locale_invalid"
                    ) from exc
            if child_id in seen_ids:
                raise RequirementLintWriterContractError(
                    "requirement_lint_duplicate_child_id"
                )
            seen_ids.add(child_id)


@dataclass(frozen=True, slots=True)
class RequirementLintWriteResult:
    receipt_id: str
    head_revision: int
    evaluated_rule_count: int
    finding_count: int
    replayed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.receipt_id, str) or not self.receipt_id.strip():
            raise RequirementLintWriterContractError(
                "requirement_lint_receipt_id_required"
            )
        for field_name in (
            "head_revision",
            "evaluated_rule_count",
            "finding_count",
        ):
            value = getattr(self, field_name)
            minimum = 1 if field_name != "finding_count" else 0
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                raise RequirementLintWriterContractError(
                    f"requirement_lint_{field_name}_invalid"
                )
        if self.finding_count > self.evaluated_rule_count:
            raise RequirementLintWriterContractError(
                "requirement_lint_finding_count_out_of_scale"
            )
        if not isinstance(self.replayed, bool):
            raise RequirementLintWriterContractError(
                "requirement_lint_replayed_invalid"
            )


__all__ = [
    "RequirementLintWriteCommand",
    "RequirementLintWriteResult",
    "RequirementLintWriter",
    "RequirementLintWriterContractError",
]
