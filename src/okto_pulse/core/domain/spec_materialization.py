"""Pure planning for legacy FR/TR/AC materialization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.services.spec_entity_canonicalization import (
    DuplicateSpecChildIdError,
    SPEC_REQUIREMENT_FIELDS,
    canonicalize_spec_requirement_fields,
)


# Backward-compatible name retained for command/import callers. Its value is
# intentionally widened to the SK-A FR/TR/AC contract.
FR_AC_FIELDS = SPEC_REQUIREMENT_FIELDS


@dataclass(frozen=True, slots=True)
class SpecFieldMaterialization:
    spec: object
    fields: tuple[tuple[str, list[Any]], ...]


@dataclass(frozen=True, slots=True)
class SpecMaterializationPlan:
    scanned: int
    skipped: int
    errors: int
    changes: tuple[SpecFieldMaterialization, ...]

    @property
    def changed(self) -> int:
        return len(self.changes)


def plan_legacy_spec_requirement_materialization(
    specs: Sequence[object],
) -> SpecMaterializationPlan:
    changes: list[SpecFieldMaterialization] = []
    skipped = 0
    errors = 0
    for spec in specs:
        try:
            current_fields = {
                # Older adapter/test records predate the TR column. Treat an
                # absent collection exactly like a stored NULL so widening
                # the governed materializer remains backwards compatible.
                field_name: getattr(spec, field_name, None)
                for field_name, _ in SPEC_REQUIREMENT_FIELDS
            }
            canonical_fields = canonicalize_spec_requirement_fields(
                current_fields,
                existing_fields=current_fields,
            )
            pending: list[tuple[str, list[Any]]] = []
            for field_name, _ in SPEC_REQUIREMENT_FIELDS:
                current = current_fields[field_name]
                canonical = canonical_fields[field_name]
                if canonical != current:
                    pending.append((field_name, canonical))
        except DuplicateSpecChildIdError:
            errors += 1
            continue
        if pending:
            changes.append(SpecFieldMaterialization(spec, tuple(pending)))
        else:
            skipped += 1
    return SpecMaterializationPlan(
        scanned=len(specs),
        skipped=skipped,
        errors=errors,
        changes=tuple(changes),
    )


def plan_legacy_fr_ac_materialization(
    specs: Sequence[object],
) -> SpecMaterializationPlan:
    """Compatibility alias for the widened FR/TR/AC materializer."""

    return plan_legacy_spec_requirement_materialization(specs)


__all__ = [
    "FR_AC_FIELDS",
    "SpecFieldMaterialization",
    "SpecMaterializationPlan",
    "plan_legacy_fr_ac_materialization",
    "plan_legacy_spec_requirement_materialization",
]
