"""Pure planning for legacy FR/AC materialization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.services.spec_entity_canonicalization import (
    DuplicateSpecChildIdError,
    canonicalize_fr_ac,
)


FR_AC_FIELDS: tuple[tuple[str, str], ...] = (
    ("functional_requirements", "functional_requirement"),
    ("acceptance_criteria", "acceptance_criterion"),
)


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


def plan_legacy_fr_ac_materialization(
    specs: Sequence[object],
) -> SpecMaterializationPlan:
    changes: list[SpecFieldMaterialization] = []
    skipped = 0
    errors = 0
    for spec in specs:
        try:
            pending: list[tuple[str, list[Any]]] = []
            for field_name, entity_type in FR_AC_FIELDS:
                current = getattr(spec, field_name)
                canonical = canonicalize_fr_ac(
                    entity_type,
                    current,
                    existing_items=current,
                )
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


__all__ = [
    "FR_AC_FIELDS",
    "SpecFieldMaterialization",
    "SpecMaterializationPlan",
    "plan_legacy_fr_ac_materialization",
]
