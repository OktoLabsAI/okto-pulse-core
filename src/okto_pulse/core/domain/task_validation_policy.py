"""Task policy resolution and deprecated, migration-only Card compatibility."""

from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


Identity = Annotated[str, Field(strict=True, min_length=1, max_length=128)]
Score = Annotated[int, Field(strict=True, ge=0, le=100)]
FIELDS = ("required", "min_confidence", "min_completeness", "max_drift")
ATTRIBUTES = ("require_task_validation", "validation_min_confidence", "validation_min_completeness", "validation_max_drift")
BOARD_ATTRIBUTES = ("require_task_validation", "min_confidence", "min_completeness", "max_drift")
DEFAULTS = (True, 70, 80, 50)


class MigratedTaskValidationOverrides(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    required: bool | None = None
    min_confidence: Score | None = None
    min_completeness: Score | None = None
    max_drift: Score | None = None

    @model_validator(mode="after")
    def nonempty(self):
        if not any(getattr(self, field) is not None for field in FIELDS):
            raise ValueError("card_validation_compatibility_empty")
        return self


class MigratedTaskValidationPolicy(BaseModel):
    """Deprecated compatibility, never an executor-editable policy hierarchy.

    Retire only after inventory proves no required override remains or an
    authorized human policy revision replaces it. Never expire by time or
    silently weaken gates. Source IDs are opaque historical provenance.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    contract_version: Literal["card-validation-compatibility/v1"]
    board_id: Identity
    card_id: Identity
    source_sprint_id: Identity
    source_spec_id: Identity | None = None
    migration_id: Identity
    overrides: MigratedTaskValidationOverrides


def _value(record, field):
    return record.get(field) if isinstance(record, Mapping) else getattr(record, field, None)


def read_migrated_validation_policy(card, sprint=None) -> MigratedTaskValidationPolicy | None:
    raw = _value(card, "migrated_validation_policy")
    if raw is None:
        return None
    policy = MigratedTaskValidationPolicy.model_validate(raw)
    if (policy.card_id, policy.board_id) != (_value(card, "id"), _value(card, "board_id")):
        raise ValueError("card_validation_compatibility_scope_mismatch")
    if sprint is not None or _value(card, "sprint_id") is not None:
        raise ValueError("card_validation_compatibility_requires_detached_sprint")
    return policy


def resolve_task_validation_config(card, spec, sprint, board_settings: Mapping) -> dict:
    """Preserve existing per-field Sprint/Spec/Board/null/default semantics."""
    compatibility = read_migrated_validation_policy(card, sprint)
    result, sources = {}, {}
    for field, attribute, board_attribute, default in zip(FIELDS, ATTRIBUTES, BOARD_ATTRIBUTES, DEFAULTS, strict=True):
        migrated = getattr(compatibility.overrides, field) if compatibility else None
        layers = ((migrated, "card_compatibility"), (_value(sprint, attribute), "sprint"),
            (_value(spec, attribute), "spec"), (board_settings.get(board_attribute, default), "board"))
        # Historical explicit board null differs from an absent required flag.
        value, source = next(((value, source) for value, source in layers if value is not None),
            (False if field == "required" else default, "default"))
        result[field] = bool(value) if field == "required" else value
        sources[field] = source
    return {**result, "resolved_from": sources["required"], "resolved_sources": sources}


def plan_migrated_validation_policy(*, card, spec, sprint, board_settings: Mapping,
                                   migration_id: str) -> MigratedTaskValidationPolicy | None:
    """Compute only effective differences; does not write or detach anything.

    The Community cutover must archive Sprint history and apply this result with
    link removal in one fenced migration. Existing compatibility is not recaptured
    from a new baseline. Invalid/orphan/cross-board input blocks migration.
    """
    if _value(card, "migrated_validation_policy") is not None:
        raise ValueError("card_validation_compatibility_already_present")
    if (sprint is None or not _value(card, "sprint_id")
        or _value(card, "sprint_id") != _value(sprint, "id")
        or _value(card, "board_id") != _value(sprint, "board_id")
        or _value(card, "spec_id") != _value(spec, "id")
        or (spec is not None and _value(card, "board_id") != _value(spec, "board_id"))):
        raise ValueError("card_validation_migration_scope_invalid")
    before = resolve_task_validation_config(card, spec, sprint, board_settings)
    after = resolve_task_validation_config(card, spec, None, board_settings)
    # Validate the effective contract before the no-difference shortcut. Do not
    # normalize corrupt scores or reject unused lower layers masked by a valid
    # Sprint override. A typed difference (e.g. 1 versus True) still needs saving.
    MigratedTaskValidationOverrides.model_validate({field: before[field] for field in FIELDS})
    differences = {field: before[field] for field in FIELDS
        if before[field] != after[field] or type(before[field]) is not type(after[field])}
    if not differences:
        return None
    return MigratedTaskValidationPolicy(contract_version="card-validation-compatibility/v1",
        board_id=_value(card, "board_id"), card_id=_value(card, "id"), source_sprint_id=_value(sprint, "id"),
        source_spec_id=_value(card, "spec_id"), migration_id=migration_id,
        overrides=MigratedTaskValidationOverrides(**differences))


def reject_migrated_validation_policy_write(value):
    if isinstance(value, Mapping) and "migrated_validation_policy" in value:
        raise ValueError("card_validation_compatibility_migration_only")
    return value
