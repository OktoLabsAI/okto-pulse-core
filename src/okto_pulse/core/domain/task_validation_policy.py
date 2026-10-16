"""Current task-validation policy resolved by field from Spec and Board."""

from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict

FIELDS = ("required", "min_confidence", "min_completeness", "max_drift")
ATTRIBUTES = ("require_task_validation", "validation_min_confidence", "validation_min_completeness", "validation_max_drift")
BOARD_ATTRIBUTES = ("require_task_validation", "min_confidence", "min_completeness", "max_drift")
DEFAULTS = (True, 70, 80, 50)

ValidationPolicySource = Literal["spec", "board", "default"]


class ResolvedTaskValidationSources(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    required: ValidationPolicySource
    min_confidence: ValidationPolicySource
    min_completeness: ValidationPolicySource
    max_drift: ValidationPolicySource


class ResolvedTaskValidationConfig(BaseModel):
    """Read projection of the Core policy; never an editable Card override."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    required: bool
    min_confidence: int
    min_completeness: int
    max_drift: int
    resolved_from: ValidationPolicySource
    resolved_sources: ResolvedTaskValidationSources


def _value(record, field):
    return record.get(field) if isinstance(record, Mapping) else getattr(record, field, None)


def resolve_task_validation_config(spec, board_settings: Mapping) -> dict:
    """Resolve current policy without a Card-specific compatibility hierarchy."""
    result, sources = {}, {}
    for field, attribute, board_attribute, default in zip(FIELDS, ATTRIBUTES, BOARD_ATTRIBUTES, DEFAULTS, strict=True):
        layers = ((_value(spec, attribute), "spec"), (board_settings.get(board_attribute, default), "board"))
        # Preserve the current distinction between an absent Board setting and
        # an explicit null required flag; false and zero are explicit values.
        value, source = next(((value, source) for value, source in layers if value is not None),
            (False if field == "required" else default, "default"))
        result[field] = bool(value) if field == "required" else value
        sources[field] = source
    return {**result, "resolved_from": sources["required"], "resolved_sources": sources}
