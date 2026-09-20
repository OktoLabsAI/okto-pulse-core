"""Public pure contract for preserving task policy during the Sprint cutover.

Deprecated compatibility only: no executor authoring or new policy authority.
Remove preserved overrides only after inventory or an authorized replacement.
"""

from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Mapping

from okto_pulse.core.domain.task_validation_policy import (
    FIELDS, Identity, MigratedTaskValidationPolicy, plan_migrated_validation_policy,
    resolve_task_validation_config,
)
from pydantic import TypeAdapter


@dataclass(frozen=True, slots=True)
class CardValidationMigrationPlan:
    policy: MigratedTaskValidationPolicy | None
    before: dict
    after: dict


def plan_card_validation_migration(*, card: Mapping, spec: Mapping | None, sprint: Mapping,
                                  board_settings: Mapping, migration_id: str) -> CardValidationMigrationPlan:
    """Plan the exact detached policy; facts stay edition-loaded and unmodified."""
    identity = TypeAdapter(Identity)
    for value in (migration_id, card.get("id"), card.get("board_id"), sprint.get("id")):
        identity.validate_python(value)
    policy = plan_migrated_validation_policy(card=card, spec=spec, sprint=sprint,
        board_settings=board_settings, migration_id=migration_id)
    before = resolve_task_validation_config(card, spec, sprint, board_settings)
    detached = {**card, "sprint_id": None,
        "migrated_validation_policy": policy.model_dump(mode="json", exclude_none=True) if policy else None}
    after = resolve_task_validation_config(detached, spec, None, board_settings)
    for field in FIELDS:
        if before[field] != after[field] or type(before[field]) is not type(after[field]):
            raise ValueError("card_validation_migration_parity_changed")
    return CardValidationMigrationPlan(policy, deepcopy(before), deepcopy(after))


def verify_card_validation_migration(*, card: Mapping, spec: Mapping | None,
                                     board_settings: Mapping, expected: Mapping) -> None:
    """Verify persisted detached facts through the same resolver as live gates."""
    if card.get("sprint_id") is not None:
        raise ValueError("card_validation_migration_link_retained")
    actual = resolve_task_validation_config(card, spec, None, board_settings)
    if actual != expected or any(type(actual[field]) is not type(expected[field]) for field in FIELDS):
        raise ValueError("card_validation_migration_parity_changed")
