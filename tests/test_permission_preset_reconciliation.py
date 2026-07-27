"""F07 pure Core tests for built-in permission-preset reconciliation."""

from __future__ import annotations

import asyncio

import pytest

from okto_pulse.core.application.use_cases.permission_preset_reconciliation import (
    ReconcilePermissionPresetsUseCase,
)
from okto_pulse.core.domain.permission_presets import (
    PermissionPresetDefinition,
    PermissionPresetReconciliationError,
    PersistedPermissionPreset,
    ReconciliationAction,
    builtin_permission_preset_definitions,
    plan_permission_preset_reconciliation,
)
from okto_pulse.core.ports.permission_preset_reconciliation import (
    PermissionPresetReconciliationRepository,
)


def _persisted(
    definition: PermissionPresetDefinition,
    *,
    preset_id: str = "builtin-1",
    is_builtin: bool = True,
) -> PersistedPermissionPreset:
    return PersistedPermissionPreset(
        id=preset_id,
        owner_id=None,
        name=definition.name,
        description=definition.description,
        is_builtin=is_builtin,
        flags=definition.flags,
        base_preset_name=definition.base_preset_name,
    )


def test_planner_emits_catalog_ordered_inserts_for_empty_state() -> None:
    catalog = builtin_permission_preset_definitions()
    commands = plan_permission_preset_reconciliation(catalog, ())

    assert [command.action for command in commands] == [
        ReconciliationAction.INSERT
    ] * len(catalog)
    assert [command.definition.name for command in commands] == [
        definition.name for definition in catalog
    ]


def test_planner_is_idempotent_and_updates_only_drifted_builtin() -> None:
    definition = PermissionPresetDefinition("Reader", "read", {"board": {"read": True}})
    matching = _persisted(definition)
    assert plan_permission_preset_reconciliation((definition,), (matching,)) == ()

    drifted = PersistedPermissionPreset(
        id=matching.id,
        name=matching.name,
        description="old",
        is_builtin=True,
        flags={"board": {"read": False}},
    )
    commands = plan_permission_preset_reconciliation((definition,), (drifted,))
    assert len(commands) == 1
    assert commands[0].action is ReconciliationAction.UPDATE
    assert commands[0].preset_id == matching.id


def test_planner_preserves_custom_presets_and_inheritance() -> None:
    definition = PermissionPresetDefinition(
        "Executor",
        "executes",
        {"card": {"run": True}},
        base_preset_name="Base",
    )
    custom_same_name = _persisted(
        definition,
        preset_id="custom-1",
        is_builtin=False,
    )
    custom_other = PersistedPermissionPreset(
        id="custom-2",
        owner_id="user-1",
        name="My preset",
        description="custom",
        is_builtin=False,
        flags={"card": {"run": False}},
        base_preset_name="Executor",
    )

    commands = plan_permission_preset_reconciliation(
        (definition,),
        (custom_same_name, custom_other),
    )
    assert len(commands) == 1
    assert commands[0].action is ReconciliationAction.INSERT
    assert commands[0].definition.base_preset_name == "Base"
    assert all(command.preset_id not in {"custom-1", "custom-2"} for command in commands)


def test_planner_fails_closed_on_ambiguous_builtin_identity() -> None:
    definition = PermissionPresetDefinition("Reader", "read", {})
    with pytest.raises(PermissionPresetReconciliationError, match="duplicate built-in"):
        plan_permission_preset_reconciliation((definition, definition), ())
    with pytest.raises(PermissionPresetReconciliationError, match="duplicate persisted"):
        plan_permission_preset_reconciliation(
            (definition,),
            (_persisted(definition, preset_id="a"), _persisted(definition, preset_id="b")),
        )


def test_use_case_applies_one_batch_and_emits_no_second_write() -> None:
    definition = PermissionPresetDefinition("Reader", "read", {"board": {"read": True}})

    class FakeRepository:
        def __init__(self) -> None:
            self.rows: list[PersistedPermissionPreset] = []
            self.batches: list[tuple] = []

        async def list_permission_presets(self):
            return tuple(self.rows)

        async def apply_permission_preset_commands(self, commands):
            self.batches.append(tuple(commands))
            self.rows = [_persisted(command.definition) for command in commands]

    async def drive():
        repository = FakeRepository()
        assert isinstance(repository, PermissionPresetReconciliationRepository)
        use_case = ReconcilePermissionPresetsUseCase((definition,))
        first = await use_case.execute(repository)
        second = await use_case.execute(repository)
        return repository, first, second

    repository, first, second = asyncio.run(drive())
    assert first.changed is True
    assert second.changed is False
    assert len(repository.batches) == 1
