"""Fail-closed regression for the retired schema-v1 guideline surface."""

from __future__ import annotations

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.import_export import (
    ExportGuidelinesCommand,
    ExportGuidelinesUseCase,
    GuidelineExportV3Required,
    ImportGuidelinesCommand,
    ImportGuidelinesUseCase,
)


class _PoisonUow:
    """Any UoW access means authorization happened too late."""

    def __getattribute__(self, name: str):
        raise AssertionError(f"denied import touched the UoW: {name}")


@pytest.mark.asyncio
async def test_legacy_guideline_import_denies_before_any_read_or_write() -> None:
    actor = ActorContext(
        "limited-importer",
        "rest",
        realm_id="local",
        permissions={},
    )

    with pytest.raises(
        PermissionDeniedError,
        match=r"guidelines\.revisions\.create",
    ):
        await ImportGuidelinesUseCase().execute(
            ImportGuidelinesCommand(items=[]),
            actor=actor,
            uow=_PoisonUow(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_authorized_legacy_import_fails_closed_before_uow_access() -> None:
    actor = ActorContext(
        "authorized-importer",
        "rest",
        realm_id="local",
        permissions={
            "guidelines": {
                "revisions": {
                    "create": True,
                }
            },
            # SK-B introduction keeps the historical edit authority as a
            # conservative bridge for pre-existing custom permission sets.
            "spec": {"entity": {"edit_fields": True}},
        },
    )

    with pytest.raises(
        GuidelineExportV3Required,
        match="guideline_export_v3_required",
    ):
        await ImportGuidelinesUseCase().execute(
            ImportGuidelinesCommand(items=[]),
            actor=actor,
            uow=_PoisonUow(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_authorized_legacy_export_fails_closed_before_uow_access() -> None:
    actor = ActorContext(
        "authorized-exporter",
        "rest",
        realm_id="local",
        permissions={
            "guidelines": {
                "read": True,
                "revisions": {"read": True},
            },
        },
    )

    with pytest.raises(
        GuidelineExportV3Required,
        match="guideline_export_v3_required",
    ):
        await ExportGuidelinesUseCase().execute(
            ExportGuidelinesCommand(),
            actor=actor,
            uow=_PoisonUow(),  # type: ignore[arg-type]
        )
