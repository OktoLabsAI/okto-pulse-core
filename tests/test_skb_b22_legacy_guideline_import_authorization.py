"""Authorization regression for the schema-v1 guideline importer."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.import_export import (
    ImportGuidelinesCommand,
    ImportGuidelinesUseCase,
)
from okto_pulse.core.models.schemas import GuidelineCreate


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


class _GuidelineService:
    def __init__(self) -> None:
        self.reads = 0
        self.created: list[tuple[str, GuidelineCreate, str]] = []

    async def list_guidelines(self, owner_id: str, **_kwargs):
        self.reads += 1
        assert owner_id == "authorized-importer"
        return []

    async def create_guideline(
        self,
        owner_id: str,
        data: GuidelineCreate,
        *,
        actor_type: str,
        **_kwargs,
    ):
        self.created.append((owner_id, data, actor_type))
        return SimpleNamespace(id="created-guideline")


class _AllowedUow:
    def __init__(self, service: _GuidelineService) -> None:
        self.services = SimpleNamespace(guidelines=service)
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


@pytest.mark.asyncio
async def test_legacy_guideline_import_allows_revision_creator() -> None:
    service = _GuidelineService()
    uow = _AllowedUow(service)
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

    result = await ImportGuidelinesUseCase().execute(
        ImportGuidelinesCommand(
            items=[
                GuidelineCreate(
                    title="Imported policy",
                    content="Every API change has a compatibility note.",
                    scope="global",
                )
            ]
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    assert result.created == 1
    assert service.reads == 1
    assert len(service.created) == 1
    owner_id, created, actor_type = service.created[0]
    assert owner_id == actor.actor_id
    assert created.title == "Imported policy"
    assert actor_type == "user"
    assert uow.commits == 1
    assert uow.rollbacks == 0
