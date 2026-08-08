"""SK-B / B11 — exact guideline revisions in default board templates.

The suite exercises the Core contract independently from Community transport:
native closed writes, compatibility aliases, dual head/default projection,
copy-on-write template upgrades, no-drift board materialization, and retirement.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.models.schemas import (
    BoardCreate,
    BoardSettings,
    GuidelineCreate,
    GuidelineUpdate,
)
from okto_pulse.core.ports.default_board_configuration import (
    DEFAULT_GUIDELINE_REF_NATIVE_FIELDS,
)
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)
from okto_pulse.core.services.default_board_config_api import (
    DefaultBoardConfigApiService,
)
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService, GuidelineService

pytestmark = pytest.mark.asyncio

USER_ID = "skb-b11-core-user"


def _scope() -> str:
    return f"skb-b11-{uuid.uuid4().hex[:10]}"


async def _guideline(db, title: str):
    return await GuidelineService(db).create_guideline(
        USER_ID,
        GuidelineCreate(
            title=title,
            content="revision-1",
            scope="global",
            board_id=None,
        ),
    )


async def _next_revision(db, guideline, content: str):
    updated = await GuidelineService(db).update_guideline(
        guideline.id,
        USER_ID,
        GuidelineUpdate(content=content),
    )
    assert updated is not None
    return updated


def _native_ref(guideline, *, priority: int | None = None) -> dict:
    ref = {
        "guideline_id": guideline.id,
        "revision_id": guideline.revision_id,
        "revision_number": guideline.version,
        "semantic_version": guideline.semantic_version,
        "revision_digest": guideline.revision_digest,
    }
    if priority is not None:
        ref["priority"] = priority
    return ref


async def test_native_write_persists_only_the_closed_exact_pin():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        guideline = await _guideline(db, "Closed native ref")
        template = await DefaultBoardConfigurationService(db).create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=_scope(),
            guideline_default_refs=[_native_ref(guideline)],
        )

        assert template.guideline_default_refs is not None
        persisted = template.guideline_default_refs[0]
        assert set(persisted) == DEFAULT_GUIDELINE_REF_NATIVE_FIELDS
        assert persisted["priority"] == 0
        assert persisted["revision_id"] == guideline.revision_id
        assert "guideline_version" not in persisted


async def test_native_incomplete_unknown_and_duplicate_refs_write_nothing():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        service = DefaultBoardConfigurationService(db)
        guideline = await _guideline(db, "Rejected refs")
        draft = await service.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=scope,
        )

        cases = (
            (
                [{"guideline_id": guideline.id}],
                "default_guideline_pin_incomplete",
            ),
            (
                [{**_native_ref(guideline), "head_revision": {}}],
                "default_guideline_ref_invalid",
            ),
            (
                [_native_ref(guideline), _native_ref(guideline, priority=9)],
                "default_guideline_duplicate",
            ),
        )
        for refs, expected_code in cases:
            with pytest.raises(DefaultBoardConfigurationError) as exc:
                await service.update_guideline_default_refs(
                    draft.id,
                    refs,
                    USER_ID,
                )
            assert exc.value.code == expected_code

        reloaded = await service._require(draft.id)
        assert reloaded.guideline_default_refs is None
        assert [item.id for item in await service.list_versions(scope)] == [draft.id]


async def test_compatibility_alias_is_explicit_and_retained_only_when_supplied():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        guideline = await _guideline(db, "Legacy import")
        template = await DefaultBoardConfigurationService(db).create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=_scope(),
            guideline_default_refs=[
                {
                    "guideline_id": guideline.id,
                    "priority": 3,
                    "guideline_version": guideline.version,
                }
            ],
            compatibility_import=True,
        )

        assert template.guideline_default_refs is not None
        persisted = template.guideline_default_refs[0]
        assert set(persisted) == {
            *DEFAULT_GUIDELINE_REF_NATIVE_FIELDS,
            "guideline_version",
        }
        assert all(value is not None for value in persisted.values())
        assert persisted["guideline_version"] == guideline.version
        assert persisted["revision_id"] == guideline.revision_id
        assert persisted["semantic_version"] == guideline.semantic_version
        assert persisted["revision_digest"] == guideline.revision_digest


async def test_compatibility_preview_normalizes_alias_before_diffing_exact_pin():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.default_board_configuration import (
        guideline_ref_diff_has_changes,
    )

    async with get_session_factory()() as db:
        scope = _scope()
        guideline = await _guideline(db, "Equivalent compatibility preview")
        service = DefaultBoardConfigurationService(db)
        await service.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=scope,
            guideline_default_refs=[_native_ref(guideline, priority=3)],
            activate=True,
        )

        diff = await service.preview_create_guideline_ref_diff(
            scope=scope,
            guideline_default_refs=[
                {
                    "guideline_id": guideline.id,
                    "priority": 3,
                    "guideline_version": guideline.version,
                }
            ],
            compatibility_import=True,
        )

        assert guideline_ref_diff_has_changes(diff) is False
        assert diff == {"added": [], "removed": [], "reordered": []}


async def test_board_config_import_is_the_only_explicit_compatibility_seam():
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.import_export import (
        ImportBoardConfigCommand,
        ImportBoardConfigUseCase,
    )
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID

    class _DefaultConfigSpy:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def preview_create_guideline_ref_diff(self, **kwargs):
            assert kwargs["compatibility_import"] is True
            return {
                "added": ["legacy-guideline"],
                "removed": [],
                "reordered": [],
            }

        async def create_version(self, **kwargs):
            self.calls.append(kwargs)
            return {}

    class _Uow:
        def __init__(self, service) -> None:
            self.services = SimpleNamespace(default_board_config=service)
            self.committed = False

        async def commit(self) -> None:
            self.committed = True

    service = _DefaultConfigSpy()
    uow = _Uow(service)
    actor = ActorContext(
        USER_ID,
        "rest",
        realm_id=LOCAL_REALM_ID,
        roles=("admin",),
        permissions=[
            "spec.entity.edit_fields",
            "guidelines.adoption.manage",
        ],
    )
    result = await ImportBoardConfigUseCase().execute(
        ImportBoardConfigCommand(
            items=[
                {
                    "scope": "global",
                    "guideline_default_refs": [
                        {
                            "guideline_id": "legacy-guideline",
                            "guideline_version": 7,
                        }
                    ],
                }
            ]
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    assert result.created == 1
    assert uow.committed is True
    assert len(service.calls) == 1
    call = service.calls[0]
    assert call["compatibility_import"] is True
    assert call["guideline_default_refs"] == [
        {
            "guideline_id": "legacy-guideline",
            "guideline_version": 7,
        }
    ]


async def test_candidates_separate_current_head_from_template_default_pin():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        guideline_v1 = await _guideline(db, "Dual projection")
        template = await DefaultBoardConfigurationService(db).create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=scope,
            guideline_default_refs=[_native_ref(guideline_v1, priority=4)],
            activate=True,
        )
        guideline_v2 = await _next_revision(db, guideline_v1, "revision-2")

        payload = await DefaultBoardConfigApiService(db).list_default_candidates(
            scope=scope,
            template_id=template.id,
        )
        candidate = next(
            item
            for item in payload["candidates"]
            if item["guideline_id"] == guideline_v1.id
        )

        assert candidate["head_revision"] == {
            "revision_id": guideline_v2.revision_id,
            "revision_number": guideline_v2.version,
            "semantic_version": guideline_v2.semantic_version,
            "revision_digest": guideline_v2.revision_digest,
        }
        assert candidate["default_revision"] == {
            "revision_id": guideline_v1.revision_id,
            "revision_number": guideline_v1.version,
            "semantic_version": guideline_v1.semantic_version,
            "revision_digest": guideline_v1.revision_digest,
        }
        assert candidate["revision_id"] == guideline_v2.revision_id
        assert candidate["is_default"] is True
        assert candidate["priority"] == 4
        assert candidate["retired"] is False
        assert candidate["eligible"] is True
        assert candidate["eligibility_reason"] is None

        persisted = (await DefaultBoardConfigurationService(db)._require(template.id))
        assert persisted.guideline_default_refs is not None
        assert (
            persisted.guideline_default_refs[0]["revision_id"]
            == guideline_v1.revision_id
        )


async def test_copy_on_write_and_board_materialization_never_drift():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        service = DefaultBoardConfigurationService(db)
        guideline_v1 = await _guideline(db, "No drift")
        template_v1 = await service.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope="global",
            guideline_default_refs=[_native_ref(guideline_v1, priority=2)],
            activate=True,
        )
        guideline_v2 = await _next_revision(db, guideline_v1, "revision-2")

        board_v1 = await BoardService(db).create_board(
            USER_ID,
            BoardCreate(name=f"skb-b11-v1-{uuid.uuid4().hex[:8]}"),
        )
        policy = require_relational_application_adapter().guideline_policy(db)
        binding_v1 = await policy.get_binding(
            board_id=board_v1.id,
            guideline_id=guideline_v1.id,
        )
        assert binding_v1 is not None
        assert binding_v1.revision_id == guideline_v1.revision_id
        assert binding_v1.priority == 2

        template_v2 = await service.update_guideline_default_refs(
            template_v1.id,
            [_native_ref(guideline_v2, priority=7)],
            USER_ID,
        )
        assert template_v2.id != template_v1.id
        assert template_v2.version == template_v1.version + 1
        assert template_v2.is_active is True

        board_v2 = await BoardService(db).create_board(
            USER_ID,
            BoardCreate(name=f"skb-b11-v2-{uuid.uuid4().hex[:8]}"),
        )
        binding_v2 = await policy.get_binding(
            board_id=board_v2.id,
            guideline_id=guideline_v1.id,
        )
        unchanged_v1 = await policy.get_binding(
            board_id=board_v1.id,
            guideline_id=guideline_v1.id,
        )
        assert binding_v2 is not None
        assert binding_v2.revision_id == guideline_v2.revision_id
        assert binding_v2.priority == 7
        assert unchanged_v1 is not None
        assert unchanged_v1.revision_id == guideline_v1.revision_id
        assert unchanged_v1.priority == 2

        old_template = await service._require(template_v1.id)
        assert old_template.guideline_default_refs is not None
        assert (
            old_template.guideline_default_refs[0]["revision_id"]
            == guideline_v1.revision_id
        )


async def test_retired_candidate_keeps_historical_default_pin_visible():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        service = DefaultBoardConfigurationService(db)
        guideline = await _guideline(db, "Retired default")
        template = await service.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            scope=scope,
            guideline_default_refs=[_native_ref(guideline, priority=1)],
            activate=True,
        )
        assert await GuidelineService(db).delete_guideline(
            guideline.id,
            USER_ID,
        )

        payload = await service.list_default_candidates(
            scope=scope,
            template_id=template.id,
        )
        candidate = next(
            item
            for item in payload["candidates"]
            if item["guideline_id"] == guideline.id
        )
        assert candidate["retired"] is True
        assert candidate["eligible"] is False
        assert candidate["eligibility_reason"] == "guideline_retired"
        assert candidate["is_default"] is True
        assert candidate["default_revision"]["revision_id"] == guideline.revision_id

        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await service.update_guideline_default_refs(
                template.id,
                [_native_ref(guideline)],
                USER_ID,
            )
        assert exc.value.code == "default_guideline_retired"
        assert len(await service.list_versions(scope)) == 1
