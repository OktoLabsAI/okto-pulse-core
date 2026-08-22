from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.entity_export import (
    GetEntityExportBundleCommand,
    GetEntityExportBundleUseCase,
)
from okto_pulse.core.domain.entity_export import (
    ENTITY_EXPORT_SECTION_CONTRACT_VERSION,
    EntityExportBundle,
    EntityExportContractError,
    EntityExportHistoryScope,
    EntityExportManifest,
    EntityExportSection,
    EntityExportSectionManifestEntry,
    EntityExportSectionStatus,
    EntityExportSubjectSnapshot,
    EntityExportType,
)
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import SpecDependencyRecord


NOW = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)


def _bundle(*, observed_at: datetime = NOW) -> EntityExportBundle:
    section = EntityExportSection(
        section_key="identity",
        schema_version=ENTITY_EXPORT_SECTION_CONTRACT_VERSION,
        payload={"title": "Deterministic export", "nested": [1, 2]},
    )
    return EntityExportBundle(
        subject=EntityExportSubjectSnapshot(
            board_id="board-1",
            entity_type=EntityExportType.SPEC,
            entity_id="spec-1",
            title="Deterministic export",
            status="draft",
            version=7,
            edition=3,
            captured_at=observed_at,
        ),
        history_scope=EntityExportHistoryScope.COMPLETE,
        sections=(section,),
        manifest=EntityExportManifest(
            entries=(
                EntityExportSectionManifestEntry(
                    section_key="identity",
                    status=EntityExportSectionStatus.INCLUDED,
                    schema_version=ENTITY_EXPORT_SECTION_CONTRACT_VERSION,
                    total_count=1,
                    included_count=1,
                    pagination_complete=True,
                    source_complete=True,
                    complete_for_actor=True,
                ),
            ),
            source_complete=True,
            complete_for_actor=True,
        ),
        generated_at=observed_at,
    )


def test_bundle_manifest_is_closed_and_snapshot_fingerprint_is_time_stable() -> None:
    first = _bundle()
    later = _bundle(observed_at=NOW + timedelta(minutes=5))

    assert first.complete_for_actor is True
    assert first.source_complete is True
    assert first.overall_state.value == "complete"
    assert first.snapshot_fingerprint == later.snapshot_fingerprint
    assert first.bundle_digest != later.bundle_digest
    assert first.to_dict()["manifest"]["entries"][0]["status"] == "included"


def test_permission_omission_never_leaks_counts() -> None:
    with pytest.raises(
        EntityExportContractError,
        match="entity_export_permission_omission_count_forbidden",
    ):
        EntityExportSectionManifestEntry(
            section_key="policy_compliance",
            status=EntityExportSectionStatus.OMITTED,
            reason_code="permission_denied",
            required_permission="spec.quality.read",
            total_count=4,
            complete_for_actor=False,
        )


def test_permission_omission_can_be_complete_for_actor_but_not_source() -> None:
    base = _bundle()
    redacted = EntityExportBundle(
        subject=base.subject,
        history_scope=base.history_scope,
        sections=base.sections,
        manifest=EntityExportManifest(
            entries=(
                *base.manifest.entries,
                EntityExportSectionManifestEntry(
                    section_key="policy_compliance",
                    status=EntityExportSectionStatus.OMITTED,
                    reason_code="permission_denied",
                    required_permission="spec.quality.read",
                    source_complete=False,
                    complete_for_actor=True,
                ),
            ),
            source_complete=False,
            complete_for_actor=True,
        ),
        generated_at=NOW,
    )
    assert redacted.complete_for_actor is True
    assert redacted.source_complete is False
    assert redacted.overall_state.value == "redacted"


def test_dependency_history_snapshots_are_nullable_and_serialized_when_known() -> None:
    legacy = SpecDependencyRecord(
        id="dep-1",
        board_id="board-1",
        source_spec_id="spec-1",
        target_spec_id="spec-2",
        created_at=NOW,
        created_by="actor-1",
        source_version_on_create=4,
        source_status_on_create=SpecStatus.DRAFT,
        target_status_on_create=SpecStatus.DONE,
        target_version_on_create=8,
        resolved_on_create=True,
    )
    assert "source_title_on_create" not in legacy.to_dict()

    sealed = replace(
        legacy,
        source_title_on_create="Dependent",
        source_edition_on_create=2,
        target_title_on_create="Prerequisite at creation",
        target_edition_on_create=4,
        source_title_on_remove="Renamed dependent",
        source_edition_on_remove=3,
        target_title_on_remove="Prerequisite",
        target_edition_on_remove=5,
    ).to_dict()
    assert sealed["source_title_on_create"] == "Dependent"
    assert sealed["source_edition_on_create"] == 2
    assert sealed["target_title_on_create"] == "Prerequisite at creation"
    assert sealed["target_edition_on_create"] == 4
    assert sealed["target_title_on_remove"] == "Prerequisite"
    assert sealed["target_edition_on_remove"] == 5


class _Boards:
    def __init__(self, board: object, operations: list[str]) -> None:
        self._board = board
        self._operations = operations

    async def get(self, board_id: str) -> object | None:
        self._operations.append(f"board:{board_id}")
        return self._board


class _Reader:
    def __init__(self, operations: list[str]) -> None:
        self.operations = operations
        self.calls = 0

    async def build_bundle(self, **kwargs: object) -> EntityExportBundle:
        self.calls += 1
        self.operations.append("reader")
        disclosure = kwargs["disclosure"]
        assert disclosure.allows("spec.entity.read")
        return _bundle()


class _Uow:
    def __init__(self, *, owner_id: str = "actor-1") -> None:
        self.operations: list[str] = []
        self.realm_scope = RealmScope.local()
        self.boards = _Boards(
            SimpleNamespace(
                id="board-1",
                owner_id=owner_id,
                realm_id=self.realm_scope.realm_id,
            ),
            self.operations,
        )
        self.services = SimpleNamespace(shares=object())
        self.entity_exports = _Reader(self.operations)

    async def begin_consistent_read(self) -> None:
        self.operations.append("snapshot")


@pytest.mark.asyncio
async def test_use_case_starts_snapshot_before_access_and_calls_bound_reader() -> None:
    uow = _Uow()
    result = await GetEntityExportBundleUseCase().execute(
        GetEntityExportBundleCommand(
            board_id="board-1",
            entity_type=EntityExportType.SPEC,
            entity_id="spec-1",
        ),
        actor=ActorContext(
            "actor-1",
            "rest",
            board_id="board-1",
            realm_scope=RealmScope.local(),
            permissions={"spec": {"entity": {"read": True}}},
        ),
        uow=uow,
    )

    assert result.bundle.subject.entity_id == "spec-1"
    assert uow.operations == ["snapshot", "board:board-1", "reader"]


@pytest.mark.asyncio
async def test_root_denial_is_non_enumerable_and_never_calls_reader() -> None:
    uow = _Uow()
    with pytest.raises(EntityNotFoundError):
        await GetEntityExportBundleUseCase().execute(
            GetEntityExportBundleCommand(
                board_id="board-1",
                entity_type=EntityExportType.SPEC,
                entity_id="spec-secret",
            ),
            actor=ActorContext(
                "actor-1",
                "rest",
                board_id="board-1",
                realm_scope=RealmScope.local(),
                permissions=[],
            ),
            uow=uow,
        )

    assert uow.entity_exports.calls == 0
    assert uow.operations == ["snapshot", "board:board-1"]
