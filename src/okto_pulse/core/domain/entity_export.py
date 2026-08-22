"""Renderer-neutral, versioned contracts for complete entity exports.

The bundle is the single canonical read model consumed by Markdown, HTML and
future renderers.  It deliberately carries a manifest for every considered
section: an absent payload is never ambiguous with an empty, permission-
filtered, unsupported or failed section.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from okto_pulse.core.domain.quality_canonicalization import (
    canonical_sha256,
    normalize_canonical_value,
)


ENTITY_EXPORT_BUNDLE_CONTRACT_VERSION = "entity-export-bundle/v1"
ENTITY_EXPORT_MANIFEST_CONTRACT_VERSION = "entity-export-manifest/v1"
ENTITY_EXPORT_SECTION_CONTRACT_VERSION = "entity-export-section/v1"

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_VERSION_RE = re.compile(r"^[a-z][a-z0-9_-]*/v[1-9][0-9]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EntityExportContractError(ValueError):
    """A producer attempted to build a non-canonical export bundle."""


class EntityExportType(str, Enum):
    STORY = "story"
    IDEATION = "ideation"
    REFINEMENT = "refinement"
    SPEC = "spec"
    SPRINT = "sprint"
    CARD = "card"
    TEST_SCENARIO = "test_scenario"


class EntityExportHistoryScope(str, Enum):
    CURRENT = "current"
    COMPLETE = "complete"


class EntityExportSectionStatus(str, Enum):
    INCLUDED = "included"
    EMPTY = "empty"
    OMITTED = "omitted"
    NOT_APPLICABLE = "not_applicable"
    UNAVAILABLE = "unavailable"
    ERROR = "error"


class EntityExportOverallState(str, Enum):
    COMPLETE = "complete"
    REDACTED = "redacted"
    PARTIAL = "partial"


def _required_text(value: object, code: str, *, maximum: int = 4096) -> str:
    normalized = value.strip() if isinstance(value, str) else ""
    if not normalized or len(normalized) > maximum:
        raise EntityExportContractError(code)
    return normalized


def _optional_text(
    value: object,
    code: str,
    *,
    maximum: int = 4096,
) -> str | None:
    if value is None:
        return None
    return _required_text(value, code, maximum=maximum)


def _key(value: object, code: str) -> str:
    normalized = _required_text(value, code, maximum=128)
    if not _KEY_RE.fullmatch(normalized):
        raise EntityExportContractError(code)
    return normalized


def _contract_version(value: object, code: str) -> str:
    normalized = _required_text(value, code, maximum=128)
    if not _VERSION_RE.fullmatch(normalized):
        raise EntityExportContractError(code)
    return normalized


def _aware_utc(value: object, code: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise EntityExportContractError(code)
    return value.astimezone(timezone.utc)


def _freeze(value: Any) -> Any:
    normalized = normalize_canonical_value(value)
    if isinstance(normalized, dict):
        return MappingProxyType({key: _freeze(item) for key, item in normalized.items()})
    if isinstance(normalized, list):
        return tuple(_freeze(item) for item in normalized)
    return normalized


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class EntityExportRequest:
    board_id: str
    entity_type: EntityExportType
    entity_id: str
    history_scope: EntityExportHistoryScope = EntityExportHistoryScope.COMPLETE
    requested_sections: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _required_text(self.board_id, "entity_export_board_id_required"))
        if not isinstance(self.entity_type, EntityExportType):
            raise EntityExportContractError("entity_export_type_invalid")
        object.__setattr__(self, "entity_id", _required_text(self.entity_id, "entity_export_entity_id_required"))
        if not isinstance(self.history_scope, EntityExportHistoryScope):
            raise EntityExportContractError("entity_export_history_scope_invalid")
        sections = tuple(_key(item, "entity_export_requested_section_invalid") for item in self.requested_sections)
        if len(set(sections)) != len(sections):
            raise EntityExportContractError("entity_export_requested_section_duplicate")
        object.__setattr__(self, "requested_sections", sections)


@dataclass(frozen=True, slots=True)
class EntityExportDisclosure:
    """Permission decisions resolved by Core before an edition reads sections."""

    granted_permissions: frozenset[str]
    requested_sections: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.granted_permissions, frozenset):
            object.__setattr__(self, "granted_permissions", frozenset(self.granted_permissions))
        normalized_permissions = frozenset(
            _required_text(item, "entity_export_permission_invalid", maximum=256)
            for item in self.granted_permissions
        )
        object.__setattr__(self, "granted_permissions", normalized_permissions)
        sections = tuple(_key(item, "entity_export_requested_section_invalid") for item in self.requested_sections)
        if len(set(sections)) != len(sections):
            raise EntityExportContractError("entity_export_requested_section_duplicate")
        object.__setattr__(self, "requested_sections", sections)

    def allows(self, permission: str | None) -> bool:
        return permission is None or permission in self.granted_permissions


@dataclass(frozen=True, slots=True)
class EntityExportSubjectSnapshot:
    board_id: str
    entity_type: EntityExportType
    entity_id: str
    title: str
    status: str
    captured_at: datetime
    version: int | None = None
    edition: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _required_text(self.board_id, "entity_export_subject_board_id_required"))
        if not isinstance(self.entity_type, EntityExportType):
            raise EntityExportContractError("entity_export_subject_type_invalid")
        object.__setattr__(self, "entity_id", _required_text(self.entity_id, "entity_export_subject_id_required"))
        object.__setattr__(self, "title", _required_text(self.title, "entity_export_subject_title_required", maximum=16_384))
        object.__setattr__(self, "status", _required_text(self.status, "entity_export_subject_status_required", maximum=128))
        object.__setattr__(self, "captured_at", _aware_utc(self.captured_at, "entity_export_subject_captured_at_invalid"))
        for field_name in ("version", "edition"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 1):
                raise EntityExportContractError(f"entity_export_subject_{field_name}_invalid")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "board_id": self.board_id,
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "title": self.title,
            "status": self.status,
            "captured_at": self.captured_at.isoformat(),
        }
        if self.version is not None:
            payload["version"] = self.version
        if self.edition is not None:
            payload["edition"] = self.edition
        return payload


@dataclass(frozen=True, slots=True)
class EntityExportSectionManifestEntry:
    section_key: str
    status: EntityExportSectionStatus
    complete_for_actor: bool
    source_complete: bool | None = None
    schema_version: str | None = None
    reason_code: str | None = None
    required_permission: str | None = None
    total_count: int | None = None
    included_count: int | None = None
    pagination_complete: bool | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "section_key", _key(self.section_key, "entity_export_section_key_invalid"))
        if not isinstance(self.status, EntityExportSectionStatus):
            raise EntityExportContractError("entity_export_section_status_invalid")
        if not isinstance(self.complete_for_actor, bool):
            raise EntityExportContractError("entity_export_section_actor_completeness_invalid")
        if self.source_complete is not None and not isinstance(self.source_complete, bool):
            raise EntityExportContractError("entity_export_section_source_completeness_invalid")
        if self.schema_version is not None:
            object.__setattr__(self, "schema_version", _contract_version(self.schema_version, "entity_export_section_schema_version_invalid"))
        object.__setattr__(self, "reason_code", _optional_text(self.reason_code, "entity_export_section_reason_invalid", maximum=128))
        object.__setattr__(self, "required_permission", _optional_text(self.required_permission, "entity_export_section_permission_invalid", maximum=256))
        for field_name in ("total_count", "included_count"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
                raise EntityExportContractError(f"entity_export_section_{field_name}_invalid")
        if self.total_count is not None and self.included_count is not None and self.included_count > self.total_count:
            raise EntityExportContractError("entity_export_section_count_inconsistent")
        if self.pagination_complete is not None and not isinstance(self.pagination_complete, bool):
            raise EntityExportContractError("entity_export_section_pagination_invalid")
        carries_payload = self.status in {EntityExportSectionStatus.INCLUDED, EntityExportSectionStatus.EMPTY}
        if carries_payload and self.schema_version is None:
            raise EntityExportContractError("entity_export_section_schema_version_required")
        if self.status is EntityExportSectionStatus.EMPTY and any(value not in (None, 0) for value in (self.total_count, self.included_count)):
            raise EntityExportContractError("entity_export_empty_section_count_invalid")
        if self.reason_code == "permission_denied":
            if self.status is not EntityExportSectionStatus.OMITTED or self.required_permission is None:
                raise EntityExportContractError("entity_export_permission_omission_invalid")
            if self.total_count is not None or self.included_count is not None:
                raise EntityExportContractError("entity_export_permission_omission_count_forbidden")
        if (
            self.status
            in {
                EntityExportSectionStatus.OMITTED,
                EntityExportSectionStatus.UNAVAILABLE,
                EntityExportSectionStatus.ERROR,
            }
            and self.reason_code is None
        ):
            raise EntityExportContractError("entity_export_section_reason_required")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "section_key": self.section_key,
            "status": self.status.value,
            "complete_for_actor": self.complete_for_actor,
        }
        for key in (
            "source_complete",
            "schema_version",
            "reason_code",
            "required_permission",
            "total_count",
            "included_count",
            "pagination_complete",
        ):
            value = getattr(self, key)
            if value is not None:
                payload[key] = value
        return payload


@dataclass(frozen=True, slots=True)
class EntityExportSection:
    section_key: str
    schema_version: str
    payload: Mapping[str, Any]
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "section_key", _key(self.section_key, "entity_export_section_key_invalid"))
        object.__setattr__(self, "schema_version", _contract_version(self.schema_version, "entity_export_section_schema_version_invalid"))
        if not isinstance(self.payload, Mapping):
            raise EntityExportContractError("entity_export_section_payload_invalid")
        frozen = _freeze(self.payload)
        object.__setattr__(self, "payload", frozen)
        object.__setattr__(self, "digest", canonical_sha256({"section_key": self.section_key, "schema_version": self.schema_version, "payload": frozen}))

    def to_dict(self) -> dict[str, Any]:
        return {
            "section_key": self.section_key,
            "schema_version": self.schema_version,
            "payload": _thaw(self.payload),
            "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class EntityExportManifest:
    entries: tuple[EntityExportSectionManifestEntry, ...]
    source_complete: bool
    complete_for_actor: bool
    contract_version: str = ENTITY_EXPORT_MANIFEST_CONTRACT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_version", _contract_version(self.contract_version, "entity_export_manifest_version_invalid"))
        entries = tuple(self.entries)
        if not entries or any(not isinstance(item, EntityExportSectionManifestEntry) for item in entries):
            raise EntityExportContractError("entity_export_manifest_entries_invalid")
        if len({item.section_key for item in entries}) != len(entries):
            raise EntityExportContractError("entity_export_manifest_section_duplicate")
        if not isinstance(self.source_complete, bool) or not isinstance(self.complete_for_actor, bool):
            raise EntityExportContractError("entity_export_manifest_completeness_invalid")
        if self.source_complete and any(item.source_complete is False for item in entries):
            raise EntityExportContractError("entity_export_manifest_source_completeness_inconsistent")
        if self.complete_for_actor and any(not item.complete_for_actor for item in entries):
            raise EntityExportContractError("entity_export_manifest_actor_completeness_inconsistent")
        object.__setattr__(self, "entries", entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "source_complete": self.source_complete,
            "complete_for_actor": self.complete_for_actor,
            "entries": [item.to_dict() for item in self.entries],
        }


@dataclass(frozen=True, slots=True)
class EntityExportBundle:
    subject: EntityExportSubjectSnapshot
    history_scope: EntityExportHistoryScope
    sections: tuple[EntityExportSection, ...]
    manifest: EntityExportManifest
    generated_at: datetime
    contract_version: str = ENTITY_EXPORT_BUNDLE_CONTRACT_VERSION
    snapshot_fingerprint: str = field(init=False)
    bundle_digest: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_version", _contract_version(self.contract_version, "entity_export_bundle_version_invalid"))
        if not isinstance(self.subject, EntityExportSubjectSnapshot):
            raise EntityExportContractError("entity_export_subject_invalid")
        if not isinstance(self.history_scope, EntityExportHistoryScope):
            raise EntityExportContractError("entity_export_history_scope_invalid")
        if not isinstance(self.manifest, EntityExportManifest):
            raise EntityExportContractError("entity_export_manifest_invalid")
        sections = tuple(self.sections)
        if any(not isinstance(item, EntityExportSection) for item in sections):
            raise EntityExportContractError("entity_export_sections_invalid")
        section_map = {item.section_key: item for item in sections}
        if len(section_map) != len(sections):
            raise EntityExportContractError("entity_export_section_duplicate")
        manifest_map = {item.section_key: item for item in self.manifest.entries}
        payload_keys = {
            key
            for key, item in manifest_map.items()
            if item.status in {EntityExportSectionStatus.INCLUDED, EntityExportSectionStatus.EMPTY}
        }
        if set(section_map) != payload_keys:
            raise EntityExportContractError("entity_export_manifest_payload_mismatch")
        for key, section in section_map.items():
            if manifest_map[key].schema_version != section.schema_version:
                raise EntityExportContractError("entity_export_section_version_mismatch")
        object.__setattr__(self, "sections", sections)
        object.__setattr__(self, "generated_at", _aware_utc(self.generated_at, "entity_export_generated_at_invalid"))
        object.__setattr__(
            self,
            "snapshot_fingerprint",
            canonical_sha256(self._snapshot_fingerprint_payload()),
        )
        object.__setattr__(self, "bundle_digest", canonical_sha256(self._digest_payload()))

    @property
    def source_complete(self) -> bool:
        return self.manifest.source_complete

    @property
    def complete_for_actor(self) -> bool:
        return self.manifest.complete_for_actor

    @property
    def overall_state(self) -> EntityExportOverallState:
        if self.source_complete and self.complete_for_actor:
            return EntityExportOverallState.COMPLETE
        if self.complete_for_actor and any(
            item.reason_code == "permission_denied"
            for item in self.manifest.entries
        ):
            return EntityExportOverallState.REDACTED
        return EntityExportOverallState.PARTIAL

    def _digest_payload(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "subject": self.subject.to_dict(),
            "history_scope": self.history_scope.value,
            "sections": [item.to_dict() for item in self.sections],
            "manifest": self.manifest.to_dict(),
            "generated_at": self.generated_at.isoformat(),
            "source_complete": self.source_complete,
            "complete_for_actor": self.complete_for_actor,
            "overall_state": self.overall_state.value,
        }

    def _snapshot_fingerprint_payload(self) -> dict[str, Any]:
        """Stable optimistic fence, excluding observation timestamps."""

        return {
            "contract_version": self.contract_version,
            "subject": {
                "board_id": self.subject.board_id,
                "entity_type": self.subject.entity_type.value,
                "entity_id": self.subject.entity_id,
                "version": self.subject.version,
                "edition": self.subject.edition,
            },
            "history_scope": self.history_scope.value,
            "section_digests": [
                {"section_key": item.section_key, "digest": item.digest}
                for item in sorted(self.sections, key=lambda value: value.section_key)
            ],
            "manifest": {
                **self.manifest.to_dict(),
                "entries": [
                    item.to_dict()
                    for item in sorted(
                        self.manifest.entries,
                        key=lambda value: value.section_key,
                    )
                ],
            },
            "source_complete": self.source_complete,
            "complete_for_actor": self.complete_for_actor,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._digest_payload(),
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "bundle_digest": self.bundle_digest,
        }


__all__ = [
    "ENTITY_EXPORT_BUNDLE_CONTRACT_VERSION",
    "ENTITY_EXPORT_MANIFEST_CONTRACT_VERSION",
    "ENTITY_EXPORT_SECTION_CONTRACT_VERSION",
    "EntityExportBundle",
    "EntityExportContractError",
    "EntityExportDisclosure",
    "EntityExportHistoryScope",
    "EntityExportManifest",
    "EntityExportOverallState",
    "EntityExportRequest",
    "EntityExportSection",
    "EntityExportSectionManifestEntry",
    "EntityExportSectionStatus",
    "EntityExportSubjectSnapshot",
    "EntityExportType",
]
