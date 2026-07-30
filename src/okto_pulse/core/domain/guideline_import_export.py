"""Transport-free ``guideline-export/v2`` codec and atomic import planner.

The contract deliberately keeps persistence and transport concerns outside the
domain.  It serializes the complete immutable guideline aggregate, validates
the complete envelope before exposing a plan, and never authorizes an
overwrite.  A persistence adapter may apply a non-dry-run, conflict-free plan
inside one unit of work.

Legacy ``schema_version=1`` guideline envelopes are accepted only through the
dispatcher.  Because v1 did not carry stable identities, immutable history, or
executable rules, every item is converted into a deterministic contextual
``1.0.0`` baseline with no bindings.  Blocking-like legacy flags are recorded
as diagnostics but never become executable policy.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelineLifecycleError,
    guideline_revision_content_digest_v1,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_BINDING_ORIGIN_MAX_LENGTH,
    GUIDELINE_BINDING_SOURCE_KIND_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_LEGACY_VERSION_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineContextScope,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRule,
    GuidelineRuleOperator,
    GuidelineScope,
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    PolicyEntityType,
    normalize_guideline_semantic_version,
    normalize_guideline_sha256,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_json_bytes


GUIDELINE_EXPORT_CONTRACT_VERSION = "guideline-export/v2"
GUIDELINE_EXPORT_SCHEMA_VERSION = "2"
GUIDELINE_EXPORT_KIND = "guidelines"
GUIDELINE_EXPORT_LEGACY_SCHEMA_VERSION = "1"
GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION = "1.0.0"
GUIDELINE_EXPORT_LEGACY_OWNER = "legacy-import"
GUIDELINE_EXPORT_LEGACY_ACTOR = "legacy-import"

_SHA256_LENGTH = 64
_LEGACY_ENVELOPE_REQUIRED_FIELDS = frozenset({"schema_version", "kind", "items"})
_LEGACY_ENVELOPE_OPTIONAL_FIELDS = frozenset({"exported_at"})
_LEGACY_ITEM_FIELDS = frozenset(
    {
        "title",
        "content",
        "tags",
        "scope",
        "board_id",
        "legacy_version",
        "version",
        "blocking",
        "is_blocking",
        "enforcement",
        "rules",
    }
)


class GuidelineImportExportError(ValueError):
    """A closed export/import contract invariant was violated."""

    def __init__(
        self,
        code: str,
        *,
        path: str | None = None,
        message: str | None = None,
    ) -> None:
        self.code = code
        self.path = path
        self.message = message or code
        rendered = self.message if path is None else f"{self.message} at {path}"
        super().__init__(rendered)


class GuidelineHistoryStatus(str, Enum):
    """How faithfully one exported aggregate represents source history."""

    COMPLETE = "complete"
    BASELINE_ONLY = "baseline_only"


class GuidelineImportRevisionDisposition(str, Enum):
    CREATE = "create"
    SKIP_IDENTICAL = "skip_identical"
    CONFLICT = "conflict"


class GuidelineImportBindingDisposition(str, Enum):
    """How an adapter must materialize imported binding history."""

    PENDING_ADOPTION = "pending_adoption"
    STORE_INERT_HISTORY = "store_inert_history"
    SKIP_IDENTICAL_HISTORY = "skip_identical_history"
    NO_BINDINGS = "no_bindings"


class GuidelineImportTransactionStatus(str, Enum):
    PLANNED = "planned"
    DRY_RUN = "dry_run"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


class GuidelineBindingMaterialization(str, Enum):
    LIVE = "live"
    CANDIDATE = "candidate"


def canonical_guideline_json_bytes(value: object) -> bytes:
    """Return compact, sorted-key, NFC-normalized UTF-8 canonical JSON."""

    try:
        return canonical_json_bytes(value)
    except (TypeError, ValueError) as exc:
        raise GuidelineImportExportError(
            "guideline_export_canonicalization_invalid"
        ) from exc


def canonical_guideline_sha256(value: object) -> str:
    return hashlib.sha256(canonical_guideline_json_bytes(value)).hexdigest()


def _required_text(value: object, code: str, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelineImportExportError(code, path=path)
    return value.strip()


def _optional_text(value: object, code: str, path: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code, path)


def _bounded_text(
    value: object,
    code: str,
    path: str,
    *,
    max_length: int,
) -> str:
    normalized = _required_text(value, code, path)
    if len(normalized) > max_length:
        raise GuidelineImportExportError(code, path=path)
    return normalized


def _optional_bounded_text(
    value: object,
    code: str,
    path: str,
    *,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, code, path, max_length=max_length)


def _strict_bool(value: object, code: str, path: str) -> bool:
    if not isinstance(value, bool):
        raise GuidelineImportExportError(code, path=path)
    return value


def _strict_int(
    value: object,
    code: str,
    path: str,
    *,
    minimum: int = 0,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > POLICY_SQL_INTEGER_MAX
    ):
        raise GuidelineImportExportError(code, path=path)
    return value


def _mapping(value: object, code: str, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GuidelineImportExportError(code, path=path)
    if any(not isinstance(key, str) for key in value):
        raise GuidelineImportExportError(code, path=path)
    return value


def _sequence(value: object, code: str, path: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        str | bytes | bytearray,
    ):
        raise GuidelineImportExportError(code, path=path)
    return value


def _closed(
    value: Mapping[str, Any],
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
    path: str,
) -> None:
    fields = set(value)
    missing = required - fields
    if missing:
        raise GuidelineImportExportError(
            "guideline_export_field_required",
            path=f"{path}.{sorted(missing)[0]}",
        )
    unknown = fields - required - optional
    if unknown:
        raise GuidelineImportExportError(
            "guideline_export_unknown_field",
            path=f"{path}.{sorted(unknown)[0]}",
        )


def _aware_utc(value: object, code: str, path: str) -> datetime:
    if not isinstance(value, datetime):
        raise GuidelineImportExportError(code, path=path)
    if value.tzinfo is None or value.utcoffset() is None:
        raise GuidelineImportExportError(code, path=path)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: object, code: str, path: str) -> datetime:
    raw = _required_text(value, code, path)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GuidelineImportExportError(code, path=path) from exc
    return _aware_utc(parsed, code, path)


def _datetime_payload(value: datetime) -> str:
    return (
        _aware_utc(
            value,
            "guideline_export_datetime_invalid",
            "$",
        )
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _enum_value(
    value: object,
    enum_type: type[Enum],
    code: str,
    path: str,
) -> Enum:
    if not isinstance(value, str):
        raise GuidelineImportExportError(code, path=path)
    try:
        return enum_type(value)
    except ValueError as exc:
        raise GuidelineImportExportError(code, path=path) from exc


def _domain_error(exc: ValueError, path: str) -> GuidelineImportExportError:
    code = getattr(exc, "code", "guideline_export_domain_value_invalid")
    return GuidelineImportExportError(str(code), path=path)


@dataclass(frozen=True, slots=True)
class GuidelineExportRevision:
    """One immutable revision plus honest legacy provenance."""

    revision: GuidelineRevision
    published_head_revision: int | None = None
    published_head_updated_at: datetime | None = None
    legacy_version: str | None = None
    legacy_version_unresolvable: bool = False
    legacy_tags: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.revision, GuidelineRevision):
            raise GuidelineImportExportError("guideline_export_revision_invalid")
        published_head_revision = (
            self.revision.revision_number
            if self.published_head_revision is None
            else _strict_int(
                self.published_head_revision,
                "guideline_export_published_head_revision_invalid",
                "$.published_head_revision",
                minimum=1,
            )
        )
        if published_head_revision != self.revision.revision_number:
            raise GuidelineImportExportError(
                "guideline_export_published_head_revision_invalid"
            )
        published_head_updated_at = _aware_utc(
            (
                self.revision.created_at
                if self.published_head_updated_at is None
                else self.published_head_updated_at
            ),
            "guideline_export_published_head_updated_at_invalid",
            "$.published_head_updated_at",
        )
        if published_head_updated_at < self.revision.created_at:
            raise GuidelineImportExportError(
                "guideline_export_published_head_time_before_revision",
                path="$.published_head_updated_at",
            )
        legacy_version = _optional_bounded_text(
            self.legacy_version,
            "guideline_export_legacy_version_invalid",
            "$.legacy_version",
            max_length=GUIDELINE_LEGACY_VERSION_MAX_LENGTH,
        )
        if not isinstance(self.legacy_version_unresolvable, bool):
            raise GuidelineImportExportError(
                "guideline_export_legacy_resolution_invalid"
            )
        if self.legacy_version_unresolvable and legacy_version is None:
            raise GuidelineImportExportError(
                "guideline_export_unresolvable_legacy_version_required"
            )
        if legacy_version is not None and not self.legacy_version_unresolvable:
            raise GuidelineImportExportError(
                "guideline_export_legacy_version_resolution_invalid"
            )
        if self.legacy_tags is None:
            legacy_tags = None
        else:
            if not isinstance(self.legacy_tags, tuple | list):
                raise GuidelineImportExportError("guideline_export_legacy_tags_invalid")
            legacy_tags = tuple(
                sorted(
                    {
                        _required_text(
                            item,
                            "guideline_export_legacy_tag_invalid",
                            "$.legacy_tags",
                        )
                        for item in self.legacy_tags
                    }
                )
            )
        if legacy_tags is not None and not self.legacy_version_unresolvable:
            raise GuidelineImportExportError(
                "guideline_export_legacy_tags_resolution_invalid"
            )
        object.__setattr__(
            self,
            "published_head_revision",
            published_head_revision,
        )
        object.__setattr__(
            self,
            "published_head_updated_at",
            published_head_updated_at,
        )
        object.__setattr__(self, "legacy_version", legacy_version)
        object.__setattr__(self, "legacy_tags", legacy_tags)

    @property
    def revision_id(self) -> str:
        return self.revision.revision_id

    @property
    def semantic_version(self) -> str:
        return self.revision.semantic_version

    @property
    def revision_digest(self) -> str:
        return self.revision.content_digest

    @property
    def legacy_version_as_int(self) -> int | None:
        """Compatibility projection without losing the textual source value."""

        if self.legacy_version is None or not self.legacy_version.isascii():
            return None
        if not self.legacy_version.isdigit():
            return None
        parsed = int(self.legacy_version)
        return parsed if parsed <= POLICY_SQL_INTEGER_MAX else None


@dataclass(frozen=True, slots=True)
class GuidelineExportBinding:
    """Logical binding history plus physical provenance and evidence."""

    binding: BoardGuidelineBinding
    physical_source_kind: str
    binding_origin: str
    materialization: GuidelineBindingMaterialization
    legacy_source_id: str | None = None
    legacy_guideline_version: str | None = None
    legacy_template_id: str | None = None
    legacy_template_version: str | None = None
    legacy_version_unresolvable: bool = False
    evidence_refs: tuple[tuple[str, str], ...] = ()
    binding_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.binding, BoardGuidelineBinding):
            raise GuidelineImportExportError(
                "guideline_export_binding_snapshot_invalid"
            )
        for field_name, max_length in (
            ("physical_source_kind", GUIDELINE_BINDING_SOURCE_KIND_MAX_LENGTH),
            ("binding_origin", GUIDELINE_BINDING_ORIGIN_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    f"guideline_export_binding_{field_name}_required",
                    f"$.{field_name}",
                    max_length=max_length,
                ),
            )
        if not isinstance(self.materialization, GuidelineBindingMaterialization):
            raise GuidelineImportExportError(
                "guideline_export_binding_materialization_invalid"
            )
        for field_name, max_length in (
            ("legacy_source_id", GUIDELINE_ID_MAX_LENGTH),
            ("legacy_guideline_version", GUIDELINE_LEGACY_VERSION_MAX_LENGTH),
            ("legacy_template_id", GUIDELINE_ID_MAX_LENGTH),
            ("legacy_template_version", GUIDELINE_LEGACY_VERSION_MAX_LENGTH),
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                value = str(value)
            object.__setattr__(
                self,
                field_name,
                _optional_bounded_text(
                    value,
                    f"guideline_export_binding_{field_name}_invalid",
                    f"$.{field_name}",
                    max_length=max_length,
                ),
            )
        if not isinstance(self.legacy_version_unresolvable, bool):
            raise GuidelineImportExportError(
                "guideline_export_binding_legacy_resolution_invalid"
            )
        if not isinstance(self.evidence_refs, tuple | list):
            raise GuidelineImportExportError(
                "guideline_export_binding_evidence_refs_invalid"
            )
        refs: list[tuple[str, str]] = []
        seen_ref_kinds: set[str] = set()
        for index, item in enumerate(self.evidence_refs):
            if not isinstance(item, tuple | list) or len(item) != 2:
                raise GuidelineImportExportError(
                    "guideline_export_binding_evidence_ref_invalid",
                    path=f"$.evidence_refs[{index}]",
                )
            kind = _required_text(
                item[0],
                "guideline_export_binding_evidence_kind_invalid",
                f"$.evidence_refs[{index}][0]",
            )
            reference = _required_text(
                item[1],
                "guideline_export_binding_evidence_value_invalid",
                f"$.evidence_refs[{index}][1]",
            )
            if kind in seen_ref_kinds:
                raise GuidelineImportExportError(
                    "guideline_export_binding_evidence_kind_duplicate"
                )
            seen_ref_kinds.add(kind)
            refs.append((kind, reference))
        refs_tuple = tuple(sorted(refs))
        expected_digest = canonical_guideline_sha256(
            self.digest_payload(include_digest=False)
            | {"evidence_refs": [list(item) for item in refs_tuple]}
        )
        if self.binding_digest is not None:
            try:
                provided_digest = normalize_guideline_sha256(
                    self.binding_digest,
                    "guideline_export_binding_digest_invalid",
                )
            except GuidelinePolicyContractError as exc:
                raise _domain_error(exc, "$.binding_digest") from exc
            if provided_digest != expected_digest:
                raise GuidelineImportExportError(
                    "guideline_export_binding_digest_mismatch"
                )
        object.__setattr__(self, "evidence_refs", refs_tuple)
        object.__setattr__(self, "binding_digest", expected_digest)

    def __getattr__(self, name: str) -> object:
        """Compatibility projection for immutable logical binding fields."""

        return getattr(self.binding, name)

    @property
    def candidate_key(self) -> tuple[str, str, str, int]:
        return (
            self.binding.board_id,
            self.binding.guideline_id,
            self.binding.binding_id,
            self.binding.binding_revision,
        )

    def digest_payload(self, *, include_digest: bool = True) -> dict[str, object]:
        binding = self.binding
        payload: dict[str, object] = {
            "binding": _binding_payload(binding),
            "physical_source_kind": self.physical_source_kind,
            "binding_origin": self.binding_origin,
            "materialization": self.materialization.value,
            "legacy_source_id": self.legacy_source_id,
            "legacy_guideline_version": self.legacy_guideline_version,
            "legacy_template_id": self.legacy_template_id,
            "legacy_template_version": self.legacy_template_version,
            "legacy_version_unresolvable": self.legacy_version_unresolvable,
            "evidence_refs": [list(item) for item in self.evidence_refs],
        }
        if include_digest:
            payload["binding_digest"] = self.binding_digest
        return payload


@dataclass(frozen=True, slots=True)
class GuidelineExportAggregate:
    """Complete logical aggregate exported by one consistent snapshot."""

    identity: Guideline
    revisions: tuple[GuidelineExportRevision, ...]
    head: GuidelineHead
    retirement: GuidelineRetirement | None = None
    bindings: tuple[GuidelineExportBinding, ...] = ()
    history_status: GuidelineHistoryStatus = GuidelineHistoryStatus.COMPLETE
    migration_notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.identity, Guideline):
            raise GuidelineImportExportError("guideline_export_identity_invalid")
        if not isinstance(self.head, GuidelineHead):
            raise GuidelineImportExportError("guideline_export_head_invalid")
        if not isinstance(self.history_status, GuidelineHistoryStatus):
            raise GuidelineImportExportError("guideline_export_history_status_invalid")
        if not isinstance(self.revisions, tuple | list) or any(
            not isinstance(item, GuidelineExportRevision) for item in self.revisions
        ):
            raise GuidelineImportExportError("guideline_export_revisions_invalid")
        revisions = tuple(
            sorted(self.revisions, key=lambda item: item.revision.revision_number)
        )
        if not revisions:
            raise GuidelineImportExportError("guideline_export_revisions_required")
        identity_id = self.identity.guideline_id
        if any(item.revision.guideline_id != identity_id for item in revisions):
            raise GuidelineImportExportError(
                "guideline_export_revision_identity_mismatch"
            )
        expected_numbers = tuple(range(1, len(revisions) + 1))
        actual_numbers = tuple(item.revision.revision_number for item in revisions)
        if actual_numbers != expected_numbers:
            raise GuidelineImportExportError(
                "guideline_export_revision_history_incomplete"
            )
        if len({item.revision_id for item in revisions}) != len(revisions):
            raise GuidelineImportExportError("guideline_export_duplicate_revision_id")
        if len({item.semantic_version for item in revisions}) != len(revisions):
            raise GuidelineImportExportError(
                "guideline_export_duplicate_semantic_version"
            )
        previous_published_head_updated_at: datetime | None = None
        for index, exported_revision in enumerate(revisions):
            revision = exported_revision.revision
            try:
                digest = guideline_revision_content_digest_v1(
                    title=revision.title,
                    content=revision.content,
                    rules=revision.rules,
                    tags=revision.tags,
                )
            except GuidelineLifecycleError as exc:
                raise _domain_error(exc, f"$.revisions[{index}]") from exc
            if digest != revision.content_digest:
                raise GuidelineImportExportError(
                    "guideline_export_revision_digest_mismatch",
                    path=f"$.revisions[{index}].content_digest",
                )
            expected_parent = None if index == 0 else revisions[index - 1].revision_id
            if revision.parent_revision_id != expected_parent:
                raise GuidelineImportExportError(
                    "guideline_export_revision_parent_mismatch",
                    path=f"$.revisions[{index}].parent_revision_id",
                )
            if (
                previous_published_head_updated_at is not None
                and exported_revision.published_head_updated_at
                < previous_published_head_updated_at
            ):
                raise GuidelineImportExportError(
                    "guideline_export_published_head_time_not_monotonic",
                    path=f"$.revisions[{index}].published_head_updated_at",
                )
            previous_published_head_updated_at = (
                exported_revision.published_head_updated_at
            )

        latest = revisions[-1].revision
        if (
            self.head.guideline_id != identity_id
            or self.head.revision_id != latest.revision_id
            or self.head.revision_number != latest.revision_number
            or self.head.head_revision != latest.revision_number
            or self.head.semantic_version != latest.semantic_version
        ):
            raise GuidelineImportExportError("guideline_export_head_mismatch")
        if self.head.updated_at != revisions[-1].published_head_updated_at:
            raise GuidelineImportExportError(
                "guideline_export_head_publication_time_mismatch",
                path="$.head.updated_at",
            )

        retirement = self.retirement
        if retirement is not None:
            if not isinstance(retirement, GuidelineRetirement):
                raise GuidelineImportExportError("guideline_export_retirement_invalid")
            if (
                retirement.guideline_id != identity_id
                or retirement.retired_revision_id != latest.revision_id
                or retirement.retired_revision_number != latest.revision_number
                or retirement.retired_semantic_version != latest.semantic_version
                or retirement.retired_revision_digest != latest.content_digest
                or retirement.retired_head_revision != self.head.head_revision
            ):
                raise GuidelineImportExportError(
                    "guideline_export_retirement_head_mismatch"
                )

        if not isinstance(self.bindings, tuple | list) or any(
            not isinstance(item, GuidelineExportBinding) for item in self.bindings
        ):
            raise GuidelineImportExportError("guideline_export_bindings_invalid")
        bindings = tuple(
            sorted(
                self.bindings,
                key=lambda item: (
                    item.binding.board_id,
                    item.binding.binding_id,
                    item.binding.binding_revision,
                ),
            )
        )
        revisions_by_id = {item.revision_id: item.revision for item in revisions}
        histories: dict[tuple[str, str], list[int]] = {}
        binding_candidate_keys: set[tuple[str, str, int]] = set()
        for exported_binding in bindings:
            binding = exported_binding.binding
            if binding.guideline_id != identity_id:
                raise GuidelineImportExportError(
                    "guideline_export_binding_identity_mismatch"
                )
            revision = revisions_by_id.get(binding.revision_id)
            if (
                revision is None
                or revision.semantic_version != binding.semantic_version
                or revision.content_digest != binding.revision_digest
            ):
                raise GuidelineImportExportError(
                    "guideline_export_binding_revision_mismatch"
                )
            if (
                self.identity.scope is GuidelineScope.INLINE
                and binding.board_id != self.identity.board_id
            ):
                raise GuidelineImportExportError(
                    "guideline_export_inline_binding_board_mismatch"
                )
            histories.setdefault(
                (binding.board_id, binding.binding_id),
                [],
            ).append(binding.binding_revision)
            if exported_binding.candidate_key in binding_candidate_keys:
                raise GuidelineImportExportError(
                    "guideline_export_duplicate_binding_revision"
                )
            binding_candidate_keys.add(exported_binding.candidate_key)
        for revisions_in_history in histories.values():
            if revisions_in_history != list(range(1, len(revisions_in_history) + 1)):
                raise GuidelineImportExportError(
                    "guideline_export_binding_history_incomplete"
                )

        if not isinstance(self.migration_notes, tuple | list) or any(
            not isinstance(item, str) or not item.strip()
            for item in self.migration_notes
        ):
            raise GuidelineImportExportError("guideline_export_migration_notes_invalid")
        migration_notes = tuple(sorted(set(self.migration_notes)))
        if self.history_status is GuidelineHistoryStatus.BASELINE_ONLY:
            if len(revisions) != 1 or not revisions[0].legacy_version_unresolvable:
                raise GuidelineImportExportError(
                    "guideline_export_baseline_history_mismatch"
                )
            if revisions[0].revision.rules or bindings:
                raise GuidelineImportExportError(
                    "guideline_export_legacy_baseline_must_be_contextual"
                )
        elif any(item.legacy_version is not None for item in revisions):
            raise GuidelineImportExportError(
                "guideline_export_complete_history_legacy_metadata_forbidden"
            )

        object.__setattr__(self, "revisions", revisions)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "migration_notes", migration_notes)

    @property
    def guideline_id(self) -> str:
        return self.identity.guideline_id

    @property
    def contains_blocking_policy(self) -> bool:
        revisions = {item.revision_id: item.revision for item in self.revisions}
        return any(
            exported_binding.binding.state is GuidelineBindingState.ACTIVE
            and (
                exported_binding.binding.default_enforcement
                is GuidelineEnforcement.BLOCKING
                or any(
                    rule.enforcement is GuidelineEnforcement.BLOCKING
                    for rule in revisions[exported_binding.binding.revision_id].rules
                )
            )
            for exported_binding in self.bindings
        )


@dataclass(frozen=True, slots=True)
class GuidelineExportSnapshot:
    """Consistent persistence snapshot used to construct one envelope."""

    aggregates: tuple[GuidelineExportAggregate, ...]
    source_board_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.aggregates, tuple | list) or any(
            not isinstance(item, GuidelineExportAggregate) for item in self.aggregates
        ):
            raise GuidelineImportExportError(
                "guideline_export_snapshot_aggregates_invalid"
            )
        aggregates = tuple(sorted(self.aggregates, key=lambda item: item.guideline_id))
        if len({item.guideline_id for item in aggregates}) != len(aggregates):
            raise GuidelineImportExportError("guideline_export_duplicate_guideline_id")
        source_board_id = _optional_bounded_text(
            self.source_board_id,
            "guideline_export_source_board_id_invalid",
            "$.source_board_id",
            max_length=POLICY_BOARD_ID_MAX_LENGTH,
        )
        if source_board_id is not None:
            for aggregate in aggregates:
                if aggregate.identity.scope is GuidelineScope.INLINE:
                    if aggregate.identity.board_id != source_board_id:
                        raise GuidelineImportExportError(
                            "guideline_export_snapshot_board_mismatch"
                        )
                if any(
                    binding.board_id != source_board_id
                    for binding in aggregate.bindings
                ):
                    raise GuidelineImportExportError(
                        "guideline_export_snapshot_binding_board_mismatch"
                    )
        object.__setattr__(self, "aggregates", aggregates)
        object.__setattr__(self, "source_board_id", source_board_id)


@dataclass(frozen=True, slots=True)
class GuidelineExportEnvelope:
    contract_version: str
    schema_version: str
    kind: str
    exported_at: datetime
    source_board_id: str | None
    content_digest: str
    guidelines: tuple[GuidelineExportAggregate, ...]
    source_schema_version: str = GUIDELINE_EXPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != GUIDELINE_EXPORT_CONTRACT_VERSION:
            raise GuidelineImportExportError(
                "guideline_export_contract_version_unsupported"
            )
        if self.schema_version != GUIDELINE_EXPORT_SCHEMA_VERSION:
            raise GuidelineImportExportError(
                "guideline_export_schema_version_unsupported"
            )
        if self.kind != GUIDELINE_EXPORT_KIND:
            raise GuidelineImportExportError("guideline_export_kind_invalid")
        if self.source_schema_version not in {
            GUIDELINE_EXPORT_SCHEMA_VERSION,
            GUIDELINE_EXPORT_LEGACY_SCHEMA_VERSION,
        }:
            raise GuidelineImportExportError(
                "guideline_export_source_schema_version_invalid"
            )
        exported_at = _aware_utc(
            self.exported_at,
            "guideline_export_exported_at_invalid",
            "$.exported_at",
        )
        snapshot = GuidelineExportSnapshot(
            aggregates=self.guidelines,
            source_board_id=self.source_board_id,
        )
        try:
            content_digest = normalize_guideline_sha256(
                self.content_digest,
                "guideline_export_content_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise _domain_error(exc, "$.content_digest") from exc
        expected_digest = _content_digest(
            snapshot.aggregates,
            snapshot.source_board_id,
        )
        if content_digest != expected_digest:
            raise GuidelineImportExportError(
                "guideline_export_content_digest_mismatch",
                path="$.content_digest",
            )
        object.__setattr__(self, "exported_at", exported_at)
        object.__setattr__(self, "source_board_id", snapshot.source_board_id)
        object.__setattr__(self, "content_digest", content_digest)
        object.__setattr__(self, "guidelines", snapshot.aggregates)

    @property
    def snapshot(self) -> GuidelineExportSnapshot:
        return GuidelineExportSnapshot(
            aggregates=self.guidelines,
            source_board_id=self.source_board_id,
        )


@dataclass(frozen=True, slots=True)
class ExistingGuidelineRevision:
    guideline_id: str
    revision_id: str
    semantic_version: str
    revision_digest: str

    def __post_init__(self) -> None:
        for field_name in ("guideline_id", "revision_id"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    f"guideline_import_existing_{field_name}_required",
                    f"$.{field_name}",
                    max_length=(
                        GUIDELINE_ID_MAX_LENGTH
                        if field_name == "guideline_id"
                        else GUIDELINE_REVISION_ID_MAX_LENGTH
                    ),
                ),
            )
        try:
            semantic_version = normalize_guideline_semantic_version(
                self.semantic_version,
                "guideline_import_existing_semantic_version_invalid",
            )
            revision_digest = normalize_guideline_sha256(
                self.revision_digest,
                "guideline_import_existing_revision_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise _domain_error(exc, "$") from exc
        object.__setattr__(self, "semantic_version", semantic_version)
        object.__setattr__(self, "revision_digest", revision_digest)

    @classmethod
    def from_revision(cls, revision: GuidelineRevision) -> ExistingGuidelineRevision:
        if not isinstance(revision, GuidelineRevision):
            raise GuidelineImportExportError(
                "guideline_import_existing_revision_invalid"
            )
        return cls(
            guideline_id=revision.guideline_id,
            revision_id=revision.revision_id,
            semantic_version=revision.semantic_version,
            revision_digest=revision.content_digest,
        )


@dataclass(frozen=True, slots=True)
class GuidelineImportRevisionAction:
    guideline_id: str
    revision_id: str
    semantic_version: str
    revision_digest: str
    disposition: GuidelineImportRevisionDisposition
    resolved_revision_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, GuidelineImportRevisionDisposition):
            raise GuidelineImportExportError("guideline_import_disposition_invalid")
        ExistingGuidelineRevision(
            guideline_id=self.guideline_id,
            revision_id=self.revision_id,
            semantic_version=self.semantic_version,
            revision_digest=self.revision_digest,
        )
        object.__setattr__(
            self,
            "resolved_revision_id",
            _bounded_text(
                self.resolved_revision_id,
                "guideline_import_resolved_revision_id_required",
                "$.resolved_revision_id",
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
            ),
        )


@dataclass(frozen=True, slots=True)
class GuidelineImportBindingCandidate:
    """Inert ledger payload; it is never a live binding write.

    Importing a source binding directly into the live projection would bypass
    the preview/adoption receipts and forge lineage.  The adapter therefore
    stores this payload only in the import candidate ledger.  A later native
    adoption flow may consume it through the regular preview/currentness
    fences.
    """

    source_board_id: str
    target_board_id: str
    binding_id: str
    source_history: tuple[GuidelineExportBinding, ...]
    disposition: GuidelineImportBindingDisposition

    def __post_init__(self) -> None:
        for field_name, max_length in (
            ("source_board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("target_board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    f"guideline_import_candidate_{field_name}_required",
                    f"$.{field_name}",
                    max_length=max_length,
                ),
            )
        if self.disposition not in {
            GuidelineImportBindingDisposition.PENDING_ADOPTION,
            GuidelineImportBindingDisposition.STORE_INERT_HISTORY,
            GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY,
        }:
            raise GuidelineImportExportError(
                "guideline_import_candidate_disposition_invalid"
            )
        if not isinstance(self.source_history, tuple | list) or not self.source_history:
            raise GuidelineImportExportError(
                "guideline_import_candidate_history_required"
            )
        history = tuple(
            sorted(
                self.source_history,
                key=lambda item: item.binding.binding_revision,
            )
        )
        if any(
            not isinstance(item, GuidelineExportBinding)
            or item.binding.board_id != self.source_board_id
            or item.binding.binding_id != self.binding_id
            for item in history
        ):
            raise GuidelineImportExportError(
                "guideline_import_candidate_history_mismatch"
            )
        if tuple(item.binding.binding_revision for item in history) != tuple(
            range(1, len(history) + 1)
        ):
            raise GuidelineImportExportError(
                "guideline_import_candidate_history_incomplete"
            )
        object.__setattr__(self, "source_history", history)

    @property
    def latest(self) -> GuidelineExportBinding:
        return self.source_history[-1]

    @property
    def live_write_forbidden(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class GuidelineImportPlanEntry:
    aggregate: GuidelineExportAggregate
    revision_actions: tuple[GuidelineImportRevisionAction, ...]
    binding_disposition: GuidelineImportBindingDisposition
    binding_candidates: tuple[GuidelineImportBindingCandidate, ...] = ()
    identity_conflicts: tuple[str, ...] = ()
    binding_conflicts: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.aggregate, GuidelineExportAggregate):
            raise GuidelineImportExportError("guideline_import_plan_aggregate_invalid")
        if not isinstance(self.revision_actions, tuple | list) or any(
            not isinstance(item, GuidelineImportRevisionAction)
            for item in self.revision_actions
        ):
            raise GuidelineImportExportError("guideline_import_plan_actions_invalid")
        actions = tuple(self.revision_actions)
        expected = {
            (item.revision_id, item.semantic_version, item.revision_digest)
            for item in self.aggregate.revisions
        }
        actual = {
            (item.revision_id, item.semantic_version, item.revision_digest)
            for item in actions
        }
        if expected != actual or any(
            item.guideline_id != self.aggregate.guideline_id for item in actions
        ):
            raise GuidelineImportExportError("guideline_import_plan_actions_incomplete")
        if not isinstance(
            self.binding_disposition,
            GuidelineImportBindingDisposition,
        ):
            raise GuidelineImportExportError(
                "guideline_import_binding_disposition_invalid"
            )
        if not isinstance(self.binding_candidates, tuple | list) or any(
            not isinstance(item, GuidelineImportBindingCandidate)
            for item in self.binding_candidates
        ):
            raise GuidelineImportExportError(
                "guideline_import_binding_candidates_invalid"
            )
        candidates = tuple(self.binding_candidates)
        candidate_dispositions = {candidate.disposition for candidate in candidates}
        if not candidates:
            expected_binding_disposition = GuidelineImportBindingDisposition.NO_BINDINGS
        elif candidate_dispositions == {
            GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
        }:
            expected_binding_disposition = (
                GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
            )
        elif GuidelineImportBindingDisposition.PENDING_ADOPTION in (
            candidate_dispositions
        ):
            expected_binding_disposition = (
                GuidelineImportBindingDisposition.PENDING_ADOPTION
            )
        else:
            expected_binding_disposition = (
                GuidelineImportBindingDisposition.STORE_INERT_HISTORY
            )
        if self.binding_disposition is not expected_binding_disposition:
            raise GuidelineImportExportError(
                "guideline_import_binding_disposition_mismatch"
            )
        if not isinstance(self.binding_conflicts, tuple | list) or any(
            not isinstance(item, str) or not item for item in self.binding_conflicts
        ):
            raise GuidelineImportExportError(
                "guideline_import_binding_conflicts_invalid"
            )
        binding_conflicts = tuple(sorted(set(self.binding_conflicts)))
        if not isinstance(self.identity_conflicts, tuple | list) or any(
            not isinstance(item, str) or not item for item in self.identity_conflicts
        ):
            raise GuidelineImportExportError(
                "guideline_import_identity_conflicts_invalid"
            )
        identity_conflicts = tuple(sorted(set(self.identity_conflicts)))
        diagnostics = tuple(sorted(set(self.diagnostics)))
        if any(not isinstance(item, str) or not item for item in diagnostics):
            raise GuidelineImportExportError("guideline_import_diagnostics_invalid")
        object.__setattr__(self, "revision_actions", actions)
        object.__setattr__(self, "binding_candidates", candidates)
        object.__setattr__(self, "identity_conflicts", identity_conflicts)
        object.__setattr__(self, "binding_conflicts", binding_conflicts)
        object.__setattr__(self, "diagnostics", diagnostics)

    @property
    def is_identical(self) -> bool:
        return (
            not self.identity_conflicts
            and not self.binding_conflicts
            and bool(self.revision_actions)
            and all(
                action.disposition is GuidelineImportRevisionDisposition.SKIP_IDENTICAL
                for action in self.revision_actions
            )
            and self.binding_disposition
            in {
                GuidelineImportBindingDisposition.NO_BINDINGS,
                GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY,
            }
        )

    @property
    def has_conflict(self) -> bool:
        return (
            bool(self.identity_conflicts)
            or bool(self.binding_conflicts)
            or any(
                action.disposition is GuidelineImportRevisionDisposition.CONFLICT
                for action in self.revision_actions
            )
        )

    @property
    def live_binding_writes(self) -> tuple[BoardGuidelineBinding, ...]:
        """Normative proof that import cannot bypass native adoption."""

        return ()

    @property
    def resolved_head_revision_id(self) -> str:
        for action in self.revision_actions:
            if action.revision_id == self.aggregate.head.revision_id:
                return action.resolved_revision_id
        raise GuidelineImportExportError("guideline_import_resolved_head_missing")


def guideline_import_digest(
    *,
    envelope_content_digest: str,
    target_owner_id: str,
    target_board_id: str | None,
    dry_run: bool,
) -> str:
    """Bind one import intention to content and its target partition.

    Dry-run and real execution deliberately share the same digest: a successful
    dry-run is a simulation of the exact intention later committed, not a
    distinct mutation identity.
    """

    try:
        content_digest = normalize_guideline_sha256(
            envelope_content_digest,
            "guideline_import_envelope_digest_invalid",
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, "$.envelope_content_digest") from exc
    owner_id = _bounded_text(
        target_owner_id,
        "guideline_import_target_owner_id_required",
        "$.target_owner_id",
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
    )
    board_id = _optional_bounded_text(
        target_board_id,
        "guideline_import_target_board_id_invalid",
        "$.target_board_id",
        max_length=POLICY_BOARD_ID_MAX_LENGTH,
    )
    if not isinstance(dry_run, bool):
        raise GuidelineImportExportError("guideline_import_dry_run_invalid")
    return canonical_guideline_sha256(
        {
            "contract": "guideline-import-plan/v1",
            "envelope_content_digest": content_digest,
            "target_owner_id": owner_id,
            "target_board_id": board_id,
        }
    )


@dataclass(frozen=True, slots=True)
class GuidelineImportPlan:
    envelope: GuidelineExportEnvelope
    entries: tuple[GuidelineImportPlanEntry, ...]
    dry_run: bool
    transaction_status: GuidelineImportTransactionStatus
    target_owner_id: str
    target_board_id: str | None
    import_digest: str
    error_code: str | None = None
    overwritten_row_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, GuidelineExportEnvelope):
            raise GuidelineImportExportError("guideline_import_plan_envelope_invalid")
        if not isinstance(self.entries, tuple | list) or any(
            not isinstance(item, GuidelineImportPlanEntry) for item in self.entries
        ):
            raise GuidelineImportExportError("guideline_import_plan_entries_invalid")
        if not isinstance(self.dry_run, bool):
            raise GuidelineImportExportError("guideline_import_plan_dry_run_invalid")
        target_owner_id = _bounded_text(
            self.target_owner_id,
            "guideline_import_target_owner_id_required",
            "$.target_owner_id",
            max_length=POLICY_ACTOR_ID_MAX_LENGTH,
        )
        target_board_id = _optional_bounded_text(
            self.target_board_id,
            "guideline_import_target_board_id_invalid",
            "$.target_board_id",
            max_length=POLICY_BOARD_ID_MAX_LENGTH,
        )
        try:
            import_digest = normalize_guideline_sha256(
                self.import_digest,
                "guideline_import_digest_invalid",
            )
        except GuidelinePolicyContractError as exc:
            raise _domain_error(exc, "$.import_digest") from exc
        expected_import_digest = guideline_import_digest(
            envelope_content_digest=self.envelope.content_digest,
            target_owner_id=target_owner_id,
            target_board_id=target_board_id,
            dry_run=self.dry_run,
        )
        if import_digest != expected_import_digest:
            raise GuidelineImportExportError("guideline_import_digest_mismatch")
        entries = tuple(self.entries)
        envelope_guideline_ids = tuple(
            aggregate.guideline_id for aggregate in self.envelope.guidelines
        )
        entry_guideline_ids = tuple(entry.aggregate.guideline_id for entry in entries)
        if entry_guideline_ids != envelope_guideline_ids:
            raise GuidelineImportExportError("guideline_import_plan_entries_mismatch")
        source_by_guideline_id = {
            aggregate.guideline_id: aggregate for aggregate in self.envelope.guidelines
        }
        for entry in entries:
            source = source_by_guideline_id[entry.aggregate.guideline_id]
            candidate_materialization = _remap_aggregate(
                source,
                target_owner_id,
                target_board_id,
                skip_identical=False,
            )
            identical_materialization = _remap_aggregate(
                source,
                target_owner_id,
                target_board_id,
                skip_identical=True,
            )
            if (
                entry.aggregate != candidate_materialization
                and entry.aggregate != identical_materialization
            ):
                raise GuidelineImportExportError(
                    "guideline_import_plan_aggregate_source_mismatch"
                )
        if not isinstance(
            self.transaction_status,
            GuidelineImportTransactionStatus,
        ):
            raise GuidelineImportExportError(
                "guideline_import_transaction_status_invalid"
            )
        if self.overwritten_row_count != 0:
            raise GuidelineImportExportError("guideline_import_overwrite_forbidden")
        has_conflict = any(entry.has_conflict for entry in self.entries)
        if has_conflict:
            if (
                self.error_code != "conflict"
                or self.transaction_status
                is not GuidelineImportTransactionStatus.ROLLED_BACK
            ):
                raise GuidelineImportExportError(
                    "guideline_import_conflict_must_roll_back"
                )
        elif self.error_code is not None:
            raise GuidelineImportExportError("guideline_import_error_code_unexpected")
        elif self.dry_run:
            if self.transaction_status is not GuidelineImportTransactionStatus.DRY_RUN:
                raise GuidelineImportExportError(
                    "guideline_import_dry_run_status_invalid"
                )
        elif self.transaction_status is not GuidelineImportTransactionStatus.PLANNED:
            raise GuidelineImportExportError("guideline_import_plan_status_invalid")
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "target_owner_id", target_owner_id)
        object.__setattr__(self, "target_board_id", target_board_id)
        object.__setattr__(self, "import_digest", import_digest)

    @property
    def create_count(self) -> int:
        return sum(
            action.disposition is GuidelineImportRevisionDisposition.CREATE
            for entry in self.entries
            for action in entry.revision_actions
        )

    @property
    def skip_identical_count(self) -> int:
        return sum(
            action.disposition is GuidelineImportRevisionDisposition.SKIP_IDENTICAL
            for entry in self.entries
            for action in entry.revision_actions
        )

    @property
    def conflict_count(self) -> int:
        return sum(
            action.disposition is GuidelineImportRevisionDisposition.CONFLICT
            for entry in self.entries
            for action in entry.revision_actions
        ) + sum(
            len(entry.identity_conflicts) + len(entry.binding_conflicts)
            for entry in self.entries
        )

    @property
    def can_apply(self) -> bool:
        return (
            not self.dry_run
            and self.error_code is None
            and self.transaction_status is GuidelineImportTransactionStatus.PLANNED
        )

    @property
    def entries_to_apply(self) -> tuple[GuidelineImportPlanEntry, ...]:
        if not self.can_apply:
            return ()
        return tuple(entry for entry in self.entries if not entry.is_identical)

    @property
    def live_binding_writes(self) -> tuple[BoardGuidelineBinding, ...]:
        """Imported binding payloads are candidate-ledger records only."""

        return ()


@dataclass(frozen=True, slots=True)
class GuidelineImportResult:
    """Transport-neutral result returned after applying or simulating a plan."""

    transaction_status: GuidelineImportTransactionStatus
    created_count: int
    skip_identical_count: int
    conflict_count: int
    overwritten_row_count: int
    dry_run: bool
    error_code: str | None = None

    @classmethod
    def from_plan(
        cls,
        plan: GuidelineImportPlan,
        *,
        committed: bool = False,
    ) -> GuidelineImportResult:
        if not isinstance(plan, GuidelineImportPlan):
            raise GuidelineImportExportError("guideline_import_result_plan_invalid")
        if committed and not plan.can_apply:
            raise GuidelineImportExportError("guideline_import_result_commit_forbidden")
        status = (
            GuidelineImportTransactionStatus.COMMITTED
            if committed
            else plan.transaction_status
        )
        created_count = plan.create_count if committed else 0
        return cls(
            transaction_status=status,
            created_count=created_count,
            skip_identical_count=plan.skip_identical_count,
            conflict_count=plan.conflict_count,
            overwritten_row_count=0,
            dry_run=plan.dry_run,
            error_code=plan.error_code,
        )

    def __post_init__(self) -> None:
        for field_name in (
            "created_count",
            "skip_identical_count",
            "conflict_count",
            "overwritten_row_count",
        ):
            _strict_int(
                getattr(self, field_name),
                f"guideline_import_result_{field_name}_invalid",
                f"$.{field_name}",
            )
        if self.overwritten_row_count != 0:
            raise GuidelineImportExportError("guideline_import_overwrite_forbidden")
        if not isinstance(self.dry_run, bool):
            raise GuidelineImportExportError("guideline_import_result_dry_run_invalid")


def _predicate_payload(predicate: GuidelinePredicate) -> dict[str, object]:
    return {
        "predicate_code": predicate.predicate_code,
        "parameters": [
            [
                key,
                list(value) if isinstance(value, tuple) else value,
            ]
            for key, value in predicate.parameters
        ],
    }


def _rule_payload(rule: GuidelineRule) -> dict[str, object]:
    return {
        "rule_id": rule.rule_id,
        "code": rule.code,
        "title": rule.title,
        "description": rule.description,
        "target_entity_types": [
            entity_type.value for entity_type in rule.target_entity_types
        ],
        "predicates": [_predicate_payload(predicate) for predicate in rule.predicates],
        "enforcement": rule.enforcement.value,
        "operator": rule.operator.value,
        "waivable": rule.waivable,
        "policy_class": rule.policy_class,
    }


def _revision_payload(exported: GuidelineExportRevision) -> dict[str, object]:
    revision = exported.revision
    return {
        "revision_id": revision.revision_id,
        "guideline_id": revision.guideline_id,
        "revision_number": revision.revision_number,
        "semantic_version": revision.semantic_version,
        "title": revision.title,
        "content": revision.content,
        "content_digest": revision.content_digest,
        "rules": [_rule_payload(rule) for rule in revision.rules],
        "created_by": revision.created_by,
        "created_at": _datetime_payload(revision.created_at),
        "parent_revision_id": revision.parent_revision_id,
        "tags": list(revision.tags),
        "published_head_revision": exported.published_head_revision,
        "published_head_updated_at": _datetime_payload(
            exported.published_head_updated_at
        ),
        "legacy_version": exported.legacy_version,
        "legacy_version_unresolvable": exported.legacy_version_unresolvable,
        "legacy_tags": (
            list(exported.legacy_tags) if exported.legacy_tags is not None else None
        ),
    }


def _identity_payload(identity: Guideline) -> dict[str, object]:
    return {
        "guideline_id": identity.guideline_id,
        "owner_id": identity.owner_id,
        "scope": identity.scope.value,
        "board_id": identity.board_id,
        "context_scope": identity.context_scope.value,
        "created_at": _datetime_payload(identity.created_at),
    }


def _head_payload(head: GuidelineHead) -> dict[str, object]:
    return {
        "guideline_id": head.guideline_id,
        "revision_id": head.revision_id,
        "revision_number": head.revision_number,
        "semantic_version": head.semantic_version,
        "head_revision": head.head_revision,
        "updated_at": _datetime_payload(head.updated_at),
    }


def _retirement_payload(
    retirement: GuidelineRetirement | None,
) -> dict[str, object] | None:
    if retirement is None:
        return None
    return {
        "retirement_id": retirement.retirement_id,
        "guideline_id": retirement.guideline_id,
        "status": retirement.status.value,
        "retired_revision_id": retirement.retired_revision_id,
        "retired_revision_number": retirement.retired_revision_number,
        "retired_semantic_version": retirement.retired_semantic_version,
        "retired_revision_digest": retirement.retired_revision_digest,
        "retired_head_revision": retirement.retired_head_revision,
        "reason": retirement.reason,
        "retired_by": retirement.retired_by,
        "retired_at": _datetime_payload(retirement.retired_at),
        "superseded_by_guideline_id": retirement.superseded_by_guideline_id,
    }


def _binding_payload(binding: BoardGuidelineBinding) -> dict[str, object]:
    return {
        "binding_id": binding.binding_id,
        "board_id": binding.board_id,
        "guideline_id": binding.guideline_id,
        "revision_id": binding.revision_id,
        "semantic_version": binding.semantic_version,
        "revision_digest": binding.revision_digest,
        "priority": binding.priority,
        "binding_revision": binding.binding_revision,
        "adopted_by": binding.adopted_by,
        "adopted_at": _datetime_payload(binding.adopted_at),
        "default_enforcement": binding.default_enforcement.value,
        "state": binding.state.value,
        "source_kind": binding.source_kind.value,
    }


def _aggregate_payload(
    aggregate: GuidelineExportAggregate,
) -> dict[str, object]:
    return {
        "identity": _identity_payload(aggregate.identity),
        "revisions": [_revision_payload(revision) for revision in aggregate.revisions],
        "head": _head_payload(aggregate.head),
        "retirement": _retirement_payload(aggregate.retirement),
        "bindings": [binding.digest_payload() for binding in aggregate.bindings],
        "history_status": aggregate.history_status.value,
        "migration_notes": list(aggregate.migration_notes),
    }


def _content_manifest(
    aggregates: tuple[GuidelineExportAggregate, ...],
    source_board_id: str | None,
) -> dict[str, object]:
    return {
        "contract_version": GUIDELINE_EXPORT_CONTRACT_VERSION,
        "schema_version": GUIDELINE_EXPORT_SCHEMA_VERSION,
        "kind": GUIDELINE_EXPORT_KIND,
        "source_board_id": source_board_id,
        "guidelines": [_aggregate_payload(item) for item in aggregates],
    }


def _content_digest(
    aggregates: tuple[GuidelineExportAggregate, ...],
    source_board_id: str | None,
) -> str:
    return canonical_guideline_sha256(_content_manifest(aggregates, source_board_id))


def build_guideline_export_v2(
    snapshot: GuidelineExportSnapshot
    | tuple[GuidelineExportAggregate, ...]
    | list[GuidelineExportAggregate],
    *,
    exported_at: datetime,
    source_board_id: str | None = None,
) -> GuidelineExportEnvelope:
    """Build a canonical v2 envelope from one consistent snapshot."""

    if isinstance(snapshot, GuidelineExportSnapshot):
        if source_board_id is not None and source_board_id != snapshot.source_board_id:
            raise GuidelineImportExportError(
                "guideline_export_source_board_override_mismatch"
            )
        resolved = snapshot
    else:
        resolved = GuidelineExportSnapshot(
            aggregates=tuple(snapshot),
            source_board_id=source_board_id,
        )
    return GuidelineExportEnvelope(
        contract_version=GUIDELINE_EXPORT_CONTRACT_VERSION,
        schema_version=GUIDELINE_EXPORT_SCHEMA_VERSION,
        kind=GUIDELINE_EXPORT_KIND,
        exported_at=exported_at,
        source_board_id=resolved.source_board_id,
        content_digest=_content_digest(
            resolved.aggregates,
            resolved.source_board_id,
        ),
        guidelines=resolved.aggregates,
    )


def guideline_export_payload(
    envelope: GuidelineExportEnvelope,
) -> dict[str, object]:
    """Return the closed JSON-compatible v2 envelope."""

    if not isinstance(envelope, GuidelineExportEnvelope):
        raise GuidelineImportExportError("guideline_export_envelope_invalid")
    return {
        "contract_version": envelope.contract_version,
        "schema_version": envelope.schema_version,
        "kind": envelope.kind,
        "exported_at": _datetime_payload(envelope.exported_at),
        "source_board_id": envelope.source_board_id,
        "content_digest": envelope.content_digest,
        "guidelines": [
            _aggregate_payload(aggregate) for aggregate in envelope.guidelines
        ],
    }


def guideline_export_json_bytes(
    envelope: GuidelineExportEnvelope,
) -> bytes:
    return canonical_guideline_json_bytes(guideline_export_payload(envelope))


def _parse_predicate(raw: object, path: str) -> GuidelinePredicate:
    value = _mapping(
        raw,
        "guideline_export_predicate_invalid",
        path,
    )
    _closed(
        value,
        required=frozenset({"predicate_code", "parameters"}),
        path=path,
    )
    parameters: list[tuple[str, object]] = []
    for index, item in enumerate(
        _sequence(
            value["parameters"],
            "guideline_export_predicate_parameters_invalid",
            f"{path}.parameters",
        )
    ):
        pair = _sequence(
            item,
            "guideline_export_predicate_parameter_invalid",
            f"{path}.parameters[{index}]",
        )
        if len(pair) != 2:
            raise GuidelineImportExportError(
                "guideline_export_predicate_parameter_invalid",
                path=f"{path}.parameters[{index}]",
            )
        parameter_value: object = pair[1]
        if isinstance(parameter_value, list | tuple):
            parameter_value = tuple(parameter_value)
        parameters.append(
            (
                _required_text(
                    pair[0],
                    "guideline_export_predicate_parameter_key_invalid",
                    f"{path}.parameters[{index}][0]",
                ),
                parameter_value,
            )
        )
    try:
        return GuidelinePredicate(
            predicate_code=value["predicate_code"],
            parameters=tuple(parameters),
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_rule(raw: object, path: str) -> GuidelineRule:
    value = _mapping(raw, "guideline_export_rule_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "rule_id",
                "code",
                "title",
                "description",
                "target_entity_types",
                "predicates",
                "enforcement",
                "operator",
                "waivable",
                "policy_class",
            }
        ),
        path=path,
    )
    target_entity_types = tuple(
        _enum_value(
            item,
            PolicyEntityType,
            "guideline_export_rule_target_invalid",
            f"{path}.target_entity_types[{index}]",
        )
        for index, item in enumerate(
            _sequence(
                value["target_entity_types"],
                "guideline_export_rule_targets_invalid",
                f"{path}.target_entity_types",
            )
        )
    )
    predicates = tuple(
        _parse_predicate(item, f"{path}.predicates[{index}]")
        for index, item in enumerate(
            _sequence(
                value["predicates"],
                "guideline_export_rule_predicates_invalid",
                f"{path}.predicates",
            )
        )
    )
    try:
        return GuidelineRule(
            rule_id=value["rule_id"],
            code=value["code"],
            title=value["title"],
            description=value["description"],
            target_entity_types=target_entity_types,
            predicates=predicates,
            enforcement=_enum_value(
                value["enforcement"],
                GuidelineEnforcement,
                "guideline_export_rule_enforcement_invalid",
                f"{path}.enforcement",
            ),
            operator=_enum_value(
                value["operator"],
                GuidelineRuleOperator,
                "guideline_export_rule_operator_invalid",
                f"{path}.operator",
            ),
            waivable=_strict_bool(
                value["waivable"],
                "guideline_export_rule_waivable_invalid",
                f"{path}.waivable",
            ),
            policy_class=value["policy_class"],
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_revision(raw: object, path: str) -> GuidelineExportRevision:
    value = _mapping(raw, "guideline_export_revision_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "revision_id",
                "guideline_id",
                "revision_number",
                "semantic_version",
                "title",
                "content",
                "content_digest",
                "rules",
                "created_by",
                "created_at",
                "parent_revision_id",
                "tags",
                "published_head_revision",
                "published_head_updated_at",
                "legacy_version",
                "legacy_version_unresolvable",
                "legacy_tags",
            }
        ),
        path=path,
    )
    rules = tuple(
        _parse_rule(item, f"{path}.rules[{index}]")
        for index, item in enumerate(
            _sequence(
                value["rules"],
                "guideline_export_revision_rules_invalid",
                f"{path}.rules",
            )
        )
    )
    tags = tuple(
        _required_text(
            item,
            "guideline_export_revision_tag_invalid",
            f"{path}.tags[{index}]",
        )
        for index, item in enumerate(
            _sequence(
                value["tags"],
                "guideline_export_revision_tags_invalid",
                f"{path}.tags",
            )
        )
    )
    try:
        revision = GuidelineRevision(
            revision_id=value["revision_id"],
            guideline_id=value["guideline_id"],
            revision_number=_strict_int(
                value["revision_number"],
                "guideline_export_revision_number_invalid",
                f"{path}.revision_number",
                minimum=1,
            ),
            semantic_version=value["semantic_version"],
            title=value["title"],
            content=value["content"],
            content_digest=value["content_digest"],
            rules=rules,
            created_by=value["created_by"],
            created_at=_parse_datetime(
                value["created_at"],
                "guideline_export_revision_created_at_invalid",
                f"{path}.created_at",
            ),
            parent_revision_id=_optional_text(
                value["parent_revision_id"],
                "guideline_export_revision_parent_invalid",
                f"{path}.parent_revision_id",
            ),
            tags=tags,
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc
    return GuidelineExportRevision(
        revision=revision,
        published_head_revision=_strict_int(
            value["published_head_revision"],
            "guideline_export_published_head_revision_invalid",
            f"{path}.published_head_revision",
            minimum=1,
        ),
        published_head_updated_at=_parse_datetime(
            value["published_head_updated_at"],
            "guideline_export_published_head_updated_at_invalid",
            f"{path}.published_head_updated_at",
        ),
        legacy_version=_optional_text(
            value["legacy_version"],
            "guideline_export_legacy_version_invalid",
            f"{path}.legacy_version",
        ),
        legacy_version_unresolvable=_strict_bool(
            value["legacy_version_unresolvable"],
            "guideline_export_legacy_resolution_invalid",
            f"{path}.legacy_version_unresolvable",
        ),
        legacy_tags=(
            None
            if value["legacy_tags"] is None
            else tuple(
                _required_text(
                    item,
                    "guideline_export_legacy_tag_invalid",
                    f"{path}.legacy_tags[{index}]",
                )
                for index, item in enumerate(
                    _sequence(
                        value["legacy_tags"],
                        "guideline_export_legacy_tags_invalid",
                        f"{path}.legacy_tags",
                    )
                )
            )
        ),
    )


def _parse_identity(raw: object, path: str) -> Guideline:
    value = _mapping(raw, "guideline_export_identity_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "guideline_id",
                "owner_id",
                "scope",
                "board_id",
                "context_scope",
                "created_at",
            }
        ),
        path=path,
    )
    try:
        return Guideline(
            guideline_id=value["guideline_id"],
            owner_id=value["owner_id"],
            scope=_enum_value(
                value["scope"],
                GuidelineScope,
                "guideline_export_scope_invalid",
                f"{path}.scope",
            ),
            created_at=_parse_datetime(
                value["created_at"],
                "guideline_export_identity_created_at_invalid",
                f"{path}.created_at",
            ),
            board_id=_optional_text(
                value["board_id"],
                "guideline_export_board_id_invalid",
                f"{path}.board_id",
            ),
            context_scope=_enum_value(
                value["context_scope"],
                GuidelineContextScope,
                "guideline_export_context_scope_invalid",
                f"{path}.context_scope",
            ),
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_head(raw: object, path: str) -> GuidelineHead:
    value = _mapping(raw, "guideline_export_head_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "guideline_id",
                "revision_id",
                "revision_number",
                "semantic_version",
                "head_revision",
                "updated_at",
            }
        ),
        path=path,
    )
    try:
        return GuidelineHead(
            guideline_id=value["guideline_id"],
            revision_id=value["revision_id"],
            revision_number=_strict_int(
                value["revision_number"],
                "guideline_export_head_revision_number_invalid",
                f"{path}.revision_number",
                minimum=1,
            ),
            semantic_version=value["semantic_version"],
            head_revision=_strict_int(
                value["head_revision"],
                "guideline_export_head_revision_invalid",
                f"{path}.head_revision",
                minimum=1,
            ),
            updated_at=_parse_datetime(
                value["updated_at"],
                "guideline_export_head_updated_at_invalid",
                f"{path}.updated_at",
            ),
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_retirement(
    raw: object,
    path: str,
) -> GuidelineRetirement | None:
    if raw is None:
        return None
    value = _mapping(raw, "guideline_export_retirement_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "retirement_id",
                "guideline_id",
                "status",
                "retired_revision_id",
                "retired_revision_number",
                "retired_semantic_version",
                "retired_revision_digest",
                "retired_head_revision",
                "reason",
                "retired_by",
                "retired_at",
                "superseded_by_guideline_id",
            }
        ),
        path=path,
    )
    try:
        return GuidelineRetirement(
            retirement_id=value["retirement_id"],
            guideline_id=value["guideline_id"],
            status=_enum_value(
                value["status"],
                GuidelineLifecycleStatus,
                "guideline_export_retirement_status_invalid",
                f"{path}.status",
            ),
            retired_revision_id=value["retired_revision_id"],
            retired_revision_number=_strict_int(
                value["retired_revision_number"],
                "guideline_export_retired_revision_number_invalid",
                f"{path}.retired_revision_number",
                minimum=1,
            ),
            retired_semantic_version=value["retired_semantic_version"],
            retired_revision_digest=value["retired_revision_digest"],
            retired_head_revision=_strict_int(
                value["retired_head_revision"],
                "guideline_export_retired_head_revision_invalid",
                f"{path}.retired_head_revision",
                minimum=1,
            ),
            reason=value["reason"],
            retired_by=value["retired_by"],
            retired_at=_parse_datetime(
                value["retired_at"],
                "guideline_export_retired_at_invalid",
                f"{path}.retired_at",
            ),
            superseded_by_guideline_id=_optional_text(
                value["superseded_by_guideline_id"],
                "guideline_export_successor_invalid",
                f"{path}.superseded_by_guideline_id",
            ),
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_logical_binding(
    raw: object,
    path: str,
) -> BoardGuidelineBinding:
    value = _mapping(raw, "guideline_export_binding_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "binding_id",
                "board_id",
                "guideline_id",
                "revision_id",
                "semantic_version",
                "revision_digest",
                "priority",
                "binding_revision",
                "adopted_by",
                "adopted_at",
                "default_enforcement",
                "state",
                "source_kind",
            }
        ),
        path=path,
    )
    try:
        return BoardGuidelineBinding(
            binding_id=value["binding_id"],
            board_id=value["board_id"],
            guideline_id=value["guideline_id"],
            revision_id=value["revision_id"],
            semantic_version=value["semantic_version"],
            revision_digest=value["revision_digest"],
            priority=_strict_int(
                value["priority"],
                "guideline_export_binding_priority_invalid",
                f"{path}.priority",
            ),
            binding_revision=_strict_int(
                value["binding_revision"],
                "guideline_export_binding_revision_invalid",
                f"{path}.binding_revision",
                minimum=1,
            ),
            adopted_by=value["adopted_by"],
            adopted_at=_parse_datetime(
                value["adopted_at"],
                "guideline_export_binding_adopted_at_invalid",
                f"{path}.adopted_at",
            ),
            default_enforcement=_enum_value(
                value["default_enforcement"],
                GuidelineEnforcement,
                "guideline_export_binding_enforcement_invalid",
                f"{path}.default_enforcement",
            ),
            state=_enum_value(
                value["state"],
                GuidelineBindingState,
                "guideline_export_binding_state_invalid",
                f"{path}.state",
            ),
            source_kind=_enum_value(
                value["source_kind"],
                GuidelineBindingProvenance,
                "guideline_export_binding_source_invalid",
                f"{path}.source_kind",
            ),
        )
    except GuidelinePolicyContractError as exc:
        raise _domain_error(exc, path) from exc


def _parse_binding(raw: object, path: str) -> GuidelineExportBinding:
    value = _mapping(raw, "guideline_export_binding_snapshot_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "binding",
                "physical_source_kind",
                "binding_origin",
                "materialization",
                "legacy_source_id",
                "legacy_guideline_version",
                "legacy_template_id",
                "legacy_template_version",
                "legacy_version_unresolvable",
                "evidence_refs",
                "binding_digest",
            }
        ),
        path=path,
    )
    evidence_refs = tuple(
        tuple(
            _sequence(
                item,
                "guideline_export_binding_evidence_ref_invalid",
                f"{path}.evidence_refs[{index}]",
            )
        )
        for index, item in enumerate(
            _sequence(
                value["evidence_refs"],
                "guideline_export_binding_evidence_refs_invalid",
                f"{path}.evidence_refs",
            )
        )
    )
    return GuidelineExportBinding(
        binding=_parse_logical_binding(
            value["binding"],
            f"{path}.binding",
        ),
        physical_source_kind=_required_text(
            value["physical_source_kind"],
            "guideline_export_binding_physical_source_kind_required",
            f"{path}.physical_source_kind",
        ),
        binding_origin=_required_text(
            value["binding_origin"],
            "guideline_export_binding_binding_origin_required",
            f"{path}.binding_origin",
        ),
        materialization=_enum_value(
            value["materialization"],
            GuidelineBindingMaterialization,
            "guideline_export_binding_materialization_invalid",
            f"{path}.materialization",
        ),
        legacy_source_id=_optional_text(
            value["legacy_source_id"],
            "guideline_export_binding_legacy_source_id_invalid",
            f"{path}.legacy_source_id",
        ),
        legacy_guideline_version=_optional_text(
            value["legacy_guideline_version"],
            "guideline_export_binding_legacy_guideline_version_invalid",
            f"{path}.legacy_guideline_version",
        ),
        legacy_template_id=_optional_text(
            value["legacy_template_id"],
            "guideline_export_binding_legacy_template_id_invalid",
            f"{path}.legacy_template_id",
        ),
        legacy_template_version=_optional_text(
            value["legacy_template_version"],
            "guideline_export_binding_legacy_template_version_invalid",
            f"{path}.legacy_template_version",
        ),
        legacy_version_unresolvable=_strict_bool(
            value["legacy_version_unresolvable"],
            "guideline_export_binding_legacy_resolution_invalid",
            f"{path}.legacy_version_unresolvable",
        ),
        evidence_refs=evidence_refs,
        binding_digest=_required_text(
            value["binding_digest"],
            "guideline_export_binding_digest_invalid",
            f"{path}.binding_digest",
        ),
    )


def _parse_aggregate(raw: object, path: str) -> GuidelineExportAggregate:
    value = _mapping(raw, "guideline_export_aggregate_invalid", path)
    _closed(
        value,
        required=frozenset(
            {
                "identity",
                "revisions",
                "head",
                "retirement",
                "bindings",
                "history_status",
                "migration_notes",
            }
        ),
        path=path,
    )
    revisions = tuple(
        _parse_revision(item, f"{path}.revisions[{index}]")
        for index, item in enumerate(
            _sequence(
                value["revisions"],
                "guideline_export_revisions_invalid",
                f"{path}.revisions",
            )
        )
    )
    bindings = tuple(
        _parse_binding(item, f"{path}.bindings[{index}]")
        for index, item in enumerate(
            _sequence(
                value["bindings"],
                "guideline_export_bindings_invalid",
                f"{path}.bindings",
            )
        )
    )
    notes = tuple(
        _required_text(
            item,
            "guideline_export_migration_note_invalid",
            f"{path}.migration_notes[{index}]",
        )
        for index, item in enumerate(
            _sequence(
                value["migration_notes"],
                "guideline_export_migration_notes_invalid",
                f"{path}.migration_notes",
            )
        )
    )
    return GuidelineExportAggregate(
        identity=_parse_identity(value["identity"], f"{path}.identity"),
        revisions=revisions,
        head=_parse_head(value["head"], f"{path}.head"),
        retirement=_parse_retirement(
            value["retirement"],
            f"{path}.retirement",
        ),
        bindings=bindings,
        history_status=_enum_value(
            value["history_status"],
            GuidelineHistoryStatus,
            "guideline_export_history_status_invalid",
            f"{path}.history_status",
        ),
        migration_notes=notes,
    )


def _validate_successor_graph(
    aggregates: tuple[GuidelineExportAggregate, ...],
) -> None:
    by_id = {item.guideline_id: item for item in aggregates}
    for start in by_id:
        seen: set[str] = set()
        cursor = start
        while cursor in by_id:
            if cursor in seen:
                raise GuidelineImportExportError("guideline_export_supersedence_cycle")
            seen.add(cursor)
            retirement = by_id[cursor].retirement
            successor = (
                retirement.superseded_by_guideline_id
                if retirement is not None
                and retirement.status is GuidelineLifecycleStatus.SUPERSEDED
                else None
            )
            if successor is None:
                break
            cursor = successor


def _parse_v2(raw: Mapping[str, Any]) -> GuidelineExportEnvelope:
    _closed(
        raw,
        required=frozenset(
            {
                "contract_version",
                "schema_version",
                "kind",
                "exported_at",
                "source_board_id",
                "content_digest",
                "guidelines",
            }
        ),
        path="$",
    )
    aggregates = tuple(
        _parse_aggregate(item, f"$.guidelines[{index}]")
        for index, item in enumerate(
            _sequence(
                raw["guidelines"],
                "guideline_export_guidelines_invalid",
                "$.guidelines",
            )
        )
    )
    _validate_successor_graph(aggregates)
    return GuidelineExportEnvelope(
        contract_version=_required_text(
            raw["contract_version"],
            "guideline_export_contract_version_unsupported",
            "$.contract_version",
        ),
        schema_version=str(raw["schema_version"]),
        kind=_required_text(
            raw["kind"],
            "guideline_export_kind_invalid",
            "$.kind",
        ),
        exported_at=_parse_datetime(
            raw["exported_at"],
            "guideline_export_exported_at_invalid",
            "$.exported_at",
        ),
        source_board_id=_optional_text(
            raw["source_board_id"],
            "guideline_export_source_board_id_invalid",
            "$.source_board_id",
        ),
        content_digest=_required_text(
            raw["content_digest"],
            "guideline_export_content_digest_invalid",
            "$.content_digest",
        ),
        guidelines=aggregates,
    )


def _legacy_version(item: Mapping[str, Any]) -> str:
    raw = item.get("legacy_version", item.get("version", "1"))
    if raw is None:
        return "1"
    if isinstance(raw, bool) or not isinstance(raw, str | int | float):
        raise GuidelineImportExportError("guideline_export_legacy_version_invalid")
    normalized = str(raw).strip()
    if not normalized:
        raise GuidelineImportExportError("guideline_export_legacy_version_invalid")
    return normalized


def _legacy_blocking_requested(
    item: Mapping[str, Any],
    *,
    path: str,
) -> tuple[bool, bool]:
    """Validate compatibility hints without importing executable rules."""

    blocking = False

    def inspect(candidate: Mapping[str, Any], candidate_path: str) -> None:
        nonlocal blocking
        for field_name in ("blocking", "is_blocking"):
            if field_name in candidate:
                blocking = (
                    _strict_bool(
                        candidate[field_name],
                        "guideline_export_legacy_blocking_invalid",
                        f"{candidate_path}.{field_name}",
                    )
                    or blocking
                )
        if "enforcement" in candidate:
            enforcement = _required_text(
                candidate["enforcement"],
                "guideline_export_legacy_enforcement_invalid",
                f"{candidate_path}.enforcement",
            ).lower()
            if enforcement not in {
                GuidelineEnforcement.ADVISORY.value,
                GuidelineEnforcement.BLOCKING.value,
            }:
                raise GuidelineImportExportError(
                    "guideline_export_legacy_enforcement_invalid",
                    path=f"{candidate_path}.enforcement",
                )
            blocking = enforcement == GuidelineEnforcement.BLOCKING.value or blocking

    inspect(item, path)
    raw_rules = item.get("rules")
    if raw_rules is None:
        return blocking, False
    rules = _sequence(
        raw_rules,
        "guideline_export_legacy_rules_invalid",
        f"{path}.rules",
    )
    for index, raw_rule in enumerate(rules):
        rule = _mapping(
            raw_rule,
            "guideline_export_legacy_rule_invalid",
            f"{path}.rules[{index}]",
        )
        inspect(rule, f"{path}.rules[{index}]")
    return blocking, bool(rules)


def _parse_legacy_v1(
    raw: Mapping[str, Any],
    *,
    fallback_exported_at: datetime | None,
) -> GuidelineExportEnvelope:
    allowed = _LEGACY_ENVELOPE_REQUIRED_FIELDS | _LEGACY_ENVELOPE_OPTIONAL_FIELDS
    unknown = set(raw) - allowed
    missing = _LEGACY_ENVELOPE_REQUIRED_FIELDS - set(raw)
    if missing:
        raise GuidelineImportExportError(
            "guideline_export_field_required",
            path=f"$.{sorted(missing)[0]}",
        )
    if unknown:
        raise GuidelineImportExportError(
            "guideline_export_unknown_field",
            path=f"$.{sorted(unknown)[0]}",
        )
    if raw["kind"] != GUIDELINE_EXPORT_KIND:
        raise GuidelineImportExportError(
            "guideline_export_kind_invalid",
            path="$.kind",
        )
    if "exported_at" in raw:
        exported_at = _parse_datetime(
            raw["exported_at"],
            "guideline_export_exported_at_invalid",
            "$.exported_at",
        )
    else:
        exported_at = _aware_utc(
            fallback_exported_at or datetime(1970, 1, 1, tzinfo=timezone.utc),
            "guideline_export_exported_at_invalid",
            "$.exported_at",
        )
    items = _sequence(
        raw["items"],
        "guideline_export_legacy_items_invalid",
        "$.items",
    )
    aggregates: list[GuidelineExportAggregate] = []
    seen_ids: set[str] = set()
    for index, raw_item in enumerate(items):
        path = f"$.items[{index}]"
        item = _mapping(
            raw_item,
            "guideline_export_legacy_item_invalid",
            path,
        )
        unknown_item_fields = set(item) - _LEGACY_ITEM_FIELDS
        if unknown_item_fields:
            raise GuidelineImportExportError(
                "guideline_export_unknown_field",
                path=f"{path}.{sorted(unknown_item_fields)[0]}",
            )
        title = _required_text(
            item.get("title"),
            "guideline_export_legacy_title_required",
            f"{path}.title",
        )
        content = _required_text(
            item.get("content"),
            "guideline_export_legacy_content_required",
            f"{path}.content",
        )
        scope = _enum_value(
            item.get("scope", GuidelineScope.GLOBAL.value),
            GuidelineScope,
            "guideline_export_legacy_scope_invalid",
            f"{path}.scope",
        )
        board_id = _optional_text(
            item.get("board_id"),
            "guideline_export_legacy_board_id_invalid",
            f"{path}.board_id",
        )
        tags_raw = item.get("tags")
        tags_sequence = (
            ()
            if tags_raw is None
            else _sequence(
                tags_raw,
                "guideline_export_legacy_tags_invalid",
                f"{path}.tags",
            )
        )
        tags = tuple(
            _required_text(
                tag,
                "guideline_export_legacy_tag_invalid",
                f"{path}.tags[{tag_index}]",
            )
            for tag_index, tag in enumerate(tags_sequence)
        )
        legacy_version = _legacy_version(item)
        identity_seed = {
            "contract": "guideline-export/v1-baseline-identity",
            "title": title,
            "content": content,
            "tags": sorted(tags),
            "scope": scope.value,
            "board_id": board_id,
            "legacy_version": legacy_version,
        }
        identity_digest = canonical_guideline_sha256(identity_seed)
        # Durable identity columns are VARCHAR(36).  Keep the descriptive
        # deterministic prefixes while retaining 76 bits of collision space.
        guideline_id = f"legacy-guideline-{identity_digest[:19]}"
        if guideline_id in seen_ids:
            raise GuidelineImportExportError(
                "guideline_export_duplicate_guideline_id",
                path=path,
            )
        seen_ids.add(guideline_id)
        revision_id = f"legacy-revision-{identity_digest[:20]}"
        try:
            identity = Guideline(
                guideline_id=guideline_id,
                owner_id=GUIDELINE_EXPORT_LEGACY_OWNER,
                scope=scope,
                board_id=board_id,
                context_scope=GuidelineContextScope.ALL,
                created_at=exported_at,
            )
            revision = GuidelineRevision(
                revision_id=revision_id,
                guideline_id=guideline_id,
                revision_number=1,
                semantic_version=GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION,
                title=title,
                content=content,
                content_digest=guideline_revision_content_digest_v1(
                    title=title,
                    content=content,
                    tags=tags,
                    rules=(),
                ),
                rules=(),
                created_by=GUIDELINE_EXPORT_LEGACY_ACTOR,
                created_at=exported_at,
                parent_revision_id=None,
                tags=tags,
            )
            head = GuidelineHead(
                guideline_id=guideline_id,
                revision_id=revision_id,
                revision_number=1,
                semantic_version=GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION,
                head_revision=1,
                updated_at=exported_at,
            )
        except (GuidelinePolicyContractError, GuidelineLifecycleError) as exc:
            raise _domain_error(exc, path) from exc
        notes = [
            "legacy_history_unresolvable",
            "legacy_identity_synthesized",
            "legacy_v1_contextual_baseline",
        ]
        blocking_requested, rules_present = _legacy_blocking_requested(
            item,
            path=path,
        )
        if rules_present:
            notes.append("legacy_rules_dropped_contextual_baseline")
        if blocking_requested:
            notes.append("legacy_blocking_downgraded_to_advisory")
        aggregates.append(
            GuidelineExportAggregate(
                identity=identity,
                revisions=(
                    GuidelineExportRevision(
                        revision=revision,
                        published_head_revision=1,
                        published_head_updated_at=exported_at,
                        legacy_version=legacy_version,
                        legacy_version_unresolvable=True,
                        legacy_tags=tags,
                    ),
                ),
                head=head,
                bindings=(),
                history_status=GuidelineHistoryStatus.BASELINE_ONLY,
                migration_notes=tuple(notes),
            )
        )
    snapshot = GuidelineExportSnapshot(aggregates=tuple(aggregates))
    return GuidelineExportEnvelope(
        contract_version=GUIDELINE_EXPORT_CONTRACT_VERSION,
        schema_version=GUIDELINE_EXPORT_SCHEMA_VERSION,
        kind=GUIDELINE_EXPORT_KIND,
        exported_at=exported_at,
        source_board_id=None,
        content_digest=_content_digest(snapshot.aggregates, None),
        guidelines=snapshot.aggregates,
        source_schema_version=GUIDELINE_EXPORT_LEGACY_SCHEMA_VERSION,
    )


def parse_guideline_export(
    raw: object,
    *,
    legacy_exported_at: datetime | None = None,
) -> GuidelineExportEnvelope:
    """Dispatch and fully validate v2 or legacy v1 before planning."""

    envelope = _mapping(
        raw,
        "guideline_export_envelope_invalid",
        "$",
    )
    schema_version = str(envelope.get("schema_version", "")).strip()
    if schema_version == GUIDELINE_EXPORT_SCHEMA_VERSION:
        return _parse_v2(envelope)
    if schema_version == GUIDELINE_EXPORT_LEGACY_SCHEMA_VERSION:
        return _parse_legacy_v1(
            envelope,
            fallback_exported_at=legacy_exported_at,
        )
    raise GuidelineImportExportError(
        "guideline_export_schema_version_unsupported",
        path="$.schema_version",
    )


def _remap_aggregate(
    aggregate: GuidelineExportAggregate,
    target_owner_id: str,
    target_board_id: str | None,
    *,
    skip_identical: bool,
) -> GuidelineExportAggregate:
    owner_id = _bounded_text(
        target_owner_id,
        "guideline_import_target_owner_id_required",
        "$.target_owner_id",
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
    )
    target = (
        None
        if target_board_id is None
        else _bounded_text(
            target_board_id,
            "guideline_import_target_board_id_invalid",
            "$.target_board_id",
            max_length=POLICY_BOARD_ID_MAX_LENGTH,
        )
    )
    identity = aggregate.identity
    if identity.owner_id != owner_id:
        identity = replace(identity, owner_id=owner_id)
    if (
        target is not None
        and identity.scope is GuidelineScope.INLINE
        and identity.board_id != target
    ):
        identity = replace(identity, board_id=target)
    if skip_identical and identity == aggregate.identity and target is None:
        return aggregate
    bindings = tuple(
        replace(
            exported_binding,
            binding=(
                replace(exported_binding.binding, board_id=target)
                if target is not None and exported_binding.binding.board_id != target
                else exported_binding.binding
            ),
            materialization=GuidelineBindingMaterialization.CANDIDATE,
            binding_digest=None,
        )
        for exported_binding in aggregate.bindings
    )
    return replace(
        aggregate,
        identity=identity,
        bindings=bindings,
        migration_notes=tuple(
            sorted(
                set(
                    (
                        *aggregate.migration_notes,
                        *(("binding_history_stored_inert",) if bindings else ()),
                    )
                )
            )
        ),
    )


def _existing_refs(
    values: Sequence[ExistingGuidelineRevision | GuidelineRevision],
) -> tuple[ExistingGuidelineRevision, ...]:
    refs: list[ExistingGuidelineRevision] = []
    for item in values:
        if isinstance(item, ExistingGuidelineRevision):
            refs.append(item)
        elif isinstance(item, GuidelineRevision):
            refs.append(ExistingGuidelineRevision.from_revision(item))
        else:
            raise GuidelineImportExportError(
                "guideline_import_existing_revision_invalid"
            )
    keys: set[tuple[str, str]] = set()
    for ref in refs:
        key = (ref.guideline_id, ref.semantic_version)
        if key in keys:
            raise GuidelineImportExportError(
                "guideline_import_existing_revision_duplicate"
            )
        keys.add(key)
    return tuple(refs)


def plan_guideline_import(
    envelope: GuidelineExportEnvelope,
    *,
    existing_revisions: Sequence[ExistingGuidelineRevision | GuidelineRevision] = (),
    existing_aggregates: Sequence[GuidelineExportAggregate] = (),
    dry_run: bool = False,
    target_owner_id: str,
    target_board_id: str | None = None,
) -> GuidelineImportPlan:
    """Return an all-or-nothing, zero-overwrite import plan.

    ``existing_aggregates`` enables a same-environment exact round trip to
    preserve historical binding state as ``skip_identical``.  The lighter
    ``existing_revisions`` collection is sufficient for the normative
    identity+SemVer+digest collision rule.
    """

    if not isinstance(envelope, GuidelineExportEnvelope):
        raise GuidelineImportExportError("guideline_import_envelope_invalid")
    if not isinstance(dry_run, bool):
        raise GuidelineImportExportError("guideline_import_dry_run_invalid")
    target_owner_id = _bounded_text(
        target_owner_id,
        "guideline_import_target_owner_id_required",
        "$.target_owner_id",
        max_length=POLICY_ACTOR_ID_MAX_LENGTH,
    )
    target_board_id = _optional_bounded_text(
        target_board_id,
        "guideline_import_target_board_id_invalid",
        "$.target_board_id",
        max_length=POLICY_BOARD_ID_MAX_LENGTH,
    )
    refs = list(_existing_refs(existing_revisions))
    existing_by_id: dict[str, GuidelineExportAggregate] = {}
    for aggregate in existing_aggregates:
        if not isinstance(aggregate, GuidelineExportAggregate):
            raise GuidelineImportExportError(
                "guideline_import_existing_aggregate_invalid"
            )
        if aggregate.guideline_id in existing_by_id:
            raise GuidelineImportExportError(
                "guideline_import_existing_aggregate_duplicate"
            )
        existing_by_id[aggregate.guideline_id] = aggregate
        refs.extend(
            ExistingGuidelineRevision.from_revision(item.revision)
            for item in aggregate.revisions
        )
    existing: dict[tuple[str, str], ExistingGuidelineRevision] = {}
    for ref in refs:
        key = (ref.guideline_id, ref.semantic_version)
        previous = existing.get(key)
        if previous is not None and previous != ref:
            raise GuidelineImportExportError(
                "guideline_import_existing_revision_duplicate"
            )
        existing[key] = ref
    existing_bindings: dict[
        tuple[str, str, str, int],
        GuidelineExportBinding,
    ] = {}
    for existing_aggregate in existing_by_id.values():
        for exported_binding in existing_aggregate.bindings:
            key = exported_binding.candidate_key
            previous = existing_bindings.get(key)
            if (
                previous is not None
                and previous.binding_digest != exported_binding.binding_digest
            ):
                raise GuidelineImportExportError(
                    "guideline_import_existing_binding_conflict"
                )
            existing_bindings[key] = exported_binding

    entries: list[GuidelineImportPlanEntry] = []
    for original in envelope.guidelines:
        identity_conflicts: list[str] = []
        identity_diagnostics: list[str] = []
        exact_existing = existing_by_id.get(original.guideline_id)
        intended_board_id = (
            target_board_id
            if original.identity.scope is GuidelineScope.INLINE
            and target_board_id is not None
            else original.identity.board_id
        )
        if exact_existing is not None:
            existing_identity = exact_existing.identity
            if existing_identity.owner_id != target_owner_id:
                identity_conflicts.append("identity_owner_conflict")
            if existing_identity.scope is not original.identity.scope:
                identity_conflicts.append("identity_scope_conflict")
            elif (
                original.identity.scope is GuidelineScope.INLINE
                and existing_identity.board_id != intended_board_id
            ):
                identity_conflicts.append("identity_board_conflict")
            if existing_identity.created_at != original.identity.created_at:
                identity_conflicts.append("identity_created_at_conflict")
            if existing_identity.context_scope is not original.identity.context_scope:
                identity_conflicts.append("identity_context_scope_conflict")
            if exact_existing.head.head_revision > original.head.head_revision:
                identity_diagnostics.append("local_head_ahead_source_skipped")
            if (exact_existing.retirement is None) != (original.retirement is None):
                identity_conflicts.append("identity_retirement_state_conflict")
            elif (
                exact_existing.retirement is not None
                and exact_existing.retirement != original.retirement
            ):
                identity_conflicts.append("identity_retirement_history_conflict")
        if target_board_id is None:
            owner_aligned_identity = (
                original.identity
                if original.identity.owner_id == target_owner_id
                else replace(original.identity, owner_id=target_owner_id)
            )
            owner_aligned = replace(
                original,
                identity=owner_aligned_identity,
            )
            aggregate_identical = (
                not identity_conflicts and exact_existing == owner_aligned
            )
        else:
            aggregate_identical = False
        aggregate = _remap_aggregate(
            original,
            target_owner_id,
            target_board_id,
            skip_identical=aggregate_identical,
        )
        actions: list[GuidelineImportRevisionAction] = []
        for exported_revision in aggregate.revisions:
            revision = exported_revision.revision
            existing_revision = existing.get(
                (revision.guideline_id, revision.semantic_version)
            )
            if existing_revision is None:
                disposition = GuidelineImportRevisionDisposition.CREATE
            elif existing_revision.revision_digest == revision.content_digest:
                disposition = GuidelineImportRevisionDisposition.SKIP_IDENTICAL
            else:
                disposition = GuidelineImportRevisionDisposition.CONFLICT
            actions.append(
                GuidelineImportRevisionAction(
                    guideline_id=revision.guideline_id,
                    revision_id=revision.revision_id,
                    semantic_version=revision.semantic_version,
                    revision_digest=revision.content_digest,
                    disposition=disposition,
                    resolved_revision_id=(
                        existing_revision.revision_id
                        if disposition
                        is GuidelineImportRevisionDisposition.SKIP_IDENTICAL
                        and existing_revision is not None
                        else revision.revision_id
                    ),
                )
            )

        diagnostics = [
            *aggregate.migration_notes,
            *identity_diagnostics,
        ]
        planned_bindings = {
            exported_binding.candidate_key: exported_binding
            for exported_binding in aggregate.bindings
        }
        source_histories: dict[
            tuple[str, str],
            list[GuidelineExportBinding],
        ] = {}
        binding_conflicts: list[str] = []
        for source_binding in original.bindings:
            source_histories.setdefault(
                (
                    source_binding.binding.board_id,
                    source_binding.binding.binding_id,
                ),
                [],
            ).append(source_binding)
            target_candidate_key = (
                target_board_id or source_binding.binding.board_id,
                source_binding.binding.guideline_id,
                source_binding.binding.binding_id,
                source_binding.binding.binding_revision,
            )
            known_binding = existing_bindings.get(target_candidate_key)
            planned_binding = planned_bindings.get(target_candidate_key)
            if planned_binding is None:
                raise GuidelineImportExportError(
                    "guideline_import_planned_binding_missing"
                )
            if known_binding is not None and known_binding.binding_digest not in {
                source_binding.binding_digest,
                planned_binding.binding_digest,
            }:
                binding_conflicts.append(
                    ":".join(
                        (
                            source_binding.binding.guideline_id,
                            target_candidate_key[0],
                            source_binding.binding.binding_id,
                            str(source_binding.binding.binding_revision),
                        )
                    )
                )
        candidates: list[GuidelineImportBindingCandidate] = []
        for (source_board_id, binding_id), history in sorted(source_histories.items()):
            ordered_history = tuple(
                sorted(
                    history,
                    key=lambda item: item.binding.binding_revision,
                )
            )
            latest = ordered_history[-1].binding
            history_identical = all(
                (
                    known := existing_bindings.get(
                        (
                            target_board_id or source_binding.binding.board_id,
                            source_binding.binding.guideline_id,
                            source_binding.binding.binding_id,
                            source_binding.binding.binding_revision,
                        )
                    )
                )
                is not None
                and known.binding_digest
                in {
                    source_binding.binding_digest,
                    planned_bindings[
                        (
                            target_board_id or source_binding.binding.board_id,
                            source_binding.binding.guideline_id,
                            source_binding.binding.binding_id,
                            source_binding.binding.binding_revision,
                        )
                    ].binding_digest,
                }
                for source_binding in ordered_history
            )
            if aggregate_identical or history_identical:
                candidate_disposition = (
                    GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
                )
            elif (
                original.retirement is not None
                or latest.state is GuidelineBindingState.UNLINKED
            ):
                candidate_disposition = (
                    GuidelineImportBindingDisposition.STORE_INERT_HISTORY
                )
            else:
                candidate_disposition = (
                    GuidelineImportBindingDisposition.PENDING_ADOPTION
                )
            candidates.append(
                GuidelineImportBindingCandidate(
                    source_board_id=source_board_id,
                    target_board_id=target_board_id or source_board_id,
                    binding_id=binding_id,
                    source_history=ordered_history,
                    disposition=candidate_disposition,
                )
            )
        candidate_dispositions = {candidate.disposition for candidate in candidates}
        if not candidates:
            binding_disposition = GuidelineImportBindingDisposition.NO_BINDINGS
        elif candidate_dispositions == {
            GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
        }:
            binding_disposition = (
                GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
            )
        elif GuidelineImportBindingDisposition.PENDING_ADOPTION in (
            candidate_dispositions
        ):
            binding_disposition = GuidelineImportBindingDisposition.PENDING_ADOPTION
            diagnostics.append(
                "source_active_binding_pending_explicit_preview_and_adoption"
            )
        else:
            binding_disposition = GuidelineImportBindingDisposition.STORE_INERT_HISTORY
        entries.append(
            GuidelineImportPlanEntry(
                aggregate=aggregate,
                revision_actions=tuple(actions),
                binding_disposition=binding_disposition,
                binding_candidates=tuple(candidates),
                identity_conflicts=tuple(identity_conflicts),
                binding_conflicts=tuple(binding_conflicts),
                diagnostics=tuple(diagnostics),
            )
        )

    has_conflict = any(entry.has_conflict for entry in entries)
    if has_conflict:
        status = GuidelineImportTransactionStatus.ROLLED_BACK
        error_code = "conflict"
    elif dry_run:
        status = GuidelineImportTransactionStatus.DRY_RUN
        error_code = None
    else:
        status = GuidelineImportTransactionStatus.PLANNED
        error_code = None
    return GuidelineImportPlan(
        envelope=envelope,
        entries=tuple(entries),
        dry_run=dry_run,
        transaction_status=status,
        target_owner_id=target_owner_id,
        target_board_id=target_board_id,
        import_digest=guideline_import_digest(
            envelope_content_digest=envelope.content_digest,
            target_owner_id=target_owner_id,
            target_board_id=target_board_id,
            dry_run=dry_run,
        ),
        error_code=error_code,
        overwritten_row_count=0,
    )


__all__ = [
    "GUIDELINE_EXPORT_CONTRACT_VERSION",
    "GUIDELINE_EXPORT_KIND",
    "GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION",
    "GUIDELINE_EXPORT_LEGACY_SCHEMA_VERSION",
    "GUIDELINE_EXPORT_SCHEMA_VERSION",
    "ExistingGuidelineRevision",
    "GuidelineBindingMaterialization",
    "GuidelineExportAggregate",
    "GuidelineExportBinding",
    "GuidelineExportEnvelope",
    "GuidelineExportRevision",
    "GuidelineExportSnapshot",
    "GuidelineHistoryStatus",
    "GuidelineImportBindingCandidate",
    "GuidelineImportBindingDisposition",
    "GuidelineImportExportError",
    "GuidelineImportPlan",
    "GuidelineImportPlanEntry",
    "GuidelineImportResult",
    "GuidelineImportRevisionAction",
    "GuidelineImportRevisionDisposition",
    "GuidelineImportTransactionStatus",
    "build_guideline_export_v2",
    "canonical_guideline_json_bytes",
    "canonical_guideline_sha256",
    "guideline_export_json_bytes",
    "guideline_export_payload",
    "guideline_import_digest",
    "parse_guideline_export",
    "plan_guideline_import",
]
