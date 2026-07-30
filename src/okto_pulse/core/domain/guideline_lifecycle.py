"""Pure lifecycle rules for immutable guideline policy aggregates.

The module owns the deterministic part of SK-B/B04: canonical partial patches,
minimum SemVer classification, immutable terminal tombstones, and append-only
binding transitions.  It deliberately has no transport, database, clock, UUID,
or framework dependency.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import IntEnum
from functools import total_ordering
from typing import TypeAlias

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS,
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    Guideline,
    GuidelineBindingState,
    GuidelineContextScope,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelinePolicyContractError,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRule,
    GuidelineScope,
)
from okto_pulse.core.domain.guideline_predicate_catalog import (
    validate_guideline_rule,
)


GUIDELINE_LIFECYCLE_CONTRACT_VERSION = "guideline-lifecycle/v1"
GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION = "guideline-revision-digest/v1"
GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION = "guideline-request-digest/v1"

_SEMVER_RE = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


def _canonical_json_bytes(value: object) -> bytes:
    """Preserve the byte-exact JSON encoding established by B03."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


class GuidelineLifecycleError(GuidelinePolicyContractError):
    """A requested lifecycle transition violates the B04 contract."""


class GuidelineVersionUnderBump(GuidelineLifecycleError):
    """A declared version cannot authorize the classified semantic change."""

    def __init__(
        self,
        *,
        current: SemanticVersion,
        declared: SemanticVersion,
        minimum: SemanticVersion,
        required_bump: GuidelineVersionBump,
    ) -> None:
        self.current = current
        self.declared = declared
        self.minimum = minimum
        self.required_bump = required_bump
        super().__init__("guideline_semver_below_minimum")


class GuidelineVersionBump(IntEnum):
    PATCH = 1
    MINOR = 2
    MAJOR = 3


@total_ordering
@dataclass(frozen=True, slots=True)
class SemanticVersion:
    """SemVer 2.0 value with precedence-aware comparison.

    Build metadata is retained for display but intentionally ignored by
    precedence, as required by SemVer. Numeric prerelease identifiers compare
    numerically and sort below non-numeric identifiers.
    """

    major: int
    minor: int
    patch: int
    prerelease: tuple[str, ...] = ()
    build: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("major", "minor", "patch"):
            value = getattr(self, name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value >= 10**GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS
            ):
                raise GuidelineLifecycleError("guideline_semver_invalid")
        if (
            not isinstance(self.prerelease, tuple)
            or not isinstance(self.build, tuple)
            or any(
                not isinstance(identifier, str)
                for identifier in (*self.prerelease, *self.build)
            )
        ):
            raise GuidelineLifecycleError("guideline_semver_invalid")
        for identifier in (*self.prerelease, *self.build):
            if not identifier or not re.fullmatch(r"[0-9A-Za-z-]+", identifier):
                raise GuidelineLifecycleError("guideline_semver_invalid")
        for identifier in self.prerelease:
            if re.fullmatch(r"[0-9]+", identifier) and (
                len(identifier) > GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS
                or (len(identifier) > 1 and identifier.startswith("0"))
            ):
                raise GuidelineLifecycleError("guideline_semver_invalid")

    @classmethod
    def parse(cls, value: str) -> SemanticVersion:
        if not isinstance(value, str):
            raise GuidelineLifecycleError("guideline_semver_invalid")
        match = _SEMVER_RE.fullmatch(value)
        if match is None:
            raise GuidelineLifecycleError("guideline_semver_invalid")
        if any(
            len(component) > GUIDELINE_SEMVER_MAX_NUMERIC_DIGITS
            for component in match.group(1, 2, 3)
        ):
            raise GuidelineLifecycleError("guideline_semver_invalid")
        prerelease = tuple(match.group(4).split(".")) if match.group(4) else ()
        build = tuple(match.group(5).split(".")) if match.group(5) else ()
        try:
            return cls(
                major=int(match.group(1)),
                minor=int(match.group(2)),
                patch=int(match.group(3)),
                prerelease=prerelease,
                build=build,
            )
        except (ValueError, OverflowError) as exc:
            raise GuidelineLifecycleError("guideline_semver_invalid") from exc

    def __str__(self) -> str:
        value = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            value += "-" + ".".join(self.prerelease)
        if self.build:
            value += "+" + ".".join(self.build)
        return value

    @property
    def core(self) -> tuple[int, int, int]:
        return (self.major, self.minor, self.patch)

    @staticmethod
    def _compare_prerelease(
        left: tuple[str, ...],
        right: tuple[str, ...],
    ) -> int:
        if not left and not right:
            return 0
        if not left:
            return 1
        if not right:
            return -1
        for left_id, right_id in zip(left, right, strict=False):
            if left_id == right_id:
                continue
            left_numeric = left_id.isdigit()
            right_numeric = right_id.isdigit()
            if left_numeric and right_numeric:
                if len(left_id) != len(right_id):
                    return -1 if len(left_id) < len(right_id) else 1
                return -1 if left_id < right_id else 1
            if left_numeric != right_numeric:
                return -1 if left_numeric else 1
            return -1 if left_id < right_id else 1
        if len(left) == len(right):
            return 0
        return -1 if len(left) < len(right) else 1

    def _precedence(self, other: SemanticVersion) -> int:
        left_core = (self.major, self.minor, self.patch)
        right_core = (other.major, other.minor, other.patch)
        if left_core != right_core:
            return -1 if left_core < right_core else 1
        return self._compare_prerelease(self.prerelease, other.prerelease)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._precedence(other) < 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return False
        return self._precedence(other) == 0

    def __hash__(self) -> int:
        """Hash SemVer precedence identity; build metadata is non-precedential."""

        return hash((self.major, self.minor, self.patch, self.prerelease))

    def minimum_successor(self, bump: GuidelineVersionBump) -> SemanticVersion:
        if not isinstance(bump, GuidelineVersionBump):
            raise GuidelineLifecycleError("guideline_version_bump_invalid")
        if bump is GuidelineVersionBump.MAJOR:
            return SemanticVersion(self.major + 1, 0, 0)
        if bump is GuidelineVersionBump.MINOR:
            return SemanticVersion(self.major, self.minor + 1, 0)
        return SemanticVersion(self.major, self.minor, self.patch + 1)


@dataclass(frozen=True, slots=True)
class GuidelineRevisionPatch:
    """Partial patch; ``None`` means the field was not supplied."""

    title: str | None = None
    content: str | None = None
    tags: tuple[str, ...] | None = None
    rules: tuple[GuidelineRule, ...] | None = None

    def __post_init__(self) -> None:
        if self.title is not None:
            object.__setattr__(
                self,
                "title",
                _canonical_text(
                    self.title,
                    code="guideline_patch_title_required",
                ),
            )
        if self.content is not None:
            object.__setattr__(
                self,
                "content",
                _canonical_text(
                    self.content,
                    code="guideline_patch_content_required",
                ),
            )
        if self.tags is not None:
            object.__setattr__(self, "tags", _canonical_tags(self.tags))
        if self.rules is not None:
            object.__setattr__(self, "rules", _canonical_rules(self.rules))


@dataclass(frozen=True, slots=True)
class GuidelinePatchPlan:
    """Deterministic output consumed by the persistence/application layer."""

    title: str
    content: str
    tags: tuple[str, ...]
    rules: tuple[GuidelineRule, ...]
    minimum_bump: GuidelineVersionBump | None
    semantic_version: str

    @property
    def is_noop(self) -> bool:
        return self.minimum_bump is None


def _canonical_text(value: str, *, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelineLifecycleError(code)
    return value.strip()


def _canonical_tags(values: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(values, tuple | list):
        raise GuidelineLifecycleError("guideline_patch_tags_invalid")
    normalized = tuple(
        _canonical_text(value, code="guideline_patch_tags_invalid") for value in values
    )
    if len(set(normalized)) != len(normalized):
        raise GuidelineLifecycleError("guideline_patch_tags_duplicate")
    return tuple(sorted(normalized))


def _predicate_sort_key(rule_predicate: object) -> bytes:
    return _canonical_json_bytes(
        {
            "predicate_code": rule_predicate.predicate_code,
            "parameters": rule_predicate.parameters,
        }
    )


def _canonical_rule(rule: GuidelineRule) -> GuidelineRule:
    canonical = validate_guideline_rule(rule)
    return replace(
        canonical,
        predicates=tuple(sorted(canonical.predicates, key=_predicate_sort_key)),
    )


def _canonical_rules(values: tuple[GuidelineRule, ...]) -> tuple[GuidelineRule, ...]:
    if not isinstance(values, tuple | list) or any(
        not isinstance(value, GuidelineRule) for value in values
    ):
        raise GuidelineLifecycleError("guideline_patch_rules_invalid")
    rules = tuple(_canonical_rule(value) for value in values)
    if len({rule.rule_id for rule in rules}) != len(rules):
        raise GuidelineLifecycleError("guideline_patch_duplicate_rule_id")
    if len({rule.code for rule in rules}) != len(rules):
        raise GuidelineLifecycleError("guideline_patch_duplicate_rule_code")
    return tuple(sorted(rules, key=lambda rule: rule.code))


def _rule_manifest(rule: GuidelineRule) -> dict[str, object]:
    return {
        "rule_id": rule.rule_id,
        "code": rule.code,
        "title": rule.title,
        "description": rule.description,
        "target_entity_types": [
            entity_type.value for entity_type in rule.target_entity_types
        ],
        "predicates": [
            {
                "predicate_code": predicate.predicate_code,
                "parameters": [[key, value] for key, value in predicate.parameters],
            }
            for predicate in rule.predicates
        ],
        "enforcement": rule.enforcement.value,
        "operator": rule.operator.value,
        "waivable": rule.waivable,
        "policy_class": rule.policy_class,
    }


def guideline_revision_content_digest_v1(
    *,
    title: str,
    content: str,
    rules: tuple[GuidelineRule, ...] | list[GuidelineRule] = (),
    tags: tuple[str, ...] | list[str] = (),
) -> str:
    """Digest one canonical immutable guideline revision snapshot.

    This is Core's single source of truth for the
    ``guideline-revision-digest/v1`` contract.  Adapters must call this helper
    instead of independently serializing revision payloads.
    """

    # B03 established the v1 bytes before B04 introduced canonical command
    # snapshots.  Preserve title/content bytes and predicate order here; B04
    # planners canonicalize those inputs *before* they call this helper.
    if not isinstance(title, str) or not title.strip():
        raise GuidelineLifecycleError("guideline_revision_title_required")
    if not isinstance(content, str) or not content.strip():
        raise GuidelineLifecycleError("guideline_revision_content_required")
    canonical_tags = _canonical_tags(tags)
    if not isinstance(rules, tuple | list) or any(
        not isinstance(rule, GuidelineRule) for rule in rules
    ):
        raise GuidelineLifecycleError("guideline_revision_rules_invalid")
    canonical_rules = tuple(sorted(rules, key=lambda rule: rule.code))
    return _canonical_sha256(
        {
            "contract": GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION,
            "title": title,
            "content": content,
            "tags": canonical_tags,
            "rules": tuple(_rule_manifest(rule) for rule in canonical_rules),
        }
    )


# Compatibility-friendly name matching the original B03 adapter helper.
guideline_revision_content_digest = guideline_revision_content_digest_v1


def _rule_bump(
    previous: GuidelineRule,
    proposed: GuidelineRule,
) -> GuidelineVersionBump | None:
    bump: GuidelineVersionBump | None = None

    def record(candidate: GuidelineVersionBump) -> None:
        nonlocal bump
        bump = candidate if bump is None else max(bump, candidate)

    if previous.code != proposed.code:
        record(GuidelineVersionBump.MAJOR)
    if previous.predicates != proposed.predicates:
        record(GuidelineVersionBump.MAJOR)
    if previous.operator is not proposed.operator:
        record(GuidelineVersionBump.MAJOR)
    if previous.policy_class != proposed.policy_class:
        record(GuidelineVersionBump.MAJOR)

    old_targets = set(previous.target_entity_types)
    new_targets = set(proposed.target_entity_types)
    if old_targets - new_targets:
        record(GuidelineVersionBump.MAJOR)
    if new_targets - old_targets:
        record(
            GuidelineVersionBump.MINOR
            if proposed.enforcement is GuidelineEnforcement.ADVISORY
            else GuidelineVersionBump.MAJOR
        )

    if previous.enforcement is not proposed.enforcement:
        record(
            GuidelineVersionBump.MAJOR
            if proposed.enforcement is GuidelineEnforcement.BLOCKING
            else GuidelineVersionBump.MINOR
        )
    if previous.waivable != proposed.waivable:
        record(
            GuidelineVersionBump.MAJOR
            if not proposed.waivable
            else GuidelineVersionBump.MINOR
        )
    if previous.title != proposed.title or previous.description != proposed.description:
        record(GuidelineVersionBump.PATCH)
    return bump


def classify_guideline_change(
    previous: GuidelineRevision,
    *,
    title: str,
    content: str,
    tags: tuple[str, ...],
    rules: tuple[GuidelineRule, ...],
) -> GuidelineVersionBump | None:
    """Return the maximum required severity across a mixed change."""

    title = _canonical_text(title, code="guideline_patch_title_required")
    content = _canonical_text(
        content,
        code="guideline_patch_content_required",
    )
    tags = _canonical_tags(tags)
    rules = _canonical_rules(rules)
    previous_title = _canonical_text(
        previous.title,
        code="guideline_revision_title_required",
    )
    previous_content = _canonical_text(
        previous.content,
        code="guideline_revision_content_required",
    )
    previous_tags = _canonical_tags(previous.tags)
    previous_rules = _canonical_rules(previous.rules)

    bump: GuidelineVersionBump | None = None

    def record(candidate: GuidelineVersionBump | None) -> None:
        nonlocal bump
        if candidate is not None:
            bump = candidate if bump is None else max(bump, candidate)

    if previous_title != title or previous_content != content or previous_tags != tags:
        record(GuidelineVersionBump.PATCH)

    old_by_id = {rule.rule_id: rule for rule in previous_rules}
    new_by_id = {rule.rule_id: rule for rule in rules}
    if set(old_by_id) - set(new_by_id):
        record(GuidelineVersionBump.MAJOR)
    for rule_id in set(new_by_id) - set(old_by_id):
        record(
            GuidelineVersionBump.MAJOR
            if new_by_id[rule_id].enforcement is GuidelineEnforcement.BLOCKING
            else GuidelineVersionBump.MINOR
        )
    for rule_id in set(old_by_id) & set(new_by_id):
        record(_rule_bump(old_by_id[rule_id], new_by_id[rule_id]))
    return bump


def plan_guideline_patch(
    current: GuidelineRevision,
    patch: GuidelineRevisionPatch,
    *,
    requested_semantic_version: str | None = None,
) -> GuidelinePatchPlan:
    """Canonicalize a partial patch and enforce its minimum SemVer bump."""

    requested_version = (
        None
        if requested_semantic_version is None
        else SemanticVersion.parse(requested_semantic_version)
    )
    title = (
        _canonical_text(
            current.title,
            code="guideline_revision_title_required",
        )
        if patch.title is None
        else _canonical_text(
            patch.title,
            code="guideline_patch_title_required",
        )
    )
    content = (
        _canonical_text(
            current.content,
            code="guideline_revision_content_required",
        )
        if patch.content is None
        else _canonical_text(
            patch.content,
            code="guideline_patch_content_required",
        )
    )
    tags = (
        _canonical_tags(current.tags)
        if patch.tags is None
        else _canonical_tags(patch.tags)
    )
    rules = (
        _canonical_rules(current.rules)
        if patch.rules is None
        else _canonical_rules(patch.rules)
    )
    minimum_bump = classify_guideline_change(
        current,
        title=title,
        content=content,
        tags=tags,
        rules=rules,
    )
    if minimum_bump is None:
        return GuidelinePatchPlan(
            title=title,
            content=content,
            tags=tags,
            rules=rules,
            minimum_bump=None,
            semantic_version=current.semantic_version,
        )

    current_semver = SemanticVersion.parse(current.semantic_version)
    minimum = current_semver.minimum_successor(minimum_bump)
    proposed = minimum if requested_version is None else requested_version
    if proposed <= current_semver or proposed.core < minimum.core:
        raise GuidelineVersionUnderBump(
            current=current_semver,
            declared=proposed,
            minimum=minimum,
            required_bump=minimum_bump,
        )
    return GuidelinePatchPlan(
        title=title,
        content=content,
        tags=tags,
        rules=rules,
        minimum_bump=minimum_bump,
        semantic_version=str(proposed),
    )


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelineLifecycleError(code)
    return value.strip()


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelineLifecycleError(code)
    return value.astimezone(timezone.utc)


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise GuidelineLifecycleError(code)
    return value


def _non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise GuidelineLifecycleError(code)
    return value


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise GuidelineLifecycleError(code)
    return normalized


def _rules_manifest(rules: tuple[GuidelineRule, ...]) -> tuple[dict[str, object], ...]:
    return tuple(_rule_manifest(rule) for rule in _canonical_rules(rules))


def guideline_request_digest_v1(
    *,
    operation: str,
    scope_id: str,
    payload: Mapping[str, object],
) -> str:
    """Bind one idempotent lifecycle request to a canonical semantic payload."""

    operation = _required_text(
        operation,
        "guideline_request_operation_required",
    )
    scope_id = _required_text(
        scope_id,
        "guideline_request_scope_id_required",
    )
    if not isinstance(payload, Mapping):
        raise GuidelineLifecycleError("guideline_request_payload_invalid")
    try:
        return _canonical_sha256(
            {
                "contract": GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION,
                "operation": operation,
                "scope_id": scope_id,
                "payload": dict(payload),
            }
        )
    except (TypeError, ValueError) as exc:
        raise GuidelineLifecycleError("guideline_request_payload_invalid") from exc


@dataclass(frozen=True, slots=True)
class GuidelineCreateCommand:
    """Pure create intent with every identity, actor, and clock injected."""

    guideline_id: str
    revision_id: str
    owner_id: str
    scope: GuidelineScope
    title: str
    content: str
    created_by: str
    created_at: datetime
    idempotency_key: str
    board_id: str | None = None
    context_scope: GuidelineContextScope = GuidelineContextScope.ALL
    tags: tuple[str, ...] = ()
    rules: tuple[GuidelineRule, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.scope, GuidelineScope):
            raise GuidelineLifecycleError("guideline_create_scope_invalid")
        if not isinstance(self.context_scope, GuidelineContextScope):
            raise GuidelineLifecycleError("guideline_create_context_scope_invalid")
        for field_name in (
            "guideline_id",
            "revision_id",
            "owner_id",
            "created_by",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_create_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "title",
            _canonical_text(
                self.title,
                code="guideline_create_title_required",
            ),
        )
        object.__setattr__(
            self,
            "content",
            _canonical_text(
                self.content,
                code="guideline_create_content_required",
            ),
        )
        board_id = _optional_text(
            self.board_id,
            "guideline_create_board_id_invalid",
        )
        if self.scope is GuidelineScope.INLINE and board_id is None:
            raise GuidelineLifecycleError("inline_guideline_board_id_required")
        if self.scope is GuidelineScope.GLOBAL and board_id is not None:
            raise GuidelineLifecycleError("global_guideline_board_id_forbidden")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "guideline_create_time_invalid",
            ),
        )
        object.__setattr__(self, "tags", _canonical_tags(self.tags))
        object.__setattr__(self, "rules", _canonical_rules(self.rules))


@dataclass(frozen=True, slots=True)
class GuidelineCreateResult:
    """Atomic create bundle; ``expected_head_revision=0`` fences absence."""

    command: GuidelineCreateCommand
    guideline: Guideline
    revision: GuidelineRevision
    head: GuidelineHead
    idempotency_key: str
    request_digest: str
    expected_head_revision: int = 0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.command, GuidelineCreateCommand)
            or not isinstance(self.guideline, Guideline)
            or not isinstance(self.revision, GuidelineRevision)
            or not isinstance(self.head, GuidelineHead)
        ):
            raise GuidelineLifecycleError("guideline_create_result_invalid")
        command = self.command
        if self.expected_head_revision != 0:
            raise GuidelineLifecycleError("guideline_create_expected_head_invalid")
        if (
            self.guideline.guideline_id != self.revision.guideline_id
            or self.guideline.guideline_id != self.head.guideline_id
            or self.revision.revision_id != self.head.revision_id
            or self.revision.revision_number != 1
            or self.head.revision_number != 1
            or self.head.head_revision != 1
            or self.revision.semantic_version != "1.0.0"
            or self.head.semantic_version != "1.0.0"
            or self.revision.parent_revision_id is not None
            or self.guideline.created_at != self.revision.created_at
            or self.guideline.created_at != self.head.updated_at
            or self.guideline.guideline_id != command.guideline_id
            or self.guideline.owner_id != command.owner_id
            or self.guideline.scope is not command.scope
            or self.guideline.board_id != command.board_id
            or self.guideline.context_scope is not command.context_scope
            or self.revision.revision_id != command.revision_id
            or self.revision.title != command.title
            or self.revision.content != command.content
            or self.revision.tags != command.tags
            or self.revision.rules != command.rules
            or self.revision.created_by != command.created_by
            or self.revision.created_at != command.created_at
        ):
            raise GuidelineLifecycleError("guideline_create_result_mismatch")
        expected_digest = guideline_revision_content_digest_v1(
            title=self.revision.title,
            content=self.revision.content,
            tags=self.revision.tags,
            rules=self.revision.rules,
        )
        if self.revision.content_digest != expected_digest:
            raise GuidelineLifecycleError("guideline_create_revision_digest_mismatch")
        idempotency_key = _required_text(
            self.idempotency_key,
            "guideline_create_idempotency_key_required",
        )
        request_digest = _sha256(
            self.request_digest,
            "guideline_create_request_digest_invalid",
        )
        if (
            idempotency_key != command.idempotency_key
            or request_digest != guideline_create_request_digest_v1(command)
        ):
            raise GuidelineLifecycleError("guideline_create_result_request_mismatch")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_digest", request_digest)


def guideline_create_request_digest_v1(
    command: GuidelineCreateCommand,
) -> str:
    """Hash canonical create intent, excluding injected output IDs and clock."""

    if not isinstance(command, GuidelineCreateCommand):
        raise GuidelineLifecycleError("guideline_create_command_invalid")
    return guideline_request_digest_v1(
        operation="create",
        scope_id=command.guideline_id,
        payload={
            "guideline_id": command.guideline_id,
            "owner_id": command.owner_id,
            "scope": command.scope.value,
            "board_id": command.board_id,
            "context_scope": command.context_scope.value,
            "title": command.title,
            "content": command.content,
            "tags": command.tags,
            "rules": _rules_manifest(command.rules),
            "actor_id": command.created_by,
        },
    )


def plan_guideline_creation(
    command: GuidelineCreateCommand,
) -> GuidelineCreateResult:
    """Build the immutable identity, ``1.0.0`` revision, and first head."""

    if not isinstance(command, GuidelineCreateCommand):
        raise GuidelineLifecycleError("guideline_create_command_invalid")
    guideline = Guideline(
        guideline_id=command.guideline_id,
        owner_id=command.owner_id,
        scope=command.scope,
        board_id=command.board_id,
        context_scope=command.context_scope,
        created_at=command.created_at,
    )
    digest = guideline_revision_content_digest_v1(
        title=command.title,
        content=command.content,
        tags=command.tags,
        rules=command.rules,
    )
    revision = GuidelineRevision(
        revision_id=command.revision_id,
        guideline_id=command.guideline_id,
        revision_number=1,
        semantic_version="1.0.0",
        title=command.title,
        content=command.content,
        content_digest=digest,
        rules=command.rules,
        tags=command.tags,
        created_by=command.created_by,
        created_at=command.created_at,
    )
    head = GuidelineHead(
        guideline_id=command.guideline_id,
        revision_id=command.revision_id,
        revision_number=1,
        semantic_version="1.0.0",
        head_revision=1,
        updated_at=command.created_at,
    )
    return GuidelineCreateResult(
        command=command,
        guideline=guideline,
        revision=revision,
        head=head,
        idempotency_key=command.idempotency_key,
        request_digest=guideline_create_request_digest_v1(command),
    )


@dataclass(frozen=True, slots=True)
class GuidelinePatchCommand:
    """Pure patch intent bound to an exact immutable revision/head snapshot."""

    current_revision: GuidelineRevision
    current_head: GuidelineHead
    patch: GuidelineRevisionPatch
    next_revision_id: str
    actor_id: str
    occurred_at: datetime
    idempotency_key: str
    declared_semantic_version: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.current_revision, GuidelineRevision):
            raise GuidelineLifecycleError("guideline_patch_current_revision_invalid")
        if not isinstance(self.current_head, GuidelineHead):
            raise GuidelineLifecycleError("guideline_patch_current_head_invalid")
        if not isinstance(self.patch, GuidelineRevisionPatch):
            raise GuidelineLifecycleError("guideline_patch_invalid")
        for field_name in (
            "next_revision_id",
            "actor_id",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_patch_{field_name}_required",
                ),
            )
        current = self.current_revision
        head = self.current_head
        if (
            current.guideline_id != head.guideline_id
            or current.revision_id != head.revision_id
            or current.revision_number != head.revision_number
            or current.semantic_version != head.semantic_version
            or self.next_revision_id == current.revision_id
        ):
            raise GuidelineLifecycleError("guideline_patch_snapshot_mismatch")
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "guideline_patch_time_invalid",
            ),
        )
        if self.declared_semantic_version is not None:
            parsed = SemanticVersion.parse(self.declared_semantic_version)
            object.__setattr__(
                self,
                "declared_semantic_version",
                str(parsed),
            )
        if self.occurred_at <= head.updated_at:
            raise GuidelineLifecycleError("guideline_patch_time_not_monotonic")


@dataclass(frozen=True, slots=True)
class GuidelinePatchApplied:
    status: str
    command: GuidelinePatchCommand
    revision: GuidelineRevision
    head: GuidelineHead
    minimum_bump: GuidelineVersionBump
    expected_head_revision: int
    expected_revision_id: str
    expected_revision_number: int
    expected_semantic_version: str
    expected_revision_digest: str
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if self.status != "applied":
            raise GuidelineLifecycleError("guideline_patch_result_status_invalid")
        if (
            not isinstance(self.command, GuidelinePatchCommand)
            or not isinstance(self.revision, GuidelineRevision)
            or not isinstance(self.head, GuidelineHead)
            or not isinstance(self.minimum_bump, GuidelineVersionBump)
        ):
            raise GuidelineLifecycleError("guideline_patch_result_invalid")
        expected_head_revision = _positive_int(
            self.expected_head_revision,
            "guideline_patch_expected_head_invalid",
        )
        expected_revision_id = _required_text(
            self.expected_revision_id,
            "guideline_patch_expected_revision_id_required",
        )
        expected_revision_number = _positive_int(
            self.expected_revision_number,
            "guideline_patch_expected_revision_number_invalid",
        )
        expected_semantic_version = str(
            SemanticVersion.parse(self.expected_semantic_version)
        )
        expected_revision_digest = _sha256(
            self.expected_revision_digest,
            "guideline_patch_expected_revision_digest_invalid",
        )
        revision = self.revision
        head = self.head
        expected_digest = guideline_revision_content_digest_v1(
            title=revision.title,
            content=revision.content,
            tags=revision.tags,
            rules=revision.rules,
        )
        current_version = SemanticVersion.parse(expected_semantic_version)
        proposed_version = SemanticVersion.parse(revision.semantic_version)
        minimum_version = current_version.minimum_successor(self.minimum_bump)
        if (
            revision.parent_revision_id != expected_revision_id
            or revision.revision_number != expected_revision_number + 1
            or revision.content_digest != expected_digest
            or revision.guideline_id != head.guideline_id
            or revision.revision_id != head.revision_id
            or revision.revision_number != head.revision_number
            or revision.semantic_version != head.semantic_version
            or revision.created_at != head.updated_at
            or head.head_revision != expected_head_revision + 1
            or proposed_version <= current_version
            or proposed_version.core < minimum_version.core
        ):
            raise GuidelineLifecycleError("guideline_patch_result_bundle_mismatch")
        object.__setattr__(
            self,
            "expected_revision_id",
            expected_revision_id,
        )
        object.__setattr__(
            self,
            "expected_revision_number",
            expected_revision_number,
        )
        object.__setattr__(
            self,
            "expected_semantic_version",
            expected_semantic_version,
        )
        object.__setattr__(
            self,
            "expected_revision_digest",
            expected_revision_digest,
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "guideline_patch_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "guideline_patch_request_digest_invalid",
            ),
        )
        plan = plan_guideline_patch(
            self.command.current_revision,
            self.command.patch,
            requested_semantic_version=(self.command.declared_semantic_version),
        )
        if (
            plan.is_noop
            or self.minimum_bump is not plan.minimum_bump
            or revision.revision_id != self.command.next_revision_id
            or revision.guideline_id != self.command.current_revision.guideline_id
            or revision.title != plan.title
            or revision.content != plan.content
            or revision.tags != plan.tags
            or revision.rules != plan.rules
            or revision.semantic_version != plan.semantic_version
            or revision.created_by != self.command.actor_id
            or revision.created_at != self.command.occurred_at
            or expected_head_revision != self.command.current_head.head_revision
            or expected_revision_id != self.command.current_revision.revision_id
            or expected_revision_number != self.command.current_revision.revision_number
            or expected_semantic_version
            != self.command.current_revision.semantic_version
            or expected_revision_digest != self.command.current_revision.content_digest
            or self.idempotency_key != self.command.idempotency_key
            or self.request_digest != guideline_patch_request_digest_v1(self.command)
        ):
            raise GuidelineLifecycleError("guideline_patch_result_command_mismatch")


@dataclass(frozen=True, slots=True)
class GuidelinePatchNoop:
    """Canonical no-op: deliberately contains no revision/head write output."""

    status: str
    command: GuidelinePatchCommand
    expected_head_revision: int
    expected_revision_id: str
    expected_revision_number: int
    expected_semantic_version: str
    expected_revision_digest: str
    idempotency_key: str
    request_digest: str
    revision: None = None
    head: None = None

    def __post_init__(self) -> None:
        if (
            self.status != "noop"
            or not isinstance(self.command, GuidelinePatchCommand)
            or self.revision is not None
            or self.head is not None
        ):
            raise GuidelineLifecycleError("guideline_patch_noop_result_invalid")
        _positive_int(
            self.expected_head_revision,
            "guideline_patch_expected_head_invalid",
        )
        _positive_int(
            self.expected_revision_number,
            "guideline_patch_expected_revision_number_invalid",
        )
        object.__setattr__(
            self,
            "expected_revision_id",
            _required_text(
                self.expected_revision_id,
                "guideline_patch_expected_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_semantic_version",
            str(SemanticVersion.parse(self.expected_semantic_version)),
        )
        object.__setattr__(
            self,
            "expected_revision_digest",
            _sha256(
                self.expected_revision_digest,
                "guideline_patch_expected_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "guideline_patch_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "guideline_patch_request_digest_invalid",
            ),
        )
        plan = plan_guideline_patch(
            self.command.current_revision,
            self.command.patch,
            requested_semantic_version=(self.command.declared_semantic_version),
        )
        if (
            not plan.is_noop
            or self.expected_head_revision != self.command.current_head.head_revision
            or self.expected_revision_id != self.command.current_revision.revision_id
            or self.expected_revision_number
            != self.command.current_revision.revision_number
            or self.expected_semantic_version
            != self.command.current_revision.semantic_version
            or self.expected_revision_digest
            != self.command.current_revision.content_digest
            or self.idempotency_key != self.command.idempotency_key
            or self.request_digest != guideline_patch_request_digest_v1(self.command)
        ):
            raise GuidelineLifecycleError("guideline_patch_noop_command_mismatch")


@dataclass(frozen=True, slots=True)
class GuidelinePatchRejected:
    """Under-bump result with typed diagnostics and zero persistence output."""

    status: str
    command: GuidelinePatchCommand
    code: str
    minimum_bump: GuidelineVersionBump
    minimum_semantic_version: str
    declared_semantic_version: str
    expected_head_revision: int
    expected_revision_id: str
    expected_revision_number: int
    expected_semantic_version: str
    expected_revision_digest: str
    idempotency_key: str
    request_digest: str
    revision: None = None
    head: None = None

    def __post_init__(self) -> None:
        if (
            self.status != "rejected"
            or not isinstance(self.command, GuidelinePatchCommand)
            or self.code != "guideline_semver_below_minimum"
            or self.revision is not None
            or self.head is not None
        ):
            raise GuidelineLifecycleError("guideline_patch_rejected_result_invalid")
        if not isinstance(self.minimum_bump, GuidelineVersionBump):
            raise GuidelineLifecycleError("guideline_patch_minimum_bump_invalid")
        _positive_int(
            self.expected_head_revision,
            "guideline_patch_expected_head_invalid",
        )
        _positive_int(
            self.expected_revision_number,
            "guideline_patch_expected_revision_number_invalid",
        )
        expected_version = SemanticVersion.parse(self.expected_semantic_version)
        minimum_version = SemanticVersion.parse(self.minimum_semantic_version)
        declared_version = SemanticVersion.parse(self.declared_semantic_version)
        if minimum_version != expected_version.minimum_successor(self.minimum_bump) or (
            declared_version > expected_version
            and declared_version.core >= minimum_version.core
        ):
            raise GuidelineLifecycleError("guideline_patch_rejected_version_mismatch")
        object.__setattr__(
            self,
            "expected_revision_id",
            _required_text(
                self.expected_revision_id,
                "guideline_patch_expected_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_semantic_version",
            str(expected_version),
        )
        object.__setattr__(
            self,
            "minimum_semantic_version",
            str(minimum_version),
        )
        object.__setattr__(
            self,
            "declared_semantic_version",
            str(declared_version),
        )
        object.__setattr__(
            self,
            "expected_revision_digest",
            _sha256(
                self.expected_revision_digest,
                "guideline_patch_expected_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "guideline_patch_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "guideline_patch_request_digest_invalid",
            ),
        )
        try:
            plan_guideline_patch(
                self.command.current_revision,
                self.command.patch,
                requested_semantic_version=(self.command.declared_semantic_version),
            )
        except GuidelineVersionUnderBump as error:
            matching_rejection = (
                self.minimum_bump is error.required_bump
                and self.minimum_semantic_version == str(error.minimum)
                and self.declared_semantic_version == str(error.declared)
            )
        else:
            matching_rejection = False
        if (
            not matching_rejection
            or self.expected_head_revision != self.command.current_head.head_revision
            or self.expected_revision_id != self.command.current_revision.revision_id
            or self.expected_revision_number
            != self.command.current_revision.revision_number
            or self.expected_semantic_version
            != self.command.current_revision.semantic_version
            or self.expected_revision_digest
            != self.command.current_revision.content_digest
            or self.idempotency_key != self.command.idempotency_key
            or self.request_digest != guideline_patch_request_digest_v1(self.command)
        ):
            raise GuidelineLifecycleError("guideline_patch_rejected_command_mismatch")


GuidelinePatchResult: TypeAlias = (
    GuidelinePatchApplied | GuidelinePatchNoop | GuidelinePatchRejected
)


def guideline_patch_request_digest_v1(
    command: GuidelinePatchCommand,
) -> str:
    """Hash the canonical full desired snapshot and all optimistic fences."""

    if not isinstance(command, GuidelinePatchCommand):
        raise GuidelineLifecycleError("guideline_patch_command_invalid")
    canonical = plan_guideline_patch(
        command.current_revision,
        command.patch,
    )
    declared_version = (
        command.current_revision.semantic_version
        if canonical.is_noop
        else (command.declared_semantic_version or canonical.semantic_version)
    )
    return guideline_request_digest_v1(
        operation="patch",
        scope_id=command.current_revision.guideline_id,
        payload={
            "guideline_id": command.current_revision.guideline_id,
            "expected_head_revision": command.current_head.head_revision,
            "expected_revision_id": command.current_revision.revision_id,
            "expected_revision_number": (command.current_revision.revision_number),
            "expected_semantic_version": (command.current_revision.semantic_version),
            "expected_revision_digest": (command.current_revision.content_digest),
            "declared_semantic_version": declared_version,
            "title": canonical.title,
            "content": canonical.content,
            "tags": canonical.tags,
            "rules": _rules_manifest(canonical.rules),
            "actor_id": command.actor_id,
        },
    )


def _patch_fence_values(
    command: GuidelinePatchCommand,
) -> dict[str, object]:
    return {
        "expected_head_revision": command.current_head.head_revision,
        "expected_revision_id": command.current_revision.revision_id,
        "expected_revision_number": (command.current_revision.revision_number),
        "expected_semantic_version": (command.current_revision.semantic_version),
        "expected_revision_digest": command.current_revision.content_digest,
        "idempotency_key": command.idempotency_key,
        "request_digest": guideline_patch_request_digest_v1(command),
    }


def execute_guideline_patch(
    command: GuidelinePatchCommand,
    *,
    retirement: GuidelineRetirement | None = None,
) -> GuidelinePatchResult:
    """Return an applied/no-op/rejected result without performing any write."""

    if not isinstance(command, GuidelinePatchCommand):
        raise GuidelineLifecycleError("guideline_patch_command_invalid")
    if retirement is not None:
        raise GuidelineLifecycleError("guideline_is_terminal")
    fences = _patch_fence_values(command)
    try:
        plan = plan_guideline_patch(
            command.current_revision,
            command.patch,
            requested_semantic_version=command.declared_semantic_version,
        )
    except GuidelineVersionUnderBump as error:
        return GuidelinePatchRejected(
            status="rejected",
            command=command,
            code=error.code,
            minimum_bump=error.required_bump,
            minimum_semantic_version=str(error.minimum),
            declared_semantic_version=str(error.declared),
            **fences,
        )
    if plan.is_noop:
        return GuidelinePatchNoop(
            status="noop",
            command=command,
            **fences,
        )

    revision = GuidelineRevision(
        revision_id=command.next_revision_id,
        guideline_id=command.current_revision.guideline_id,
        revision_number=command.current_revision.revision_number + 1,
        semantic_version=plan.semantic_version,
        title=plan.title,
        content=plan.content,
        content_digest=guideline_revision_content_digest_v1(
            title=plan.title,
            content=plan.content,
            tags=plan.tags,
            rules=plan.rules,
        ),
        rules=plan.rules,
        tags=plan.tags,
        created_by=command.actor_id,
        created_at=command.occurred_at,
        parent_revision_id=command.current_revision.revision_id,
    )
    head = GuidelineHead(
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        head_revision=command.current_head.head_revision + 1,
        updated_at=command.occurred_at,
    )
    validate_revision_transition(command.current_head, revision, head)
    return GuidelinePatchApplied(
        status="applied",
        command=command,
        revision=revision,
        head=head,
        minimum_bump=plan.minimum_bump,
        **fences,
    )


@dataclass(frozen=True, slots=True)
class GuidelineRetirementCommand:
    """Terminal retirement/supersedence intent over an exact frozen head."""

    current_revision: GuidelineRevision
    current_head: GuidelineHead
    retirement_id: str
    status: GuidelineLifecycleStatus
    reason: str
    actor_id: str
    occurred_at: datetime
    idempotency_key: str
    superseded_by_guideline_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.current_revision, GuidelineRevision):
            raise GuidelineLifecycleError(
                "guideline_retirement_current_revision_invalid"
            )
        if not isinstance(self.current_head, GuidelineHead):
            raise GuidelineLifecycleError("guideline_retirement_current_head_invalid")
        if not isinstance(self.status, GuidelineLifecycleStatus):
            raise GuidelineLifecycleError("guideline_retirement_status_invalid")
        for field_name in (
            "retirement_id",
            "reason",
            "actor_id",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_retirement_{field_name}_required",
                ),
            )
        successor = _optional_text(
            self.superseded_by_guideline_id,
            "guideline_retirement_successor_invalid",
        )
        if self.status is GuidelineLifecycleStatus.SUPERSEDED:
            if successor is None or successor == self.current_revision.guideline_id:
                raise GuidelineLifecycleError(
                    "guideline_supersedence_successor_required"
                )
        elif successor is not None:
            raise GuidelineLifecycleError("guideline_retirement_successor_forbidden")
        object.__setattr__(
            self,
            "superseded_by_guideline_id",
            successor,
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "guideline_retirement_time_invalid",
            ),
        )
        current = self.current_revision
        head = self.current_head
        if (
            current.guideline_id != head.guideline_id
            or current.revision_id != head.revision_id
            or current.revision_number != head.revision_number
            or current.semantic_version != head.semantic_version
        ):
            raise GuidelineLifecycleError("guideline_retirement_snapshot_mismatch")
        if self.occurred_at <= head.updated_at:
            raise GuidelineLifecycleError("guideline_retirement_time_not_monotonic")


@dataclass(frozen=True, slots=True)
class GuidelineRetirementResult:
    command: GuidelineRetirementCommand
    retirement: GuidelineRetirement
    expected_guideline_id: str
    expected_head_revision: int
    expected_revision_id: str
    expected_revision_number: int
    expected_semantic_version: str
    expected_revision_digest: str
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.command, GuidelineRetirementCommand) or not isinstance(
            self.retirement, GuidelineRetirement
        ):
            raise GuidelineLifecycleError("guideline_retirement_result_invalid")
        expected_guideline_id = _required_text(
            self.expected_guideline_id,
            "guideline_retirement_expected_guideline_id_required",
        )
        expected_head_revision = _positive_int(
            self.expected_head_revision,
            "guideline_retirement_expected_head_invalid",
        )
        expected_revision_id = _required_text(
            self.expected_revision_id,
            "guideline_retirement_expected_revision_id_required",
        )
        expected_revision_number = _positive_int(
            self.expected_revision_number,
            "guideline_retirement_expected_revision_number_invalid",
        )
        expected_semantic_version = str(
            SemanticVersion.parse(self.expected_semantic_version)
        )
        expected_revision_digest = _sha256(
            self.expected_revision_digest,
            "guideline_retirement_expected_revision_digest_invalid",
        )
        if (
            self.retirement.guideline_id != expected_guideline_id
            or self.retirement.retired_head_revision != expected_head_revision
            or self.retirement.retired_revision_id != expected_revision_id
            or self.retirement.retired_revision_number != expected_revision_number
            or self.retirement.retired_semantic_version != expected_semantic_version
            or self.retirement.retired_revision_digest != expected_revision_digest
        ):
            raise GuidelineLifecycleError("guideline_retirement_result_fence_mismatch")
        object.__setattr__(
            self,
            "expected_guideline_id",
            expected_guideline_id,
        )
        object.__setattr__(
            self,
            "expected_revision_id",
            expected_revision_id,
        )
        object.__setattr__(
            self,
            "expected_revision_number",
            expected_revision_number,
        )
        object.__setattr__(
            self,
            "expected_semantic_version",
            expected_semantic_version,
        )
        object.__setattr__(
            self,
            "expected_revision_digest",
            expected_revision_digest,
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "guideline_retirement_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "guideline_retirement_request_digest_invalid",
            ),
        )
        command = self.command
        if (
            expected_guideline_id != command.current_revision.guideline_id
            or expected_head_revision != command.current_head.head_revision
            or expected_revision_id != command.current_revision.revision_id
            or expected_revision_number != command.current_revision.revision_number
            or expected_semantic_version != command.current_revision.semantic_version
            or expected_revision_digest != command.current_revision.content_digest
            or self.retirement.retirement_id != command.retirement_id
            or self.retirement.status is not command.status
            or self.retirement.reason != command.reason
            or self.retirement.retired_by != command.actor_id
            or self.retirement.retired_at != command.occurred_at
            or self.retirement.superseded_by_guideline_id
            != command.superseded_by_guideline_id
            or self.idempotency_key != command.idempotency_key
            or self.request_digest != guideline_retirement_request_digest_v1(command)
        ):
            raise GuidelineLifecycleError(
                "guideline_retirement_result_command_mismatch"
            )


def guideline_retirement_request_digest_v1(
    command: GuidelineRetirementCommand,
) -> str:
    if not isinstance(command, GuidelineRetirementCommand):
        raise GuidelineLifecycleError("guideline_retirement_command_invalid")
    current = command.current_revision
    return guideline_request_digest_v1(
        operation="retire",
        scope_id=current.guideline_id,
        payload={
            "guideline_id": current.guideline_id,
            "expected_head_revision": command.current_head.head_revision,
            "retired_revision_id": current.revision_id,
            "retired_revision_number": current.revision_number,
            "retired_semantic_version": current.semantic_version,
            "retired_revision_digest": current.content_digest,
            "status": command.status.value,
            "reason": command.reason,
            "superseded_by_guideline_id": (command.superseded_by_guideline_id),
            "actor_id": command.actor_id,
        },
    )


def plan_guideline_retirement(
    command: GuidelineRetirementCommand,
    *,
    current_retirement: GuidelineRetirement | None = None,
) -> GuidelineRetirementResult:
    """Freeze the exact current head in an append-only terminal record."""

    if not isinstance(command, GuidelineRetirementCommand):
        raise GuidelineLifecycleError("guideline_retirement_command_invalid")
    if current_retirement is not None:
        raise GuidelineLifecycleError("guideline_is_terminal")
    current = command.current_revision
    retirement = GuidelineRetirement(
        retirement_id=command.retirement_id,
        guideline_id=current.guideline_id,
        status=command.status,
        retired_revision_id=current.revision_id,
        retired_revision_number=current.revision_number,
        retired_semantic_version=current.semantic_version,
        retired_revision_digest=current.content_digest,
        retired_head_revision=command.current_head.head_revision,
        reason=command.reason,
        retired_by=command.actor_id,
        retired_at=command.occurred_at,
        superseded_by_guideline_id=(command.superseded_by_guideline_id),
    )
    return GuidelineRetirementResult(
        command=command,
        retirement=retirement,
        expected_guideline_id=current.guideline_id,
        expected_head_revision=command.current_head.head_revision,
        expected_revision_id=current.revision_id,
        expected_revision_number=current.revision_number,
        expected_semantic_version=current.semantic_version,
        expected_revision_digest=current.content_digest,
        idempotency_key=command.idempotency_key,
        request_digest=guideline_retirement_request_digest_v1(command),
    )


@dataclass(frozen=True, slots=True)
class GuidelineBindingTransitionCommand:
    """Append-only ACTIVE/UNLINKED transition with an explicit CAS fence."""

    binding_id: str
    board_id: str
    guideline_id: str
    state: GuidelineBindingState
    actor_id: str
    occurred_at: datetime
    idempotency_key: str
    expected_binding_revision: int | None
    revision_id: str | None = None
    semantic_version: str | None = None
    revision_digest: str | None = None
    priority: int | None = None
    default_enforcement: GuidelineEnforcement | None = None
    source_kind: GuidelineBindingProvenance = GuidelineBindingProvenance.NATIVE

    def __post_init__(self) -> None:
        if not isinstance(self.state, GuidelineBindingState):
            raise GuidelineLifecycleError("guideline_binding_state_invalid")
        if not isinstance(self.source_kind, GuidelineBindingProvenance):
            raise GuidelineLifecycleError("guideline_binding_source_kind_invalid")
        for field_name in (
            "binding_id",
            "board_id",
            "guideline_id",
            "actor_id",
            "idempotency_key",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_binding_{field_name}_required",
                ),
            )
        expected = self.expected_binding_revision
        if expected is not None:
            object.__setattr__(
                self,
                "expected_binding_revision",
                _positive_int(
                    expected,
                    "guideline_binding_expected_revision_invalid",
                ),
            )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "guideline_binding_time_invalid",
            ),
        )
        snapshot = (
            self.revision_id,
            self.semantic_version,
            self.revision_digest,
            self.priority,
            self.default_enforcement,
        )
        if self.state is GuidelineBindingState.UNLINKED:
            if any(value is not None for value in snapshot):
                raise GuidelineLifecycleError(
                    "guideline_binding_unlink_snapshot_forbidden"
                )
            return
        if any(value is None for value in snapshot):
            raise GuidelineLifecycleError("guideline_binding_active_snapshot_required")
        object.__setattr__(
            self,
            "revision_id",
            _required_text(
                self.revision_id,
                "guideline_binding_revision_id_required",
            ),
        )
        semantic_version = str(SemanticVersion.parse(self.semantic_version))
        object.__setattr__(
            self,
            "semantic_version",
            semantic_version,
        )
        object.__setattr__(
            self,
            "revision_digest",
            _sha256(
                self.revision_digest,
                "guideline_binding_revision_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "priority",
            _non_negative_int(
                self.priority,
                "guideline_binding_priority_invalid",
            ),
        )
        if not isinstance(
            self.default_enforcement,
            GuidelineEnforcement,
        ):
            raise GuidelineLifecycleError("guideline_binding_enforcement_invalid")


@dataclass(frozen=True, slots=True)
class GuidelineBindingApplied:
    status: str
    command: GuidelineBindingTransitionCommand
    previous_binding: BoardGuidelineBinding | None
    binding: BoardGuidelineBinding
    expected_binding_revision: int | None
    idempotency_key: str
    request_digest: str

    def __post_init__(self) -> None:
        if (
            self.status != "applied"
            or not isinstance(
                self.command,
                GuidelineBindingTransitionCommand,
            )
            or (
                self.previous_binding is not None
                and not isinstance(
                    self.previous_binding,
                    BoardGuidelineBinding,
                )
            )
            or not isinstance(self.binding, BoardGuidelineBinding)
        ):
            raise GuidelineLifecycleError("guideline_binding_result_invalid")
        expected = self.expected_binding_revision
        if expected is None:
            if self.binding.binding_revision != 1:
                raise GuidelineLifecycleError("guideline_binding_result_fence_mismatch")
        elif (
            self.binding.binding_revision
            != _positive_int(
                expected,
                "guideline_binding_expected_revision_invalid",
            )
            + 1
        ):
            raise GuidelineLifecycleError("guideline_binding_result_fence_mismatch")
        idempotency_key = _required_text(
            self.idempotency_key,
            "guideline_binding_idempotency_key_required",
        )
        request_digest = _sha256(
            self.request_digest,
            "guideline_binding_request_digest_invalid",
        )
        command = self.command
        binding = self.binding
        previous = self.previous_binding
        if previous is None:
            validate_binding_transition(None, binding)
        else:
            validate_binding_transition(previous, binding)
        active_snapshot_mismatch = command.state is GuidelineBindingState.ACTIVE and (
            binding.revision_id != command.revision_id
            or binding.semantic_version != command.semantic_version
            or binding.revision_digest != command.revision_digest
            or binding.priority != command.priority
            or binding.default_enforcement is not command.default_enforcement
            or binding.source_kind
            is not (
                previous.source_kind if previous is not None else command.source_kind
            )
        )
        if (
            command.expected_binding_revision != self.expected_binding_revision
            or binding.binding_id != command.binding_id
            or binding.board_id != command.board_id
            or binding.guideline_id != command.guideline_id
            or binding.state is not command.state
            or binding.adopted_by != command.actor_id
            or binding.adopted_at != command.occurred_at
            or active_snapshot_mismatch
            or idempotency_key != command.idempotency_key
            or request_digest != guideline_binding_request_digest_v1(command)
        ):
            raise GuidelineLifecycleError("guideline_binding_result_command_mismatch")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_digest", request_digest)


@dataclass(frozen=True, slots=True)
class GuidelineBindingNoop:
    """Effect-free state equality; no appendable binding row is returned."""

    status: str
    command: GuidelineBindingTransitionCommand
    current_binding: BoardGuidelineBinding
    expected_binding_revision: int
    idempotency_key: str
    request_digest: str
    binding: None = None

    def __post_init__(self) -> None:
        if (
            self.status != "noop"
            or not isinstance(
                self.command,
                GuidelineBindingTransitionCommand,
            )
            or not isinstance(
                self.current_binding,
                BoardGuidelineBinding,
            )
            or self.binding is not None
        ):
            raise GuidelineLifecycleError("guideline_binding_noop_result_invalid")
        expected = _positive_int(
            self.expected_binding_revision,
            "guideline_binding_expected_revision_invalid",
        )
        idempotency_key = _required_text(
            self.idempotency_key,
            "guideline_binding_idempotency_key_required",
        )
        request_digest = _sha256(
            self.request_digest,
            "guideline_binding_request_digest_invalid",
        )
        command = self.command
        current = self.current_binding
        active_noop = (
            command.state is GuidelineBindingState.ACTIVE
            and current.state is GuidelineBindingState.ACTIVE
            and command.revision_id == current.revision_id
            and command.semantic_version == current.semantic_version
            and command.revision_digest == current.revision_digest
            and command.priority == current.priority
            and command.default_enforcement is current.default_enforcement
        )
        unlink_noop = (
            command.state is GuidelineBindingState.UNLINKED
            and current.state is GuidelineBindingState.UNLINKED
        )
        if (
            expected != current.binding_revision
            or expected != command.expected_binding_revision
            or command.binding_id != current.binding_id
            or command.board_id != current.board_id
            or command.guideline_id != current.guideline_id
            or not (active_noop or unlink_noop)
            or idempotency_key != command.idempotency_key
            or request_digest != guideline_binding_request_digest_v1(command)
        ):
            raise GuidelineLifecycleError("guideline_binding_noop_command_mismatch")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_digest", request_digest)


GuidelineBindingTransitionResult: TypeAlias = (
    GuidelineBindingApplied | GuidelineBindingNoop
)


def guideline_binding_request_digest_v1(
    command: GuidelineBindingTransitionCommand,
) -> str:
    if not isinstance(command, GuidelineBindingTransitionCommand):
        raise GuidelineLifecycleError("guideline_binding_command_invalid")
    return guideline_request_digest_v1(
        operation="binding_transition",
        scope_id=f"{command.board_id}:{command.guideline_id}",
        payload={
            "binding_id": command.binding_id,
            "board_id": command.board_id,
            "guideline_id": command.guideline_id,
            "expected_binding_revision": (command.expected_binding_revision),
            "state": command.state.value,
            "revision_id": command.revision_id,
            "semantic_version": command.semantic_version,
            "revision_digest": command.revision_digest,
            "priority": command.priority,
            "default_enforcement": (
                command.default_enforcement.value
                if command.default_enforcement is not None
                else None
            ),
            "source_kind": command.source_kind.value,
            "actor_id": command.actor_id,
        },
    )


def plan_guideline_binding_transition(
    command: GuidelineBindingTransitionCommand,
    *,
    current: BoardGuidelineBinding | None,
    retirement: GuidelineRetirement | None = None,
) -> GuidelineBindingTransitionResult:
    """Build one appendable binding revision or a typed effect-free no-op."""

    if not isinstance(command, GuidelineBindingTransitionCommand):
        raise GuidelineLifecycleError("guideline_binding_command_invalid")
    if current is not None and not isinstance(
        current,
        BoardGuidelineBinding,
    ):
        raise GuidelineLifecycleError("guideline_binding_current_invalid")
    if current is not None and command.source_kind is not current.source_kind:
        raise GuidelineLifecycleError("guideline_binding_origin_immutable")
    if retirement is not None and command.state is GuidelineBindingState.ACTIVE:
        raise GuidelineLifecycleError("guideline_is_terminal")
    request_digest = guideline_binding_request_digest_v1(command)
    if current is None:
        if command.expected_binding_revision is not None:
            raise GuidelineLifecycleError("guideline_binding_fence_mismatch")
        if command.state is not GuidelineBindingState.ACTIVE:
            raise GuidelineLifecycleError("guideline_initial_binding_must_be_active")
        proposed = BoardGuidelineBinding(
            binding_id=command.binding_id,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
            revision_id=command.revision_id,
            semantic_version=command.semantic_version,
            revision_digest=command.revision_digest,
            priority=command.priority,
            binding_revision=1,
            adopted_by=command.actor_id,
            adopted_at=command.occurred_at,
            default_enforcement=command.default_enforcement,
            state=GuidelineBindingState.ACTIVE,
            source_kind=command.source_kind,
        )
        validate_binding_transition(
            None,
            proposed,
            retirement=retirement,
        )
        return GuidelineBindingApplied(
            status="applied",
            command=command,
            previous_binding=None,
            binding=proposed,
            expected_binding_revision=None,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )

    if (
        command.binding_id != current.binding_id
        or command.board_id != current.board_id
        or command.guideline_id != current.guideline_id
        or command.expected_binding_revision != current.binding_revision
    ):
        raise GuidelineLifecycleError("guideline_binding_fence_mismatch")

    if command.state is GuidelineBindingState.UNLINKED:
        if current.state is GuidelineBindingState.UNLINKED:
            return GuidelineBindingNoop(
                status="noop",
                command=command,
                current_binding=current,
                expected_binding_revision=current.binding_revision,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )
        proposed = replace(
            current,
            binding_revision=current.binding_revision + 1,
            adopted_by=command.actor_id,
            adopted_at=command.occurred_at,
            state=GuidelineBindingState.UNLINKED,
        )
    else:
        same_snapshot = (
            current.state is GuidelineBindingState.ACTIVE
            and command.revision_id == current.revision_id
            and command.semantic_version == current.semantic_version
            and command.revision_digest == current.revision_digest
            and command.priority == current.priority
            and command.default_enforcement is current.default_enforcement
        )
        if same_snapshot:
            return GuidelineBindingNoop(
                status="noop",
                command=command,
                current_binding=current,
                expected_binding_revision=current.binding_revision,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )
        proposed = BoardGuidelineBinding(
            binding_id=current.binding_id,
            board_id=current.board_id,
            guideline_id=current.guideline_id,
            revision_id=command.revision_id,
            semantic_version=command.semantic_version,
            revision_digest=command.revision_digest,
            priority=command.priority,
            binding_revision=current.binding_revision + 1,
            adopted_by=command.actor_id,
            adopted_at=command.occurred_at,
            default_enforcement=command.default_enforcement,
            state=GuidelineBindingState.ACTIVE,
            source_kind=current.source_kind,
        )
    validate_binding_transition(
        current,
        proposed,
        retirement=retirement,
    )
    return GuidelineBindingApplied(
        status="applied",
        command=command,
        previous_binding=current,
        binding=proposed,
        expected_binding_revision=current.binding_revision,
        idempotency_key=command.idempotency_key,
        request_digest=request_digest,
    )


def validate_revision_transition(
    current_head: GuidelineHead,
    next_revision: GuidelineRevision,
    next_head: GuidelineHead,
    *,
    retirement: GuidelineRetirement | None = None,
) -> None:
    """Fail closed before persistence applies the compare-and-swap."""

    if retirement is not None:
        raise GuidelineLifecycleError("guideline_is_terminal")
    valid = (
        next_revision.guideline_id == current_head.guideline_id
        and next_revision.parent_revision_id == current_head.revision_id
        and next_revision.revision_number == current_head.revision_number + 1
        and next_head.guideline_id == current_head.guideline_id
        and next_head.revision_id == next_revision.revision_id
        and next_head.revision_number == next_revision.revision_number
        and next_head.semantic_version == next_revision.semantic_version
        and next_head.head_revision == current_head.head_revision + 1
    )
    if not valid:
        raise GuidelineLifecycleError("guideline_revision_transition_invalid")


def validate_binding_transition(
    current: BoardGuidelineBinding | None,
    proposed: BoardGuidelineBinding,
    *,
    retirement: GuidelineRetirement | None = None,
) -> None:
    """Validate append-only priority/unlink/relink while preserving identity."""

    if retirement is not None and proposed.state is GuidelineBindingState.ACTIVE:
        raise GuidelineLifecycleError("guideline_is_terminal")
    if current is None:
        if proposed.binding_revision != 1:
            raise GuidelineLifecycleError("guideline_binding_sequence_invalid")
        if proposed.state is not GuidelineBindingState.ACTIVE:
            raise GuidelineLifecycleError("guideline_initial_binding_must_be_active")
        return

    if (
        proposed.binding_id != current.binding_id
        or proposed.board_id != current.board_id
        or proposed.guideline_id != current.guideline_id
        or proposed.binding_revision != current.binding_revision + 1
    ):
        raise GuidelineLifecycleError("guideline_binding_transition_invalid")
    if proposed.adopted_at <= current.adopted_at:
        raise GuidelineLifecycleError("guideline_binding_time_not_monotonic")
    if proposed.source_kind is not current.source_kind:
        raise GuidelineLifecycleError("guideline_binding_origin_immutable")

    snapshot_fields = (
        "revision_id",
        "semantic_version",
        "revision_digest",
        "priority",
        "default_enforcement",
    )
    snapshot_changed = any(
        getattr(proposed, field_name) != getattr(current, field_name)
        for field_name in snapshot_fields
    )
    if proposed.state is GuidelineBindingState.UNLINKED:
        if current.state is GuidelineBindingState.UNLINKED:
            raise GuidelineLifecycleError(
                "guideline_binding_unlinked_terminal"
                if snapshot_changed
                else "guideline_binding_noop"
            )
        if snapshot_changed:
            raise GuidelineLifecycleError("guideline_binding_unlink_snapshot_changed")

    if current.state is proposed.state and not snapshot_changed:
        raise GuidelineLifecycleError("guideline_binding_noop")


__all__ = [
    "GUIDELINE_LIFECYCLE_CONTRACT_VERSION",
    "GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION",
    "GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION",
    "GuidelineBindingApplied",
    "GuidelineBindingNoop",
    "GuidelineBindingTransitionCommand",
    "GuidelineBindingTransitionResult",
    "GuidelineCreateCommand",
    "GuidelineCreateResult",
    "GuidelineLifecycleError",
    "GuidelinePatchApplied",
    "GuidelinePatchCommand",
    "GuidelinePatchNoop",
    "GuidelinePatchPlan",
    "GuidelinePatchRejected",
    "GuidelinePatchResult",
    "GuidelineRetirement",
    "GuidelineRetirementCommand",
    "GuidelineRetirementResult",
    "GuidelineRevisionPatch",
    "GuidelineVersionBump",
    "GuidelineVersionUnderBump",
    "SemanticVersion",
    "classify_guideline_change",
    "execute_guideline_patch",
    "guideline_binding_request_digest_v1",
    "guideline_create_request_digest_v1",
    "guideline_patch_request_digest_v1",
    "guideline_request_digest_v1",
    "guideline_retirement_request_digest_v1",
    "guideline_revision_content_digest",
    "guideline_revision_content_digest_v1",
    "plan_guideline_binding_transition",
    "plan_guideline_creation",
    "plan_guideline_patch",
    "plan_guideline_retirement",
    "validate_binding_transition",
    "validate_revision_transition",
]
