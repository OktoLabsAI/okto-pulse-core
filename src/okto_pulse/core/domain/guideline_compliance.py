"""Guideline revision projections and signed policy keyset contracts."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import ClassVar, Generic, TypeVar

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_PAGE_LIMIT_MAX,
    POLICY_KEYSET_CONTRACT_VERSION,
    GuidelineImpactItem,
    GuidelinePolicyContractError,
    GuidelineRevision,
    GuidelineRevisionPageCursor,
    GuidelineMetric,
)
from okto_pulse.core.domain.guideline_semantic_projection import (
    SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION,
    SemanticAssessmentPageCursor,
    SemanticFindingPageCursor,
    SemanticSkipPageCursor,
    SemanticWaiverPageCursor,
)


POLICY_CURSOR_TOKEN_MAX_LENGTH = 8192


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelinePolicyContractError(code)
    return value.strip()


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise GuidelinePolicyContractError(code)
    return normalized


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelinePolicyContractError(code)
    return value.astimezone(timezone.utc)


def _positive_limit(value: object) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= GUIDELINE_PAGE_LIMIT_MAX
    ):
        raise GuidelinePolicyContractError("policy_page_limit_invalid")
    return value


class PolicyProjection(str, Enum):
    """Closed list projection surface shared by REST and MCP adapters."""

    SUMMARY = "summary"
    DETAIL = "detail"


@dataclass(frozen=True, slots=True)
class GuidelineRevisionListItem:
    """Projection-safe immutable revision row shared by REST and MCP."""

    projection: PolicyProjection
    revision_id: str
    guideline_id: str
    revision_number: int
    semantic_version: str
    title: str
    created_by: str
    created_at: datetime
    parent_revision_id: str | None
    content: str | None = None
    revision_digest: str | None = None
    tags: tuple[str, ...] | None = None
    metrics: tuple[GuidelineMetric, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.projection, PolicyProjection):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_invalid"
            )
        for field_name in (
            "revision_id",
            "guideline_id",
            "semantic_version",
            "title",
            "created_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"guideline_revision_projection_{field_name}_required",
                ),
            )
        if (
            not isinstance(self.revision_number, int)
            or isinstance(self.revision_number, bool)
            or self.revision_number < 1
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_number_invalid"
            )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "guideline_revision_projection_created_at_invalid",
            ),
        )
        if self.projection is PolicyProjection.SUMMARY:
            if any(
                value is not None
                for value in (
                    self.content,
                    self.revision_digest,
                    self.tags,
                    self.metrics,
                )
            ):
                raise GuidelinePolicyContractError(
                    "guideline_revision_summary_not_slim"
                )
        elif (
            self.content is None
            or self.revision_digest is None
            or self.tags is None
            or self.metrics is None
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_detail_incomplete"
            )


@dataclass(frozen=True, slots=True)
class GuidelineRevisionProjectionPage:
    items: tuple[GuidelineRevisionListItem, ...]
    limit: int
    next_cursor: GuidelineRevisionPageCursor | None
    has_more: bool

    ordering: ClassVar[tuple[str, str]] = (
        "revision_number DESC",
        "revision_id DESC",
    )

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list) or any(
            not isinstance(item, GuidelineRevisionListItem) for item in self.items
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_items_invalid"
            )
        object.__setattr__(self, "items", tuple(self.items))
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= GUIDELINE_PAGE_LIMIT_MAX
        ):
            raise GuidelinePolicyContractError("guideline_page_limit_invalid")
        if len(self.items) > self.limit:
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_limit_exceeded"
            )
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            GuidelineRevisionPageCursor,
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_cursor_invalid"
            )
        if not isinstance(self.has_more, bool) or self.has_more != (
            self.next_cursor is not None
        ):
            raise GuidelinePolicyContractError(
                "guideline_revision_projection_page_cursor_mismatch"
            )


def project_guideline_revision(
    revision: GuidelineRevision,
    *,
    projection: PolicyProjection,
) -> GuidelineRevisionListItem:
    if not isinstance(revision, GuidelineRevision):
        raise GuidelinePolicyContractError("guideline_revision_invalid")
    if not isinstance(projection, PolicyProjection):
        raise GuidelinePolicyContractError("guideline_revision_projection_invalid")
    detail = projection is PolicyProjection.DETAIL
    return GuidelineRevisionListItem(
        projection=projection,
        revision_id=revision.revision_id,
        guideline_id=revision.guideline_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        title=revision.title,
        created_by=revision.created_by,
        created_at=revision.created_at,
        parent_revision_id=revision.parent_revision_id,
        content=revision.content if detail else None,
        revision_digest=revision.revision_digest if detail else None,
        tags=revision.tags if detail else None,
        metrics=revision.metrics if detail else None,
    )


POLICY_IMPACT_ORDERING: tuple[str, str, str] = (
    "entity_type ASC",
    "entity_id ASC",
    "impact_item_id ASC",
)


@dataclass(frozen=True, slots=True)
class PolicyImpactPageCursor:
    entity_type: str
    entity_id: str
    item_id: str
    filter_digest: str
    projection_digest: str
    schema_version: str = POLICY_KEYSET_CONTRACT_VERSION
    ordering: tuple[str, str, str] = POLICY_IMPACT_ORDERING

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_KEYSET_CONTRACT_VERSION:
            raise GuidelinePolicyContractError("policy_cursor_schema_version_invalid")
        if tuple(self.ordering) != POLICY_IMPACT_ORDERING:
            raise GuidelinePolicyContractError("policy_impact_cursor_ordering_invalid")
        for field_name in ("entity_type", "entity_id", "item_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_impact_cursor_{field_name}_required",
                ),
            )
        for field_name in ("filter_digest", "projection_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"policy_impact_cursor_{field_name}_invalid",
                ),
            )
        object.__setattr__(self, "ordering", POLICY_IMPACT_ORDERING)


_PolicyPageItemT = TypeVar("_PolicyPageItemT")
_PolicyPageCursorT = TypeVar("_PolicyPageCursorT")


@dataclass(frozen=True, slots=True)
class PolicyKeysetPage(Generic[_PolicyPageItemT, _PolicyPageCursorT]):
    items: tuple[_PolicyPageItemT, ...]
    limit: int
    next_cursor: _PolicyPageCursorT | None
    has_more: bool

    ordering: ClassVar[tuple[str, ...]] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise GuidelinePolicyContractError("policy_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        object.__setattr__(self, "limit", _positive_limit(self.limit))
        if not isinstance(self.has_more, bool):
            raise GuidelinePolicyContractError("policy_page_has_more_invalid")
        if len(self.items) > self.limit:
            raise GuidelinePolicyContractError("policy_page_over_limit")
        if self.has_more != (self.next_cursor is not None):
            raise GuidelinePolicyContractError("policy_page_cursor_mismatch")


@dataclass(frozen=True, slots=True)
class GuidelineImpactItemPage(
    PolicyKeysetPage[
        GuidelineImpactItem,
        PolicyImpactPageCursor,
    ]
):
    ordering: ClassVar[tuple[str, ...]] = POLICY_IMPACT_ORDERING

    def __post_init__(self) -> None:
        PolicyKeysetPage.__post_init__(self)
        if any(not isinstance(item, GuidelineImpactItem) for item in self.items):
            raise GuidelinePolicyContractError("policy_impact_page_item_invalid")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            PolicyImpactPageCursor,
        ):
            raise GuidelinePolicyContractError("policy_impact_page_cursor_invalid")


class PolicyCursorCodec:
    """HMAC-authenticated opaque transport codec for policy-keyset/v1."""

    def __init__(self, signing_key: bytes) -> None:
        if not isinstance(signing_key, bytes) or len(signing_key) < 32:
            raise GuidelinePolicyContractError("policy_cursor_signing_key_invalid")
        self._signing_key = signing_key

    @staticmethod
    def _b64encode(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")

    @staticmethod
    def _b64decode(value: str) -> bytes:
        if not isinstance(value, str) or not value:
            raise GuidelinePolicyContractError("invalid_cursor")
        padding = "=" * (-len(value) % 4)
        try:
            decoded = base64.b64decode(
                value + padding,
                altchars=b"-_",
                validate=True,
            )
        except Exception as exc:
            raise GuidelinePolicyContractError("invalid_cursor") from exc
        if PolicyCursorCodec._b64encode(decoded) != value:
            raise GuidelinePolicyContractError("invalid_cursor")
        return decoded

    def encode(
        self,
        cursor: (
            GuidelineRevisionPageCursor
            | PolicyImpactPageCursor
            | SemanticAssessmentPageCursor
            | SemanticFindingPageCursor
            | SemanticWaiverPageCursor
            | SemanticSkipPageCursor
        ),
    ) -> str:
        if isinstance(cursor, GuidelineRevisionPageCursor):
            payload: dict[str, object] = {
                "schema_version": cursor.schema_version,
                "kind": "revision",
                "ordering": list(cursor.ordering),
                "revision_number": cursor.revision_number,
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, PolicyImpactPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "impact",
                "ordering": list(cursor.ordering),
                "entity_type": cursor.entity_type,
                "entity_id": cursor.entity_id,
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticAssessmentPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_assessment",
                "ordering": list(cursor.ordering),
                "recorded_at": cursor.recorded_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticFindingPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_finding",
                "ordering": list(cursor.ordering),
                "created_at": cursor.created_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticWaiverPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_waiver",
                "ordering": list(cursor.ordering),
                "requested_at": cursor.requested_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        elif isinstance(cursor, SemanticSkipPageCursor):
            payload = {
                "schema_version": cursor.schema_version,
                "kind": "semantic_skip",
                "ordering": list(cursor.ordering),
                "created_at": cursor.created_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z"),
                "item_id": cursor.item_id,
                "filter_digest": cursor.filter_digest,
                "projection_digest": cursor.projection_digest,
            }
        else:
            raise GuidelinePolicyContractError("invalid_cursor")
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        signature = hmac.new(
            self._signing_key,
            encoded,
            hashlib.sha256,
        ).digest()
        return f"{self._b64encode(encoded)}.{self._b64encode(signature)}"

    def decode(
        self,
        token: str,
        *,
        expected_kind: str,
    ) -> (
        GuidelineRevisionPageCursor
        | PolicyImpactPageCursor
        | SemanticAssessmentPageCursor
        | SemanticFindingPageCursor
        | SemanticWaiverPageCursor
        | SemanticSkipPageCursor
    ):
        try:
            if (
                not isinstance(token, str)
                or not token
                or len(token) > POLICY_CURSOR_TOKEN_MAX_LENGTH
                or token.count(".") != 1
            ):
                raise GuidelinePolicyContractError("invalid_cursor")
            if expected_kind not in {
                "revision",
                "impact",
                "semantic_assessment",
                "semantic_finding",
                "semantic_waiver",
                "semantic_skip",
            }:
                raise GuidelinePolicyContractError("invalid_cursor")
            encoded_part, signature_part = token.split(".", 1)
            encoded = self._b64decode(encoded_part)
            signature = self._b64decode(signature_part)
            expected_signature = hmac.new(
                self._signing_key,
                encoded,
                hashlib.sha256,
            ).digest()
            if not hmac.compare_digest(signature, expected_signature):
                raise GuidelinePolicyContractError("invalid_cursor")
            payload = json.loads(encoded.decode("utf-8"))
            canonical_encoded = json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if not hmac.compare_digest(encoded, canonical_encoded):
                raise GuidelinePolicyContractError("invalid_cursor")
            semantic_kind = expected_kind.startswith("semantic_")
            expected_schema_version = (
                SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
                if semantic_kind
                else POLICY_KEYSET_CONTRACT_VERSION
            )
            if (
                not isinstance(payload, dict)
                or payload.get("schema_version") != expected_schema_version
                or payload.get("kind") != expected_kind
            ):
                raise GuidelinePolicyContractError("invalid_cursor")
            if expected_kind == "revision":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "revision_number",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                return GuidelineRevisionPageCursor(
                    revision_number=payload["revision_number"],
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind == "impact":
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    "entity_type",
                    "entity_id",
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                return PolicyImpactPageCursor(
                    entity_type=payload["entity_type"],
                    entity_id=payload["entity_id"],
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
            if expected_kind in {
                "semantic_assessment",
                "semantic_finding",
                "semantic_waiver",
                "semantic_skip",
            }:
                timestamp_field = {
                    "semantic_assessment": "recorded_at",
                    "semantic_finding": "created_at",
                    "semantic_waiver": "requested_at",
                    "semantic_skip": "created_at",
                }[expected_kind]
                if set(payload) != {
                    "schema_version",
                    "kind",
                    "ordering",
                    timestamp_field,
                    "item_id",
                    "filter_digest",
                    "projection_digest",
                }:
                    raise GuidelinePolicyContractError("invalid_cursor")
                timestamp = str(payload[timestamp_field])
                if timestamp.endswith("Z"):
                    timestamp = timestamp[:-1] + "+00:00"
                cursor_type = {
                    "semantic_assessment": SemanticAssessmentPageCursor,
                    "semantic_finding": SemanticFindingPageCursor,
                    "semantic_waiver": SemanticWaiverPageCursor,
                    "semantic_skip": SemanticSkipPageCursor,
                }[expected_kind]
                semantic_cursor = cursor_type(
                    at=datetime.fromisoformat(timestamp),
                    item_id=payload["item_id"],
                    filter_digest=payload["filter_digest"],
                    projection_digest=payload["projection_digest"],
                    schema_version=payload["schema_version"],
                    ordering=tuple(payload["ordering"]),
                )
                if (
                    self.encode(semantic_cursor).split(".", 1)[0]
                    != encoded_part
                ):
                    raise GuidelinePolicyContractError("invalid_cursor")
                return semantic_cursor
            raise GuidelinePolicyContractError("invalid_cursor")
        except GuidelinePolicyContractError:
            raise
        except Exception as exc:
            raise GuidelinePolicyContractError("invalid_cursor") from exc


__all__ = [
    "POLICY_IMPACT_ORDERING",
    "POLICY_KEYSET_CONTRACT_VERSION",
    "POLICY_CURSOR_TOKEN_MAX_LENGTH",
    "PolicyCursorCodec",
    "GuidelineImpactItemPage",
    "GuidelineRevisionListItem",
    "GuidelineRevisionProjectionPage",
    "PolicyImpactPageCursor",
    "PolicyKeysetPage",
    "PolicyProjection",
    "SemanticAssessmentPageCursor",
    "SemanticFindingPageCursor",
    "SemanticSkipPageCursor",
    "SemanticWaiverPageCursor",
    "project_guideline_revision",
]
