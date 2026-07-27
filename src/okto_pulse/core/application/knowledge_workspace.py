"""Pure read-side projection for the shared Knowledge Workspace.

The workspace is deliberately downstream of ``ResourceLineage.v2``.  It does
not resolve origins, calculate canonical roots, persist snapshots, or alter
the Resource Gate write model.  Instead it groups the already resolved
physical attachments by the canonical identity and revision stamp published
by ResourceLineage.

The projection is bounded at two levels:

* profile-specific page and response budgets;
* profile-specific per-body budgets.

Items are only removed at item boundaries.  A body that does not fit is
replaced by an explicit omission reason and a stable physical resource
reference, leaving the logical item itself visible.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import hmac
import json
from typing import Any, Literal

KnowledgeWorkspaceProfile = Literal["summary", "detail", "full"]

CONTRACT_VERSION = 2
SUPPORTED_PROFILES: tuple[str, ...] = ("summary", "detail", "full")
LEGACY_VERSION_TOKEN = "legacy"
_WORKSPACE_RESOURCE_TYPE = "knowledge_base"
_DETAIL_CURSOR_VERSION = 2
_DETAIL_CURSOR_KIND = "detail"


@dataclass(frozen=True, slots=True)
class _ProfilePolicy:
    default_limit: int
    max_limit: int
    response_budget_bytes: int
    body_budget_bytes: int | None


@dataclass(frozen=True, slots=True)
class _PageCursor:
    offset: int
    kind: Literal["page"] = "page"


@dataclass(frozen=True, slots=True)
class _DetailCursor:
    versioned_projection_id: str
    fingerprint: str
    kind: Literal["detail"] = "detail"


_DecodedCursor = _PageCursor | _DetailCursor


_PROFILE_POLICIES: dict[str, _ProfilePolicy] = {
    "summary": _ProfilePolicy(
        default_limit=25,
        max_limit=100,
        response_budget_bytes=64 * 1024,
        body_budget_bytes=None,
    ),
    "detail": _ProfilePolicy(
        default_limit=1,
        max_limit=1,
        response_budget_bytes=256 * 1024,
        body_budget_bytes=192 * 1024,
    ),
    "full": _ProfilePolicy(
        default_limit=10,
        max_limit=100,
        response_budget_bytes=1024 * 1024,
        body_budget_bytes=256 * 1024,
    ),
}


class KnowledgeWorkspaceProjectionError(ValueError):
    """Stable transport-neutral projection failure."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        status_code: int = 422,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})
        self.status_code = status_code

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": str(self),
            "status_code": self.status_code,
            "details": dict(self.details),
        }


def _json_bytes(value: Any) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    )


def _encode_cursor(offset: int) -> str:
    """Encode the stable v1 page-cursor contract."""

    raw = json.dumps(
        {"v": 1, "offset": offset},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _detail_cursor_fingerprint(
    projection: Mapping[str, Any],
    versioned_projection_id: str,
) -> str:
    """Bind a logical item identity to its entity-scoped read projection."""

    identity = {
        "board_id": str(projection.get("board_id") or ""),
        "contract_version": CONTRACT_VERSION,
        "entity_id": str(projection.get("entity_id") or ""),
        "entity_type": str(projection.get("entity_type") or ""),
        "versioned_projection_id": versioned_projection_id,
    }
    raw = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _encode_detail_cursor(
    projection: Mapping[str, Any],
    versioned_projection_id: str,
) -> str:
    raw = json.dumps(
        {
            "fingerprint": _detail_cursor_fingerprint(
                projection,
                versioned_projection_id,
            ),
            "kind": _DETAIL_CURSOR_KIND,
            "v": _DETAIL_CURSOR_VERSION,
            "versioned_projection_id": versioned_projection_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> _DecodedCursor | None:
    if cursor is None or cursor == "":
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("unsupported cursor envelope")
        if payload.get("v") == 1 and set(payload) == {"v", "offset"}:
            offset = payload["offset"]
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise ValueError("invalid cursor offset")
            return _PageCursor(offset=offset)
        if (
            payload.get("v") == _DETAIL_CURSOR_VERSION
            and payload.get("kind") == _DETAIL_CURSOR_KIND
            and set(payload)
            == {
                "fingerprint",
                "kind",
                "v",
                "versioned_projection_id",
            }
        ):
            versioned_projection_id = payload["versioned_projection_id"]
            fingerprint = payload["fingerprint"]
            if (
                not isinstance(versioned_projection_id, str)
                or not versioned_projection_id
                or not isinstance(fingerprint, str)
                or len(fingerprint) != 64
                or any(character not in "0123456789abcdef" for character in fingerprint)
            ):
                raise ValueError("invalid detail cursor identity")
            return _DetailCursor(
                versioned_projection_id=versioned_projection_id,
                fingerprint=fingerprint,
            )
        raise ValueError("unsupported cursor envelope")
    except Exception as exc:
        raise KnowledgeWorkspaceProjectionError(
            "knowledge_workspace_invalid_cursor",
            "The Knowledge Workspace cursor is invalid or expired.",
        ) from exc


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result = dict(value)
        raw = result.pop("raw", None)
        if isinstance(raw, Mapping):
            # ``ResolvedResourceLineage.to_dict`` deliberately exposes its
            # adapter ref.  Keep it internal to this projector so it can drive
            # lazy hydration without leaking raw payloads into the workspace.
            result["_workspace_raw"] = dict(raw)
        return result
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        if isinstance(payload, Mapping):
            result = dict(payload)
            raw = getattr(value, "raw", None)
            if isinstance(raw, Mapping):
                # ``raw`` is used only as a body/relevance source.  It is
                # stripped from every public attachment projection below.
                result["_workspace_raw"] = dict(raw)
            return result
    raise KnowledgeWorkspaceProjectionError(
        "knowledge_workspace_invalid_projection",
        "ResourceLineage returned a non-object attachment.",
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _canonical_root_id(
    *,
    resource_type: str,
    canonical_unique_resource_id: str,
    attachment: Mapping[str, Any],
) -> str:
    revision_stamp = attachment.get("revision_stamp")
    stamp = revision_stamp if isinstance(revision_stamp, Mapping) else {}
    root_id = _optional_text(stamp.get("root_id")) or _optional_text(
        attachment.get("root_id")
    )
    if root_id is not None:
        return root_id
    prefix = f"{resource_type}:"
    if canonical_unique_resource_id.startswith(prefix):
        # This is not a competing identity calculation: ResourceLineage has
        # already published the canonical ``type:root`` id.  The split merely
        # exposes its root component in the read DTO.
        root_id = _optional_text(canonical_unique_resource_id[len(prefix) :])
    if root_id is None:
        raise KnowledgeWorkspaceProjectionError(
            "knowledge_workspace_lineage_identity_missing",
            "A ResourceLineage attachment has no canonical root id.",
            details={
                "resource_type": resource_type,
                "canonical_unique_resource_id": canonical_unique_resource_id,
            },
        )
    return root_id


def _source_revision(attachment: Mapping[str, Any]) -> str | None:
    revision_stamp = attachment.get("revision_stamp")
    stamp = revision_stamp if isinstance(revision_stamp, Mapping) else {}
    return _optional_text(stamp.get("source_revision")) or _optional_text(
        attachment.get("source_revision")
    )


def _physical_attachment(
    attachment: Mapping[str, Any],
    *,
    resource_version: str | None,
) -> dict[str, Any]:
    revision_stamp = attachment.get("revision_stamp")
    stamp = dict(revision_stamp) if isinstance(revision_stamp, Mapping) else None
    return {
        "resource_id": _optional_text(
            attachment.get("resource_id") or attachment.get("id")
        ),
        "attachment_kind": _optional_text(attachment.get("attachment_kind")),
        "inherited": bool(attachment.get("inherited")),
        "source_entity_type": _optional_text(attachment.get("source_entity_type")),
        "source_entity_id": _optional_text(attachment.get("source_entity_id")),
        "source_entity_title": _optional_text(attachment.get("source_entity_title")),
        "effective": bool(attachment.get("effective", True)),
        "resource_version": resource_version,
        "revision_stamp": stamp,
    }


def _attachment_sort_key(attachment: Mapping[str, Any]) -> tuple[str, ...]:
    kind = _optional_text(attachment.get("attachment_kind")) or ""
    kind_rank = {
        "direct": "0",
        "inherited_reference": "1",
        "not_applicable": "2",
    }.get(kind, "3")
    return (
        kind_rank,
        _optional_text(attachment.get("resource_id") or attachment.get("id")) or "",
        _optional_text(attachment.get("source_entity_type")) or "",
        _optional_text(attachment.get("source_entity_id")) or "",
    )


def _representative(
    attachments: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    return min(attachments, key=_attachment_sort_key)


def _hydrated_index(
    projection: Mapping[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    resources = projection.get("resources")
    if not isinstance(resources, Mapping):
        return result
    for resource_type, values in resources.items():
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            continue
        for value in values:
            if not isinstance(value, Mapping):
                continue
            resource_id = _optional_text(value.get("resource_id") or value.get("id"))
            if resource_id is not None:
                result[(str(resource_type), resource_id)] = dict(value)
    return result


def _raw_source(attachment: Mapping[str, Any]) -> dict[str, Any]:
    raw = attachment.get("_workspace_raw")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _body_from_sources(
    attachment: Mapping[str, Any],
    hydrated: Mapping[str, Any] | None,
) -> Any | None:
    if hydrated is not None:
        resource = hydrated.get("resource")
        if isinstance(resource, Mapping):
            return dict(resource)
        if resource is not None:
            return resource
        if "body" in hydrated:
            return hydrated["body"]

    raw = _raw_source(attachment)
    resource = raw.get("resource")
    if isinstance(resource, Mapping):
        return dict(resource)
    if resource is not None:
        return resource
    if "body" in raw:
        return raw["body"]

    # Adapter refs may already carry one of the canonical body fields.  Keep
    # it wrapped under the original name so transports do not need a
    # resource-type-specific scalar contract.
    body_fields = (
        "content",
        "html_content",
        "global_description",
        "description",
    )
    body = {field: raw[field] for field in body_fields if field in raw}
    return body or None


def _relevance_links(
    attachment: Mapping[str, Any],
    hydrated: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    sources: list[Mapping[str, Any]] = [attachment, _raw_source(attachment)]
    if hydrated is not None:
        sources.append(hydrated)
        ref = hydrated.get("ref")
        if isinstance(ref, Mapping):
            sources.append(ref)

    links: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        candidate = source.get("relevance_links")
        if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
            for value in candidate:
                if not isinstance(value, Mapping):
                    continue
                link = dict(value)
                fingerprint = json.dumps(
                    link, sort_keys=True, separators=(",", ":"), default=str
                )
                if fingerprint not in seen:
                    seen.add(fingerprint)
                    links.append(link)
    return links


def _hydrate_metadata(
    attachment: Mapping[str, Any],
    hydrated: Mapping[str, Any] | None,
) -> dict[str, Any]:
    raw = _raw_source(attachment)
    hydrated_ref = hydrated.get("ref") if isinstance(hydrated, Mapping) else None
    ref = hydrated_ref if isinstance(hydrated_ref, Mapping) else {}
    return {
        **raw,
        **ref,
        **{
            key: value
            for key, value in attachment.items()
            if not key.startswith("_workspace_")
        },
    }


def _fixed_response_bytes(response: dict[str, Any]) -> int:
    response["response_bytes"] = 0
    for _ in range(8):
        size = _json_bytes(response)
        if response["response_bytes"] == size:
            return size
        response["response_bytes"] = size
    return _json_bytes(response)


class KnowledgeWorkspaceProjector:
    """Build the bounded, version-aware workspace from ResourceLineage output."""

    @classmethod
    def hydration_requests(
        cls,
        projection: Mapping[str, Any],
        *,
        profile: str,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return only the existing Resource Gate hydration requests for a page.

        This helper performs no I/O.  It lets an application use case invoke the
        already-existing Resource Gate hydrator lazily for detail/full without
        teaching the projector about repositories or defining another resource
        resolver.
        """

        normalized_profile = str(profile or "summary").strip().lower()
        policy = _PROFILE_POLICIES.get(normalized_profile)
        if policy is None:
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_profile",
                "profile must be one of: summary, detail, full.",
                details={
                    "profile": profile,
                    "supported_profiles": list(SUPPORTED_PROFILES),
                },
            )
        resolved_limit = cls._resolve_limit(
            profile=normalized_profile,
            policy=policy,
            limit=limit,
        )
        if normalized_profile == "summary":
            return []
        items = cls._logical_items(projection)
        offset = cls._resolve_cursor_offset(
            projection,
            items,
            profile=normalized_profile,
            cursor=cursor,
        )
        hydrated = _hydrated_index(projection)
        requests: list[dict[str, Any]] = []
        for item in items[offset : offset + resolved_limit]:
            resource_type = str(item["resource_type"])
            resource_id = item["representative_resource_id"]
            if resource_id is None or (resource_type, resource_id) in hydrated:
                continue
            representative = item["_representative"]
            raw = _raw_source(representative)
            if not raw:
                continue
            requests.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "ref": raw,
                    "attachment_kind": representative.get("attachment_kind"),
                    "inherited": bool(representative.get("inherited")),
                }
            )
        return requests

    @classmethod
    def project(
        cls,
        projection: Mapping[str, Any],
        *,
        profile: str = "summary",
        cursor: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_profile = str(profile or "summary").strip().lower()
        policy = _PROFILE_POLICIES.get(normalized_profile)
        if policy is None:
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_profile",
                "profile must be one of: summary, detail, full.",
                details={
                    "profile": profile,
                    "supported_profiles": list(SUPPORTED_PROFILES),
                },
            )
        resolved_limit = cls._resolve_limit(
            profile=normalized_profile,
            policy=policy,
            limit=limit,
        )
        items = cls._logical_items(projection)
        total_count = len(items)
        offset = cls._resolve_cursor_offset(
            projection,
            items,
            profile=normalized_profile,
            cursor=cursor,
        )

        hydrated = _hydrated_index(projection)
        page: list[dict[str, Any]] = []
        stop = min(total_count, offset + resolved_limit)
        for index in range(offset, stop):
            item = cls._profile_item(
                items[index],
                detail_cursor=_encode_detail_cursor(
                    projection,
                    str(items[index]["versioned_projection_id"]),
                ),
                profile=normalized_profile,
                policy=policy,
                hydrated=hydrated,
            )
            proposed = [*page, item]
            candidate = cls._envelope(
                projection,
                profile=normalized_profile,
                items=proposed,
                total_count=total_count,
                end_offset=index + 1,
            )
            candidate_size = _fixed_response_bytes(candidate)
            if candidate_size <= policy.response_budget_bytes:
                page = proposed
                continue

            if "body" in item:
                item = dict(item)
                item.pop("body", None)
                item["body_omitted_reason"] = "response_budget"
                item["body_ref"] = {
                    "resource_type": item["resource_type"],
                    "resource_id": item["representative_resource_id"],
                }
                proposed = [*page, item]
                candidate = cls._envelope(
                    projection,
                    profile=normalized_profile,
                    items=proposed,
                    total_count=total_count,
                    end_offset=index + 1,
                )
                candidate_size = _fixed_response_bytes(candidate)
                if candidate_size <= policy.response_budget_bytes:
                    page = proposed
                    continue

            if page:
                break
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_item_exceeds_budget",
                "A Knowledge Workspace item cannot fit the profile response budget.",
                details={
                    "profile": normalized_profile,
                    "response_budget_bytes": policy.response_budget_bytes,
                    "versioned_projection_id": item["versioned_projection_id"],
                },
            )

        response = cls._envelope(
            projection,
            profile=normalized_profile,
            items=page,
            total_count=total_count,
            end_offset=offset + len(page),
        )
        response_size = _fixed_response_bytes(response)
        if response_size > policy.response_budget_bytes:
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_response_exceeds_budget",
                "Knowledge Workspace metadata exceeds the profile response budget.",
                details={
                    "profile": normalized_profile,
                    "response_budget_bytes": policy.response_budget_bytes,
                },
            )
        return response

    @staticmethod
    def _resolve_cursor_offset(
        projection: Mapping[str, Any],
        items: Sequence[Mapping[str, Any]],
        *,
        profile: str,
        cursor: str | None,
    ) -> int:
        decoded = _decode_cursor(cursor)
        if decoded is None:
            return 0
        if profile == "detail":
            if not isinstance(decoded, _DetailCursor):
                raise KnowledgeWorkspaceProjectionError(
                    "knowledge_workspace_cursor_kind_mismatch",
                    "The Knowledge Workspace cursor is not valid for the detail profile.",
                    details={
                        "cursor_kind": decoded.kind,
                        "profile": profile,
                    },
                )
            versioned_projection_id = decoded.versioned_projection_id
            fingerprint = decoded.fingerprint
            expected_fingerprint = _detail_cursor_fingerprint(
                projection,
                versioned_projection_id,
            )
            if not hmac.compare_digest(fingerprint, expected_fingerprint):
                raise KnowledgeWorkspaceProjectionError(
                    "knowledge_workspace_cursor_identity_mismatch",
                    "The Knowledge Workspace detail cursor belongs to a different projection.",
                    details={
                        "profile": profile,
                        "versioned_projection_id": versioned_projection_id,
                    },
                )
            for index, item in enumerate(items):
                if (
                    str(item.get("versioned_projection_id") or "")
                    == versioned_projection_id
                ):
                    return index
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_cursor_target_not_found",
                "The Knowledge Workspace detail cursor target is no longer available.",
                details={
                    "profile": profile,
                    "versioned_projection_id": versioned_projection_id,
                },
            )

        if not isinstance(decoded, _PageCursor):
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_cursor_kind_mismatch",
                "The Knowledge Workspace cursor is not valid for a page profile.",
                details={
                    "cursor_kind": decoded.kind,
                    "profile": profile,
                },
            )
        offset = decoded.offset
        if offset > len(items):
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_cursor",
                "The Knowledge Workspace cursor is outside this projection.",
                details={"offset": offset, "total_count": len(items)},
            )
        return offset

    @staticmethod
    def _resolve_limit(
        *,
        profile: str,
        policy: _ProfilePolicy,
        limit: int | None,
    ) -> int:
        if limit is None:
            return policy.default_limit
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_limit",
                "limit must be an integer.",
                details={"limit": limit},
            )
        if limit < 1 or limit > policy.max_limit:
            message = (
                "detail profile requires limit=1."
                if profile == "detail"
                else f"limit must be between 1 and {policy.max_limit}."
            )
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_limit",
                message,
                details={
                    "profile": profile,
                    "limit": limit,
                    "max_limit": policy.max_limit,
                },
            )
        return limit

    @staticmethod
    def _logical_items(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
        lineage_value = projection.get("resource_lineage")
        lineage = lineage_value if isinstance(lineage_value, Mapping) else projection
        attachments_value = lineage.get("attachments")
        if not isinstance(attachments_value, Sequence) or isinstance(
            attachments_value, (str, bytes)
        ):
            raise KnowledgeWorkspaceProjectionError(
                "knowledge_workspace_invalid_projection",
                "ResourceLineage projection must contain an attachments list.",
            )

        grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
        identity: dict[tuple[str, str | None], tuple[str, str]] = {}
        for value in attachments_value:
            attachment = _mapping(value)
            if (
                not bool(attachment.get("effective", True))
                or attachment.get("attachment_kind") == "not_applicable"
            ):
                continue
            resource_type = _optional_text(attachment.get("resource_type"))
            if resource_type is None:
                raise KnowledgeWorkspaceProjectionError(
                    "knowledge_workspace_lineage_identity_missing",
                    "A ResourceLineage attachment has no resource type.",
                )
            # Knowledge Workspace is intentionally a KB-only projection.
            # Architecture and mockup lineage remain available through their
            # dedicated experiences and must not consume a workspace cursor,
            # page slot, response budget, or count.
            if resource_type != _WORKSPACE_RESOURCE_TYPE:
                continue
            canonical_id = _optional_text(
                attachment.get("canonical_unique_resource_id")
                or attachment.get("unique_resource_id")
            )
            if canonical_id is None:
                raise KnowledgeWorkspaceProjectionError(
                    "knowledge_workspace_lineage_identity_missing",
                    "A ResourceLineage attachment has no canonical identity.",
                )
            root_id = _canonical_root_id(
                resource_type=resource_type,
                canonical_unique_resource_id=canonical_id,
                attachment=attachment,
            )
            version = _source_revision(attachment)
            key = (canonical_id, version)
            grouped.setdefault(key, []).append(attachment)
            identity[key] = (resource_type, root_id)

        logical: list[dict[str, Any]] = []
        for (canonical_id, version), attachments in grouped.items():
            resource_type, root_id = identity[(canonical_id, version)]
            representative = _representative(attachments)
            representative_id = _optional_text(
                representative.get("resource_id") or representative.get("id")
            )
            version_token = version or LEGACY_VERSION_TOKEN
            revision_stamp = representative.get("revision_stamp")
            stamp = revision_stamp if isinstance(revision_stamp, Mapping) else {}
            physical = [
                _physical_attachment(item, resource_version=version)
                for item in sorted(attachments, key=_attachment_sort_key)
            ]
            logical.append(
                {
                    "resource_type": resource_type,
                    "canonical_unique_resource_id": canonical_id,
                    "versioned_projection_id": f"{canonical_id}@{version_token}",
                    "root_id": root_id,
                    "resource_version": version,
                    "representative_resource_id": representative_id,
                    "title": _optional_text(representative.get("title")),
                    "attachment_kind": _optional_text(
                        representative.get("attachment_kind")
                    ),
                    "inherited": bool(representative.get("inherited")),
                    "grandfathered": version is None,
                    "provenance": {
                        "source_entity_type": _optional_text(
                            representative.get("source_entity_type")
                        ),
                        "source_entity_id": _optional_text(
                            representative.get("source_entity_id")
                        ),
                        "source_entity_title": _optional_text(
                            representative.get("source_entity_title")
                        ),
                        "origin_class": _optional_text(
                            representative.get("origin_class")
                        ),
                        "source_revision": version,
                        "source_content_sha256": _optional_text(
                            representative.get("source_content_sha256")
                            or stamp.get("source_content_sha256")
                        ),
                    },
                    "physical_attachments": physical,
                    "_representative": representative,
                    "_sort_version": ((0, "") if version is None else (1, version)),
                }
            )
        logical.sort(
            key=lambda item: (
                item["resource_type"],
                item["root_id"],
                item["_sort_version"],
                item["representative_resource_id"] or "",
            )
        )
        return logical

    @staticmethod
    def _profile_item(
        logical_item: Mapping[str, Any],
        *,
        detail_cursor: str,
        profile: str,
        policy: _ProfilePolicy,
        hydrated: Mapping[tuple[str, str], Mapping[str, Any]],
    ) -> dict[str, Any]:
        representative = logical_item["_representative"]
        resource_type = str(logical_item["resource_type"])
        representative_id = logical_item["representative_resource_id"]
        hydrated_item = (
            hydrated.get((resource_type, representative_id))
            if representative_id is not None
            else None
        )
        metadata = _hydrate_metadata(representative, hydrated_item)
        item = {
            key: value for key, value in logical_item.items() if not key.startswith("_")
        }
        item.update(
            {
                "detail_cursor": detail_cursor,
                "stale": bool(
                    metadata.get("stale") or metadata.get("knowledge_assignment_stale")
                ),
                "superseded": bool(
                    metadata.get("superseded")
                    or metadata.get("is_superseded")
                    or metadata.get("effective") is False
                ),
                "relevance_links": _relevance_links(representative, hydrated_item),
            }
        )
        if profile == "summary":
            item["body_omitted_reason"] = "profile_summary"
            item["body_ref"] = {
                "resource_type": resource_type,
                "resource_id": representative_id,
            }
            return item

        body = _body_from_sources(representative, hydrated_item)
        if body is None:
            item["body_omitted_reason"] = "body_unavailable"
            item["body_ref"] = {
                "resource_type": resource_type,
                "resource_id": representative_id,
            }
            return item
        body_size = _json_bytes(body)
        if (
            policy.body_budget_bytes is not None
            and body_size > policy.body_budget_bytes
        ):
            item["body_omitted_reason"] = "body_size_limit"
            item["body_ref"] = {
                "resource_type": resource_type,
                "resource_id": representative_id,
            }
            return item
        item["body"] = body
        return item

    @staticmethod
    def _envelope(
        projection: Mapping[str, Any],
        *,
        profile: str,
        items: list[dict[str, Any]],
        total_count: int,
        end_offset: int,
    ) -> dict[str, Any]:
        lineage_value = projection.get("resource_lineage")
        lineage = lineage_value if isinstance(lineage_value, Mapping) else projection
        logical_items = KnowledgeWorkspaceProjector._logical_items(projection)
        canonical_ids = {
            str(item["canonical_unique_resource_id"])
            for item in logical_items
        }
        attachments = lineage.get("attachments")
        raw_attachment_count = (
            sum(
                1
                for value in attachments
                if _optional_text(_mapping(value).get("resource_type"))
                == _WORKSPACE_RESOURCE_TYPE
            )
            if isinstance(attachments, Sequence)
            and not isinstance(attachments, (str, bytes))
            else 0
        )
        truncated = end_offset < total_count
        return {
            "contract_version": CONTRACT_VERSION,
            "board_id": projection.get("board_id"),
            "entity_type": projection.get("entity_type"),
            "entity_id": projection.get("entity_id"),
            "profile": profile,
            "items": items,
            "count": len(items),
            "total_count": total_count,
            "next_cursor": _encode_cursor(end_offset) if truncated else None,
            "truncated": truncated,
            "unique_effective_count": len(canonical_ids),
            "raw_attachment_count": raw_attachment_count,
            "workspace_item_count": total_count,
            "unique_root_version_count": total_count,
            "response_bytes": 0,
        }


__all__ = [
    "CONTRACT_VERSION",
    "KnowledgeWorkspaceProfile",
    "KnowledgeWorkspaceProjectionError",
    "KnowledgeWorkspaceProjector",
    "LEGACY_VERSION_TOKEN",
    "SUPPORTED_PROFILES",
]
