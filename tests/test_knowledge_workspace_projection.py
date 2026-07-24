"""C-IMP2 conformance for the ResourceLineage.v2 Knowledge Workspace."""

from __future__ import annotations

import base64
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.knowledge_workspace import (
    KnowledgeWorkspaceProjectionError,
    KnowledgeWorkspaceProjector,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.operational_rest import (
    GetEffectiveResourcesCommand,
    GetEffectiveResourcesUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


def _attachment(
    *,
    root_id: str,
    resource_id: str,
    version: str | None,
    kind: str,
    inherited: bool,
    body_ref: bool = True,
) -> dict:
    raw = {
        "id": resource_id,
        "title": f"KB {root_id} {version or 'legacy'}",
        "root_source_kb_id": root_id,
        "source_version": version,
        "source_entity_type": "card",
        "source_entity_id": resource_id,
        "knowledge_assignment_id": f"assignment-{resource_id}",
        "knowledge_assignment_mode": "linked",
        "knowledge_assignment_state": "active",
        "relevance_links": [
            {
                "assignment_id": f"assignment-{resource_id}",
                "source": "knowledge_assignment",
            }
        ],
    }
    if not body_ref:
        raw = {}
    return {
        "contract_version": 2,
        "resource_type": "knowledge_base",
        "resource_id": resource_id,
        "id": resource_id,
        "title": f"KB {root_id} {version or 'legacy'}",
        "unique_resource_id": f"knowledge_base:{root_id}",
        "attachment_kind": kind,
        "source_entity_type": "card" if kind == "direct" else "spec",
        "source_entity_id": resource_id,
        "source_entity_title": "Source",
        "coverage_state": "not_required",
        "origin_class": "local" if kind == "direct" else "inherited",
        "effective": True,
        "inherited": inherited,
        "raw": raw,
        "root_id": root_id,
        "source_revision": version,
        "source_content_sha256": f"sha-{root_id}-{version}",
        "revision_stamp": {
            "root_id": root_id,
            "immediate_parent_id": None
            if kind == "direct"
            else f"parent-{resource_id}",
            "source_revision": version,
            "source_content_sha256": f"sha-{root_id}-{version}",
        },
    }


def _projection(attachments: list[dict], resources: list[dict] | None = None) -> dict:
    canonical_roots = {
        item["unique_resource_id"]
        for item in attachments
        if item.get("effective", True)
    }
    return {
        "board_id": "board-c",
        "entity_type": "card",
        "entity_id": "card-c",
        "resources": {
            "architecture": [],
            "mockup": [],
            "knowledge_base": list(resources or []),
        },
        "lineage_counts": {
            "unique_effective_count": len(canonical_roots),
            "raw_attachment_count": len(attachments),
        },
        "resource_lineage": {
            "contract_version": 2,
            "attachments": attachments,
            "counts": {
                "unique_effective_count": len(canonical_roots),
                "raw_attachment_count": len(attachments),
            },
        },
    }


def _serialized_bytes(value: object) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode()
    )


def test_ts_fab5c273_groups_only_same_root_version_and_keeps_canonical_identity() -> (
    None
):
    attachments = [
        _attachment(
            root_id="root-a",
            resource_id="root-a-v1-parent",
            version="v1",
            kind="inherited_reference",
            inherited=True,
        ),
        _attachment(
            root_id="root-b",
            resource_id="root-b-legacy",
            version=None,
            kind="inherited_reference",
            inherited=True,
        ),
        _attachment(
            root_id="root-a",
            resource_id="root-a-v2-parent",
            version="v2",
            kind="inherited_reference",
            inherited=True,
        ),
        _attachment(
            root_id="root-a",
            resource_id="root-a-v1-local",
            version="v1",
            kind="direct",
            inherited=False,
        ),
    ]
    source_before = deepcopy(attachments)

    first = KnowledgeWorkspaceProjector.project(
        _projection(attachments),
        profile="summary",
        limit=100,
    )
    reversed_order = KnowledgeWorkspaceProjector.project(
        _projection(list(reversed(attachments))),
        profile="summary",
        limit=100,
    )

    assert attachments == source_before
    assert first["items"] == reversed_order["items"]
    assert first["unique_effective_count"] == 2
    assert first["raw_attachment_count"] == 4
    assert first["workspace_item_count"] == 3
    assert first["unique_root_version_count"] == 3
    assert [item["versioned_projection_id"] for item in first["items"]] == [
        "knowledge_base:root-a@v1",
        "knowledge_base:root-a@v2",
        "knowledge_base:root-b@legacy",
    ]
    v1, v2, legacy = first["items"]
    assert v1["canonical_unique_resource_id"] == "knowledge_base:root-a"
    assert v1["representative_resource_id"] == "root-a-v1-local"
    assert v1["attachment_kind"] == "direct"
    assert len(v1["physical_attachments"]) == 2
    assert v2["representative_resource_id"] == "root-a-v2-parent"
    assert len(v2["physical_attachments"]) == 1
    assert legacy["resource_version"] is None
    assert legacy["grandfathered"] is True
    assert legacy["versioned_projection_id"].endswith("@legacy")
    assert all("body" not in item for item in first["items"])
    assert all(item["detail_cursor"] for item in first["items"])
    assert all("raw" not in json.dumps(item) for item in first["items"])


def test_workspace_filters_non_kb_lineage_before_counts_cursor_and_hydration() -> None:
    root_v1_parent = _attachment(
        root_id="root-a",
        resource_id="root-a-v1-parent",
        version="v1",
        kind="inherited_reference",
        inherited=True,
    )
    root_v1_local = _attachment(
        root_id="root-a",
        resource_id="root-a-v1-local",
        version="v1",
        kind="direct",
        inherited=False,
    )
    root_v2 = _attachment(
        root_id="root-a",
        resource_id="root-a-v2",
        version="v2",
        kind="direct",
        inherited=False,
    )
    historical = {
        **_attachment(
            root_id="root-a",
            resource_id="root-a-history",
            version="v0",
            kind="inherited_reference",
            inherited=True,
        ),
        "effective": False,
    }
    architecture = {
        **_attachment(
            root_id="architecture-a",
            resource_id="architecture-a",
            version="v1",
            kind="direct",
            inherited=False,
        ),
        "resource_type": "architecture",
        "unique_resource_id": "architecture:architecture-a",
    }
    mockup = {
        **_attachment(
            root_id="mockup-a",
            resource_id="mockup-a",
            version="v1",
            kind="direct",
            inherited=False,
        ),
        "resource_type": "mockup",
        "unique_resource_id": "mockup:mockup-a",
    }
    projection = _projection(
        [
            architecture,
            mockup,
            root_v1_parent,
            root_v1_local,
            root_v2,
            historical,
        ]
    )

    first = KnowledgeWorkspaceProjector.project(
        projection,
        profile="summary",
        limit=1,
    )
    second = KnowledgeWorkspaceProjector.project(
        projection,
        profile="summary",
        cursor=first["next_cursor"],
        limit=1,
    )

    assert [item["resource_type"] for item in first["items"] + second["items"]] == [
        "knowledge_base",
        "knowledge_base",
    ]
    assert first["unique_effective_count"] == 1
    assert first["raw_attachment_count"] == 4
    assert first["workspace_item_count"] == 2
    assert first["unique_root_version_count"] == 2
    assert second["next_cursor"] is None

    requests = KnowledgeWorkspaceProjector.hydration_requests(
        projection,
        profile="detail",
        cursor=first["items"][0]["detail_cursor"],
    )
    assert len(requests) == 1
    assert requests[0]["resource_type"] == "knowledge_base"


def test_workspace_preserves_explicit_relevance_without_synthesizing_assignment() -> (
    None
):
    no_relevance = _attachment(
        root_id="root-empty",
        resource_id="kb-empty",
        version="v1",
        kind="direct",
        inherited=False,
    )
    no_relevance["raw"]["relevance_links"] = []
    structured = _attachment(
        root_id="root-ac",
        resource_id="kb-ac",
        version="v1",
        kind="direct",
        inherited=False,
    )
    structured_link = {
        "entity_type": "acceptance_criterion",
        "entity_id": "ac_123",
        "reason": "Supports the bounded-read acceptance criterion.",
    }
    structured["raw"]["relevance_links"] = [structured_link]

    result = KnowledgeWorkspaceProjector.project(
        _projection([no_relevance, structured]),
        profile="summary",
        limit=100,
    )
    by_root = {item["root_id"]: item for item in result["items"]}

    assert by_root["root-empty"]["relevance_links"] == []
    assert by_root["root-ac"]["relevance_links"] == [structured_link]


def test_detail_cursor_resolves_versioned_identity_after_logical_reorder() -> None:
    original = _projection(
        [
            _attachment(
                root_id="root-b",
                resource_id="kb-b",
                version="v1",
                kind="direct",
                inherited=False,
            ),
            _attachment(
                root_id="root-c",
                resource_id="kb-c",
                version="v1",
                kind="direct",
                inherited=False,
            ),
        ]
    )
    summary = KnowledgeWorkspaceProjector.project(
        original,
        profile="summary",
    )
    target = next(
        item
        for item in summary["items"]
        if item["versioned_projection_id"] == "knowledge_base:root-c@v1"
    )

    reordered = _projection(
        [
            _attachment(
                root_id="root-a",
                resource_id="kb-a",
                version="v1",
                kind="direct",
                inherited=False,
            ),
            *original["resource_lineage"]["attachments"],
        ]
    )
    detail = KnowledgeWorkspaceProjector.project(
        reordered,
        profile="detail",
        cursor=target["detail_cursor"],
    )
    requests = KnowledgeWorkspaceProjector.hydration_requests(
        reordered,
        profile="detail",
        cursor=target["detail_cursor"],
    )

    assert detail["count"] == 1
    assert detail["items"][0]["versioned_projection_id"] == "knowledge_base:root-c@v1"
    assert requests == [
        {
            "resource_type": "knowledge_base",
            "resource_id": "kb-c",
            "ref": original["resource_lineage"]["attachments"][1]["raw"],
            "attachment_kind": "direct",
            "inherited": False,
        }
    ]


def test_detail_cursor_rejects_scope_mismatch_and_unknown_target_stably() -> None:
    projection = _projection(
        [
            _attachment(
                root_id="root-b",
                resource_id="kb-b",
                version="v1",
                kind="direct",
                inherited=False,
            )
        ]
    )
    cursor = KnowledgeWorkspaceProjector.project(
        projection,
        profile="summary",
    )["items"][0]["detail_cursor"]

    mismatched_scope = deepcopy(projection)
    mismatched_scope["entity_id"] = "another-card"
    with pytest.raises(KnowledgeWorkspaceProjectionError) as mismatch:
        KnowledgeWorkspaceProjector.project(
            mismatched_scope,
            profile="detail",
            cursor=cursor,
        )
    assert mismatch.value.code == "knowledge_workspace_cursor_identity_mismatch"

    padded = cursor + "=" * (-len(cursor) % 4)
    forged_payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    forged_payload["fingerprint"] = "0" * 64
    forged_cursor = (
        base64.urlsafe_b64encode(
            json.dumps(
                forged_payload,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        .decode("ascii")
        .rstrip("=")
    )
    with pytest.raises(KnowledgeWorkspaceProjectionError) as forged:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="detail",
            cursor=forged_cursor,
        )
    assert forged.value.code == "knowledge_workspace_cursor_identity_mismatch"

    target_removed = _projection(
        [
            _attachment(
                root_id="root-a",
                resource_id="kb-a",
                version="v1",
                kind="direct",
                inherited=False,
            )
        ]
    )
    with pytest.raises(KnowledgeWorkspaceProjectionError) as missing:
        KnowledgeWorkspaceProjector.project(
            target_removed,
            profile="detail",
            cursor=cursor,
        )
    assert missing.value.code == "knowledge_workspace_cursor_target_not_found"


def test_v1_page_cursor_remains_compatible_but_is_not_a_detail_identity() -> None:
    projection = _projection(
        [
            _attachment(
                root_id="root-a",
                resource_id="kb-a",
                version="v1",
                kind="direct",
                inherited=False,
            ),
            _attachment(
                root_id="root-b",
                resource_id="kb-b",
                version="v1",
                kind="direct",
                inherited=False,
            ),
        ]
    )
    first = KnowledgeWorkspaceProjector.project(
        projection,
        profile="summary",
        limit=1,
    )
    page_cursor = first["next_cursor"]
    assert page_cursor is not None
    padded = page_cursor + "=" * (-len(page_cursor) % 4)
    assert json.loads(base64.urlsafe_b64decode(padded).decode("utf-8")) == {
        "offset": 1,
        "v": 1,
    }

    second = KnowledgeWorkspaceProjector.project(
        projection,
        profile="summary",
        cursor=page_cursor,
        limit=1,
    )
    assert second["items"][0]["versioned_projection_id"] == ("knowledge_base:root-b@v1")

    with pytest.raises(KnowledgeWorkspaceProjectionError) as page_for_detail:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="detail",
            cursor=page_cursor,
        )
    assert page_for_detail.value.code == "knowledge_workspace_cursor_kind_mismatch"

    with pytest.raises(KnowledgeWorkspaceProjectionError) as detail_for_page:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="summary",
            cursor=first["items"][0]["detail_cursor"],
        )
    assert detail_for_page.value.code == "knowledge_workspace_cursor_kind_mismatch"


@pytest.mark.asyncio
async def test_ts_fab5c273_use_case_reuses_one_authoritative_resolution_and_lazy_hydrator() -> (
    None
):
    attachments = [
        _attachment(
            root_id="root-a",
            resource_id="kb-v1",
            version="v1",
            kind="direct",
            inherited=False,
        )
    ]
    projection = _projection(attachments)
    summary = KnowledgeWorkspaceProjector.project(projection)
    lineage = SimpleNamespace(
        to_dict=lambda: projection["resource_lineage"],
    )
    resource_gate = SimpleNamespace(
        get_effective_resources=AsyncMock(return_value=projection),
        _resolve_resource_lineage=AsyncMock(return_value=lineage),
        _effective_resource_item=AsyncMock(
            return_value={
                "id": "kb-v1",
                "resource_id": "kb-v1",
                "resource": {"content": "hydrated-on-detail-only"},
                "hydrated": True,
                "ref": attachments[0]["raw"],
            }
        ),
    )
    board = SimpleNamespace(
        id="board-c",
        owner_id="actor-c",
        realm_id=LOCAL_REALM_ID,
    )
    entity = SimpleNamespace(id="card-c", board_id="board-c")
    uow = SimpleNamespace(
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(
            shares=SimpleNamespace(get_user_permission=AsyncMock(return_value=None)),
            cards=SimpleNamespace(get_card=AsyncMock(return_value=entity)),
            resource_gate=resource_gate,
        ),
    )

    result = await GetEffectiveResourcesUseCase().execute(
        GetEffectiveResourcesCommand(
            "board-c",
            "card",
            "card-c",
            profile="detail",
            cursor=summary["items"][0]["detail_cursor"],
        ),
        actor=ActorContext(
            "actor-c",
            "rest",
            realm_id=LOCAL_REALM_ID,
        ),
        uow=uow,
    )

    resource_gate._resolve_resource_lineage.assert_awaited_once_with(
        "board-c",
        "card",
        "card-c",
        include_coverage=False,
        projection_profile="summary",
    )
    resource_gate.get_effective_resources.assert_not_awaited()
    resource_gate._effective_resource_item.assert_awaited_once()
    assert result.data["count"] == 1
    assert result.data["items"][0]["body"] == {"content": "hydrated-on-detail-only"}
    assert "resource_lineage" not in result.data
    assert "resources" not in result.data


def _large_projection() -> dict:
    attachments: list[dict] = []
    resources: list[dict] = []
    for root_number in range(4):
        root_id = f"root-{root_number}"
        for version_number in range(23):
            version = f"v{version_number:02d}"
            resource_id = f"card-{root_number:02d}-{version_number:02d}"
            attachments.append(
                _attachment(
                    root_id=root_id,
                    resource_id=resource_id,
                    version=version,
                    kind="direct",
                    inherited=False,
                )
            )
            content_size = (
                200 * 1024 if (root_number, version_number) == (1, 14) else 120 * 1024
            )
            resources.append(
                {
                    "id": resource_id,
                    "resource_id": resource_id,
                    "resource": {
                        "content": (
                            f"BODY-{root_number}-{version_number}-" + "x" * content_size
                        )
                    },
                    "hydrated": True,
                    "ref": attachments[-1]["raw"],
                }
            )

    # Twelve extra physical inheritance attachments share an already present
    # (root, version).  They raise the physical total to 104 but must not add
    # workspace rows or shadow the direct representative.
    for duplicate_number in range(12):
        root_number = duplicate_number % 4
        version_number = duplicate_number
        attachments.append(
            _attachment(
                root_id=f"root-{root_number}",
                resource_id=f"parent-{duplicate_number:02d}",
                version=f"v{version_number:02d}",
                kind="inherited_reference",
                inherited=True,
            )
        )
    return _projection(attachments, resources)


def test_ts_f7f1cf23_pages_92_snapshots_with_bounded_profiles_and_private_cursors() -> (
    None
):
    projection = _large_projection()
    cursor = None
    seen: list[str] = []
    pages = 0
    first_summary = None
    while True:
        page = KnowledgeWorkspaceProjector.project(
            projection,
            profile="summary",
            cursor=cursor,
            limit=100,
        )
        first_summary = first_summary or page
        pages += 1
        assert page["response_bytes"] == _serialized_bytes(page)
        assert page["response_bytes"] <= 64 * 1024
        serialized = json.dumps(page)
        assert "BODY-" not in serialized
        assert all("body" not in item for item in page["items"])
        seen.extend(item["versioned_projection_id"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
        assert "root-" not in cursor
        assert "BODY-" not in cursor

    assert pages >= 1
    assert len(seen) == len(set(seen)) == 92
    assert first_summary is not None
    assert first_summary["unique_effective_count"] == 4
    assert first_summary["raw_attachment_count"] == 104
    assert first_summary["workspace_item_count"] == 92
    assert first_summary["unique_root_version_count"] == 92

    target = first_summary["items"][-1]
    detail = KnowledgeWorkspaceProjector.project(
        projection,
        profile="detail",
        cursor=target["detail_cursor"],
    )
    assert detail["count"] == 1
    assert (
        detail["items"][0]["versioned_projection_id"]
        == target["versioned_projection_id"]
    )
    assert detail["response_bytes"] <= 256 * 1024

    oversized_summary = None
    cursor = None
    while oversized_summary is None:
        page = KnowledgeWorkspaceProjector.project(
            projection,
            profile="summary",
            cursor=cursor,
            limit=100,
        )
        oversized_summary = next(
            (
                item
                for item in page["items"]
                if item["representative_resource_id"] == "card-01-14"
            ),
            None,
        )
        cursor = page["next_cursor"]
        if cursor is None and oversized_summary is None:
            raise AssertionError("oversized detail target not found")
    oversized_detail = KnowledgeWorkspaceProjector.project(
        projection,
        profile="detail",
        cursor=oversized_summary["detail_cursor"],
    )
    oversized_item = oversized_detail["items"][0]
    assert "body" not in oversized_item
    assert oversized_item["body_omitted_reason"] == "body_size_limit"
    assert oversized_item["body_ref"]["resource_id"] == "card-01-14"

    full = KnowledgeWorkspaceProjector.project(projection, profile="full")
    assert full["count"] == 10
    assert full["truncated"] is True
    assert full["response_bytes"] == _serialized_bytes(full)
    assert full["response_bytes"] <= 1024 * 1024
    assert any(
        item.get("body_omitted_reason") == "response_budget" for item in full["items"]
    )
    for item in full["items"]:
        body = item.get("body")
        if body is not None:
            assert body["content"].startswith("BODY-")


def test_ts_f7f1cf23_rejects_unbounded_or_forged_page_requests() -> None:
    projection = _projection(
        [
            _attachment(
                root_id="root-a",
                resource_id="kb-a",
                version="v1",
                kind="direct",
                inherited=False,
            )
        ]
    )

    with pytest.raises(KnowledgeWorkspaceProjectionError) as detail_limit:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="detail",
            limit=2,
        )
    assert detail_limit.value.code == "knowledge_workspace_invalid_limit"

    with pytest.raises(KnowledgeWorkspaceProjectionError) as summary_limit:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="summary",
            limit=101,
        )
    assert summary_limit.value.code == "knowledge_workspace_invalid_limit"

    with pytest.raises(KnowledgeWorkspaceProjectionError) as invalid_cursor:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="summary",
            cursor="not-a-cursor",
        )
    assert invalid_cursor.value.code == "knowledge_workspace_invalid_cursor"

    with pytest.raises(KnowledgeWorkspaceProjectionError) as invalid_profile:
        KnowledgeWorkspaceProjector.project(
            projection,
            profile="legacy",
        )
    assert invalid_profile.value.code == "knowledge_workspace_invalid_profile"
