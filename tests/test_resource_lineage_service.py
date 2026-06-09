from __future__ import annotations

import pytest

from okto_pulse.core.services.resource_lineage import (
    AmbiguousResourceOrigin,
    LineageEntityRef,
    METRIC_DEDUP_GROUPS_TOTAL,
    METRIC_RESOLUTION_FAILED_TOTAL,
    METRIC_RESOLVE_DURATION_MS,
    METRIC_RESOLVE_TOTAL,
    ResolvedResourceLineageProjection,
    ResolvedResourceLineageService,
    UnsupportedLineageEntityType,
    get_resource_lineage_metric_samples,
    reset_resource_lineage_observability_for_tests,
)


class FakeLineageProvider:
    def __init__(
        self,
        *,
        roots: dict[tuple[str, str], LineageEntityRef],
        parents: dict[str, list[LineageEntityRef]] | None = None,
        refs: dict[str, dict[str, list[dict]]] | None = None,
        marks: dict[tuple[str, str], dict[str, dict]] | None = None,
    ):
        self.roots = roots
        self.parents = parents or {}
        self.refs = refs or {}
        self.marks = marks or {}

    async def load_entity_ref(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> LineageEntityRef:
        del board_id
        return self.roots[(entity_type, entity_id)]

    async def load_parent_refs(
        self,
        board_id: str,
        root: LineageEntityRef,
    ) -> list[LineageEntityRef]:
        del board_id
        return list(self.parents.get(root.ref, []))

    async def collect_refs(self, ref: LineageEntityRef) -> dict[str, list[dict]]:
        empty = {"architecture": [], "mockup": [], "knowledge_base": []}
        return {**empty, **self.refs.get(ref.ref, {})}

    async def load_active_marks(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, dict]:
        del board_id
        return dict(self.marks.get((entity_type, entity_id), {}))

    def serialize_na_mark(
        self,
        mark: dict | None,
        *,
        effective: bool,
        source: LineageEntityRef | None = None,
    ) -> dict | None:
        if mark is None:
            return None
        return {
            "id": mark["id"],
            "active": True,
            "effective": effective,
            "inherited": source is not None,
            "source_entity_type": source.entity_type if source else None,
            "source_entity_id": source.entity_id if source else None,
            "justification": mark.get("justification"),
            "source_channel": "mcp",
        }


@pytest.mark.asyncio
async def test_resolver_dedupes_direct_and_inherited_architecture_by_origin() -> None:
    reset_resource_lineage_observability_for_tests()
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "arch-copy",
                        "title": "Copied architecture",
                        "source_design_id": "arch-origin",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            },
            refinement.ref: {
                "architecture": [
                    {
                        "id": "arch-origin",
                        "title": "Original architecture",
                        "source_entity_type": "refinement",
                        "source_entity_id": "ref-1",
                    }
                ]
            },
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )

    assert resolved.counts["unique_resources_count"] == 1
    assert resolved.counts["attachment_count"] == 2
    assert [item.attachment_count for item in resolved.unique_resources] == [2]
    assert {
        item.attachment_kind for item in resolved.attachments if item.resource_type == "architecture"
    } == {"direct", "inherited_reference"}
    assert resolved.coverage_obligations[0].unique_resource_id == "architecture:arch-origin"
    samples = get_resource_lineage_metric_samples()
    assert [item for item in samples if item["metric_name"] == METRIC_RESOLVE_TOTAL]
    duration_samples = [
        item for item in samples
        if item["metric_name"] == METRIC_RESOLVE_DURATION_MS
    ]
    assert duration_samples
    assert duration_samples[-1]["value"] <= 150
    assert [item for item in samples if item["metric_name"] == METRIC_DEDUP_GROUPS_TOTAL]
    for sample in samples:
        assert set(sample["labels"]) <= {
            "owner_type",
            "outcome",
            "reason",
            "resource_type",
            "coverage_state",
        }


@pytest.mark.asyncio
async def test_resolver_projects_inherited_na_as_not_applicable_attachment() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        marks={
            ("refinement", "ref-1"): {
                "mockup": {
                    "id": "na-1",
                    "justification": "Backend-only refinement.",
                }
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )

    state_by_type = {item.resource_type: item for item in resolved.resource_states}
    assert state_by_type["mockup"].state == "not_applicable"
    na_attachment = next(
        item for item in resolved.attachments if item.attachment_kind == "not_applicable"
    )
    assert na_attachment.resource_type == "mockup"
    assert na_attachment.coverage_state == "not_applicable"
    assert na_attachment.inherited is True
    assert na_attachment.effective is True
    assert not resolved.coverage_obligations


@pytest.mark.asyncio
async def test_resolver_keeps_provided_resource_when_parent_has_na_mark() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "arch-1",
                        "title": "Spec architecture",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            }
        },
        marks={
            ("refinement", "ref-1"): {
                "architecture": {
                    "id": "na-arch",
                    "justification": "Ancestor marked architecture N/A.",
                }
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )

    state_by_type = {item.resource_type: item for item in resolved.resource_states}
    assert state_by_type["architecture"].state == "provided"
    na_attachment = next(
        item for item in resolved.attachments if item.attachment_kind == "not_applicable"
    )
    assert na_attachment.resource_type == "architecture"
    assert na_attachment.effective is False
    assert resolved.coverage_obligations[0].resource_id == "arch-1"


@pytest.mark.asyncio
async def test_resolver_rejects_unsupported_entity_type() -> None:
    reset_resource_lineage_observability_for_tests()
    service = ResolvedResourceLineageService(FakeLineageProvider(roots={}))

    with pytest.raises(UnsupportedLineageEntityType) as exc:
        await service.resolve("board-1", "topic", "topic-1")

    assert exc.value.code == "unsupported_entity_type"
    failed_samples = [
        item for item in get_resource_lineage_metric_samples()
        if item["metric_name"] == METRIC_RESOLUTION_FAILED_TOTAL
    ]
    assert failed_samples
    assert failed_samples[-1]["labels"] == {
        "owner_type": "topic",
        "outcome": "failure",
        "reason": "unsupported_entity_type",
    }


@pytest.mark.asyncio
async def test_resolver_rejects_conflicting_origin_evidence() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "arch-copy",
                        "source_ref": "architecture_design:arch-origin-a",
                        "source_design_id": "arch-origin-b",
                    }
                ]
            }
        },
    )

    with pytest.raises(AmbiguousResourceOrigin) as exc:
        await ResolvedResourceLineageService(provider).resolve(
            "board-1",
            "spec",
            "spec-1",
        )

    assert exc.value.code == "ambiguous_origin"
    assert exc.value.details["resource_type"] == "architecture"


@pytest.mark.asyncio
async def test_projection_contract_exposes_lineage_without_raw_storage_rows() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "arch-copy",
                        "title": "Copied architecture",
                        "source_design_id": "arch-origin",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ],
                "knowledge_base": [
                    {
                        "id": "kb-1",
                        "title": "Reference",
                        "content": "raw content must stay out of projection",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            },
            refinement.ref: {
                "architecture": [
                    {
                        "id": "arch-origin",
                        "title": "Original architecture",
                        "source_entity_type": "refinement",
                        "source_entity_id": "ref-1",
                    }
                ]
            }
        },
        marks={
            ("refinement", "ref-1"): {
                "mockup": {
                    "id": "na-mockup",
                    "justification": "Backend-only refinement.",
                }
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )
    projected = ResolvedResourceLineageProjection.project(resolved)

    assert projected["owner"]["entity_type"] == "spec"
    assert set(projected) >= {
        "unique_resources",
        "attachments",
        "counts",
        "provenance_labels",
        "coverage_obligations",
    }
    assert projected["counts"]["unique_resources_count"] == 2
    assert projected["counts"]["attachment_count"] == 4
    assert projected["counts"]["dedup_groups_count"] == 1
    assert projected["counts"]["not_applicable_count"] == 1

    unique_by_id = {
        item["unique_resource_id"]: item
        for item in projected["unique_resources"]
    }
    assert set(unique_by_id) == {"architecture:arch-origin", "knowledge_base:kb-1"}
    assert unique_by_id["architecture:arch-origin"]["attachment_count"] == 2
    assert unique_by_id["architecture:arch-origin"]["attachment_kinds"] == [
        "direct",
        "inherited_reference",
    ]

    attachment_kinds = {
        item["attachment_kind"]
        for item in projected["attachments"]
    }
    assert attachment_kinds == {"direct", "inherited_reference", "not_applicable"}
    for attachment in projected["attachments"]:
        assert "raw" not in attachment
        assert "content" not in attachment

    na_attachment = next(
        item for item in projected["attachments"]
        if item["attachment_kind"] == "not_applicable"
    )
    assert na_attachment["resource_type"] == "mockup"
    assert na_attachment["coverage_state"] == "not_applicable"
    assert na_attachment["source_entity_type"] == "refinement"
    assert na_attachment["effective"] is True
    assert na_attachment["inherited"] is True

    coverage_ids = {
        item["unique_resource_id"]
        for item in projected["coverage_obligations"]
    }
    assert coverage_ids == {"architecture:arch-origin", "knowledge_base:kb-1"}
    assert all(
        item["resource_type"] != "mockup"
        for item in projected["coverage_obligations"]
    )

    labels = projected["provenance_labels"]
    assert len(labels) == len(projected["attachments"])
    assert {
        label["attachment_kind"]
        for label in labels
    } == {"direct", "inherited_reference", "not_applicable"}
    assert {
        (label["resource_type"], label["attachment_kind"], label["source_entity_type"])
        for label in labels
    } >= {
        ("architecture", "direct", "spec"),
        ("architecture", "inherited_reference", "refinement"),
        ("knowledge_base", "direct", "spec"),
        ("mockup", "not_applicable", "refinement"),
    }
