from __future__ import annotations

import pytest

from okto_pulse.core.services.resource_lineage import (
    AmbiguousResourceOrigin,
    LineageEntityRef,
    METRIC_DEDUP_GROUPS_TOTAL,
    METRIC_RESOLUTION_FAILED_TOTAL,
    METRIC_RESOLVE_DURATION_MS,
    METRIC_RESOLVE_TOTAL,
    MetadataLineageCapabilityUnavailable,
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
        inherited_filter=None,
    ):
        self.roots = roots
        self.parents = parents or {}
        self.refs = refs or {}
        self.marks = marks or {}
        self.inherited_filter = inherited_filter

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

    async def filter_inherited_refs(
        self,
        root: LineageEntityRef,
        parent: LineageEntityRef,
        refs: dict[str, list[dict]],
    ) -> dict[str, list[dict]]:
        if self.inherited_filter is None:
            return refs
        return self.inherited_filter(root, parent, refs)

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
async def test_gate_profile_fails_closed_without_metadata_quartet() -> None:
    provider = FakeLineageProvider(
        roots={
            ("card", "card-1"): LineageEntityRef(
                "card",
                "card-1",
                "Card",
            )
        }
    )

    with pytest.raises(MetadataLineageCapabilityUnavailable) as exc_info:
        await ResolvedResourceLineageService(provider).resolve(
            "board-1",
            "card",
            "card-1",
            include_coverage=False,
            projection_profile="gate",
        )

    assert exc_info.value.code == "metadata_lineage_capability_unavailable"
    assert exc_info.value.details == {
        "projection_profile": "gate",
        "reason": "metadata_lineage_methods_missing",
        "required_methods": [
            "load_entity_ref_metadata",
            "load_parent_refs_metadata",
            "collect_refs_metadata",
            "filter_inherited_refs_metadata",
        ],
        "missing_methods": [
            "collect_refs_metadata",
            "filter_inherited_refs_metadata",
            "load_entity_ref_metadata",
            "load_parent_refs_metadata",
        ],
        "fallback_allowed": False,
    }


@pytest.mark.asyncio
async def test_gate_profile_uses_complete_metadata_capability_without_legacy_reads() -> None:
    card = LineageEntityRef("card", "card-1", "Card")
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    calls: list[str] = []

    class MetadataProvider(FakeLineageProvider):
        def supports_metadata_lineage(self) -> bool:
            return True

        async def load_entity_ref(self, *args, **kwargs):
            raise AssertionError("legacy entity loader reached by gate profile")

        async def load_parent_refs(self, *args, **kwargs):
            raise AssertionError("legacy parent loader reached by gate profile")

        async def collect_refs(self, *args, **kwargs):
            raise AssertionError("legacy body ref loader reached by gate profile")

        async def filter_inherited_refs(self, *args, **kwargs):
            raise AssertionError("legacy inherited ref filter reached by gate profile")

        async def load_entity_ref_metadata(self, board_id, entity_type, entity_id):
            del board_id
            calls.append("entity")
            return self.roots[(entity_type, entity_id)]

        async def load_parent_refs_metadata(self, board_id, root):
            del board_id
            calls.append("parents")
            return list(self.parents.get(root.ref, []))

        async def collect_refs_metadata(self, ref):
            calls.append(f"refs:{ref.ref}")
            empty = {"architecture": [], "mockup": [], "knowledge_base": []}
            return {**empty, **self.refs.get(ref.ref, {})}

        async def filter_inherited_refs_metadata(self, root, parent, refs):
            del root, parent
            calls.append("filter")
            return refs

    provider = MetadataProvider(
        roots={("card", "card-1"): card},
        parents={card.ref: [spec]},
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-metadata-only",
                        "title": "KB identity",
                        "root_source_kb_id": "kb-root",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "card",
        "card-1",
        include_coverage=False,
        projection_profile="gate",
    )

    knowledge_state = next(
        state
        for state in resolved.resource_states
        if state.resource_type == "knowledge_base"
    )
    assert knowledge_state.state == "provided"
    assert knowledge_state.inherited_count == 1
    assert calls == [
        "entity",
        "parents",
        "refs:card:card-1",
        "refs:spec:spec-1",
        "filter",
    ]


@pytest.mark.asyncio
async def test_v2_scope_keeps_suppressed_legacy_attachments_only_as_history() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "legacy-direct",
                        "title": "Legacy direct snapshot",
                        "root_source_kb_id": "kb-root-direct",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                        "origin_class": "legacy_all",
                        "effective": False,
                    }
                ]
            },
            refinement.ref: {
                "knowledge_base": [
                    {
                        "id": "legacy-inherited",
                        "title": "Legacy inherited snapshot",
                        "root_source_kb_id": "kb-root-inherited",
                        "source_entity_type": "refinement",
                        "source_entity_id": "ref-1",
                        "origin_class": "legacy_all",
                        "effective": False,
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
    projected = ResolvedResourceLineageProjection.project(resolved)
    knowledge_state = next(
        item
        for item in resolved.resource_states
        if item.resource_type == "knowledge_base"
    )

    assert knowledge_state.state == "missing"
    assert knowledge_state.blocking is True
    assert knowledge_state.direct_count == 0
    assert knowledge_state.inherited_count == 0
    assert knowledge_state.total_count == 0
    assert knowledge_state.direct_refs == ()
    assert knowledge_state.inherited_refs == ()
    assert resolved.unique_resources == ()
    assert resolved.coverage_obligations == ()

    assert len(resolved.attachments) == 2
    assert {item.resource_id for item in resolved.attachments} == {
        "legacy-direct",
        "legacy-inherited",
    }
    assert all(item.effective is False for item in resolved.attachments)
    assert {item.origin_class for item in resolved.attachments} == {"legacy_all"}

    counts = resolved.counts
    assert counts["attachment_count"] == 2
    assert counts["raw_attachment_count"] == 2
    assert counts["effective_attachment_count"] == 0
    assert counts["history_attachment_count"] == 2
    assert counts["unique_resources_count"] == 0
    assert counts["unique_effective_count"] == 0
    assert counts["direct_resources_count"] == 0
    assert counts["inherited_references_count"] == 0
    assert counts["covered_required_resources_count"] == 0
    assert counts["uncovered_required_resources_count"] == 0

    assert {item["origin_class"] for item in projected["attachments"]} == {
        "legacy_all"
    }
    assert all(item["effective"] is False for item in projected["attachments"])
    assert {
        item["origin_class"] for item in projected["provenance_labels"]
    } == {"legacy_all"}


@pytest.mark.asyncio
async def test_attachment_without_effective_flag_remains_effective_by_default() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "legacy-kb",
                        "title": "Pre-v2 attachment",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )
    knowledge_state = next(
        item
        for item in resolved.resource_states
        if item.resource_type == "knowledge_base"
    )

    assert resolved.attachments[0].effective is True
    assert knowledge_state.state == "provided"
    assert knowledge_state.direct_count == 1
    assert len(resolved.unique_resources) == 1
    assert len(resolved.coverage_obligations) == 1
    assert resolved.counts["effective_attachment_count"] == 1
    assert resolved.counts["history_attachment_count"] == 0
    assert resolved.counts["unique_effective_count"] == 1


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
async def test_direct_resource_shadows_only_the_same_inherited_root() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-a-local",
                        "title": "A local",
                        "root_source_kb_id": "root-a",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    }
                ]
            },
            refinement.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-a-parent",
                        "title": "A parent",
                        "root_source_kb_id": "root-a",
                        "source_entity_type": "refinement",
                        "source_entity_id": "ref-1",
                    },
                    {
                        "id": "kb-b-parent",
                        "title": "B parent",
                        "root_source_kb_id": "root-b",
                        "source_entity_type": "refinement",
                        "source_entity_id": "ref-1",
                    },
                ]
            },
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )

    assert {
        item.unique_resource_id for item in resolved.coverage_obligations
    } == {"knowledge_base:root-a", "knowledge_base:root-b"}
    by_root = {
        item.unique_resource_id: item
        for item in resolved.coverage_obligations
    }
    assert by_root["knowledge_base:root-a"].resource_id == "kb-a-local"
    assert by_root["knowledge_base:root-b"].resource_id == "kb-b-parent"


@pytest.mark.asyncio
async def test_provider_can_suppress_unselected_inherited_knowledge_refs() -> None:
    card = LineageEntityRef("card", "card-1", "Card")
    spec = LineageEntityRef("spec", "spec-1", "Spec")

    def filter_refs(_root, _parent, refs):
        return {
            **refs,
            "knowledge_base": [
                {**item, "effective": item["id"] == "kb-selected"}
                for item in refs["knowledge_base"]
            ],
        }

    provider = FakeLineageProvider(
        roots={("card", "card-1"): card},
        parents={card.ref: [spec]},
        inherited_filter=filter_refs,
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-selected",
                        "root_source_kb_id": "root-selected",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    },
                    {
                        "id": "kb-hidden",
                        "root_source_kb_id": "root-hidden",
                        "source_entity_type": "spec",
                        "source_entity_id": "spec-1",
                    },
                ]
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "card",
        "card-1",
    )
    knowledge_state = next(
        item
        for item in resolved.resource_states
        if item.resource_type == "knowledge_base"
    )

    assert [item["id"] for item in knowledge_state.inherited_refs] == [
        "kb-selected"
    ]
    assert resolved.counts["unique_effective_count"] == 1
    assert {
        item.resource_id: item.effective
        for item in resolved.attachments
        if item.resource_type == "knowledge_base"
    } == {"kb-selected": True, "kb-hidden": False}


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
async def test_resolver_allows_architecture_source_ref_hop_with_canonical_root() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "card-snapshot",
                        "source_ref": "architecture_design:refinement-snapshot",
                        "source_design_id": "ideation-root",
                    }
                ]
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1",
        "spec",
        "spec-1",
    )

    assert resolved.attachments[0].unique_resource_id == "architecture:ideation-root"
    assert resolved.unique_resources[0].unique_resource_id == "architecture:ideation-root"
    assert resolved.unique_resources[0].origin_evidence["source_ref"] == (
        "architecture_design:refinement-snapshot"
    )
    assert resolved.unique_resources[0].origin_evidence["source_design_id"] == "ideation-root"


@pytest.mark.asyncio
async def test_revision_stamp_uses_the_same_source_ref_fallback_as_dedup() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": "physical-snapshot",
                        "source_ref": "architecture_design:logical-root",
                        "source_version": 4,
                    }
                ]
            }
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1", "spec", "spec-1"
    )

    attachment = resolved.attachments[0]
    assert attachment.unique_resource_id == (
        "architecture:architecture_design:logical-root"
    )
    assert attachment.revision_stamp is not None
    assert attachment.revision_stamp.root_id == "architecture_design:logical-root"
    assert attachment.revision_stamp.source_revision == "4"


@pytest.mark.asyncio
async def test_multihop_mockup_and_kb_snapshots_dedupe_on_canonical_root() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    ideation = LineageEntityRef("ideation", "idea-1", "Ideation")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement, ideation]},
        refs={
            spec.ref: {
                "mockup": [{
                    "id": "mock-spec",
                    "origin_id": "mock-root",
                    "source_mockup_id": "mock-refinement",
                }],
                "knowledge_base": [{
                    "id": "kb-spec",
                    "source_kb_id": "kb-refinement",
                    "root_source_kb_id": "kb-root",
                    "immediate_parent_kb_id": "kb-refinement",
                }],
            },
            refinement.ref: {
                "mockup": [{
                    "id": "mock-refinement",
                    "origin_id": "mock-root",
                    "source_mockup_id": "mock-root",
                }],
                "knowledge_base": [{
                    "id": "kb-refinement",
                    "source_kb_id": "kb-root",
                    "root_source_kb_id": "kb-root",
                    "immediate_parent_kb_id": "kb-root",
                }],
            },
            ideation.ref: {
                "mockup": [{"id": "mock-root"}],
                "knowledge_base": [{"id": "kb-root"}],
            },
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1", "spec", "spec-1"
    )

    assert resolved.counts["attachment_count"] == 6
    assert resolved.counts["unique_resources_count"] == 2
    grouped = {
        item.unique_resource_id: item.attachment_count
        for item in resolved.unique_resources
    }
    assert grouped == {
        "mockup:mock-root": 3,
        "knowledge_base:kb-root": 3,
    }


@pytest.mark.asyncio
async def test_revision_v2_dedupes_one_root_and_preserves_divergent_stamps() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    refinement = LineageEntityRef("refinement", "ref-1", "Refinement")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        parents={spec.ref: [refinement]},
        refs={
            spec.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-spec",
                        "root_source_kb_id": "kb-root",
                        "immediate_parent_kb_id": "kb-refinement",
                        "source_revision": 2,
                        "source_content_sha256": "b" * 64,
                    }
                ]
            },
            refinement.ref: {
                "knowledge_base": [
                    {
                        "id": "kb-refinement",
                        "root_source_kb_id": "kb-root",
                        "immediate_parent_kb_id": "kb-root",
                        "source_version": 1,
                        "content_hash": "a" * 64,
                    }
                ]
            },
        },
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1", "spec", "spec-1"
    )

    assert resolved.counts["unique_resources_count"] == 1
    assert resolved.counts["attachment_count"] == 2
    unique = resolved.unique_resources[0]
    assert unique.unique_resource_id == "knowledge_base:kb-root"
    stamps_by_revision = {
        stamp.source_revision: stamp.to_dict() for stamp in unique.revision_stamps
    }
    assert stamps_by_revision == {
        "1": {
            "root_id": "kb-root",
            "immediate_parent_id": "kb-root",
            "source_revision": "1",
            "source_content_sha256": "a" * 64,
        },
        "2": {
            "root_id": "kb-root",
            "immediate_parent_id": "kb-refinement",
            "source_revision": "2",
            "source_content_sha256": "b" * 64,
        },
    }

    unique_payload = unique.to_dict()
    assert unique_payload["contract_version"] == 2
    assert unique_payload["root_id"] == "kb-root"
    assert unique_payload["source_revision"] is None
    assert unique_payload["source_content_sha256"] is None
    assert len(unique_payload["revision_stamps"]) == 2

    attachment_payload = resolved.attachments[0].to_dict()
    assert attachment_payload["root_id"] == "kb-root"
    assert attachment_payload["immediate_parent_id"] == "kb-refinement"
    assert attachment_payload["source_revision"] == "2"
    assert attachment_payload["source_content_sha256"] == "b" * 64
    assert attachment_payload["revision_stamp"] == {
        "root_id": "kb-root",
        "immediate_parent_id": "kb-refinement",
        "source_revision": "2",
        "source_content_sha256": "b" * 64,
    }

    coverage = resolved.coverage_obligations[0].to_dict()
    assert coverage["revision_stamp"] == attachment_payload["revision_stamp"]
    assert coverage["source_content_sha256"] == "b" * 64

    legacy_projection = resolved.to_dict()
    public_projection = ResolvedResourceLineageProjection.project(resolved)
    assert legacy_projection["contract_version"] == 2
    assert public_projection["contract_version"] == 2
    assert public_projection["attachments"][0]["revision_stamp"] == (
        attachment_payload["revision_stamp"]
    )
    assert "raw" not in public_projection["attachments"][0]


@pytest.mark.asyncio
async def test_revision_v2_keeps_legacy_null_evidence_readable() -> None:
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={spec.ref: {"knowledge_base": [{"id": "legacy-kb"}]}},
    )

    resolved = await ResolvedResourceLineageService(provider).resolve(
        "board-1", "spec", "spec-1"
    )

    attachment = resolved.attachments[0].to_dict()
    assert attachment["root_id"] == "legacy-kb"
    assert attachment["immediate_parent_id"] is None
    assert attachment["source_revision"] is None
    assert attachment["source_content_sha256"] is None
    assert attachment["revision_stamp"] == {
        "root_id": "legacy-kb",
        "immediate_parent_id": None,
        "source_revision": None,
        "source_content_sha256": None,
    }
    assert resolved.unique_resources[0].to_dict()["revision_stamps"] == [
        attachment["revision_stamp"]
    ]


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
                        "source_design_id": "arch-origin-a",
                        "origin_id": "arch-origin-b",
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
async def test_resolver_snapshot_id_with_hop_ref_and_root_design_id_no_ambiguity() -> None:
    # GOV 5c43a364 regression — the exact server-blocking shape observed while
    # validating card 23866493: a card snapshot carrying its OWN ``id``, an
    # INTERMEDIATE ``source_ref`` hop, and the canonical ROOT ``source_design_id``.
    # The own ``id`` must NOT be conflated with the canonical origin and the
    # divergent ``source_ref`` is a provenance hop, so there is NO
    # AmbiguousResourceOrigin and the resource keys on the ROOT design id.
    spec = LineageEntityRef("spec", "spec-1", "Spec")
    snapshot_id = "67f04914-bac7-433c-8209-cf81188d1122"
    intermediate = "cead91e9-3b29-4586-8311-588ad4d948fd"
    root_design_id = "345a132b-28e6-4dc0-81fa-ef59cb22a9ac"
    provider = FakeLineageProvider(
        roots={("spec", "spec-1"): spec},
        refs={
            spec.ref: {
                "architecture": [
                    {
                        "id": snapshot_id,
                        "source_ref": f"architecture_design:{intermediate}",
                        "source_design_id": root_design_id,
                    }
                ]
            }
        },
    )

    # Resolving must NOT raise AmbiguousResourceOrigin for this legitimate shape.
    resolved = await ResolvedResourceLineageService(provider).resolve("board-1", "spec", "spec-1")

    assert resolved.attachments[0].unique_resource_id == f"architecture:{root_design_id}"
    assert resolved.unique_resources[0].unique_resource_id == f"architecture:{root_design_id}"
    # the own id is preserved as the resource id (provenance), never the origin
    assert resolved.attachments[0].resource_id == snapshot_id
    evidence = resolved.unique_resources[0].origin_evidence
    assert evidence["source_design_id"] == root_design_id
    assert evidence["id"] == snapshot_id
    assert evidence["source_ref"] == f"architecture_design:{intermediate}"


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
