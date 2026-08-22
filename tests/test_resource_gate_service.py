from __future__ import annotations

import inspect
import uuid

import pytest

from okto_pulse.core.services import architecture as architecture_module
from okto_pulse.core.services import resource_gate as resource_gate_module
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationKnowledgeBase,
    IdeationStatus,
    Refinement,
    ResourceNotApplicable,
    Spec,
    SpecKnowledgeBase,
)
from okto_pulse.core.models.schemas import IdeationMove
from okto_pulse.core.services.main import IdeationService
from okto_pulse.core.services.architecture import (
    ArchitectureFindingRunStore,
    ARCHITECTURE_FINDING_ACTIVE,
    ARCHITECTURE_FINDING_RESOLVED,
)
from okto_pulse.core.services.architecture_observability import (
    METRIC_DONE_BLOCKER_TOTAL,
    METRIC_GATE_EVAL_DURATION_MS,
    METRIC_PROJECTION_TOTAL,
    assert_architecture_metric_payload_is_safe,
    get_architecture_metric_samples,
    reset_architecture_observability_for_tests,
)
from okto_pulse.core.services.resource_gate import (
    ResourceGateJustificationRequired,
    ResourceGateService,
    ResourceGateViolation,
)
from okto_pulse.core.services.resource_lineage import (
    CoverageObligation,
    METRIC_COVERAGE_UNCOVERED_TOTAL,
    LineageEntityRef,
    ResolvedResourceLineage,
    ResourceAttachment,
    ResourceStateEnvelope,
    UniqueResource,
    get_resource_lineage_metric_samples,
    reset_resource_lineage_observability_for_tests,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _lineage_with_inherited_architecture(
    *,
    metadata: dict | None = None,
) -> ResolvedResourceLineage:
    architecture_ref: dict = {
        "id": "arch-source-1",
        "title": "Inherited architecture",
        "source_entity_type": "spec",
        "source_entity_id": "spec-1",
        "source_entity_title": "Source spec",
    }
    architecture_ref.update(metadata or {})
    return ResolvedResourceLineage(
        owner=LineageEntityRef("card", "card-1", "Card"),
        unique_resources=(),
        attachments=(),
        counts={"attachment_count": 1},
        resource_states=(
            ResourceStateEnvelope(
                resource_type="architecture",
                state="provided",
                direct_count=0,
                inherited_count=1,
                total_count=1,
                direct_refs=(),
                inherited_refs=(architecture_ref,),
                na_mark=None,
                blocking=False,
            ),
            ResourceStateEnvelope(
                resource_type="mockup",
                state="missing",
                direct_count=0,
                inherited_count=0,
                total_count=0,
                direct_refs=(),
                inherited_refs=(),
                na_mark=None,
                blocking=True,
            ),
            ResourceStateEnvelope(
                resource_type="knowledge_base",
                state="missing",
                direct_count=0,
                inherited_count=0,
                total_count=0,
                direct_refs=(),
                inherited_refs=(),
                na_mark=None,
                blocking=True,
            ),
        ),
    )


def test_resource_gate_coverage_path_has_static_lineage_drift_guard():
    source = inspect.getsource(ResourceGateService.validate_spec_resource_task_coverage)

    assert "lineage.coverage_obligations" in source
    assert "_coverage_obligation_refs" not in source
    assert 'summary["resources"]' not in source


@pytest.mark.asyncio
async def test_completion_fails_closed_on_architecture_propagation_block_without_findings(monkeypatch):
    service = ResourceGateService(db=None)
    summary = {
        "resources": [
            {"resource_type": "architecture", "state": "provided"},
            {"resource_type": "mockup", "state": "not_applicable"},
            {"resource_type": "knowledge_base", "state": "not_applicable"},
        ],
        "architecture_findings": {
            "active_count": 0,
            "design_count": 1,
            "top_remediation": [],
        },
        "architecture_propagation": {
            "blocking": True,
            "ineligible_sources": [{"code": "architecture_propagation_blocked"}],
            "remediation": "Fix the source design and rerun the architecture critic.",
        },
        "architecture_propagation_blocking": True,
    }

    async def fake_summary(*_args, **_kwargs):
        return summary

    monkeypatch.setattr(service, "get_summary", fake_summary)

    result = await service.validate_entity_completion("board-1", "card", "card-1")

    assert result["allowed"] is False
    assert result["blocking_resources"] == []
    assert result["blocking_architecture_findings"] == []
    assert result["blocking_architecture_propagation"]["blocking"] is True

    with pytest.raises(ResourceGateViolation) as exc_info:
        await service.validate_or_raise_entity_completion("board-1", "card", "card-1")

    assert exc_info.value.code == "architecture_propagation_blocked"
    assert exc_info.value.details["architecture_propagation"]["blocking"] is True


@pytest.mark.asyncio
async def test_metadata_summary_never_loads_or_critiques_architecture(
    monkeypatch,
) -> None:
    lineage = _lineage_with_inherited_architecture()
    resolver_calls: list[dict] = []
    forbidden_calls: list[str] = []

    async def fake_resolve(_self, *_args, **kwargs):
        resolver_calls.append(kwargs)
        return lineage

    async def forbidden_findings(*_args, **_kwargs):
        forbidden_calls.append("architecture_finding_gate")
        raise AssertionError("metadata summary listed architecture findings")

    async def forbidden_policy(*_args, **_kwargs):
        forbidden_calls.append("propagation_policy")
        raise AssertionError("metadata summary evaluated propagation policy")

    async def forbidden_design_load(*_args, **_kwargs):
        forbidden_calls.append("design_load")
        raise AssertionError("metadata summary loaded an architecture design")

    def forbidden_critic(*_args, **_kwargs):
        forbidden_calls.append("critic")
        raise AssertionError("metadata summary ran the architecture critic")

    monkeypatch.setattr(ResourceGateService, "_resolve_resource_lineage", fake_resolve)
    monkeypatch.setattr(
        resource_gate_module.ArchitectureFindingGate,
        "evaluate",
        forbidden_findings,
    )
    monkeypatch.setattr(
        resource_gate_module.ArchitecturePropagationEligibilityPolicy,
        "evaluate",
        forbidden_policy,
    )
    monkeypatch.setattr(
        architecture_module.ArchitectureDesignRepository,
        "get",
        forbidden_design_load,
    )
    monkeypatch.setattr(
        architecture_module.ArchitectureDesignRepository,
        "critique_payload",
        forbidden_critic,
    )

    summary = await ResourceGateService(db=None).get_summary(
        "board-1",
        "card",
        "card-1",
        metadata_only=True,
    )

    assert resolver_calls == [
        {"include_coverage": False, "projection_profile": "gate"}
    ]
    assert forbidden_calls == []
    propagation = summary["architecture_propagation"]
    assert propagation["blocking"] is True
    assert propagation["evaluation_mode"] == "metadata_only"
    assert propagation["decision"] == "fail_closed"
    assert (
        propagation["reason_code"]
        == "architecture_propagation_metadata_unavailable"
    )
    assert propagation["ineligible_sources"] == [
        {
            "code": "architecture_propagation_metadata_unavailable",
            "eligible": False,
            "eligibility_state": "unknown",
            "design_id": "arch-source-1",
            "design_version": None,
            "source_design_id": None,
            "source_ref": None,
            "source_entity_type": "spec",
            "source_entity_id": "spec-1",
            "current_finding_run": None,
            "verdict_status": "unavailable",
            "revalidation_reason": "current_finding_run_missing",
        }
    ]
    assert propagation["drilldown"] == {
        "rel": "read_full_resource_gate_summary",
        "tool": "okto_pulse_get_resource_gate_summary",
        "arguments": {
            "board_id": "board-1",
            "entity_type": "card",
            "entity_id": "card-1",
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run_override", "expected_blocking", "expected_reason"),
    [
        ({}, False, None),
        ({"design_version": 6}, True, "design_version_incompatible"),
    ],
)
async def test_metadata_summary_uses_only_current_persisted_finding_run(
    monkeypatch,
    run_override,
    expected_blocking,
    expected_reason,
) -> None:
    current_run = {
        "critic_run_id": "critic-run-1",
        "design_version": 7,
        "is_current": True,
        "active_count": 0,
        "resolved_count": 2,
        "superseded_count": 1,
        "validator_valid": True,
        "validator_issue_count": 0,
        **run_override,
    }
    lineage = _lineage_with_inherited_architecture(
        metadata={
            "design_version": 7,
            "current_finding_run": current_run,
        }
    )
    forbidden_calls: list[str] = []

    async def fake_resolve(_self, *_args, **_kwargs):
        return lineage

    async def forbidden_findings(*_args, **_kwargs):
        forbidden_calls.append("architecture_finding_gate")
        raise AssertionError("metadata summary listed architecture findings")

    async def forbidden_policy(*_args, **_kwargs):
        forbidden_calls.append("propagation_policy")
        raise AssertionError("metadata summary reached the full propagation policy")

    monkeypatch.setattr(ResourceGateService, "_resolve_resource_lineage", fake_resolve)
    monkeypatch.setattr(
        resource_gate_module.ArchitectureFindingGate,
        "evaluate",
        forbidden_findings,
    )
    monkeypatch.setattr(
        resource_gate_module.ArchitecturePropagationEligibilityPolicy,
        "evaluate",
        forbidden_policy,
    )

    summary = await ResourceGateService(db=None).get_summary(
        "board-1",
        "card",
        "card-1",
        metadata_only=True,
    )

    assert forbidden_calls == []
    propagation = summary["architecture_propagation"]
    assert propagation["blocking"] is expected_blocking
    if expected_reason is None:
        assert propagation["decision"] == "eligible"
        assert propagation["ineligible_sources"] == []
        assert propagation["drilldown"] is None
    else:
        assert propagation["decision"] == "fail_closed"
        assert propagation["ineligible_sources"][0]["revalidation_reason"] == (
            expected_reason
        )


@pytest.mark.asyncio
async def test_full_summary_keeps_canonical_architecture_propagation_policy(
    monkeypatch,
) -> None:
    lineage = _lineage_with_inherited_architecture()
    policy_calls: list[str] = []

    async def fake_resolve(_self, *_args, **_kwargs):
        return lineage

    async def fake_findings(_self, **_kwargs):
        return {
            "architecture_findings": {
                "active_count": 0,
                "design_count": 1,
                "top_remediation": [],
            }
        }

    class Eligible:
        eligible = True

    async def fake_policy(_self, design_id):
        policy_calls.append(design_id)
        return Eligible()

    monkeypatch.setattr(ResourceGateService, "_resolve_resource_lineage", fake_resolve)
    monkeypatch.setattr(
        resource_gate_module.ArchitectureFindingGate,
        "evaluate",
        fake_findings,
    )
    monkeypatch.setattr(
        resource_gate_module.ArchitecturePropagationEligibilityPolicy,
        "evaluate",
        fake_policy,
    )

    summary = await ResourceGateService(db=None).get_summary(
        "board-1",
        "card",
        "card-1",
    )

    assert policy_calls == ["arch-source-1"]
    assert summary["architecture_propagation"] == {
        "blocking": False,
        "ineligible_sources": [],
        "remediation": None,
    }


@pytest.mark.asyncio
async def test_effective_resource_marks_direct_card_snapshots_read_only(monkeypatch):
    service = ResourceGateService(db=None)

    async def fake_hydrate(*_args, **_kwargs):
        return {"id": "kb-card-snapshot", "title": "Card KB snapshot"}

    monkeypatch.setattr(service, "_hydrate_effective_resource", fake_hydrate)

    item = await service._effective_resource_item(
        board_id="board-1",
        resource_type="knowledge_base",
        ref={
            "id": "kb-card-snapshot",
            "title": "Card KB snapshot",
            "source_entity_type": "card",
            "source_entity_id": "card-1",
        },
        attachment_kind="direct",
        inherited=False,
    )

    assert item["inherited"] is False
    assert item["read_only"] is True


@pytest.mark.asyncio
async def test_resource_gate_resolves_direct_inherited_and_na_precedence(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")
    refinement_id = _id("ref")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea with resources",
                created_by=actor_id,
                screen_mockups=[{"id": "mock-1", "title": "Primary flow"}],
            )
        )
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Idea architecture",
                global_description="Architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor_id,
            )
        )
        db.add(
            IdeationKnowledgeBase(
                ideation_id=ideation_id,
                title="Idea KB",
                content="Knowledge",
                created_by=actor_id,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Refinement inherits resources",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        await service.mark_not_applicable(
            board_id,
            "refinement",
            refinement_id,
            "mockup",
            actor_id,
            justification="Mockup initially considered unnecessary",
            source_channel="ui",
        )

        summary = await service.get_summary(board_id, "refinement", refinement_id)

        by_type = {item["resource_type"]: item for item in summary["resources"]}
        assert summary["blocking"] is False
        assert by_type["architecture"]["state"] == "provided"
        assert by_type["architecture"]["direct_count"] == 0
        assert by_type["architecture"]["inherited_count"] == 1
        assert by_type["mockup"]["state"] == "provided"
        assert by_type["mockup"]["na_mark"]["active"] is True
        assert by_type["mockup"]["na_mark"]["effective"] is False
        assert by_type["knowledge_base"]["state"] == "provided"
        assert by_type["knowledge_base"]["inherited_refs"][0]["source_entity_type"] == "ideation"
        assert summary["lineage_counts"]["attachment_count"] >= 3
        assert summary["resource_lineage"]["owner"]["entity_type"] == "refinement"
        assert summary["resource_lineage"]["resource_states"]


@pytest.mark.asyncio
async def test_effective_resources_hydrate_inherited_payloads_with_provenance(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    architecture_id = _id("arch")
    kb_id = _id("kb")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Effective resources", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea source",
                created_by=actor_id,
                screen_mockups=[
                    {
                        "id": "mock-source-1",
                        "title": "Source screen",
                        "description": "Parent mockup",
                        "screen_type": "page",
                        "html_content": "<main>source</main>",
                        "annotations": [],
                        "order": 0,
                    }
                ],
            )
        )
        architecture = ArchitectureDesign(
            id=architecture_id,
            board_id=board_id,
            parent_type="ideation",
            ideation_id=ideation_id,
            title="Source architecture",
            global_description="Parent architecture payload",
            entities=[{"id": "api", "name": "API", "entity_type": "service"}],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        kb = IdeationKnowledgeBase(
            id=kb_id,
            ideation_id=ideation_id,
            title="Source KB",
            content="Parent knowledge payload",
            created_by=actor_id,
        )
        db.add(architecture)
        db.add(kb)
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Refinement inherits",
                created_by=actor_id,
            )
        )
        await db.commit()

        result = await ResourceGateService(db).get_effective_resources(
            board_id,
            "refinement",
            refinement_id,
        )

    architecture_item = result["resources"]["architecture"][0]
    assert architecture_item["id"] == architecture_id
    assert architecture_item["inherited"] is True
    assert architecture_item["read_only"] is True
    assert architecture_item["source_entity_type"] == "ideation"
    assert architecture_item["source_entity_id"] == ideation_id
    assert architecture_item["resource"]["global_description"] == (
        "Parent architecture payload"
    )
    assert architecture_item["resource"]["entities"][0]["id"] == "api"

    mockup_item = result["resources"]["mockup"][0]
    assert mockup_item["id"] == "mock-source-1"
    assert mockup_item["html_content"] == "<main>source</main>"
    assert mockup_item["resource"]["html_content"] == "<main>source</main>"
    assert mockup_item["provenance"]["source_entity_title"] == "Idea source"

    kb_item = result["resources"]["knowledge_base"][0]
    assert kb_item["id"] == kb_id
    assert kb_item["content"] == "Parent knowledge payload"
    assert kb_item["source_entity_type"] == "ideation"


@pytest.mark.asyncio
async def test_effective_resources_returns_one_representative_per_logical_root(
    monkeypatch,
) -> None:
    owner = LineageEntityRef("spec", "spec-1", "Spec")

    def attachment(
        resource_id: str,
        unique_id: str,
        kind: str,
    ) -> ResourceAttachment:
        return ResourceAttachment(
            resource_type="knowledge_base",
            resource_id=resource_id,
            title=resource_id,
            unique_resource_id=unique_id,
            attachment_kind=kind,
            source_entity_type="spec" if kind == "direct" else "refinement",
            source_entity_id="spec-1" if kind == "direct" else "ref-1",
            source_entity_title=None,
            coverage_state="not_required",
            effective=True,
            inherited=kind != "direct",
            raw={"id": resource_id, "title": resource_id},
        )

    lineage = ResolvedResourceLineage(
        owner=owner,
        unique_resources=(),
        attachments=(
            attachment("root-a-local", "knowledge_base:root-a", "direct"),
            attachment("root-a-parent", "knowledge_base:root-a", "inherited_reference"),
            attachment("root-b-parent", "knowledge_base:root-b", "inherited_reference"),
        ),
        counts={"unique_effective_count": 2},
        resource_states=(),
    )

    async def resolve(_self, *_args, **_kwargs):
        return lineage

    async def hydrate(_self, **request):
        return {"id": request["ref"]["id"]}

    monkeypatch.setattr(ResourceGateService, "_resolve_resource_lineage", resolve)
    monkeypatch.setattr(ResourceGateService, "_effective_resource_item", hydrate)

    result = await ResourceGateService(object()).get_effective_resources(
        "board-1",
        "spec",
        "spec-1",
    )

    assert [item["id"] for item in result["resources"]["knowledge_base"]] == [
        "root-a-local",
        "root-b-parent",
    ]


@pytest.mark.asyncio
async def test_resource_gate_summary_delegates_to_resolver_projection(
    db_factory,
    monkeypatch,
):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")
    calls: list[tuple[tuple, dict]] = []
    real_resolver = resource_gate_module.ResolvedResourceLineageService

    class SpyResolvedResourceLineageService(real_resolver):
        async def resolve(self, *args, **kwargs):
            calls.append((args, kwargs))
            return await super().resolve(*args, **kwargs)

    monkeypatch.setattr(
        resource_gate_module,
        "ResolvedResourceLineageService",
        SpyResolvedResourceLineageService,
    )

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea delegated through resolver",
                created_by=actor_id,
            )
        )
        await db.commit()

        summary = await ResourceGateService(db).get_summary(
            board_id,
            "ideation",
            ideation_id,
        )

    assert calls
    args, kwargs = calls[0]
    assert args[:3] == (board_id, "ideation", ideation_id)
    assert kwargs["include_coverage"] is False
    assert kwargs["projection_profile"] == "summary"
    assert {item["resource_type"] for item in summary["resources"]} == {
        "architecture",
        "mockup",
        "knowledge_base",
    }
    assert {
        item["resource_type"] for item in summary["missing_resources"]
    } == {"architecture", "mockup"}
    assert [
        item["resource_type"] for item in summary["advisory_missing_resources"]
    ] == ["knowledge_base"]
    assert summary["resource_lineage"]["owner"]["entity_id"] == ideation_id
    assert summary["lineage_counts"] == summary["resource_lineage"]["counts"]


@pytest.mark.asyncio
async def test_resource_gate_summary_uses_resolver_payload_not_local_recompute(
    db_factory,
    monkeypatch,
):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    calls: list[tuple[tuple, dict]] = []

    class StubResolvedResourceLineageService:
        def __init__(self, provider):
            self.provider = provider

        async def resolve(self, *args, **kwargs):
            calls.append((args, kwargs))
            owner = LineageEntityRef("spec", spec_id, "Spec without stored resources")
            direct_ref = {
                "id": "stub-mock",
                "title": "Synthetic mockup from resolver",
                "source_entity_type": "resolver",
                "source_entity_id": "synthetic",
                "source_entity_title": "Synthetic lineage",
            }
            return ResolvedResourceLineage(
                owner=owner,
                unique_resources=(
                    UniqueResource(
                        resource_type="mockup",
                        unique_resource_id="mockup:stub-mock",
                        representative_resource_id="stub-mock",
                        title="Synthetic mockup from resolver",
                        origin_evidence={"id": "stub-mock"},
                        attachment_count=1,
                        attachment_kinds=("direct",),
                    ),
                ),
                attachments=(
                    ResourceAttachment(
                        resource_type="mockup",
                        resource_id="stub-mock",
                        title="Synthetic mockup from resolver",
                        unique_resource_id="mockup:stub-mock",
                        attachment_kind="direct",
                        source_entity_type="resolver",
                        source_entity_id="synthetic",
                        source_entity_title="Synthetic lineage",
                        coverage_state="not_required",
                        origin_evidence={"id": "stub-mock"},
                    ),
                ),
                counts={
                    "unique_resources_count": 1,
                    "attachment_count": 1,
                    "by_unique_resource": [
                        {
                            "unique_resource_id": "mockup:stub-mock",
                            "attachment_count": 1,
                        }
                    ],
                },
                resource_states=(
                    ResourceStateEnvelope(
                        resource_type="architecture",
                        state="missing",
                        direct_count=0,
                        inherited_count=0,
                        total_count=0,
                        direct_refs=(),
                        inherited_refs=(),
                        na_mark=None,
                        blocking=True,
                    ),
                    ResourceStateEnvelope(
                        resource_type="mockup",
                        state="provided",
                        direct_count=1,
                        inherited_count=0,
                        total_count=1,
                        direct_refs=(direct_ref,),
                        inherited_refs=(),
                        na_mark=None,
                        blocking=False,
                    ),
                    ResourceStateEnvelope(
                        resource_type="knowledge_base",
                        state="missing",
                        direct_count=0,
                        inherited_count=0,
                        total_count=0,
                        direct_refs=(),
                        inherited_refs=(),
                        na_mark=None,
                        blocking=True,
                    ),
                ),
                coverage_obligations=(
                    CoverageObligation(
                        resource_type="mockup",
                        resource_id="stub-mock",
                        unique_resource_id="mockup:stub-mock",
                        title="Synthetic mockup from resolver",
                        source_entity_type="resolver",
                        source_entity_id="synthetic",
                        source_entity_title="Synthetic lineage",
                        origin_evidence={"id": "stub-mock"},
                    ),
                ),
            )

    monkeypatch.setattr(
        resource_gate_module,
        "ResolvedResourceLineageService",
        StubResolvedResourceLineageService,
    )

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec without stored resources",
                created_by=actor_id,
            )
        )
        await db.commit()

        summary = await ResourceGateService(db).get_summary(board_id, "spec", spec_id)

    assert calls
    args, kwargs = calls[0]
    assert args[:3] == (board_id, "spec", spec_id)
    assert kwargs["include_coverage"] is False
    assert kwargs["projection_profile"] == "summary"
    by_type = {item["resource_type"]: item for item in summary["resources"]}
    assert by_type["mockup"]["state"] == "provided"
    assert by_type["mockup"]["direct_refs"] == [
        {
            "id": "stub-mock",
            "title": "Synthetic mockup from resolver",
            "source_entity_type": "resolver",
            "source_entity_id": "synthetic",
            "source_entity_title": "Synthetic lineage",
        }
    ]
    assert summary["resource_lineage"]["unique_resources"][0]["unique_resource_id"] == (
        "mockup:stub-mock"
    )


@pytest.mark.asyncio
async def test_resource_gate_requires_justification_for_mcp_and_returns_warning(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea without resources",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        with pytest.raises(ResourceGateJustificationRequired):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                "architecture",
                actor_id,
                source_channel="mcp",
            )

        result = await service.mark_not_applicable(
            board_id,
            "ideation",
            ideation_id,
            "architecture",
            actor_id,
            justification="Architecture is not needed for this small text-only change.",
            source_channel="mcp",
        )

        by_type = {
            item["resource_type"]: item
            for item in result["summary"]["resources"]
        }
        assert result["warning"]
        assert by_type["architecture"]["state"] == "not_applicable"
        assert by_type["architecture"]["na_mark"]["effective"] is True
        assert by_type["mockup"]["state"] == "missing"


@pytest.mark.asyncio
async def test_resource_gate_clear_na_reveals_missing_state(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec without KB",
                created_by=actor_id,
            )
        )
        await db.commit()

        service = ResourceGateService(db)
        await service.mark_not_applicable(
            board_id,
            "spec",
            spec_id,
            "knowledge_base",
            actor_id,
            source_channel="ui",
        )
        cleared = await service.clear_not_applicable(
            board_id,
            "spec",
            spec_id,
            "knowledge_base",
            actor_id,
            reason="KB is applicable after all",
        )

        by_type = {item["resource_type"]: item for item in cleared["summary"]["resources"]}
        assert cleared["cleared"] == 1
        assert by_type["knowledge_base"]["state"] == "missing"
        assert by_type["knowledge_base"]["na_mark"] is None

        rows = (
            await db.execute(
                ResourceNotApplicable.__table__.select().where(
                    ResourceNotApplicable.board_id == board_id,
                    ResourceNotApplicable.entity_type == "spec",
                    ResourceNotApplicable.entity_id == spec_id,
                )
            )
        ).all()
        assert len(rows) == 1
        assert rows[0]._mapping["active"] is False


@pytest.mark.asyncio
async def test_resource_gate_validates_spec_resources_are_covered_by_non_cancelled_tasks(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with resources",
            created_by=actor_id,
            screen_mockups=[{"id": "mock-1", "title": "Primary flow"}],
        )
        task = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implement resource-aware flow",
            created_by=actor_id,
            card_type=CardType.NORMAL,
            status=CardStatus.NOT_STARTED,
        )
        kb = SpecKnowledgeBase(
            spec_id=spec_id,
            title="Reference notes",
            content="Operational reference",
            created_by=actor_id,
        )
        db.add(spec)
        db.add(task)
        db.add(kb)
        await db.flush()
        architecture = ArchitectureDesign(
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Architecture context",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        db.add(architecture)
        await db.flush()

        service = ResourceGateService(db)
        reset_resource_lineage_observability_for_tests()
        uncovered = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert uncovered["allowed"] is False
        assert uncovered["required_resources"] == uncovered["summary"]["resource_lineage"]["coverage_obligations"]
        assert {
            item["unique_resource_id"] for item in uncovered["required_resources"]
        } == {
            f"architecture:{architecture.id}",
            "mockup:mock-1",
        }
        assert {item["resource_type"] for item in uncovered["uncovered_resources"]} == {
            "architecture",
            "mockup",
        }
        assert [
            item["unique_resource_id"]
            for item in uncovered["advisory_coverage_resources"]
        ] == [f"knowledge_base:{kb.id}"]
        uncovered_by_type = {
            item["resource_type"]: item for item in uncovered["uncovered_resources"]
        }
        expected_uncovered = {
            "architecture": f"architecture:{architecture.id}",
            "mockup": "mockup:mock-1",
        }
        for resource_type, unique_resource_id in expected_uncovered.items():
            item = uncovered_by_type[resource_type]
            assert item["unique_resource_id"] == unique_resource_id
            assert item["resource_id"] in {architecture.id, "mock-1", kb.id}
            assert item["source_entity_type"] == "spec"
            assert item["source_entity_id"] == spec_id
            assert item["origin_evidence"]["id"] in {architecture.id, "mock-1", kb.id}
            assert item["reason"] == "uncovered"
            assert item["remediation"] == (
                "Attach or copy this resource directly to at least one non-cancelled task."
            )
        uncovered_metric_samples = [
            item for item in get_resource_lineage_metric_samples()
            if item["metric_name"] == METRIC_COVERAGE_UNCOVERED_TOTAL
        ]
        assert len(uncovered_metric_samples) == 2
        assert {
            item["labels"]["resource_type"] for item in uncovered_metric_samples
        } == {"architecture", "mockup"}

        task.screen_mockups = [{"id": "card-mock-1", "origin_id": "mock-1"}]
        task.knowledge_bases = [{"id": "card-kb-1", "source_kb_id": kb.id}]
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title="Task architecture",
                global_description="Task architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                source_design_id=architecture.id,
                created_by=actor_id,
            )
        )
        await db.flush()

        covered = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert covered["allowed"] is True

        task.status = CardStatus.CANCELLED
        await db.flush()

        cancelled_only = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert cancelled_only["allowed"] is False
        assert cancelled_only["required_resources"] == uncovered["required_resources"]
        cancelled_by_type = {
            item["resource_type"]: item for item in cancelled_only["uncovered_resources"]
        }
        assert set(cancelled_by_type) == {"architecture", "mockup"}
        for item in cancelled_by_type.values():
            assert item["reason"] == "covered_only_by_cancelled_task"
            assert item["remediation"] == (
                "Attach or copy this resource to at least one non-cancelled task. "
                "Cancelled tasks do not count as coverage."
            )
            assert item["unique_resource_id"] in expected_uncovered.values()


@pytest.mark.asyncio
async def test_resource_gate_not_applicable_rows_are_not_coverage_obligations(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate N/A obligations", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec with N/A resources only",
                created_by=actor_id,
            )
        )
        await db.flush()

        service = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await service.mark_not_applicable(
                board_id,
                "spec",
                spec_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is intentionally out of scope.",
                source_channel="ui",
            )

        result = await service.validate_spec_resource_task_coverage(board_id, spec_id)

    assert result["allowed"] is True
    assert result["required_resources"] == []
    assert result["uncovered_resources"] == []
    states = {
        item["resource_type"]: item["state"] for item in result["summary"]["resources"]
    }
    assert states == {
        "architecture": "not_applicable",
        "mockup": "not_applicable",
        "knowledge_base": "not_applicable",
    }
    attachments = result["summary"]["resource_lineage"]["attachments"]
    assert {
        (item["resource_type"], item["attachment_kind"], item["coverage_state"])
        for item in attachments
    } == {
        ("architecture", "not_applicable", "not_applicable"),
        ("mockup", "not_applicable", "not_applicable"),
        ("knowledge_base", "not_applicable", "not_applicable"),
    }


@pytest.mark.asyncio
async def test_resource_gate_copied_card_kb_source_covers_spec_kb_by_origin(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate KB origin", owner_id=actor_id))
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with copied KB coverage",
            created_by=actor_id,
        )
        task = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation task with copied KB source",
            created_by=actor_id,
            card_type=CardType.NORMAL,
            status=CardStatus.IN_PROGRESS,
            knowledge_bases=[
                {
                    "id": "card-kb-copy",
                    "title": "Copied operational reference",
                    "source": f"copied_from_spec:{spec_id}:spec-kb-origin",
                }
            ],
        )
        kb = SpecKnowledgeBase(
            id="spec-kb-origin",
            spec_id=spec_id,
            title="Spec operational reference",
            content="Origin KB",
            created_by=actor_id,
        )
        db.add(spec)
        db.add(task)
        db.add(kb)
        await db.flush()

        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            board_id,
            spec_id,
        )

    assert coverage["allowed"] is True
    assert coverage["uncovered_resources"] == []
    assert coverage["required_resources"] == []
    advisory = coverage["advisory_coverage_resources"]
    assert [item["unique_resource_id"] for item in advisory] == [
        "knowledge_base:spec-kb-origin"
    ]
    assert (
        advisory
        == coverage["summary"]["resource_lineage"][
            "advisory_coverage_resources"
        ]
    )


@pytest.mark.asyncio
async def test_resource_gate_coverage_delegates_to_resolver_obligations(
    db_factory,
    monkeypatch,
):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    calls: list[tuple[tuple, dict]] = []
    real_resolver = resource_gate_module.ResolvedResourceLineageService

    class SpyResolvedResourceLineageService(real_resolver):
        async def resolve(self, *args, **kwargs):
            calls.append((args, kwargs))
            return await super().resolve(*args, **kwargs)

    def fail_if_legacy_summary_obligations_are_used(_resource):
        raise AssertionError("coverage must consume resolver coverage_obligations")

    monkeypatch.setattr(
        resource_gate_module,
        "ResolvedResourceLineageService",
        SpyResolvedResourceLineageService,
    )
    monkeypatch.setattr(
        ResourceGateService,
        "_coverage_obligation_refs",
        staticmethod(fail_if_legacy_summary_obligations_are_used),
    )

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Coverage via resolver obligations",
                created_by=actor_id,
                screen_mockups=[{"id": "mock-1", "title": "Primary flow"}],
            )
        )
        await db.commit()

        result = await ResourceGateService(db).validate_spec_resource_task_coverage(
            board_id,
            spec_id,
        )

    assert calls
    args, kwargs = calls[0]
    assert args[:3] == (board_id, "spec", spec_id)
    assert kwargs["include_coverage"] is True
    assert kwargs["projection_profile"] == "full"
    assert result["allowed"] is False
    assert result["required_resources"] == result["summary"]["resource_lineage"]["coverage_obligations"]
    assert result["required_resources"][0]["unique_resource_id"] == "mockup:mock-1"
    assert result["uncovered_resources"][0]["unique_resource_id"] == "mockup:mock-1"
    assert result["uncovered_resources"][0]["origin_evidence"]["id"] == "mock-1"


@pytest.mark.asyncio
async def test_resource_gate_blocks_done_transition_until_resources_provided_or_na(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea requiring explicit resources",
                created_by=actor_id,
                status=IdeationStatus.EVALUATING,
            )
        )
        await db.commit()

        with pytest.raises(ResourceGateViolation):
            await IdeationService(db).move_ideation(
                ideation_id,
                actor_id,
                IdeationMove(status=IdeationStatus.DONE),
            )

        service = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is intentionally not applicable in this test.",
                source_channel="ui",
            )

        moved = await IdeationService(db).move_ideation(
            ideation_id,
            actor_id,
            IdeationMove(status=IdeationStatus.DONE),
        )
        assert moved.status == IdeationStatus.DONE


@pytest.mark.asyncio
async def test_architecture_finding_gate_blocks_level1_done_until_resolved(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture finding gate", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Idea with acknowledged warning",
                created_by=actor_id,
                status=IdeationStatus.EVALUATING,
            )
        )
        await db.flush()
        architecture = ArchitectureDesign(
            board_id=board_id,
            parent_type="ideation",
            ideation_id=ideation_id,
            title="Warning-bearing architecture",
            global_description="Architecture context",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        db.add(architecture)
        await db.flush()

        service = ResourceGateService(db)
        for resource_type in ("mockup", "knowledge_base"):
            await service.mark_not_applicable(
                board_id,
                "ideation",
                ideation_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is not needed for this test.",
                source_channel="ui",
            )

        store = ArchitectureFindingRunStore(db)
        warning = {
            "code": "orphan_entity",
            "severity": "warning",
            "message": "Entity is not connected in any diagram.",
            "suggested_fix": "Connect the entity to the runtime path.",
            "diagram_id": "diag-1",
            "element_id": "entity-1",
            "path": "$.diagrams[0].elements[0]",
        }
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=architecture.id,
            design_version=architecture.version,
            critic_run_id="critic-active",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[warning],
        )
        active_findings = await store.list_findings(
            design_id=architecture.id,
            lifecycle=ARCHITECTURE_FINDING_ACTIVE,
        )
        assert len(active_findings) == 1
        acknowledgements = await store.record_acknowledgements(
            board_id=board_id,
            design_id=architecture.id,
            critic_run_id="critic-active",
            finding_keys=[active_findings[0].finding_key],
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            statement="Reviewed warning during authoring; not a done clearance.",
        )
        assert len(acknowledgements) == 1
        assert acknowledgements[0].finding_key == active_findings[0].finding_key
        assert acknowledgements[0].critic_run_id == "critic-active"
        assert acknowledgements[0].design_version == architecture.version
        assert acknowledgements[0].actor_type == "agent"
        assert acknowledgements[0].actor_id == actor_id
        assert acknowledgements[0].actor_name == "Validator"
        assert acknowledgements[0].statement == "Reviewed warning during authoring; not a done clearance."

        reset_architecture_observability_for_tests()
        summary = await service.get_summary(board_id, "ideation", ideation_id)
        assert summary["blocking"] is False
        assert summary["architecture_findings_blocking"] is True
        assert summary["architecture_findings"]["active_count"] == 1
        assert summary["architecture_findings"]["top_remediation"][0]["code"] == "orphan_entity"

        result = await service.validate_entity_completion(board_id, "ideation", ideation_id)
        assert result["allowed"] is False
        assert result["blocking_architecture_findings"][0]["target_ref"] == "entity-1"

        with pytest.raises(ResourceGateViolation) as exc_info:
            await service.validate_or_raise_entity_completion(
                board_id,
                "ideation",
                ideation_id,
                phase="ideation_done",
            )
        assert exc_info.value.code == "architecture_findings_block_done"
        assert exc_info.value.details["architecture_findings"]["active_count"] == 1
        still_active = await store.list_findings(
            design_id=architecture.id,
            lifecycle=ARCHITECTURE_FINDING_ACTIVE,
        )
        assert [finding.finding_key for finding in still_active] == [acknowledgements[0].finding_key]
        samples = get_architecture_metric_samples()
        assert [sample for sample in samples if sample["metric_name"] == METRIC_GATE_EVAL_DURATION_MS]
        assert [sample for sample in samples if sample["metric_name"] == METRIC_PROJECTION_TOTAL]
        assert [
            sample for sample in samples
            if sample["metric_name"] == METRIC_DONE_BLOCKER_TOTAL
            and sample["labels"]["outcome"] == "blocked"
            and sample["labels"]["owner_type"] == "ideation"
        ]
        for sample in samples:
            assert_architecture_metric_payload_is_safe(sample["labels"])

        await store.upsert_latest_run(
            board_id=board_id,
            design_id=architecture.id,
            design_version=architecture.version,
            critic_run_id="critic-resolved",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[],
        )
        all_findings = await store.list_findings(design_id=architecture.id)
        by_lifecycle = {finding.lifecycle for finding in all_findings}
        assert ARCHITECTURE_FINDING_ACTIVE not in by_lifecycle
        assert ARCHITECTURE_FINDING_RESOLVED in by_lifecycle

        allowed = await service.validate_entity_completion(board_id, "ideation", ideation_id)
        assert allowed["allowed"] is True


@pytest.mark.asyncio
async def test_resource_gate_summary_projects_architecture_findings_matrix(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture finding projection", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Projection spec",
                created_by=actor_id,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Projection card",
                card_type=CardType.NORMAL,
                status=CardStatus.VALIDATION,
                created_by=actor_id,
            )
        )
        await db.flush()

        designs = {}
        for slug in ("no-findings", "active", "resolved"):
            design = ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title=f"{slug} architecture",
                global_description=f"{slug} architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor_id,
            )
            db.add(design)
            await db.flush()
            designs[slug] = design

        service = ResourceGateService(db)
        for resource_type in ("mockup", "knowledge_base"):
            await service.mark_not_applicable(
                board_id,
                "card",
                card_id,
                resource_type,
                actor_id,
                justification=f"{resource_type} is not needed for projection assertions.",
                source_channel="ui",
            )

        store = ArchitectureFindingRunStore(db)
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["active"].id,
            design_version=designs["active"].version,
            critic_run_id="critic-active-projection",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[
                {
                    "code": "orphan_entity",
                    "severity": "warning",
                    "message": "Entity is not connected in any diagram.",
                    "suggested_fix": "Connect the entity to the runtime path.",
                    "diagram_id": "diag-active",
                    "element_id": "entity-active",
                    "path": "$.diagrams[0].elements[0]",
                }
            ],
        )
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["resolved"].id,
            design_version=designs["resolved"].version,
            critic_run_id="critic-resolved-before",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[
                {
                    "code": "uncovered_interface",
                    "severity": "warning",
                    "message": "Interface is declared but not shown.",
                    "suggested_fix": "Draw the interface in the diagram.",
                    "diagram_id": "diag-resolved",
                    "entity_id": "iface-resolved",
                    "path": "$.interfaces[0]",
                }
            ],
        )
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=designs["resolved"].id,
            design_version=designs["resolved"].version,
            critic_run_id="critic-resolved-after",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[],
        )

        reset_architecture_observability_for_tests()
        summary = await service.get_summary(board_id, "card", card_id)

        assert summary["blocking"] is False
        assert summary["architecture_findings_blocking"] is True
        assert summary["warnings"][0]["code"] == "architecture_findings_active"
        findings = summary["architecture_findings"]
        assert findings["owner_type"] == "card"
        assert findings["owner_id"] == card_id
        assert findings["design_count"] == 3
        assert findings["active_count"] == 1
        assert findings["resolved_count"] == 1
        assert findings["by_code"] == {"orphan_entity": 1}

        by_design = {item["design_id"]: item for item in findings["by_design"]}
        assert by_design[designs["no-findings"].id]["active_count"] == 0
        assert by_design[designs["no-findings"].id]["resolved_count"] == 0
        assert by_design[designs["no-findings"].id]["source_entity_type"] == "card"
        assert by_design[designs["active"].id]["active_count"] == 1
        assert by_design[designs["resolved"].id]["resolved_count"] == 1

        remediation = findings["top_remediation"][0]
        assert remediation["design_id"] == designs["active"].id
        assert remediation["design_title"] == "active architecture"
        assert remediation["source_entity_type"] == "card"
        assert remediation["source_entity_id"] == card_id
        assert remediation["code"] == "orphan_entity"
        assert remediation["severity"] == "warning"
        assert remediation["normalized_target_kind"] == "element"
        assert remediation["target_ref"] == "entity-active"
        assert remediation["path"] == "$.diagrams[0].elements[0]"
        assert remediation["suggested_fix"] == "Connect the entity to the runtime path."
        assert "audit-only" in remediation["remediation"]

        projection_samples = [
            sample for sample in get_architecture_metric_samples()
            if sample["metric_name"] == METRIC_PROJECTION_TOTAL
        ]
        assert projection_samples
        assert projection_samples[-1]["labels"]["outcome"] == "active"
        assert projection_samples[-1]["labels"]["owner_type"] == "card"
        assert_architecture_metric_payload_is_safe(projection_samples[-1]["labels"])


@pytest.mark.asyncio
async def test_architecture_finding_gate_blocks_all_level1_owner_types_with_same_remediation(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture finding level1 matrix", owner_id=actor_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Level1 ideation",
                created_by=actor_id,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="Level1 refinement",
                created_by=actor_id,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                refinement_id=refinement_id,
                title="Level1 spec",
                created_by=actor_id,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Level1 card",
                card_type=CardType.NORMAL,
                status=CardStatus.VALIDATION,
                created_by=actor_id,
            )
        )
        await db.flush()

        owners = [
            ("card", card_id, {"card_id": card_id}),
            ("ideation", ideation_id, {"ideation_id": ideation_id}),
            ("refinement", refinement_id, {"refinement_id": refinement_id}),
        ]
        store = ArchitectureFindingRunStore(db)
        service = ResourceGateService(db)

        for entity_type, entity_id, owner_fk in owners:
            architecture = ArchitectureDesign(
                board_id=board_id,
                parent_type=entity_type,
                title=f"{entity_type} architecture",
                global_description="Architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor_id,
                **owner_fk,
            )
            db.add(architecture)
            await db.flush()
            for resource_type in ("mockup", "knowledge_base"):
                await service.mark_not_applicable(
                    board_id,
                    entity_type,
                    entity_id,
                    resource_type,
                    actor_id,
                    justification=f"{resource_type} is not needed for this level1 matrix test.",
                    source_channel="ui",
                )
            await store.upsert_latest_run(
                board_id=board_id,
                design_id=architecture.id,
                design_version=architecture.version,
                critic_run_id=f"critic-active-{entity_type}",
                actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
                validator_summary={"valid": True, "issues": []},
                structured_warnings=[
                    {
                        "code": "orphan_entity",
                        "severity": "warning",
                        "message": "Entity is not connected in any diagram.",
                        "suggested_fix": "Connect the entity to the runtime path.",
                        "diagram_id": "diag-1",
                        "element_id": f"entity-{entity_type}",
                        "path": "$.diagrams[0].elements[0]",
                    }
                ],
            )

        remediation_shapes: list[set[str]] = []
        for entity_type, entity_id, _owner_fk in owners:
            with pytest.raises(ResourceGateViolation) as exc_info:
                await service.validate_or_raise_entity_completion(
                    board_id,
                    entity_type,
                    entity_id,
                    phase=f"{entity_type}_done",
                )
            assert exc_info.value.code == "architecture_findings_block_done"
            findings = exc_info.value.details["architecture_findings"]
            assert findings["active_count"] == 1
            assert findings["by_design"][0]["source_entity_type"] == entity_type
            remediation = findings["top_remediation"][0]
            assert remediation["code"] == "orphan_entity"
            assert remediation["severity"] == "warning"
            assert remediation["target_ref"] == f"entity-{entity_type}"
            assert remediation["path"] == "$.diagrams[0].elements[0]"
            assert remediation["suggested_fix"] == "Connect the entity to the runtime path."
            remediation_shapes.append(set(remediation.keys()))

        assert remediation_shapes == [remediation_shapes[0]] * len(remediation_shapes)


@pytest.mark.asyncio
async def test_architecture_finding_gate_visible_on_spec_validation_but_blocks_spec_done(db_factory):
    board_id = _id("board")
    actor_id = _id("agent")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture finding gate", owner_id=actor_id))
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with active architecture finding",
            created_by=actor_id,
        )
        task = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation task with copied architecture",
            created_by=actor_id,
            card_type=CardType.NORMAL,
            status=CardStatus.DONE,
        )
        db.add(spec)
        db.add(task)
        await db.flush()
        architecture = ArchitectureDesign(
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Architecture context",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
        db.add(architecture)
        await db.flush()
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="card",
                card_id=card_id,
                title="Task architecture copy",
                global_description="Task copy",
                entities=[],
                interfaces=[],
                diagrams=[],
                source_design_id=architecture.id,
                created_by=actor_id,
            )
        )
        await db.flush()
        await ArchitectureFindingRunStore(db).upsert_latest_run(
            board_id=board_id,
            design_id=architecture.id,
            design_version=architecture.version,
            critic_run_id="critic-spec-active",
            actor={"actor_id": actor_id, "actor_type": "agent", "actor_name": "Validator"},
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[
                {
                    "code": "uncovered_interface",
                    "severity": "warning",
                    "message": "Interface is declared but not shown in the diagram.",
                    "diagram_id": "diag-1",
                    "entity_id": "iface-1",
                    "path": "$.interfaces[0]",
                }
            ],
        )

        service = ResourceGateService(db)
        coverage = await service.validate_spec_resource_task_coverage(board_id, spec_id)
        assert coverage["allowed"] is True
        assert coverage["architecture_findings"]["active_count"] == 1

        visible = await service.validate_or_raise_spec_resource_task_coverage(
            board_id,
            spec_id,
            phase="spec_validation",
        )
        assert visible["allowed"] is True
        assert visible["architecture_findings"]["active_count"] == 1

        with pytest.raises(ResourceGateViolation) as exc_info:
            await service.validate_or_raise_spec_resource_task_coverage(
                board_id,
                spec_id,
                phase="spec_done",
            )
        assert exc_info.value.code == "architecture_findings_block_done"
        findings = exc_info.value.details["architecture_findings"]
        assert findings["owner_type"] == "spec"
        assert findings["owner_id"] == spec_id
        assert findings["active_count"] == 1
        assert findings["by_design"][0]["design_id"] == architecture.id
        assert findings["by_design"][0]["source_entity_type"] == "spec"
        assert findings["top_remediation"][0]["code"] == "uncovered_interface"
        assert findings["top_remediation"][0]["source_entity_type"] == "spec"
        assert exc_info.value.details["blocking_architecture_findings"] == (
            findings["top_remediation"]
        )
        blocker_samples = [
            sample for sample in get_architecture_metric_samples()
            if sample["metric_name"] == METRIC_DONE_BLOCKER_TOTAL
        ]
        assert blocker_samples
        assert blocker_samples[-1]["labels"]["owner_type"] == "spec"
        assert blocker_samples[-1]["labels"]["outcome"] == "blocked"
