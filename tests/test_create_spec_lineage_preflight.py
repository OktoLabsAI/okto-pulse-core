"""CreateSpecUseCase resolves explicit lineage before the Spec FK write."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, update

from okto_pulse.core.application.processors.consolidation import _spec_to_dict
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.application.use_cases import (
    CreateSpecCommand,
    CreateSpecUseCase,
    UpdateSpecCommand,
    UpdateSpecUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import (
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import SpecCreate, SpecUpdate
from okto_pulse.core.services.effective_resource_propagation import (
    ResourceLineageResolutionError,
)
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecLineagePreflightError,
    SpecService,
)
from sqlalchemy_domain_event_delivery_store import build_test_event_processor
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    ConsolidationQueue,
    DomainEventRow,
    Ideation,
    Refinement,
    Spec,
    SpecHistory,
)
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory


USER_ID = "create-spec-lineage-user"


@pytest.fixture
async def lineage_graph() -> dict[str, str]:
    board_id = f"spec-lineage-a-{uuid.uuid4().hex[:8]}"
    foreign_board_id = f"spec-lineage-b-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add_all(
            (
                Board(
                    id=board_id,
                    name="Spec lineage A",
                    owner_id=USER_ID,
                    realm_id=LOCAL_REALM_ID,
                ),
                Board(
                    id=foreign_board_id,
                    name="Spec lineage B",
                    owner_id=USER_ID,
                    realm_id=LOCAL_REALM_ID,
                ),
            )
        )
        await db.flush()

        idea_a = Ideation(
            board_id=board_id,
            title="Idea A",
            created_by=USER_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.SMALL,
        )
        idea_a_other = Ideation(
            board_id=board_id,
            title="Idea A other",
            created_by=USER_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.SMALL,
        )
        idea_b = Ideation(
            board_id=foreign_board_id,
            title="Idea B",
            created_by=USER_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.SMALL,
        )
        idea_draft = Ideation(
            board_id=board_id,
            title="Idea draft",
            created_by=USER_ID,
            status=IdeationStatus.DRAFT,
            complexity=IdeationComplexity.SMALL,
        )
        idea_medium = Ideation(
            board_id=board_id,
            title="Idea medium",
            created_by=USER_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.MEDIUM,
        )
        idea_large = Ideation(
            board_id=board_id,
            title="Idea large",
            created_by=USER_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.LARGE,
        )
        db.add_all(
            (
                idea_a,
                idea_a_other,
                idea_b,
                idea_draft,
                idea_medium,
                idea_large,
            )
        )
        await db.flush()

        refinement_a = Refinement(
            board_id=board_id,
            ideation_id=idea_a.id,
            title="Refinement A",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_a_other = Refinement(
            board_id=board_id,
            ideation_id=idea_a_other.id,
            title="Refinement A other",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_b = Refinement(
            board_id=foreign_board_id,
            ideation_id=idea_b.id,
            title="Refinement B",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_medium_done = Refinement(
            board_id=board_id,
            ideation_id=idea_medium.id,
            title="Refinement medium done",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_large_draft = Refinement(
            board_id=board_id,
            ideation_id=idea_large.id,
            title="Refinement large draft",
            created_by=USER_ID,
            status=RefinementStatus.DRAFT,
        )
        refinement_large_done = Refinement(
            board_id=board_id,
            ideation_id=idea_large.id,
            title="Refinement large done",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_draft_ancestor = Refinement(
            board_id=board_id,
            ideation_id=idea_draft.id,
            title="Refinement with draft ancestor",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        refinement_cross_board_ancestor = Refinement(
            board_id=board_id,
            ideation_id=idea_b.id,
            title="Refinement with cross-board ancestor",
            created_by=USER_ID,
            status=RefinementStatus.DONE,
        )
        db.add_all(
            (
                refinement_a,
                refinement_a_other,
                refinement_b,
                refinement_medium_done,
                refinement_large_draft,
                refinement_large_done,
                refinement_draft_ancestor,
                refinement_cross_board_ancestor,
            )
        )
        await db.flush()

        graph = {
            "board_id": board_id,
            "foreign_board_id": foreign_board_id,
            "idea_a": idea_a.id,
            "idea_a_other": idea_a_other.id,
            "idea_b": idea_b.id,
            "idea_draft": idea_draft.id,
            "idea_medium": idea_medium.id,
            "idea_large": idea_large.id,
            "refinement_a": refinement_a.id,
            "refinement_a_other": refinement_a_other.id,
            "refinement_b": refinement_b.id,
            "refinement_medium_done": refinement_medium_done.id,
            "refinement_large_draft": refinement_large_draft.id,
            "refinement_large_done": refinement_large_done.id,
            "refinement_draft_ancestor": refinement_draft_ancestor.id,
            "refinement_cross_board_ancestor": refinement_cross_board_ancestor.id,
        }
        await db.commit()
    return graph


async def _spec_count(board_id: str) -> int:
    async with get_session_factory()() as db:
        return int(
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            or 0
        )


async def _board_write_snapshot(board_id: str) -> dict[str, int]:
    """Bounded proof that a rejected create emitted no durable side effect."""

    async with get_session_factory()() as db:
        async def _count(model, *filters) -> int:
            return int(
                await db.scalar(
                    select(func.count()).select_from(model).where(*filters)
                )
                or 0
            )

        return {
            "specs": await _count(Spec, Spec.board_id == board_id),
            "activity": await _count(
                ActivityLog,
                ActivityLog.board_id == board_id,
            ),
            "events": await _count(
                DomainEventRow,
                DomainEventRow.board_id == board_id,
            ),
            "consolidation": await _count(
                ConsolidationQueue,
                ConsolidationQueue.board_id == board_id,
            ),
        }


async def _drain_domain_events() -> None:
    processor = build_test_event_processor(get_session_factory())
    await processor.recover_orphans()
    for _ in range(5):
        if not await processor.process_batch():
            return
    raise AssertionError("domain event processor did not drain within five batches")


async def _create_spec(
    board_id: str,
    *,
    ideation_id: str | None = None,
    refinement_id: str | None = None,
):
    actor = ActorContext(USER_ID, "rest", realm_id=LOCAL_REALM_ID)
    uow_factory = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    async with uow_factory(actor=actor) as uow:
        return await CreateSpecUseCase().execute(
            CreateSpecCommand(
                board_id,
                SpecCreate(
                    title=f"Lineage spec {uuid.uuid4().hex[:8]}",
                    ideation_id=ideation_id,
                    refinement_id=refinement_id,
                ),
            ),
            actor=actor,
            uow=uow,
        )


async def _update_spec(spec_id: str, **updates):
    actor = ActorContext(USER_ID, "rest", realm_id=LOCAL_REALM_ID)
    uow_factory = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    async with uow_factory(actor=actor) as uow:
        return await UpdateSpecUseCase().execute(
            UpdateSpecCommand(spec_id, SpecUpdate(**updates)),
            actor=actor,
            uow=uow,
        )


async def _spec_write_snapshot(board_id: str, spec_id: str) -> dict[str, object]:
    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        assert spec is not None

        async def _count(model, *filters) -> int:
            return int(
                await db.scalar(
                    select(func.count()).select_from(model).where(*filters)
                )
                or 0
            )

        return {
            "ideation_id": spec.ideation_id,
            "refinement_id": spec.refinement_id,
            "version": spec.version,
            "updated_at": spec.updated_at,
            "activity": await _count(
                ActivityLog,
                ActivityLog.board_id == board_id,
            ),
            "history": await _count(
                SpecHistory,
                SpecHistory.spec_id == spec_id,
            ),
            "events": await _count(
                DomainEventRow,
                DomainEventRow.board_id == board_id,
            ),
            "consolidation": await _count(
                ConsolidationQueue,
                ConsolidationQueue.board_id == board_id,
            ),
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("no_parent", "ideation", "refinement", "both"))
async def test_create_spec_valid_parent_matrix_persists(lineage_graph, case):
    parent_args = {
        "no_parent": {},
        "ideation": {"ideation_id": lineage_graph["idea_a"]},
        "refinement": {"refinement_id": lineage_graph["refinement_a"]},
        "both": {
            "ideation_id": lineage_graph["idea_a"],
            "refinement_id": lineage_graph["refinement_a"],
        },
    }[case]
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    result = await _create_spec(board_id, **parent_args)

    assert result.spec is not None
    assert result.spec.edition == 1
    assert result.spec.ideation_id == parent_args.get("ideation_id")
    assert result.spec.refinement_id == parent_args.get("refinement_id")
    assert await _spec_count(board_id) == before + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "missing_ideation",
        "missing_refinement",
        "cross_board_ideation",
        "cross_board_refinement",
        "cross_board_ancestor_refinement",
        "parent_mismatch",
    ),
)
async def test_create_spec_invalid_parent_matrix_is_governed_and_writes_nothing(
    lineage_graph,
    case,
):
    parent_args = {
        "missing_ideation": {"ideation_id": "missing-ideation"},
        "missing_refinement": {"refinement_id": "missing-refinement"},
        "cross_board_ideation": {"ideation_id": lineage_graph["idea_b"]},
        "cross_board_refinement": {
            "refinement_id": lineage_graph["refinement_b"]
        },
        "cross_board_ancestor_refinement": {
            "refinement_id": lineage_graph["refinement_cross_board_ancestor"]
        },
        "parent_mismatch": {
            "ideation_id": lineage_graph["idea_a"],
            "refinement_id": lineage_graph["refinement_a_other"],
        },
    }[case]
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    with pytest.raises(ResourceLineageResolutionError) as raised:
        await _create_spec(board_id, **parent_args)

    assert raised.value.to_error_dict()["error"] == (
        "resource_lineage_resolution_failed"
    )
    assert await _spec_count(board_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_args_key", "expected_code"),
    (
        ("draft_ideation", "spec_ideation_not_done"),
        ("medium_without_refinement", "spec_refinement_required"),
        ("large_without_refinement", "spec_refinement_required"),
        ("large_with_draft_refinement", "spec_refinement_not_done"),
    ),
)
async def test_create_spec_parent_lifecycle_is_checked_before_write(
    lineage_graph,
    parent_args_key,
    expected_code,
):
    parent_args = {
        "draft_ideation": {"ideation_id": lineage_graph["idea_draft"]},
        "medium_without_refinement": {
            "ideation_id": lineage_graph["idea_medium"]
        },
        "large_without_refinement": {
            "ideation_id": lineage_graph["idea_large"]
        },
        "large_with_draft_refinement": {
            "ideation_id": lineage_graph["idea_large"],
            "refinement_id": lineage_graph["refinement_large_draft"],
        },
    }[parent_args_key]
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    with pytest.raises(SpecLineagePreflightError) as raised:
        await _create_spec(board_id, **parent_args)

    assert raised.value.code == expected_code
    assert raised.value.to_dict()["code"] == expected_code
    assert await _spec_count(board_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ideation_key", "refinement_key"),
    (
        ("idea_medium", "refinement_medium_done"),
        ("idea_large", "refinement_large_done"),
    ),
)
async def test_create_spec_complex_lineage_accepts_done_refinement(
    lineage_graph,
    ideation_key,
    refinement_key,
):
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    result = await _create_spec(
        board_id,
        ideation_id=lineage_graph[ideation_key],
        refinement_id=lineage_graph[refinement_key],
    )

    assert result.spec is not None
    assert result.spec.ideation_id == lineage_graph[ideation_key]
    assert result.spec.refinement_id == lineage_graph[refinement_key]
    assert await _spec_count(board_id) == before + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refinement_key",
    (
        "refinement_a",
        "refinement_medium_done",
        "refinement_large_done",
    ),
)
async def test_create_spec_refinement_only_validates_and_accepts_done_ancestor(
    lineage_graph,
    refinement_key,
):
    """Small/medium/large ancestors share the same refinement-only contract."""

    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    result = await _create_spec(
        board_id,
        refinement_id=lineage_graph[refinement_key],
    )

    assert result.spec is not None
    assert result.spec.ideation_id is None
    assert result.spec.refinement_id == lineage_graph[refinement_key]
    assert await _spec_count(board_id) == before + 1


@pytest.mark.asyncio
async def test_create_spec_refinement_only_rejects_draft_ancestor_before_any_write(
    lineage_graph,
):
    board_id = lineage_graph["board_id"]
    before = await _board_write_snapshot(board_id)

    with pytest.raises(SpecLineagePreflightError) as raised:
        await _create_spec(
            board_id,
            refinement_id=lineage_graph["refinement_draft_ancestor"],
        )

    assert raised.value.code == "spec_ideation_not_done"
    assert raised.value.facts["ideation_id"] == lineage_graph["idea_draft"]
    assert await _board_write_snapshot(board_id) == before


@pytest.mark.asyncio
async def test_refinement_derive_rechecks_reopened_ideation_before_preflights(
    lineage_graph,
    monkeypatch,
):
    """A completed refinement cannot outlive its ancestor's DONE eligibility."""

    board_id = lineage_graph["board_id"]
    ideation_id = lineage_graph["idea_a"]
    refinement_id = lineage_graph["refinement_a"]
    async with get_session_factory()() as db:
        await db.execute(
            update(Ideation)
            .where(Ideation.id == ideation_id)
            .values(status=IdeationStatus.DRAFT)
        )
        await db.commit()
    before = await _board_write_snapshot(board_id)

    def _unexpected_artifact_preflight(**_kwargs):
        raise AssertionError("artifact preflight must not run for invalid lineage")

    monkeypatch.setattr(
        "okto_pulse.core.services.main.validate_artifact_selections",
        _unexpected_artifact_preflight,
    )

    async with get_session_factory()() as db:
        with pytest.raises(SpecLineagePreflightError) as raised:
            await RefinementService(db).derive_spec(
                refinement_id,
                USER_ID,
                skip_ownership_check=True,
            )
        await db.rollback()

    assert raised.value.code == "spec_ideation_not_done"
    assert raised.value.facts == {
        "ideation_id": ideation_id,
        "ideation_status": IdeationStatus.DRAFT.value,
    }
    assert await _board_write_snapshot(board_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ("ideation", "refinement"))
async def test_derive_spec_entrypoints_accept_valid_shared_lineage(
    lineage_graph,
    source,
):
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    async with get_session_factory()() as db:
        if source == "ideation":
            spec = await IdeationService(db).derive_spec(
                lineage_graph["idea_a"],
                USER_ID,
                skip_ownership_check=True,
            )
            expected_ideation_id = lineage_graph["idea_a"]
            expected_refinement_id = None
        else:
            spec = await RefinementService(db).derive_spec(
                lineage_graph["refinement_medium_done"],
                USER_ID,
                skip_ownership_check=True,
            )
            expected_ideation_id = lineage_graph["idea_medium"]
            expected_refinement_id = lineage_graph["refinement_medium_done"]
        await db.commit()

    assert spec is not None
    assert spec.ideation_id == expected_ideation_id
    assert spec.refinement_id == expected_refinement_id
    assert await _spec_count(board_id) == before + 1


@pytest.mark.asyncio
async def test_refinement_only_missing_ancestor_uses_typed_lineage_error(monkeypatch):
    refinement = SimpleNamespace(
        id="refinement-orphan",
        board_id="board-a",
        ideation_id="ideation-missing",
        status=RefinementStatus.DONE,
    )

    async def _fake_get(_context, entity, entity_id):
        if (entity, entity_id) == ("refinement", refinement.id):
            return refinement
        return None

    monkeypatch.setattr(
        "okto_pulse.core.services.main._application_get",
        _fake_get,
    )

    with pytest.raises(SpecLineagePreflightError) as raised:
        await SpecService(object())._validate_lineage(
            "board-a",
            ideation_id=None,
            refinement_id=refinement.id,
        )

    assert raised.value.code == "spec_ideation_not_found"
    assert raised.value.facts == {
        "board_id": "board-a",
        "ideation_id": "ideation-missing",
        "refinement_id": refinement.id,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("updates_key", "expected_code"),
    (
        ("missing_ideation", "spec_ideation_not_found"),
        ("cross_board_ideation", "spec_ideation_board_mismatch"),
        ("draft_ideation", "spec_ideation_not_done"),
        ("medium_without_refinement", "spec_refinement_required"),
        ("draft_refinement", "spec_refinement_not_done"),
        ("draft_ancestor_refinement", "spec_ideation_not_done"),
        ("cross_board_ancestor_refinement", "spec_ideation_board_mismatch"),
        ("parent_mismatch", "spec_parent_lineage_mismatch"),
    ),
)
async def test_update_spec_lineage_preflight_is_typed_and_atomic(
    lineage_graph,
    updates_key,
    expected_code,
):
    board_id = lineage_graph["board_id"]
    created = await _create_spec(board_id)
    spec_id = created.spec.id
    updates = {
        "missing_ideation": {"ideation_id": "missing-ideation"},
        "cross_board_ideation": {"ideation_id": lineage_graph["idea_b"]},
        "draft_ideation": {"ideation_id": lineage_graph["idea_draft"]},
        "medium_without_refinement": {
            "ideation_id": lineage_graph["idea_medium"],
        },
        "draft_refinement": {
            "ideation_id": lineage_graph["idea_large"],
            "refinement_id": lineage_graph["refinement_large_draft"],
        },
        "draft_ancestor_refinement": {
            "refinement_id": lineage_graph["refinement_draft_ancestor"],
        },
        "cross_board_ancestor_refinement": {
            "refinement_id": lineage_graph["refinement_cross_board_ancestor"],
        },
        "parent_mismatch": {
            "ideation_id": lineage_graph["idea_a"],
            "refinement_id": lineage_graph["refinement_a_other"],
        },
    }[updates_key]
    before = await _spec_write_snapshot(board_id, spec_id)

    with pytest.raises(SpecLineagePreflightError) as raised:
        await _update_spec(spec_id, **updates)

    assert raised.value.code == expected_code
    assert raised.value.to_error_dict()["error"] == expected_code
    assert await _spec_write_snapshot(board_id, spec_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ideation_key", "refinement_key"),
    (
        ("idea_a", None),
        ("idea_medium", "refinement_medium_done"),
        ("idea_large", "refinement_large_done"),
    ),
)
async def test_update_spec_accepts_valid_effective_lineage(
    lineage_graph,
    ideation_key,
    refinement_key,
):
    board_id = lineage_graph["board_id"]
    created = await _create_spec(board_id)
    updates = {"ideation_id": lineage_graph[ideation_key]}
    if refinement_key is not None:
        updates["refinement_id"] = lineage_graph[refinement_key]

    result = await _update_spec(created.spec.id, **updates)

    assert result.spec.ideation_id == lineage_graph[ideation_key]
    assert result.spec.refinement_id == (
        lineage_graph[refinement_key] if refinement_key is not None else None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refinement_key",
    (
        "refinement_a",
        "refinement_medium_done",
        "refinement_large_done",
    ),
)
async def test_update_spec_accepts_refinement_only_with_done_ancestor(
    lineage_graph,
    refinement_key,
):
    created = await _create_spec(lineage_graph["board_id"])

    result = await _update_spec(
        created.spec.id,
        refinement_id=lineage_graph[refinement_key],
    )

    assert result.spec.ideation_id is None
    assert result.spec.refinement_id == lineage_graph[refinement_key]


@pytest.mark.asyncio
async def test_lineage_only_relink_emits_one_semantic_reenqueue_and_switches_worker_edge(
    lineage_graph,
):
    board_id = lineage_graph["board_id"]
    created = await _create_spec(
        board_id,
        ideation_id=lineage_graph["idea_a"],
    )
    spec_id = created.spec.id
    spec_entity_id = f"spec_{spec_id[:8]}_entity"
    before_worker = DeterministicWorker().process_spec(
        _spec_to_dict(created.spec)
    )
    before_parent_edges = [
        edge
        for edge in before_worker.edges
        if edge.from_candidate_id == spec_entity_id
        and edge.candidate_id
        in {
            f"spec_{spec_id[:8]}_belongs_to_ideation",
            f"spec_{spec_id[:8]}_belongs_to_refinement",
        }
    ]
    assert [
        (edge.candidate_id, edge.to_candidate_id)
        for edge in before_parent_edges
    ] == [
        (
            f"spec_{spec_id[:8]}_belongs_to_ideation",
            f"ideation_{lineage_graph['idea_a'][:8]}_entity",
        )
    ]

    # Drain the create event first, then make its one queue row terminal. The
    # relink event must reopen that same effective row rather than insert a
    # duplicate or rely on the original pending enqueue.
    await _drain_domain_events()
    async with get_session_factory()() as db:
        queue_rows = (
            (
                await db.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == board_id,
                        ConsolidationQueue.artifact_type == "spec",
                        ConsolidationQueue.artifact_id == spec_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(queue_rows) == 1
        await db.execute(
            update(ConsolidationQueue)
            .where(ConsolidationQueue.id == queue_rows[0].id)
            .values(status="done")
        )
        await db.commit()

    old_version = created.spec.version
    updated = await _update_spec(
        spec_id,
        ideation_id=lineage_graph["idea_medium"],
        refinement_id=lineage_graph["refinement_medium_done"],
    )

    assert updated.spec.version == old_version
    after_worker = DeterministicWorker().process_spec(
        _spec_to_dict(updated.spec)
    )
    after_parent_edges = [
        edge
        for edge in after_worker.edges
        if edge.from_candidate_id == spec_entity_id
        and edge.candidate_id
        in {
            f"spec_{spec_id[:8]}_belongs_to_ideation",
            f"spec_{spec_id[:8]}_belongs_to_refinement",
        }
    ]
    assert [
        (edge.candidate_id, edge.to_candidate_id)
        for edge in after_parent_edges
    ] == [
        (
            f"spec_{spec_id[:8]}_belongs_to_refinement",
            (
                "refinement_"
                f"{lineage_graph['refinement_medium_done'][:8]}_entity"
            ),
        )
    ]
    assert all(
        edge.to_candidate_id
        != f"ideation_{lineage_graph['idea_a'][:8]}_entity"
        for edge in after_parent_edges
    )

    await _drain_domain_events()
    async with get_session_factory()() as db:
        semantic_events = (
            (
                await db.execute(
                    select(DomainEventRow).where(
                        DomainEventRow.board_id == board_id,
                        DomainEventRow.event_type == "spec.semantic_changed",
                    )
                )
            )
            .scalars()
            .all()
        )
        semantic_events = [
            event
            for event in semantic_events
            if event.payload_json.get("spec_id") == spec_id
        ]
        assert len(semantic_events) == 1
        assert semantic_events[0].payload_json["changed_fields"] == [
            "ideation_id",
            "refinement_id",
        ]

        version_events = (
            (
                await db.execute(
                    select(DomainEventRow).where(
                        DomainEventRow.board_id == board_id,
                        DomainEventRow.event_type == "spec.version_bumped",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert all(
            event.payload_json.get("spec_id") != spec_id
            for event in version_events
        )

        queue_rows = (
            (
                await db.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == board_id,
                        ConsolidationQueue.artifact_type == "spec",
                        ConsolidationQueue.artifact_id == spec_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(queue_rows) == 1
        assert queue_rows[0].status == "pending"
        assert queue_rows[0].triggered_by_event == "spec.semantic_changed"
        assert queue_rows[0].source == "event:spec.semantic_changed"


@pytest.mark.asyncio
async def test_update_spec_non_lineage_write_is_unchanged(lineage_graph):
    created = await _create_spec(lineage_graph["board_id"])

    result = await _update_spec(created.spec.id, title="Updated without lineage")

    assert result.spec.title == "Updated without lineage"
    assert result.spec.ideation_id is None
    assert result.spec.refinement_id is None


@pytest.mark.asyncio
async def test_update_spec_idempotent_lineage_resend_does_not_revalidate_ancestor(
    lineage_graph,
):
    created = await _create_spec(
        lineage_graph["board_id"],
        ideation_id=lineage_graph["idea_a"],
    )
    async with get_session_factory()() as db:
        idea = await db.get(Ideation, lineage_graph["idea_a"])
        assert idea is not None
        idea.status = IdeationStatus.DRAFT
        await db.commit()

    async with get_session_factory()() as db:
        before_semantic_events = (
            (
                await db.execute(
                    select(DomainEventRow).where(
                        DomainEventRow.board_id == lineage_graph["board_id"],
                        DomainEventRow.event_type == "spec.semantic_changed",
                    )
                )
            )
            .scalars()
            .all()
        )
        before_semantic_count = sum(
            event.payload_json.get("spec_id") == created.spec.id
            for event in before_semantic_events
        )

    result = await _update_spec(
        created.spec.id,
        ideation_id=lineage_graph["idea_a"],
    )

    assert result.spec.ideation_id == lineage_graph["idea_a"]
    async with get_session_factory()() as db:
        after_semantic_events = (
            (
                await db.execute(
                    select(DomainEventRow).where(
                        DomainEventRow.board_id == lineage_graph["board_id"],
                        DomainEventRow.event_type == "spec.semantic_changed",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert (
            sum(
                event.payload_json.get("spec_id") == created.spec.id
                for event in after_semantic_events
            )
            == before_semantic_count
        )
