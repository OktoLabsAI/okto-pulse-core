"""Focused contracts for the canonical SDLC and sprint lifecycle authority."""

from types import SimpleNamespace
import uuid

import pytest

from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    is_transition_allowed,
)
from okto_pulse.core.services.card_traceability import (
    TraceabilityTargetNotFoundError,
    link_card_traceability,
)
from okto_pulse.core.services.reviewer_separation import (
    evaluate_reviewer_separation,
)
from okto_pulse.core.services.sprint_scope import (
    SprintScopeResolver,
    completion_blockers,
)
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)


def test_registry_covers_every_sdlc_entity_and_exposes_transition_contracts() -> None:
    assert set(SDLC_REGISTRY) == {
        "story",
        "ideation",
        "refinement",
        "spec",
        "card",
        "sprint",
        "test_scenario",
    }
    for definition in SDLC_REGISTRY.values():
        assert set(definition.transitions) == {
            member.value for member in definition.status_enum
        }
        for edges in definition.transitions.values():
            for edge in edges:
                assert edge.label
                assert edge.gate
                assert edge.effects
                assert edge.reason_codes

    assert not is_transition_allowed("card", "not_started", "in_progress")
    assert is_transition_allowed(
        "card", "not_started", "in_progress", card_type="test"
    )


def test_traceability_validates_all_targets_before_mutation_and_is_idempotent() -> None:
    spec = SimpleNamespace(
        id="spec-1",
        test_scenarios=[{"id": "ts-1", "linked_task_ids": []}],
        functional_requirements=[{"id": "fr-1", "linked_task_ids": []}],
        business_rules=[{"id": "br-1", "linked_task_ids": []}],
    )
    card = SimpleNamespace(
        id="card-1", spec_id="spec-1", test_scenario_ids=[]
    )

    with pytest.raises(TraceabilityTargetNotFoundError):
        link_card_traceability(
            spec=spec,
            card=card,
            targets=[("scenario", "ts-1"), ("fr", "missing")],
        )
    assert spec.test_scenarios[0]["linked_task_ids"] == []
    assert card.test_scenario_ids == []

    first = link_card_traceability(
        spec=spec,
        card=card,
        targets=[("scenario", "ts-1"), ("fr", "fr-1"), ("rule", "br-1")],
    )
    second = link_card_traceability(
        spec=spec,
        card=card,
        targets=[("test_scenario", "ts-1"), ("functional_requirement", "fr-1")],
    )
    assert not first.idempotent
    assert second.idempotent
    assert card.test_scenario_ids == ["ts-1"]
    assert spec.test_scenarios[0]["linked_task_ids"] == ["card-1"]
    assert spec.functional_requirements[0]["linked_task_ids"] == ["card-1"]
    assert spec.business_rules[0]["linked_task_ids"] == ["card-1"]


def test_sprint_scope_union_is_version_cached_and_evidence_is_proportional() -> None:
    SprintScopeResolver.clear_cache()
    sprint = SimpleNamespace(
        id="sprint-1",
        version=1,
        test_scenario_ids=["ts-explicit"],
        business_rule_ids=["br-1"],
    )
    spec = SimpleNamespace(
        id="spec-1",
        version=1,
        functional_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[
            {"id": "ts-explicit", "status": "passed", "linked_task_ids": []},
            {"id": "ts-card", "status": "passed", "linked_task_ids": []},
        ],
        business_rules=[{"id": "br-1", "linked_task_ids": []}],
        technical_requirements=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
    )
    card = SimpleNamespace(
        id="card-1",
        version=1,
        status="done",
        card_type="test",
        test_scenario_ids=["ts-card"],
    )

    first = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card])
    cached = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card])
    # Identity/provenance is cached, but mutable payload dictionaries are always
    # reprojected into a fresh SprintScope.
    assert cached is not first
    assert cached.ids("test_scenarios") == first.ids("test_scenarios")
    assert cached.provenance == first.provenance
    assert set(first.ids("test_scenarios")) == {"ts-explicit", "ts-card"}
    blocker_codes = {item.code for item in completion_blockers(first)}
    assert blocker_codes == {
        "sprint_test_evidence_missing",
        "sprint_business_rule_uncovered",
    }

    spec.version = 2
    assert SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card]) is not first


def test_sprint_scope_dry_retry_reprojects_narrow_scenario_status_both_directions() -> None:
    """A writer may change scenario state without bumping ``Spec.version``."""
    SprintScopeResolver.clear_cache()
    sprint = SimpleNamespace(
        id="sprint-narrow-status",
        version=1,
        test_scenario_ids=["ts-1"],
        business_rule_ids=[],
    )
    spec = SimpleNamespace(
        id="spec-narrow-status",
        version=1,
        functional_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[
            {"id": "ts-1", "status": "draft", "linked_task_ids": []}
        ],
        business_rules=[],
        technical_requirements=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
    )

    draft = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
    assert [item.code for item in completion_blockers(draft)] == [
        "sprint_test_not_successful"
    ]

    # Narrow draft -> passed mutation, no semantic version bump and no explicit
    # resolver invalidation. A dry retry must immediately unblock.
    spec.test_scenarios = [
        {"id": "ts-1", "status": "passed", "linked_task_ids": []}
    ]
    passed = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
    assert passed.items["test_scenarios"][0]["status"] == "passed"
    assert completion_blockers(passed) == ()

    # Reverse direction is the safety-critical branch: cached "passed" content
    # must never make a failed scenario pass open.
    spec.test_scenarios = [
        {"id": "ts-1", "status": "failed", "linked_task_ids": []}
    ]
    failed = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
    assert failed.items["test_scenarios"][0]["status"] == "failed"
    assert [item.code for item in completion_blockers(failed)] == [
        "sprint_test_not_successful"
    ]


@pytest.mark.asyncio
async def test_real_scenario_status_writer_refreshes_scope_both_directions(
    db_factory,
) -> None:
    """The real narrow writer must not leave either stale fail-closed/open state."""
    from okto_pulse.core.services.main import SpecService

    suffix = uuid.uuid4().hex[:8]
    board_id = f"scope-status-board-{suffix}"
    spec_id = f"scope-status-spec-{suffix}"
    scenario_id = f"scope-status-ts-{suffix}"
    actor_id = f"scope-status-actor-{suffix}"
    sprint = SimpleNamespace(
        id=f"scope-status-sprint-{suffix}",
        version=1,
        test_scenario_ids=[scenario_id],
        business_rule_ids=[],
    )

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Scope status",
                owner_id=actor_id,
                settings={"skip_test_evidence_global": True},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Narrow scenario status",
                status=SpecStatus.IN_PROGRESS,
                version=11,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[
                    {
                        "id": scenario_id,
                        "title": "Narrow status",
                        "status": "draft",
                        "linked_task_ids": [],
                    }
                ],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=actor_id,
            )
        )
        await db.commit()

    SprintScopeResolver.clear_cache()
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        before = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
        assert [item.code for item in completion_blockers(before)] == [
            "sprint_test_not_successful"
        ]

    async with db_factory() as db:
        service = SpecService(db)
        result = await service.set_test_scenario_status(
            spec_id,
            actor_id,
            scenario_id,
            "passed",
        )
        assert result["new_status"] == "passed"
        assert (await service.get_spec(spec_id)).version == 11

    # The resolver cache is still primed with the same semantic versions. A
    # fresh DB read must nevertheless project the narrow writer's current data.
    async with db_factory() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        passed = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
        assert passed.items["test_scenarios"][0]["status"] == "passed"
        assert completion_blockers(passed) == ()

        recovery = await service.set_test_scenario_status(
            spec_id,
            actor_id,
            scenario_id,
            "ready",
        )
        assert recovery["new_status"] == "ready"
        result = await service.set_test_scenario_status(
            spec_id,
            actor_id,
            scenario_id,
            "failed",
        )
        assert result["new_status"] == "failed"
        assert (await service.get_spec(spec_id)).version == 11

    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        failed = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[])
        assert failed.items["test_scenarios"][0]["status"] == "failed"
        assert [item.code for item in completion_blockers(failed)] == [
            "sprint_test_not_successful"
        ]


def test_sprint_scope_dry_retry_re_resolves_narrow_link_membership() -> None:
    """Linked-task membership is fingerprinted even without version bumps."""
    SprintScopeResolver.clear_cache()
    sprint = SimpleNamespace(
        id="sprint-narrow-links",
        version=1,
        test_scenario_ids=[],
        business_rule_ids=[],
    )
    spec = SimpleNamespace(
        id="spec-narrow-links",
        version=1,
        functional_requirements=[
            {"id": "fr-1", "linked_task_ids": []}
        ],
        acceptance_criteria=[],
        test_scenarios=[],
        business_rules=[],
        technical_requirements=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
    )
    card = SimpleNamespace(
        id="card-1",
        version=1,
        status="not_started",
        card_type="normal",
        test_scenario_ids=[],
    )

    unlinked = SprintScopeResolver.resolve(
        sprint=sprint, spec=spec, cards=[card]
    )
    assert unlinked.ids("functional_requirements") == ()

    spec.functional_requirements = [
        {"id": "fr-1", "linked_task_ids": ["card-1"]}
    ]
    linked = SprintScopeResolver.resolve(
        sprint=sprint, spec=spec, cards=[card]
    )
    assert linked.ids("functional_requirements") == ("fr-1",)
    assert linked.provenance["functional_requirements"]["fr-1"] == (
        "assigned_card_link",
    )

    spec.functional_requirements = [
        {"id": "fr-1", "linked_task_ids": []}
    ]
    unlinked_again = SprintScopeResolver.resolve(
        sprint=sprint, spec=spec, cards=[card]
    )
    assert unlinked_again.ids("functional_requirements") == ()


@pytest.mark.asyncio
async def test_locked_traceability_narrow_writer_refreshes_scope_without_version_bump(
    db_factory,
) -> None:
    """Exercise the real no-version-bump writer, not only an in-memory mutation."""
    from okto_pulse.core.services.main import SpecService

    suffix = uuid.uuid4().hex[:8]
    board_id = f"scope-link-board-{suffix}"
    spec_id = f"scope-link-spec-{suffix}"
    sprint_id = f"scope-link-sprint-{suffix}"
    card_id = f"scope-link-card-{suffix}"
    requirement_id = f"scope-link-fr-{suffix}"
    actor_id = f"scope-link-actor-{suffix}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Scope link", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Locked scope link",
                status=SpecStatus.VALIDATED,
                version=7,
                current_validation_id=f"val-{suffix}",
                validations=[
                    {
                        "id": f"val-{suffix}",
                        "outcome": "success",
                        "edition": 1,
                    }
                ],
                functional_requirements=[
                    {
                        "id": requirement_id,
                        "title": "Linked narrowly",
                        "linked_task_ids": [],
                    }
                ],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=actor_id,
            )
        )
        await db.flush()
        sprint = Sprint(
            id=sprint_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Scope link sprint",
            status=SprintStatus.DRAFT,
            version=1,
            created_by=actor_id,
        )
        card = Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            sprint_id=sprint_id,
            title="Scope link card",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=actor_id,
        )
        db.add_all([sprint, card])
        await db.commit()

        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        SprintScopeResolver.clear_cache()
        before = SprintScopeResolver.resolve(
            sprint=sprint,
            spec=spec,
            cards=[card],
        )
        assert before.ids("functional_requirements") == ()

        updated, changed, task_ids = (
            await service.append_locked_traceability_task_link(
                spec_id,
                actor_id,
                target_field="functional_requirements",
                target_id=requirement_id,
                card_id=card_id,
            )
        )
        assert changed is True
        assert task_ids == [card_id]
        assert updated.version == 7
        await db.commit()

        fresh_spec = await service.get_spec(spec_id)
        assert fresh_spec.version == 7
        after = SprintScopeResolver.resolve(
            sprint=sprint,
            spec=fresh_spec,
            cards=[card],
        )
        assert after.ids("functional_requirements") == (requirement_id,)
        assert after.provenance["functional_requirements"][requirement_id] == (
            "assigned_card_link",
        )


@pytest.mark.parametrize(
    ("mode", "allowed", "warning", "source"),
    [
        (None, True, False, "legacy_absent_compat"),
        ("off", True, False, "board_settings"),
        ("warn", True, True, "board_settings"),
        ("enforce", False, False, "board_settings"),
    ],
)
def test_reviewer_separation_modes_are_explicit(
    mode: str | None, allowed: bool, warning: bool, source: str
) -> None:
    settings = {} if mode is None else {"reviewer_separation_mode": mode}
    decision = evaluate_reviewer_separation(
        board=SimpleNamespace(settings=settings),
        reviewer_id="same-user",
        sprint=SimpleNamespace(created_by="same-user"),
    )
    assert decision.allowed is allowed
    assert decision.warning is warning
    assert decision.source == source
