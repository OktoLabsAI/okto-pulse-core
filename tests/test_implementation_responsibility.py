"""Declared contribution scope is not ownership inferred from link counts or proof."""

from copy import deepcopy

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.implementation_plan import (
    RequirementImplementationPlan,
    authored_contribution_card_ids,
)
from okto_pulse.core.domain.requirement_verification import (
    requirement_verification_digest,
)
from okto_pulse.core.ports.delivery_inventory import default_delivery_inventory_policy
from test_requirement_verification import population, resolve, inherited


def allocation(
    card_id="owner", scope="whole_requirement", criterion_ids=(), summary=None
):
    return {
        "card_id": card_id,
        "scope": scope,
        "criterion_ids": list(criterion_ids),
        "summary": summary,
    }


def plan(*contributions):
    return {"contributions": list(contributions or (allocation(),))}


def card(identity="owner", **extra):
    return {
        "id": identity,
        "board_id": "board",
        "spec_id": "spec",
        "card_type": "normal",
        "status": "not_started",
        "archived": False,
        **extra,
    }


def planned_population():
    data = population()
    data["functional_requirements"][0].update(
        linked_task_ids=["owner"], implementation_plan=plan()
    )
    return data


def responsibilities(data, cards=None):
    return default_delivery_inventory_policy().resolve_implementation_responsibility(
        board_id="board",
        spec_id="spec",
        collections=data,
        cards=[card()] if cards is None else cards,
        qualification=resolve(data),
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"contributions": []},
        {"contributions": [allocation(), allocation()]},
        plan(allocation(scope="partial")),
        plan(allocation(scope="selected_criteria")),
        plan(allocation(criterion_ids=["ac-login"])),
        plan({**allocation(), "approved": True}),
        {**plan(), "trusted": True},
        plan(allocation(card_id=" ")),
        plan(
            allocation(
                scope="selected_criteria", criterion_ids=["ac-login"], summary=" "
            )
        ),
    ],
)
def test_closed_authored_plan_rejects_completion_claims_and_ambiguous_scope(payload):
    with pytest.raises(ValidationError):
        RequirementImplementationPlan.model_validate(payload)


def test_direct_links_without_authored_scope_remain_pending():
    data = population()
    data["functional_requirements"][0]["linked_task_ids"] = ["owner"]
    result = responsibilities(data)
    assert not result.complete and result.rows[0].contributions == ()
    assert "implementation_plan_required" in result.rows[0].blockers


def test_single_whole_scope_is_declared_without_execution_or_materialization():
    data = planned_population()
    before = deepcopy(data)
    result = responsibilities(data)
    assert result.complete and data == before
    fact = result.rows[0].contributions[0]
    assert fact.origin == "direct" and fact.card_id == "owner"
    assert (
        fact.criterion_ids == ("ac-lock", "ac-login") and len(fact.scope_sha256) == 64
    )


@pytest.mark.parametrize(
    "damage",
    [
        {"archived": True},
        {"status": "cancelled"},
        {"card_type": "test"},
        {"board_id": "other"},
        {"spec_id": "other"},
    ],
)
def test_bad_card_cannot_own_implementation(damage):
    assert not responsibilities(planned_population(), [card(**damage)]).complete


def split_population():
    data = planned_population()
    fr = data["functional_requirements"][0]
    fr.update(
        linked_task_ids=["ui", "authorization"],
        implementation_plan=plan(
            allocation("ui", "selected_criteria", ["ac-login"], "Login interface"),
            allocation(
                "authorization",
                "selected_criteria",
                ["ac-lock"],
                "Enforce attempt counter and blocking",
            ),
        ),
    )
    data["business_rules"] = [
        {
            "id": "br",
            "title": "Blocking",
            "rule": "Block after five failures",
            "linked_requirements": [fr["id"]],
            "verification": inherited(fr),
        }
    ]
    return data


def test_inherited_br_uses_selected_contribution_not_every_card_of_source():
    data = split_population()
    result = responsibilities(data, [card("ui"), card("authorization")])
    assert result.complete
    br = next(row for row in result.rows if row.requirement_id == "br")
    assert [fact.card_id for fact in br.contributions] == ["authorization"]
    assert br.contributions[0].sources[0].requirement_id == "fr-auth"
    assert br.contributions[0].origin == "inherited"


def test_overlapping_scopes_need_explicit_br_allocation_and_preserve_legitimate_direct_links():
    data = split_population()
    data["functional_requirements"][0]["implementation_plan"]["contributions"][0][
        "criterion_ids"
    ].append("ac-lock")
    result = responsibilities(data, [card("ui"), card("authorization")])
    br = next(row for row in result.rows if row.requirement_id == "br")
    assert not result.complete and not br.contributions
    assert "implementation_inheritance_allocation_ambiguous" in br.blockers
    data["business_rules"][0].update(
        linked_task_ids=["authorization"],
        implementation_plan=plan(allocation("authorization")),
    )
    result = responsibilities(data, [card("ui"), card("authorization")])
    assert result.complete
    br = next(row for row in result.rows if row.requirement_id == "br")
    assert [fact.origin for fact in br.contributions] == ["direct"]


def test_scope_hash_changes_only_for_affected_contribution_and_preserves_requirement_binding():
    data = split_population()
    fr = data["functional_requirements"][0]
    before = responsibilities(data, [card("ui"), card("authorization")])
    source_digest = requirement_verification_digest(
        "spec", "functional_requirement", fr
    )
    fr["implementation_plan"]["contributions"][0]["summary"] = "UI flow and display"
    after = responsibilities(data, [card("ui"), card("authorization")])

    def hashes(result):
        return {
            (row.requirement_id, fact.card_id): fact.scope_sha256
            for row in result.rows
            for fact in row.contributions
        }

    old, new = hashes(before), hashes(after)
    assert old[("fr-auth", "ui")] != new[("fr-auth", "ui")]
    assert old[("fr-auth", "authorization")] == new[("fr-auth", "authorization")]
    assert old[("br", "authorization")] == new[("br", "authorization")]
    assert (
        requirement_verification_digest("spec", "functional_requirement", fr)
        == source_digest
    )


def test_changed_declaration_requires_current_links_and_exact_criterion_ids():
    data = planned_population()

    def validate():
        return authored_contribution_card_ids(
            collections=data, criteria=data["acceptance_criteria"]
        )

    assert validate() == {"owner"}
    data["functional_requirements"][0]["linked_task_ids"] = []
    with pytest.raises(ValueError, match="canonical_task_link"):
        validate()
    previous = planned_population()
    assert (
        authored_contribution_card_ids(
            collections=data,
            criteria=data["acceptance_criteria"],
            previous_collections=previous,
        )
        == set()
    )
    assert not responsibilities(data).complete
    data["functional_requirements"][0].update(
        linked_task_ids=["owner"],
        implementation_plan=plan(
            allocation(
                scope="selected_criteria", criterion_ids=["0"], summary="Exact IDs only"
            )
        ),
    )
    with pytest.raises(ValueError, match="criterion_unresolved"):
        validate()


@pytest.mark.asyncio
async def test_existing_structured_writer_persists_all_five_types_and_rejects_bad_card_without_history(
    db_factory,
):
    from sqlalchemy import select, func
    from sqlalchemy_test_models import Spec, Card, SpecHistory
    from test_criterion_verification import seed, create, criterion
    from okto_pulse.core.infra.permissions import (
        get_builtin_presets,
        resolve_permissions,
    )
    from okto_pulse.core.domain.criterion_verification import (
        VERIFICATION_REQUIREMENT_FIELDS,
    )
    from okto_pulse.core.services.spec_structured_entities import (
        StructuredSpecEntityCommand,
        StructuredSpecEntityService,
    )

    board_id = await seed(db_factory)
    flags = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Spec")
    permissions = resolve_permissions(None, flags, None)
    async with db_factory() as db:
        created = await create(
            db,
            board_id,
            acceptance_criteria=[criterion()],
            technical_requirements=[{"id": "tr", "text": "Latency"}],
            business_rules=[
                {
                    "id": "br",
                    "title": "Block",
                    "rule": "Block",
                    "when": "Five",
                    "then": "Reject",
                }
            ],
            integration_requirements=[{"id": "ir", "title": "Contract"}],
            observability_requirements=[{"id": "or", "title": "Alert"}],
        )
        await db.commit()
        spec = await db.get(Spec, created.id)
        db.add(
            Card(
                id="owner",
                board_id=board_id,
                spec_id=spec.id,
                title="Implement",
                card_type="normal",
                status="not_started",
                created_by="author",
                archived=False,
            )
        )
        db.add(
            Card(
                id="test-owner",
                board_id=board_id,
                spec_id=spec.id,
                title="Verify",
                card_type="test",
                status="not_started",
                created_by="author",
                archived=False,
            )
        )
        db.add(
            Card(
                id="outside",
                board_id=board_id,
                spec_id=None,
                title="Unassigned",
                card_type="normal",
                status="not_started",
                created_by="author",
                archived=False,
            )
        )
        await db.commit()
        service = StructuredSpecEntityService(db)

        def command(kind, identity, payload):
            return StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec.id,
                actor_id="author",
                entity_type=kind,
                entity_id=identity,
                operation="update",
                expected_spec_version=spec.version,
                payload=payload,
                permission_set=permissions,
            )

        for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items():
            result = await service.mutate(
                command(
                    kind,
                    getattr(spec, field)[0]["id"],
                    {"linked_task_ids": ["owner"], "implementation_plan": plan()},
                )
            )
            assert result.success, result.as_dict()
            await db.commit()
            await db.refresh(spec)
            assert (
                getattr(spec, field)[0]["implementation_plan"]["contributions"][0][
                    "card_id"
                ]
                == "owner"
            )
        for card_id in ("missing", "test-owner", "outside"):
            version = spec.version
            count = await db.scalar(
                select(func.count())
                .select_from(SpecHistory)
                .where(SpecHistory.spec_id == spec.id)
            )
            result = await service.mutate(
                command(
                    "functional_requirement",
                    "fr_one",
                    {
                        "linked_task_ids": [card_id],
                        "implementation_plan": plan(allocation(card_id)),
                    },
                )
            )
            assert not result.success
            assert spec.version == version
            assert (
                await db.scalar(
                    select(func.count())
                    .select_from(SpecHistory)
                    .where(SpecHistory.spec_id == spec.id)
                )
                == count
            )
        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec.id,
                actor_id="author",
                entity_type="acceptance_criterion",
                entity_id="ac_one",
                operation="revoke",
                expected_spec_version=spec.version,
                preview_only=True,
                permission_set=permissions,
            )
        )
        assert preview.success, preview.as_dict()
        assert any(
            ref["target_id"] == "fr_one"
            and ref["reason"] == "revoke_affects_implementation_plan"
            for ref in preview.impact_report["impacted_refs"]
        )
        spec.status = "in_progress"
        await db.commit()
        from okto_pulse.core.domain.human_validation_cycle import (
            SubjectEditRequiresDraftError,
        )

        with pytest.raises(SubjectEditRequiresDraftError):
            await service.mutate(
                command(
                    "functional_requirement", "fr_one", {"implementation_plan": None}
                )
            )


def test_criterion_change_updates_only_dependent_scope_and_editorial_notes_do_not():
    data = split_population()

    def hashes():
        return {
            (row.requirement_id, fact.card_id): fact.scope_sha256
            for row in responsibilities(data, [card("ui"), card("authorization")]).rows
            for fact in row.contributions
        }

    before = hashes()
    data["acceptance_criteria"][0]["notes"] = "Editorial note"
    assert hashes() == before
    data["acceptance_criteria"][0]["text"] = "A different observable login outcome"
    after = hashes()
    assert before[("fr-auth", "ui")] != after[("fr-auth", "ui")]
    assert before[("fr-auth", "authorization")] == after[("fr-auth", "authorization")]
    assert before[("br", "authorization")] == after[("br", "authorization")]


def test_missing_or_oversized_card_population_is_unavailable_not_empty():
    for cards in (None, [card()] * 5001):
        result = (
            default_delivery_inventory_policy().resolve_implementation_responsibility(
                board_id="board",
                spec_id="spec",
                collections=planned_population(),
                cards=cards,
                qualification=resolve(planned_population()),
            )
        )
        assert not result.complete and not result.population_complete


def test_whole_source_scope_can_supply_br_with_its_own_explicit_criterion():
    data = planned_population()
    data["business_rules"] = [
        {
            "id": "br",
            "rule": "Block",
            "linked_requirements": ["fr-auth"],
            "verification": {"mode": "explicit", "required_profiles": ["functional"]},
        }
    ]
    data["acceptance_criteria"].append(
        {
            "id": "ac-br",
            "text": "Block after five failures",
            "verification_profile": "functional",
            "requirement_links": [
                {"requirement_type": "business_rule", "requirement_id": "br"}
            ],
        }
    )
    result = responsibilities(data)
    assert result.complete
    br = next(row for row in result.rows if row.requirement_id == "br")
    assert br.contributions[0].card_id == "owner"
    assert br.contributions[0].criterion_ids == ("ac-br",)
