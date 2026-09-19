"""Qualification is explicit, source-bound and never transitive proof credit."""

import copy

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.criterion_verification import (
    VERIFICATION_REQUIREMENT_FIELDS,
)
from okto_pulse.core.domain.requirement_verification import (
    RequirementVerification,
    requirement_verification_digest,
    validate_requirement_verification_references,
    verification_default_proposal,
)
from okto_pulse.core.domain.requirement_verification_resolution import (
    resolve_requirement_verification,
)
from okto_pulse.core.application.use_cases.requirement_verification import (
    GetRequirementVerificationCommand,
    RequirementVerificationReadError,
    project_requirement_verification,
)
from okto_pulse.core.models.schemas import (
    BusinessRule,
    IntegrationRequirement,
    ObservabilityRequirement,
)


def explicit(*profiles):
    return {"mode": "explicit", "required_profiles": list(profiles or ("functional",))}


def inherited(
    source,
    *,
    kind="functional_requirement",
    criteria=("ac-lock",),
    profile="functional",
):
    return {
        "mode": "inherited",
        "required_profiles": [profile],
        "inheritance": [
            {
                "source": {"requirement_type": kind, "requirement_id": source["id"]},
                "source_digest": requirement_verification_digest("spec", kind, source),
                "criterion_ids": list(criteria),
                "covered_aspect": "Five attempts and access blocking",
            }
        ],
    }


def population():
    return {
        **{field: [] for field in VERIFICATION_REQUIREMENT_FIELDS.values()},
        "functional_requirements": [
            {
                "id": "fr-auth",
                "text": "Authenticate and block after five failures",
                "verification": explicit(),
            }
        ],
        "acceptance_criteria": [
            {
                "id": "ac-login",
                "text": "A correct login succeeds",
                "verification_profile": "functional",
                "requirement_links": [
                    {
                        "requirement_type": "functional_requirement",
                        "requirement_id": "fr-auth",
                    }
                ],
            },
            {
                "id": "ac-lock",
                "text": "Five failures block further access",
                "verification_profile": "functional",
                "requirement_links": [
                    {
                        "requirement_type": "functional_requirement",
                        "requirement_id": "fr-auth",
                    }
                ],
            },
        ],
    }


def resolve(data, **kwargs):
    return resolve_requirement_verification(spec_id="spec", collections=data, **kwargs)


def row(result, identity):
    return next(
        item for item in result["requirements"] if item["requirement_id"] == identity
    )


@pytest.mark.parametrize(
    "value",
    [
        {"mode": "none", "required_profiles": ["functional"]},
        {"mode": "explicit", "required_profiles": []},
        {"mode": "explicit", "required_profiles": ["functional", "functional"]},
        {"mode": "inherited", "required_profiles": ["functional"]},
        {**explicit(), "verified": True},
        {**explicit(), "evidence_policy_ref": "skip-all"},
        {"mode": "explicit", "required_profiles": ["manual"]},
    ],
)
def test_closed_configuration_never_accepts_dispensing_or_evidence_flags(value):
    with pytest.raises(ValidationError):
        RequirementVerification.model_validate(value)


@pytest.mark.parametrize(
    "model,payload",
    [
        (
            BusinessRule,
            {"id": "br", "title": "BR", "rule": "R", "when": "W", "then": "T"},
        ),
        (IntegrationRequirement, {"id": "ir", "title": "IR"}),
        (ObservabilityRequirement, {"id": "or", "title": "OR"}),
    ],
)
def test_typed_read_models_do_not_backfill_legacy_metadata(model, payload):
    assert "verification" not in model.model_validate(payload).model_dump()
    assert (
        model.model_validate({**payload, "verification": None}).model_dump()[
            "verification"
        ]
        is None
    )
    assert (
        model.model_validate({**payload, "verification": explicit()}).model_dump()[
            "verification"
        ]["mode"]
        == "explicit"
    )


def test_existing_br_fr_link_does_not_supply_qualification_or_credit():
    data = population()
    data["business_rules"] = [
        {
            "id": "br-lock",
            "rule": "Five failures block access",
            "linked_requirements": ["fr-auth"],
        }
    ]
    result = resolve(data)
    br = row(result, "br-lock")
    assert not br["qualification_resolved"]
    assert br["criteria_paths"] == []
    assert br["default_proposal"] is None
    assert not result["criteria_resolution_complete"]
    assert not any(
        result[name]
        for name in (
            "methods_evaluated",
            "execution_evaluated",
            "semantic_review_evaluated",
            "delivery_evaluated",
            "rollout_evaluated",
        )
    )


def test_selected_terminal_criteria_preserve_multihop_path_and_aspects():
    data = population()
    fr = data["functional_requirements"][0]
    first = {
        "id": "br-lock",
        "rule": "Block access after five failures",
        "verification": inherited(fr),
    }
    second = {
        "id": "br-audit",
        "rule": "Observe lock policy",
        "verification": inherited(first, kind="business_rule"),
    }
    data["business_rules"] = [first, second]
    result = resolve(data)
    assert result["criteria_resolution_complete"]
    selected = row(result, "br-audit")["criteria_paths"]
    assert [item["criterion_id"] for item in selected] == ["ac-lock"]
    assert [step["requirement_id"] for step in selected[0]["path"]] == [
        "br-audit",
        "br-lock",
        "fr-auth",
    ]
    assert len(selected[0]["source_digests"]) == 2
    assert selected[0]["aspects"] == [
        "Five attempts and access blocking",
        "Five attempts and access blocking",
        None,
    ]


def test_structural_success_is_not_semantic_approval_of_wrong_selected_condition():
    data = population()
    data["business_rules"] = [
        {
            "id": "br-lock",
            "rule": "Five failures block access",
            "verification": inherited(
                data["functional_requirements"][0], criteria=("ac-login",)
            ),
        }
    ]
    result = resolve(data)
    assert result[
        "criteria_resolution_complete"
    ]  # reviewer must reject the unrelated successful-login condition
    assert result["semantic_review_evaluated"] is False
    assert result["delivery_evaluated"] is False


@pytest.mark.parametrize(
    "damage,code",
    [
        ("material_change", "spec_scope_revision_conflict"),
        ("inactive_source", "verification_inheritance_source_invalid"),
        ("missing_terminal", "verification_inheritance_terminal_missing"),
        ("wrong_profile", "verification_path_missing"),
    ],
)
def test_inheritance_does_not_hide_changes_or_incompatible_profiles(damage, code):
    data = population()
    fr = data["functional_requirements"][0]
    config = inherited(
        fr, profile="technical" if damage == "wrong_profile" else "functional"
    )
    data["business_rules"] = [{"id": "br", "rule": "Rule", "verification": config}]
    if damage == "material_change":
        fr["text"] = "Block after ten failures"
    elif damage == "inactive_source":
        fr["status"] = "revoked"
    elif damage == "missing_terminal":
        config["inheritance"][0]["criterion_ids"] = ["other-criterion"]
    result = resolve(data)
    assert not result["criteria_resolution_complete"]
    assert code in {block["code"] for block in row(result, "br")["blockers"]}


def test_editorial_change_and_assignment_do_not_change_source_binding():
    data = population()
    fr = data["functional_requirements"][0]
    original = requirement_verification_digest("spec", "functional_requirement", fr)
    fr.update(notes="Editorial note", locale="pt", linked_task_ids=["task-a"])
    assert (
        requirement_verification_digest("spec", "functional_requirement", fr)
        == original
    )
    assert (
        requirement_verification_digest("other-spec", "functional_requirement", fr)
        != original
    )


def test_cycle_is_detected_even_when_its_digests_are_stale_and_direct_criteria_exist():
    data = population()
    fr = data["functional_requirements"][0]
    br = {"id": "br-cycle", "rule": "Rule", "verification": inherited(fr)}
    fr["verification"] = inherited(br, kind="business_rule")
    data["business_rules"] = [br]
    result = resolve(data)
    for identity in ("fr-auth", "br-cycle"):
        assert "verification_inheritance_cycle" in {
            block["code"] for block in row(result, identity)["blockers"]
        }
    assert not result["criteria_resolution_complete"]


def test_every_required_profile_and_policy_minimum_counts():
    data = population()
    data["functional_requirements"][0]["verification"] = explicit(
        "functional", "technical"
    )
    result = resolve(data)
    assert {"code": "verification_path_missing", "profile": "technical"} in row(
        result, "fr-auth"
    )["blockers"]
    result = resolve(
        population(),
        minimum_profiles={
            ("functional_requirement", "fr-auth"): frozenset({"technical"})
        },
    )
    assert {"code": "verification_policy_minimum_required"} in row(result, "fr-auth")[
        "blockers"
    ]


@pytest.mark.parametrize(
    "damage,code",
    [
        ("orphan", "criterion_requirement_link_missing"),
        ("condition", "criterion_condition_required"),
        ("profile", "criterion_profile_required"),
        ("duplicate", "verification_criterion_ambiguous"),
    ],
)
def test_all_active_criteria_remain_normative_planning_inputs(damage, code):
    data = population()
    if damage == "orphan":
        data["acceptance_criteria"][0]["requirement_links"] = []
    elif damage == "condition":
        data["acceptance_criteria"][0]["text"] = " "
    elif damage == "profile":
        data["acceptance_criteria"][0].pop("verification_profile")
    else:
        data["acceptance_criteria"].append(
            copy.deepcopy(data["acceptance_criteria"][0])
        )
    result = resolve(data)
    assert code in {item["code"] for item in result["issues"]}
    assert not result["criteria_resolution_complete"]


def test_draft_defaults_are_versioned_proposals_not_silent_writes():
    data = population()
    data["functional_requirements"][0].pop("verification")
    before = copy.deepcopy(data)
    result = resolve(data)
    assert data == before
    assert row(result, "fr-auth")["verification"] is None
    assert row(result, "fr-auth")["default_proposal"] == verification_default_proposal(
        "functional_requirement"
    )
    assert not result["criteria_resolution_complete"]


def test_authoring_rejects_wrong_snapshot_but_source_changes_leave_existing_selection_reviewable():
    data = population()
    data["business_rules"] = [
        {"id": "br", "verification": inherited(data["functional_requirements"][0])}
    ]
    before = copy.deepcopy(data)
    data["functional_requirements"][0]["text"] = "Changed condition"
    validate_requirement_verification_references(
        spec_id="spec",
        collections=data,
        criteria=data["acceptance_criteria"],
        previous_collections=before,
    )
    with pytest.raises(ValueError, match="spec_scope_revision_conflict"):
        validate_requirement_verification_references(
            spec_id="spec", collections=data, criteria=data["acceptance_criteria"]
        )


@pytest.mark.parametrize("damage", ["self", "unknown_source", "unknown_criterion"])
def test_invalid_inheritance_references_are_rejected_on_write(damage):
    data = population()
    config = inherited(data["functional_requirements"][0])
    data["business_rules"] = [{"id": "br", "verification": config}]
    if damage == "self":
        config["inheritance"][0]["source"] = {
            "requirement_type": "business_rule",
            "requirement_id": "br",
        }
    elif damage == "unknown_source":
        config["inheritance"][0]["source"]["requirement_id"] = "other"
    else:
        config["inheritance"][0]["criterion_ids"] = ["other"]
    with pytest.raises(ValueError, match="verification_inheritance_"):
        validate_requirement_verification_references(
            spec_id="spec", collections=data, criteria=data["acceptance_criteria"]
        )


def test_pagination_does_not_narrow_global_blockers_and_unknown_is_not_empty():
    data = population()
    data["technical_requirements"] = [{"id": "tr-pending", "text": "Latency"}]
    projected = project_requirement_verification(
        resolve(data), GetRequirementVerificationCommand("board", "spec", limit=1)
    )
    assert len(projected["items"]) == 1
    assert projected["total"] == 2 and projected["resolved_count"] == 1
    assert projected["criteria_resolution_complete"] is False
    data.pop("integration_requirements")
    projected = project_requirement_verification(
        resolve(data), GetRequirementVerificationCommand("board", "spec")
    )
    assert projected["total"] is None and projected["counts_scope"] == "observed"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": True},
        {"limit": 101},
        {"offset": -1},
        {"paths_offset": 1},
        {"requirement_type": "business_rule"},
        {"requirement_type": "unsupported", "requirement_id": "x"},
    ],
)
def test_read_window_and_identity_are_closed_before_io(kwargs):
    with pytest.raises(RequirementVerificationReadError):
        GetRequirementVerificationCommand("board", "spec", **kwargs)


def test_normalizing_defaults_is_not_a_new_author_selection():
    data = population()
    data["business_rules"] = [
        {"id": "br", "verification": inherited(data["functional_requirements"][0])}
    ]
    before = copy.deepcopy(data)
    data["functional_requirements"][0]["text"] = "A material change"
    data["business_rules"][0]["verification"] = RequirementVerification.model_validate(
        data["business_rules"][0]["verification"]
    ).model_dump(mode="json")
    validate_requirement_verification_references(
        spec_id="spec",
        collections=data,
        criteria=data["acceptance_criteria"],
        previous_collections=before,
    )
    assert not resolve(data)["criteria_resolution_complete"]


@pytest.mark.parametrize("damage", ["long_identity", "unknown_criterion_state"])
def test_unrepresentable_identity_or_unknown_state_cannot_be_complete(damage):
    data = population()
    if damage == "long_identity":
        data["functional_requirements"][0]["id"] = "x" * 256
    else:
        data["acceptance_criteria"][0]["status"] = "unrecognized"
    result = resolve(data)
    assert not result["criteria_resolution_complete"]
    assert result["issue_count"] > 0


def test_diamond_keeps_distinct_origins_without_duplicating_requirements():
    data = population()
    fr = data["functional_requirements"][0]
    left = {"id": "br-left", "verification": inherited(fr)}
    right = {"id": "br-right", "verification": inherited(fr)}
    top = inherited(left, kind="business_rule")
    top["inheritance"].extend(inherited(right, kind="business_rule")["inheritance"])
    data["business_rules"] = [left, right, {"id": "br-top", "verification": top}]
    result = resolve(data)
    assert result["criteria_resolution_complete"]
    assert len(result["requirements"]) == 4
    paths = row(result, "br-top")["criteria_paths"]
    assert len(paths) == 2
    assert {path["criterion_id"] for path in paths} == {"ac-lock"}
    assert {path["path"][1]["requirement_id"] for path in paths} == {"br-left", "br-right"}


def test_limits_fail_closed_and_terminal_pagination_makes_progress(monkeypatch):
    from okto_pulse.core.domain import requirement_verification_resolution as module

    data = population()
    template = data["acceptance_criteria"][0]
    data["acceptance_criteria"] = [{**template, "id": f"ac-{i:03}"} for i in range(125)]
    result = resolve(data)
    first = project_requirement_verification(
        result, GetRequirementVerificationCommand("board", "spec")
    )
    assert first["criteria_resolution_complete"]
    assert first["items"][0]["paths_has_more"]
    offset = first["items"][0]["next_paths_offset"]
    second = project_requirement_verification(
        result,
        GetRequirementVerificationCommand(
            "board",
            "spec",
            requirement_type="functional_requirement",
            requirement_id="fr-auth",
            paths_offset=offset,
        ),
    )
    ids = [
        path["criterion_id"]
        for page in (first, second)
        for path in page["items"][0]["criteria_paths"]
    ]
    assert len(ids) == len(set(ids)) == 125
    monkeypatch.setattr(module, "_MAX_NODES", 100)
    result = resolve(data)
    assert (
        not result["population_complete"] and not result["criteria_resolution_complete"]
    )


@pytest.mark.asyncio
async def test_real_writer_preserves_all_requirement_kinds_and_refusals_preserve_history(
    db_factory,
):
    from sqlalchemy import select, func
    from sqlalchemy_test_models import Spec, SpecHistory
    from test_criterion_verification import seed, create, criterion
    from okto_pulse.core.infra.permissions import (
        get_builtin_presets,
        resolve_permissions,
    )
    from okto_pulse.core.services.spec_structured_entities import (
        StructuredSpecEntityCommand,
        StructuredSpecEntityService,
    )

    board = await seed(db_factory)
    flags = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Spec")
    permissions = resolve_permissions(None, flags, None)
    async with db_factory() as db:
        spec = await create(
            db,
            board,
            acceptance_criteria=[criterion()],
            technical_requirements=[{"id": "tr", "text": "Latency"}],
            business_rules=[
                {
                    "id": "br",
                    "title": "Blocking",
                    "rule": "Block",
                    "when": "Five failures",
                    "then": "Reject",
                }
            ],
            integration_requirements=[{"id": "ir", "title": "Wire protocol"}],
            observability_requirements=[{"id": "or", "title": "Observe blocking"}],
        )
        await db.commit()
        spec = await db.get(Spec, spec.id)
        service = StructuredSpecEntityService(db)

        def command(kind, identity, payload, **kwargs):
            return StructuredSpecEntityCommand(
                board_id=board,
                spec_id=spec.id,
                actor_id="author",
                entity_type=kind,
                entity_id=identity,
                operation="update",
                expected_spec_version=spec.version,
                payload=payload,
                permission_set=permissions,
                **kwargs,
            )

        for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items():
            identity = getattr(spec, field)[0]["id"]
            result = await service.mutate(
                command(kind, identity, {"verification": explicit()})
            )
            assert result.success, result.as_dict()
            await db.commit()
            await db.refresh(spec)
            assert getattr(spec, field)[0]["verification"]["mode"] == "explicit"
        version = spec.version
        histories = await db.scalar(
            select(func.count())
            .select_from(SpecHistory)
            .where(SpecHistory.spec_id == spec.id)
        )
        bad = await service.mutate(
            command(
                "functional_requirement", "fr_one", {"verification": {"mode": "none"}}
            )
        )
        assert not bad.success
        assert spec.version == version
        assert (
            await db.scalar(
                select(func.count())
                .select_from(SpecHistory)
                .where(SpecHistory.spec_id == spec.id)
            )
            == histories
        )

        # The actual persisted, canonical source is what a client binds.
        config = inherited(spec.functional_requirements[0], criteria=("ac_one",))
        config["inheritance"][0]["source_digest"] = requirement_verification_digest(
            spec.id, "functional_requirement", spec.functional_requirements[0]
        )
        result = await service.mutate(
            command("business_rule", "br", {"verification": config})
        )
        assert result.success, result.as_dict()
        await db.commit()
        await db.refresh(spec)
        preserved = copy.deepcopy(spec.business_rules[0]["verification"])
        result = await service.mutate(
            command(
                "functional_requirement",
                "fr_one",
                {"text": "Ten failures block access"},
            )
        )
        assert result.success, result.as_dict()
        await db.commit()
        await db.refresh(spec)
        assert spec.business_rules[0]["verification"] == preserved
        resolved = resolve_requirement_verification(
            spec_id=spec.id,
            collections={
                **{
                    field: getattr(spec, field)
                    for field in VERIFICATION_REQUIREMENT_FIELDS.values()
                },
                "acceptance_criteria": spec.acceptance_criteria,
            },
        )
        assert "spec_scope_revision_conflict" in {
            item["code"] for item in row(resolved, "br")["blockers"]
        }
        edited = copy.deepcopy(preserved)
        edited["inheritance"][0]["covered_aspect"] = (
            "New selection over the stale definition"
        )
        refused = await service.mutate(
            command("business_rule", "br", {"verification": edited})
        )
        assert not refused.success
        assert (await db.get(Spec, spec.id)).business_rules[0][
            "verification"
        ] == preserved
        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board,
                spec_id=spec.id,
                actor_id="author",
                entity_type="functional_requirement",
                entity_id="fr_one",
                operation="revoke",
                expected_spec_version=spec.version,
                permission_set=permissions,
            )
        )
        assert preview.error_code == "impact_ack_required"
        assert any(
            ref["target_type"] == "business_rule" and ref["target_id"] == "br"
            for ref in preview.impact_report["impacted_refs"]
        )
