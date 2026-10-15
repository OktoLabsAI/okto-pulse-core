"""Method authoring is semantic; method labels and legacy evidence grant no credit."""

from types import SimpleNamespace
from copy import deepcopy

import pytest
from pydantic import ValidationError
from sqlalchemy import update
from sqlalchemy_test_models import Spec

from okto_pulse.core.domain.test_scenarios import (
    VALID_SCENARIO_TYPES,
    VALID_VERIFICATION_METHODS,
)
from okto_pulse.core.models.schemas import (
    TestScenario as Scenario,
    TestScenarioWrite as ScenarioWrite,
    SpecUpdate,
    scenario_collection_snapshot,
)
from okto_pulse.core.ports.test_evidence import (
    register_test_evidence_write_verifier,
    supported_test_verification_methods,
)
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services import main as service_module
from okto_pulse.core.services.test_scenario_lifecycle import (
    compute_test_scenario_semantic_sha256,
    evidence_invalidated_by_semantic_edit,
    resolve_scenario_types_for_whole_list_write,
    validate_scenario_types_for_write,
    scenario_has_authenticated_required_evidence,
)
from test_test_scenario_lifecycle import _seed_spec, _VALID_EVIDENCE, USER


def scenario(**kwargs):
    return {
        "id": "ts",
        "title": "Observe blocking",
        "scenario_type": "manual",
        "given": "Five attempts",
        "when": "Access is requested",
        "then": "Access is blocked",
        "status": "ready",
        "linked_criteria": ["ac_one"],
        **kwargs,
    }


@pytest.mark.parametrize("method", VALID_VERIFICATION_METHODS)
def test_method_and_scenario_type_are_independent_closed_dimensions(method):
    parsed = ScenarioWrite.model_validate(scenario(verification_method=method))
    assert parsed.scenario_type == "manual" and parsed.verification_method == method
    assert VALID_SCENARIO_TYPES == ("unit", "integration", "e2e", "manual", "negative")


@pytest.mark.parametrize(
    "method", ["passing", "performance", "none", True, {"trusted": True}]
)
def test_unsupported_values_are_not_ignored_by_model_or_direct_writer(method):
    with pytest.raises(ValidationError):
        ScenarioWrite.model_validate(scenario(verification_method=method))
    with pytest.raises(ValueError, match="verification_method_invalid"):
        validate_scenario_types_for_write([scenario(verification_method=method)], [])


def test_omission_preserves_current_method_and_invalid_values_are_refused():
    assert "verification_method" not in Scenario.model_validate(scenario()).model_dump()
    current = scenario(verification_method="inspection")
    resolved = resolve_scenario_types_for_whole_list_write([scenario()], [current])
    assert resolved[0]["verification_method"] == "inspection"
    validate_scenario_types_for_write(resolved, [current])
    unknown = scenario(verification_method="old-observation")
    with pytest.raises(ValidationError):
        Scenario.model_validate(unknown)
    with pytest.raises(ValueError, match="verification_method_invalid"):
        validate_scenario_types_for_write([unknown], [unknown])




def test_method_changes_receipt_binding_but_absence_and_editorial_changes_do_not():
    def digest(value):
        return compute_test_scenario_semantic_sha256(
            board_id="b",
            spec_id="s",
            scenario=value,
            acceptance_criteria=[{"id": "ac_one", "text": "Block access"}],
        )

    base = digest(scenario())
    assert base == digest(scenario(verification_method=None))
    assert base == digest(scenario(notes="Editorial", title="Renamed"))
    assert (
        len(
            {
                digest(scenario(verification_method=m))
                for m in VALID_VERIFICATION_METHODS
            }
            | {base}
        )
        == 5
    )
    assert evidence_invalidated_by_semantic_edit(["verification_method"])


def test_capability_requires_both_the_core_contract_and_concrete_verifier():
    assert supported_test_verification_methods() is None
    register_test_evidence_write_verifier(SimpleNamespace(verify=lambda **kw: None))
    assert supported_test_verification_methods() is None
    register_test_evidence_write_verifier(
        SimpleNamespace(verification_methods=frozenset(VALID_VERIFICATION_METHODS))
    )
    assert supported_test_verification_methods() == frozenset(VALID_VERIFICATION_METHODS)
    register_test_evidence_write_verifier(SimpleNamespace(verification_methods=frozenset({"automated_test"})))
    assert supported_test_verification_methods() == frozenset({"automated_test"})


def test_native_digest_binds_selected_criterion_qualification_even_before_method_selection():
    criterion = {"id": "ac_one", "text": "Block access"}

    def digest(method, **metadata):
        return compute_test_scenario_semantic_sha256(
            board_id="b",
            spec_id="s",
            scenario=scenario(verification_method=method),
            acceptance_criteria=[{**criterion, **metadata}],
        )

    for metadata in (
        {"verification_profile": "functional"},
        {
            "requirement_links": [
                {
                    "requirement_type": "business_rule",
                    "requirement_id": "br_block",
                    "aspect": "Five attempts",
                }
            ]
        },
    ):
        assert digest(None) != digest(None, **metadata)
        assert digest("automated_test") != digest("automated_test", **metadata)
    assert digest("automated_test") == digest(
        "automated_test", linked_task_ids=["card"]
    )


def test_native_digest_limits_applicability_to_exact_selected_criteria():
    selected = {"id": "ac_one", "text": "Latency below 200 ms", "verification_profile": "technical"}
    other = {"id": "other", "text": "An independent condition"}

    def digest(criteria, **changes):
        return compute_test_scenario_semantic_sha256(board_id="b", spec_id="s",
            scenario=scenario(verification_method="automated_test", **changes), acceptance_criteria=criteria)

    baseline = digest([selected, other])
    assert baseline == digest([{**other, "text": "Changed independent condition"}, selected])
    assert baseline == digest([{**selected, "notes": "Spelling corrected", "locale": "pt"}])
    assert baseline != digest([{**selected, "text": "Latency below 100 ms"}])
    assert baseline != digest([{**selected, "requirement_links": [
        {"requirement_type": "technical_requirement", "requirement_id": "tr"}]}])
    assert baseline != digest([selected, other], linked_criteria=["ac_one", "other"])
    for criteria in ([], [other], [selected, selected]):
        with pytest.raises(ValueError, match="scenario_criterion_scope_unresolved"):
            digest(criteria)
    for links in (["ac_one", "ac_one"], [0], [" "]):
        with pytest.raises(ValueError, match="scenario_criterion_scope_invalid"):
            digest([selected], linked_criteria=links)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", VALID_VERIFICATION_METHODS)
@pytest.mark.parametrize("skip", [False, True])
async def test_explicit_method_cannot_receive_legacy_credit_even_with_board_skip(
    db_factory, method, skip
):
    register_test_evidence_write_verifier(
        SimpleNamespace(verification_methods=frozenset({"automated_test"}))
    )
    board, spec_id, _ = await _seed_spec(
        db_factory, scenarios=[scenario(verification_method=method)], skip_evidence=skip
    )
    async with db_factory() as db:
        with pytest.raises(
            ValueError,
            match="verification_evidence_authenticated_result_required|verification_method_unsupported",
        ):
            await SpecService(db).set_test_scenario_status(
                spec_id, USER, "ts", "passed", _VALID_EVIDENCE
            )
        assert (await db.get(Spec, spec_id)).test_scenarios[0]["status"] == "ready"
    assert not scenario_has_authenticated_required_evidence(
        board_id=board,
        spec_id=spec_id,
        scenario=scenario(
            verification_method=method, status="passed", evidence=_VALID_EVIDENCE
        ),
        acceptance_criteria=[],
    )


@pytest.mark.asyncio
async def test_body_edit_preserves_type_resets_proof_and_rejects_stale_version(
    db_factory,
):
    _, spec_id, _ = await _seed_spec(
        db_factory,
        scenarios=[
            scenario(status="passed", evidence=_VALID_EVIDENCE),
            scenario(id="other"),
        ],
    )
    async with db_factory() as db:
        service = SpecService(db)
        before = await db.get(Spec, spec_id)
        version = before.version
        with pytest.raises(ValueError, match="spec_version_conflict"):
            await service.update_test_scenario(
                spec_id,
                USER,
                "ts",
                verification_method="automated_test",
                expected_spec_version=version + 1,
            )
        result = await service.update_test_scenario(
            spec_id,
            USER,
            "ts",
            verification_method="automated_test",
            expected_spec_version=version,
        )
        assert result["evidence_invalidated"]
        await db.refresh(before)
        value = before.test_scenarios[0]
        assert (
            value["verification_method"] == "automated_test"
            and value["scenario_type"] == "manual"
        )
        assert value["status"] == "ready" and value["evidence"] is None
        assert before.test_scenarios[1]["id"] == "other"
        replacement = [dict(item) for item in before.test_scenarios]
        replacement[0].pop("verification_method")
        await service.update_spec(spec_id, USER, SpecUpdate(test_scenarios=replacement))
        await db.commit()
        await db.refresh(before)
        assert before.test_scenarios[0]["verification_method"] == "automated_test"


@pytest.mark.asyncio
async def test_method_version_fence_rechecks_after_initial_read(
    db_factory, monkeypatch
):
    _, spec_id, _ = await _seed_spec(db_factory, scenarios=[scenario()])
    original_fence = service_module._application_fence

    async def intervening_write(context, entity, record_id, *, expected_values):
        # Advance the real row after the service's read, before its conditional
        # write. The stale in-memory version alone must never authorize saving.
        await context.execute(
            update(Spec)
            .where(Spec.id == record_id)
            .values(version=expected_values["version"] + 1)
        )
        return await original_fence(
            context, entity, record_id, expected_values=expected_values
        )

    monkeypatch.setattr(service_module, "_application_fence", intervening_write)
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
        with pytest.raises(ValueError, match="spec_version_conflict"):
            await SpecService(db).update_test_scenario(
                spec_id,
                USER,
                "ts",
                verification_method="automated_test",
                expected_spec_version=before.version,
            )
        await db.refresh(before)
        assert before.test_scenarios[0].get("verification_method") is None


@pytest.mark.asyncio
async def test_collection_version_fence_rechecks_after_initial_read(db_factory, monkeypatch):
    _, spec_id, _ = await _seed_spec(db_factory, scenarios=[scenario()])
    original_fence = service_module._application_fence
    calls = []

    async def intervening_write(context, entity, record_id, *, expected_values):
        calls.append(record_id)
        await context.execute(
            update(Spec).where(Spec.id == record_id).values(version=expected_values["version"] + 1)
        )
        return await original_fence(context, entity, record_id, expected_values=expected_values)

    monkeypatch.setattr(service_module, "_application_fence", intervening_write)
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
        original_scenarios = [dict(item) for item in before.test_scenarios]
        with pytest.raises(ValueError, match="spec_version_conflict"):
            await SpecService(db).update_spec(
                spec_id, USER,
                SpecUpdate(test_scenarios=[], expected_spec_version=before.version,
                    expected_scenario_snapshot=scenario_collection_snapshot(
                        board_id=before.board_id, spec_id=before.id, edition=before.edition,
                        version=before.version, scenarios=before.test_scenarios)),
            )
        assert calls == [spec_id]
        await db.refresh(before)
        assert before.test_scenarios == original_scenarios


@pytest.mark.asyncio
@pytest.mark.parametrize("operational_change", ["status", "task_link"])
async def test_collection_does_not_restore_stale_operational_state_without_version_bump(
    db_factory, operational_change,
):
    _, spec_id, card_id = await _seed_spec(
        db_factory, scenarios=[scenario(status="draft")], card_scenarios=["ts"],
    )
    async with db_factory() as db:
        row = await db.get(Spec, spec_id)
        row.test_scenarios = [{**row.test_scenarios[0], "linked_task_ids": [card_id]}]
        await db.commit()
        authored = deepcopy(row.test_scenarios)
        version = row.version
        snapshot = scenario_collection_snapshot(board_id=row.board_id, spec_id=row.id,
            edition=row.edition, version=row.version, scenarios=authored)

    async with db_factory() as db:
        service = SpecService(db)
        if operational_change == "status":
            await service.set_test_scenario_status(spec_id, USER, "ts", "ready")
        else:
            await service.remove_scenario_traceability_task_link(
                spec_id, USER, scenario_id="ts", card_id=card_id,
            )
            await db.commit()
        current = await db.get(Spec, spec_id)
        assert current.version == version
        operational_state = deepcopy(current.test_scenarios[0])

    async with db_factory() as db:
        try:
            await SpecService(db).update_spec(
                spec_id, USER,
                SpecUpdate(test_scenarios=authored + [scenario(id="new", status="draft")],
                           expected_spec_version=version, expected_scenario_snapshot=snapshot),
            )
            await db.commit()
        except ValueError as error:
            # A scoped conflict or the existing status-only writer guard is
            # acceptable; a whole-list author must never restore old metadata.
            assert str(error).startswith(("spec_version_conflict", "scenario_snapshot_conflict",
                                          "test_scenario_status_requires_scoped_update"))
            await db.rollback()
    async with db_factory() as db:
        row = await db.get(Spec, spec_id)
        current = next(item for item in row.test_scenarios if item["id"] == "ts")
        assert current["linked_task_ids"] == operational_state["linked_task_ids"]
        assert current["status"] == operational_state["status"]


@pytest.mark.asyncio
async def test_collection_snapshot_is_rechecked_under_fence_with_stale_identity_map(db_factory, monkeypatch):
    _, spec_id, _ = await _seed_spec(db_factory, scenarios=[scenario(status="draft")])
    original_fence = service_module._application_fence
    changed = scenario(status="ready")

    async def intervening_write(context, entity, record_id, *, expected_values):
        # A real row mutation between read and fence, deliberately keeping the
        # loaded ORM identity stale and the Spec version unchanged.
        await context.execute(update(Spec).where(Spec.id == record_id)
            .values(test_scenarios=[changed]).execution_options(synchronize_session=False))
        return await original_fence(context, entity, record_id, expected_values=expected_values)

    monkeypatch.setattr(service_module, "_application_fence", intervening_write)
    async with db_factory() as db:
        before = await db.get(Spec, spec_id)
        version = before.version
        snapshot = scenario_collection_snapshot(board_id=before.board_id, spec_id=before.id,
            edition=before.edition, version=version, scenarios=before.test_scenarios)
        with pytest.raises(ValueError, match="^scenario_snapshot_conflict$"):
            await SpecService(db).update_spec(spec_id, USER, SpecUpdate(
                test_scenarios=[scenario(status="draft"), scenario(id="new", status="draft")],
                expected_spec_version=version, expected_scenario_snapshot=snapshot))
        await db.refresh(before)
        assert before.version == version
        assert before.test_scenarios == [changed]


def test_scenario_snapshot_binds_operational_facts_and_scope_not_json_key_order():
    def digest(items, **scope):
        return scenario_collection_snapshot(**({"board_id": "b", "spec_id": "s", "edition": 1,
            "version": 1} | scope), scenarios=items)

    original = scenario(linked_task_ids=["task"])
    baseline = digest([original])
    assert baseline == digest([dict(reversed(list(original.items())))])
    assert baseline == digest([Scenario.model_validate(original)])
    assert baseline == digest([{**original, "verification_method": None}])
    for changes in ({"linked_task_ids": []}, {"status": "draft"}, {"notes": "changed"},
                    {"evidence": {"test_run_id": "run"}}):
        assert baseline != digest([{**original, **changes}])
    for scope in ({"board_id": "other"}, {"spec_id": "other"}, {"edition": 2}, {"version": 2}):
        assert baseline != digest([original], **scope)
    assert digest(None) == digest([])
