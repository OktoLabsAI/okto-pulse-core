"""Method authoring is semantic; method labels and legacy evidence grant no credit."""

from types import SimpleNamespace

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


def test_legacy_omission_is_not_defaulted_or_rewritten():
    assert "verification_method" not in Scenario.model_validate(scenario()).model_dump()
    unknown = scenario(verification_method="old-observation")
    assert Scenario.model_validate(unknown).verification_method == "old-observation"
    assert (
        resolve_scenario_types_for_whole_list_write([scenario()], [unknown])[0][
            "verification_method"
        ]
        == "old-observation"
    )
    validate_scenario_types_for_write([unknown], [unknown])
    assert (
        resolve_scenario_types_for_whole_list_write([scenario()], [])[0].get(
            "verification_method"
        )
        is None
    )


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


def test_authored_method_binds_criterion_qualification_without_rewriting_legacy_hash():
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
        assert digest(None) == digest(None, **metadata)
        assert digest("automated_test") != digest("automated_test", **metadata)
    assert digest("automated_test") == digest(
        "automated_test", linked_task_ids=["card"]
    )


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
